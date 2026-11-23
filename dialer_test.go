package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testNetwork(hedge, timeout time.Duration, parallel int) func() NetworkConfig {
	return func() NetworkConfig {
		n := defaultNetwork()
		n.HedgeAfter, n.ConnectTimeout, n.MaxParallel = hedge, timeout, parallel
		return n
	}
}

// fakeConn records Close so tests can check losers are cleaned up.
type fakeConn struct {
	net.Conn
	ip     string
	closed atomic.Bool
}

func (c *fakeConn) Close() error { c.closed.Store(true); return nil }

type scriptedDialer struct {
	mu    sync.Mutex
	ips   []string
	conns []*fakeConn
	plan  func(attempt int) (delay time.Duration, err error)
}

func (s *scriptedDialer) handshake(ctx context.Context, ip, _, _ string) (net.Conn, error) {
	s.mu.Lock()
	n := len(s.ips)
	s.ips = append(s.ips, ip)
	s.mu.Unlock()
	delay, err := s.plan(n)
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		if delay > time.Hour { // a stalled handshake only ends when cancelled
			return nil, ctx.Err()
		}
		<-time.After(delay) // finishes anyway after losing; must still be closed
	}
	if err != nil {
		return nil, err
	}
	c := &fakeConn{ip: ip}
	s.mu.Lock()
	s.conns = append(s.conns, c)
	s.mu.Unlock()
	return c, nil
}

func newScripted(t *testing.T, hedge, timeout time.Duration, parallel int, ips []string,
	plan func(int) (time.Duration, error)) (*HedgedDialer, *scriptedDialer) {
	t.Helper()
	s := &scriptedDialer{plan: plan}
	d := NewHedgedDialer(testNetwork(hedge, timeout, parallel))
	d.lookup = func(context.Context, string) ([]string, error) { return ips, nil }
	d.handshake = s.handshake
	return d, s
}

func TestHedgeBeatsSlowHandshake(t *testing.T) {
	d, s := newScripted(t, 50*time.Millisecond, 5*time.Second, 3, []string{"ip1", "ip2"},
		func(n int) (time.Duration, error) {
			if n == 0 {
				return 300 * time.Millisecond, nil // slow, finishes after losing
			}
			return 10 * time.Millisecond, nil
		})
	start := time.Now()
	conn, err := d.DialTLSContext(context.Background(), "tcp", "gw.example:443")
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("hedge should win in ~60ms, took %v", took)
	}
	if got := conn.(*fakeConn).ip; got != "ip2" {
		t.Errorf("winner should be the hedged attempt on the next IP, got %s", got)
	}
	time.Sleep(400 * time.Millisecond)
	s.mu.Lock()
	defer s.mu.Unlock()
	byIP := map[string]*fakeConn{}
	for _, c := range s.conns {
		byIP[c.ip] = c
	}
	if len(s.conns) != 2 || !byIP["ip1"].closed.Load() || byIP["ip2"].closed.Load() {
		t.Errorf("late loser (ip1) must be closed and winner (ip2) kept open: %d conns", len(s.conns))
	}
	st := d.Stats("gw.example")
	if st.Dials.Load() != 1 || st.Hedges.Load() != 1 {
		t.Errorf("stats dials=%d hedges=%d", st.Dials.Load(), st.Hedges.Load())
	}
}

func TestFastFailuresMoveToNextAddressImmediately(t *testing.T) {
	d, s := newScripted(t, time.Hour, 5*time.Second, 3, []string{"a", "b", "c"},
		func(n int) (time.Duration, error) {
			if n < 2 {
				return 0, errors.New("connection refused")
			}
			return 0, nil
		})
	conn, err := d.DialTLSContext(context.Background(), "tcp", "gw.example:443")
	if err != nil {
		t.Fatal(err)
	}
	if conn.(*fakeConn).ip != "c" || strings.Join(s.ips, ",") != "a,b,c" {
		t.Errorf("attempts should rotate a,b,c without waiting for hedge_after: %v", s.ips)
	}
}

func TestAllAttemptsFail(t *testing.T) {
	d, _ := newScripted(t, time.Millisecond, 5*time.Second, 3, []string{"a"},
		func(int) (time.Duration, error) { return 0, errors.New("reset") })
	_, err := d.DialTLSContext(context.Background(), "tcp", "gw.example:443")
	if err == nil || !strings.Contains(err.Error(), "all 3 attempts failed") {
		t.Fatalf("got %v", err)
	}
	if d.Stats("gw.example").Failures.Load() != 1 {
		t.Error("failure not counted")
	}
}

func TestAllAttemptsStallHitsConnectTimeout(t *testing.T) {
	d, s := newScripted(t, 20*time.Millisecond, 150*time.Millisecond, 3, []string{"a", "b"},
		func(int) (time.Duration, error) { return 2 * time.Hour, nil })
	start := time.Now()
	_, err := d.DialTLSContext(context.Background(), "tcp", "gw.example:443")
	if err == nil || !strings.Contains(err.Error(), "no handshake finished") {
		t.Fatalf("got %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("must give up at connect_timeout, took %v", took)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ips) != 3 {
		t.Errorf("expected 3 parallel attempts, got %d", len(s.ips))
	}
}

// stallListener swallows the first n connections without answering, so the
// client's TLS handshake hangs exactly like on the lossy path.
type stallListener struct {
	net.Listener
	n       atomic.Int32
	stalled chan net.Conn
}

func (l *stallListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.n.Add(-1) >= 0 {
			l.stalled <- c
			continue
		}
		return c, nil
	}
}

func TestRealTLSHedgeOverHTTP2(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte(r.Proto)); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	srv.EnableHTTP2 = true
	sl := &stallListener{Listener: srv.Listener, stalled: make(chan net.Conn, 4)}
	sl.n.Store(1)
	srv.Listener = sl
	srv.StartTLS()
	defer srv.Close()

	d := NewHedgedDialer(testNetwork(100*time.Millisecond, 5*time.Second, 3))
	d.tlsConfig.RootCAs = srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	tr := &http.Transport{DialTLSContext: d.DialTLSContext, ForceAttemptHTTP2: true}
	defer tr.CloseIdleConnections()

	start := time.Now()
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Errorf("custom dialer must keep HTTP/2, got %s", resp.Proto)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("stalled first handshake should be raced, took %v", took)
	}

	stalled := <-sl.stalled
	defer stalled.Close()
	if err := stalled.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	for {
		if _, err := stalled.Read(buf); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatal("losing attempt was never closed by the client")
			}
			break // EOF/reset: the client hung up, as it should
		}
	}
}

func TestResolveFallsBackToLastGoodAnswer(t *testing.T) {
	fail := false
	var budgets []time.Duration
	d := NewHedgedDialer(testNetwork(time.Second, 5*time.Second, 1))
	d.lookup = func(ctx context.Context, host string) ([]string, error) {
		if dl, ok := ctx.Deadline(); ok {
			budgets = append(budgets, time.Until(dl))
		}
		if fail {
			return nil, errors.New("lookup gw.example: i/o timeout")
		}
		return []string{"10.0.0.1", "10.0.0.2"}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if _, err := d.resolve(ctx, "never.example"); err != nil {
		t.Fatal(err)
	}
	fail = true
	if _, err := d.resolve(ctx, "other.example"); err == nil {
		t.Fatal("no cached answer: the DNS error must surface")
	}
	ips, err := d.resolve(ctx, "never.example")
	if err != nil || strings.Join(ips, ",") != "10.0.0.1,10.0.0.2" {
		t.Fatalf("DNS failure should fall back to the cached answer: %v %v", ips, err)
	}
	if n := d.Stats("never.example").StaleDNS.Load(); n != 1 {
		t.Errorf("stale DNS uses counted %d, want 1", n)
	}
	// first lookup had no cache: full ctx (~1m); later ones for the cached host are cut to freshDNSBudget
	if budgets[0] < 30*time.Second || budgets[2] > freshDNSBudget {
		t.Errorf("lookup budgets = %v", budgets)
	}

	d.dns.Store("old.example", dnsEntry{ips: []string{"10.9.9.9"}, at: time.Now().Add(-25 * time.Hour)})
	if _, err := d.resolve(ctx, "old.example"); err == nil {
		t.Error("answers older than 24h must not be used")
	}
}
