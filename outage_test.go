package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fastOutage(t *testing.T) {
	t.Helper()
	fastRetries(t)
	orig := outageBackoff
	outageBackoff = func(int) time.Duration { return 50 * time.Millisecond }
	t.Cleanup(func() { outageBackoff = orig })
}

func TestOutageHoldsUntilUpstreamReturns(t *testing.T) {
	fastOutage(t)
	addr := freeAddr(t) // nothing listens yet: the "network" is down
	relay := singleProviderRelay(t, "http://"+addr, "{retries: 0, keep_warm: 0s, outage_wait: 5s}")

	go func() { // the network comes back after ~400ms
		time.Sleep(400 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("listen: %v", err)
			return
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "event: message_stop\n\n")
		})}
		done := make(chan error, 1)
		go func() { done <- srv.Serve(l) }()
		t.Cleanup(func() {
			srv.Close()
			if err := <-done; err != nil && err != http.ErrServerClosed {
				t.Logf("upstream serve: %v", err)
			}
		})
	}()

	start := time.Now()
	status, body := postBody(t, relay.URL+"/v1/messages", []byte(`{"model":"m"}`))
	if status != 200 || body != "event: message_stop\n\n" {
		t.Fatalf("held request should succeed once the upstream is back: %d %q", status, body)
	}
	if took := time.Since(start); took < 300*time.Millisecond || took > 3*time.Second {
		t.Errorf("should have waited for the upstream (~400ms), took %v", took)
	}
}

func TestOutageGivesUpAfterOutageWait(t *testing.T) {
	fastOutage(t)
	relay := singleProviderRelay(t, "http://"+freeAddr(t), "{retries: 0, keep_warm: 0s, outage_wait: 400ms}")
	start := time.Now()
	resp, err := http.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	took := time.Since(start)
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("want retryable 503 after the wait, got %d", resp.StatusCode)
	}
	if took < 350*time.Millisecond || took > 3*time.Second {
		t.Errorf("should give up after ~outage_wait (400ms), took %v", took)
	}
}

func TestOutageHoldEndsWhenClientLeaves(t *testing.T) {
	fastOutage(t)
	var attempts atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		conn.Close()
	}))
	defer up.Close()
	relay := singleProviderRelay(t, up.URL, "{retries: 0, keep_warm: 0s, outage_wait: 1m}")
	client := &http.Client{Timeout: 300 * time.Millisecond}
	if _, err := client.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`)); err == nil {
		t.Fatal("expected the client timeout while the relay holds")
	}
	time.Sleep(300 * time.Millisecond)
	n := attempts.Load()
	time.Sleep(300 * time.Millisecond)
	if attempts.Load() != n {
		t.Errorf("relay kept retrying after the client left: %d -> %d", n, attempts.Load())
	}
}

func TestHTTPErrorsAreNotHeld(t *testing.T) {
	fastOutage(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable) // the gateway answered: not an outage
		fmt.Fprint(w, "busy")
	}))
	defer up.Close()
	relay := singleProviderRelay(t, up.URL, "{retries: 0, keep_warm: 0s, outage_wait: 1m}")
	start := time.Now()
	resp, err := http.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 503 || string(b) != "busy" || time.Since(start) > 2*time.Second {
		t.Fatalf("an upstream answer must be relayed at once: %d %q after %v", resp.StatusCode, b, time.Since(start))
	}
}

func TestOutageStatus(t *testing.T) {
	fastOutage(t)
	addr := freeAddr(t)
	t.Setenv("RELAY_TOKEN", "tok")
	cfgPath := writeConfig(t, fmt.Sprintf(
		"providers: [{name: a, base_url: http://%s, auth: {env: RELAY_TOKEN}, discover: false, models: [m]}]\nnetwork: {retries: 0, keep_warm: 0s, outage_wait: 0s}\n", addr))
	srv, err := NewServer(cfgPath, slogDiscard())
	if err != nil {
		t.Fatal(err)
	}
	relay := httptest.NewServer(srv)
	defer relay.Close()

	postBody(t, relay.URL+"/v1/messages", []byte(`{"model":"m"}`))
	if srv.outageSince.Load() == 0 || srv.lastUpstreamOK.Load() != 0 {
		t.Fatal("an all-unreachable pass should open an outage")
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	up := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
	done := make(chan error, 1)
	go func() { done <- up.Serve(l) }()
	defer func() {
		up.Close()
		if err := <-done; err != nil && err != http.ErrServerClosed {
			t.Errorf("upstream serve: %v", err)
		}
	}()
	postBody(t, relay.URL+"/v1/messages", []byte(`{"model":"m"}`))
	if srv.outageSince.Load() != 0 || srv.lastUpstreamOK.Load() == 0 {
		t.Fatal("the first upstream answer should close the outage")
	}
}

func slogDiscard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
