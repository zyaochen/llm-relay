package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// concurrencyCapped behaves like a per-key capped gateway: above cap concurrent requests it
// answers 429 with a gateway-style message. It records the peak it saw.
type concurrencyCapped struct {
	*httptest.Server
	cur, peak, rejected, models atomic.Int32
}

const capMessage = `{"error":{"message":"429 最多同时 4 个请求，请稍后重试。 (This key allows 4 concurrent requests. Retry when one finishes.)"}}`

func newCapped(t *testing.T, limit int32, hold time.Duration) *concurrencyCapped {
	t.Helper()
	c := &concurrencyCapped{}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			c.models.Add(1)
			fmt.Fprint(w, `{"data":[{"id":"m"}]}`)
			return
		}
		n := c.cur.Add(1)
		defer c.cur.Add(-1)
		for {
			p := c.peak.Load()
			if n <= p || c.peak.CompareAndSwap(p, n) {
				break
			}
		}
		if n > limit {
			c.rejected.Add(1)
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, capMessage)
			return
		}
		fmt.Fprint(w, "event: message_start\n\n")
		w.(http.Flusher).Flush()
		if r.URL.Query().Get("hang") != "" {
			<-r.Context().Done()
			return
		}
		time.Sleep(hold)
		fmt.Fprint(w, "event: message_stop\n\n")
	}))
	t.Cleanup(c.Close)
	return c
}

func cappedRelay(t *testing.T, upURL string, maxConc int) *httptest.Server {
	t.Helper()
	t.Setenv("RELAY_TOKEN", "tok")
	cfg := fmt.Sprintf("providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [m], max_concurrent: %d}]\n"+
		"network: {retries: 0, keep_warm: 0s, outage_wait: 0s}\n", upURL, maxConc)
	srv, err := NewServer(writeConfig(t, cfg), slogDiscard())
	if err != nil {
		t.Fatal(err)
	}
	relay := httptest.NewServer(srv)
	t.Cleanup(relay.Close)
	return relay
}

func TestConcurrencyLimitQueuesInsteadOf429(t *testing.T) {
	up := newCapped(t, 2, 150*time.Millisecond)
	relay := cappedRelay(t, up.URL, 2)

	var wg sync.WaitGroup
	codes := make([]int, 6)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			defer resp.Body.Close()
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				t.Errorf("request %d body: %v", i, err)
			}
			codes[i] = resp.StatusCode
		}()
	}
	wg.Wait()
	for i, c := range codes {
		if c != 200 {
			t.Errorf("request %d got %d", i, c)
		}
	}
	if up.peak.Load() > 2 || up.rejected.Load() != 0 {
		t.Errorf("upstream saw peak %d concurrent and %d rejections; the relay must queue at 2",
			up.peak.Load(), up.rejected.Load())
	}
}

func TestSlotHeldForWholeStreamAndFreedOnDisconnect(t *testing.T) {
	up := newCapped(t, 1, 0)
	relay := cappedRelay(t, up.URL, 1)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, relay.URL+"/v1/messages?hang=1", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	// While the first stream is open, a second request must wait, not reach the upstream.
	second := make(chan int, 1)
	go func() {
		r2, err := http.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
		if err != nil {
			t.Errorf("second: %v", err)
			second <- 0
			return
		}
		defer r2.Body.Close()
		if _, err := io.Copy(io.Discard, r2.Body); err != nil {
			t.Errorf("second body: %v", err)
		}
		second <- r2.StatusCode
	}()
	select {
	case <-second:
		t.Fatal("second request finished while the first stream still held the only slot")
	case <-time.After(300 * time.Millisecond):
	}
	cancel() // client walks away mid-stream: its slot must come back
	resp.Body.Close()
	select {
	case code := <-second:
		if code != 200 {
			t.Errorf("second got %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("slot was not released when the first client disconnected")
	}
	if up.rejected.Load() != 0 {
		t.Errorf("upstream rejected %d", up.rejected.Load())
	}
}

func TestKeepWarmNeverTakesAClientSlot(t *testing.T) {
	orig := warmCheckInterval
	warmCheckInterval = 10 * time.Millisecond
	t.Cleanup(func() { warmCheckInterval = orig })
	up := newCapped(t, 1, 0)
	t.Setenv("RELAY_TOKEN", "tok")
	srv, err := NewServer(writeConfig(t, fmt.Sprintf(
		"providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [m], max_concurrent: 1}]\n"+
			"network: {keep_warm: 50ms, connections: 1, outage_wait: 0s}\n", up.URL)), slogDiscard())
	if err != nil {
		t.Fatal(err)
	}
	relay := httptest.NewServer(srv)
	defer relay.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, relay.URL+"/v1/messages?hang=1", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req) // occupies the only slot
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	warmCtx, stopWarm := context.WithCancel(context.Background())
	defer stopWarm()
	go srv.keepWarm(warmCtx)
	time.Sleep(300 * time.Millisecond)
	if n := up.models.Load(); n != 0 {
		t.Errorf("keep-warm sent %d requests while the only slot was busy", n)
	}
}

func TestLimiterBasics(t *testing.T) {
	if r, ok := newLimiter(0).tryAcquire(); !ok || r == nil {
		t.Fatal("unlimited limiter must always grant")
	}
	l := newLimiter(1)
	release, _, err := l.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.tryAcquire(); ok {
		t.Fatal("second slot granted beyond the limit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := l.acquire(ctx); err == nil {
		t.Fatal("acquire must give up with its context")
	}
	release()
	release() // idempotent
	if l.inUse() != 0 {
		t.Fatalf("in use = %d after release", l.inUse())
	}
	var ls limiters
	a := ls.get("p", 2)
	if ls.get("p", 2) != a || ls.get("p", 3) == a {
		t.Error("limiter must be reused for the same limit and replaced when it changes")
	}
}

func TestConcurrency429IsRetriedInPlace(t *testing.T) {
	origFor, origBackoff := concurrencyRetryFor, concurrencyBackoff
	concurrencyBackoff = func(int) time.Duration { return 20 * time.Millisecond }
	t.Cleanup(func() { concurrencyRetryFor, concurrencyBackoff = origFor, origBackoff })

	// Someone outside the relay holds the key's slots for the first 3 attempts.
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, capMessage)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer up.Close()

	relay := cappedRelay(t, up.URL, 4)
	status, body := postBody(t, relay.URL+"/v1/messages", []byte(`{"model":"m"}`))
	if status != 200 || body != "ok" || calls.Load() != 4 {
		t.Fatalf("concurrency 429 should be waited out on the same provider: %d %q calls=%d", status, body, calls.Load())
	}
}

func TestConcurrency429GivesUpAfterBound(t *testing.T) {
	origFor, origBackoff := concurrencyRetryFor, concurrencyBackoff
	concurrencyRetryFor, concurrencyBackoff = 100*time.Millisecond, func(int) time.Duration { return 20 * time.Millisecond }
	t.Cleanup(func() { concurrencyRetryFor, concurrencyBackoff = origFor, origBackoff })
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, capMessage)
	}))
	defer up.Close()
	relay := cappedRelay(t, up.URL, 4)
	start := time.Now()
	if status, _ := postBody(t, relay.URL+"/v1/messages", []byte(`{"model":"m"}`)); status != 429 {
		t.Fatalf("after the bound the 429 goes to the client, got %d", status)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("gave up too late: %v", time.Since(start))
	}
}
