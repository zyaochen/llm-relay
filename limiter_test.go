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
	if r, ok := newLimiter(0, 0).tryAcquire(); !ok || r == nil {
		t.Fatal("unlimited limiter must always grant")
	}
	l := newLimiter(1, 0)
	release, _, err := l.acquire(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.tryAcquire(); ok {
		t.Fatal("second slot granted beyond the limit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := l.acquire(ctx, false); err == nil {
		t.Fatal("acquire must give up with its context")
	}
	release()
	release() // idempotent
	if inUse, waiting, _ := l.counts(); inUse != 0 || waiting != 0 {
		t.Fatalf("in use = %d, waiting = %d after release", inUse, waiting)
	}
	var ls limiters
	p := ProviderConfig{Name: "p", MaxConcurrent: 2}
	a := ls.get(p)
	p3, pr := p, p
	p3.MaxConcurrent = 3
	pr.ReservedSlots = 1
	if ls.get(p) != a || ls.get(p3) == a || ls.get(pr) == ls.get(p3) {
		t.Error("limiter must be reused for the same limits and replaced when they change")
	}
}

// waitGranted acquires in the background and reports the order slots arrive.
func waitGranted(t *testing.T, l *limiter, priority bool, name string, order chan<- string) {
	t.Helper()
	go func() {
		release, _, err := l.acquire(context.Background(), priority)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			return
		}
		order <- name
		release()
	}()
}

func waitQueued(t *testing.T, l *limiter, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, w, _ := l.counts(); w == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d waiters", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLimiterServesPriorityFirst(t *testing.T) {
	l := newLimiter(1, 0)
	hold, _, err := l.acquire(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	order := make(chan string, 3)
	waitGranted(t, l, false, "normal-1", order)
	waitQueued(t, l, 1)
	waitGranted(t, l, false, "normal-2", order)
	waitQueued(t, l, 2)
	waitGranted(t, l, true, "priority", order)
	waitQueued(t, l, 3)
	hold()
	got := []string{<-order, <-order, <-order}
	if want := "priority normal-1 normal-2"; strings.Join(got, " ") != want {
		t.Fatalf("grant order = %v, want %s", got, want)
	}
}

func TestLimiterReservedSlotOnlyForPriority(t *testing.T) {
	l := newLimiter(2, 1)
	a, _, err := l.acquire(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := l.acquire(ctx, false); err == nil {
		t.Fatal("a normal request took the reserved slot")
	}
	if _, ok := l.tryAcquire(); ok {
		t.Fatal("keep-warm took the reserved slot")
	}
	b, waited, err := l.acquire(context.Background(), true)
	if err != nil || waited != 0 {
		t.Fatalf("priority request should get the reserved slot at once: waited=%v err=%v", waited, err)
	}
	a()
	b()
	if inUse, waiting, _ := l.counts(); inUse != 0 || waiting != 0 {
		t.Fatalf("leaked: in use %d, waiting %d", inUse, waiting)
	}
}

func TestLimiterCancelledWaiterPassesSlotOn(t *testing.T) {
	l := newLimiter(1, 0)
	hold, _, err := l.acquire(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	gone := make(chan error, 1)
	go func() {
		_, _, err := l.acquire(ctx, true)
		gone <- err
	}()
	waitQueued(t, l, 1)
	order := make(chan string, 1)
	waitGranted(t, l, false, "next", order)
	waitQueued(t, l, 2)
	cancel()
	if err := <-gone; err == nil {
		t.Fatal("cancelled waiter must fail")
	}
	hold()
	select {
	case <-order:
	case <-time.After(2 * time.Second):
		t.Fatal("slot was not passed on after the priority waiter left")
	}
}

func TestPriorityModelSkipsTheQueueEndToEnd(t *testing.T) {
	var mu sync.Mutex
	var served []string
	gate := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("upstream read: %v", err)
		}
		mu.Lock()
		served = append(served, string(b))
		mu.Unlock()
		if strings.Contains(string(b), "hold") {
			<-gate
		}
		fmt.Fprint(w, "ok")
	}))
	defer up.Close()
	t.Setenv("RELAY_TOKEN", "tok")
	srv, err := NewServer(writeConfig(t, fmt.Sprintf(
		"providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [big, small],"+
			" max_concurrent: 1, priority_models: [\"big*\"]}]\n"+
			"network: {retries: 0, keep_warm: 0s, outage_wait: 0s}\n", up.URL)), slogDiscard())
	if err != nil {
		t.Fatal(err)
	}
	relay := httptest.NewServer(srv)
	defer relay.Close()

	var wg sync.WaitGroup
	post := func(body string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if status, _ := postBody(t, relay.URL+"/v1/messages", []byte(body)); status != 200 {
				t.Errorf("%s: %d", body, status)
			}
		}()
	}
	post(`{"model":"small","x":"hold"}`)
	lim := srv.limits.get(srv.st.Load().cfg.Providers[0])
	waitFor := func(n int) {
		deadline := time.Now().Add(2 * time.Second)
		for {
			if inUse, w, _ := lim.counts(); inUse+w == n {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("want %d requests at the limiter", n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitFor(1)
	post(`{"model":"small","x":"2"}`)
	waitFor(2)
	post(`{"model":"big[1m]","x":"3"}`)
	waitFor(3)
	close(gate)
	wg.Wait()
	if len(served) != 3 || !strings.Contains(served[1], "big") {
		t.Fatalf("priority model should be served right after the holder: %q", served)
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

func TestPriorityConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		p  ProviderConfig
		ok bool
	}{
		{ProviderConfig{MaxConcurrent: 4, PriorityModels: []string{"claude-opus-*"}, ReservedSlots: 1}, true},
		{ProviderConfig{MaxConcurrent: 4, PriorityModels: []string{"claude-opus-*"}}, true},
		{ProviderConfig{MaxConcurrent: 4, ReservedSlots: 1}, false},                                // nothing to reserve for
		{ProviderConfig{MaxConcurrent: 2, PriorityModels: []string{"x"}, ReservedSlots: 2}, false}, // nothing left for others
		{ProviderConfig{PriorityModels: []string{"x"}}, false},                                     // no limit, no queue
		{ProviderConfig{MaxConcurrent: 2, PriorityModels: []string{"["}}, false},                   // bad glob
		{ProviderConfig{MaxConcurrent: 2, PriorityModels: []string{"x"}, ReservedSlots: -1}, false},
	} {
		if err := tc.p.normalizeLimits(); (err == nil) != tc.ok {
			t.Errorf("%+v: err=%v, want ok=%v", tc.p, err, tc.ok)
		}
	}
	if !isPriority(ProviderConfig{PriorityModels: []string{"claude-opus-*"}}, "claude-opus-5-5[1m]") ||
		isPriority(ProviderConfig{PriorityModels: []string{"claude-opus-*"}}, "claude-haiku-4-5") {
		t.Error("isPriority must match normalized model ids against the globs")
	}
}
