package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func mkLane(id int, inflight int32, sinceByte time.Duration, now time.Time) *lane {
	l := &lane{id: id}
	l.inflight.Store(inflight)
	l.progress.Store(now.Add(-sinceByte).UnixNano())
	return l
}

func TestPickLane(t *testing.T) {
	orig := stallAfter
	stallAfter = 5 * time.Second
	t.Cleanup(func() { stallAfter = orig })
	now := time.Now()

	cases := []struct {
		name  string
		lanes []*lane
		avoid int
		want  int
	}{
		{"idle: lowest id keeps traffic on one connection",
			[]*lane{mkLane(0, 0, time.Hour, now), mkLane(1, 0, time.Hour, now)}, -1, 0},
		{"fewest in flight",
			[]*lane{mkLane(0, 2, 0, now), mkLane(1, 1, 0, now)}, -1, 1},
		{"stalled lane avoided even with fewer requests",
			[]*lane{mkLane(0, 1, 10*time.Second, now), mkLane(1, 3, time.Second, now)}, -1, 1},
		{"all stalled: fewest in flight",
			[]*lane{mkLane(0, 2, time.Minute, now), mkLane(1, 1, time.Minute, now)}, -1, 1},
		{"avoid the lane that just failed",
			[]*lane{mkLane(0, 0, 0, now), mkLane(1, 5, 0, now)}, 0, 1},
		{"avoid ignored with a single lane",
			[]*lane{mkLane(0, 0, 0, now)}, 0, 0},
	}
	for _, c := range cases {
		var avoid *lane
		if c.avoid >= 0 {
			avoid = c.lanes[c.avoid]
		}
		if got := pickLane(c.lanes, avoid, now); got.id != c.want {
			t.Errorf("%s: picked lane %d, want %d", c.name, got.id, c.want)
		}
	}
}

func TestLanePoolResize(t *testing.T) {
	made := 0
	lp := newLanePool(func() *http.Client { made++; return &http.Client{} })
	if got := len(lp.get("a", 3)); got != 3 || made != 3 {
		t.Fatalf("lanes=%d made=%d", got, made)
	}
	if got := lp.get("a", 1); len(got) != 1 || got[0].id != 0 {
		t.Fatalf("shrink should keep lane 0: %v", got)
	}
	if got := lp.get("a", 2); len(got) != 2 || made != 4 {
		t.Fatalf("regrow should create one new lane: lanes=%d made=%d", len(got), made)
	}
}

// laneUpstream serves ?mode=hang (no bytes until released), ?mode=trickle
// (a byte every 20ms for 600ms) and anything else immediately, recording
// which client connection each mode arrived on.
type laneUpstream struct {
	*httptest.Server
	release chan struct{}
	mu      sync.Mutex
	remote  map[string]string
}

func newLaneUpstream(t *testing.T) *laneUpstream {
	u := &laneUpstream{release: make(chan struct{}), remote: map[string]string{}}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := r.URL.Query().Get("mode")
		u.mu.Lock()
		u.remote[mode] = r.RemoteAddr
		u.mu.Unlock()
		switch mode {
		case "hang":
			<-u.release
		case "trickle":
			for range 30 {
				fmt.Fprint(w, ".")
				w.(http.Flusher).Flush()
				time.Sleep(20 * time.Millisecond)
			}
		}
		fmt.Fprintf(w, "done-%s", mode)
	}))
	t.Cleanup(func() {
		close(u.release)
		u.Close()
	})
	return u
}

func TestStalledLaneIsAvoided(t *testing.T) {
	orig := stallAfter
	stallAfter = 80 * time.Millisecond
	t.Cleanup(func() { stallAfter = orig })
	up := newLaneUpstream(t)
	relay := singleProviderRelay(t, up.URL, "{connections: 2, keep_warm: 0s}")

	post := func(mode string) *http.Response {
		resp, err := http.Post(relay.URL+"/v1/messages?mode="+mode, "application/json", strings.NewReader(`{"model":"m"}`))
		if err != nil {
			t.Errorf("%s: %v", mode, err)
			return nil
		}
		return resp
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // occupies lane 0 and stops sending bytes
		defer wg.Done()
		if resp := post("hang"); resp != nil {
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				t.Errorf("drain: %v", err)
			}
			resp.Body.Close()
		}
	}()
	time.Sleep(30 * time.Millisecond)
	go func() { // occupies lane 1 but keeps making progress
		defer wg.Done()
		if resp := post("trickle"); resp != nil {
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				t.Errorf("drain: %v", err)
			}
			resp.Body.Close()
		}
	}()
	time.Sleep(200 * time.Millisecond) // lane 0 now stalled, lane 1 trickling

	// Both lanes have one request in flight; only the stall tells them apart.
	start := time.Now()
	resp := post("fast")
	if resp == nil {
		t.FailNow()
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if lane := resp.Header.Get("X-Relay-Lane"); lane != "1" || string(body) != "done-fast" {
		t.Errorf("new request should avoid stalled lane 0: lane=%s body=%s", lane, body)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("request on the healthy lane took %v", took)
	}
	up.mu.Lock()
	if up.remote["hang"] == up.remote["trickle"] {
		t.Errorf("lanes must use separate upstream connections, both on %s", up.remote["hang"])
	}
	up.mu.Unlock()
	up.release <- struct{}{}
	wg.Wait()
}

func TestRetryMovesToAnotherLane(t *testing.T) {
	fastRetries(t)
	first := true
	var mu sync.Mutex
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		drop := first
		first = false
		mu.Unlock()
		if drop {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			conn.Close()
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer up.Close()
	relay := singleProviderRelay(t, up.URL, "{connections: 2, retries: 1, keep_warm: 0s}")
	resp, err := http.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Relay-Lane") != "1" {
		t.Errorf("retry should succeed on lane 1, got %d lane=%s", resp.StatusCode, resp.Header.Get("X-Relay-Lane"))
	}
}
