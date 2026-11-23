package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// syncBuffer lets the test read what concurrent loggers wrote.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) count(sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Count(s.b.String(), sub)
}

func TestDiscoveryBacksOffAndLogsOncePerStreak(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	var polls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		polls.Add(1)
		if failing.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		fmt.Fprint(w, `{"data":[{"id":"m1"},{"id":"m2"}]}`)
	}))
	defer up.Close()
	t.Setenv("RELAY_TOKEN", "tok")
	var logs syncBuffer
	srv, err := NewServer(writeConfig(t, fmt.Sprintf(
		"providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}}]\nnetwork: {keep_warm: 0s}\n", up.URL)),
		slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	st := srv.st.Load()
	time.Sleep(50 * time.Millisecond) // let the load-time discovery finish
	srv.discover(st, true)
	srv.discover(st, true)
	if n := logs.count("model discovery failed"); n != 1 {
		t.Errorf("a failure streak should warn once, got %d", n)
	}
	before := polls.Load()
	srv.discover(st, false) // backing off: not due yet
	if polls.Load() != before {
		t.Error("a provider in backoff must not be polled before its retry time")
	}
	failing.Store(false)
	srv.discover(st, true)
	srv.discover(st, true)
	if logs.count("model discovery recovered") != 1 || logs.count("models discovered") != 1 {
		t.Errorf("want one recovery and one change line, logs:\n%s", logs.b.String())
	}
	if got := st.router.Table(); len(got["m1"]) != 1 {
		t.Errorf("router not updated: %v", got)
	}
	if discoveryBackoff(1) != time.Minute || discoveryBackoff(3) != 4*time.Minute || discoveryBackoff(9) != discoveryInterval {
		t.Error("backoff must double from 1m and cap at the poll interval")
	}
}
