package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeepWarmPingsOnlyIdleProviders(t *testing.T) {
	orig := warmCheckInterval
	warmCheckInterval = 10 * time.Millisecond
	t.Cleanup(func() { warmCheckInterval = orig })

	var warms, conns atomic.Int32
	up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			warms.Add(1)
		}
		fmt.Fprint(w, `{"data":[]}`)
	}))
	up.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	up.Start()
	defer up.Close()

	t.Setenv("RELAY_TOKEN", "tok")
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(`
providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [m]}]
network: {keep_warm: 100ms, connections: 1}
`, up.URL))
	srv, err := NewServer(cfgPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.keepWarm(ctx)

	time.Sleep(350 * time.Millisecond) // idle: first warm at once, then every ~100ms
	if n := warms.Load(); n < 2 || n > 5 {
		t.Errorf("idle provider should be warmed ~3 times, got %d", n)
	}
	if c := conns.Load(); c != 1 {
		t.Errorf("warming must reuse one connection, opened %d", c)
	}

	before := warms.Load()
	for range 8 { // busy: real traffic every 40ms keeps it warm already
		srv.activity.touch(laneKey("a", 0))
		time.Sleep(40 * time.Millisecond)
	}
	if extra := warms.Load() - before; extra > 0 {
		t.Errorf("busy provider should not be warmed, got %d extra", extra)
	}
}

func TestKeepWarmHealsDiscovery(t *testing.T) {
	orig := warmCheckInterval
	warmCheckInterval = 10 * time.Millisecond
	t.Cleanup(func() { warmCheckInterval = orig })

	var up atomic.Bool // gateway down at startup, back a moment later
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		fmt.Fprint(w, `{"data":[{"id":"late-model"}]}`)
	}))
	defer gw.Close()
	t.Setenv("RELAY_TOKEN", "tok")
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(
		"providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}}]\nnetwork: {keep_warm: 50ms, connections: 1}\n", gw.URL))
	srv, err := NewServer(cfgPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // startup discovery fails
	if _, err := srv.st.Load().router.Candidates("late-model"); err == nil {
		t.Fatal("model should be unknown while the gateway is down")
	}
	up.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.keepWarm(ctx)
	time.Sleep(300 * time.Millisecond)
	if ps, err := srv.st.Load().router.Candidates("late-model"); err != nil || ps[0] != "a" {
		t.Fatalf("keep-warm should have discovered the model: %v %v", ps, err)
	}
}
