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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestShutdownDrainsInFlightStream(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := range 10 {
			fmt.Fprintf(w, "chunk%d\n", i)
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
	}))
	defer up.Close()

	addr := freeAddr(t)
	t.Setenv("RELAY_TOKEN", "tok")
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(
		"listen: %s\nproviders: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [m]}]\nnetwork: {keep_warm: 0s}\n",
		addr, up.URL))
	srv, err := NewServer(cfgPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- srv.Run(ctx) }()
	waitListening(t, addr)

	resp, err := http.Post("http://"+addr+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 7)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	stop() // what SIGTERM does, mid-stream

	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("in-flight stream was cut by the restart: %v", err)
	}
	if !strings.HasSuffix(string(rest), "chunk9\n") {
		t.Fatalf("stream incomplete: %q", rest)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the drain")
	}
	if _, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		t.Error("listener should be closed after shutdown")
	}
}

func waitListening(t *testing.T, addr string) {
	t.Helper()
	for range 100 {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("relay never listened on %s", addr)
}

func TestWaitIdle(t *testing.T) {
	var polls atomic.Int32
	busyFor := int32(2)
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := polls.Add(1)
		active := 0
		if n <= busyFor {
			active = 1
		}
		fmt.Fprintf(w, `{"ok":true,"active_requests":%d}`, active)
	}))
	defer health.Close()
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(
		"listen: %s\nproviders: [{name: a, base_url: https://a, auth: {env: HOME}}]\n",
		strings.TrimPrefix(health.URL, "http://")))

	idle, err := waitIdle(cfgPath, time.Second, time.Millisecond)
	if err != nil || !idle || polls.Load() != 3 {
		t.Fatalf("should report idle on the 3rd poll: idle=%v err=%v polls=%d", idle, err, polls.Load())
	}

	busyFor = 1 << 30
	polls.Store(0)
	idle, err = waitIdle(cfgPath, 30*time.Millisecond, 5*time.Millisecond)
	if err != nil || idle {
		t.Fatalf("always busy should give up after wait: idle=%v err=%v", idle, err)
	}
}
