package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fastRetries(t *testing.T) {
	orig := retryBackoff
	retryBackoff = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(func() { retryBackoff = orig })
}

func singleProviderRelay(t *testing.T, baseURL, network string) *httptest.Server {
	t.Helper()
	t.Setenv("RELAY_TOKEN", "tok")
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(
		"providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [m]}]\nnetwork: %s\n",
		baseURL, network))
	srv, err := NewServer(cfgPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	relay := httptest.NewServer(srv)
	t.Cleanup(relay.Close)
	return relay
}

func TestRetriesSameProviderAfterDroppedConnection(t *testing.T) {
	fastRetries(t)
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			// connection dies before any response, like a reset on a lossy path
			conn, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			conn.Close()
			return
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer up.Close()

	relay := singleProviderRelay(t, up.URL, "{retries: 1}")
	status, body := postBody(t, relay.URL+"/v1/messages", []byte(`{"model":"m"}`))
	if status != 200 || !strings.Contains(body, `"ok":true`) || calls.Load() != 2 {
		t.Fatalf("dropped connection should be retried once on the same provider: %d %s calls=%d",
			status, body, calls.Load())
	}
}

func TestUnreachableReturnsRetryable503(t *testing.T) {
	fastRetries(t)
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	relay := singleProviderRelay(t, deadURL, "{retries: 2, outage_wait: 0s}")
	resp, err := http.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "3" ||
		resp.Header.Get("X-Should-Retry") != "true" || !strings.Contains(string(body), "api_error") {
		t.Fatalf("want retryable 503, got %d headers=%v body=%s", resp.StatusCode, resp.Header, body)
	}
}

func TestOriginTimeoutIsRetryableWithoutLongRetryAfter(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(statusOriginTimeout)
		fmt.Fprint(w, `{"status":524,"error_name":"origin_response_timeout"}`)
	}))
	defer up.Close()

	relay := singleProviderRelay(t, up.URL, "{retries: 0}")
	resp, err := http.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != statusOriginTimeout || resp.Header.Get("Retry-After") != "" ||
		resp.Header.Get("X-Should-Retry") != "true" || !strings.Contains(string(body), "origin_response_timeout") {
		t.Fatalf("want 524 without Retry-After and with X-Should-Retry, got %d headers=%v body=%s",
			resp.StatusCode, resp.Header, body)
	}
}

func TestNoRetryWhenDisabled(t *testing.T) {
	fastRetries(t)
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		conn.Close()
	}))
	defer up.Close()

	relay := singleProviderRelay(t, up.URL, "{retries: 0, outage_wait: 0s}")
	if status, _ := postBody(t, relay.URL+"/v1/messages", []byte(`{"model":"m"}`)); status != 503 {
		t.Errorf("status %d", status)
	}
	if calls.Load() != 1 {
		t.Errorf("retries: 0 must not retry, upstream saw %d", calls.Load())
	}
}

// dropAfterHeaders sends 200 headers, then kills the connection before any
// body byte, on the first call only.
func dropAfterHeaders(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			rc := http.NewResponseController(w)
			if err := rc.Flush(); err != nil {
				t.Errorf("flush: %v", err)
			}
			conn, _, err := rc.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			conn.Close()
			return
		}
		fmt.Fprint(w, "event: message_stop\n\n")
	}))
	t.Cleanup(up.Close)
	return up, &calls
}

func TestLossBeforeFirstByteIsRetriedInvisibly(t *testing.T) {
	fastRetries(t)
	up, calls := dropAfterHeaders(t)
	relay := singleProviderRelay(t, up.URL, "{connections: 2, retries: 1, keep_warm: 0s}")
	resp, err := http.Post(relay.URL+"/v1/messages", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("client must get a complete body, got read error %v", err)
	}
	if resp.StatusCode != 200 || string(body) != "event: message_stop\n\n" || calls.Load() != 2 {
		t.Fatalf("want full retried stream, got %d %q calls=%d", resp.StatusCode, body, calls.Load())
	}
	if resp.Header.Get("X-Relay-Lane") != "1" {
		t.Errorf("retry should use the other lane, got %s", resp.Header.Get("X-Relay-Lane"))
	}
}

func TestEmptyBodyIsNotAnError(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()
	relay := singleProviderRelay(t, up.URL, "{keep_warm: 0s}")
	if status, body := postBody(t, relay.URL+"/v1/messages", []byte(`{"model":"m"}`)); status != 200 || body != "" {
		t.Fatalf("empty 200 must pass through: %d %q", status, body)
	}
}
