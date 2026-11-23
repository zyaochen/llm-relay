package main

import (
	"errors"
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

func TestClassify(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   failKind
	}{
		{200, "", failNone},
		{400, `{"error":"messages: bad field"}`, failNone},
		{400, `Your credit balance is too low`, failQuota},
		{402, ``, failQuota},
		{403, `error code: 1010`, failTransient},
		{403, `{"error":"insufficient_quota"}`, failQuota},
		{429, `{"error":"rate limit exceeded"}`, failTransient},
		{429, `{"error":"You exceeded your current quota"}`, failQuota},
		{429, `今日额度已用完`, failQuota},
		{429, `429 最多同时 4 个请求，请稍后重试。`, failConcurrency},
		{429, `{"error":"This key allows 4 concurrent requests"}`, failConcurrency},
		{401, ``, failTransient},
		{404, `model not found`, failSkip},
		{408, ``, failTransient},
		{413, ``, failNone},
		{500, ``, failTransient},
		{529, `overloaded`, failTransient},
	}
	for _, c := range cases {
		if got := classify(c.status, []byte(c.body)); got != c.want {
			t.Errorf("classify(%d, %q) = %s, want %s", c.status, c.body, got, c.want)
		}
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for v, want := range map[string]time.Duration{
		"":        0,
		"120":     2 * time.Minute,
		"garbage": 0,
		now.Add(time.Hour).Format(http.TimeFormat):  time.Hour,
		now.Add(-time.Hour).Format(http.TimeFormat): 0,
	} {
		h := http.Header{}
		if v != "" {
			h.Set("Retry-After", v)
		}
		if got := retryAfter(h, now); got != want {
			t.Errorf("retryAfter(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestHealthOrderResetActive(t *testing.T) {
	now := time.Unix(1000, 0)
	h := NewHealth()
	h.now = func() time.Time { return now }
	h.Mark("a", "m", 10*time.Minute, "quota")
	h.Mark("b", "m", time.Minute, "transient")
	h.Mark("a", "other", time.Minute, "transient")

	if got := strings.Join(h.Order("m", []string{"a", "b", "c"}), ","); got != "c,b,a" {
		t.Errorf("healthy first, then soonest-expiring cooldown: got %s", got)
	}
	if got := strings.Join(h.Order("x", []string{"a", "b"}), ","); got != "a,b" {
		t.Errorf("cooldowns are per model: got %s", got)
	}
	if n := len(h.Active()); n != 3 {
		t.Errorf("active = %d, want 3", n)
	}
	now = now.Add(2 * time.Minute)
	if got := strings.Join(h.Order("m", []string{"a", "b"}), ","); got != "b,a" {
		t.Errorf("expired cooldown should be healthy again: got %s", got)
	}
	if n := len(h.Active()); n != 1 {
		t.Errorf("active after expiry = %d, want 1", n)
	}
	if n := h.Reset("a"); n != 1 {
		t.Errorf("reset cleared %d, want 1", n)
	}
	h.OK("b", "m")
	if n := len(h.Active()); n != 0 {
		t.Errorf("active after reset = %d", n)
	}
}

// failoverFixture: providers a and b both serve model "m"; a's behaviour is
// switchable per test, b always answers.
type failoverFixture struct {
	relay  *httptest.Server
	srv    *Server
	aMode  atomic.Value // "ok", "quota", "rate", "bad", "stream-error"
	aCalls atomic.Int32
	bCalls atomic.Int32
}

func newFailoverFixture(t *testing.T, aURL string) *failoverFixture {
	t.Helper()
	f := &failoverFixture{}
	f.aMode.Store("ok")
	upA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			fmt.Fprint(w, `{"data":[{"id":"m"}]}`)
			return
		}
		f.aCalls.Add(1)
		switch f.aMode.Load().(string) {
		case "quota":
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"quota exceeded for this key"}}`)
		case "rate":
			w.Header().Set("Retry-After", "600")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"rate limit"}}`)
		case "bad":
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":{"message":"messages.0: invalid"}}`)
		case "stream-error":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}\n\n")
		default:
			fmt.Fprint(w, `{"from":"a"}`)
		}
	}))
	upB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			fmt.Fprint(w, `{"data":[{"id":"m"}]}`)
			return
		}
		f.bCalls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, `{"from":"b","echo":%s}`, body)
	}))
	t.Cleanup(upA.Close)
	t.Cleanup(upB.Close)
	if aURL == "" {
		aURL = upA.URL
	}

	t.Setenv("RELAY_TOKEN", "tok")
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(`
providers:
  - {name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [m]}
  - {name: b, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [m]}
failover: {cooldown: 1m, quota_cooldown: 30m}
`, aURL, upB.URL))
	srv, err := NewServer(cfgPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	f.srv = srv
	f.relay = httptest.NewServer(srv)
	t.Cleanup(f.relay.Close)
	return f
}

func (f *failoverFixture) post(t *testing.T) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(f.relay.URL+"/v1/messages", "application/json",
		strings.NewReader(`{"model":"m[1m]","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(body)
}

func (f *failoverFixture) cooldown(provider string) (Cooldown, bool) {
	for _, c := range f.srv.health.Active() {
		if c.Provider == provider && c.Model == "m" {
			return c, true
		}
	}
	return Cooldown{}, false
}

func TestFailoverOnQuotaIsTransparentAndSticky(t *testing.T) {
	f := newFailoverFixture(t, "")
	f.aMode.Store("quota")

	resp, body := f.post(t)
	if resp.StatusCode != 200 || !strings.Contains(body, `"from":"b"`) {
		t.Fatalf("expected b to answer, got %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"model":"m[1m]"`) {
		t.Errorf("request body must reach b unchanged: %s", body)
	}
	if got := resp.Header.Get("X-Relay-Failover"); got != "a(429 quota) > b" {
		t.Errorf("X-Relay-Failover = %q", got)
	}
	c, ok := f.cooldown("a")
	if !ok || time.Until(c.Until) < 29*time.Minute {
		t.Fatalf("a should cool for ~30m after quota, got %+v ok=%v", c, ok)
	}

	f.post(t) // sticky: goes straight to b
	if a := f.aCalls.Load(); a != 1 {
		t.Errorf("cooling provider was called again: a=%d", a)
	}

	f.aMode.Store("ok")
	if n := f.srv.health.Reset("a"); n != 1 {
		t.Fatalf("reset cleared %d", n)
	}
	if _, body := f.post(t); !strings.Contains(body, `"from":"a"`) {
		t.Errorf("after reset traffic should return to a, got %s", body)
	}
}

func TestRetryAfterExtendsCooldown(t *testing.T) {
	f := newFailoverFixture(t, "")
	f.aMode.Store("rate")
	f.post(t)
	c, ok := f.cooldown("a")
	if !ok || time.Until(c.Until) < 9*time.Minute {
		t.Fatalf("Retry-After 600 should beat the 1m cooldown, got %+v", c)
	}
}

func TestNoFailoverOnRequestError(t *testing.T) {
	f := newFailoverFixture(t, "")
	f.aMode.Store("bad")
	resp, body := f.post(t)
	if resp.StatusCode != 400 || !strings.Contains(body, "invalid") || f.bCalls.Load() != 0 {
		t.Fatalf("a 400 caused by the request must be returned as is: %d %s b=%d", resp.StatusCode, body, f.bCalls.Load())
	}
	if _, ok := f.cooldown("a"); ok {
		t.Error("request errors must not cool the provider")
	}
}

func TestFailoverOnUnreachable(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	f := newFailoverFixture(t, deadURL)
	resp, body := f.post(t)
	if resp.StatusCode != 200 || !strings.Contains(body, `"from":"b"`) {
		t.Fatalf("unreachable a should fail over to b, got %d %s", resp.StatusCode, body)
	}
	if !strings.HasPrefix(resp.Header.Get("X-Relay-Failover"), "a(network)") {
		t.Errorf("X-Relay-Failover = %q", resp.Header.Get("X-Relay-Failover"))
	}
}

func TestStreamErrorCoolsProviderForNextRequest(t *testing.T) {
	f := newFailoverFixture(t, "")
	f.aMode.Store("stream-error")
	if _, body := f.post(t); strings.Contains(body, "overloaded_error") {
		t.Fatalf("a mid-stream error event must not reach Claude Code (it would be final): %s", body)
	}
	if _, ok := f.cooldown("a"); !ok {
		t.Fatal("in-stream overload should cool a")
	}
	if _, body := f.post(t); !strings.Contains(body, `"from":"b"`) {
		t.Errorf("client retry should land on b, got %s", body)
	}
}

func TestAllCoolingStillTries(t *testing.T) {
	f := newFailoverFixture(t, "")
	f.srv.health.Mark("a", "m", time.Hour, "quota")
	f.srv.health.Mark("b", "m", time.Minute, "transient")
	if _, body := f.post(t); !strings.Contains(body, `"from":"b"`) {
		t.Errorf("soonest-expiring provider should be tried, got %s", body)
	}
}

func TestFailKindString(t *testing.T) {
	if failQuota.String() != "quota" || failSkip.String() != "not_found" {
		t.Fatal(errors.New("unexpected failKind names"))
	}
}
