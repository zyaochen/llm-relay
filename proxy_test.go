package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	relay      *httptest.Server
	srv        *Server
	configPath string
	release    chan struct{} // lets upstream A send its second SSE event
	aCalls     atomic.Int32
	aSawAuth   atomic.Value
}

const clientKey = "local-test-key"

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{release: make(chan struct{})}

	upA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/messages" {
			f.aCalls.Add(1)
			f.aSawAuth.Store(r.Header.Get("Authorization") + "|" + r.Header.Get("X-Api-Key"))
		}
		switch r.URL.Path {
		case "/v1/models":
			if r.Header.Get("Accept-Encoding") != "identity" {
				http.Error(w, "models must be fetched uncompressed", http.StatusTeapot)
				return
			}
			fmt.Fprint(w, `{"data":[{"id":"claude-a","type":"model"},{"id":"shared"}]}`)
		case "/v1/messages":
			if r.Header.Get("Authorization") != "Bearer fresh-a" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "event: one\n\n")
			w.(http.Flusher).Flush()
			<-f.release
			fmt.Fprint(w, "event: two\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	upB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			fmt.Fprint(w, `{"data":[{"id":"kimi-b"},{"id":"shared"}]}`)
		default:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, `{"path":%q,"key":%q,"body":%s}`, r.URL.RequestURI(), r.Header.Get("X-Api-Key"), body)
		}
	}))
	t.Cleanup(upA.Close)
	t.Cleanup(upB.Close)

	dir := t.TempDir()
	keyFile := filepath.Join(dir, "client.key")
	if err := os.WriteFile(keyFile, []byte(clientKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_B_TOKEN", "tok-b")
	f.configPath = filepath.Join(dir, "config.yaml")
	writeFile(t, f.configPath, fmt.Sprintf(`
client_key_file: %s
providers:
  - {name: a, base_url: %s, auth: {cmd: [unused]}}
  - {name: b, base_url: %s, auth: {env: RELAY_B_TOKEN, header: x-api-key}, models: [static-b]}
routes:
  - {match: "claude-*", provider: a}
`, keyFile, upA.URL, upB.URL))

	stubTokens(t)
	srv, err := NewServer(f.configPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	f.srv = srv
	f.relay = httptest.NewServer(srv)
	t.Cleanup(f.relay.Close)
	return f
}

// stubTokens makes provider a hand out a stale token first, then fresh ones.
func stubTokens(t *testing.T) {
	var n atomic.Int32
	orig := commandRunner
	commandRunner = func(context.Context, []string) (string, error) {
		if n.Add(1) == 1 {
			return "stale-a", nil
		}
		return "fresh-a", nil
	}
	t.Cleanup(func() { commandRunner = orig })
}

func writeFile(t *testing.T, p, body string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) do(t *testing.T, method, path, key, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, f.relay.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestRejectsWrongClientKey(t *testing.T) {
	f := newFixture(t)
	for _, key := range []string{"", "nope"} {
		resp := f.do(t, "POST", "/v1/messages", key, `{"model":"claude-a"}`)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("key %q: status %d", key, resp.StatusCode)
		}
	}
	resp := f.do(t, "GET", "/relay/health", "", "")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("health should not need a key, got %d", resp.StatusCode)
	}
}

func TestStreamsIncrementallyAndRefreshesOn401(t *testing.T) {
	f := newFixture(t)
	resp := f.do(t, "POST", "/v1/messages?beta=true", clientKey, `{"model":"claude-a[1m]","stream":true}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	rd := bufio.NewReader(resp.Body)
	first, err := rd.ReadString('\n')
	if err != nil || first != "event: one\n" {
		t.Fatalf("first event not delivered before upstream finished: %q %v", first, err)
	}
	close(f.release)
	rest, _ := io.ReadAll(rd)
	if !strings.Contains(string(rest), "event: two") {
		t.Fatalf("second event missing: %q", rest)
	}
	if got := f.aCalls.Load(); got != 2 {
		t.Errorf("expected one 401 then one retry, upstream saw %d calls", got)
	}
	if got := f.aSawAuth.Load().(string); got != "Bearer fresh-a|" {
		t.Errorf("client key leaked or wrong upstream auth: %q", got)
	}
}

func TestRoutesByDiscoveryAndStaticModels(t *testing.T) {
	f := newFixture(t)
	f.srv.discover(f.srv.st.Load(), true)

	for _, model := range []string{"kimi-b", "static-b"} {
		resp := f.do(t, "POST", "/v1/responses", clientKey, fmt.Sprintf(`{"model":%q}`, model))
		var got struct{ Path, Key string }
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got.Path != "/v1/responses" || got.Key != "tok-b" {
			t.Errorf("%s: upstream saw path %q key %q", model, got.Path, got.Key)
		}
	}

	resp := f.do(t, "POST", "/v1/messages", clientKey, `{"model":"who-knows"}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("unknown model without default: status %d", resp.StatusCode)
	}
}

func TestMergedModels(t *testing.T) {
	f := newFixture(t)
	resp := f.do(t, "GET", "/v1/models?limit=1000", clientKey, "")
	defer resp.Body.Close()
	var list struct {
		Data []struct{ ID string } `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range list.Data {
		ids = append(ids, m.ID)
	}
	if want := "claude-a,shared,kimi-b,static-b"; strings.Join(ids, ",") != want {
		t.Errorf("merged ids = %v, want %s", ids, want)
	}
	if ps, _ := f.srv.st.Load().router.Candidates("shared"); strings.Join(ps, ",") != "a,b" {
		t.Errorf("shared model should try a then fail over to b, got %v", ps)
	}
}

func TestHotReloadKeepsOldConfigOnError(t *testing.T) {
	f := newFixture(t)
	before := f.srv.st.Load()

	writeFile(t, f.configPath, "providers: [this is not valid")
	bump(t, f.configPath)
	f.srv.reloadIfChanged()
	if f.srv.st.Load() != before {
		t.Fatal("broken config replaced the running state")
	}

	writeFile(t, f.configPath, `
providers:
  - {name: only, base_url: http://127.0.0.1:1, auth: {env: RELAY_B_TOKEN}, discover: false}
default_provider: only
`)
	bump(t, f.configPath)
	f.srv.reloadIfChanged()
	if got := f.srv.st.Load().cfg.providerNames(); len(got) != 1 || got[0] != "only" {
		t.Fatalf("valid config not applied: %v", got)
	}
}

// bump guarantees a distinct mtime even on coarse-grained filesystems.
func bump(t *testing.T, p string) {
	t.Helper()
	ts := time.Now().Add(time.Duration(time.Now().UnixNano()%1000+1) * time.Second)
	if err := os.Chtimes(p, ts, ts); err != nil {
		t.Fatal(err)
	}
}
