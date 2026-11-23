package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

// Shapes taken from real responses: kimi signs with 32 hex chars, Claude with
// long base64.
var (
	kimiSig   = "d048899d695044578a1f0c2e3b4d5e6f"
	claudeSig = "Er" + strings.Repeat("AbCd0123+/", 30)
)

func historyBody(t *testing.T) []byte {
	t.Helper()
	body := map[string]any{
		"model":      "claude-x",
		"max_tokens": 1024,
		"thinking":   map[string]any{"type": "enabled", "budget_tokens": 512},
		"tools":      []any{map[string]any{"name": "get_time"}},
		"messages": []any{
			map[string]any{"role": "user", "content": "what time is it"},
			map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "thinking", "thinking": "kimi thoughts", "signature": kimiSig},
				map[string]any{"type": "tool_use", "id": "t1", "name": "get_time", "input": map[string]any{}},
			}},
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "12:34"},
			}},
			map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "thinking", "thinking": "claude thoughts", "signature": claudeSig},
				map[string]any{"type": "redacted_thinking", "data": "opaque"},
				map[string]any{"type": "text", "text": "It is 12:34."},
			}},
			map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "thinking", "thinking": "alone", "signature": kimiSig},
			}},
			map[string]any{"role": "user", "content": "thanks"},
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func decode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func blockTypes(t *testing.T, raw []byte) []string {
	t.Helper()
	var out []string
	for _, m := range decode(t, raw)["messages"].([]any) {
		msg := m.(map[string]any)
		blocks, ok := msg["content"].([]any)
		if !ok {
			out = append(out, msg["role"].(string)+":text")
			continue
		}
		for _, b := range blocks {
			out = append(out, msg["role"].(string)+":"+b.(map[string]any)["type"].(string))
		}
	}
	return out
}

func TestStripThinking(t *testing.T) {
	in := historyBody(t)

	foreign, n, err := stripThinking(in, true)
	if err != nil || n != 2 {
		t.Fatalf("foreignOnly removed %d, err %v; want the two kimi blocks", n, err)
	}
	want := "user:text,assistant:tool_use,user:tool_result,assistant:thinking,assistant:redacted_thinking," +
		"assistant:text,user:text"
	if got := strings.Join(blockTypes(t, foreign), ","); got != want {
		t.Errorf("foreignOnly result (thinking-only turn dropped):\n got %s\nwant %s", got, want)
	}

	all, n, err := stripThinking(in, false)
	if err != nil || n != 4 {
		t.Fatalf("all removed %d, err %v; want 4", n, err)
	}
	want = "user:text,assistant:tool_use,user:tool_result,assistant:text,user:text"
	if got := strings.Join(blockTypes(t, all), ","); got != want {
		t.Errorf("all result:\n got %s\nwant %s", got, want)
	}

	before, after := decode(t, in), decode(t, all)
	delete(before, "messages")
	delete(after, "messages")
	if !reflect.DeepEqual(before, after) {
		t.Errorf("fields other than messages changed:\n%v\n%v", before, after)
	}
}

func TestStripThinkingNoop(t *testing.T) {
	in := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	out, n, err := stripThinking(in, false)
	if err != nil || n != 0 || string(out) != string(in) {
		t.Fatalf("no thinking blocks must leave the body byte-identical: %s %d %v", out, n, err)
	}
	if _, _, err := stripThinking([]byte(`{"model":"m"}`), true); err == nil {
		t.Error("body without messages should error")
	}
}

// claudeLike rejects foreign signatures (or, when strict, any thinking) the
// way the real API does, and reports what it finally accepted.
func claudeLike(strict bool, calls *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if strings.Contains(string(body), kimiSig) || (strict && strings.Contains(string(body), `"type":"thinking"`)) {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0: Invalid `+"`signature`"+` in `+"`thinking`"+` block"}}`)
			return
		}
		fmt.Fprintf(w, `{"accepted":true,"claude_sig_kept":%t}`, strings.Contains(string(body), claudeSig))
	}
}

func sanitizeRelay(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	up := httptest.NewServer(h)
	t.Cleanup(up.Close)
	t.Setenv("RELAY_TOKEN", "tok")
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(
		"providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [claude-x]}]\n", up.URL))
	srv, err := NewServer(cfgPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	relay := httptest.NewServer(srv)
	t.Cleanup(relay.Close)
	return relay
}

func postBody(t *testing.T, url string, body []byte) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(out)
}

func TestSanitizeKeepsClaudeThinking(t *testing.T) {
	var calls atomic.Int32
	relay := sanitizeRelay(t, claudeLike(false, &calls))
	status, body := postBody(t, relay.URL+"/v1/messages", historyBody(t))
	if status != 200 || !strings.Contains(body, `"claude_sig_kept":true`) {
		t.Fatalf("pass 1 should drop only foreign thinking: %d %s", status, body)
	}
	if calls.Load() != 2 {
		t.Errorf("want reject + one sanitized retry, upstream saw %d", calls.Load())
	}
}

func TestSanitizeFallsBackToDroppingAll(t *testing.T) {
	var calls atomic.Int32
	relay := sanitizeRelay(t, claudeLike(true, &calls))
	status, body := postBody(t, relay.URL+"/v1/messages", historyBody(t))
	if status != 200 || calls.Load() != 3 {
		t.Fatalf("pass 2 should drop every thinking block: %d %s calls=%d", status, body, calls.Load())
	}
}

func TestSanitizeOnlyForMessages(t *testing.T) {
	var calls atomic.Int32
	relay := sanitizeRelay(t, claudeLike(false, &calls))
	status, _ := postBody(t, relay.URL+"/v1/chat/completions", historyBody(t))
	if status != 400 || calls.Load() != 1 {
		t.Fatalf("non-Messages endpoints must pass the 400 through untouched: %d calls=%d", status, calls.Load())
	}
}
