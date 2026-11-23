package main

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	usageStart = `{"type":"message_start","message":{"id":"m1","usage":{"input_tokens":10,` +
		`"cache_creation_input_tokens":200,"cache_read_input_tokens":3000,"output_tokens":1}}}`
	usageDelta = `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":42}}`
)

func TestTokenUsageAddJSON(t *testing.T) {
	var u tokenUsage
	u.addJSON([]byte(usageStart))
	u.addJSON([]byte(usageDelta))
	if !u.seen || u.in != 10 || u.cacheWrite != 200 || u.cacheRead != 3000 || u.out != 42 {
		t.Errorf("stream usage: %+v", u)
	}

	u = tokenUsage{} // a gateway that only reports input at the end
	u.addJSON([]byte(`{"type":"message_start","message":{"usage":{"input_tokens":0,"output_tokens":0}}}`))
	u.addJSON([]byte(`{"type":"message_delta","usage":{"input_tokens":7,"output_tokens":5}}`))
	if u.in != 7 || u.out != 5 {
		t.Errorf("late input: %+v", u)
	}

	u = tokenUsage{} // non-streaming reply
	u.addJSON([]byte(`{"id":"m","content":[{"type":"text","text":"{\"usage\":1}"}],"usage":{"input_tokens":3,"output_tokens":4}}`))
	if u.in != 3 || u.out != 4 {
		t.Errorf("body usage: %+v", u)
	}

	u = tokenUsage{}
	u.addJSON([]byte(`not json`))
	u.addJSON([]byte(`{"type":"ping"}`))
	if u.seen || u.attrs() != nil {
		t.Errorf("no usage must log nothing: %+v", u)
	}
}

func TestSSEDataJoinsDataLines(t *testing.T) {
	ev := []byte("event: x\r\ndata: {\"a\":\r\ndata:1}\r\n\r\n")
	if got := string(sseData(ev)); got != "{\"a\":\n1}" {
		t.Errorf("sseData = %q", got)
	}
}

// TestRelayLogsUsageForStats checks the whole path: usage read from a stream
// and from a non-streaming reply, logged, and summed by stats.
func TestRelayLogsUsageForStats(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") == "application/json" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"id":"m2","content":[],"usage":{"input_tokens":5,"cache_read_input_tokens":1000,"output_tokens":9}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: message_start\ndata: %s\n\n", usageStart)
		fmt.Fprint(w, ssev("content_block_start")+ssev("content_block_stop"))
		fmt.Fprintf(w, "event: message_delta\ndata: %s\n\n", usageDelta)
		fmt.Fprint(w, ssev("message_stop"))
	}))
	defer up.Close()

	t.Setenv("RELAY_TOKEN", "tok")
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, cfgPath, fmt.Sprintf(
		"providers: [{name: a, base_url: %s, auth: {env: RELAY_TOKEN}, discover: false, models: [m]}]\n", up.URL))
	var logs syncBuffer
	srv, err := NewServer(cfgPath, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	relay := httptest.NewServer(srv)
	defer relay.Close()

	for _, accept := range []string{"text/event-stream", "application/json"} {
		req, _ := http.NewRequest(http.MethodPost, relay.URL+"/v1/messages", strings.NewReader(`{"model":"m"}`))
		req.Header.Set("Accept", accept)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	// The JSON reply has a Content-Length, so the client can finish reading
	// before the relay has logged it.
	if !waitUntil(5*time.Second, func() bool { return logs.count("msg=proxied") == 2 }) {
		t.Fatalf("expected two proxied lines:\n%s", logs.String())
	}

	agg := newStatsAgg()
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		kv, err := parseLogLine(line)
		if err != nil {
			t.Fatal(err)
		}
		agg.add(kv)
	}
	m := agg.models["m"]
	if m == nil || m.replies != 2 || m.in != 15 || m.cacheRead != 4000 || m.cacheWrite != 200 || m.out != 51 {
		t.Fatalf("stats from the log: %+v\n%s", m, logs.String())
	}
	var out bytes.Buffer
	if err := agg.renderTokens(&out); err != nil {
		t.Fatal(err)
	}
	// 4000 cached of 4215 prompt tokens
	if !strings.Contains(out.String(), "4.0k") || !strings.Contains(out.String(), "95%") {
		t.Errorf("token table:\n%s", out.String())
	}
}

func TestTokenCount(t *testing.T) {
	for n, want := range map[int64]string{950: "950", 12345: "12.3k", 4_560_000: "4.56M"} {
		if got := tokenCount(n); got != want {
			t.Errorf("tokenCount(%d) = %s, want %s", n, got, want)
		}
	}
}
