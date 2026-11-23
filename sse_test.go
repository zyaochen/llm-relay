package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func ssev(name string) string { return "event: " + name + "\ndata: {\"type\":\"" + name + "\"}\n\n" }

// chunked feeds data in small pieces so events straddle read boundaries.
type chunked struct {
	data string
	size int
	err  error
}

func (c *chunked) Read(p []byte) (int, error) {
	if c.data == "" {
		return 0, c.err
	}
	n := min(c.size, len(c.data), len(p))
	copy(p, c.data[:n])
	c.data = c.data[n:]
	return n, nil
}

func TestRelayMessagesSSEDefersBlockStops(t *testing.T) {
	in := ssev("message_start") + ssev("content_block_start") + ssev("content_block_delta") + ssev("content_block_stop") +
		ssev("content_block_start") + ssev("content_block_delta") + ssev("content_block_stop") +
		ssev("message_delta") + ssev("message_stop")
	rec := httptest.NewRecorder()
	if err := relayMessagesSSE(rec, &chunked{data: in, size: 7, err: io.EOF}, func() {}); err != nil {
		t.Fatal(err)
	}
	want := ssev("message_start") + ssev("content_block_start") + ssev("content_block_delta") +
		ssev("content_block_start") + ssev("content_block_delta") +
		ssev("content_block_stop") + ssev("content_block_stop") + ssev("message_delta") + ssev("message_stop")
	if got := rec.Body.String(); got != want {
		t.Errorf("stops must move, in order, to just before message_delta:\n got %q\nwant %q", got, want)
	}
}

func TestRelayMessagesSSEOnLoss(t *testing.T) {
	in := ssev("message_start") + ssev("content_block_start") + ssev("content_block_stop") + "event: content_block_d"
	rec := httptest.NewRecorder()
	err := relayMessagesSSE(rec, &chunked{data: in, size: 5, err: errors.New("read: connection reset")}, func() {})
	if err == nil || strings.Contains(rec.Body.String(), "content_block_stop") || strings.Contains(rec.Body.String(), "content_block_d\n") {
		t.Fatalf("loss must report an error and deliver neither held stops nor partial events: %v %q", err, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	err = relayMessagesSSE(rec, &chunked{data: ssev("message_start") + ssev("content_block_stop"), size: 99, err: io.EOF}, func() {})
	if !errors.Is(err, errStreamTruncated) || strings.Contains(rec.Body.String(), "content_block_stop") {
		t.Fatalf("EOF before message_stop is a truncated stream: %v %q", err, rec.Body.String())
	}
}

func TestRelayMessagesSSEErrorEvent(t *testing.T) {
	in := ssev("message_start") + ssev("content_block_start") +
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}\n\n"
	rec := httptest.NewRecorder()
	called := false
	err := relayMessagesSSE(rec, &chunked{data: in, size: 99, err: io.EOF}, func() { called = true })
	if !errors.Is(err, errStreamErrorEvt) || !called || strings.Contains(rec.Body.String(), "overloaded") {
		t.Fatalf("error event: err=%v cooled=%v body=%q", err, called, rec.Body.String())
	}
}

func TestSSEHelpers(t *testing.T) {
	for in, want := range map[string]int{"a\n\nb": 3, "a\r\n\r\nb": 5, "no end": -1, "x\r\n\r\ny\n\n": 5} {
		if got := eventEnd([]byte(in)); got != want {
			t.Errorf("eventEnd(%q) = %d, want %d", in, got, want)
		}
	}
	if got := sseEventName([]byte("data: {\"type\":\"ping\"}\n\n")); got != "ping" {
		t.Errorf("name from data type = %q", got)
	}
	if got := sseEventName([]byte("event: message_delta\r\ndata: {}\r\n\r\n")); got != "message_delta" {
		t.Errorf("name with CRLF = %q", got)
	}
}

// gzipUpstream compresses its SSE whenever the request allows it, like
// some Cloudflare-fronted gateways do, and records what the relay asked for.
func gzipUpstream(t *testing.T, seen *atomic.Value) *httptest.Server {
	t.Helper()
	full := ssev("message_start") + ssev("content_block_start") + ssev("content_block_delta") +
		ssev("content_block_stop") + ssev("message_delta") + ssev("message_stop")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Type", "text/event-stream")
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			fmt.Fprint(w, full)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		if _, err := io.WriteString(zw, full); err != nil {
			t.Errorf("gzip write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Errorf("gzip close: %v", err)
		}
	}))
	t.Cleanup(up.Close)
	return up
}

func TestMessagesStreamsAreFetchedUncompressed(t *testing.T) {
	var seen atomic.Value
	up := gzipUpstream(t, &seen)
	relay := singleProviderRelay(t, up.URL, "{retries: 0, keep_warm: 0s}")
	req, err := http.NewRequest(http.MethodPost, relay.URL+"/v1/messages?beta=true", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Encoding", "gzip, deflate, br, zstd") // what Claude Code sends
	resp, err := http.DefaultTransport.RoundTrip(req)            // no transparent decoding: see the raw bytes
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got := seen.Load(); got != "identity" {
		t.Errorf("relay must ask the upstream for identity on /messages, sent %q", got)
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" {
		t.Errorf("client must get an uncompressed stream, got Content-Encoding %q", enc)
	}
	if !strings.HasSuffix(string(body), ssev("message_stop")) {
		t.Errorf("stream must arrive complete and parsed: %q", body)
	}
}

func TestCompressedStreamIsPassedThroughUntouched(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	resp := &http.Response{StatusCode: 200, Header: http.Header{
		"Content-Type": {"text/event-stream"}, "Content-Encoding": {"gzip"}}}
	if isMessagesStream(r, resp) {
		t.Error("a gzip body must not be parsed as SSE events")
	}
}
