package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// cutAfter streams prefix as SSE, then kills the connection.
func cutAfter(t *testing.T, contentType, prefix string) *httptest.Server {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		fmt.Fprint(w, prefix)
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
	}))
	t.Cleanup(up.Close)
	return up
}

func readAll(t *testing.T, url string) (string, error) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

const messageStart = "event: message_start\ndata: {\"type\":\"message_start\"}\n\n"

// A completed block followed by a cut: the shape that used to end in a final
// "socket connection was closed unexpectedly" in Claude Code.
const completedBlockThenMore = messageStart +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"text\":\"A\"}}\n\n" +
	"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1}\n\n"

func TestMidStreamLossEndsCleanlyWithNoCompletedBlock(t *testing.T) {
	for name, tail := range map[string]string{
		"on an event boundary": "",
		"inside an event":      "event: content_block_delta\ndata: {\"delta\":\"hal",
	} {
		t.Run(name, func(t *testing.T) {
			up := cutAfter(t, "text/event-stream", completedBlockThenMore+tail)
			relay := singleProviderRelay(t, up.URL, "{retries: 0, keep_warm: 0s}")
			body, err := readAll(t, relay.URL+"/v1/messages")
			if err != nil {
				t.Fatalf("the client must get a clean end (it then re-asks without streaming): %v", err)
			}
			if strings.Contains(body, "content_block_stop") {
				t.Errorf("no block may look completed to the client: %q", body)
			}
			if !strings.HasPrefix(body, messageStart) || !strings.Contains(body, `"text":"A"`) {
				t.Errorf("events before the cut must still stream live: %q", body)
			}
			if strings.Contains(body, "hal") {
				t.Errorf("a partial event must never be forwarded: %q", body)
			}
		})
	}
}

func TestMidStreamLossOnResponsesAborts(t *testing.T) {
	up := cutAfter(t, "text/event-stream", "event: response.created\ndata: {}\n\n")
	relay := singleProviderRelay(t, up.URL, "{retries: 0, keep_warm: 0s}")
	if body, err := readAll(t, relay.URL+"/v1/responses"); err == nil {
		t.Fatalf("Codex stream cut must surface as a read error, got clean %q", body)
	}
}
