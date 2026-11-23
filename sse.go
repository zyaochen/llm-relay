package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
)

// Claude Code recovers from a broken stream only one way: when a stream ends
// "with message_start but no content blocks completed" it re-asks without
// streaming. Every other ending is final — measured with real Claude Code, an
// upstream cut after one completed block gave: clean end → the half answer
// accepted silently; an overloaded_error event → final error; a dropped
// connection → "socket connection was closed unexpectedly".
//
// So for Anthropic Messages streams the relay forwards whole events but holds
// every content_block_stop until the message is finishing (just before
// message_delta). Text and tool input still stream live, and if the upstream
// is lost anywhere before that point the relay simply ends the response: to
// the client no block ever completed, and it retries on its own. Measured: the
// deferred order changes nothing for normal replies or tool calls.

var (
	errStreamTruncated = errors.New("upstream stream ended before message_stop")
	errStreamErrorEvt  = errors.New("upstream sent an error event mid-stream")
)

// isMessagesStream reports whether resp is an Anthropic Messages SSE stream.
// A compressed body cannot be parsed into events, so it is never treated as
// one; it is passed through untouched instead (send asks for identity, so
// this only guards against an upstream ignoring that).
func isMessagesStream(r *http.Request, resp *http.Response) bool {
	enc := resp.Header.Get("Content-Encoding")
	return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages") &&
		resp.StatusCode == http.StatusOK &&
		strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") &&
		(enc == "" || strings.EqualFold(enc, "identity"))
}

// relayMessagesSSE forwards body event by event, recording token usage in u.
// It returns nil once message_stop has been forwarded, a clientWriteError if
// the client went away, and otherwise the reason the upstream stream broke
// (the response is then left to end cleanly, with no completed block delivered).
func relayMessagesSSE(w http.ResponseWriter, body io.Reader, onErrorEvent func(), u *tokenUsage) error {
	rc := http.NewResponseController(w)
	write := func(b []byte) error {
		if _, err := w.Write(b); err != nil {
			return clientWriteError{err}
		}
		if err := rc.Flush(); err != nil {
			return clientWriteError{err}
		}
		return nil
	}

	var pending, held []byte
	buf := make([]byte, 32<<10)
	for {
		n, rerr := body.Read(buf)
		pending = append(pending, buf[:n]...)
		for {
			end := eventEnd(pending)
			if end < 0 {
				break
			}
			ev := pending[:end]
			name := sseEventName(ev)
			if name == "message_start" || name == "message_delta" {
				u.addJSON(sseData(ev))
			}
			switch name {
			case "content_block_stop":
				held = append(held, ev...)
			case "message_delta", "message_stop":
				if err := write(append(held, ev...)); err != nil {
					return err
				}
				held = nil
				if name == "message_stop" {
					return nil
				}
			case "error":
				onErrorEvent()
				return errStreamErrorEvt
			default:
				if err := write(ev); err != nil {
					return err
				}
			}
			pending = pending[end:]
		}
		if errors.Is(rerr, io.EOF) {
			return errStreamTruncated
		}
		if rerr != nil {
			return rerr
		}
	}
}

// eventEnd returns the length of the first complete SSE event (including its
// blank-line terminator) in b, or -1.
func eventEnd(b []byte) int {
	i, j := bytes.Index(b, []byte("\n\n")), bytes.Index(b, []byte("\r\n\r\n"))
	switch {
	case i < 0 && j < 0:
		return -1
	case j >= 0 && (i < 0 || j < i):
		return j + 4
	default:
		return i + 2
	}
}

// sseEventName reads the `event:` field, falling back to the JSON "type" of the
// data for streams that omit event names.
func sseEventName(ev []byte) string {
	for _, line := range strings.Split(string(ev), "\n") {
		line = strings.TrimRight(line, "\r")
		if name, ok := strings.CutPrefix(line, "event:"); ok {
			return strings.TrimSpace(name)
		}
	}
	if i := bytes.Index(ev, []byte(`"type":"`)); i >= 0 {
		rest := ev[i+len(`"type":"`):]
		if j := bytes.IndexByte(rest, '"'); j >= 0 {
			return string(rest[:j])
		}
	}
	return ""
}
