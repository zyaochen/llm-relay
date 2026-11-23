package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

// maxUsageBody caps how much of a non-streaming Messages reply is kept to read
// its usage; anything larger is passed through and simply not counted.
const maxUsageBody = 8 << 20

// tokenUsage is the token accounting of one Anthropic Messages reply, read from the
// stream's message_start and message_delta events or from a non-streaming
// body, so `llm-relay stats` can show spend and prompt cache hits.
type tokenUsage struct {
	seen                           bool
	in, out, cacheRead, cacheWrite int64
}

type usageFields struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	CacheRead    int64 `json:"cache_read_input_tokens"`
	CacheWrite   int64 `json:"cache_creation_input_tokens"`
}

// addJSON reads the usage of a message_start / message_delta event's data or
// of a whole non-streaming reply. Counts in message_delta are cumulative, so a
// non-zero count replaces the earlier one; message_start carries the input
// side, and some gateways only fill it in at message_delta.
func (u *tokenUsage) addJSON(b []byte) {
	var d struct {
		Message struct {
			Usage *usageFields `json:"usage"`
		} `json:"message"`
		Usage *usageFields `json:"usage"`
	}
	if json.Unmarshal(b, &d) != nil {
		return
	}
	for _, f := range []*usageFields{d.Message.Usage, d.Usage} {
		if f == nil {
			continue
		}
		u.seen = true
		for _, c := range []struct {
			dst *int64
			v   int64
		}{{&u.in, f.InputTokens}, {&u.out, f.OutputTokens}, {&u.cacheRead, f.CacheRead}, {&u.cacheWrite, f.CacheWrite}} {
			if c.v > 0 {
				*c.dst = c.v
			}
		}
	}
}

// attrs are the log fields stats reads back; none when no usage was seen.
func (u *tokenUsage) attrs() []any {
	if u == nil || !u.seen {
		return nil
	}
	return []any{"in", u.in, "out", u.out, "cache_read", u.cacheRead, "cache_write", u.cacheWrite}
}

// sseData joins the data lines of one SSE event.
func sseData(ev []byte) []byte {
	var out []byte
	for _, line := range bytes.Split(ev, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if d, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			if len(out) > 0 {
				out = append(out, '\n')
			}
			out = append(out, bytes.TrimPrefix(d, []byte(" "))...)
		}
	}
	return out
}

// isMessagesJSON reports whether resp is a non-streaming Messages reply (what
// Claude Code re-asks with after a lost stream).
func isMessagesJSON(r *http.Request, resp *http.Response) bool {
	enc := resp.Header.Get("Content-Encoding")
	return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages") &&
		resp.StatusCode == http.StatusOK &&
		strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") &&
		(enc == "" || strings.EqualFold(enc, "identity"))
}
