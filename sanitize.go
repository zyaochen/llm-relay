package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
)

// anthropicSig matches thinking signatures issued by Anthropic models (long
// base64). Other vendors' signatures, e.g. kimi's 32-char hex, do not match.
var anthropicSig = regexp.MustCompile(`^[A-Za-z0-9+/]{200,}={0,2}$`)

// retrySanitized handles one specific 400: a Claude model rejecting a thinking
// block it did not sign (history produced by another model). Normal traffic
// never reaches here, so it is never rewritten. Pass 1 drops only foreign
// thinking blocks; pass 2 drops all of them. Text, tool calls and tool results
// are never touched, so the conversation itself is preserved.
func (s *Server) retrySanitized(r *http.Request, l *lane, p *provider, body []byte, resp *http.Response) (*http.Response, error) {
	for _, foreignOnly := range []bool{true, false} {
		peek, err := peekBody(resp)
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		if !signatureRejected(peek) {
			return resp, nil
		}
		clean, n, err := stripThinking(body, foreignOnly)
		if err != nil {
			s.log.Warn("cannot sanitize thinking blocks", "provider", p.cfg.Name, "err", err)
			return resp, nil
		}
		if n == 0 {
			continue
		}
		resp.Body.Close()
		s.log.Warn("thinking signature rejected; retrying without those blocks",
			"provider", p.cfg.Name, "removed", n, "foreign_only", foreignOnly)
		if resp, err = s.sendRefreshing(r, l, p, clean); err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusBadRequest {
			return resp, nil
		}
	}
	return resp, nil
}

func signatureRejected(body []byte) bool {
	return bytes.Contains(body, []byte("signature")) && bytes.Contains(body, []byte("thinking"))
}

// peekBody reads up to maxErrorPeek bytes and puts them back in front of the body.
func peekBody(resp *http.Response) ([]byte, error) {
	peek, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorPeek))
	if err != nil {
		return nil, err
	}
	resp.Body = readCloser{io.MultiReader(bytes.NewReader(peek), resp.Body), resp.Body}
	return peek, nil
}

// stripThinking removes thinking blocks from assistant messages of an
// Anthropic Messages body. With foreignOnly it keeps Anthropic-signed thinking
// and every redacted_thinking block. Fields it does not touch are carried over
// as raw JSON. An assistant turn left with no content is removed.
func stripThinking(body []byte, foreignOnly bool) ([]byte, int, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, 0, err
	}
	raw, ok := top["messages"]
	if !ok {
		return nil, 0, errors.New("body has no messages")
	}
	var msgs []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil, 0, err
	}

	removed := 0
	out := make([]map[string]json.RawMessage, 0, len(msgs))
	for _, m := range msgs {
		keep, n, err := stripMessage(m, foreignOnly)
		if err != nil {
			return nil, 0, err
		}
		removed += n
		if keep {
			out = append(out, m)
		}
	}
	if removed == 0 {
		return body, 0, nil
	}

	enc, err := json.Marshal(out)
	if err != nil {
		return nil, 0, err
	}
	top["messages"] = enc
	clean, err := json.Marshal(top)
	return clean, removed, err
}

// stripMessage removes dropped thinking blocks from one message in place. It
// reports whether the message stays and how many blocks it lost.
func stripMessage(m map[string]json.RawMessage, foreignOnly bool) (bool, int, error) {
	var role string
	var blocks []json.RawMessage
	// plain-string content has no thinking blocks
	if json.Unmarshal(m["role"], &role) != nil || role != "assistant" ||
		json.Unmarshal(m["content"], &blocks) != nil {
		return true, 0, nil
	}
	kept := make([]json.RawMessage, 0, len(blocks))
	for _, b := range blocks {
		var head struct {
			Type      string `json:"type"`
			Signature string `json:"signature"`
		}
		if json.Unmarshal(b, &head) == nil && dropThinking(head.Type, head.Signature, foreignOnly) {
			continue
		}
		kept = append(kept, b)
	}
	removed := len(blocks) - len(kept)
	switch {
	case removed == 0:
		return true, 0, nil
	case len(kept) == 0:
		// A thinking-only turn: drop it. The API merges the neighbouring
		// same-role turns, and empty content would be rejected anyway.
		return false, removed, nil
	}
	content, err := json.Marshal(kept)
	if err != nil {
		return false, 0, err
	}
	m["content"] = content
	return true, removed, nil
}

func dropThinking(typ, signature string, foreignOnly bool) bool {
	switch typ {
	case "thinking":
		return !foreignOnly || !anthropicSig.MatchString(signature)
	case "redacted_thinking":
		return !foreignOnly
	}
	return false
}
