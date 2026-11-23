package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	maxRequestBody = 256 << 20
	maxErrorPeek   = 64 << 10

	// statusOriginTimeout is Cloudflare's 524: the gateway's origin sent no
	// response headers within 120s.
	statusOriginTimeout = 524
)

var hopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

// streamErrRe marks an in-stream error event as a capacity problem worth a cooldown.
var streamErrRe = regexp.MustCompile(`(?i)overloaded|rate.?limit|quota|capacity|unavailable`)

// proxy forwards one model request, failing over across the providers that
// serve the same model. A provider is committed to only once its status is
// known and before any byte reaches the client, so the client never sees a
// half answer; failover shows up only in the X-Relay-* headers and the log.
func (s *Server) proxy(w http.ResponseWriter, r *http.Request, st *state) {
	start := time.Now()
	s.active.Add(1)
	defer s.active.Add(-1)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "relay: read body: "+err.Error())
		return
	}
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "relay: body is not JSON")
		return
	}
	candidates, err := st.router.Candidates(req.Model)
	if err != nil {
		writeError(w, http.StatusBadRequest, "relay: "+err.Error())
		return
	}
	model := NormalizeModel(req.Model)

	// Each pass tries every candidate once. If none gave any HTTP answer the
	// network is down (seen: 30 min with DNS failing for internal and external
	// hosts alike); hold the request and try again rather than failing fast,
	// since nothing has reached the client yet.
	var held time.Time
	for n := 0; ; n++ {
		resp, name, trail, answered := s.pass(r, st, req.Model, model, candidates, body)
		if r.Context().Err() != nil {
			if resp != nil {
				resp.Body.Close()
			}
			return // client went away
		}
		if resp != nil {
			if !held.IsZero() {
				s.log.Info("network outage: request recovered", "model", req.Model,
					"held", time.Since(held).Round(time.Second))
			}
			s.relay(w, r, resp, st, name, req.Model, trail, start)
			return
		}
		wait := *st.cfg.Network.OutageWait
		if answered {
			wait = 0 // a gateway did answer, so the network is up: no outage to wait out
		}
		if !answered {
			s.outageSince.CompareAndSwap(0, time.Now().UnixNano())
		}
		if held.IsZero() {
			held = time.Now()
			if wait > 0 {
				s.log.Warn("network outage: holding request", "model", req.Model, "max", wait,
					"tried", strings.Join(trail, " > "))
			}
		}
		left := wait - time.Since(held)
		if left <= 0 {
			writeRetryable(w, "relay: every provider unreachable for "+model+": "+strings.Join(trail, " > "))
			return
		}
		select {
		case <-time.After(min(outageBackoff(n), left)):
		case <-r.Context().Done():
			return
		}
	}
}

// outageBackoff paces re-tries while the network is down; a var for tests.
var outageBackoff = func(n int) time.Duration {
	return min(time.Second<<min(n, 4), 10*time.Second)
}

// pass tries each candidate once, failing over on retryable answers. It
// returns the response to relay (possibly a final error answer), or nil when
// none is left; answered reports whether any provider gave an HTTP response.
func (s *Server) pass(r *http.Request, st *state, reqModel, model string, candidates []string,
	body []byte) (resp *http.Response, name string, trail []string, answered bool) {
	order := s.health.Order(model, candidates)
	for i, name := range order {
		last := i == len(order)-1
		resp, err := s.attemptRetrying(r, st, st.providers[name], body)
		if err == nil {
			resp, err = s.waitOutConcurrency(r, st, name, body, resp)
		}
		if err != nil {
			if r.Context().Err() != nil {
				return nil, "", trail, answered
			}
			s.health.Mark(name, model, st.cfg.Failover.Cooldown, "network: "+err.Error())
			trail = append(trail, name+"(network)")
			s.log.Warn("upstream unreachable", "model", reqModel, "provider", name, "err", err, "last", last)
			continue
		}

		answered = true
		kind := failNone
		if resp.StatusCode >= 400 {
			if peek, perr := peekBody(resp); perr != nil {
				kind = failTransient // body unreadable: treat like a dropped connection
			} else {
				kind = classify(resp.StatusCode, peek)
			}
		}
		switch {
		case kind == failNone && resp.StatusCode < 400:
			s.health.OK(name, model)
		case kind != failNone:
			s.cool(st, name, model, kind, resp)
			trail = append(trail, fmt.Sprintf("%s(%d %s)", name, resp.StatusCode, kind))
			if !last {
				s.log.Warn("failover", "model", reqModel, "from", name, "to", order[i+1],
					"status", resp.StatusCode, "reason", kind)
				resp.Body.Close()
				continue
			}
		}
		return resp, name, trail, answered
	}
	return nil, "", trail, answered
}

// concurrencyRetryFor bounds resending on a concurrency 429; concurrencyBackoff
// paces it. Vars for tests.
var (
	concurrencyRetryFor = time.Minute
	concurrencyBackoff  = func(n int) time.Duration {
		return min(500*time.Millisecond<<min(n, 2), 2*time.Second)
	}
)

// waitOutConcurrency resends to the same provider while it answers "too many
// concurrent requests". The relay's own limit should prevent that, so this is
// for callers outside the relay sharing the key. It is not a provider fault:
// no cooldown, no failover.
func (s *Server) waitOutConcurrency(r *http.Request, st *state, name string, body []byte,
	resp *http.Response) (*http.Response, error) {
	start := time.Now()
	for n := 0; resp.StatusCode == http.StatusTooManyRequests; n++ {
		peek, err := peekBody(resp)
		if err != nil || classify(resp.StatusCode, peek) != failConcurrency || time.Since(start) >= concurrencyRetryFor {
			return resp, nil // let the caller classify it as usual
		}
		resp.Body.Close()
		if n == 0 {
			s.log.Info("provider at its concurrency limit; waiting to resend", "provider", name)
		}
		select {
		case <-time.After(concurrencyBackoff(n)):
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		var aerr error
		if resp, aerr = s.attemptRetrying(r, st, st.providers[name], body); aerr != nil {
			return nil, aerr
		}
	}
	return resp, nil
}

type readCloser struct {
	io.Reader
	io.Closer
}

func (s *Server) cool(st *state, provider, model string, kind failKind, resp *http.Response) {
	var d time.Duration
	switch kind {
	case failQuota:
		d = st.cfg.Failover.QuotaCooldown
	case failTransient:
		d = st.cfg.Failover.Cooldown
	default:
		return
	}
	if ra := retryAfter(resp.Header, time.Now()); ra > d {
		d = ra
	}
	s.health.Mark(provider, model, d, fmt.Sprintf("%s (HTTP %d)", kind, resp.StatusCode))
}

// retryBackoff is the pause before the n-th same-provider retry; a var for tests.
var retryBackoff = func(n int) time.Duration { return time.Duration(n) * 300 * time.Millisecond }

// attemptRetrying retries the same provider on connection-level errors (no
// response at all) before the caller fails over, on a different lane when
// there is one. The body is buffered, so the resend is exact; LLM calls are
// stateless, so the worst case is paying twice for a request whose answer was
// lost.
func (s *Server) attemptRetrying(r *http.Request, st *state, p *provider, body []byte) (*http.Response, error) {
	retries := *st.cfg.Network.Retries
	l := s.laneFor(p, nil)
	for n := 0; ; n++ {
		resp, err := s.attempt(r, l, p, body)
		if err == nil || n >= retries || r.Context().Err() != nil {
			return resp, err
		}
		failed := l
		l = s.laneFor(p, failed)
		s.log.Warn("connection error; retrying same provider", "provider", p.cfg.Name,
			"retry", n+1, "of", retries, "lane", failed.id, "next_lane", l.id, "err", err)
		select {
		case <-time.After(retryBackoff(n + 1)):
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
}

// writeRetryable answers in a way Claude Code and Codex treat as "try again"
// (5xx + Retry-After + x-should-retry) instead of a hard failure.
func writeRetryable(w http.ResponseWriter, msg string) {
	w.Header().Set("Retry-After", "3")
	w.Header().Set("X-Should-Retry", "true")
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{
		"type":  "error",
		"error": map[string]string{"type": "api_error", "message": msg},
	})
}

// attempt sends to one provider. Recoverable rejections are retried on the same
// provider: an expired helper token, and foreign thinking signatures.
func (s *Server) attempt(r *http.Request, l *lane, p *provider, body []byte) (*http.Response, error) {
	resp, err := s.sendRefreshing(r, l, p, body)
	if err == nil && resp.StatusCode == http.StatusBadRequest && strings.HasSuffix(r.URL.Path, "/messages") {
		resp, err = s.retrySanitized(r, l, p, body, resp)
	}
	if err != nil || resp.StatusCode >= 300 {
		return resp, err
	}
	// Nothing reaches the client until the first byte is here, so a connection
	// lost before it is still an ordinary retryable attempt error.
	if err := awaitFirstByte(resp); err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("upstream connection lost before the first byte: %w", err)
	}
	return resp, nil
}

// awaitFirstByte blocks until the body yields data (or a clean empty end) and
// puts what it read back in front of the body.
func awaitFirstByte(resp *http.Response) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 || errors.Is(err, io.EOF) {
			resp.Body = readCloser{io.MultiReader(bytes.NewReader(buf[:n]), resp.Body), resp.Body}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// sendRefreshing refreshes a helper token once on 401; helper tokens can
// expire upstream before our TTL does.
func (s *Server) sendRefreshing(r *http.Request, l *lane, p *provider, body []byte) (*http.Response, error) {
	resp, err := s.send(r.Context(), l, p, r.Method, r.URL.RequestURI(), r.Header, body, false)
	if err == nil && resp.StatusCode == http.StatusUnauthorized && p.cred.Refreshable() {
		resp.Body.Close()
		resp, err = s.send(r.Context(), l, p, r.Method, r.URL.RequestURI(), r.Header, body, true)
	}
	return resp, err
}

// relay streams the committed response to the client.
func (s *Server) relay(w http.ResponseWriter, r *http.Request, resp *http.Response, st *state,
	name, model string, trail []string, start time.Time) {
	defer resp.Body.Close()
	copyHeaders(w.Header(), resp.Header)
	messages := isMessagesStream(r, resp)
	if messages {
		w.Header().Del("Content-Length") // the relay may reorder or drop events
	}
	if resp.StatusCode == statusOriginTimeout {
		// Cloudflare's 524 carries "Retry-After: 120", and Claude Code gives up
		// on any Retry-After above 60s (tengu_api_retry_after_too_long), so a
		// slow origin became a final error. A fresh request is the right answer.
		w.Header().Del("Retry-After")
		w.Header().Set("X-Should-Retry", "true")
	}
	w.Header().Set("X-Relay-Provider", name)
	if len(trail) > 0 {
		w.Header().Set("X-Relay-Failover", strings.Join(append(trail, name), " > "))
	}
	w.WriteHeader(resp.StatusCode)

	coolOnce := false
	cool := func() {
		// let the client's own retry land on the next provider
		if !coolOnce {
			coolOnce = true
			s.health.Mark(name, NormalizeModel(model), st.cfg.Failover.Cooldown, "stream error event")
		}
	}

	attrs := []any{"path", r.URL.Path, "model", model, "provider", name, "lane", resp.Header.Get("X-Relay-Lane"),
		"status", resp.StatusCode}
	if len(trail) > 0 {
		attrs = append(attrs, "failover", strings.Join(trail, " > "))
	}
	var err error
	if messages {
		err = relayMessagesSSE(w, resp.Body, cool)
	} else {
		err = stream(w, resp.Body, func(b []byte) {
			if bytes.Contains(b, []byte("event: error")) && streamErrRe.Match(b) {
				cool()
			}
		})
	}
	attrs = append(attrs, "ms", time.Since(start).Milliseconds())
	var cw clientWriteError
	switch {
	case err == nil:
		s.log.Info("proxied", attrs...)
	case r.Context().Err() != nil || errors.As(err, &cw):
		// Codex hangs up right after response.completed; that is a normal end.
		s.log.Info("proxied", append(attrs, "client_closed", true)...)
	case messages:
		// No completed block was delivered (see sse.go): ending cleanly makes
		// Claude Code re-ask without streaming instead of showing an error.
		s.log.Warn("upstream lost mid-stream; ended early so the client re-asks without streaming",
			append(attrs, "err", err)...)
	default:
		// Other clients (Codex) retry a broken stream but would accept a clean
		// end as a complete answer, so make the break visible.
		s.log.Warn("upstream lost mid-stream; aborting client connection so it retries", append(attrs, "err", err)...)
		panic(http.ErrAbortHandler) // net/http closes the connection without a clean end
	}
}

// send issues one upstream request on lane l with the provider's credential
// swapped in for whatever the client presented. The lane counts the request as
// in flight until the response body is closed.
func (s *Server) send(ctx context.Context, l *lane, p *provider, method, uri string, h http.Header, body []byte, refresh bool) (*http.Response, error) {
	token, err := p.cred.Get(ctx, refresh)
	if err != nil {
		return nil, err
	}
	s.activity.touch(laneKey(p.cfg.Name, l.id))
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.cfg.BaseURL+uri, rd)
	if err != nil {
		return nil, err
	}
	req.Header = h.Clone()
	removeHop(req.Header)
	req.Header.Del("Authorization")
	req.Header.Del("X-Api-Key")
	req.Header.Del("Content-Length")
	if method == http.MethodPost && strings.HasSuffix(strings.SplitN(uri, "?", 2)[0], "/messages") {
		// The relay must read Messages streams event by event (sse.go);
		// some gateways gzip SSE when allowed to. Loopback needs no compression.
		req.Header.Set("Accept-Encoding", "identity")
	}
	if p.cfg.Auth.Header == "x-api-key" {
		req.Header.Set("X-Api-Key", token)
	} else {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	release, err := s.takeSlot(ctx, p)
	if err != nil {
		return nil, err
	}
	l.begin()
	resp, err := l.client.Do(req)
	if err != nil {
		l.inflight.Add(-1)
		release()
		return nil, err
	}
	s.lastUpstreamOK.Store(time.Now().UnixNano())
	s.outageSince.Store(0) // any HTTP answer means the network is back
	resp.Body = &laneBody{ReadCloser: resp.Body, l: l, release: release}
	resp.Header.Set("X-Relay-Lane", strconv.Itoa(l.id))
	return resp, nil
}

// clientWriteError marks a failure writing to the client, as opposed to
// reading from the upstream.
type clientWriteError struct{ error }

// stream copies the upstream body, flushing every read so SSE events are not
// held back in buffers.
func stream(w http.ResponseWriter, body io.Reader, onChunk func([]byte)) error {
	rc := http.NewResponseController(w)
	buf := make([]byte, 32<<10)
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			onChunk(buf[:n])
			if _, err := w.Write(buf[:n]); err != nil {
				return clientWriteError{err}
			}
			if err := rc.Flush(); err != nil {
				return clientWriteError{err}
			}
		}
		if errors.Is(rerr, io.EOF) {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

func removeHop(h http.Header) {
	for _, f := range strings.Split(h.Get("Connection"), ",") {
		if f = strings.TrimSpace(f); f != "" {
			h.Del(f)
		}
	}
	for _, k := range hopHeaders {
		h.Del(k)
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
	removeHop(dst)
}
