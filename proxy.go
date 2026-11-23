package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st := s.st.Load()
	if r.Method == http.MethodGet && r.URL.Path == "/relay/health" {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "providers": st.cfg.providerNames(), "tls_dials": s.dialSummary(st),
			"lanes": s.laneSummary(st), "active_requests": s.active.Load(), "concurrency": s.concurrencySummary(st),
			"outage_since": unixOrNil(s.outageSince.Load()), "last_upstream_ok": unixOrNil(s.lastUpstreamOK.Load()),
		})
		return
	}
	if !st.authorized(r) {
		writeError(w, http.StatusUnauthorized, "relay: missing or wrong client key")
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/relay/routes":
		writeJSON(w, http.StatusOK, map[string]any{
			"routes":    st.router.Table(),
			"cooldowns": s.health.Active(),
		})
	case r.Method == http.MethodPost && r.URL.Path == "/relay/reset":
		n := s.health.Reset(r.URL.Query().Get("provider"))
		s.log.Info("cooldowns reset", "provider", r.URL.Query().Get("provider"), "cleared", n)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "cleared": n})
	case r.Method == http.MethodPost && r.URL.Path == "/relay/reload":
		if err := s.Reload(); err != nil {
			writeError(w, http.StatusBadRequest, "relay: reload rejected: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case r.Method == http.MethodGet && (r.URL.Path == "/v1/models" || r.URL.Path == "/models"):
		s.mergedModels(w, r, st)
	case r.Method == http.MethodPost:
		s.proxy(w, r, st)
	default:
		writeError(w, http.StatusNotFound, fmt.Sprintf("relay: unsupported %s %s", r.Method, r.URL.Path))
	}
}

func (st *state) authorized(r *http.Request) bool {
	if len(st.clientKey) == 0 {
		return true
	}
	got := r.Header.Get("X-Api-Key")
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		got = strings.TrimPrefix(auth, "Bearer ")
	}
	return subtle.ConstantTimeCompare([]byte(got), st.clientKey) == 1
}

// dialSummary reports TLS dial counters per provider since the relay started.
// Few dials relative to requests means connections are being reused.
func (s *Server) dialSummary(st *state) map[string]map[string]int64 {
	out := make(map[string]map[string]int64)
	for _, p := range st.cfg.Providers {
		u, err := url.Parse(p.BaseURL)
		if err != nil || u.Scheme != "https" {
			continue
		}
		d := s.dialer.Stats(u.Hostname())
		avg := int64(0)
		if n := d.Dials.Load(); n > 0 {
			avg = d.TotalMs.Load() / n
		}
		out[p.Name] = map[string]int64{
			"dials": d.Dials.Load(), "hedges": d.Hedges.Load(), "attempt_failures": d.AttemptFails.Load(),
			"dial_failures": d.Failures.Load(), "last_ms": d.LastMs.Load(), "avg_ms": avg,
			"stale_dns": d.StaleDNS.Load(),
		}
	}
	return out
}

// concurrencySummary shows each limited provider's slots: limit, in use and
// requests queued for one.
func (s *Server) concurrencySummary(st *state) map[string]map[string]int {
	out := make(map[string]map[string]int)
	for _, p := range st.cfg.Providers {
		if p.MaxConcurrent <= 0 {
			continue
		}
		l := s.limits.get(p)
		inUse, waiting, waitingPriority := l.counts()
		out[p.Name] = map[string]int{"limit": l.limit(), "reserved": l.reserved, "in_use": inUse,
			"waiting": waiting, "waiting_priority": waitingPriority}
	}
	return out
}

// unixOrNil renders a unix-nanos timestamp for JSON, nil when unset.
func unixOrNil(ns int64) any {
	if ns == 0 {
		return nil
	}
	return time.Unix(0, ns).Format(time.RFC3339)
}

func (c *Config) providerNames() []string {
	names := make([]string, len(c.Providers))
	for i, p := range c.Providers {
		names[i] = p.Name
	}
	return names
}

// mergedModels serves the union of every provider's model list, so /model in
// Claude Code and Codex shows all of them.
func (s *Server) mergedModels(w http.ResponseWriter, r *http.Request, st *state) {
	names := st.cfg.providerNames()
	results := make([][]json.RawMessage, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := st.providers[name]
			data, ids, err := s.fetchModels(r.Context(), p, r.URL.RequestURI())
			if err != nil {
				s.log.Warn("models fetch failed", "provider", name, "err", err)
				return
			}
			results[i] = data
			st.router.SetDiscovered(name, ids)
		}()
	}
	wg.Wait()

	seen := make(map[string]bool)
	merged := make([]json.RawMessage, 0)
	add := func(id string, raw json.RawMessage) {
		if id != "" && !seen[id] {
			seen[id] = true
			merged = append(merged, raw)
		}
	}
	for i, name := range names {
		for _, raw := range results[i] {
			var m struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(raw, &m) == nil {
				add(m.ID, raw)
			}
		}
		for _, id := range st.providers[name].cfg.Models {
			raw, err := json.Marshal(map[string]string{"id": id, "object": "model", "type": "model"})
			if err != nil {
				s.log.Error("encode static model", "model", id, "err", err)
				continue
			}
			add(id, raw)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": merged, "has_more": false})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // client already gone; nothing to report to
}

// writeError uses a shape both Anthropic and OpenAI clients can display.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"type":  "error",
		"error": map[string]string{"type": "relay_error", "message": msg},
	})
}
