package main

import (
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

// failKind says what an upstream failure means for the next request.
type failKind int

const (
	failNone        failKind = iota // a real answer (or a client error): return it
	failSkip                        // provider lacks the model: try next, no cooldown
	failTransient                   // rate limit, 5xx, network: short cooldown
	failQuota                       // out of quota/credit: long cooldown
	failConcurrency                 // too many requests in flight on this key: wait and resend, no cooldown
)

func (k failKind) String() string {
	return [...]string{"none", "not_found", "transient", "quota", "concurrency"}[k]
}

// quotaRe spots "no money left" wording. "exceeded" alone is deliberately
// absent: "rate limit exceeded" is transient, not quota.
var quotaRe = regexp.MustCompile(`(?i)quota|insufficient|balance|credit|billing|payment|余额|额度`)

// concurrencyRe spots "too many requests in flight" (e.g. "最多同时 4 个请求" /
// "allows 4 concurrent requests"), which a short wait cures.
var concurrencyRe = regexp.MustCompile(`(?i)concurren|同时`)

// classify decides whether a failed attempt should move to the next provider.
// The same model on another gateway gets the identical request, so anything
// caused by the request itself (400, 413, 422, ...) is returned as is.
func classify(status int, body []byte) failKind {
	switch {
	case status == http.StatusPaymentRequired:
		return failQuota
	case status == http.StatusTooManyRequests && concurrencyRe.Match(body):
		return failConcurrency
	case status == http.StatusTooManyRequests || status == http.StatusForbidden ||
		status == http.StatusBadRequest:
		if quotaRe.Match(body) {
			return failQuota
		}
		if status == http.StatusBadRequest {
			return failNone
		}
		return failTransient
	case status == http.StatusUnauthorized: // already retried with a fresh token
		return failTransient
	case status == http.StatusNotFound:
		return failSkip
	case status == http.StatusRequestTimeout || status >= 500:
		return failTransient
	}
	return failNone
}

// retryAfter parses Retry-After as seconds or an HTTP date.
func retryAfter(h http.Header, now time.Time) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

// Cooldown is one provider being skipped for one model.
type Cooldown struct {
	Provider string    `json:"provider"`
	Model    string    `json:"model"`
	Until    time.Time `json:"until"`
	Reason   string    `json:"reason"`
}

// Health remembers which provider failed for which model and until when. It
// lives on the Server so a config reload does not forget an exhausted quota.
type Health struct {
	mu   sync.Mutex
	cool map[[2]string]Cooldown
	now  func() time.Time
}

func NewHealth() *Health {
	return &Health{cool: make(map[[2]string]Cooldown), now: time.Now}
}

func (h *Health) Mark(provider, model string, d time.Duration, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cool[[2]string{provider, model}] = Cooldown{provider, model, h.now().Add(d), reason}
}

func (h *Health) OK(provider, model string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.cool, [2]string{provider, model})
}

// Order keeps healthy candidates in their given order and moves cooling ones
// to the end, soonest-expiring first. Nothing is dropped: when every provider
// is cooling, trying one beats failing outright.
func (h *Health) Order(model string, candidates []string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	var healthy, cooling []string
	for _, p := range candidates {
		if c, ok := h.cool[[2]string{p, model}]; ok && now.Before(c.Until) {
			cooling = append(cooling, p)
		} else {
			healthy = append(healthy, p)
		}
	}
	sort.SliceStable(cooling, func(i, j int) bool {
		return h.cool[[2]string{cooling[i], model}].Until.Before(h.cool[[2]string{cooling[j], model}].Until)
	})
	return append(healthy, cooling...)
}

// Reset clears cooldowns for one provider, or all when provider is empty.
func (h *Health) Reset(provider string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for k := range h.cool {
		if provider == "" || k[0] == provider {
			delete(h.cool, k)
			n++
		}
	}
	return n
}

// Active lists unexpired cooldowns, longest-lasting first.
func (h *Health) Active() []Cooldown {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	out := make([]Cooldown, 0, len(h.cool))
	for k, c := range h.cool {
		if now.Before(c.Until) {
			out = append(out, c)
		} else {
			delete(h.cool, k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Until.After(out[j].Until) })
	return out
}
