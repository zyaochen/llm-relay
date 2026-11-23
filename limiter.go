package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// limiter caps how many requests are in flight to one provider. Some
// gateways answer a fifth concurrent request with 429 "this key allows 4
// concurrent requests"; queueing locally turns that into a short wait instead
// of a rejected request and a client-side retry. Goroutines blocked sending on
// a channel are served FIFO, so the queue is fair.
type limiter struct {
	slots   chan struct{} // nil: unlimited
	waiting atomic.Int32
}

func newLimiter(n int) *limiter {
	if n <= 0 {
		return &limiter{}
	}
	return &limiter{slots: make(chan struct{}, n)}
}

func (l *limiter) limit() int { return cap(l.slots) }
func (l *limiter) inUse() int { return len(l.slots) }

func (l *limiter) releaser() func() {
	var once sync.Once
	return func() { once.Do(func() { <-l.slots }) }
}

// acquire waits for a slot until ctx ends and reports how long it waited.
func (l *limiter) acquire(ctx context.Context) (func(), time.Duration, error) {
	if l.slots == nil {
		return func() {}, 0, nil
	}
	select {
	case l.slots <- struct{}{}:
		return l.releaser(), 0, nil
	default:
	}
	l.waiting.Add(1)
	defer l.waiting.Add(-1)
	start := time.Now()
	select {
	case l.slots <- struct{}{}:
		return l.releaser(), time.Since(start), nil
	case <-ctx.Done():
		return nil, time.Since(start), ctx.Err()
	}
}

func (l *limiter) tryAcquire() (func(), bool) {
	if l.slots == nil {
		return func() {}, true
	}
	select {
	case l.slots <- struct{}{}:
		return l.releaser(), true
	default:
		return nil, false
	}
}

// limiters keeps one limiter per provider across config reloads; a provider
// gets a fresh one only when its limit changes (holders of the old one still
// release into it, so nothing leaks).
type limiters struct {
	mu sync.Mutex
	m  map[string]*limiter
}

func (ls *limiters) get(provider string, n int) *limiter {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.m == nil {
		ls.m = make(map[string]*limiter)
	}
	l, ok := ls.m[provider]
	if !ok || l.limit() != max(n, 0) {
		l = newLimiter(n)
		ls.m[provider] = l
	}
	return l
}

// slotPolicy says how a request may wait for a provider slot; it travels in
// the context so every caller of send keeps its signature.
type slotPolicy int

const (
	slotWait  slotPolicy = iota // client requests: queue as long as the client waits
	slotTry                     // keep-warm: only if a slot is free right now
	slotBrief                   // model listings: wait a little, then skip
)

const slotBriefWait = 5 * time.Second

type slotPolicyKey struct{}

func withSlotPolicy(ctx context.Context, p slotPolicy) context.Context {
	return context.WithValue(ctx, slotPolicyKey{}, p)
}

var errNoSlot = errors.New("provider concurrency limit reached")

// takeSlot acquires a slot for provider under the context's policy.
func (s *Server) takeSlot(ctx context.Context, p *provider) (func(), error) {
	lim := s.limits.get(p.cfg.Name, p.cfg.MaxConcurrent)
	policy, _ := ctx.Value(slotPolicyKey{}).(slotPolicy)
	switch policy {
	case slotTry:
		if release, ok := lim.tryAcquire(); ok {
			return release, nil
		}
		return nil, errNoSlot
	case slotBrief:
		bctx, cancel := context.WithTimeout(ctx, slotBriefWait)
		defer cancel()
		release, _, err := lim.acquire(bctx)
		if err != nil {
			return nil, errNoSlot
		}
		return release, nil
	}
	release, waited, err := lim.acquire(ctx)
	if err != nil {
		return nil, err
	}
	if waited > 0 {
		s.log.Info("waited for a free provider slot", "provider", p.cfg.Name,
			"limit", lim.limit(), "waited", waited.Round(time.Millisecond))
	}
	return release, nil
}
