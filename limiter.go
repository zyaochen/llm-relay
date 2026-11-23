package main

import (
	"container/list"
	"context"
	"errors"
	"path"
	"sync"
	"time"
)

// limiter caps how many requests are in flight to one provider. Some
// gateways answer a fifth concurrent request with 429 "this key allows 4
// concurrent requests"; queueing locally turns that into a short wait instead
// of a rejected request and a client-side retry.
//
// Waiters are served FIFO within two classes, priority first. Priority is for
// the interactive model: background traffic (Claude Code's haiku calls) is
// twice its volume and used to hold every slot while the main turn queued.
// reserved slots are never given to normal requests, so a priority request
// usually finds one free without waiting at all.
type limiter struct {
	n, reserved int // n <= 0: unlimited

	mu      sync.Mutex
	inUse   int
	waiters [2]list.List // [normal, priority] of *waiter
}

type waiter struct {
	ready   chan struct{}
	granted bool // set under mu when a slot is handed over
}

func newLimiter(n, reserved int) *limiter {
	if n <= 0 {
		return &limiter{}
	}
	return &limiter{n: n, reserved: min(max(reserved, 0), n-1)}
}

func (l *limiter) limit() int { return max(l.n, 0) }

// counts reports slots in use and waiters per class.
func (l *limiter) counts() (inUse, waiting, waitingPriority int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inUse, l.waiters[0].Len() + l.waiters[1].Len(), l.waiters[1].Len()
}

// free reports whether a request of the given class may take a slot now.
// Called with mu held.
func (l *limiter) free(priority bool) bool {
	if priority {
		return l.inUse < l.n
	}
	return l.inUse < l.n-l.reserved
}

func class(priority bool) int {
	if priority {
		return 1
	}
	return 0
}

func (l *limiter) releaser() func() {
	var once sync.Once
	return func() { once.Do(l.release) }
}

func (l *limiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inUse--
	l.handOver()
}

// handOver gives freed slots to waiters, priority class first. Called with mu held.
func (l *limiter) handOver() {
	for c := 1; c >= 0; c-- {
		for l.waiters[c].Len() > 0 && l.free(c == 1) {
			w := l.waiters[c].Remove(l.waiters[c].Front()).(*waiter)
			w.granted = true
			l.inUse++
			close(w.ready)
		}
	}
}

// acquire waits for a slot until ctx ends and reports how long it waited.
func (l *limiter) acquire(ctx context.Context, priority bool) (func(), time.Duration, error) {
	if l.n <= 0 {
		return func() {}, 0, nil
	}
	l.mu.Lock()
	if l.waiters[class(priority)].Len() == 0 && l.free(priority) {
		l.inUse++
		l.mu.Unlock()
		return l.releaser(), 0, nil
	}
	w := &waiter{ready: make(chan struct{})}
	e := l.waiters[class(priority)].PushBack(w)
	l.mu.Unlock()

	start := time.Now()
	select {
	case <-w.ready:
		return l.releaser(), time.Since(start), nil
	case <-ctx.Done():
		l.mu.Lock()
		defer l.mu.Unlock()
		if w.granted { // the slot arrived as the client gave up: pass it on
			l.inUse--
			l.handOver()
		} else {
			l.waiters[class(priority)].Remove(e)
		}
		return nil, time.Since(start), ctx.Err()
	}
}

// tryAcquire takes a slot only if one is free for normal requests right now.
func (l *limiter) tryAcquire() (func(), bool) {
	if l.n <= 0 {
		return func() {}, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.waiters[0].Len()+l.waiters[1].Len() > 0 || !l.free(false) {
		return nil, false
	}
	l.inUse++
	return l.releaser(), true
}

// limiters keeps one limiter per provider across config reloads; a provider
// gets a fresh one only when its limits change (holders of the old one still
// release into it, so nothing leaks).
type limiters struct {
	mu sync.Mutex
	m  map[string]*limiter
}

func (ls *limiters) get(p ProviderConfig) *limiter {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.m == nil {
		ls.m = make(map[string]*limiter)
	}
	want := newLimiter(p.MaxConcurrent, p.ReservedSlots)
	l, ok := ls.m[p.Name]
	if !ok || l.n != want.n || l.reserved != want.reserved {
		l = want
		ls.m[p.Name] = l
	}
	return l
}

// isPriority reports whether model matches one of the provider's priority globs.
func isPriority(p ProviderConfig, model string) bool {
	model = NormalizeModel(model)
	for _, g := range p.PriorityModels {
		if ok, _ := path.Match(g, model); ok { // globs are validated at load
			return true
		}
	}
	return false
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

// requestModelKey carries the client's model to takeSlot, for priority and logs.
type requestModelKey struct{}

func withRequestModel(ctx context.Context, model string) context.Context {
	return context.WithValue(ctx, requestModelKey{}, model)
}

func requestModel(ctx context.Context) string {
	m, _ := ctx.Value(requestModelKey{}).(string)
	return m
}

var errNoSlot = errors.New("provider concurrency limit reached")

// takeSlot acquires a slot for provider under the context's policy.
func (s *Server) takeSlot(ctx context.Context, p *provider) (func(), error) {
	lim := s.limits.get(p.cfg)
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
		release, _, err := lim.acquire(bctx, false)
		if err != nil {
			return nil, errNoSlot
		}
		return release, nil
	}
	model := requestModel(ctx)
	priority := isPriority(p.cfg, model)
	release, waited, err := lim.acquire(ctx, priority)
	if err != nil {
		return nil, err
	}
	if waited > 0 {
		s.log.Info("waited for a free provider slot", "provider", p.cfg.Name, "model", model,
			"priority", priority, "limit", lim.limit(), "waited", waited.Round(time.Millisecond))
	}
	return release, nil
}
