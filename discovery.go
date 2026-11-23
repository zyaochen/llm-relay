package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"
)

// Model lists are polled every discoveryInterval. A week of logs had ~100
// "discovery failed" warnings: half were a busy provider (all slots held by
// streams, so the poll was skipped on purpose), half a lossy network. Both
// now get an early retry instead of a 10-minute gap, and a failure streak
// logs one warning plus one line when it recovers.
const (
	discoveryInterval  = 10 * time.Minute
	discoveryTick      = time.Minute // how often due providers are checked
	discoveryTimeout   = 30 * time.Second
	discoveryBusyRetry = time.Minute
)

// discoveryBackoff is the wait after the n-th failure in a row (n >= 1).
func discoveryBackoff(n int) time.Duration {
	return min(time.Minute<<min(n-1, 4), discoveryInterval)
}

type discoveryState struct {
	next     time.Time
	failures int
	models   string // sorted ids, so only changes are logged
}

type discoveries struct {
	mu sync.Mutex
	m  map[string]*discoveryState
}

func (d *discoveries) get(provider string) *discoveryState {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.m == nil {
		d.m = make(map[string]*discoveryState)
	}
	if d.m[provider] == nil {
		d.m[provider] = &discoveryState{}
	}
	return d.m[provider]
}

// discover polls every provider that is due (all of them when force is set,
// e.g. after a config load) and updates the router.
func (s *Server) discover(st *state, force bool) {
	var wg sync.WaitGroup
	now := time.Now()
	for _, p := range st.providers {
		if !p.cfg.discoverEnabled() {
			continue
		}
		ds := s.discovery.get(p.cfg.Name)
		s.discovery.mu.Lock()
		due := force || !now.Before(ds.next)
		s.discovery.mu.Unlock()
		if !due {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.discoverOne(st, p, ds)
		}()
	}
	wg.Wait()
}

func (s *Server) discoverOne(st *state, p *provider, ds *discoveryState) {
	ctx, cancel := context.WithTimeout(context.Background(), discoveryTimeout)
	defer cancel()
	_, ids, err := s.fetchModels(ctx, p, "/v1/models")
	s.discovery.mu.Lock()
	defer s.discovery.mu.Unlock()
	name := p.cfg.Name
	switch {
	case errors.Is(err, errNoSlot):
		// Every slot is serving a client; that is not a failure.
		ds.next = time.Now().Add(discoveryBusyRetry)
		s.log.Debug("model discovery skipped: provider busy", "provider", name)
	case err != nil:
		ds.failures++
		wait := discoveryBackoff(ds.failures)
		ds.next = time.Now().Add(wait)
		if ds.failures == 1 {
			s.log.Warn("model discovery failed; keeping previous list", "provider", name, "err", err, "retry_in", wait)
		} else {
			s.log.Debug("model discovery still failing", "provider", name, "failures", ds.failures, "err", err)
		}
	default:
		st.router.SetDiscovered(name, ids)
		if ds.failures > 0 {
			s.log.Info("model discovery recovered", "provider", name, "failures", ds.failures)
		}
		sorted := slices.Sorted(slices.Values(ids))
		if key := strings.Join(sorted, "\n"); key != ds.models {
			ds.models = key
			s.log.Info("models discovered", "provider", name, "count", len(ids))
		}
		ds.failures = 0
		ds.next = time.Now().Add(discoveryInterval)
	}
}
