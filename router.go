package main

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
)

// Router maps a model id to the ordered providers that can serve it. The first
// candidate is preferred; the rest are failover targets for the same model.
// Order: providers of the first matching route, then every other provider that
// serves the model (discovered or static) in provider priority order, then —
// only if nothing serves it — the default provider.
type Router struct {
	routes          []RouteConfig
	order           []string
	static          map[string][]string
	defaultProvider string

	mu         sync.RWMutex
	discovered map[string][]string // provider -> model ids from /v1/models
	table      map[string][]string // model id -> providers serving it, priority order
}

func NewRouter(cfg *Config) *Router {
	r := &Router{
		routes:          cfg.Routes,
		static:          make(map[string][]string),
		defaultProvider: cfg.DefaultProvider,
		discovered:      make(map[string][]string),
	}
	for _, p := range cfg.Providers {
		r.order = append(r.order, p.Name)
		r.static[p.Name] = p.Models
	}
	r.rebuild()
	return r
}

// NormalizeModel strips client-side decorations such as Claude Code's "[1m]".
func NormalizeModel(model string) string {
	model = strings.TrimSpace(model)
	if i := strings.IndexByte(model, '['); i > 0 {
		model = model[:i]
	}
	return model
}

// Candidates returns the providers to try for model, best first.
func (r *Router) Candidates(model string) ([]string, error) {
	model = NormalizeModel(model)
	if model == "" {
		return nil, fmt.Errorf("request has no model")
	}
	r.mu.RLock()
	served := r.table[model]
	r.mu.RUnlock()
	if out := r.merge(model, served); len(out) > 0 {
		return out, nil
	}
	if r.defaultProvider != "" {
		return []string{r.defaultProvider}, nil
	}
	return nil, fmt.Errorf("no provider serves model %q (known: %s)", model, strings.Join(r.Models(), ", "))
}

// merge puts the first matching route's providers ahead of the serving ones.
func (r *Router) merge(model string, served []string) []string {
	var out []string
	seen := make(map[string]bool)
	add := func(ps []string) {
		for _, p := range ps {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	for _, rt := range r.routes {
		if ok, _ := path.Match(rt.Match, model); ok {
			add(rt.Providers)
			break
		}
	}
	add(served)
	return out
}

// SetDiscovered replaces the model list learned from one provider.
func (r *Router) SetDiscovered(provider string, ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.discovered[provider] = ids
	r.rebuildLocked()
}

// Table returns every known model with its candidate providers, best first.
func (r *Router) Table() map[string][]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string][]string, len(r.table))
	for m, served := range r.table {
		out[m] = r.merge(m, served)
	}
	return out
}

func (r *Router) Models() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.table))
	for m := range r.table {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func (r *Router) rebuild() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rebuildLocked()
}

func (r *Router) rebuildLocked() {
	table := make(map[string][]string)
	for _, p := range r.order {
		seen := make(map[string]bool)
		for _, ids := range [][]string{r.static[p], r.discovered[p]} {
			for _, id := range ids {
				if !seen[id] {
					seen[id] = true
					table[id] = append(table[id], p)
				}
			}
		}
	}
	r.table = table
}
