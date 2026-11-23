package main

import (
	"context"
	"net/http/httptrace"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const warmTimeout = 30 * time.Second

// warmCheckInterval is how often idle providers are looked at; a var for tests.
var warmCheckInterval = 5 * time.Second

// activity tracks when each provider last carried traffic, so idle ones can be
// kept warm and busy ones are left alone.
type activity struct {
	last    sync.Map // provider -> *atomic.Int64 (unix nanos)
	warming sync.Map // provider -> *atomic.Bool
}

func (a *activity) touch(provider string) {
	v, _ := a.last.LoadOrStore(provider, new(atomic.Int64))
	v.(*atomic.Int64).Store(time.Now().UnixNano())
}

func (a *activity) idleFor(provider string) time.Duration {
	v, ok := a.last.Load(provider)
	if !ok {
		return time.Duration(1<<63 - 1) // never used: warm it right away
	}
	return time.Since(time.Unix(0, v.(*atomic.Int64).Load()))
}

func (a *activity) claim(provider string) (release func(), ok bool) {
	v, _ := a.warming.LoadOrStore(provider, new(atomic.Bool))
	flag := v.(*atomic.Bool)
	if !flag.CompareAndSwap(false, true) {
		return nil, false
	}
	return func() { flag.Store(false) }, true
}

func laneKey(provider string, id int) string { return provider + "#" + strconv.Itoa(id) }

// keepWarm sends a cheap request on every lane that has been idle for
// network.keep_warm, so a CLI request finds an established connection on any
// lane instead of paying a TLS handshake (seconds, sometimes failing, on
// lossy paths).
func (s *Server) keepWarm(ctx context.Context) {
	tick := time.NewTicker(warmCheckInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		st := s.st.Load()
		every := *st.cfg.Network.KeepWarm
		if every == 0 {
			continue
		}
		for _, p := range st.providers {
			for _, l := range s.lanes.get(p.cfg.Name, st.cfg.Network.Connections) {
				key := laneKey(p.cfg.Name, l.id)
				if s.activity.idleFor(key) < every {
					continue
				}
				if release, ok := s.activity.claim(key); ok {
					go func() {
						defer release()
						s.warm(ctx, l, p)
					}()
				}
			}
		}
	}
}

func (s *Server) warm(ctx context.Context, l *lane, p *provider) {
	ctx, cancel := context.WithTimeout(ctx, warmTimeout)
	defer cancel()
	var reused bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
	})
	// Never take a slot a client could use; if all are busy the connections are warm anyway.
	_, ids, err := s.fetchModelsOn(withSlotPolicy(ctx, slotTry), l, p, "/v1/models")
	if err != nil {
		s.log.Debug("keep-warm failed", "provider", p.cfg.Name, "lane", l.id, "reused_conn", reused, "err", err)
		return
	}
	s.log.Debug("keep-warm", "provider", p.cfg.Name, "lane", l.id, "reused_conn", reused, "models", len(ids))
	// Doubles as discovery, so a provider that was down when the relay started
	// gets its models within one keep-warm period instead of the next 10-minute
	// discovery round.
	if p.cfg.discoverEnabled() {
		s.st.Load().router.SetDiscovered(p.cfg.Name, ids)
	}
}
