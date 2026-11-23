package main

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// stallAfter: a lane with requests in flight but no byte received for this
// long is treated as stalled (TCP retransmitting on a lossy path), and new
// requests avoid it. A var for tests.
var stallAfter = 5 * time.Second

// lane is one independent connection pool to a provider. HTTP/2 multiplexes
// every request onto a single connection, so one lost packet stalls all CLI
// sessions at once (head-of-line blocking); several lanes confine a stall to
// the requests on that lane and let new ones go elsewhere.
type lane struct {
	id       int
	client   *http.Client
	inflight atomic.Int32
	progress atomic.Int64 // unix nanos of the last byte (or request start)
}

func (l *lane) stalled(now time.Time) bool {
	return l.inflight.Load() > 0 && now.Sub(time.Unix(0, l.progress.Load())) > stallAfter
}

func (l *lane) begin() {
	l.inflight.Add(1)
	l.progress.Store(time.Now().UnixNano())
}

// laneBody records progress on every read and ends the request on Close.
type laneBody struct {
	io.ReadCloser
	l       *lane
	release func() // provider concurrency slot
	once    sync.Once
}

func (b *laneBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.l.progress.Store(time.Now().UnixNano())
	}
	return n, err
}

func (b *laneBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		b.l.inflight.Add(-1)
		b.release()
	})
	return err
}

// lanePool keeps each provider's lanes. Like Health it outlives config
// reloads, so warm connections survive an edit to the config file.
type lanePool struct {
	mu        sync.Mutex
	byName    map[string][]*lane
	newClient func() *http.Client
}

func newLanePool(newClient func() *http.Client) *lanePool {
	return &lanePool{byName: make(map[string][]*lane), newClient: newClient}
}

// get returns the provider's first n lanes, creating them as needed. Lanes
// beyond n (after lowering `connections`) have their idle connections closed.
func (lp *lanePool) get(provider string, n int) []*lane {
	lp.mu.Lock()
	defer lp.mu.Unlock()
	cur := lp.byName[provider]
	for len(cur) < n {
		cur = append(cur, &lane{id: len(cur), client: lp.newClient()})
	}
	if len(cur) > n {
		for _, l := range cur[n:] {
			l.client.CloseIdleConnections()
		}
		cur = cur[:n]
	}
	lp.byName[provider] = cur
	return cur
}

// pickLane prefers lanes that are not stalled, then the fewest requests in
// flight, then the lowest id — so a quiet provider uses one connection and the
// others stay warm as standbys. avoid skips a lane that just failed, if there
// is another.
func pickLane(lanes []*lane, avoid *lane, now time.Time) *lane {
	var best *lane
	for _, l := range lanes {
		if l == avoid && len(lanes) > 1 {
			continue
		}
		if best == nil {
			best = l
			continue
		}
		ls, bs := l.stalled(now), best.stalled(now)
		if ls != bs {
			if !ls {
				best = l
			}
			continue
		}
		if l.inflight.Load() < best.inflight.Load() {
			best = l
		}
	}
	return best
}

// LaneStatus is one lane as shown by `llm-relay status`.
type LaneStatus struct {
	Lane         int   `json:"lane"`
	InFlight     int32 `json:"in_flight"`
	Stalled      bool  `json:"stalled"`
	SinceByteSec int64 `json:"since_last_byte_s"`
}

func (s *Server) laneSummary(st *state) map[string][]LaneStatus {
	now := time.Now()
	out := make(map[string][]LaneStatus)
	for _, name := range st.cfg.providerNames() {
		for _, l := range s.lanes.get(name, st.cfg.Network.Connections) {
			out[name] = append(out[name], LaneStatus{
				Lane: l.id, InFlight: l.inflight.Load(), Stalled: l.stalled(now),
				SinceByteSec: int64(now.Sub(time.Unix(0, l.progress.Load())).Seconds()),
			})
		}
	}
	return out
}

func (s *Server) laneFor(p *provider, avoid *lane) *lane {
	n := s.st.Load().cfg.Network.Connections
	return pickLane(s.lanes.get(p.cfg.Name, n), avoid, time.Now())
}
