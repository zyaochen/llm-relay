package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// drainTimeout bounds how long a restart waits for in-flight requests; the
// launchd plist's ExitTimeOut must be longer. A var for tests.
var drainTimeout = 90 * time.Second

const (
	configPollInterval = 2 * time.Second
	discoveryInterval  = 10 * time.Minute
	discoveryTimeout   = 30 * time.Second
)

type provider struct {
	cfg  ProviderConfig
	cred *Credential
}

// state is everything derived from one config file; reloads swap it atomically
// so in-flight requests keep the state they started with.
type state struct {
	cfg       *Config
	providers map[string]*provider
	router    *Router
	clientKey []byte
}

type Server struct {
	configPath string
	log        *slog.Logger
	lanes      *lanePool
	st         atomic.Pointer[state]
	health     *Health // outlives config reloads on purpose
	dialer     *HedgedDialer
	activity   activity
	active     atomic.Int32 // client requests being proxied right now
	limits     limiters     // per-provider concurrency, outlives reloads
	// Network view for status: when an upstream last answered, and since when
	// no provider has been reachable at all (0 = not in an outage).
	lastUpstreamOK atomic.Int64
	outageSince    atomic.Int64

	reloadMu sync.Mutex
	modTime  time.Time
}

func NewServer(configPath string, log *slog.Logger) (*Server, error) {
	s := &Server{configPath: configPath, log: log, health: NewHealth()}
	// Read per dial, so network settings hot-reload like everything else.
	s.dialer = NewHedgedDialer(func() NetworkConfig {
		if st := s.st.Load(); st != nil {
			return st.cfg.Network
		}
		return defaultNetwork()
	})
	s.dialer.log = log
	// Each lane gets its own Transport, i.e. its own connection pool.
	s.lanes = newLanePool(func() *http.Client {
		return &http.Client{
			// Streams can legitimately run for many minutes; no overall timeout.
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialTLSContext:        s.dialer.DialTLSContext, // direct HTTPS only; proxied requests use DialContext
				DisableCompression:    true,                    // pass gzip bytes through untouched
				ForceAttemptHTTP2:     true,
				MaxIdleConnsPerHost:   16,
				IdleConnTimeout:       15 * time.Minute, // a warm connection is worth keeping
				ResponseHeaderTimeout: 10 * time.Minute,
				// Ping HTTP/2 connections quiet for 45s so a dead one is dropped
				// before a request is put on it. Keep-warm (30s) normally keeps them
				// from going quiet: on a lossy path a lost pong would otherwise close
				// a healthy connection and the next request would pay a handshake.
				HTTP2: &http.HTTP2Config{SendPingTimeout: 45 * time.Second, PingTimeout: 20 * time.Second},
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	})
	if err := s.Reload(); err != nil {
		return nil, err
	}
	return s, nil
}

func buildState(cfg *Config) (*state, error) {
	st := &state{cfg: cfg, providers: make(map[string]*provider), router: NewRouter(cfg)}
	for _, p := range cfg.Providers {
		st.providers[p.Name] = &provider{cfg: p, cred: NewCredential(p.Auth)}
	}
	if cfg.ClientKeyFile != "" {
		raw, err := os.ReadFile(cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("read client key: %w", err)
		}
		st.clientKey = []byte(strings.TrimSpace(string(raw)))
		if len(st.clientKey) == 0 {
			return nil, fmt.Errorf("client key file %s is empty", cfg.ClientKeyFile)
		}
	}
	return st, nil
}

// Reload re-reads the config. On any error the running state is kept.
func (s *Server) Reload() error {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	info, err := os.Stat(s.configPath)
	if err != nil {
		return fmt.Errorf("stat config: %w", err)
	}
	cfg, err := LoadConfig(s.configPath)
	if err != nil {
		return err
	}
	st, err := buildState(cfg)
	if err != nil {
		return err
	}
	if old := s.st.Load(); old != nil && old.cfg.Listen != cfg.Listen {
		s.log.Warn("listen address changes need a restart", "running", old.cfg.Listen, "configured", cfg.Listen)
	}
	s.modTime = info.ModTime()
	s.st.Store(st)
	go s.discover(st)
	s.log.Info("config loaded", "path", s.configPath, "providers", len(cfg.Providers), "routes", len(cfg.Routes))
	return nil
}

func (s *Server) reloadIfChanged() {
	info, err := os.Stat(s.configPath)
	if err != nil {
		s.log.Error("config stat failed; keeping running config", "err", err)
		return
	}
	s.reloadMu.Lock()
	changed := !info.ModTime().Equal(s.modTime)
	s.reloadMu.Unlock()
	if !changed {
		return
	}
	if err := s.Reload(); err != nil {
		s.log.Error("config reload rejected; keeping running config", "err", err)
		s.reloadMu.Lock()
		s.modTime = info.ModTime() // don't re-log the same broken file every poll
		s.reloadMu.Unlock()
	}
}

func (s *Server) discover(st *state) {
	var wg sync.WaitGroup
	for _, p := range st.providers {
		if !p.cfg.discoverEnabled() {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), discoveryTimeout)
			defer cancel()
			_, ids, err := s.fetchModels(ctx, p, "/v1/models")
			if err != nil {
				s.log.Warn("model discovery failed; keeping previous list", "provider", p.cfg.Name, "err", err)
				return
			}
			st.router.SetDiscovered(p.cfg.Name, ids)
			s.log.Info("models discovered", "provider", p.cfg.Name, "count", len(ids))
		}()
	}
	wg.Wait()
}

// fetchModels GETs a provider's model list and returns raw entries plus ids.
func (s *Server) fetchModels(ctx context.Context, p *provider, uri string) ([]json.RawMessage, []string, error) {
	// Don't queue /model behind long streams: wait briefly for a slot, else skip.
	return s.fetchModelsOn(withSlotPolicy(ctx, slotBrief), s.laneFor(p, nil), p, uri)
}

// fetchModelsOn is fetchModels on a given lane; it reads the whole body, so
// the connection goes back to that lane's pool.
func (s *Server) fetchModelsOn(ctx context.Context, l *lane, p *provider, uri string) ([]json.RawMessage, []string, error) {
	resp, err := s.send(ctx, l, p, http.MethodGet, uri, http.Header{"Accept-Encoding": {"identity"}}, nil, false)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("read models: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("models: upstream status %d", resp.StatusCode)
	}
	var list struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, nil, fmt.Errorf("decode models: %w", err)
	}
	ids := make([]string, 0, len(list.Data))
	for _, raw := range list.Data {
		var m struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &m) == nil && m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	return list.Data, ids, nil
}

// Run serves until ctx is cancelled, hot-reloading on config change or SIGHUP.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{Addr: s.st.Load().cfg.Listen, Handler: s, ReadHeaderTimeout: 10 * time.Second}

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go s.keepWarm(ctx)
	go func() {
		poll := time.NewTicker(configPollInterval)
		disc := time.NewTicker(discoveryInterval)
		defer poll.Stop()
		defer disc.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-poll.C:
				s.reloadIfChanged()
			case <-hup:
				if err := s.Reload(); err != nil {
					s.log.Error("SIGHUP reload rejected; keeping running config", "err", err)
				}
			case <-disc.C:
				s.discover(s.st.Load())
			}
		}
	}()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		// Stop accepting and let in-flight streams finish, so a restart does not
		// cut a CLI mid-answer ("socket connection was closed unexpectedly").
		s.log.Info("draining", "active_requests", s.active.Load(), "max", drainTimeout)
		shutdown, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			s.log.Error("drain timed out; cutting remaining requests", "active_requests", s.active.Load(), "err", err)
			return
		}
		s.log.Info("drained")
	}()

	s.log.Info("listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-drained // ListenAndServe returns as soon as Shutdown starts; wait for the drain
	return nil
}
