package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// HedgedDialer makes upstream TLS connections on lossy paths. A handshake
// that has not finished after HedgeAfter is raced by another one (next
// resolved IP), up to MaxParallel; the first to finish wins and the rest are
// closed. Losses on such paths are per attempt, so the race cuts the long
// tail (measured 2-14 s, ~8% never finishing) to roughly the fastest attempt.
type HedgedDialer struct {
	settings  func() NetworkConfig
	tlsConfig *tls.Config
	lookup    func(ctx context.Context, host string) ([]string, error)
	handshake func(ctx context.Context, ip, port, serverName string) (net.Conn, error)
	log       *slog.Logger // optional

	stats sync.Map // host -> *DialStats
	dns   sync.Map // host -> dnsEntry, last good answer
}

type dnsEntry struct {
	ips []string
	at  time.Time
}

const (
	staleDNSMax = 24 * time.Hour
	// freshDNSBudget bounds a lookup when a cached answer could be used
	// instead, so a hanging resolver does not eat the whole connect timeout.
	freshDNSBudget = 3 * time.Second
)

// resolve prefers a fresh lookup and falls back to the last good answer when
// DNS fails (seen: the resolver timing out for internal and external hosts
// alike while the gateways themselves may still be reachable).
func (d *HedgedDialer) resolve(ctx context.Context, host string) ([]string, error) {
	cached, haveCache := d.dns.Load(host)
	lookupCtx := ctx
	if haveCache {
		var cancel context.CancelFunc
		lookupCtx, cancel = context.WithTimeout(ctx, freshDNSBudget)
		defer cancel()
	}
	ips, err := d.lookup(lookupCtx, host)
	if err == nil && len(ips) > 0 {
		d.dns.Store(host, dnsEntry{ips: ips, at: time.Now()})
		return ips, nil
	}
	if err == nil {
		err = errors.New("no addresses")
	}
	if haveCache {
		if e := cached.(dnsEntry); time.Since(e.at) < staleDNSMax {
			d.Stats(host).StaleDNS.Add(1)
			if d.log != nil {
				d.log.Warn("DNS failed; using last good answer", "host", host,
					"age", time.Since(e.at).Round(time.Second), "err", err)
			}
			return e.ips, nil
		}
	}
	return nil, err
}

// DialStats counts dials per upstream host, for `llm-relay status`.
type DialStats struct {
	Dials        atomic.Int64 // connections established
	Failures     atomic.Int64 // dials where every attempt failed
	Hedges       atomic.Int64 // extra parallel attempts started
	AttemptFails atomic.Int64 // individual attempts that failed
	LastMs       atomic.Int64
	TotalMs      atomic.Int64
	StaleDNS     atomic.Int64 // dials that used a cached DNS answer because lookup failed
}

func NewHedgedDialer(settings func() NetworkConfig) *HedgedDialer {
	d := &HedgedDialer{
		settings:  settings,
		tlsConfig: &tls.Config{NextProtos: []string{"h2", "http/1.1"}, MinVersion: tls.VersionTLS12},
		lookup:    net.DefaultResolver.LookupHost,
	}
	d.handshake = d.tlsHandshake
	return d
}

func (d *HedgedDialer) Stats(host string) *DialStats {
	v, _ := d.stats.LoadOrStore(host, &DialStats{})
	return v.(*DialStats)
}

type dialResult struct {
	conn net.Conn
	err  error
}

// DialTLSContext has the signature http.Transport expects.
func (d *HedgedDialer) DialTLSContext(ctx context.Context, network, addr string) (net.Conn, error) {
	cfg := d.settings()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()

	ips := []string{host}
	if net.ParseIP(host) == nil {
		if ips, err = d.resolve(ctx, host); err != nil {
			return nil, fmt.Errorf("resolve %s: %w", host, err)
		}
	}

	stats := d.Stats(host)
	start := time.Now()
	attempts, cancelAttempts := context.WithCancel(ctx)
	defer cancelAttempts()
	results := make(chan dialResult, cfg.MaxParallel)
	started, failed := 0, 0
	launch := func() {
		ip := ips[started%len(ips)]
		started++
		go func() {
			conn, err := d.handshake(attempts, ip, port, host)
			results <- dialResult{conn, err}
		}()
	}

	launch()
	hedge := time.NewTimer(cfg.HedgeAfter)
	defer hedge.Stop()
	var firstErr error
	for {
		select {
		case r := <-results:
			if r.err == nil {
				cancelAttempts()
				go closeLosers(results, started-failed-1)
				ms := time.Since(start).Milliseconds()
				stats.Dials.Add(1)
				stats.LastMs.Store(ms)
				stats.TotalMs.Add(ms)
				return r.conn, nil
			}
			failed++
			stats.AttemptFails.Add(1)
			if firstErr == nil {
				firstErr = r.err
			}
			if failed < started {
				continue // others still running
			}
			if started < cfg.MaxParallel {
				launch() // everything failed fast; try the next address now
				stats.Hedges.Add(1)
				hedge.Reset(cfg.HedgeAfter)
				continue
			}
			stats.Failures.Add(1)
			return nil, fmt.Errorf("tls dial %s: all %d attempts failed: %w", addr, started, firstErr)
		case <-hedge.C:
			if started < cfg.MaxParallel {
				launch()
				stats.Hedges.Add(1)
				hedge.Reset(cfg.HedgeAfter)
			}
		case <-ctx.Done():
			go closeLosers(results, started-failed)
			stats.Failures.Add(1)
			return nil, fmt.Errorf("tls dial %s: no handshake finished within %s (%d attempts): %w",
				addr, cfg.ConnectTimeout, started, ctx.Err())
		}
	}
}

// closeLosers waits for attempts still in flight and closes any that
// completed after the race was decided.
func closeLosers(results <-chan dialResult, pending int) {
	for range pending {
		if r := <-results; r.conn != nil {
			r.conn.Close()
		}
	}
}

func (d *HedgedDialer) tlsHandshake(ctx context.Context, ip, port, serverName string) (net.Conn, error) {
	var nd net.Dialer
	raw, err := nd.DialContext(ctx, "tcp", net.JoinHostPort(ip, port))
	if err != nil {
		return nil, err
	}
	cfg := d.tlsConfig.Clone()
	cfg.ServerName = serverName
	conn := tls.Client(raw, cfg)
	if err := conn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	return conn, nil
}
