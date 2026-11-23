package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"time"
)

// probe compares fresh single-attempt handshakes with hedged ones against one
// provider, to show what the network path looks like and what hedging buys.
func probe(configPath string, args []string) error {
	fl := flag.NewFlagSet("probe", flag.ContinueOnError)
	n := fl.Int("n", 20, "handshakes per mode")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if fl.NArg() != 1 {
		return fmt.Errorf("usage: llm-relay probe [-n 20] <provider>")
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	var target *ProviderConfig
	for i := range cfg.Providers {
		if cfg.Providers[i].Name == fl.Arg(0) {
			target = &cfg.Providers[i]
		}
	}
	if target == nil {
		return fmt.Errorf("unknown provider %q", fl.Arg(0))
	}
	u, err := url.Parse(target.BaseURL)
	if err != nil {
		return err
	}
	if u.Scheme != "https" {
		return fmt.Errorf("provider %s is not https; nothing to probe", target.Name)
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(u.Hostname(), port)

	single := cfg.Network
	single.MaxParallel = 1
	modes := []struct {
		name string
		net  NetworkConfig
	}{{"single attempt", single}, {"hedged", cfg.Network}}

	fmt.Printf("probing %s (%s): %d fresh TLS handshakes per mode, connect_timeout %s, hedge_after %s x%d\n",
		target.Name, addr, *n, cfg.Network.ConnectTimeout, cfg.Network.HedgeAfter, cfg.Network.MaxParallel)
	for _, m := range modes {
		d := NewHedgedDialer(func() NetworkConfig { return m.net })
		times, fails := runProbe(d, addr, *n)
		fmt.Fprintf(os.Stdout, "  %-15s ok %2d/%d  %s\n", m.name, len(times), *n, summarize(times))
		if fails > 0 {
			fmt.Fprintf(os.Stdout, "  %-15s %d failed to connect within %s\n", "", fails, m.net.ConnectTimeout)
		}
	}
	return nil
}

// runProbe dials n times one after another, like the relay does when it needs
// a new connection; parallel probes would congest the path and skew results.
func runProbe(d *HedgedDialer, addr string, n int) ([]time.Duration, int) {
	var times []time.Duration
	fails := 0
	for range n {
		start := time.Now()
		conn, err := d.DialTLSContext(context.Background(), "tcp", addr)
		if err != nil {
			fails++
			continue
		}
		times = append(times, time.Since(start))
		conn.Close()
	}
	return times, fails
}

func summarize(times []time.Duration) string {
	if len(times) == 0 {
		return ""
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	pct := func(p float64) time.Duration { return times[int(p*float64(len(times)-1))] }
	return fmt.Sprintf("p50 %5.2fs  p90 %5.2fs  max %5.2fs",
		pct(0.5).Seconds(), pct(0.9).Seconds(), times[len(times)-1].Seconds())
}
