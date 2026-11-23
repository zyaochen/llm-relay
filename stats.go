package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// stats summarises the relay's log: per-model latency and slot waits, and
// per-provider failures. It reads the log rather than in-memory counters so
// it covers restarts and days of history.
func stats(args []string) error {
	fl := flag.NewFlagSet("stats", flag.ContinueOnError)
	since := fl.Duration("since", 24*time.Hour, "look back this far")
	logPath := fl.String("log", defaultLogPath(), "log file (rotated .1 .. .N are read too)")
	if err := fl.Parse(args); err != nil {
		return err
	}
	files := logFiles(*logPath)
	if len(files) == 0 {
		return fmt.Errorf("no log at %s", *logPath)
	}
	from := time.Now().Add(-*since)
	agg := newStatsAgg()
	for _, f := range files {
		if err := agg.readFile(f, from); err != nil {
			return err
		}
	}
	return agg.render(os.Stdout, *since)
}

// parseLogLine splits one slog text line into its key=value pairs.
func parseLogLine(line string) (map[string]string, error) {
	kv := make(map[string]string, 12)
	for i := 0; i < len(line); {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		eq := strings.IndexByte(line[i:], '=')
		if eq <= 0 {
			break
		}
		key := line[i : i+eq]
		i += eq + 1
		if i < len(line) && line[i] == '"' {
			end := i + 1
			for end < len(line) && line[end] != '"' {
				if line[end] == '\\' {
					end++
				}
				end++
			}
			if end >= len(line) {
				return nil, errors.New("unterminated quote")
			}
			v, err := strconv.Unquote(line[i : end+1])
			if err != nil {
				return nil, err
			}
			kv[key] = v
			i = end + 1
			continue
		}
		end := strings.IndexByte(line[i:], ' ')
		if end < 0 {
			end = len(line) - i
		}
		kv[key] = line[i : i+end]
		i += end
	}
	return kv, nil
}

type modelStats struct {
	ok, failed int
	ms         []float64 // request duration, successful requests
	waits      []float64 // seconds waited for a provider slot

	// token usage, from replies whose usage the relay read (Messages API)
	replies                        int
	in, out, cacheRead, cacheWrite int64
}

type providerStats struct {
	status       map[int]int
	failovers    int
	connRetries  int
	unreachable  int
	midstreamEnd int // stream lost before a block completed: client re-asks
	midstreamCut int // non-Messages stream aborted: client retries
}

type statsAgg struct {
	first, last time.Time
	models      map[string]*modelStats
	providers   map[string]*providerStats
	outages     int
	malformed   int
}

func newStatsAgg() *statsAgg {
	return &statsAgg{models: map[string]*modelStats{}, providers: map[string]*providerStats{}}
}

func (a *statsAgg) model(m string) *modelStats {
	if m == "" {
		m = "(unknown)"
	}
	if a.models[m] == nil {
		a.models[m] = &modelStats{}
	}
	return a.models[m]
}

func (a *statsAgg) provider(p string) *providerStats {
	if a.providers[p] == nil {
		a.providers[p] = &providerStats{status: map[int]int{}}
	}
	return a.providers[p]
}

func (a *statsAgg) readFile(path string, from time.Time) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		kv, err := parseLogLine(sc.Text())
		if err != nil {
			a.malformed++
			continue
		}
		t, err := time.Parse(time.RFC3339, kv["time"])
		if err != nil {
			a.malformed++
			continue
		}
		if t.Before(from) {
			continue
		}
		if a.first.IsZero() || t.Before(a.first) {
			a.first = t
		}
		if t.After(a.last) {
			a.last = t
		}
		a.add(kv)
	}
	return sc.Err()
}

func (a *statsAgg) add(kv map[string]string) {
	p := kv["provider"]
	switch msg := kv["msg"]; {
	case msg == "proxied":
		a.addProxied(kv)
	case msg == "waited for a free provider slot":
		if d, err := time.ParseDuration(kv["waited"]); err == nil {
			m := a.model(kv["model"])
			m.waits = append(m.waits, d.Seconds())
		}
	case msg == "failover":
		a.provider(kv["from"]).failovers++
	case msg == "connection error; retrying same provider":
		a.provider(p).connRetries++
	case msg == "upstream unreachable":
		a.provider(p).unreachable++
	case strings.HasPrefix(msg, "upstream lost mid-stream; ended early"):
		a.provider(p).midstreamEnd++
		a.model(kv["model"]).failed++
	case strings.HasPrefix(msg, "upstream lost mid-stream; aborting"):
		a.provider(p).midstreamCut++
		a.model(kv["model"]).failed++
	case msg == "network outage: holding request":
		a.outages++
	}
	if _, ok := kv["cache_read"]; ok {
		a.addTokens(kv)
	}
}

// addTokens counts a reply's usage; lost streams count too, their input was
// already processed upstream.
func (a *statsAgg) addTokens(kv map[string]string) {
	n := func(k string) int64 {
		v, _ := strconv.ParseInt(kv[k], 10, 64)
		return v
	}
	m := a.model(kv["model"])
	m.replies++
	m.in += n("in")
	m.out += n("out")
	m.cacheRead += n("cache_read")
	m.cacheWrite += n("cache_write")
}

func (a *statsAgg) addProxied(kv map[string]string) {
	status, err := strconv.Atoi(kv["status"])
	if err != nil {
		return
	}
	a.provider(kv["provider"]).status[status]++
	m := a.model(kv["model"])
	if status >= 400 {
		m.failed++
		return
	}
	m.ok++
	if ms, err := strconv.ParseFloat(kv["ms"], 64); err == nil {
		m.ms = append(m.ms, ms)
	}
}

// pct returns the q-quantile of v (sorted in place); 0 for an empty slice.
func pct(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	return v[min(int(float64(len(v))*q), len(v)-1)]
}

func sum(v []float64) float64 {
	var s float64
	for _, x := range v {
		s += x
	}
	return s
}

func (a *statsAgg) render(w io.Writer, since time.Duration) error {
	if a.first.IsZero() {
		_, err := fmt.Fprintf(w, "no log lines in the last %s\n", since)
		return err
	}
	fmt.Fprintf(w, "%s .. %s\n\n", a.first.Format("2006-01-02 15:04"), a.last.Format("2006-01-02 15:04"))
	if err := a.renderModels(w); err != nil {
		return err
	}
	fmt.Fprintln(w)
	if err := a.renderTokens(w); err != nil {
		return err
	}
	if err := a.renderProviders(w); err != nil {
		return err
	}
	fmt.Fprintln(w)
	a.renderPaidTwice(w)
	return nil
}

func (a *statsAgg) renderModels(w io.Writer) error {
	names := make([]string, 0, len(a.models))
	for n := range a.models {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		mi, mj := a.models[names[i]], a.models[names[j]]
		return mi.ok+mi.failed > mj.ok+mj.failed
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MODEL\tREQS\tFAILED\tP50\tP90\tSLOT WAITS\tWAIT P50\tWAIT P90\tWAITED")
	for _, n := range names {
		m := a.models[n]
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.1fs\t%.1fs\t%d\t%.1fs\t%.1fs\t%s\n", n, m.ok+m.failed, m.failed,
			pct(m.ms, .5)/1000, pct(m.ms, .9)/1000, len(m.waits), pct(m.waits, .5), pct(m.waits, .9),
			time.Duration(sum(m.waits)*float64(time.Second)).Round(time.Second))
	}
	return tw.Flush()
}

// renderTokens shows token usage per model. Cache hit is the share of prompt
// tokens served from the prompt cache; input excludes cached tokens, as in the
// Messages API usage.
func (a *statsAgg) renderTokens(w io.Writer) error {
	var names []string
	for n, m := range a.models {
		if m.replies > 0 {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Slice(names, func(i, j int) bool {
		mi, mj := a.models[names[i]], a.models[names[j]]
		return mi.in+mi.cacheRead+mi.cacheWrite > mj.in+mj.cacheRead+mj.cacheWrite
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MODEL\tREPLIES\tINPUT\tCACHE WRITE\tCACHE READ\tOUTPUT\tCACHE HIT")
	for _, n := range names {
		m := a.models[n]
		hit := "-"
		if prompt := m.in + m.cacheRead + m.cacheWrite; prompt > 0 {
			hit = fmt.Sprintf("%.0f%%", float64(m.cacheRead)*100/float64(prompt))
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n", n, m.replies, tokenCount(m.in),
			tokenCount(m.cacheWrite), tokenCount(m.cacheRead), tokenCount(m.out), hit)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

// tokenCount prints n as 950, 12.3k or 4.56M.
func tokenCount(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1e6)
	case n >= 1000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	}
	return strconv.FormatInt(n, 10)
}

func (a *statsAgg) renderProviders(w io.Writer) error {
	names := make([]string, 0, len(a.providers))
	for n := range a.providers {
		names = append(names, n)
	}
	sort.Strings(names)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PROVIDER\tOK\t429\t5XX\t(524)\tFAILOVERS\tCONN RETRIES\tUNREACHABLE\tLOST MID-STREAM")
	for _, n := range names {
		p := a.providers[n]
		var ok, c429, c5xx int
		for s, c := range p.status {
			switch {
			case s < 400:
				ok += c
			case s == 429:
				c429 += c
			case s >= 500:
				c5xx += c
			}
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", n, ok, c429, c5xx, p.status[statusOriginTimeout],
			p.failovers, p.connRetries, p.unreachable, p.midstreamEnd+p.midstreamCut)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if a.outages > 0 {
		fmt.Fprintf(w, "requests held through a network outage: %d\n", a.outages)
	}
	return nil
}

// renderPaidTwice counts answers the client asked for again after the upstream
// may already have done (and billed) the work.
func (a *statsAgg) renderPaidTwice(w io.Writer) {
	var lost, timeouts, retried int
	for _, p := range a.providers {
		lost += p.midstreamEnd + p.midstreamCut
		timeouts += p.status[statusOriginTimeout]
		retried += p.connRetries
	}
	fmt.Fprintf(w, "possibly billed twice: %d (lost mid-stream, re-asked: %d; origin timeouts 524, retried: %d)\n",
		lost+timeouts, lost, timeouts)
	if retried > 0 {
		fmt.Fprintf(w, "resent after a connection error (usually not billed, the request never arrived): %d\n", retried)
	}
	if a.malformed > 0 {
		fmt.Fprintf(w, "unparsed log lines: %d\n", a.malformed)
	}
}
