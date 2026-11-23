package main

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseLogLineMatchesSlogOutput(t *testing.T) {
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("upstream lost mid-stream; ended early",
		"model", "claude-opus-5-5", "err", `read: "reset" by peer`, "ms", 1234)
	kv, err := parseLogLine(strings.TrimSpace(buf.String()))
	if err != nil {
		t.Fatal(err)
	}
	if kv["msg"] != "upstream lost mid-stream; ended early" || kv["model"] != "claude-opus-5-5" ||
		kv["err"] != `read: "reset" by peer` || kv["ms"] != "1234" || kv["level"] != "INFO" {
		t.Fatalf("parsed %v", kv)
	}
	if _, err := parseLogLine(`msg="unterminated`); err == nil {
		t.Error("an unterminated quote must be reported")
	}
}

func TestStatsAggregatesTheLog(t *testing.T) {
	now := time.Now()
	ts := func(ago time.Duration) string { return now.Add(-ago).Format(time.RFC3339Nano) }
	lines := []string{
		fmt.Sprintf(`time=%s level=INFO msg=proxied path=/v1/messages model=old provider=a status=200 ms=1`, ts(48*time.Hour)),
		fmt.Sprintf(`time=%s level=INFO msg=proxied path=/v1/messages model=big provider=a status=200 ms=1000`, ts(time.Hour)),
		fmt.Sprintf(`time=%s level=INFO msg=proxied path=/v1/messages model=big provider=a status=200 ms=3000`, ts(time.Hour)),
		fmt.Sprintf(`time=%s level=INFO msg=proxied path=/v1/messages model=big provider=a status=524 ms=190000`, ts(time.Hour)),
		fmt.Sprintf(`time=%s level=INFO msg="waited for a free provider slot" provider=a model=small priority=false limit=4 waited=1m30s`, ts(time.Hour)),
		fmt.Sprintf(`time=%s level=WARN msg="upstream lost mid-stream; ended early so the client re-asks without streaming" model=small provider=a status=200 ms=5`, ts(time.Hour)),
		fmt.Sprintf(`time=%s level=WARN msg="connection error; retrying same provider" provider=a retry=1`, ts(time.Hour)),
		`garbage "line`,
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "relay.log")
	writeFile(t, path, strings.Join(lines, "\n")+"\n")

	agg := newStatsAgg()
	if err := agg.readFile(path, now.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if agg.models["old"] != nil {
		t.Error("lines before -since must be skipped")
	}
	big, small := agg.models["big"], agg.models["small"]
	if big.ok != 2 || big.failed != 1 || pct(big.ms, .5) != 3000 {
		t.Errorf("big: %+v", big)
	}
	if small.failed != 1 || len(small.waits) != 1 || small.waits[0] != 90 {
		t.Errorf("small: %+v", small)
	}
	if p := agg.providers["a"]; p.status[statusOriginTimeout] != 1 || p.midstreamEnd != 1 || p.connRetries != 1 {
		t.Errorf("provider a: %+v", p)
	}
	var out bytes.Buffer
	if err := agg.render(&out, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "possibly billed twice: 2 (lost mid-stream, re-asked: 1; origin timeouts 524, retried: 1)") ||
		!strings.Contains(out.String(), "unparsed log lines: 1") {
		t.Errorf("render:\n%s", out.String())
	}
}

func TestRotatingFileKeepsNFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "relay.log")
	r, err := openRotating(path, 100, 2)
	if err != nil {
		t.Fatal(err)
	}
	line := strings.Repeat("x", 59) + "\n" // 60 bytes: one per file
	for i := 0; i < 5; i++ {
		if _, err := io.WriteString(r, line); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{path, path + ".1", path + ".2"} {
		b, err := os.ReadFile(f)
		if err != nil || string(b) != line {
			t.Errorf("%s: %q %v", f, b, err)
		}
	}
	if _, err := os.Stat(path + ".3"); err == nil {
		t.Error("only keep rotations may exist")
	}
	r2, err := openRotating(path, 100, 2) // reopen appends and counts the existing size
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.size != int64(len(line)) {
		t.Errorf("size after reopen = %d", r2.size)
	}
	if got := logFiles(path); len(got) != 3 || got[0] != path+".2" || got[2] != path {
		// logFiles looks for up to logKeep rotations, oldest first
		t.Errorf("logFiles = %v", got)
	}
}
