package main

import (
	"testing"
	"time"
)

func TestSummarize(t *testing.T) {
	if got := summarize(nil); got != "" {
		t.Errorf("empty = %q", got)
	}
	ts := []time.Duration{5 * time.Second, time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second}
	if got, want := summarize(ts), "p50  3.00s  p90  4.00s  max  5.00s"; got != want {
		t.Errorf("summarize = %q, want %q", got, want)
	}
}
