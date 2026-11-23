package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testAgentSpec() agentSpec {
	return agentSpec{bin: "/opt/my tools/llm-relay", config: "/home/u/.config/llm-relay/config.yaml",
		logFile: "/home/u/.local/state/llm-relay/llm-relay.log", stderrFile: "/home/u/.local/state/llm-relay/llm-relay.stderr.log",
		home: "/home/u", path: "/home/u/.local/bin:/usr/bin"}
}

func TestSystemdUnitQuotesAndPointsTheLog(t *testing.T) {
	unit := systemdUnit(testAgentSpec())
	for _, want := range []string{
		`ExecStart="/opt/my tools/llm-relay" -config "/home/u/.config/llm-relay/config.yaml" serve`,
		`Environment="LLM_RELAY_LOG=/home/u/.local/state/llm-relay/llm-relay.log"`,
		`Restart=always`, `TimeoutStopSec=100`, `WantedBy=default.target`,
	} {
		if !strings.Contains(unit, want) {
			t.Errorf("unit lacks %q:\n%s", want, unit)
		}
	}
	if got := systemdQuote(`a "b" 100% \x`); got != `"a \"b\" 100%% \\x"` {
		t.Errorf("systemdQuote = %s", got)
	}
}

func TestLaunchdPlistSendsLogToTheRelay(t *testing.T) {
	plist := launchdPlist(testAgentSpec())
	if !strings.Contains(plist, "<key>LLM_RELAY_LOG</key><string>/home/u/.local/state/llm-relay/llm-relay.log</string>") ||
		!strings.Contains(plist, "<key>StandardErrorPath</key><string>/home/u/.local/state/llm-relay/llm-relay.stderr.log</string>") {
		t.Errorf("plist:\n%s", plist)
	}
}

func TestSystemdUnitPathHonoursXDG(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/x/cfg")
	if got := systemdUnitPath("/home/u"); got != filepath.Join("/x/cfg", "systemd", "user", systemdUnitName) {
		t.Errorf("got %s", got)
	}
	t.Setenv("XDG_CONFIG_HOME", "")
	if got := systemdUnitPath("/home/u"); got != "/home/u/.config/systemd/user/"+systemdUnitName {
		t.Errorf("got %s", got)
	}
}

func TestWaitUntil(t *testing.T) {
	n := 0
	if !waitUntil(2*time.Second, func() bool { n++; return n == 3 }) {
		t.Fatal("condition met on the third poll should succeed")
	}
	if waitUntil(300*time.Millisecond, func() bool { return false }) {
		t.Fatal("a condition that never holds must time out")
	}
}

func TestAgentLogAppendsToTheRelayLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "relay.log")
	orig := agentLogPath
	agentLogPath = func() string { return path }
	t.Cleanup(func() { agentLogPath = orig })
	agentLog(slog.LevelWarn, "agent reload: bootstrap failed", "attempt", 2, "out", "5: Input/output error")
	agentLog(slog.LevelInfo, "agent reload: bootstrapped", "attempt", 3)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %q", raw)
	}
	kv, err := parseLogLine(lines[0])
	if err != nil {
		t.Fatal(err)
	}
	if kv["level"] != "WARN" || kv["msg"] != "agent reload: bootstrap failed" || kv["out"] != "5: Input/output error" ||
		kv["source"] != "cli" || kv["time"] == "" {
		t.Errorf("parsed %v", kv)
	}
}
