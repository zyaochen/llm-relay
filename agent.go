package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// The relay runs as a per-user background service: a launchd agent on macOS,
// a systemd user unit on Linux. Both restart it on crash, start it at login
// and point its log at defaultLogPath (rotated by the relay itself).

const systemdUnitName = "llm-relay.service"

// agentSpec is what either service manager needs to run `llm-relay serve`.
type agentSpec struct {
	bin, config, logFile, stderrFile, home, path string
}

func newAgentSpec(configPath string) (agentSpec, error) {
	bin, err := os.Executable()
	if err != nil {
		return agentSpec{}, err
	}
	if bin, err = filepath.EvalSymlinks(bin); err != nil {
		return agentSpec{}, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return agentSpec{}, err
	}
	absConfig, err := filepath.Abs(configPath)
	if err != nil {
		return agentSpec{}, err
	}
	logFile := defaultLogPath()
	path := strings.Join([]string{filepath.Join(home, ".local/bin"), "/opt/homebrew/bin",
		"/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"}, ":")
	return agentSpec{bin: bin, config: absConfig, logFile: logFile,
		stderrFile: strings.TrimSuffix(logFile, ".log") + ".stderr.log", home: home, path: path}, nil
}

const plistTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>%s</string>
  <key>ProgramArguments</key>
  <array><string>%s</string><string>-config</string><string>%s</string><string>serve</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>5</integer>
  <key>ExitTimeOut</key><integer>100</integer>
  <key>StandardOutPath</key><string>%s</string>
  <key>StandardErrorPath</key><string>%s</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>%s</string>
    <key>HOME</key><string>%s</string>
    <key>LLM_RELAY_LOG</key><string>%s</string>
  </dict>
</dict>
</plist>
`

// launchdPlist renders the agent definition. The relay writes (and rotates)
// its own log; launchd's stdout/stderr file only catches crashes.
func launchdPlist(s agentSpec) string {
	return fmt.Sprintf(plistTemplate, agentLabel, s.bin, s.config, s.stderrFile, s.stderrFile, s.path, s.home, s.logFile)
}

const unitTemplate = `[Unit]
Description=llm-relay: one local base URL for every LLM gateway
After=network-online.target

[Service]
ExecStart=%s -config %s serve
Restart=always
RestartSec=5
# Above the relay's 90s drain, so a restart lets streams finish.
TimeoutStopSec=100
Environment=%s
Environment=%s

[Install]
WantedBy=default.target
`

// systemdQuote quotes one word for a unit file (spaces, quotes and
// backslashes survive; % is systemd's specifier character).
func systemdQuote(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `%`, `%%`)
	return `"` + r.Replace(s) + `"`
}

func systemdUnit(s agentSpec) string {
	return fmt.Sprintf(unitTemplate, systemdQuote(s.bin), systemdQuote(s.config),
		systemdQuote("LLM_RELAY_LOG="+s.logFile), systemdQuote("PATH="+s.path))
}

func systemdUnitPath(home string) string {
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "systemd", "user", systemdUnitName)
	}
	return filepath.Join(home, ".config", "systemd", "user", systemdUnitName)
}

// installAgent writes the service definition and (re)starts it. Reloading a
// running agent cuts its streams, so it first waits for an idle moment like
// restart does.
func installAgent(configPath string, args []string) error {
	fl := flag.NewFlagSet("install-agent", flag.ContinueOnError)
	wait := fl.Duration("wait", 2*time.Minute, "how long to wait for an idle moment")
	force := fl.Bool("force", false, "reinstall even if still busy after -wait")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if _, err := LoadConfig(configPath); err != nil {
		return fmt.Errorf("refusing to install with a broken config: %w", err)
	}
	spec, err := newAgentSpec(configPath)
	if err != nil {
		return err
	}
	if idle, err := waitIdle(configPath, *wait, 500*time.Millisecond); err == nil && !idle && !*force {
		return fmt.Errorf("relay still busy after %s; not reinstalling (try again, raise -wait, or add -force)", *wait)
	} // err: relay not running, nothing to cut
	if err := os.MkdirAll(filepath.Dir(spec.logFile), 0o700); err != nil {
		return err
	}
	switch runtime.GOOS {
	case "darwin":
		err = installLaunchd(spec)
	case "linux":
		err = installSystemd(spec)
	default:
		err = fmt.Errorf("install-agent supports macOS (launchd) and Linux (systemd), not %s; run `llm-relay serve` under your own supervisor", runtime.GOOS)
	}
	if err != nil {
		return err
	}
	healthy := func() bool { return call(configPath, "GET", "/relay/health", func([]byte) error { return nil }) == nil }
	if !waitUntil(agentWait, healthy) {
		return fmt.Errorf("agent installed but the relay is not answering after %s; check %s and run `%s`",
			agentWait, spec.stderrFile, agentHint())
	}
	fmt.Printf("relay is answering; logs: %s (crashes: %s)\n", spec.logFile, spec.stderrFile)
	return nil
}

func installLaunchd(spec agentSpec) error {
	plistPath := filepath.Join(spec.home, "Library/LaunchAgents", agentLabel+".plist")
	if err := os.WriteFile(plistPath, []byte(launchdPlist(spec)), 0o644); err != nil {
		return err
	}
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	target := domain + "/" + agentLabel
	// bootout fails when the agent is not loaded yet; that is the expected first-install case.
	if out, err := exec.Command("launchctl", "bootout", target).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "launchctl bootout (ok on first install): %s", out)
	}
	// bootout returns before the job is gone; bootstrapping then fails with
	// "5: Input/output error" and leaves no agent at all (seen: 1h43m without
	// a relay). Wait for the old job to disappear, and retry the bootstrap.
	if !waitUntil(agentWait, func() bool { return exec.Command("launchctl", "print", target).Run() != nil }) {
		fmt.Fprintf(os.Stderr, "launchctl: %s still loaded after %s; bootstrapping anyway\n", target, agentWait)
	}
	var out []byte
	var err error
	for attempt := 1; attempt <= agentBootstrapTries; attempt++ {
		if out, err = exec.Command("launchctl", "bootstrap", domain, plistPath).CombinedOutput(); err == nil {
			break
		}
		time.Sleep(agentRetryPause)
	}
	if err != nil {
		return fmt.Errorf("launchctl bootstrap failed %d times, the relay is NOT running; retry with "+
			"`launchctl bootstrap %s %s`: %w: %s", agentBootstrapTries, domain, plistPath, err, out)
	}
	fmt.Println("installed", plistPath)
	return nil
}

// Pacing for reloading an agent; vars for tests.
var (
	agentWait           = 15 * time.Second
	agentRetryPause     = time.Second
	agentBootstrapTries = 5
)

// waitUntil polls cond every 200ms until it holds or d passes.
func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func installSystemd(spec agentSpec) error {
	unitPath := systemdUnitPath(spec.home)
	if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(unitPath, []byte(systemdUnit(spec)), 0o644); err != nil {
		return err
	}
	for _, args := range [][]string{{"daemon-reload"}, {"enable", systemdUnitName}, {"restart", systemdUnitName}} {
		if out, err := exec.Command("systemctl", append([]string{"--user"}, args...)...).CombinedOutput(); err != nil {
			return fmt.Errorf("systemctl --user %s: %w: %s", strings.Join(args, " "), err, out)
		}
	}
	fmt.Println("installed", unitPath, "(to keep it running after logout: loginctl enable-linger)")
	return nil
}

// agentCommand is the service manager call that starts (restart false) or
// restarts the installed agent.
func agentCommand(restart bool) (*exec.Cmd, error) {
	switch runtime.GOOS {
	case "darwin":
		target := fmt.Sprintf("gui/%d/%s", os.Getuid(), agentLabel)
		if restart {
			return exec.Command("launchctl", "kickstart", "-k", target), nil
		}
		return exec.Command("launchctl", "kickstart", target), nil
	case "linux":
		verb := "start"
		if restart {
			verb = "restart"
		}
		return exec.Command("systemctl", "--user", verb, systemdUnitName), nil
	}
	return nil, fmt.Errorf("no service manager support on %s", runtime.GOOS)
}

func runAgent(restart bool) error {
	cmd, err := agentCommand(restart)
	if err != nil {
		return err
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %w: %s", strings.Join(cmd.Args, " "), err, out)
	}
	return nil
}

// agentHint tells a user how to bring the relay back by hand.
func agentHint() string {
	if cmd, err := agentCommand(true); err == nil {
		return strings.Join(cmd.Args, " ")
	}
	return "llm-relay serve"
}
