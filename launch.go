package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
)

// managedEnv are the variables a launch owns. They are always cleared from the
// parent environment first, so a stale export in the shell can never redirect
// a session to the wrong gateway or model.
var managedEnv = []string{
	"ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_MODEL",
	"ANTHROPIC_DEFAULT_OPUS_MODEL", "ANTHROPIC_DEFAULT_SONNET_MODEL", "ANTHROPIC_DEFAULT_HAIKU_MODEL",
	"CLAUDE_CODE_SUBAGENT_MODEL", "CLAUDE_CODE_MAX_CONTEXT_TOKENS", "CLAUDE_CODE_MAX_OUTPUT_TOKENS",
	"CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY", "CLAUDE_STREAM_IDLE_TIMEOUT_MS",
	"CLAUDE_ACTIVE_PROFILE", "LLM_RELAY_KEY",
}

// codexProvider is the provider id injected into Codex with -c overrides, so
// ~/.codex/config.toml needs no relay entry.
const codexProvider = "llm_relay"

type launchSpec struct {
	argv   []string // argv[0] is the client binary name
	env    []string
	banner string
}

// buildLaunch turns a preset into the exact process to exec. Pure, for tests.
func buildLaunch(cfg *Config, name string, p PresetConfig, key string, args, parentEnv []string) launchSpec {
	env := launchEnv(p, parentEnv)
	set := func(k, v string) {
		if v != "" {
			env = append(env, k+"="+v)
		}
	}
	base := "http://" + cfg.Listen

	if p.Client == "codex" {
		set("LLM_RELAY_KEY", key)
		return launchSpec{
			argv:   codexArgv(p, base, args),
			env:    env,
			banner: fmt.Sprintf("llm-relay: %s -> codex via %s (model %s)", name, base, p.Model),
		}
	}

	set("ANTHROPIC_BASE_URL", base)
	set("ANTHROPIC_AUTH_TOKEN", key)
	set("ANTHROPIC_MODEL", p.Model)
	set("ANTHROPIC_DEFAULT_OPUS_MODEL", p.Opus)
	set("ANTHROPIC_DEFAULT_SONNET_MODEL", p.Sonnet)
	set("ANTHROPIC_DEFAULT_HAIKU_MODEL", p.Haiku)
	set("CLAUDE_CODE_SUBAGENT_MODEL", p.Subagent)
	set("CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY", "1")
	set("CLAUDE_ACTIVE_PROFILE", name)
	model := p.Model
	if model == "" {
		model = "client default"
	}
	return launchSpec{
		argv:   append([]string{"claude"}, args...),
		env:    env,
		banner: fmt.Sprintf("llm-relay: %s -> claude via %s (model %s)", name, base, model),
	}
}

// launchEnv is the parent environment minus every gateway-related variable
// and the preset's own keys, plus the preset's non-empty env values.
func launchEnv(p PresetConfig, parentEnv []string) []string {
	drop := make(map[string]bool, len(managedEnv)+len(p.Env))
	for _, k := range managedEnv {
		drop[k] = true
	}
	for k := range p.Env {
		drop[k] = true
	}
	env := make([]string, 0, len(parentEnv)+16)
	for _, kv := range parentEnv {
		if k, _, _ := strings.Cut(kv, "="); !drop[k] {
			env = append(env, kv)
		}
	}
	for k, v := range p.Env {
		if v != "" {
			env = append(env, k+"="+v)
		}
	}
	return env
}

// codexExecCommands are Codex subcommands that drop top-level -c overrides
// (codex 0.142: `codex -c ... exec` still dialed api.openai.com), so the
// overrides must follow the subcommand instead.
var codexExecCommands = map[string]bool{"exec": true, "e": true}

// codexArgv is the full Codex command line for a preset launch.
func codexArgv(p PresetConfig, base string, args []string) []string {
	argv := []string{"codex"}
	if len(args) > 0 && codexExecCommands[args[0]] {
		argv = append(argv, args[0])
		args = args[1:]
	}
	return append(append(argv, codexArgs(p, base)...), args...)
}

// codexArgs points Codex at the relay through -c overrides, so its config
// file needs no relay entry.
func codexArgs(p PresetConfig, base string) []string {
	pre := []string{
		"-c", fmt.Sprintf("model_provider=%q", codexProvider),
		"-c", fmt.Sprintf("model_providers.%s.name=%q", codexProvider, "llm-relay"),
		"-c", fmt.Sprintf("model_providers.%s.base_url=%q", codexProvider, base+"/v1"),
		"-c", fmt.Sprintf("model_providers.%s.env_key=%q", codexProvider, "LLM_RELAY_KEY"),
		"-c", fmt.Sprintf("model_providers.%s.wire_api=%q", codexProvider, "responses"),
		"-c", fmt.Sprintf("model_providers.%s.supports_websockets=false", codexProvider),
		"-m", p.Model,
	}
	if p.ReasoningEffort != "" {
		pre = append(pre, "-c", fmt.Sprintf("model_reasoning_effort=%q", p.ReasoningEffort))
	}
	return pre
}

// splitPreset takes a leading preset name off args. A leading non-flag that
// is not a preset is an error rather than being passed to the client, so a
// typo like `llm-relay run ctaa` fails loudly.
func splitPreset(cfg *Config, args []string) (string, []string, error) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", args, nil
	}
	if _, ok := cfg.Presets[args[0]]; ok {
		return args[0], args[1:], nil
	}
	return "", nil, fmt.Errorf("unknown preset %q (have: %v)", args[0], cfg.presetNames())
}

// nextTarget is ccnext's choice when no preset is named: the first Claude
// preset other than the one this shell runs in.
func nextTarget(cfg *Config, current string) (string, error) {
	for _, n := range cfg.presetNames() {
		if n != current && cfg.Presets[n].Client == "claude" {
			return n, nil
		}
	}
	return "", errors.New("no other claude preset to continue on")
}

// withContinue resumes the directory's latest session unless the caller
// already chose a session.
func withContinue(args []string) []string {
	for _, a := range args {
		switch a {
		case "-c", "--continue", "-r", "--resume":
			return args
		}
		if strings.HasPrefix(a, "--resume=") {
			return args
		}
	}
	return append([]string{"--continue"}, args...)
}

// runPreset implements `run` and, with next set, `next`.
func runPreset(configPath string, args []string, next bool) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	name, rest, err := splitPreset(cfg, args)
	if err != nil {
		return err
	}
	if next {
		if name == "" {
			if name, err = nextTarget(cfg, os.Getenv("CLAUDE_ACTIVE_PROFILE")); err != nil {
				return err
			}
		}
		rest = withContinue(rest)
	}
	name, p, err := cfg.preset(name)
	if err != nil {
		return err
	}
	if next && p.Client != "claude" {
		return fmt.Errorf("next resumes Claude sessions; preset %s is %s", name, p.Client)
	}
	key, err := readClientKey(cfg)
	if err != nil {
		return err
	}
	if err := ensureRelay(configPath); err != nil {
		return err
	}
	warnUnknownModels(configPath, name, p)

	spec := buildLaunch(cfg, name, p, key, rest, os.Environ())
	bin, err := exec.LookPath(spec.argv[0])
	if err != nil {
		return fmt.Errorf("%s not found on PATH: %w", spec.argv[0], err)
	}
	fmt.Fprintln(os.Stderr, spec.banner)
	return syscall.Exec(bin, spec.argv, spec.env)
}

func readClientKey(cfg *Config) (string, error) {
	if cfg.ClientKeyFile == "" {
		return "local", nil // relay accepts anything without a key file
	}
	raw, err := os.ReadFile(cfg.ClientKeyFile)
	if err != nil {
		return "", fmt.Errorf("read client key: %w", err)
	}
	return strings.TrimSpace(string(raw)), nil
}

// ensureRelay starts the background service if the relay is not answering,
// rather than letting every request of the session fail.
func ensureRelay(configPath string) error {
	healthy := func() bool { return call(configPath, "GET", "/relay/health", func([]byte) error { return nil }) == nil }
	if healthy() {
		return nil
	}
	fmt.Fprintln(os.Stderr, "llm-relay: relay not answering; starting the background service")
	if err := runAgent(false); err != nil {
		return fmt.Errorf("relay is down and could not be started; run `llm-relay install-agent`: %w", err)
	}
	for range 20 {
		time.Sleep(500 * time.Millisecond)
		if healthy() {
			return nil
		}
	}
	return fmt.Errorf("relay did not come up within 10s; see %s", defaultLogPath())
}

// warnUnknownModels flags preset models no provider advertises (stale or
// mistyped ids). Advisory only: a route or default provider may still serve
// them.
func warnUnknownModels(configPath, name string, p PresetConfig) {
	var known map[string][]string
	err := call(configPath, "GET", "/relay/routes", func(b []byte) error {
		var r struct {
			Routes map[string][]string `json:"routes"`
		}
		if err := json.Unmarshal(b, &r); err != nil {
			return err
		}
		known = r.Routes
		return nil
	})
	if err != nil {
		return // advisory; the launch itself proceeds
	}
	for _, m := range p.models() {
		if _, ok := known[m]; !ok {
			fmt.Fprintf(os.Stderr, "llm-relay: warning: preset %s uses %q, which no provider advertises (see `llm-relay routes`)\n", name, m)
		}
	}
}

func listPresets(configPath string) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PRESET\tCLIENT\tMODEL\tOPUS / SONNET / HAIKU\tDESCRIPTION")
	for _, n := range cfg.presetNames() {
		p := cfg.Presets[n]
		mark := " "
		if n == cfg.DefaultPreset {
			mark = "*"
		}
		tiers := "-"
		if p.Client == "claude" {
			tiers = strings.Join([]string{orDash(p.Opus), orDash(p.Sonnet), orDash(p.Haiku)}, " / ")
		}
		fmt.Fprintf(tw, "%s%s\t%s\t%s\t%s\t%s\n", mark, n, p.Client, orDash(p.Model), tiers, p.Description)
	}
	return tw.Flush()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func printShellInit(configPath string) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := shellQuote(exe)
	if configPath != DefaultConfigPath() {
		cmd += " -config " + shellQuote(configPath)
	}
	fmt.Print(shellInit(cfg, cmd))
	return nil
}

// shellInit prints aliases for every preset plus the helper commands, so a
// preset added to the config becomes a command in the next shell. cmd is the
// already shell-quoted command prefix.
func shellInit(cfg *Config, cmd string) string {
	var b strings.Builder
	b.WriteString("# generated by `llm-relay shell-init`; edit presets in the relay config instead\n")
	for _, n := range cfg.presetNames() {
		fmt.Fprintf(&b, "alias %s=%s\n", n, shellQuote(cmd+" run "+n))
	}
	for _, h := range [][2]string{{"ccr", "run"}, {"ccp", "presets"}, {"ccnext", "next"}} {
		if _, taken := cfg.Presets[h[0]]; !taken {
			fmt.Fprintf(&b, "alias %s=%s\n", h[0], shellQuote(cmd+" "+h[1]))
		}
	}
	return b.String()
}

// shellQuote wraps s in single quotes for sh/zsh.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
