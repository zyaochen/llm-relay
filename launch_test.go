package main

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func loadPresets(t *testing.T) *Config {
	t.Helper()
	cfg, err := LoadConfig(writeConfig(t, presetsConfig))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func envMap(env []string) map[string]string {
	m := map[string]string{}
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		m[k] = v
	}
	return m
}

func TestBuildLaunchClaude(t *testing.T) {
	cfg := loadPresets(t)
	parent := []string{"PATH=/bin", "HOME=/h", "ANTHROPIC_MODEL=bogus", "ANTHROPIC_API_KEY=leak",
		"CLAUDE_CODE_MAX_OUTPUT_TOKENS=1", "LLM_RELAY_KEY=old"}
	spec := buildLaunch(cfg, "ccl", cfg.Presets["ccl"], "k", []string{"-p", "hi"}, parent)
	env := envMap(spec.env)

	if !slices.Equal(spec.argv, []string{"claude", "-p", "hi"}) {
		t.Errorf("argv = %v", spec.argv)
	}
	want := map[string]string{
		"PATH": "/bin", "HOME": "/h",
		"ANTHROPIC_BASE_URL": "http://" + defaultListen, "ANTHROPIC_AUTH_TOKEN": "k",
		"ANTHROPIC_MODEL": "kimi-k3[1m]", "ANTHROPIC_DEFAULT_SONNET_MODEL": "glm-5.3[1m]",
		"CLAUDE_CODE_MAX_OUTPUT_TOKENS": "131072", "CLAUDE_ACTIVE_PROFILE": "ccl",
		"CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY": "1",
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("%s = %q, want %q", k, env[k], v)
		}
	}
	for _, k := range []string{"ANTHROPIC_API_KEY", "LLM_RELAY_KEY", "CLAUDE_CODE_SUBAGENT_MODEL"} {
		if _, ok := env[k]; ok {
			t.Errorf("%s must not leak from the parent shell or be set empty", k)
		}
	}
	if n := strings.Count(strings.Join(spec.env, "\n"), "CLAUDE_CODE_MAX_OUTPUT_TOKENS="); n != 1 {
		t.Errorf("preset env must replace, not duplicate, the parent value (%d copies)", n)
	}
}

func TestBuildLaunchCodex(t *testing.T) {
	cfg := loadPresets(t)
	spec := buildLaunch(cfg, "cxr", cfg.Presets["cxr"], "k", []string{"exec", "hi"},
		[]string{"PATH=/bin", "ANTHROPIC_BASE_URL=x"})
	args := strings.Join(spec.argv, " ")
	for _, frag := range []string{
		`codex -c model_provider="llm_relay"`,
		`model_providers.llm_relay.base_url="http://` + defaultListen + `/v1"`,
		`model_providers.llm_relay.env_key="LLM_RELAY_KEY"`,
		`model_providers.llm_relay.wire_api="responses"`,
		`-m gpt-5.6-terra`, `model_reasoning_effort="high"`,
	} {
		if !strings.Contains(args, frag) {
			t.Errorf("argv missing %q:\n%s", frag, args)
		}
	}
	if !strings.HasSuffix(args, " exec hi") {
		t.Errorf("client args must come last: %s", args)
	}
	env := envMap(spec.env)
	if env["LLM_RELAY_KEY"] != "k" || env["ANTHROPIC_BASE_URL"] != "" {
		t.Errorf("codex env: key=%q anthropic=%q", env["LLM_RELAY_KEY"], env["ANTHROPIC_BASE_URL"])
	}
}

func TestSplitPresetAndNext(t *testing.T) {
	cfg := loadPresets(t)
	if n, rest, err := splitPreset(cfg, []string{"cta", "-p", "x"}); err != nil || n != "cta" || len(rest) != 2 {
		t.Errorf("named: %s %v %v", n, rest, err)
	}
	if n, rest, err := splitPreset(cfg, []string{"-p", "x"}); err != nil || n != "" || len(rest) != 2 {
		t.Errorf("default: %s %v %v", n, rest, err)
	}
	if _, _, err := splitPreset(cfg, []string{"ctaa"}); err == nil {
		t.Error("a typo must fail, not reach the client")
	}

	if n, _ := nextTarget(cfg, "ccl"); n != "cta" {
		t.Errorf("next from ccl = %s, want cta (first other claude preset)", n)
	}
	if n, _ := nextTarget(cfg, "cta"); n != "ccl" {
		t.Errorf("next from cta = %s", n)
	}

	if got := withContinue([]string{"-p", "x"}); got[0] != "--continue" {
		t.Errorf("should add --continue: %v", got)
	}
	for _, own := range [][]string{{"-r"}, {"--resume", "id"}, {"--resume=id"}, {"-c"}} {
		if got := withContinue(own); slices.Contains(got, "--continue") && own[0] != "-c" {
			t.Errorf("caller's own session choice %v must be kept: %v", own, got)
		}
	}
}

func TestShellInit(t *testing.T) {
	cfg := loadPresets(t)
	for _, line := range []string{"alias ccl=", "alias cta=", "alias cxr=", "alias ccr=", "alias ccp=", "alias ccnext="} {
		if out := shellInit(cfg, "llm-relay"); !strings.Contains(out, line) {
			t.Errorf("missing %q in:\n%s", line, out)
		}
	}
	// Run the aliases in real shells with a stand-in command that prints each
	// argument it receives, so quoting of paths with spaces is checked end to end.
	stub := "printf " + shellQuote("[%s]") + " " + shellQuote("/opt/llm relay/bin") + " -config " + shellQuote("/tmp/c d.yaml")
	body := shellInit(cfg, stub) + "\neval 'ccl --model x'\necho\neval ccnext\n"
	want := "[/opt/llm relay/bin][-config][/tmp/c d.yaml][run][ccl][--model][x]\n" +
		"[/opt/llm relay/bin][-config][/tmp/c d.yaml][next]"
	ran := 0
	for _, sh := range []struct{ name, prelude string }{
		{"zsh", ""},
		{"bash", "shopt -s expand_aliases\n"}, // non-interactive bash ignores aliases otherwise
	} {
		if _, err := exec.LookPath(sh.name); err != nil {
			continue // CI images differ: macOS has zsh, Ubuntu runners only bash
		}
		ran++
		args := []string{"-c", sh.prelude + body}
		if sh.name == "zsh" {
			args = append([]string{"-f"}, args...)
		}
		got, err := exec.Command(sh.name, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s rejected shell-init output: %v\n%s", sh.name, err, got)
		}
		if string(got) != want {
			t.Errorf("%s expanded the aliases to\n%s\nwant\n%s", sh.name, got, want)
		}
	}
	if ran == 0 {
		t.Fatal("neither zsh nor bash found; shell-init output is untested")
	}
}
