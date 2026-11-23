package main

import (
	"strings"
	"testing"
)

const presetsConfig = `
providers: [{name: a, base_url: https://a, auth: {env: X}}]
default_preset: ccl
presets:
  ccl: {client: claude, model: "kimi-k3[1m]", opus: "kimi-k3[1m]", sonnet: "glm-5.3[1m]", haiku: glm-5.3-flash,
        env: {CLAUDE_CODE_MAX_OUTPUT_TOKENS: "131072"}}
  cta: {client: claude, model: "opus[1m]", opus: claude-opus-5-5}
  cxr: {client: codex, model: gpt-5.6-terra, reasoning_effort: high}
`

func TestPresetsLoad(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, presetsConfig))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cfg.presetNames(), ","); got != "ccl,cta,cxr" {
		t.Errorf("names = %s", got)
	}
	name, p, err := cfg.preset("")
	if err != nil || name != "ccl" || p.Env["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] != "131072" {
		t.Errorf("default preset: %s %+v %v", name, p, err)
	}
	if _, _, err := cfg.preset("nope"); err == nil || !strings.Contains(err.Error(), "ccl") {
		t.Errorf("unknown preset should list known ones: %v", err)
	}
	if got := strings.Join(cfg.Presets["ccl"].models(), ","); got != "kimi-k3,glm-5.3,glm-5.3-flash" {
		t.Errorf("ccl models = %s", got)
	}
	if got := strings.Join(cfg.Presets["cta"].models(), ","); got != "claude-opus-5-5" {
		t.Errorf("client-side alias opus must be skipped, got %s", got)
	}
}

func TestPresetsReject(t *testing.T) {
	base := "providers: [{name: a, base_url: https://a, auth: {env: X}}]\n"
	for name, body := range map[string]string{
		"bad name":       "presets: {Bad Name: {client: claude}}",
		"bad client":     "presets: {x: {client: cursor}}",
		"codex no model": "presets: {x: {client: codex}}",
		"codex opus":     "presets: {x: {client: codex, model: m, opus: o}}",
		"claude effort":  "presets: {x: {client: claude, reasoning_effort: high}}",
		"bad default":    "presets: {x: {client: claude}}\ndefault_preset: y",
		"unknown field":  "presets: {x: {client: claude, modle: typo}}",
	} {
		if _, err := LoadConfig(writeConfig(t, base+body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
