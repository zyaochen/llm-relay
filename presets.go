package main

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
)

// PresetConfig is a named way to launch a client through the relay. Its name
// becomes a shell alias via `llm-relay shell-init`.
type PresetConfig struct {
	Client      string `yaml:"client"` // claude | codex
	Description string `yaml:"description"`
	Model       string `yaml:"model"`
	// Claude only: what /model opus|sonnet|haiku and subagents resolve to.
	Opus     string `yaml:"opus"`
	Sonnet   string `yaml:"sonnet"`
	Haiku    string `yaml:"haiku"`
	Subagent string `yaml:"subagent"`
	// Codex only.
	ReasoningEffort string `yaml:"reasoning_effort"`
	// Extra environment for the client process.
	Env map[string]string `yaml:"env"`
}

var presetNameRe = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

func (c *Config) normalizePresets() error {
	for name, p := range c.Presets {
		if !presetNameRe.MatchString(name) {
			return fmt.Errorf("preset %q: name must match %s (it becomes a shell alias)", name, presetNameRe)
		}
		switch p.Client {
		case "claude":
			if p.ReasoningEffort != "" {
				return fmt.Errorf("preset %s: reasoning_effort is codex-only", name)
			}
		case "codex":
			if p.Opus != "" || p.Sonnet != "" || p.Haiku != "" || p.Subagent != "" {
				return fmt.Errorf("preset %s: opus/sonnet/haiku/subagent are claude-only", name)
			}
			if p.Model == "" {
				return fmt.Errorf("preset %s: codex presets need a model", name)
			}
		default:
			return fmt.Errorf("preset %s: client must be claude or codex, got %q", name, p.Client)
		}
	}
	if c.DefaultPreset != "" {
		if _, ok := c.Presets[c.DefaultPreset]; !ok {
			return fmt.Errorf("default_preset: unknown preset %q", c.DefaultPreset)
		}
	}
	return nil
}

// presetNames returns preset names in a stable order.
func (c *Config) presetNames() []string {
	names := make([]string, 0, len(c.Presets))
	for n := range c.Presets {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// preset resolves a name, falling back to default_preset when name is empty.
func (c *Config) preset(name string) (string, PresetConfig, error) {
	if name == "" {
		name = c.DefaultPreset
	}
	if name == "" {
		return "", PresetConfig{}, errors.New("no preset given and no default_preset configured")
	}
	p, ok := c.Presets[name]
	if !ok {
		return "", PresetConfig{}, fmt.Errorf("unknown preset %q (have: %v)", name, c.presetNames())
	}
	return name, p, nil
}

// models lists the concrete model ids a preset may send, for sanity checks.
func (p PresetConfig) models() []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range []string{p.Model, p.Opus, p.Sonnet, p.Haiku, p.Subagent} {
		m = NormalizeModel(m)
		// opus/sonnet/haiku are client-side aliases, resolved by the client itself.
		if m == "" || m == "opus" || m == "sonnet" || m == "haiku" || seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, m)
	}
	return out
}
