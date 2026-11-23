package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const validConfig = `
client_key_file: ~/relay.key
providers:
  - name: a
    base_url: https://a.example/
    auth: {cmd: [~/bin/tok.sh], ttl: 5m}
  - name: b
    base_url: https://b.example
    auth: {env: B_TOKEN, header: x-api-key}
    models: [static-b]
routes:
  - {match: "claude-*", provider: a}
default_provider: b
`

func TestLoadConfig(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != defaultListen {
		t.Errorf("listen = %q", cfg.Listen)
	}
	if cfg.ClientKeyFile != filepath.Join(home, "relay.key") {
		t.Errorf("client key not expanded: %q", cfg.ClientKeyFile)
	}
	a := cfg.Providers[0]
	if a.BaseURL != "https://a.example" || a.Auth.Cmd[0] != filepath.Join(home, "bin/tok.sh") ||
		a.Auth.TTL != 5*time.Minute || a.Auth.Header != "authorization" {
		t.Errorf("provider a not normalized: %+v", a)
	}
	if cfg.Providers[1].Auth.Header != "x-api-key" {
		t.Errorf("provider b header = %q", cfg.Providers[1].Auth.Header)
	}
}

func TestLoadConfigRejects(t *testing.T) {
	cases := map[string]string{
		"no providers":   `listen: x`,
		"unknown field":  "providers: [{name: a, base_url: https://a, auth: {env: X}, nope: 1}]",
		"dup name":       "providers: [{name: a, base_url: https://a, auth: {env: X}}, {name: a, base_url: https://b, auth: {env: X}}]",
		"bad url":        "providers: [{name: a, base_url: a.example, auth: {env: X}}]",
		"both auth":      "providers: [{name: a, base_url: https://a, auth: {env: X, cmd: [y]}}]",
		"no auth":        "providers: [{name: a, base_url: https://a}]",
		"bad header":     "providers: [{name: a, base_url: https://a, auth: {env: X, header: cookie}}]",
		"route provider": "providers: [{name: a, base_url: https://a, auth: {env: X}}]\nroutes: [{match: '*', provider: z}]",
		"bad glob":       "providers: [{name: a, base_url: https://a, auth: {env: X}}]\nroutes: [{match: '[', provider: a}]",
		"bad default":    "providers: [{name: a, base_url: https://a, auth: {env: X}}]\ndefault_provider: z",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadConfig(writeConfig(t, body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestNormalizeModel(t *testing.T) {
	for in, want := range map[string]string{
		"opus[1m]": "opus", " kimi-k3[1m] ": "kimi-k3", "glm-5.3": "glm-5.3", "[x]": "[x]",
	} {
		if got := NormalizeModel(in); got != want {
			t.Errorf("NormalizeModel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLoadConfigRoutesAndFailover(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if r := cfg.Routes[0]; r.Provider != "" || strings.Join(r.Providers, ",") != "a" {
		t.Errorf("provider shorthand not folded into providers: %+v", r)
	}
	if cfg.Failover.Cooldown != defaultCooldown || cfg.Failover.QuotaCooldown != defaultQuotaCooldown {
		t.Errorf("failover defaults not applied: %+v", cfg.Failover)
	}

	cfg, err = LoadConfig(writeConfig(t, `
providers: [{name: a, base_url: https://a, auth: {env: X}}, {name: b, base_url: https://b, auth: {env: X}}]
routes: [{match: "m*", provider: b, providers: [a]}]
failover: {cooldown: 5s, quota_cooldown: 2h}
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cfg.Routes[0].Providers, ","); got != "b,a" {
		t.Errorf("providers = %s, want b,a", got)
	}
	if cfg.Failover.Cooldown != 5*time.Second || cfg.Failover.QuotaCooldown != 2*time.Hour {
		t.Errorf("failover = %+v", cfg.Failover)
	}

	for name, body := range map[string]string{
		"empty route":       "providers: [{name: a, base_url: https://a, auth: {env: X}}]\nroutes: [{match: '*'}]",
		"unknown in list":   "providers: [{name: a, base_url: https://a, auth: {env: X}}]\nroutes: [{match: '*', providers: [a, z]}]",
		"negative cooldown": "providers: [{name: a, base_url: https://a, auth: {env: X}}]\nfailover: {cooldown: -1s}",
	} {
		if _, err := LoadConfig(writeConfig(t, body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestRouterCandidates(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, `
providers:
  - {name: a, base_url: https://a, auth: {env: X}}
  - {name: b, base_url: https://b, auth: {env: X}, models: [static-b]}
  - {name: c, base_url: https://c, auth: {env: X}}
routes:
  - {match: "claude-*", providers: [c, a]}
  - {match: "claude-x", providers: [b]}   # shadowed: first match wins
default_provider: b
`))
	if err != nil {
		t.Fatal(err)
	}
	r := NewRouter(cfg)
	r.SetDiscovered("a", []string{"shared", "only-a", "claude-x"})
	r.SetDiscovered("b", []string{"shared", "only-b", "claude-x"})
	r.SetDiscovered("c", []string{"shared"})

	for model, want := range map[string]string{
		"claude-x[1m]": "c,a,b", // route order first, then other servers as failover
		"claude-new":   "c,a",   // routed but not discovered anywhere
		"shared":       "a,b,c", // provider priority
		"only-b":       "b",
		"static-b":     "b",
		"unknown":      "b", // default provider only when nobody serves it
	} {
		got, err := r.Candidates(model)
		if err != nil || strings.Join(got, ",") != want {
			t.Errorf("Candidates(%q) = %v, %v; want %s", model, got, err, want)
		}
	}
	if _, err := r.Candidates(""); err == nil {
		t.Error("empty model should fail")
	}
	if got := strings.Join(r.Table()["claude-x"], ","); got != "c,a,b" {
		t.Errorf("Table should show the full failover chain, got %s", got)
	}
}

func TestRouterNoDefault(t *testing.T) {
	r := NewRouter(&Config{Providers: []ProviderConfig{{Name: "a"}}})
	r.SetDiscovered("a", []string{"m1"})
	_, err := r.Candidates("nope")
	if err == nil || !strings.Contains(err.Error(), "m1") {
		t.Fatalf("want error listing known models, got %v", err)
	}
}

func TestCredentialCaching(t *testing.T) {
	calls := 0
	now := time.Unix(0, 0)
	c := NewCredential(AuthConfig{Cmd: []string{"x"}, TTL: time.Minute})
	c.now = func() time.Time { return now }
	c.run = func(context.Context, []string) (string, error) {
		calls++
		return "tok", nil
	}
	ctx := context.Background()
	for range 3 {
		if v, err := c.Get(ctx, false); err != nil || v != "tok" {
			t.Fatal(v, err)
		}
	}
	if calls != 1 {
		t.Fatalf("cached get ran %d times", calls)
	}
	now = now.Add(2 * time.Minute)
	for _, refresh := range []bool{false, true} {
		if _, err := c.Get(ctx, refresh); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 {
		t.Fatalf("expiry + forced refresh should run twice more, ran %d total", calls)
	}
}

func TestCredentialErrors(t *testing.T) {
	c := NewCredential(AuthConfig{Cmd: []string{"x"}})
	c.run = func(context.Context, []string) (string, error) { return "", errors.New("boom") }
	if _, err := c.Get(context.Background(), false); err == nil {
		t.Fatal("expected command error")
	}
	t.Setenv("RELAY_TEST_TOKEN", "")
	e := NewCredential(AuthConfig{Env: "RELAY_TEST_TOKEN"})
	if _, err := e.Get(context.Background(), false); err == nil {
		t.Fatal("expected empty env error")
	}
	if e.Refreshable() {
		t.Fatal("env credentials cannot be refreshed")
	}
}

func TestRunCommand(t *testing.T) {
	v, err := runCommand(context.Background(), []string{"printf", "  secret\n"})
	if err != nil || v != "secret" {
		t.Fatalf("got %q, %v", v, err)
	}
	if _, err := runCommand(context.Background(), []string{"true"}); err == nil {
		t.Fatal("empty output should fail")
	}
	if _, err := runCommand(context.Background(), []string{"false"}); err == nil {
		t.Fatal("non-zero exit should fail")
	}
}

func TestNetworkConfig(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	n := cfg.Network
	if n.HedgeAfter != defaultHedgeAfter || n.MaxParallel != defaultMaxParallel ||
		n.ConnectTimeout != defaultConnectTimeout || *n.Retries != defaultRetries || *n.KeepWarm != defaultKeepWarm {
		t.Errorf("defaults not applied: %+v", n)
	}

	cfg, err = LoadConfig(writeConfig(t, `
providers: [{name: a, base_url: https://a, auth: {env: X}}]
network: {hedge_after: 1s, max_parallel: 2, connect_timeout: 9s, retries: 0, keep_warm: 0s}
`))
	if err != nil {
		t.Fatal(err)
	}
	if n := cfg.Network; n.HedgeAfter != time.Second || n.MaxParallel != 2 || n.ConnectTimeout != 9*time.Second ||
		*n.Retries != 0 || *n.KeepWarm != 0 {
		t.Errorf("explicit values (including zero) not kept: %+v", n)
	}

	for name, net := range map[string]string{
		"parallel":  "{max_parallel: 9}",
		"timeout":   "{hedge_after: 5s, connect_timeout: 1s}",
		"retries":   "{retries: -1}",
		"keep_warm": "{keep_warm: -1s}",
	} {
		body := "providers: [{name: a, base_url: https://a, auth: {env: X}}]\nnetwork: " + net
		if _, err := LoadConfig(writeConfig(t, body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
