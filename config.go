package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultListen = "127.0.0.1:18765"

// Config is the on-disk relay configuration. Adding a gateway means adding one
// Providers entry; nothing else in the toolchain needs to change.
type Config struct {
	Listen          string           `yaml:"listen"`
	ClientKeyFile   string           `yaml:"client_key_file"`
	Providers       []ProviderConfig `yaml:"providers"`
	Routes          []RouteConfig    `yaml:"routes"`
	DefaultProvider string           `yaml:"default_provider"`
	Failover        FailoverConfig   `yaml:"failover"`
	Network         NetworkConfig    `yaml:"network"`
	// Launch presets for `llm-relay run <name>`; see presets.go.
	Presets       map[string]PresetConfig `yaml:"presets"`
	DefaultPreset string                  `yaml:"default_preset"`
}

// FailoverConfig sets how long a provider is skipped for a model after it
// fails. An upstream Retry-After wins when it is longer.
type FailoverConfig struct {
	Cooldown      time.Duration `yaml:"cooldown"`
	QuotaCooldown time.Duration `yaml:"quota_cooldown"`
}

// NetworkConfig tunes how upstream connections are made and kept. Pointer
// fields distinguish "unset" from an explicit 0.
type NetworkConfig struct {
	// HedgeAfter starts another TLS handshake in parallel when the running
	// ones have not finished after this long.
	HedgeAfter     time.Duration  `yaml:"hedge_after"`
	MaxParallel    int            `yaml:"max_parallel"`
	ConnectTimeout time.Duration  `yaml:"connect_timeout"`
	Retries        *int           `yaml:"retries"`   // same-provider retries on connection errors
	KeepWarm       *time.Duration `yaml:"keep_warm"` // 0 disables
	// Connections is how many independent connections (lanes) each provider
	// gets, so a stall on one does not freeze every session.
	Connections int `yaml:"connections"`
	// OutageWait holds a request while no provider can be reached at all,
	// retrying until the network is back; 0 answers 503 at once.
	OutageWait *time.Duration `yaml:"outage_wait"`
}

const (
	defaultCooldown       = time.Minute
	defaultQuotaCooldown  = 30 * time.Minute
	defaultHedgeAfter     = 2 * time.Second
	defaultMaxParallel    = 3
	defaultConnectTimeout = 15 * time.Second
	defaultRetries        = 1
	defaultKeepWarm       = 30 * time.Second // below the 45s h2 ping trigger, see server.go
	defaultConnections    = 2
	defaultOutageWait     = 3 * time.Minute // well inside Claude Code's 10-minute API timeout
)

func (n *NetworkConfig) normalize() error {
	if n.HedgeAfter == 0 {
		n.HedgeAfter = defaultHedgeAfter
	}
	if n.MaxParallel == 0 {
		n.MaxParallel = defaultMaxParallel
	}
	if n.ConnectTimeout == 0 {
		n.ConnectTimeout = defaultConnectTimeout
	}
	if n.Retries == nil {
		r := defaultRetries
		n.Retries = &r
	}
	if n.KeepWarm == nil {
		k := defaultKeepWarm
		n.KeepWarm = &k
	}
	if n.Connections == 0 {
		n.Connections = defaultConnections
	}
	if n.OutageWait == nil {
		w := defaultOutageWait
		n.OutageWait = &w
	}
	switch {
	case n.Connections < 1 || n.Connections > 4:
		return errors.New("network: connections must be 1..4")
	case n.HedgeAfter < 0 || n.ConnectTimeout < 0 || *n.KeepWarm < 0 || *n.OutageWait < 0:
		return errors.New("network: durations must not be negative")
	case n.MaxParallel < 1 || n.MaxParallel > 8:
		return errors.New("network: max_parallel must be 1..8")
	case n.ConnectTimeout < n.HedgeAfter:
		return errors.New("network: connect_timeout must be at least hedge_after")
	case *n.Retries < 0 || *n.Retries > 5:
		return errors.New("network: retries must be 0..5")
	case *n.OutageWait > 8*time.Minute:
		return errors.New("network: outage_wait must stay under Claude Code's 10-minute API timeout (max 8m)")
	}
	return nil
}

func defaultNetwork() NetworkConfig {
	var n NetworkConfig
	_ = n.normalize() // noqa: errcheck reason: zero value always normalizes to valid defaults
	return n
}

// ProviderConfig describes one upstream gateway. Order in Config.Providers is
// priority: when several providers serve a model, earlier ones are tried first
// and later ones are failover targets.
type ProviderConfig struct {
	Name    string     `yaml:"name"`
	BaseURL string     `yaml:"base_url"`
	Auth    AuthConfig `yaml:"auth"`
	// Discover controls whether /v1/models is polled to learn served models.
	Discover *bool `yaml:"discover"`
	// Models are served by this provider in addition to discovered ones.
	Models []string `yaml:"models"`
	// MaxConcurrent caps in-flight requests to this provider (0 = unlimited);
	// extra requests queue in the relay instead of drawing a 429.
	MaxConcurrent int `yaml:"max_concurrent"`
	// PriorityModels are model globs whose requests are served before others
	// when requests queue for a slot (the interactive model over background ones).
	PriorityModels []string `yaml:"priority_models"`
	// ReservedSlots of MaxConcurrent are only given to priority models.
	ReservedSlots int `yaml:"reserved_slots"`
}

func (p *ProviderConfig) normalizeLimits() error {
	switch {
	case p.MaxConcurrent < 0:
		return errors.New("max_concurrent must not be negative")
	case p.ReservedSlots < 0:
		return errors.New("reserved_slots must not be negative")
	case p.ReservedSlots > 0 && p.ReservedSlots >= p.MaxConcurrent:
		return errors.New("reserved_slots must be below max_concurrent")
	case p.ReservedSlots > 0 && len(p.PriorityModels) == 0:
		return errors.New("reserved_slots needs priority_models")
	case len(p.PriorityModels) > 0 && p.MaxConcurrent == 0:
		return errors.New("priority_models needs max_concurrent")
	}
	for _, g := range p.PriorityModels {
		if _, err := path.Match(g, ""); err != nil {
			return fmt.Errorf("priority_models: bad glob %q: %w", g, err)
		}
	}
	return nil
}

// AuthConfig says where the upstream credential comes from. Exactly one of
// Cmd or Env must be set; secrets are never written into the config itself.
type AuthConfig struct {
	Cmd []string      `yaml:"cmd"`
	Env string        `yaml:"env"`
	TTL time.Duration `yaml:"ttl"`
	// Header is "authorization" (Bearer, default) or "x-api-key".
	Header string `yaml:"header"`
}

// RouteConfig puts the listed providers first, in order, for models matching
// a glob. Other providers that also serve the model remain failover targets.
type RouteConfig struct {
	Match     string   `yaml:"match"`
	Provider  string   `yaml:"provider"` // shorthand for a single-entry Providers
	Providers []string `yaml:"providers"`
}

func (p ProviderConfig) discoverEnabled() bool {
	return p.Discover == nil || *p.Discover
}

// LoadConfig reads, expands and validates a config file.
func LoadConfig(file string) (*Config, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", file, err)
	}
	if err := cfg.normalize(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", file, err)
	}
	return &cfg, nil
}

func (c *Config) normalize() error {
	if c.Listen == "" {
		c.Listen = defaultListen
	}
	c.ClientKeyFile = expandHome(c.ClientKeyFile)
	seen, err := c.normalizeProviders()
	if err != nil {
		return err
	}
	if err := c.normalizeRoutes(seen); err != nil {
		return err
	}
	if c.DefaultProvider != "" && !seen[c.DefaultProvider] {
		return fmt.Errorf("default_provider: unknown provider %q", c.DefaultProvider)
	}

	f := &c.Failover
	if f.Cooldown < 0 || f.QuotaCooldown < 0 {
		return errors.New("failover: cooldowns must not be negative")
	}
	if f.Cooldown == 0 {
		f.Cooldown = defaultCooldown
	}
	if f.QuotaCooldown == 0 {
		f.QuotaCooldown = defaultQuotaCooldown
	}
	if err := c.Network.normalize(); err != nil {
		return err
	}
	return c.normalizePresets()
}

// normalizeProviders validates providers and returns the set of their names.
func (c *Config) normalizeProviders() (map[string]bool, error) {
	if len(c.Providers) == 0 {
		return nil, errors.New("at least one provider is required")
	}
	seen := make(map[string]bool, len(c.Providers))
	for i := range c.Providers {
		p := &c.Providers[i]
		if p.Name == "" {
			return nil, fmt.Errorf("providers[%d]: name is required", i)
		}
		if seen[p.Name] {
			return nil, fmt.Errorf("providers[%d]: duplicate name %q", i, p.Name)
		}
		seen[p.Name] = true

		u, err := url.Parse(p.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("provider %s: base_url must be an absolute http(s) URL", p.Name)
		}
		p.BaseURL = strings.TrimRight(p.BaseURL, "/")

		if err := p.Auth.normalize(); err != nil {
			return nil, fmt.Errorf("provider %s: %w", p.Name, err)
		}
		if err := p.normalizeLimits(); err != nil {
			return nil, fmt.Errorf("provider %s: %w", p.Name, err)
		}
	}
	return seen, nil
}

func (c *Config) normalizeRoutes(providers map[string]bool) error {
	for i := range c.Routes {
		r := &c.Routes[i]
		if r.Provider != "" {
			r.Providers = append([]string{r.Provider}, r.Providers...)
			r.Provider = ""
		}
		if len(r.Providers) == 0 {
			return fmt.Errorf("routes[%d]: provider or providers is required", i)
		}
		for _, p := range r.Providers {
			if !providers[p] {
				return fmt.Errorf("routes[%d]: unknown provider %q", i, p)
			}
		}
		if _, err := path.Match(r.Match, ""); err != nil {
			return fmt.Errorf("routes[%d]: bad glob %q: %w", i, r.Match, err)
		}
	}
	return nil
}

func (a *AuthConfig) normalize() error {
	if (len(a.Cmd) == 0) == (a.Env == "") {
		return errors.New("auth: set exactly one of cmd or env")
	}
	for i, arg := range a.Cmd {
		a.Cmd[i] = expandHome(arg)
	}
	switch strings.ToLower(a.Header) {
	case "", "authorization":
		a.Header = "authorization"
	case "x-api-key":
		a.Header = "x-api-key"
	default:
		return fmt.Errorf("auth: header must be authorization or x-api-key, got %q", a.Header)
	}
	if a.TTL < 0 {
		return errors.New("auth: ttl must not be negative")
	}
	return nil
}

func expandHome(p string) string {
	if p != "~" && !strings.HasPrefix(p, "~/") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	return filepath.Join(home, strings.TrimPrefix(p, "~"))
}

// DefaultConfigPath honours LLM_RELAY_CONFIG, else ~/.config/llm-relay/config.yaml.
func DefaultConfigPath() string {
	if p := os.Getenv("LLM_RELAY_CONFIG"); p != "" {
		return p
	}
	return expandHome("~/.config/llm-relay/config.yaml")
}
