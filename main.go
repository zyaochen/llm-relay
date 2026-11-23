// Command llm-relay is a local passthrough relay that lets Claude Code, Codex
// and anything else speaking Anthropic/OpenAI wire formats reach many gateways
// through one base URL. See README.md.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
)

const (
	agentLabel = "io.github.chenziyaos.llm-relay"
	usage      = `usage: llm-relay [-config path] <command>

launching (presets live in the config):
  run [preset] [client args...]
                 start claude/codex through the relay with a preset (default_preset if omitted)
  next [preset] [claude args...]
                 continue this directory's latest Claude session on another preset
  presets        list presets
  shell-init     print shell aliases for every preset: eval "$(llm-relay shell-init)"

relay:
  serve          run the relay in the foreground
  init           create config dir, client key and a starter config (never overwrites)
  key            print the client key local tools must present
  status         show relay health
  routes         show each model's providers in failover order, and active cooldowns
  reset [name]   clear cooldowns (all, or one provider) so traffic goes back now
  probe [-n N] <provider>
                 compare fresh single vs hedged TLS handshakes to a provider
  reload         re-read the config now (it is also picked up automatically)
  restart [-wait 2m] [-force]
                 restart the agent once no client request is in flight; gives up after
                 -wait unless -force (then in-flight streams get up to 90s to drain)
  install-agent  install and (re)start the launchd agent ` + agentLabel + `
`
)

func main() {
	fl := flag.NewFlagSet("llm-relay", flag.ExitOnError)
	configPath := fl.String("config", DefaultConfigPath(), "config file")
	fl.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	_ = fl.Parse(os.Args[1:]) // ExitOnError exits on failure
	takesArgs := map[string]bool{"reset": true, "probe": true, "restart": true, "run": true, "next": true}
	if fl.NArg() == 0 || (fl.NArg() > 1 && !takesArgs[fl.Arg(0)]) {
		fl.Usage()
		os.Exit(2)
	}

	var err error
	switch fl.Arg(0) {
	case "run", "next":
		err = runPreset(*configPath, fl.Args()[1:], fl.Arg(0) == "next")
	case "presets":
		err = listPresets(*configPath)
	case "shell-init":
		err = printShellInit(*configPath)
	case "serve":
		err = serve(*configPath)
	case "init":
		err = initConfig(*configPath)
	case "key":
		err = printKey(*configPath)
	case "status":
		err = call(*configPath, http.MethodGet, "/relay/health", printBody)
	case "routes":
		err = call(*configPath, http.MethodGet, "/relay/routes", printRoutes)
	case "reset":
		err = call(*configPath, http.MethodPost, "/relay/reset?provider="+url.QueryEscape(fl.Arg(1)), printBody)
	case "restart":
		err = restart(*configPath, fl.Args()[1:])
	case "probe":
		err = probe(*configPath, fl.Args()[1:])
	case "reload":
		err = call(*configPath, http.MethodPost, "/relay/reload", printBody)
	case "install-agent":
		err = installAgent(*configPath)
	default:
		fl.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "llm-relay:", err)
		os.Exit(1)
	}
}

func serve(configPath string) error {
	opts := &slog.HandlerOptions{}
	if os.Getenv("LLM_RELAY_DEBUG") != "" {
		opts.Level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, opts))
	srv, err := NewServer(configPath, log)
	if err != nil {
		return err
	}
	if len(srv.st.Load().clientKey) == 0 {
		log.Warn("no client_key_file configured: any local process can use the relay")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return srv.Run(ctx)
}

const starterConfig = `# llm-relay config. Edits are picked up automatically within a few seconds.
listen: 127.0.0.1:18765
client_key_file: ~/.config/llm-relay/client.key
providers:                 # order = priority when two providers serve the same model
  - name: example
    base_url: https://gateway.example.com
    auth: {env: EXAMPLE_TOKEN}
routes: []                 # e.g. [{match: "claude-*", provider: example}]
`

func initConfig(configPath string) error {
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return err
	}
	keyFile := expandHome("~/.config/llm-relay/client.key")
	if _, err := os.Stat(keyFile); errors.Is(err, fs.ErrNotExist) {
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			return err
		}
		if err := os.WriteFile(keyFile, []byte("relay-"+hex.EncodeToString(buf)+"\n"), 0o600); err != nil {
			return err
		}
		fmt.Println("created", keyFile)
	}
	if _, err := os.Stat(configPath); errors.Is(err, fs.ErrNotExist) {
		if err := os.WriteFile(configPath, []byte(starterConfig), 0o600); err != nil {
			return err
		}
		fmt.Println("created", configPath)
	}
	return nil
}

func printKey(configPath string) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	if cfg.ClientKeyFile == "" {
		return errors.New("config has no client_key_file")
	}
	raw, err := os.ReadFile(cfg.ClientKeyFile)
	if err != nil {
		return err
	}
	fmt.Println(strings.TrimSpace(string(raw)))
	return nil
}

// call talks to the running relay using the address and key from the config.
func call(configPath, method, path string, show func([]byte) error) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(method, "http://"+cfg.Listen+path, nil)
	if err != nil {
		return err
	}
	if cfg.ClientKeyFile != "" {
		raw, err := os.ReadFile(cfg.ClientKeyFile)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(raw)))
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("relay not reachable on %s (launchctl kickstart -k gui/%d/%s): %w",
			cfg.Listen, os.Getuid(), agentLabel, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("relay returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return show(body)
}

func printBody(b []byte) error {
	_, err := os.Stdout.Write(b)
	return err
}

func printRoutes(b []byte) error {
	var resp struct {
		Routes    map[string][]string `json:"routes"`
		Cooldowns []Cooldown          `json:"cooldowns"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		return err
	}
	cooling := make(map[[2]string]Cooldown, len(resp.Cooldowns))
	for _, c := range resp.Cooldowns {
		cooling[[2]string{c.Provider, c.Model}] = c
	}
	chain := make(map[string]string, len(resp.Routes))
	models := make([]string, 0, len(resp.Routes))
	for m, ps := range resp.Routes {
		labels := make([]string, len(ps))
		for i, p := range ps {
			labels[i] = p
			if c, ok := cooling[[2]string{p, m}]; ok {
				labels[i] = fmt.Sprintf("%s[cooling %s: %s]", p, time.Until(c.Until).Round(time.Second), c.Reason)
			}
		}
		chain[m] = strings.Join(labels, " > ")
		models = append(models, m)
	}
	sort.Slice(models, func(i, j int) bool {
		if chain[models[i]] != chain[models[j]] {
			return chain[models[i]] < chain[models[j]]
		}
		return models[i] < models[j]
	})
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MODEL\tPROVIDERS (failover order)")
	for _, m := range models {
		fmt.Fprintf(tw, "%s\t%s\n", m, chain[m])
	}
	return tw.Flush()
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
  </dict>
</dict>
</plist>
`

func installAgent(configPath string) error {
	if _, err := LoadConfig(configPath); err != nil {
		return fmt.Errorf("refusing to install with a broken config: %w", err)
	}
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	if bin, err = filepath.EvalSymlinks(bin); err != nil {
		return err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	absConfig, err := filepath.Abs(configPath)
	if err != nil {
		return err
	}
	logFile := filepath.Join(home, "Library/Logs/llm-relay.log")
	path := strings.Join([]string{filepath.Join(home, ".local/bin"), "/opt/homebrew/bin",
		"/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"}, ":")
	plist := fmt.Sprintf(plistTemplate, agentLabel, bin, absConfig, logFile, logFile, path, home)
	plistPath := filepath.Join(home, "Library/LaunchAgents", agentLabel+".plist")
	if err := os.WriteFile(plistPath, []byte(plist), 0o644); err != nil {
		return err
	}

	domain := fmt.Sprintf("gui/%d", os.Getuid())
	// bootout fails when the agent is not loaded yet; that is the expected first-install case.
	if out, err := exec.Command("launchctl", "bootout", domain+"/"+agentLabel).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "launchctl bootout (ok on first install): %s", out)
	}
	if out, err := exec.Command("launchctl", "bootstrap", domain, plistPath).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w: %s", err, out)
	}
	fmt.Printf("installed %s\nlogs: %s\n", plistPath, logFile)
	return nil
}
