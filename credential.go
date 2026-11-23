package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const credentialCmdTimeout = 30 * time.Second

// Credential resolves and caches an upstream token. A zero TTL caches until an
// explicit refresh (e.g. after an upstream 401).
type Credential struct {
	cmd []string
	env string
	ttl time.Duration

	mu      sync.Mutex
	value   string
	fetched time.Time

	now func() time.Time
	run func(ctx context.Context, argv []string) (string, error)
}

// commandRunner is swapped in tests before any Credential is built.
var commandRunner = runCommand

func NewCredential(a AuthConfig) *Credential {
	return &Credential{cmd: a.Cmd, env: a.Env, ttl: a.TTL, now: time.Now, run: commandRunner}
}

// Refreshable reports whether a retry with a fresh token can help.
func (c *Credential) Refreshable() bool { return len(c.cmd) > 0 }

func (c *Credential) Get(ctx context.Context, refresh bool) (string, error) {
	if c.env != "" {
		v := os.Getenv(c.env)
		if v == "" {
			return "", fmt.Errorf("credential env %s is empty", c.env)
		}
		return v, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	fresh := c.value != "" && (c.ttl == 0 || c.now().Sub(c.fetched) < c.ttl)
	if fresh && !refresh {
		return c.value, nil
	}
	v, err := c.run(ctx, c.cmd)
	if err != nil {
		return "", err
	}
	c.value, c.fetched = v, c.now()
	return v, nil
}

func runCommand(ctx context.Context, argv []string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, credentialCmdTimeout)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		// stderr is safe to surface; stdout may hold a partial secret.
		return "", fmt.Errorf("credential command %s failed: %w: %s",
			argv[0], err, strings.TrimSpace(stderr.String()))
	}
	v := strings.TrimSpace(stdout.String())
	if v == "" {
		return "", errors.New("credential command " + argv[0] + " printed nothing")
	}
	return v, nil
}
