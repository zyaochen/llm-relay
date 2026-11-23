package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"time"
)

// restart waits for a moment with no client request in flight, then has
// launchd restart the agent, so updating the relay does not cut anyone's
// stream. Without -force it gives up if that moment does not come within
// -wait: a forced restart drains running requests (up to drainTimeout), but
// while it drains no new connection is accepted, and a measured 84s drain
// cost another session 7 of Claude Code's 10 retries.
func restart(configPath string, args []string) error {
	fl := flag.NewFlagSet("restart", flag.ContinueOnError)
	wait := fl.Duration("wait", 2*time.Minute, "how long to wait for an idle moment")
	force := fl.Bool("force", false, "restart even if still busy after -wait")
	if err := fl.Parse(args); err != nil {
		return err
	}
	idle, err := waitIdle(configPath, *wait, 500*time.Millisecond)
	if err != nil {
		return err
	}
	switch {
	case idle:
		fmt.Println("no requests in flight; restarting")
	case *force:
		fmt.Printf("still busy after %s; restarting with up to %s drain (new connections wait meanwhile)\n",
			*wait, drainTimeout)
	default:
		return fmt.Errorf("still busy after %s; not restarting (try again, raise -wait, or add -force)", *wait)
	}
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), agentLabel)
	if out, err := exec.Command("launchctl", "kickstart", "-k", target).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, out)
	}
	return nil
}

// waitIdle polls the relay's health until active_requests is 0 or wait ends.
func waitIdle(configPath string, wait, every time.Duration) (bool, error) {
	deadline := time.Now().Add(wait)
	for {
		var active int32
		err := call(configPath, "GET", "/relay/health", func(b []byte) error {
			var h struct {
				Active int32 `json:"active_requests"`
			}
			if err := json.Unmarshal(b, &h); err != nil {
				return err
			}
			active = h.Active
			return nil
		})
		if err != nil {
			return false, err
		}
		if active == 0 {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(every)
	}
}
