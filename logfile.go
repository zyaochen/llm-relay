package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// The log grew ~1 MB a day with nothing trimming it. The relay now writes it
// itself (LLM_RELAY_LOG, set by install-agent) and keeps logKeep rotated files.
const (
	logMaxBytes = 20 << 20
	logKeep     = 3
)

// defaultLogPath is where install-agent points the relay's log.
func defaultLogPath() string {
	if runtime.GOOS == "darwin" {
		return expandHome("~/Library/Logs/llm-relay.log")
	}
	if d := os.Getenv("XDG_STATE_HOME"); d != "" {
		return filepath.Join(d, "llm-relay", "llm-relay.log")
	}
	return expandHome("~/.local/state/llm-relay/llm-relay.log")
}

// logFiles lists path and its rotations, oldest first, skipping missing ones.
func logFiles(path string) []string {
	var out []string
	for i := logKeep; i >= 1; i-- {
		if _, err := os.Stat(fmt.Sprintf("%s.%d", path, i)); err == nil {
			out = append(out, fmt.Sprintf("%s.%d", path, i))
		}
	}
	if _, err := os.Stat(path); err == nil {
		out = append(out, path)
	}
	return out
}

// rotatingFile is an append-only log that renames itself to path.1 (shifting
// older ones up to path.<keep>) once it would grow past max. slog writes one
// record per Write, so a record never straddles two files.
type rotatingFile struct {
	path      string
	max       int64
	keep      int
	mu        sync.Mutex
	f         *os.File
	size      int64
	rotateErr error // last rotation failure, reported once per change
}

func openRotating(path string, max int64, keep int) (*rotatingFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	r := &rotatingFile{path: path, max: max, keep: keep}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rotatingFile) open() error {
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	r.f, r.size = f, st.Size()
	return nil
}

func (r *rotatingFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size > 0 && r.size+int64(len(p)) > r.max {
		if err := r.rotate(); err != nil {
			// Keep logging into the current file rather than losing records;
			// say so on stderr (launchd/journald keep that) once per failure.
			if r.rotateErr == nil || r.rotateErr.Error() != err.Error() {
				fmt.Fprintln(os.Stderr, "llm-relay: log rotation failed:", err)
			}
			r.rotateErr = err
		} else {
			r.rotateErr = nil
		}
	}
	n, err := r.f.Write(p)
	r.size += int64(n)
	return n, err
}

// rotate shifts the files up by one. Whatever fails, the log is reopened so
// writing goes on (into the unrotated file if a rename failed).
func (r *rotatingFile) rotate() error {
	cerr := r.f.Close()
	var rerr error
	for i := r.keep - 1; i >= 1 && rerr == nil; i-- {
		err := os.Rename(fmt.Sprintf("%s.%d", r.path, i), fmt.Sprintf("%s.%d", r.path, i+1))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			rerr = err
		}
	}
	if rerr == nil {
		rerr = os.Rename(r.path, r.path+".1")
	}
	return errors.Join(cerr, rerr, r.open())
}

func (r *rotatingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.f.Close()
}
