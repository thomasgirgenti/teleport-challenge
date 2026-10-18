package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// run runs jobctl with args and returns its exit code, stdout, and stderr
func run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(t.Context(), args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// TestUsageErrors checks that bad invocations fail with exitUsage before loading certs or connecting.
// None of these have usable certs, so reaching the network would fail with exitError instead.
func TestUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no command", nil},
		{"unknown command", []string{"list"}},
		{"unknown global flag", []string{"--bogus", "status", "ID"}},
		{"start without command", []string{"start"}},
		{"start with only --", []string{"start", "--"}},
		{"start with unknown flag", []string{"start", "-la"}},
		{"stop without ID", []string{"stop"}},
		{"stop with two IDs", []string{"stop", "A", "B"}},
		{"status without ID", []string{"status"}},
		{"status with two IDs", []string{"status", "A", "B"}},
		{"logs without ID", []string{"logs"}},
		{"logs with two IDs", []string{"logs", "A", "B"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := run(t, tt.args...)
			if code != exitUsage {
				t.Errorf("exit code = %d, want %d; stderr: %s", code, exitUsage, stderr)
			}
			if stdout != "" {
				t.Errorf("stdout = %q, want empty", stdout)
			}
			if !strings.Contains(stderr, "usage: jobctl") {
				t.Errorf("stderr = %q, want usage", stderr)
			}
		})
	}
}

func TestHelp(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"status", "-h"}} {
		code, _, stderr := run(t, args...)
		if code != exitOK || !strings.Contains(stderr, "usage: jobctl") {
			t.Errorf("Run(%q) = %d, stderr %q; want %d and usage", args, code, stderr, exitOK)
		}
	}
}

func TestBadCertFiles(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	code, _, stderr := run(t, "--cert", missing, "--key", missing, "status", "ID")
	if code != exitError {
		t.Errorf("exit code = %d, want %d", code, exitError)
	}
	if !strings.HasPrefix(stderr, "error: load client key pair:") {
		t.Errorf("stderr = %q, want key pair error", stderr)
	}
}
