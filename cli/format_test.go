package cli

import (
	"testing"
)

func TestShellQuote(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"ls", "-la", "/tmp"}, "ls -la /tmp"},
		{[]string{"echo", "hello world"}, "echo 'hello world'"},
		{[]string{"echo", "it's"}, `echo 'it'\''s'`},
		{[]string{"echo", ""}, "echo ''"},
		{[]string{"bash", "-c", `echo "$HOME"; sleep 1`}, `bash -c 'echo "$HOME"; sleep 1'`},
	}
	for _, tt := range tests {
		if got := shellQuote(tt.args); got != tt.want {
			t.Errorf("shellQuote(%q) = %s, want %s", tt.args, got, tt.want)
		}
	}
}
