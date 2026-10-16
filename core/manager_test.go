//go:build linux

package core

import (
	"errors"
	"os/exec"
	"testing"
)

func TestManagerStartInvalid(t *testing.T) {
	tests := []struct {
		name    string
		owner   string
		cmd     string
		wantErr error
	}{
		{"empty owner", "", "true", ErrEmptyOwner},
		{"empty command", "alice", "", ErrEmptyCommand},
		{"command not found", "alice", "no-such-command-7f3a", exec.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t)
			if j, err := m.Start(tt.owner, tt.cmd, nil); !errors.Is(err, tt.wantErr) || j != nil {
				t.Fatalf("Start = %v, %v; want nil, %v", j, err, tt.wantErr)
			}
		})
	}
}

func TestManagerGet(t *testing.T) {
	m := newTestManager(t)
	j := mustStart(t, m, "true")

	if got, err := m.Get(j.ID()); err != nil || got != j {
		t.Fatalf("Get(%q) = %v, %v; want %v, nil", j.ID(), got, err, j)
	}
	if _, err := m.Get("unknown"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestManagerShutdown(t *testing.T) {
	m := NewManager()
	running := mustStart(t, m, "cat", newFIFO(t))
	exited := mustStart(t, m, "true")
	<-exited.Done()

	if err := m.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if st := running.Status(); st.State != StateStopped {
		t.Errorf("running job: state = %v, want %v", st.State, StateStopped)
	}
	if st := exited.Status(); st.State != StateExited {
		t.Errorf("exited job: state = %v, want %v", st.State, StateExited)
	}
	if _, err := m.Start("alice", "true", nil); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("Start after Shutdown error = %v, want ErrManagerClosed", err)
	}
}
