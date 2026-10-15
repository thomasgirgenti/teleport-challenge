//go:build linux

package core

import (
	"errors"
	"os/exec"
	"sync"
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
	running := mustStart(t, m, "sleep", "100")
	exited := mustStart(t, m, "true")
	waitDone(t, exited)

	if err := m.Shutdown(testContext(t)); err != nil {
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

func TestManagerStartDuringShutdown(t *testing.T) {
	// Starters keep starting jobs until the manager closes, so some Start calls are mid-flight when Shutdown runs
	m := newTestManager(t)
	const starters = 8
	jobs := make([][]*Job, starters)
	var running, done sync.WaitGroup // running: every starter has made its first Start call
	running.Add(starters)
	for i := range starters {
		done.Add(1)
		go func() {
			defer done.Done()
			for n := 0; ; n++ {
				j, err := m.Start("alice", "sleep", []string{"100"})
				if n == 0 {
					running.Done()
				}
				if errors.Is(err, ErrManagerClosed) {
					return
				}
				if err != nil {
					t.Errorf("Start: %v", err)
					return
				}
				jobs[i] = append(jobs[i], j)
			}
		}()
	}

	running.Wait()
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	done.Wait()

	// Every job Start returned was registered before Shutdown closed the manager, so Shutdown must have stopped it
	for _, js := range jobs {
		for _, j := range js {
			select {
			case <-j.Done():
			default:
				t.Fatalf("job %s still running after Shutdown returned", j.ID())
			}
			if st := j.Status(); st.State != StateStopped {
				t.Fatalf("job %s: state = %v, want %v", j.ID(), st.State, StateStopped)
			}
		}
	}
}
