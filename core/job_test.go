//go:build linux

package core

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestJobExitSuccess(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", "exit 0")
	waitDone(t, j)

	st := j.Status()
	if st.State != StateExited || st.ExitCode != 0 {
		t.Fatalf("state, exit code = %v, %d; want %v, 0", st.State, st.ExitCode, StateExited)
	}
}

func TestJobExitFailure(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", "exit 3")
	waitDone(t, j)

	if st := j.Status(); st.State != StateExited || st.ExitCode != 3 {
		t.Fatalf("state, exit code = %v, %d; want %v, 3", st.State, st.ExitCode, StateExited)
	}
}

func TestJobSignaled(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", "kill -KILL $$")
	waitDone(t, j)

	if st := j.Status(); st.State != StateSignaled || st.Signal != syscall.SIGKILL {
		t.Fatalf("state, signal = %v, %v; want %v, SIGKILL", st.State, st.Signal, StateSignaled)
	}
}

func TestJobOutputCombinesStdoutAndStderr(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", "echo out; echo err >&2; echo out2")

	if got, want := readAll(t, j), "out\nerr\nout2\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestJobOutputStreamsLive(t *testing.T) {
	// The job prints a line, then waits for the test to create a file
	gate := filepath.Join(t.TempDir(), "gate")
	script := `echo first; while [ ! -e "$1" ]; do sleep 0.01; done; echo second`
	j := mustStart(t, newTestManager(t), "sh", "-c", script, "sh", gate)

	r := bufio.NewReader(j.Output(testContext(t)))
	if line, err := r.ReadString('\n'); err != nil || line != "first\n" {
		t.Fatalf("ReadString = %q, %v; want %q, nil", line, err, "first\n")
	}

	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if rest, err := io.ReadAll(r); err != nil || string(rest) != "second\n" {
		t.Fatalf("ReadAll = %q, %v; want %q, nil", rest, err, "second\n")
	}

	// A reader created after the job ended still gets everything.
	if got, want := readAll(t, j), "first\nsecond\n"; got != want {
		t.Fatalf("late reader output = %q, want %q", got, want)
	}
}

func TestJobStop(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sleep", "100")

	if err := j.Stop(testContext(t)); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st := j.Status(); st.State != StateStopped || st.Signal != syscall.SIGKILL {
		t.Fatalf("state, signal = %v, %v; want %v, SIGKILL", st.State, st.Signal, StateStopped)
	}
	if err := j.Stop(testContext(t)); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("second Stop error = %v, want ErrNotRunning", err)
	}
}

func TestJobStopKillsProcessTree(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", "sleep 100 & echo $!; wait")
	pid := readPID(t, j)

	if err := j.Stop(testContext(t)); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	waitProcessGone(t, pid)
}

func TestJobLeaderExitKillsGroup(t *testing.T) {
	// The background sleep keeps the output pipe open after sh exits
	j := mustStart(t, newTestManager(t), "sh", "-c", "sleep 100 & echo $!")
	pid := readPID(t, j)

	waitDone(t, j)
	if st := j.Status(); st.State != StateExited || st.ExitCode != 0 {
		t.Fatalf("state, exit code = %v, %d; want %v, 0", st.State, st.ExitCode, StateExited)
	}
	waitProcessGone(t, pid)
}

func TestJobConcurrentStop(t *testing.T) {
	// Stop races with the job exiting on its own, with other Stops, and with readers.
	m := newTestManager(t)
	for i := range 20 {
		j := mustStart(t, m, "true")

		const stoppers = 4
		errs := make([]error, stoppers)
		var wg sync.WaitGroup
		for k := range stoppers {
			wg.Add(2)
			go func() {
				defer wg.Done()
				errs[k] = j.Stop(testContext(t))
			}()
			go func() {
				defer wg.Done()
				_ = j.Status()
				readAll(t, j)
			}()
		}
		wg.Wait()

		for _, err := range errs {
			if err != nil && !errors.Is(err, ErrNotRunning) {
				t.Fatalf("iteration %d: Stop error = %v, want nil or ErrNotRunning", i, err)
			}
		}

		// A nil Stop may still end in StateExited if the kill hit an already exited leader.
		// StateStopped is only valid if some Stop actually sent the kill.
		st := j.Status()
		switch {
		case st.State == StateExited && st.ExitCode == 0:
		case st.State == StateStopped && st.Signal == syscall.SIGKILL && slices.Contains(errs, nil):
		default:
			t.Fatalf("iteration %d: status = %+v after Stop errors %v", i, st, errs)
		}
	}
}

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m := NewManager()
	t.Cleanup(func() {
		if err := m.Shutdown(testContext(t)); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return m
}

// testContext returns a context that is canceled after testTimeout or when the test ends
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

func mustStart(t *testing.T, m *Manager, cmd string, args ...string) *Job {
	t.Helper()
	j, err := m.Start("alice", cmd, args)
	if err != nil {
		t.Fatalf("Start(%q, %q): %v", cmd, args, err)
	}
	return j
}

func waitDone(t *testing.T, j *Job) {
	t.Helper()
	select {
	case <-j.Done():
	case <-time.After(testTimeout):
		t.Fatalf("job %s did not finish within %v", j.ID(), testTimeout)
	}
}

func readAll(t *testing.T, j *Job) string {
	t.Helper()
	b, err := io.ReadAll(j.Output(testContext(t)))
	if err != nil {
		t.Errorf("reading output of job %s: %v", j.ID(), err)
	}
	return string(b)
}

// readPID parses the first line of the job's output as a pid
func readPID(t *testing.T, j *Job) int {
	t.Helper()
	line, err := bufio.NewReader(j.Output(testContext(t))).ReadString('\n')
	if err != nil {
		t.Fatalf("reading pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("parsing pid from %q: %v", line, err)
	}
	return pid
}

// waitProcessGone fails the test if pid is still alive after testTimeout.
// A zombie counts as gone: it has been killed and only awaits reaping by init.
func waitProcessGone(t *testing.T, pid int) {
	t.Helper()
	for deadline := time.Now().Add(testTimeout); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		// The state field follows the parenthesized command name, which may contain spaces.
		if err != nil || strings.HasPrefix(string(b[strings.LastIndexByte(string(b), ')')+1:]), " Z") {
			return
		}
	}
	t.Fatalf("process %d still alive after %v", pid, testTimeout)
}
