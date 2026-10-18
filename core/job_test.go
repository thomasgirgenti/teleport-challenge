//go:build linux

package core

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestJobExitSuccess(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", "exit 0")
	<-j.Done()

	st := j.Status()
	if st.State != StateExited || st.ExitCode != 0 {
		t.Fatalf("state, exit code = %v, %d; want %v, 0", st.State, st.ExitCode, StateExited)
	}
}

func TestJobExitFailure(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", "exit 3")
	<-j.Done()

	if st := j.Status(); st.State != StateExited || st.ExitCode != 3 {
		t.Fatalf("state, exit code = %v, %d; want %v, 3", st.State, st.ExitCode, StateExited)
	}
}

func TestJobSignaled(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", "kill -KILL $$")
	<-j.Done()

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
	// The job prints a line, then blocks reading the FIFO until the test writes to it
	gate := newFIFO(t)
	script := `echo first; read line < "$1"; echo second`
	j := mustStart(t, newTestManager(t), "sh", "-c", script, "sh", gate)

	r := bufio.NewReader(j.Output(t.Context()))
	if line, err := r.ReadString('\n'); err != nil || line != "first\n" {
		t.Fatalf("ReadString = %q, %v; want %q, nil", line, err, "first\n")
	}

	// Opening for writing blocks until the job opens the FIFO for reading
	if err := os.WriteFile(gate, []byte("go\n"), 0); err != nil {
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
	j := mustStart(t, newTestManager(t), "cat", newFIFO(t))

	if err := j.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st := j.Status(); st.State != StateStopped || st.Signal != syscall.SIGKILL {
		t.Fatalf("state, signal = %v, %v; want %v, SIGKILL", st.State, st.Signal, StateStopped)
	}
	if err := j.Stop(t.Context()); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("second Stop error = %v, want ErrNotRunning", err)
	}
}

func TestJobStopKillsProcessTree(t *testing.T) {
	j := mustStart(t, newTestManager(t), "sh", "-c", `cat "$1" & echo $!; wait`, "sh", newFIFO(t))
	waitExit := watchProcess(t, readPID(t, j))

	if err := j.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	waitExit()
}

func TestJobLeaderExitKillsGroup(t *testing.T) {
	// The background cat never exits on its own and keeps the output pipe open after sh exits
	j := mustStart(t, newTestManager(t), "sh", "-c", `cat "$1" & echo $!`, "sh", newFIFO(t))
	waitExit := watchProcess(t, readPID(t, j))

	<-j.Done()
	if st := j.Status(); st.State != StateExited || st.ExitCode != 0 {
		t.Fatalf("state, exit code = %v, %d; want %v, 0", st.State, st.ExitCode, StateExited)
	}
	waitExit()
}

func TestJobKillAtEachLifecycleStage(t *testing.T) {
	// Each case drives the job to the stage where a kill lands, rather than racing a kill against the job exiting
	tests := []struct {
		name      string
		argv      []string
		advance   func(j *Job)
		wantErr   error
		wantState State
	}{
		{
			// The leader can't exit on its own, so the kill is what ends it
			name:      "leader running",
			argv:      []string{"cat", newFIFO(t)},
			advance:   func(j *Job) {},
			wantState: StateStopped,
		},
		{
			// The leader has exited but is unreaped, so the group's only member is a zombie.
			// The zombie still belongs to the group, so the kill succeeds rather than failing with ESRCH, but the job exited on its own.
			name:      "leader zombie",
			argv:      []string{"true"},
			advance:   func(j *Job) { waitOnLeader(j.cmd.Process.Pid) },
			wantState: StateExited,
		},
		{
			// The job has finished, so the kill reports it isn't running and leaves the recorded state alone
			name:      "leader reaped",
			argv:      []string{"true"},
			advance:   func(j *Job) { j.wait() },
			wantErr:   ErrNotRunning,
			wantState: StateExited,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := startUnwaited(t, tt.argv[0], tt.argv[1:]...)
			tt.advance(j)

			if err := j.kill(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("kill error = %v, want %v", err, tt.wantErr)
			}

			// Finish the job unless advance already did
			select {
			case <-j.done:
			default:
				j.wait()
			}

			st := j.Status()
			switch tt.wantState {
			case StateStopped:
				if st.State != StateStopped || st.Signal != syscall.SIGKILL {
					t.Fatalf("state, signal = %v, %v; want %v, SIGKILL", st.State, st.Signal, StateStopped)
				}
			case StateExited:
				if st.State != StateExited || st.ExitCode != 0 {
					t.Fatalf("state, exit code = %v, %d; want %v, 0", st.State, st.ExitCode, StateExited)
				}
			}
		})
	}
}

func TestJobConcurrentStop(t *testing.T) {
	// Stop races with other Stops and with readers.
	// The leader can't exit on its own, so the outcome doesn't depend on which goroutine wins.
	m := newTestManager(t)
	fifo := newFIFO(t)
	for i := range 20 {
		j := mustStart(t, m, "cat", fifo)

		const stoppers = 4
		errs := make([]error, stoppers)
		var wg sync.WaitGroup
		for k := range stoppers {
			wg.Add(2)
			go func() {
				defer wg.Done()
				errs[k] = j.Stop(t.Context())
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

		// The first kill always reaches the running leader
		if !slices.Contains(errs, nil) {
			t.Fatalf("iteration %d: Stop errors = %v, want at least one nil", i, errs)
		}
		if st := j.Status(); st.State != StateStopped || st.Signal != syscall.SIGKILL {
			t.Fatalf("iteration %d: state, signal = %v, %v; want %v, SIGKILL", i, st.State, st.Signal, StateStopped)
		}
	}
}

// startUnwaited starts a job without its wait goroutine, so the test drives the lifecycle by calling waitOnLeader, kill, and wait itself
func startUnwaited(t *testing.T, command string, args ...string) *Job {
	t.Helper()
	path, err := exec.LookPath(command)
	if err != nil {
		t.Fatal(err)
	}
	j := newJob(t.Name(), "alice", command, path, args)
	if err := j.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-j.done:
		default:
			// The test failed before finishing the job; kill and reap it
			_ = syscall.Kill(-j.cmd.Process.Pid, syscall.SIGKILL)
			j.wait()
		}
	})
	return j
}

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m := NewManager()
	t.Cleanup(func() {
		// t.Context is already canceled when cleanups run
		if err := m.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return m
}

func mustStart(t *testing.T, m *Manager, cmd string, args ...string) *Job {
	t.Helper()
	j, err := m.Start("alice", cmd, args)
	if err != nil {
		t.Fatalf("Start(%q, %q): %v", cmd, args, err)
	}
	return j
}

func readAll(t *testing.T, j *Job) string {
	t.Helper()
	b, err := io.ReadAll(j.Output(t.Context()))
	if err != nil {
		t.Errorf("reading output of job %s: %v", j.ID(), err)
	}
	return string(b)
}

// readPID parses the first line of the job's output as a pid
func readPID(t *testing.T, j *Job) int {
	t.Helper()
	line, err := bufio.NewReader(j.Output(t.Context())).ReadString('\n')
	if err != nil {
		t.Fatalf("reading pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("parsing pid from %q: %v", line, err)
	}
	return pid
}

// newFIFO returns the path of a new FIFO.
// Opening it blocks until the other end is opened, so `cat` on a FIFO that is never written blocks until it is killed.
func newFIFO(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fifo")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// watchProcess opens a pidfd for pid and returns a function that blocks until the process exits.
// A pidfd will refer to the same process even if pid is reused, so call watchProcess while pid is known to be alive.
func watchProcess(t *testing.T, pid int) (waitExit func()) {
	t.Helper()
	fd, err := unix.PidfdOpen(pid, 0)
	if errors.Is(err, unix.ESRCH) {
		// Already exited and reaped
		return func() {}
	}
	if err != nil {
		t.Fatalf("pidfd_open(%d): %v", pid, err)
	}
	t.Cleanup(func() { unix.Close(fd) })

	return func() {
		t.Helper()
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		for {
			_, err := unix.Poll(fds, -1)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil {
				t.Fatalf("poll pidfd of process %d: %v", pid, err)
			}
			return
		}
	}
}
