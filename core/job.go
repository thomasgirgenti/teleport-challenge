//go:build linux

package core

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"slices"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const waitDelay = 2 * time.Second

type State int

const (
	StateRunning  State = iota + 1
	StateExited         // exited on its own; Status.ExitCode is set
	StateSignaled       // killed by a signal not sent by Stop; Status.Signal is set
	StateStopped        // killed by Stop; Status.Signal is set
)

// Status is a point-in-time snapshot of a Job
type Status struct {
	ID        string
	Owner     string
	Command   string
	Args      []string
	State     State
	ExitCode  int            // valid only for StateExited
	Signal    syscall.Signal // valid only for StateSignaled and StateStopped
	StartedAt time.Time
	EndedAt   time.Time // zero while running
}

type Job struct {
	id, owner string
	command   string
	args      []string
	cmd       *exec.Cmd
	output    *outputBuffer
	done      chan struct{} // closed after the final state is recorded

	mu            sync.Mutex // guards the fields below
	state         State
	exitCode      int
	signal        syscall.Signal
	startedAt     time.Time
	endedAt       time.Time
	leaderExited  bool // set before leader is reaped; guards signaling its pgid
	stopRequested bool
}

func newJob(id, owner, command, path string, args []string) *Job {
	output := newOutputBuffer()
	return &Job{
		id:        id,
		owner:     owner,
		command:   command,
		args:      slices.Clone(args),
		output:    output,
		done:      make(chan struct{}),
		state:     StateRunning,
		startedAt: time.Now(),
		cmd: &exec.Cmd{
			Path: path,
			Args: append([]string{command}, args...),

			// Stdout and Stderr are the same writer. os/exec gives the child a single pipe for both and their interleaving is preserved.
			Stdout: output,
			Stderr: output,

			SysProcAttr: &syscall.SysProcAttr{
				// Child leads its own process group (pgid == pid), so the whole tree can be killed with a single signal
				Setpgid: true,
			},

			// waitDelay bounds how long Wait reads output after the job's leader has exited
			WaitDelay: waitDelay,
		},
	}
}

func (j *Job) ID() string { return j.id }

func (j *Job) Owner() string { return j.owner }

// Done returns a channel that is closed once the job has reached a terminal state and all of its output has been captured
func (j *Job) Done() <-chan struct{} { return j.done }

func (j *Job) Status() Status {
	j.mu.Lock()
	defer j.mu.Unlock()
	return Status{
		ID:        j.id,
		Owner:     j.owner,
		Command:   j.command,
		Args:      slices.Clone(j.args),
		State:     j.state,
		ExitCode:  j.exitCode,
		Signal:    j.signal,
		StartedAt: j.startedAt,
		EndedAt:   j.endedAt,
	}
}

// Output returns a reader of the job's combined stdout and stderr, starting from the first byte.
// Read blocks until new output is available; returns io.EOF once the job has ended and all output has been read or returns ctx.Err() when ctx is done.
// Each call returns an independent reader.
func (j *Job) Output(ctx context.Context) io.Reader {
	return &outputReader{ctx: ctx, buf: j.output}
}

// Stop kills the job's process group with SIGKILL and waits until the job reaches a terminal state or ctx is done.
func (j *Job) Stop(ctx context.Context) error {
	err := j.kill()
	if err != nil && !errors.Is(err, ErrNotRunning) {
		return err
	}

	select {
	case <-j.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// kill sends SIGKILL to the job's process group without waiting for it to exit
func (j *Job) kill() error {
	// wait sets leaderExited under the same lock before reaping the leader, so the pgid can't be reused while we signal it.
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.leaderExited {
		return ErrNotRunning
	}

	// Set before the kill so wait records StateStopped rather than StateSignaled
	j.stopRequested = true

	if err := unix.Kill(-j.cmd.Process.Pid, unix.SIGKILL); err != nil {
		j.stopRequested = false
		if errors.Is(err, unix.ESRCH) {
			return ErrNotRunning
		}
		return err
	}
	return nil
}

// wait is the job's only goroutine.
// It waits for the process to exit, kills any processes left in its group, captures the remaining output, and records the final state.
// TODO: use cgroups to more effectively manage job & child process lifecycles
func (j *Job) wait() {
	defer close(j.done)

	pgid := j.cmd.Process.Pid

	// Block until the process group leader exits w/o reaping -- prevents pgid reuse before kill signal is issued
	j.waitOnLeader(pgid)

	// Kill group members left behind. They were reparented to init, which reaps them.
	j.mu.Lock()
	j.leaderExited = true
	_ = unix.Kill(-pgid, unix.SIGKILL)
	j.mu.Unlock()

	// Reap the leader and wait for the output copy to reach EOF.
	// If a process that left the group still holds the pipe, WaitDelay closes it.
	// ProcessState is set once the leader is reaped, even if Wait returns exec.ErrWaitDelay.
	_ = j.cmd.Wait()

	// Record the final state before closing output, so a reader that sees io.EOF also sees the terminal Status
	j.recordExit()

	// Readers drain the remaining output, then get io.EOF
	j.output.Close()
}

// recordExit records the job's terminal state from the reaped leader's ProcessState
func (j *Job) recordExit() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.endedAt = time.Now()
	if j.cmd.ProcessState == nil {
		// Wait failed to reap the leader
		j.state, j.exitCode = StateExited, -1
		return
	}
	ws := j.cmd.ProcessState.Sys().(syscall.WaitStatus)
	switch {
	case ws.Signaled() && j.stopRequested:
		j.state, j.signal = StateStopped, ws.Signal()
	case ws.Signaled():
		j.state, j.signal = StateSignaled, ws.Signal()
	default:
		j.state, j.exitCode = StateExited, ws.ExitStatus()
	}
}

// waitOnLeader blocks until the process group leader exits, but leaves it unreaped (WNOWAIT). Ignores EINTR.
func (j *Job) waitOnLeader(pgid int) {

	// cmd.Wait shouldn't be used here because:
	// - It reaps the main process (leader) before returning. If the pid/pgid is reissued, an unrelated process group may be killed.
	// - If a child process holds the output pipe, `cmd.Wait` blocks until WaitDelay expires. When Go closes the pipe, the children processes still need to be killed.
	var info unix.Siginfo
	for {
		// unix.Waitid with WNOWAIT breaks macOS compatibility
		err := unix.Waitid(unix.P_PID, pgid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if !errors.Is(err, unix.EINTR) {
			// retry on EINTR
			break
		}
	}
}
