//go:build linux

package core

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os/exec"
	"sync"
)

// Manager starts jobs and keeps track of them. Safe for concurrent use.
type Manager struct {
	mu     sync.Mutex      // guards jobs and closed
	jobs   map[string]*Job // keyed by job ID
	closed bool
}

func NewManager() *Manager {
	return &Manager{jobs: make(map[string]*Job)}
}

// Start runs command with args as a new job owned by owner.
// If the process fails to start, no job is registered and the error is returned.
func (m *Manager) Start(owner, command string, args []string) (*Job, error) {
	// TODO: Limit the number of running jobs.
	// Each running job holds a goroutine blocked in a syscall (see job.wait()), which counts against the Go runtime's 10k os thread limit.

	if owner == "" {
		return nil, ErrEmptyOwner
	}
	if command == "" {
		return nil, ErrEmptyCommand
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrManagerClosed
	}
	m.mu.Unlock()

	path, err := exec.LookPath(command)
	if err != nil {
		return nil, fmt.Errorf("look up command %q: %w", command, err)
	}

	j := newJob(rand.Text(), owner, command, path, args)
	if err := j.cmd.Start(); err != nil {
		return nil, fmt.Errorf("start command %q: %w", command, err)
	}
	go j.wait()

	m.mu.Lock()
	if m.closed {
		// Shutdown ran while the process was starting and didn't see this job
		m.mu.Unlock()
		_ = j.Stop(context.Background())
		return nil, ErrManagerClosed
	}
	m.jobs[j.id] = j
	m.mu.Unlock()
	return j, nil
}

func (m *Manager) Get(id string) (*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return j, nil
}

// Shutdown rejects new jobs, kills every running job, and waits until all jobs have reached a terminal state or ctx is done.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	var jobs []*Job
	for _, j := range m.jobs {
		select {
		case <-j.done:
			// Already in a terminal state, so there is nothing to kill or wait for
		default:
			jobs = append(jobs, j)
		}
	}
	m.mu.Unlock()

	// Kill every job before waiting on any, so jobs die in parallel
	var errs []error
	for _, j := range jobs {
		if err := j.kill(); err != nil && !errors.Is(err, ErrNotRunning) {
			errs = append(errs, fmt.Errorf("kill job %s: %w", j.id, err))
		}
	}
	for _, j := range jobs {
		select {
		case <-j.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.Join(errs...)
}
