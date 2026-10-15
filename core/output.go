package core

import (
	"context"
	"io"
	"sync"
)

// outputBuffer is an append-only, in-memory record of a job's output.
// Readers wait for new data on notify, which is closed and replaced on every Write and on Close, so a single close wakes every waiting reader.
// Output is never truncated, since the worker is assumed to have unlimited memory.
//
// TODO: In production, store output in fixed-size chunks with a per-job cap, and spill to disk or evict output of finished jobs.
type outputBuffer struct {
	mu     sync.Mutex    // guards data, closed, and notify
	data   []byte        // all output written so far
	closed bool          // true once the job's output pipe has hit EOF
	notify chan struct{} // closed and replaced on every Write and Close
}

func newOutputBuffer() *outputBuffer {
	return &outputBuffer{notify: make(chan struct{})}
}

// Write appends p to the buffer and notifies every waiting reader. Never blocks on readers.
func (b *outputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	b.data = append(b.data, p...)
	close(b.notify)
	b.notify = make(chan struct{})
	return len(p), nil
}

// Close marks the end of output. Readers drain the remaining data and then get io.EOF.
func (b *outputBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	close(b.notify)
}

// readAt copies data starting at off into p and returns the number of bytes copied.
// If there is no data at off and the buffer is still open, it returns a channel that is closed when new data is written or the buffer is closed.
func (b *outputBuffer) readAt(p []byte, off int) (int, <-chan struct{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if off < len(b.data) {
		return copy(p, b.data[off:]), b.notify, nil
	}
	if b.closed {
		return 0, nil, io.EOF
	}
	return 0, b.notify, nil
}

// outputReader reads a job's output from the first byte.
// Each reader keeps its own offset, so readers are independent of each other and of the job.
// Not safe for concurrent use by multiple goroutines.
type outputReader struct {
	ctx context.Context
	buf *outputBuffer
	off int
}

// Read copies the next available output into p.
// If no output is available, it blocks until the job writes more (n > 0), the job ends and all output has been read (io.EOF), or the reader's context is done (ctx.Err()).
func (r *outputReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		n, wait, err := r.buf.readAt(p, r.off)
		if n > 0 || err != nil {
			r.off += n
			return n, err
		}
		select {
		case <-wait: // new data or EOF; re-check
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
}
