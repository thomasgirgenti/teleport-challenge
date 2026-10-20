//go:build linux

package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"

	pb "github.com/thomasgirgenti/teleport-challenge/gen/worker/v1"
)

func TestStartJobAndStatus(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	id := startJob(t, alice, "sh", "-c", "exit 3")
	readOutput(t, alice, id)

	st := getStatus(t, alice, id)
	if st.GetJobId() != id || st.GetOwner() != "alice" || st.GetCommand() != "sh" || !slices.Equal(st.GetArgs(), []string{"-c", "exit 3"}) {
		t.Errorf("job_id, owner, command, args = %q, %q, %q, %q; want %q, alice, sh, [-c exit 3]",
			st.GetJobId(), st.GetOwner(), st.GetCommand(), st.GetArgs(), id)
	}
	if st.GetState() != pb.JobState_JOB_STATE_EXITED || st.GetExitCode() != 3 || st.HasSignal() {
		t.Errorf("state, exit code, has signal = %v, %d, %v; want EXITED, 3, false", st.GetState(), st.GetExitCode(), st.HasSignal())
	}
	if !st.HasStartedAt() || !st.HasEndedAt() || st.GetEndedAt().AsTime().Before(st.GetStartedAt().AsTime()) {
		t.Errorf("started_at, ended_at = %v, %v; want both set, in order", st.GetStartedAt(), st.GetEndedAt())
	}
}

func TestStatusWhileRunning(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	id := startJob(t, alice, "cat", newFIFO(t))

	st := getStatus(t, alice, id)
	if st.GetState() != pb.JobState_JOB_STATE_RUNNING || st.HasExitCode() || st.HasEndedAt() {
		t.Fatalf("state, has exit code, has ended_at = %v, %v, %v; want RUNNING, false, false", st.GetState(), st.HasExitCode(), st.HasEndedAt())
	}
}

func TestStartJobInvalid(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	// Executable, but not a binary or script the kernel can run, so it passes the lookup and fails to start
	noShebang := filepath.Join(t.TempDir(), "no-shebang")
	if err := os.WriteFile(noShebang, []byte("echo hi\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		command string
	}{
		{"empty command", ""},
		{"command not found", "no-such-command-7f3a"},
		{"exec format error", noShebang},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := alice.StartJob(t.Context(), pb.StartJobRequest_builder{Command: new(tt.command)}.Build())
			wantCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestSignaledStatus(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	tests := []struct {
		name       string
		signal     string
		wantSignal string
	}{
		{"named signal", "-KILL", "SIGKILL"},
		// Real-time signals have no name, but still terminate the process by default
		{"unnamed signal", "-40", "signal 40"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := startJob(t, alice, "sh", "-c", "kill "+tt.signal+" $$")
			readOutput(t, alice, id)

			st := getStatus(t, alice, id)
			if st.GetState() != pb.JobState_JOB_STATE_SIGNALED || st.GetSignal() != tt.wantSignal || st.HasExitCode() {
				t.Fatalf("state, signal, has exit code = %v, %q, %v; want SIGNALED, %q, false", st.GetState(), st.GetSignal(), st.HasExitCode(), tt.wantSignal)
			}
		})
	}
}

func TestStopJob(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	id := startJob(t, alice, "cat", newFIFO(t))
	if err := stopJob(t, alice, id); err != nil {
		t.Fatalf("StopJob: %v", err)
	}

	// StopJob returns once the job has ended, so the final state is already recorded
	st := getStatus(t, alice, id)
	if st.GetState() != pb.JobState_JOB_STATE_STOPPED || st.GetSignal() != "SIGKILL" {
		t.Fatalf("state, signal = %v, %q; want STOPPED, SIGKILL", st.GetState(), st.GetSignal())
	}

	wantCode(t, stopJob(t, alice, id), codes.FailedPrecondition)
}

func TestAuthorization(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)
	bob := ts.client(t, ts.pki.bob)

	aliceJob := startJob(t, alice, "cat", newFIFO(t))

	rpcs := []struct {
		name string
		call func(t *testing.T, c pb.WorkerServiceClient, id string) error
	}{
		{"GetJobStatus", func(t *testing.T, c pb.WorkerServiceClient, id string) error {
			_, err := c.GetJobStatus(t.Context(), pb.GetJobStatusRequest_builder{JobId: new(id)}.Build())
			return err
		}},
		{"StopJob", func(t *testing.T, c pb.WorkerServiceClient, id string) error {
			return stopJob(t, c, id)
		}},
		{"StreamJobOutput", func(t *testing.T, c pb.WorkerServiceClient, id string) error {
			_, err := io.ReadAll(openStream(t, t.Context(), c, id))
			return err
		}},
	}
	for _, rpc := range rpcs {
		t.Run(rpc.name, func(t *testing.T) {
			// Another user's job must look exactly like a job that doesn't exist
			errOther := rpc.call(t, bob, aliceJob)
			errUnknown := rpc.call(t, bob, "unknown")
			wantCode(t, errOther, codes.NotFound)
			wantCode(t, errUnknown, codes.NotFound)
			if errOther.Error() != errUnknown.Error() {
				t.Errorf("other user's job: %v; unknown job: %v; want the same error", errOther, errUnknown)
			}

			wantCode(t, rpc.call(t, alice, ""), codes.InvalidArgument)
		})
	}

	// Nothing bob did affected alice's job
	if st := getStatus(t, alice, aliceJob); st.GetState() != pb.JobState_JOB_STATE_RUNNING {
		t.Fatalf("alice's job state = %v, want RUNNING", st.GetState())
	}
}

func TestStreamBinaryOutput(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	id := startJob(t, alice, "printf", `\000\377hello\n\001`)

	if got, want := readOutput(t, alice, id), []byte("\x00\xffhello\n\x01"); !bytes.Equal(got, want) {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestStreamLargeOutput(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	const lines = 50000
	var want bytes.Buffer
	for i := 1; i <= lines; i++ {
		fmt.Fprintf(&want, "%d\n", i)
	}

	id := startJob(t, alice, "seq", "1", fmt.Sprint(lines))
	stream, err := alice.StreamJobOutput(t.Context(), pb.StreamJobOutputRequest_builder{JobId: new(id)}.Build())
	if err != nil {
		t.Fatal(err)
	}

	var got bytes.Buffer
	var messages int
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if n := len(msg.GetData()); n == 0 || n > streamChunkSize {
			t.Fatalf("message %d has %d bytes, want 1 to %d", messages, n, streamChunkSize)
		}
		got.Write(msg.GetData())
		messages++
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatalf("got %d bytes of output, want %d bytes of seq output", got.Len(), want.Len())
	}
	if minMessages := want.Len() / streamChunkSize; messages < minMessages {
		t.Fatalf("got %d messages, want at least %d", messages, minMessages)
	}
}

func TestStreamFollowsLiveOutput(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	// The job prints a line, then blocks reading the FIFO until the test writes to it
	gate := newFIFO(t)
	id := startJob(t, alice, "sh", "-c", `echo first; read line < "$1"; echo second`, "sh", gate)

	r := bufio.NewReader(openStream(t, t.Context(), alice, id))
	if line, err := r.ReadString('\n'); err != nil || line != "first\n" {
		t.Fatalf("ReadString = %q, %v; want %q, nil", line, err, "first\n")
	}

	// Opening for writing blocks until the job opens the FIFO for reading
	if err := os.WriteFile(gate, []byte("go\n"), 0); err != nil {
		t.Fatal(err)
	}
	if rest, err := io.ReadAll(r); err != nil || string(rest) != "second\n" {
		t.Fatalf("rest of output = %q, %v; want %q, nil", rest, err, "second\n")
	}
}

func TestStreamConcurrentReaders(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	gate := newFIFO(t)
	id := startJob(t, alice, "sh", "-c", `seq 1 1000; read line < "$1"; seq 1001 2000`, "sh", gate)

	const readers = 5
	outputs := make([][]byte, readers+1)
	errs := make([]error, readers+1)
	var wg sync.WaitGroup
	for i := range readers {
		// Open every stream before the job ends, so they follow it live
		r := openStream(t, t.Context(), alice, id)
		wg.Go(func() { outputs[i], errs[i] = io.ReadAll(r) })
	}

	if err := os.WriteFile(gate, []byte("go\n"), 0); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// A stream opened after the job ended still gets the output from the first byte
	outputs[readers], errs[readers] = io.ReadAll(openStream(t, t.Context(), alice, id))

	var want strings.Builder
	for i := 1; i <= 2000; i++ {
		fmt.Fprintf(&want, "%d\n", i)
	}
	for i := range outputs {
		if errs[i] != nil || string(outputs[i]) != want.String() {
			t.Errorf("reader %d: got %d bytes, %v; want %d bytes, nil", i, len(outputs[i]), errs[i], want.Len())
		}
	}
}

func TestStreamClientCancel(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	id := startJob(t, alice, "sh", "-c", `echo ready; cat "$1"`, "sh", newFIFO(t))

	ctx, cancel := context.WithCancel(t.Context())
	r := bufio.NewReader(openStream(t, ctx, alice, id))
	// Reading the first line means the server is streaming
	if line, err := r.ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("ReadString = %q, %v; want %q, nil", line, err, "ready\n")
	}

	cancel()
	_, err := io.ReadAll(r)
	wantCode(t, err, codes.Canceled)

	// Cancelling a stream doesn't affect the job
	if st := getStatus(t, alice, id); st.GetState() != pb.JobState_JOB_STATE_RUNNING {
		t.Fatalf("state = %v, want RUNNING", st.GetState())
	}
}

func TestShutdownEndsStreams(t *testing.T) {
	ts := startTestServer(t)
	alice := ts.client(t, ts.pki.alice)

	id := startJob(t, alice, "sh", "-c", `echo ready; cat "$1"`, "sh", newFIFO(t))

	r := bufio.NewReader(openStream(t, t.Context(), alice, id))
	if line, err := r.ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("ReadString = %q, %v; want %q, nil", line, err, "ready\n")
	}

	// Shutdown kills the job, which ends its output, so the open stream finishes with OK rather than being cut off
	if err := ts.stop(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if rest, err := io.ReadAll(r); err != nil || len(rest) != 0 {
		t.Fatalf("rest of output = %q, %v; want empty, nil", rest, err)
	}
}
