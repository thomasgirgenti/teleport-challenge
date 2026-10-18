//go:build linux

package cli

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thomasgirgenti/teleport-challenge/internal/pki"
	"github.com/thomasgirgenti/teleport-challenge/server"
)

// testServer is a worker server on a random local port, with every cert written to dir as <name>.crt and <name>.key
type testServer struct {
	addr string
	dir  string
}

// startServer runs server.Serve with a fresh PKI: alice and bob are trusted clients, eve's cert is signed by an untrusted CA.
// The test fails if Serve doesn't return cleanly once cleanup cancels it.
func startServer(t *testing.T) *testServer {
	t.Helper()
	ts := &testServer{dir: t.TempDir()}
	must := func(c *pki.Cert, err error) *pki.Cert {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	serverCA := must(pki.NewCA("test-server-ca"))
	clientCA := must(pki.NewCA("test-client-ca"))
	untrustedCA := must(pki.NewCA("untrusted-ca"))
	ts.write(t, "server-ca", serverCA, false)
	ts.write(t, "client-ca", clientCA, false)
	ts.write(t, "server", must(pki.NewServerCert(serverCA)), true)
	ts.write(t, "alice", must(pki.NewClientCert(clientCA, "alice")), true)
	ts.write(t, "bob", must(pki.NewClientCert(clientCA, "bob")), true)
	ts.write(t, "eve", must(pki.NewClientCert(untrustedCA, "eve")), true)

	cfg, err := server.TLSConfig(ts.path("server.crt"), ts.path("server.key"), ts.path("client-ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ts.addr = lis.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, lis, cfg) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return ts
}

func (ts *testServer) path(name string) string { return filepath.Join(ts.dir, name) }

func (ts *testServer) write(t *testing.T, name string, c *pki.Cert, withKey bool) {
	t.Helper()
	if err := os.WriteFile(ts.path(name+".crt"), c.CertPEM(), 0o600); err != nil {
		t.Fatal(err)
	}
	if !withKey {
		return
	}
	keyPEM, err := c.KeyPEM()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ts.path(name+".key"), keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
}

// globalFlags returns the flags that connect to ts as user
func (ts *testServer) globalFlags(user string) []string {
	return []string{
		"--addr", ts.addr,
		"--cert", ts.path(user + ".crt"),
		"--key", ts.path(user + ".key"),
		"--ca", ts.path("server-ca.crt"),
	}
}

// run runs jobctl as user and returns its exit code, stdout, and stderr
func (ts *testServer) run(t *testing.T, user string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(t.Context(), append(ts.globalFlags(user), args...), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// mustRun runs jobctl as user, fails the test unless it succeeds, and returns its stdout
func (ts *testServer) mustRun(t *testing.T, user string, args ...string) string {
	t.Helper()
	code, stdout, stderr := ts.run(t, user, args...)
	if code != exitOK {
		t.Fatalf("jobctl %q as %s: exit code %d, stderr: %s", args, user, code, stderr)
	}
	return stdout
}

// startJob starts a job as alice and returns its ID
func (ts *testServer) startJob(t *testing.T, args ...string) string {
	t.Helper()
	return strings.TrimSpace(ts.mustRun(t, "alice", append([]string{"start", "--"}, args...)...))
}

func TestStartLogsStatus(t *testing.T) {
	ts := startServer(t)
	id := ts.startJob(t, "echo", "hello")

	// logs returns once the job has ended, so status after it shows the final state
	if got := ts.mustRun(t, "alice", "logs", id); got != "hello\n" {
		t.Errorf("logs = %q, want %q", got, "hello\n")
	}
	st := ts.mustRun(t, "alice", "status", id)
	for _, want := range []string{"ID:       " + id, "Owner:    alice", "Command:  echo hello", "State:    EXITED", "Exit:     0", "Ended:"} {
		if !strings.Contains(st, want) {
			t.Errorf("status missing %q:\n%s", want, st)
		}
	}
}

func TestStartWithoutDashDash(t *testing.T) {
	ts := startServer(t)
	id := strings.TrimSpace(ts.mustRun(t, "alice", "start", "echo", "-n", "x"))
	if got := ts.mustRun(t, "alice", "logs", id); got != "x" {
		t.Errorf("logs = %q, want %q", got, "x")
	}
}

func TestLogsBinaryOutput(t *testing.T) {
	ts := startServer(t)
	id := ts.startJob(t, "printf", `\000\377\n\001`)
	want := "\x00\xff\n\x01"
	if got := ts.mustRun(t, "alice", "logs", id); got != want {
		t.Errorf("logs = %q, want %q", got, want)
	}
}

func TestStop(t *testing.T) {
	ts := startServer(t)
	id := ts.startJob(t, "sleep", "60")

	if got := ts.mustRun(t, "alice", "stop", id); got != "" {
		t.Errorf("stop stdout = %q, want empty", got)
	}
	st := ts.mustRun(t, "alice", "status", id)
	for _, want := range []string{"State:    STOPPED", "Signal:   SIGKILL"} {
		if !strings.Contains(st, want) {
			t.Errorf("status missing %q:\n%s", want, st)
		}
	}

	code, _, stderr := ts.run(t, "alice", "stop", id)
	if code != exitError || stderr != "error: job is not running\n" {
		t.Errorf("second stop = %d, %q; want %d, job is not running", code, stderr, exitError)
	}
}

func TestStartBadCommand(t *testing.T) {
	ts := startServer(t)
	code, stdout, stderr := ts.run(t, "alice", "start", "no-such-command-jobctl-test")
	if code != exitError || stdout != "" || !strings.Contains(stderr, "executable file not found") {
		t.Errorf("start = %d, stdout %q, stderr %q; want %d and executable not found", code, stdout, stderr, exitError)
	}
}

// TestOtherUsersJob checks that bob can't see or act on alice's job, and gets the same error as for a job that doesn't exist
func TestOtherUsersJob(t *testing.T) {
	ts := startServer(t)
	id := ts.startJob(t, "sleep", "60")

	for _, args := range [][]string{{"status", id}, {"stop", id}, {"logs", id}, {"status", "NO-SUCH-JOB"}} {
		code, stdout, stderr := ts.run(t, "bob", args...)
		if code != exitError || stdout != "" || stderr != "error: job not found\n" {
			t.Errorf("bob %q = %d, stdout %q, stderr %q; want %d, job not found", args, code, stdout, stderr, exitError)
		}
	}

	// bob's stop must not have touched alice's job
	if st := ts.mustRun(t, "alice", "status", id); !strings.Contains(st, "State:    RUNNING") {
		t.Errorf("alice's job after bob's stop:\n%s", st)
	}
}

// TestUntrustedClientCert checks that cert signed by an untreated CA is refused
func TestUntrustedClientCert(t *testing.T) {
	ts := startServer(t)
	for _, args := range [][]string{{"start", "echo", "hi"}, {"status", "ID"}} {
		code, stdout, stderr := ts.run(t, "eve", args...)
		if code != exitError || stdout != "" || !strings.HasPrefix(stderr, "error: ") {
			t.Errorf("eve %q = %d, stdout %q, stderr %q; want %d and an error", args, code, stdout, stderr, exitError)
		}
	}
}

func TestUntrustedServer(t *testing.T) {
	ts := startServer(t)
	// alice trusts only the client CA, which didn't sign the server's cert
	args := append(ts.globalFlags("alice"), "--ca", ts.path("client-ca.crt"), "status", "ID")
	var stderr bytes.Buffer
	if code := Run(t.Context(), args, &bytes.Buffer{}, &stderr); code != exitError || !strings.Contains(stderr.String(), "certificate signed by unknown authority") {
		t.Errorf("status = %d, stderr %q; want %d and unknown authority", code, stderr.String(), exitError)
	}
}

// TestLogsInterrupted checks that cancelling ctx (as SIGINT does) stops a logs that is following a running job
func TestLogsInterrupted(t *testing.T) {
	ts := startServer(t)
	id := ts.startJob(t, "sh", "-c", "echo ready; sleep 60")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stdout := &firstWriteNotifier{written: make(chan struct{})}
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- Run(ctx, append(ts.globalFlags("alice"), "logs", id), stdout, &stderr) }()

	select {
	case <-stdout.written:
	case <-time.After(10 * time.Second):
		t.Fatal("logs wrote no output")
	}
	cancel()
	select {
	case code := <-done:
		if code != exitInterrupted || stderr.Len() != 0 {
			t.Errorf("logs = %d, stderr %q; want %d and no error", code, stderr.String(), exitInterrupted)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("logs didn't return after ctx was cancelled")
	}
	if got := stdout.buf.String(); got != "ready\n" {
		t.Errorf("logs = %q, want %q", got, "ready\n")
	}
}

// firstWriteNotifier is a buffer that closes written on its first Write.
// buf may only be read after the writer is done.
type firstWriteNotifier struct {
	buf     bytes.Buffer
	once    sync.Once
	written chan struct{}
}

func (w *firstWriteNotifier) Write(p []byte) (int, error) {
	n, err := w.buf.Write(p)
	w.once.Do(func() { close(w.written) })
	return n, err
}
