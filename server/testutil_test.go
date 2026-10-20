//go:build linux

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	pb "github.com/thomasgirgenti/teleport-challenge/gen/worker/v1"
	"github.com/thomasgirgenti/teleport-challenge/internal/pki"
)

// testPKI holds the certs the tests use, generated in memory
type testPKI struct {
	serverCA, clientCA *pki.Cert
	server             *pki.Cert
	alice, bob         *pki.Cert
	eve                *pki.Cert // signed by a CA the server doesn't trust
	emptyCN            *pki.Cert // signed by the client CA, but names no user
	serverEKU          *pki.Cert // signed by the client CA, but only valid for server auth
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	must := func(c *pki.Cert, err error) *pki.Cert {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	p := &testPKI{
		serverCA: must(pki.NewCA("test-server-ca")),
		clientCA: must(pki.NewCA("test-client-ca")),
	}
	untrustedCA := must(pki.NewCA("untrusted-ca"))

	p.server = must(pki.NewServerCert(p.serverCA))
	p.alice = must(pki.NewClientCert(p.clientCA, "alice"))
	p.bob = must(pki.NewClientCert(p.clientCA, "bob"))
	p.eve = must(pki.NewClientCert(untrustedCA, "eve"))
	p.emptyCN = must(pki.NewClientCert(p.clientCA, ""))
	p.serverEKU = must(pki.NewLeaf(p.clientCA, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "mallory"},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}))
	return p
}

type testServer struct {
	addr string
	pki  *testPKI

	// stop cancels Serve and returns its error once it has returned. Safe to call more than once.
	stop func() error
}

// startTestServer runs Serve on a random local port.
// The test fails if Serve doesn't return after cleanup cancels it, which catches RPCs or goroutines that never finish.
func startTestServer(t *testing.T) *testServer {
	t.Helper()
	p := newTestPKI(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, lis, newTLSConfig(p.server.TLSCertificate(), pki.Pool(p.clientCA)))
	}()

	ts := &testServer{
		addr: lis.Addr().String(),
		pki:  p,
		stop: sync.OnceValue(func() error {
			cancel()
			return <-done
		}),
	}
	t.Cleanup(func() {
		if err := ts.stop(); err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return ts
}

// clientTLSConfig is the config a client presenting cert uses; a nil cert presents none
func (ts *testServer) clientTLSConfig(cert *pki.Cert) *tls.Config {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pki.Pool(ts.pki.serverCA),
		ServerName: "localhost",
	}
	if cert != nil {
		cfg.Certificates = []tls.Certificate{cert.TLSCertificate()}
	}
	return cfg
}

// client returns a client that authenticates with cert
func (ts *testServer) client(t *testing.T, cert *pki.Cert) pb.WorkerServiceClient {
	t.Helper()
	return ts.clientWithConfig(t, ts.clientTLSConfig(cert))
}

func (ts *testServer) clientWithConfig(t *testing.T, cfg *tls.Config) pb.WorkerServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(ts.addr, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewWorkerServiceClient(conn)
}

func startJob(t *testing.T, c pb.WorkerServiceClient, command string, args ...string) string {
	t.Helper()
	req := pb.StartJobRequest_builder{Command: new(command), Args: args}.Build()
	resp, err := c.StartJob(t.Context(), req)
	if err != nil {
		t.Fatalf("StartJob(%q, %q): %v", command, args, err)
	}
	return resp.GetJobId()
}

func getStatus(t *testing.T, c pb.WorkerServiceClient, id string) *pb.JobStatus {
	t.Helper()
	resp, err := c.GetJobStatus(t.Context(), pb.GetJobStatusRequest_builder{JobId: new(id)}.Build())
	if err != nil {
		t.Fatalf("GetJobStatus(%s): %v", id, err)
	}
	return resp.GetStatus()
}

func stopJob(t *testing.T, c pb.WorkerServiceClient, id string) error {
	_, err := c.StopJob(t.Context(), pb.StopJobRequest_builder{JobId: new(id)}.Build())
	return err
}

// openStream opens an output stream and returns it as a reader of the job's output.
// Read returns io.EOF once the stream ends with OK, and the stream's status error otherwise.
func openStream(t *testing.T, ctx context.Context, c pb.WorkerServiceClient, id string) *streamReader {
	t.Helper()
	stream, err := c.StreamJobOutput(ctx, pb.StreamJobOutputRequest_builder{JobId: new(id)}.Build())
	if err != nil {
		t.Fatalf("StreamJobOutput(%s): %v", id, err)
	}
	return &streamReader{stream: stream}
}

// readOutput returns a job's entire output. When it returns, the job has ended and its final status is recorded.
func readOutput(t *testing.T, c pb.WorkerServiceClient, id string) []byte {
	t.Helper()
	out, err := io.ReadAll(openStream(t, t.Context(), c, id))
	if err != nil {
		t.Fatalf("reading output of job %s: %v", id, err)
	}
	return out
}

type streamReader struct {
	stream grpc.ServerStreamingClient[pb.StreamJobOutputResponse]
	buf    []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		msg, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buf = msg.GetData()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// wantCode fails the test unless err is a status error with the given code
func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("error = %v, want code %v", err, want)
	}
}

// newFIFO returns the path of a new FIFO.
// `cat` on a FIFO that is never written blocks until it is killed, which makes a job that runs until stopped.
func newFIFO(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fifo")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
