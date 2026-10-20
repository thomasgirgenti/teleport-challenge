//go:build linux

package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/thomasgirgenti/teleport-challenge/core"
	pb "github.com/thomasgirgenti/teleport-challenge/gen/worker/v1"
)

const (
	// The server pings an idle connection after keepaliveTime and closes it if no ack arrives within keepaliveTimeout
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second

	// handshakeTimeout bounds the TLS handshake and HTTP/2 preface of a new connection.
	// Stop and GracefulStop wait for in-progress handshakes, so this also bounds shutdown when a peer connects and sends nothing.
	// grpc's default is 120s.
	handshakeTimeout = 5 * time.Second

	// jobShutdownTimeout bounds how long shutdown waits for killed jobs to end
	jobShutdownTimeout = 10 * time.Second

	// grpcShutdownTimeout bounds how long shutdown waits for in-flight RPCs before closing every connection
	grpcShutdownTimeout = 5 * time.Second
)

// Serve serves the WorkerService on lst with mTLS until ctx is done or lst fails.
// On return, every job has been killed and the gRPC server has stopped.
//
// TODO: Limit concurrent streams per connection and the number of running jobs per user.
// TODO: Audit log every RPC with its user, method, job ID, and resulting status code.
func Serve(ctx context.Context, lst net.Listener, tlsConfig *tls.Config) error {
	jobs := core.NewManager()
	gs := grpc.NewServer(
		grpc.Creds(newIdentityCreds(tlsConfig)),
		grpc.ConnectionTimeout(handshakeTimeout),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
	)
	pb.RegisterWorkerServiceServer(gs, &service{jobs: jobs})

	// Buffered so the goroutine can always send and exit, even if nobody receives
	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lst) }()

	select {
	case <-ctx.Done():
		err := shutdown(gs, jobs)
		// Serve returns once the server is stopped
		<-serveErr
		return err
	case err := <-serveErr:
		// The listener failed. The jobs' processes would outlive the server, so kill them.
		return errors.Join(fmt.Errorf("serve: %w", err), shutdown(gs, jobs))
	}
}

// shutdown kills every job, then stops the gRPC server.
// Killing jobs first closes their output, so every open stream ends with OK and GracefulStop doesn't wait on them.
func shutdown(gs *grpc.Server, jobs *core.Manager) error {
	ctx, cancel := context.WithTimeout(context.Background(), jobShutdownTimeout)
	defer cancel()
	var err error
	if jobsErr := jobs.Shutdown(ctx); jobsErr != nil {
		err = fmt.Errorf("stop jobs: %w", jobsErr)
	}

	timer := time.AfterFunc(grpcShutdownTimeout, gs.Stop)
	defer timer.Stop()
	gs.GracefulStop()
	return err
}
