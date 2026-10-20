//go:build linux

// Command workerd runs the job worker gRPC server.
//
// It listens on 127.0.0.1:50051 and loads its certs from the certs directory, which `make certs` creates.
// On SIGINT or SIGTERM it kills every job and stops.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/thomasgirgenti/teleport-challenge/server"
)

// TODO: Add configuration system.
// Consider a CLI library that supports both flags and environment variables with reasonable defaults.
const (
	// addr is an explicit IP because "localhost" also resolves to ::1, and net.Listen binds only one of its addresses.
	// A client trying ::1 first would see "connection refused" instead of the real error, e.g. a rejected cert, when its fallback to 127.0.0.1 fails.
	addr         = "127.0.0.1:50051"
	certFile     = "certs/server.crt"
	keyFile      = "certs/server.key"
	clientCAFile = "certs/client-ca.crt"
)

func main() {
	if err := run(); err != nil {
		slog.Error("workerd failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tlsConfig, err := server.TLSConfig(certFile, keyFile, clientCAFile)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	slog.Info("serving", "addr", lis.Addr().String())
	if err := server.Serve(ctx, lis, tlsConfig); err != nil {
		return err
	}
	slog.Info("stopped")
	return nil
}
