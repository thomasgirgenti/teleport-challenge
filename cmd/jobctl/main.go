// Command jobctl is the command line client for the job worker server.
//
// By default it connects to 127.0.0.1:50051 as alice, with the certs that `make certs` writes to the certs directory.
// Run `jobctl -h` for usage.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/thomasgirgenti/teleport-challenge/cli"
)

func main() {
	// SIGINT and SIGTERM cancel in-flight RPCs, so `jobctl logs` stops following and exits cleanly
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
