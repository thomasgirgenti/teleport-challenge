package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	pb "github.com/thomasgirgenti/teleport-challenge/gen/worker/v1"
)

// rpcTimeout bounds every unary RPC.
// Longer than server's 10s StopJob timeout so the server error reaches the CLI
// The output stream has no timeout because it lasts as long as the job.
const rpcTimeout = 15 * time.Second

// client runs jobctl subcommands against the worker server
type client struct {
	c pb.WorkerServiceClient
}

// start starts args[0] with the remaining args and prints the new job's ID
func (cl *client) start(ctx context.Context, args []string, stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := cl.c.StartJob(ctx, pb.StartJobRequest_builder{
		Command: new(args[0]),
		Args:    args[1:],
	}.Build())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, resp.GetJobId())
	return err
}

// stop kills a running job and returns once it has ended
func (cl *client) stop(ctx context.Context, args []string, _ io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	_, err := cl.c.StopJob(ctx, pb.StopJobRequest_builder{JobId: new(args[0])}.Build())
	return err
}

// jobStatus prints a job's status
func (cl *client) jobStatus(ctx context.Context, args []string, stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := cl.c.GetJobStatus(ctx, pb.GetJobStatusRequest_builder{JobId: new(args[0])}.Build())
	if err != nil {
		return err
	}
	return formatStatus(stdout, resp.GetStatus())
}

// logs writes a job's output to stdout from the first byte, following it until the job ends.
// Output is written as raw bytes in the order received, so binary output passes through unchanged.
func (cl *client) logs(ctx context.Context, args []string, stdout io.Writer) error {
	// Cancelling ends the stream if we return before the server does, e.g. when stdout fails
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := cl.c.StreamJobOutput(ctx, pb.StreamJobOutputRequest_builder{JobId: new(args[0])}.Build())
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// The job ended and all of its output was received
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := stdout.Write(msg.GetData()); err != nil {
			return err
		}
	}
}
