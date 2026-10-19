//go:build linux

package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/thomasgirgenti/teleport-challenge/core"
	pb "github.com/thomasgirgenti/teleport-challenge/gen/worker/v1"
)

const (
	// stopTimeout bounds how long StopJob waits for a killed job to end. Matches CLI timeout for unary calls.
	stopTimeout = 10 * time.Second

	// streamChunkSize is the most output sent in one StreamJobOutput message.
	// It bounds per-stream memory and stays well under gRPC's default 4 MiB max message size.
	streamChunkSize = 32*1024 - 4 // tag + 3-byte length keeps the encoded message at 32 KiB
)

// service implements WorkerService on top of a core.Manager.
// Every per-job RPC goes through authorizeJob, so a caller can only see and act on jobs it started.
type service struct {
	pb.UnimplementedWorkerServiceServer
	jobs *core.Manager
}

func (s *service) StartJob(ctx context.Context, req *pb.StartJobRequest) (*pb.StartJobResponse, error) {
	user, err := userFrom(ctx)
	if err != nil {
		return nil, err
	}

	job, err := s.jobs.Start(user, req.GetCommand(), req.GetArgs())
	if err != nil {
		return nil, toStatusError(err)
	}
	return pb.StartJobResponse_builder{JobId: new(job.ID())}.Build(), nil
}

func (s *service) StopJob(ctx context.Context, req *pb.StopJobRequest) (*pb.StopJobResponse, error) {
	job, err := s.authorizeJob(ctx, req.GetJobId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, stopTimeout)
	defer cancel()
	if err := job.Stop(ctx); err != nil {
		return nil, toStatusError(err)
	}
	return &pb.StopJobResponse{}, nil
}

func (s *service) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (*pb.GetJobStatusResponse, error) {
	job, err := s.authorizeJob(ctx, req.GetJobId())
	if err != nil {
		return nil, err
	}
	return pb.GetJobStatusResponse_builder{Status: toProto(job.Status())}.Build(), nil
}

// StreamJobOutput sends the job's output from the first byte and follows it until the job ends.
// Each call reads with its own reader, so a slow client only falls behind itself; the job and other streams are unaffected.
func (s *service) StreamJobOutput(req *pb.StreamJobOutputRequest, stream grpc.ServerStreamingServer[pb.StreamJobOutputResponse]) error {
	job, err := s.authorizeJob(stream.Context(), req.GetJobId())
	if err != nil {
		return err
	}

	r := job.Output(stream.Context())
	for {
		buf := make([]byte, streamChunkSize)
		n, err := r.Read(buf)
		if n > 0 {
			if err := stream.Send(pb.StreamJobOutputResponse_builder{Data: buf[:n]}.Build()); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			// The job ended and all of its output was sent
			return nil
		}
		if err != nil {
			// The client went away or the server is stopping
			return toStatusError(err)
		}
	}
}

// authorizeJob returns the job with the given ID if the caller owns it.
// A job owned by someone else is reported exactly like a job that doesn't exist, so callers can't learn which IDs exist.
func (s *service) authorizeJob(ctx context.Context, id string) (*core.Job, error) {
	user, err := userFrom(ctx)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "job ID must not be empty")
	}

	job, err := s.jobs.Get(id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if job.Owner() != user {
		return nil, toStatusError(core.ErrNotFound)
	}
	return job, nil
}

// toStatusError maps an error from the core library to a gRPC status error
func toStatusError(err error) error {
	switch {
	case errors.Is(err, core.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, core.ErrEmptyCommand), isBadCommand(err):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, core.ErrNotRunning):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, core.ErrManagerClosed):
		return status.Error(codes.Unavailable, "server is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		// Details may describe the server's environment, so they are logged rather than returned
		slog.Error("internal error", "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}

// isBadCommand reports whether err means the requested command can't be run, rather than that the server failed
func isBadCommand(err error) bool {
	var execErr *exec.Error
	return errors.As(err, &execErr) || // not found, or not executable
		errors.Is(err, syscall.ENOEXEC) || // executable bit set, but not a binary or script the kernel can run
		errors.Is(err, syscall.EACCES) || // e.g. on a noexec mount
		errors.Is(err, syscall.ENOENT) // e.g. a script's interpreter doesn't exist
}

// signalName returns a signal's name, like "SIGKILL", or "signal N" for signals without one, such as real-time signals
func signalName(sig syscall.Signal) string {
	if name := unix.SignalName(sig); name != "" {
		return name
	}
	return sig.String()
}

// toProto converts a job status to its API form.
// exit_code is only set for jobs that exited on their own and signal only for jobs killed by a signal.
func toProto(st core.Status) *pb.JobStatus {
	b := pb.JobStatus_builder{
		JobId:     new(st.ID),
		Owner:     new(st.Owner),
		Command:   new(st.Command),
		Args:      st.Args,
		StartedAt: timestamppb.New(st.StartedAt),
	}
	switch st.State {
	case core.StateRunning:
		b.State = pb.JobState_JOB_STATE_RUNNING.Enum()
	case core.StateExited:
		b.State = pb.JobState_JOB_STATE_EXITED.Enum()
		b.ExitCode = new(int32(st.ExitCode))
	case core.StateSignaled:
		b.State = pb.JobState_JOB_STATE_SIGNALED.Enum()
		b.Signal = new(signalName(st.Signal))
	case core.StateStopped:
		b.State = pb.JobState_JOB_STATE_STOPPED.Enum()
		b.Signal = new(signalName(st.Signal))
	}
	if !st.EndedAt.IsZero() {
		b.EndedAt = timestamppb.New(st.EndedAt)
	}
	return b.Build()
}
