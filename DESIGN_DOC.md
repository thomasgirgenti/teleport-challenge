# Design Doc

## 1. Overview

This document describes a job worker service that runs arbitrary Linux processes on behalf of authenticated clients. It has three components:

| Component | Path     | Responsibility                                                                                                              |
|-----------|----------|-----------------------------------------------------------------------------------------------------------------------------|
| Library   | `core`   | Start, stop, and query jobs. Captures output in memory and streams it to any number of readers. Owns all job logic.         |
| Server    | `server` | gRPC API that implements the core library to start/stop/get status/stream output of jobs. Handles authentication with mTLS. |
| CLI       | `cli`    | Command line client to interact with a worker instance to manage jobs and follow job logs.                                  |

### Scope

In scope:

- Start, stop, and get status of jobs, plus streaming of job output from the beginning of execution for any number of concurrent clients.
- Efficient discovery of new job output, with no polling or busy-waiting.
- mTLS with TLS 1.3 only, strong certificate crypto.

Out of scope:

- Persistence - jobs and output live in memory only and are lost when the server restarts.
- Configuration system - addresses, paths, timeouts, etc. are hardcoded constants.
- Certificate lifecycle management (PKI).
- RBAC - users will be able to interact w/ only the jobs they've started/own.

### Assumptions

- The worker machine has unlimited memory, so all output is kept for the lifetime of the server.
- Jobs are killed w/ `SIGKILL`- both on user-issued stop command and on server shutdown.
- Dependencies are kept to the Go standard library plus google.golang.org/grpc and google.golang.org/protobuf

---

## 2. Library Design

### Manager & Job

```go
// Manager owns the jobs, with concurrency safety
type Manager struct {
    mu     sync.Mutex // guards the `jobs` map and `closed`
    jobs   map[string]*Job // key is job id generated w/ crypto/rand.Text()
    closed bool
}

func NewManager() *Manager

// Start launches a cmd with args
func (m *Manager) Start(cmd string, args []string) (*Job, error) {
    // TODO: enforce limit for max # running jobs. runtime has 10k thread limit
	...
}

// Get returns a job by id
func (m *Manager) Get(id string) (*Job, error) { }

// Shutdown rejects new jobs, stops running jobs, and stops the worker
func (m *Manager) Shutdown(ctx context.Context) error { }

type Job struct { /* defined under Process Lifecycle */ }

func (j *Job) ID() string { }
func (j *Job) Owner() string { }
func (j *Job) Status() Status { }

// Stop SIGKILLs the job's process group and waits until the job reaches a terminal state or ctx is done (timeout)
func (j *Job) Stop(ctx context.Context) error { }

// Output returns a reader of job's entire output from the first byte
// Each call returns an independent reader

// Output returns a reader of the job's combined stdout and stderr from the first byte
// Read blocks until new data is available, the job ends (io.EOF), or ctx is done (ctx.Err()). 
// Each call returns an independent reader.
func (j *Job) Output(ctx context.Context) io.Reader { }

// Done is closed when the job has reached a terminal state
func (j *Job) Done() <-chan struct{} { }

type Status struct { /* Defined under Process Lifecycle */}
```

### Output Storage and Streaming

Append-only byte buffer with a broadcast notification channel:

```go
type outputBuffer struct {
    mu     sync.Mutex    // guards `data`, `closed`, and `notify`
    data   []byte        // append-only array of all job output
    closed bool          // true when the job's pipe hits EOF
    notify chan struct{} // closed and replaced on every Write and Close
}

// Write appends new bytes to `data`, notifies waiting readers w/ `close(b.notify)`, and replaces `notify` with a new channel
func (b *outputBuffer) Write(p []byte) (n int, err error) { }

// Close is called when a job's pipe hits EOF and sets `b.closed` to true
// Prevents new logs from being appended and signals to readers that there will be no new output
func (b *outputBuffer) Close() { }

// readAt copies data at offset into p, returns # bytes copied
// If there is no data at offset, returns `b.notify` so readers are notified when new output is available
func (b *outputBuffer) readAt(p []byte, offset int) (int, <-chan struct{}, error) {
    b.mu.Lock()
    defer b.mu.Unlock()
    if off < len(b.data) {
        return copy(p, b.data[off:]), nil, nil
    }
    if b.closed {
        return 0, nil, io.EOF
    }
    return 0, b.notify, nil
}
```

Each reader tracks its own offset:

```go
type OutputReader struct {
    ctx context.Context
    buf *outputBuffer
    offset int
}

// Read copies output from offset into p and increases r.offset
// Blocks on r.buf.notify if there is nothing to read
func (r *OutputReader) Read(p []byte) (int, error) {
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
        case <-wait:          // new data or EOF; loop and re-check
        case <-r.ctx.Done():
            return 0, r.ctx.Err()
		}
    }
}
```

**Features of this output structure:**

- Output is stored from start of job execution and never truncated
- Readers do not sleep and re-check; a reader with nothing to read waits until new output becomes available (via notify channel). `Write` wakes every waiting reader exactly once by closing the channel.
- Multiple concurrent clients at little cost: Readers share the buffer read-only and keep their own offsets. Adding a reader costs O(1) and needs no manual registration and cleanup.
- Data is handled as `[]byte`, so it is binary safe w/ no encoding assumptions
- `Write` doesn't wait for readers. A slow gRPC client only falls behind on its own offset. A job is never blocked by a consumer.
- Readers start no goroutines, so there is nothing to close. Cancelling the context unblocks a waiting `Read` right away. Minimizes risk of goroutine leaks.

**Alternatives considered:**

- `sync.Cond`: `Wait` ignores contexts, so cancellation needs a `context.AfterFunc` that locks before `Broadcast` and wakes every waiter. A closed channel in a `select` with `ctx.Done()` does the same thing more simply.
- Per-subscriber channels (fan-out)
    - Fanning out data forces the writer to block on a slow subscriber or drop data.
    - Fanning out notifications instead (a 1-buffered `chan struct{}` per reader, non-blocking send) avoids that, but needs a subscriber registry with register/unregister on every reader. Closing a shared channel gives the same wakeups with no registry.
- Ring buffer: breaks the "output from start" requirement.

### Process Lifecycle

```go
type Job struct {
    id, owner string
    cmd       *exec.Cmd
    output    *outputBuffer
    done      chan struct{} // closed after final state is recorded
	
    mu            sync.Mutex // guards the state fields below
    state         State
    exitCode      int
    signal        syscall.Signal
    startedAt     time.Time
    endedAt       time.Time
    leaderExited  bool // set before the leader is reaped; guards kill()
    stopRequested bool
}

type State int

const (
    StateRunning  State = iota + 1
    StateExited         // exited on its own
    StateSignaled       // killed by a signal not sent by Stop
    StateStopped        // killed by Stop
)

type Status struct {
    ID        string
    Owner     string
    Command   string
    Args      []string
    State     State
    ExitCode  int            // valid only for StateExited
    Signal    syscall.Signal // valid for StateSignaled / StateStopped
    StartedAt time.Time
    EndedAt   time.Time      // zero while running
}
```

**Starting a job** (`Manager.Start`):

1. Reject the call if `cmd == ""`, the user-provided command is not found (check `exec.LookPath(cmd)`), or the manager is closed (shutting down).
2. Build an [`exec.Cmd`](https://pkg.go.dev/os/exec#Cmd):
```go
output := outputBuffer{...}
c := &exec.Cmd{
    Path: path,
    Args: append([]string{cmd}, args...),

    // Child reads from /dev/null
    Stdin: nil,

    // Same writer for both, so os/exec gives the child a single pipe for fd 1 and fd 2
    // stdout/stderr interleaving is preserved as the child wrote it
    Stdout: &output,
    Stderr: &output,

    SysProcAttr: &syscall.SysProcAttr{
        // The child leads its own process group (pgid == pid), so Stop kills its whole tree
        Setpgid: true,
    },

    // Backstop so Wait returns even if a descendant that escaped the process group keeps the pipe open.
    // Real containment of escaped descendants needs cgroups (Level 5).
    WaitDelay: 2 * time.Second,
}
```
3. Call `c.Start()`. On failure, no job is registered and the error is returned to the caller.
4. Register the job under `manager.jobs`, then start a single waiter goroutine:

**Waiter goroutine.** This is the only goroutine per job:

```go
// go j.wait() is called from Manager.Start after cmd.Start() succeeds.
func (j *Job) wait() {
    defer close(j.done)

    // Since leader started with Setpgid, pid == pgid
    pgid := j.cmd.Process.Pid

    // Block until the leader exits, leave it unreaped (WNOWAIT).
    var info unix.Siginfo
    for {
        err := unix.Waitid(unix.P_PID, pgid, &info, unix.WEXITED|unix.WNOWAIT, nil)
        if err != unix.EINTR {
            break // other errors resurface from cmd.Wait below
        }
    }

    // Kill group members left behind. They were reparented to init, which reaps them.
    // Stop checks leaderExited under the same lock, so it never signals the group after the reap below.
    j.mu.Lock()
    j.leaderExited = true
    _ = unix.Kill(-pgid, unix.SIGKILL)
    j.mu.Unlock()

    // Reap the leader and wait for the output copy to reach EOF.
    // If a process that left the group still holds the pipe, WaitDelay closes it so this won't hang.
    // ProcessState is set once the leader is reaped, even if Wait also returns ErrWaitDelay.
    _ = j.cmd.Wait()

    // Readers drain the remaining output, then get io.EOF.
    j.output.Close()


    // Update job endedAt, state, signal, exitStatus under j.mu.Lock()
    j.mu.Lock()
    defer j.mu.Unlock()
    j.endedAt = time.Now()
    ws := j.cmd.ProcessState.Sys().(syscall.WaitStatus)
    switch {
    case ws.Signaled() && j.stopRequested:
        j.state, j.signal = StateStopped, ws.Signal()
    case ws.Signaled():
        j.state, j.signal = StateSignaled, ws.Signal()
    default:
        j.state, j.exitCode = StateExited, ws.ExitStatus()
    }
}
```

**Stopping a job** (`Job.Stop`):

```go
func (j *Job) Stop(ctx context.Context) error {
    // Hold j.mu across the kill.
	// wait() goroutine sets leaderExited under the same lock before reaping the leader, so the pgid can't be reused while we signal it.
    j.mu.Lock()
    if j.leaderExited {
        j.mu.Unlock()
        return ErrNotRunning
    }

    // Set before the kill so wait() records StateStopped rather than StateSignaled.
    j.stopRequested = true

    // Since leader started with Setpgid, pid == pgid
    err := unix.Kill(-j.cmd.Process.Pid, unix.SIGKILL)
    j.mu.Unlock()
    if err != nil {
        return err
    }

    // Return once wait() has recorded the final state, or when ctx is done (timeout).
    select {
    case <-j.done:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

### Concurrency Notes

**Lock scopes:**
- `Manager.mu` guards only the jobs map and `closed`
- `Job.mu` guards state fields
- `outputBuffer.mu` guards `data`, `closed`, and `notify`

No lock is held across blocking operations. That includes `cmd.Wait`, channel waits, and gRPC `Send`.

---

## 3. API Design (gRPC)

### Protobuf

```protobuf
syntax = "proto3";

package worker.v1;

import "google/protobuf/timestamp.proto";

service WorkerService {
  rpc StartJob(StartJobRequest) returns (StartJobResponse);
  
  rpc StopJob(StopJobRequest) returns (StopJobResponse);

  rpc GetJobStatus(GetJobStatusRequest) returns (GetJobStatusResponse);

  rpc StreamJobOutput(StreamJobOutputRequest) returns (stream StreamJobOutputResponse);
}

message StartJobRequest {
  string command = 1;
  repeated string args = 2;
}

message StartJobResponse {
  string job_id = 1;
}

message StopJobRequest {
  string job_id = 1;
}

message StopJobResponse {}

message GetJobStatusRequest {
  string job_id = 1;
}

message GetJobStatusResponse {
  JobStatus status = 1;
}

message StreamJobOutputRequest {
  string job_id = 1;
}

message StreamJobOutputResponse {
  bytes data = 1;
}

enum JobState {
  JOB_STATE_UNSPECIFIED = 0;
  JOB_STATE_RUNNING = 1;
  JOB_STATE_EXITED = 2;   // exited on its own; exit_code is set
  JOB_STATE_SIGNALED = 3; // killed by a signal not sent via StopJob; signal is set
  JOB_STATE_STOPPED = 4;  // killed via StopJob
}

message JobStatus {
  string job_id = 1;
  string owner = 2;
  string command = 3;
  repeated string args = 4;
  JobState state = 5;
  int32 exit_code = 6;
  string signal = 7;
  google.protobuf.Timestamp started_at = 8;
  google.protobuf.Timestamp ended_at = 9;
}
```

### Output Streaming

A `StreamJobOutput` handler reads from its own `OutputReader` into a fixed 32 KiB buffer and sends each `Read` result as one message:

```go
func (s *server) StreamJobOutput(req *pb.StreamJobOutputRequest, stream grpc.ServerStreamingServer[pb.StreamJobOutputResponse]) error {
    // NotFound for unknown jobs and jobs the caller doesn't own
    job, err := s.authorizeJob(stream.Context(), req.GetJobId())
    if err != nil {
        return err
    }

    buf := make([]byte, 32*1024)
    r := job.Output(stream.Context())
    for {
        n, err := r.Read(buf)
        if n > 0 {
            if err := stream.Send(&pb.StreamJobOutputResponse{Data: buf[:n]}); err != nil {
                return err
            }
        }
        if err == io.EOF {
            return nil // job ended and all output was sent
        }
        if err != nil {
            return err // ctx cancelled: client went away or server is stopping
        }
    }
}
```

- A chunk contains the data between a reader's offset and the end of the buffer, up to 32 KiB.
    - A reader catching up gets full 32 KiB chunks; a live reader gets one small message per `Write`, with no batching delay.
- Chunk boundaries don't align with lines, UTF-8 characters, or the job's writes. The client appends `data` to stdout in order.
- 32 KiB stays well under gRPC's default 4 MiB max receive message size and bounds per-stream memory.
- `buf` is reused safely because `Send` serializes the message before returning.
- `Send` blocks under HTTP/2 flow control when a client is slow. That only holds back this reader's offset; the job and other readers are unaffected.

### Networking

- Keepalive: Server sets `keepalive.ServerParameters{Time: 30s, Timeout: 10s}` and client sets matching params.
- Timeouts: CLI unary calls use a 10s timeout (matching server req timeout). Output stream has no timeout because it lives as long as the job.
- Server shutdown on `SIGINT`/`SIGTERM`: `manager.Shutdown(ctx)` kills all jobs. Their output buffers close, so every open stream ends with `OK`. Then `grpcServer.GracefulStop()` runs with a fallback to `Stop()`.

---

## 4. Security

### Threat Model

- Clients are authenticated only by X.509 client certificates (mTLS). No passwords, tokens, or other schemes are layered on top.
- Any authenticated user can run arbitrary commands as the server's Unix user.
- All traffic is encrypted and mutually authenticated.

### Certificates

A bootstrap command will use only `crypto/x509` and generate the following:

| Cert                    | Key         | Key usage         | Ext key usage | Subject / SAN                        |
|-------------------------|-------------|-------------------|---------------|--------------------------------------|
| Server CA (self-signed) | ECDSA P-256 | CertSign, CRLSign | —             | `CN=worker-server-ca`                |
| Client CA (self-signed) | ECDSA P-256 | CertSign, CRLSign | —             | `CN=worker-client-ca`                |
| Server leaf             | ECDSA P-256 | DigitalSignature  | ServerAuth    | SAN: `localhost`, `127.0.0.1`, `::1` |
| Client leaf `alice`     | ECDSA P-256 | DigitalSignature  | ClientAuth    | `CN=alice`                           |
| Client leaf `bob`       | ECDSA P-256 | DigitalSignature  | ClientAuth    | `CN=bob`                             |

**Features:**

- Separate server and client CAs: the server trusts only the client CA for client auth, and clients trust only the server CA. A stolen client key can never impersonate the server.
- Signatures are ECDSA with SHA-256.
- ECDSA P-256 for smaller/faster keys (vs RSA) with universal support.

### TLS Config

Server:
```go
&tls.Config{
    MinVersion:       tls.VersionTLS13,
    Certificates:     []tls.Certificate{serverCert},
    ClientAuth:       tls.RequireAndVerifyClientCert,
    ClientCAs:        clientCAPool,
}
```

Client:
```go
&tls.Config{
    MinVersion:       tls.VersionTLS13,
    Certificates:     []tls.Certificate{clientCert},
    RootCAs:          serverCAPool,
    ServerName:       "localhost",
}
```

**Features:**
- TLS 1.3 only: we manage both peers, so legacy TLS support is unnecessary.
- Verification: with `RequireAndVerifyClientCert` the server rejects connections that lack a client cert chaining to the client CA. The client checks the server cert against the server CA and the `localhost` SAN.

### Authentication and Authorization

The client identity is fixed for the lifetime of a connection, so it is extracted once, at the TLS handshake, by wrapping the TLS transport credentials:

```go
// authInfo embeds TLSInfo so AuthType() and grpc's security-level checks keep working
type authInfo struct {
    credentials.TLSInfo
    User string
}

// identityCreds wraps credentials.NewTLS(serverTLSConfig)
type identityCreds struct {
    credentials.TransportCredentials
}

// ServerHandshake is called by grpc once per accepted connection, before any RPC on it
func (c identityCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
    conn, info, err := c.TransportCredentials.ServerHandshake(raw)
    if err != nil {
        return nil, nil, err
    }
    tlsInfo := info.(credentials.TLSInfo)

    user, err := userFromCert(tlsInfo.State.VerifiedChains[0][0])
    if err != nil {
        conn.Close()
        return nil, nil, err // connection is refused; no RPC runs on it
    }
    return conn, authInfo{TLSInfo: tlsInfo, User: user}, nil
}

// Clone must return the wrapper, or the embedded Clone would drop it
func (c identityCreds) Clone() credentials.TransportCredentials {
    return identityCreds{c.TransportCredentials.Clone()}
}

// userFrom is what handlers call: one type assertion, no certificate parsing per RPC
func userFrom(ctx context.Context) (string, bool) {
    p, ok := peer.FromContext(ctx)
    if !ok {
        return "", false
    }
    ai, ok := p.AuthInfo.(authInfo)
    return ai.User, ok
}
```

`userFromCert` rules:
1. Use `State.VerifiedChains[0][0]` to get the leaf after verification, not the unverified `PeerCertificates`. `RequireAndVerifyClientCert` guarantees a verified chain exists once the TLS handshake succeeds.
2. `CN` is the username and must be non-empty. Otherwise the handshake fails.

Notes:
- grpc attaches the connection's `AuthInfo` to every RPC via `peer`, so nothing is added to the context per RPC and no identity interceptor is needed.
- A client with an invalid cert sees a failed connection (`Unavailable`, "authentication handshake failed") rather than a per-RPC error.
- Reconnecting runs a new handshake, so a changed cert is picked up on the next connection.

**Authorization policy:**

- `StartJob` records `User` as the job's owner.
- A non-owner gets `NotFound` when querying/stopping/streaming a job it doesn't own
- Authz check runs in a single helper (i.e. `authorizeJob(ctx, id)`) which every per-job handler calls. It reads the caller with `userFrom(ctx)`.

---

## 5. CLI UX

The CLI binary is `jobctl`.
- uses the standard library `flag` package with one `FlagSet` per subcommand
- global flags come before the subcommand

```
jobctl [--addr localhost:50051] \
       [--cert certs/alice.crt] \
       [--key certs/alice.key] \
       [--ca certs/server-ca.crt] \
       <command> [args]

Commands:
  start [--] <cmd> [args...]   Start a job; prints the job ID
  stop <job-id>                Kill a running job (SIGKILL)
  status <job-id>              Show job status
  logs <job-id>                Stream output from the start, following until the job ends
  list                         List jobs (running and finished)
```

Examples:

```console
$ jobctl start -- bash -c 'for i in 1 2 3; do echo "tick $i"; sleep 1; done'
Q5ZJ3KXM7TAB2C4VNWHRY6DPLE

# returns when the job ends
$ jobctl logs Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
tick 1
tick 2
tick 3
$

$ jobctl status Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
ID:       Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
Owner:    alice
Command:  bash -c 'for i in 1 2 3; do echo "tick $i"; sleep 1; done'
State:    EXITED
Exit:     0
Started:  2026-10-09T18:40:01Z
Ended:    2026-10-09T18:40:04Z

# bob (role=user) cannot see alice's jobs
$ jobctl --cert certs/bob.crt --key certs/bob.key status Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
error: job not found

# Client cert signed by an untrusted CA
$ jobctl --cert certs/eve.crt --key certs/eve.key status Q5ZJ...
error: unknown certificate authority
```

## 6. Dependencies

| Dependency                                                                                                              | Purpose            |
|-------------------------------------------------------------------------------------------------------------------------|--------------------|
| Standard library (`os/exec`, `syscall`, `crypto/tls`, `crypto/x509`, `crypto/ecdsa`, `crypto/rand`, `log/slog`, `flag`) | Core functionality |
| `google.golang.org/grpc`                                                                                                | RPC transport      |
| `google.golang.org/protobuf`                                                                                            | Message runtime    |
| `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`                                                                         | Buildtime codegen  |

---

## 7. Future Considerations

| Area           | Current choice                                        | Production alternative                                                                    |
|----------------|-------------------------------------------------------|-------------------------------------------------------------------------------------------|
| Output storage | Unbounded in-memory `[]byte`                          | Chunked buffer, size caps per job, spill to disk, retention and eviction of finished jobs |
| Configuration  | Hardcoded address, paths, timeouts; minimal CLI flags | Config file and env vars                                                                  |
| AuthZ roles    | None                                                  | Custom cert extension, RBAC with per-command allow-lists                                  |
| PKI            | Offline dev CA via `make certs`                       | Online CA issuing short-lived certs, revocation, rotation                                 |
| Isolation      | Process group + `Pdeathsig`                           | cgroups v2 (CPU/memory/IO limits), namespaces, unprivileged uid per job (Level 5)         |
| Availability   | Single process, in-memory state, lost on restart      | Persist job metadata and output; multiple workers behind a scheduler                      |
| Auditing       | None                                                  | Update server to record each RPC w/ user, role, method, job ID, and resulting status code |