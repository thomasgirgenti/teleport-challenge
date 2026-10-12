# Design Doc: Job Worker Service

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
- RBAC - for our current purposes two roles will exist: `admin` and `user`. `admin` role will be able to manage any job, while `user` can only stream/stop/query jobs they own.

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
    mu     sync.RWMutex
    jobs   map[string]*Job
    closed bool
}

func NewManager() *Manager

// Start launches a cmd with args
func (m *Manager) Start(cmd string, args []string) (*Job, error)

// Get returns a job by id
func (m *Manager) Get(id string) (*Job, error)

// List enumerates all jobs (current and completed)
func (m *Manager) List() ([]*Job, error)

// Shutdown rejects new jobs, stops running jobs, and stops the worker
func (m *Manager) Shutdown(ctx context.Context) error

type Job struct { /* defined under Process Lifecycle */ }

func (j *Job) ID() string
func (j *Job) Owner() string
func (j *Job) Status() Status

// Stop SIGKILLs the job's process group and waits until the job reaches a terminal state or ctx is done (timeout)
func (j *Job) Stop(ctx context.Context) error

// Output returns a reader of job's entire output from the first byte
// Each call returns an independent reader
func (j *Job) Output(ctx context.Context) io.Reader

// Done is closed when the job has reached a terminal state
func (j *Job) Done() <-chan struct{}

type Status struct { /* Defined under Process Lifecycle */}
```

### Output Storage and Streaming

Append-only byte buffer with a broadcast notification channel:

```go
type OutputBuffer struct {
    mu     sync.Mutex    // locking mechanism for concurrency safety on Write, Close, and readAt
    data   []byte        // append-only array of all job output
    closed bool          // true when the job's pipe hits EOF
    notify chan struct{} // closed and replaced on every Write and Close
}

// Write appends new bytes to `data`, notifies waiting readers w/ `close(b.notify)`, and replaces `notify` with a new channel
// Holds lock while writing/notifying
func (b *OutputBuffer) Write(p []byte) error { }

// Close is called when a job's pipe hits EOF and sets `b.closed` to true
// Prevents new logs from being appended and signals to readers that there will be no new output
// Holds lock while setting closed/notifying
func (b *OutputBuffer) Close() { }

// readAt copies data at offset into p, returns # bytes copied
// If there is no data at offset, returns `b.notify` so readers are notified when new output is available
func (b *OutputBuffer) readAt(p []byte, offset int) (int, <-chan struct{}, error) { }
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
func (r *OutputReader) Read(p []byte) (int, error) { }
```

**Features of this output structure:**

- Output is stored from start of job execution and never truncated
- Readers do not sleep and re-check; a reader with nothing to read waits until new output becomes available (via notify channel). `Write` wakes every waiting reader exactly once by closing the channel.
- Multiple concurrent clients at little cost: Readers share the buffer read-only and keep their own offsets. Adding a reader costs O(1) and needs no registration/cleanup.
- Data is handled as `[]byte`, so it is binary safe w/ no encoding assumptions
- `Write` doesn't wait for readers. A slow gRPC client only falls behind on its own offset. A job is never blocked by a consumer.
- Readers start no goroutines, so there is nothing to close. Cancelling the context unblocks a waiting `Read` right away. Minimizes risk of goroutine leaks.

**Alternatives considered:**

- `sync.Cond`: can't be combined with context cancellation, so cancelling a waiter would need an extra goroutine that calls `Broadcast`. This can be complex to manage and lead to leaks if not handled precisely.
- Per-subscriber channels (fan-out): the writer would have to block on a slow subscriber or drop data.
- Ring buffer: breaks the "output from start" requirement.

### Process Lifecycle

```go
type Job struct {
    id, owner string
    cmd       *exec.Cmd
    output    *outputBuffer
    done      chan struct{} // closed after final state is recorded
    mu            sync.Mutex
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

1. Reject the call if `cmd == ""`, or with `ErrShuttingDown` if the manager is closed.
2. Build an [`exec.Cmd`](https://pkg.go.dev/os/exec#Cmd).
3. Call `cmd.Start()`. On failure, no job is registered and the error is returned to the caller.
4. Register the job under `manager.jobs`, then start a single waiter goroutine:

**Waiter goroutine.** This is the only goroutine per job:

```go
go func(job *Job) { 
	// block until leader exits
	// get lock on job
	// reap orphaned group members
	// release job lock
	
	// reap leader
	// readers drain remaining output, then get io.EOF
	
	// get lock on job
	// update job state, exit code / signal / endedAt
	// unlock job
	
	// close(job.done)
}(j)
```

**Stopping a job** (`Job.Stop`):

```go
func (j *Job) Stop(ctx context.Context) error {
	// get lock on job
	// if job.leaderExited, release job and return ErrNotRunning
	// set job.stopRequested = true
	// kill process group with SIGKILL
	// unlock job
	
	// return when job.done is signaled or on timeout (ctx.Done)
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

  rpc ListJobs(ListJobsRequest) returns (ListJobsResponse);
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

message ListJobsRequest {}

message ListJobsResponse {
  repeated JobStatus jobs = 1;
}
```

### Networking

- **Keepalive:** Server sets `keepalive.ServerParameters{Time: 30s, Timeout: 10s}` and client sets matching params. A dead client on a long-running` logs` stream is detected within ~40s. Its stream context is then cancelled so nothing leaks.
- **Timeouts:** CLI unary calls use a 10s timeout (matching server req timeout). The output stream has no timeout because it lives as long as the job. It is canceled by Ctrl-C through `signal.NotifyContext`.
- **Server shutdown** on `SIGINT`/`SIGTERM`: `manager.Shutdown(ctx)` kills all jobs. Their buffers close, so every open stream ends naturally with `OK`. Then `grpcServer.GracefulStop()` runs with a fallback to `Stop()`.

---

## 4. Security

### Threat Model

- Clients are authenticated only by X.509 client certificates (mTLS). No passwords, tokens, or other schemes are layered on top.
- Any authenticated user can run arbitrary commands as the server's Unix user.
- All traffic is encrypted and mutually authenticated.

### Certificates

A small bootstrap tool (run w/ `make certs`) will use only `crypto/x509` and generate the following:

| Cert                    | Key         | Key usage         | Ext key usage | Subject / SAN                             |
|-------------------------|-------------|-------------------|---------------|-------------------------------------------|
| Server CA (self-signed) | ECDSA P-256 | CertSign, CRLSign | —             | `CN=worker-server-ca`                     |
| Client CA (self-signed) | ECDSA P-256 | CertSign, CRLSign | —             | `CN=worker-client-ca`                     |
| Server leaf             | ECDSA P-256 | DigitalSignature  | ServerAuth    | SAN: `localhost`, `127.0.0.1`, `::1`      |
| Client leaf `alice`     | ECDSA P-256 | DigitalSignature  | ClientAuth    | `CN=alice, OU=admin`                      |
| Client leaf `bob`       | ECDSA P-256 | DigitalSignature  | ClientAuth    | `CN=bob, OU=user`                         |
| Client leaf `rogue`     | ECDSA P-256 | DigitalSignature  | ClientAuth    | `CN=rogue, OU=user`, signed by a rogue CA |

- There are separate server and client CAs. The server trusts only the client CA for client auth, and clients trust only the server CA. A stolen client key can never impersonate the server.
- Signatures are ECDSA with SHA-256. Serial numbers are 128-bit values from `crypto/rand`.
- ECDSA P-256 over RSA for smaller/faster keys at roughly the 128-bit security level with universal support.

### TLS Config

Server:

```go
&tls.Config{
    MinVersion:       tls.VersionTLS13,
    Certificates:     []tls.Certificate{serverCert},
    ClientAuth:       tls.RequireAndVerifyClientCert,
    ClientCAs:        clientCAPool,
    CurvePreferences: []tls.CurveID{tls.X25519MLKEM768},
}
```

Client:

```go
&tls.Config{
    MinVersion:       tls.VersionTLS13,
    Certificates:     []tls.Certificate{clientCert},
    RootCAs:          serverCAPool,
    ServerName:       "localhost",
    CurvePreferences: []tls.CurveID{tls.X25519MLKEM768},
}
```

- **TLS 1.3 only.** We manage both peers, so legacy TLS support is unnecessary.
- **Key exchange:** uses post-quantum `X25519MLKEM768`. Can include X25519 and P-256 as fallbacks if needed.
- **Verification:** with `RequireAndVerifyClientCert`, the server rejects connections that lack a client cert chaining to the client CA. The client checks the server cert against the server CA and the `localhost` SAN.

### Authentication and Authorization

Identity extraction happens in a unary and a stream interceptor, so every RPC passes through it:

1. `peer.FromContext(ctx)` gives `credentials.TLSInfo`. Will use `State.VerifiedChains[0][0]`to get leaf after verification, not the unverified `PeerCertificates`.
2. `CN` is the username and must be non-empty.
3. `OU` is the role. It must contain exactly one value, and that value must be one of `admin` or `user` (placeholder roles). Otherwise the RPC is rejected with `PermissionDenied`.
4. Store an `Identity{User, Role}` in the context under an unexported key type.

OU field is used for simplicity. A production system would put roles in a custom X.509 extension OID.

**Authorization policy:**

| Role    | StartJob | StopJob / GetJobStatus / StreamJobOutput / ListJobs |
|---------|----------|-----------------------------------------------------|
| `admin` | allowed  | any job                                             |
| `user`  | allowed  | only jobs where `job.Owner() == identity.User`      |

- `StartJob` records `identity.User` as the job's owner.
- A non-owner gets `NotFound`, not `PermissionDenied`. The response does not reveal whether a job ID exists.
- The check runs in a single helper, `authorizedJob(ctx, id)`, which every per-job handler calls.
- The core library stays unaware of auth, storing `owner` as an opaque string.

---

## 5. CLI UX

The CLI binary is `teleport`.
- uses the standard library `flag` package with one `FlagSet` per subcommand
- global flags come before the subcommand

```
teleport [--addr localhost:50051] \
         [--cert certs/alice.crt] \
         [--key certs/alice.key] \
         [--ca certs/server-ca.crt] \
         <command> [args]

Commands:
  start [--] <cmd> [args...]   Start a job; prints the job ID
  stop <job-id>                Kill a running job (SIGKILL)
  status <job-id>              Show job status
  logs <job-id>                Stream output from the start, following until the job ends
  list                         List jobs (running and finished), oldest first
```

Examples:

```console
$ teleport start -- bash -c 'for i in 1 2 3; do echo "tick $i"; sleep 1; done'
Q5ZJ3KXM7TAB2C4VNWHRY6DPLE

# returns when the job ends
$ teleport logs Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
tick 1
tick 2
tick 3
$

$ teleport status Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
ID:       Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
Owner:    alice
Command:  bash -c 'for i in 1 2 3; do echo "tick $i"; sleep 1; done'
State:    EXITED
Exit:     0
Started:  2026-10-09T18:40:01Z
Ended:    2026-10-09T18:40:04Z

$ teleport start sleep 1000
RT4L2QW...
$ teleport stop RT4L2QW...
$ teleport status RT4L2QW...
...
State:    STOPPED
Signal:   killed

$ teleport stop RT4L2QW...
error: job is not running

$ teleport list
ID                          OWNER  STATE     STARTED               COMMAND
Q5ZJ3KXM7TAB2C4VNWHRY6DPLE  alice  EXITED    2026-10-09T18:40:01Z  bash -c 'for i in 1 2 3; do echo "tick $i"; sleep 1; done'
RT4L2QWX8NB5KZ3MJD7HPC6YVE  alice  STOPPED   2026-10-09T18:41:15Z  sleep 1000
M8D2VKX4PQ7RZ5NBT3WJHC9GYA  bob    RUNNING   2026-10-09T18:42:30Z  tail -f /var/log/syslog

# bob (role=user) cannot see alice's jobs
$ teleport --cert certs/bob.crt --key certs/bob.key status Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
error: job not found

# bob's list shows only bob's own jobs
$ teleport --cert certs/bob.crt --key certs/bob.key list
ID                          OWNER  STATE     STARTED               COMMAND
M8D2VKX4PQ7RZ5NBT3WJHC9GYA  bob    RUNNING   2026-10-09T18:42:30Z  tail -f /var/log/syslog

# Client cert signed by an untrusted CA
$ teleport --cert certs/eve.crt --key certs/eve.key status Q5ZJ...
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

| Area           | Current choice                                        | Production path                                                                           |
|----------------|-------------------------------------------------------|-------------------------------------------------------------------------------------------|
| Output storage | Unbounded in-memory `[]byte`                          | Chunked buffer, size caps per job, spill to disk, retention and eviction of finished jobs |
| Configuration  | Hardcoded address, paths, timeouts; minimal CLI flags | Config file and env vars                                                                  |
| AuthZ roles    | X.509 `OU` with two roles                             | Custom cert extension, RBAC with per-command allow-lists                                  |
| PKI            | Offline dev CA via `make certs`                       | Online CA issuing short-lived certs, revocation, rotation                                 |
| Isolation      | Process group + `Pdeathsig`                           | cgroups v2 (CPU/memory/IO limits), namespaces, unprivileged uid per job (Level 5)         |
| Availability   | Single process, in-memory state, lost on restart      | Persist job metadata and output; multiple workers behind a scheduler                      |
| Auditing       | None                                                  | Update server to record each RPC w/ user, role, method, job ID, and resulting status code |