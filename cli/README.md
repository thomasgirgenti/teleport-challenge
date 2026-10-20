# Teleport CLI
`jobctl` connects to the worker service over mTLS and can start, stop, get status, and stream output of a job.

## Running

```sh
make certs              # writes a dev CA, server cert, and client certs (alice, bob, eve) to certs/
go run ./cmd/workerd &  # Linux only
go run ./cmd/jobctl -h
```

`jobctl` itself builds and runs on any OS. `make build` puts it in `bin/jobctl`.

## Usage

```
jobctl [flags] <command> [args]

Commands:
  start <cmd> [args...]        Start a job; prints the job ID
  stop <job-id>                Kill a running job (SIGKILL)
  status <job-id>              Show job status
  logs <job-id>                Stream output from the start, following until the job ends

Flags:
  --addr   worker server address       (default 127.0.0.1:50051)
  --cert   client certificate file     (default certs/alice.crt)
  --key    client private key file     (default certs/alice.key)
  --ca     server CA certificate file  (default certs/server-ca.crt)
```

Global flags come before the command. The defaults use the certs from `make certs` and connect as alice, so run `jobctl` from the repo root.
Everything after `start` is the job's command line, passed through as-is.

## Examples

```console
$ jobctl start bash -c 'for i in 1 2 3; do echo "tick $i"; sleep 1; done'
Q5ZJ3KXM7TAB2C4VNWHRY6DPLE

# returns when the job ends
$ jobctl logs Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
tick 1
tick 2
tick 3

$ jobctl status Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
ID:       Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
Owner:    alice
Command:  ["bash" "-c" "for i in 1 2 3; do echo \"tick $i\"; sleep 1; done"]
State:    EXITED
Exit:     0
Started:  2026-10-09T18:40:01Z
Ended:    2026-10-09T18:40:04Z

# bob can't see alice's jobs
$ jobctl --cert certs/bob.crt --key certs/bob.key status Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
error: job not found

# eve's cert is signed by a CA the server doesn't trust
$ jobctl --cert certs/eve.crt --key certs/eve.key status Q5ZJ3KXM7TAB2C4VNWHRY6DPLE
error: connection error: desc = "error reading server preface: remote error: tls: unknown certificate authority"
```

In TLS 1.3 the client's side of the handshake ends before the server checks the client cert.
So a rejected client sometimes sees `broken pipe` or `connection reset by peer` instead of the server's reason, when it was already writing as the server closed the connection.

`status` shows `Exit` for jobs that exited on their own, `Signal` for jobs that were killed (`STOPPED` if by `jobctl stop`, `SIGNALED` otherwise), and `Ended` once the job has ended.

`logs` writes the job's combined stdout and stderr as raw bytes, so binary output can be piped or redirected unchanged. Ctrl-C stops following; the job keeps running.

## Exit codes

| Code  | Meaning                                                        |
|-------|----------------------------------------------------------------|
| `0`   | Success                                                        |
| `1`   | The command failed, e.g. an RPC error or an unreadable cert    |
| `2`   | Bad flags or arguments; nothing was sent to the server         |
| `130` | Interrupted by SIGINT or SIGTERM                               |

## Timeouts

`start`, `stop`, and `status` time out after 10s. `logs` has no timeout and follows the job for as long as it runs.

## Testing

```sh
go test ./cli/    # argument parsing and output formatting, on any OS
make test-linux   # also runs the tests against a real server, in a golang:1.27 container w/ Docker
```
