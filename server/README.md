# Teleport Server
[gRPC](https://grpc.io) API to start/stop/get status/stream output of a job, built on the [core](../core) library.

## Running

```sh
make certs              # writes a dev CA, server cert, and client certs (alice, bob, eve) to certs/
go run ./cmd/workerd    # serves on 127.0.0.1:50051 until SIGINT/SIGTERM
```

`--addr` changes the listen address, e.g. `--addr 0.0.0.0:50051`. The cert paths are hardcoded and relative to the working directory (see `cmd/workerd/main.go`).

Linux only. On macOS, run it in Docker: see [Running on macOS with Docker](../README.md#running-on-macos-with-docker).

On SIGINT/SIGTERM the server kills every job with SIGKILL, so every open output stream ends with `OK`, then stops gracefully.
Jobs and their output live in memory and are lost when the server stops.

## Authentication

mTLS with TLS 1.3 only. Clients must present a cert signed by the client CA (`certs/client-ca.crt`) with the `ClientAuth` extended key usage.
The username is the cert's common name, read once per connection during the TLS handshake.
A connection with an untrusted cert, no cert, or an empty common name is refused before any RPC runs, and the client sees `Unavailable`.

Clients verify the server against a separate server CA (`certs/server-ca.crt`), so a stolen client key can't be used to impersonate the server.

## Authorization

A job is owned by the user who started it. Only the owner can get its status, stop it, or stream its output.
Other users get `NotFound`, the same error as for a job that doesn't exist.

## Errors

| Condition                                         | Code                 |
|---------------------------------------------------|----------------------|
| Unknown job, or a job owned by another user       | `NotFound`           |
| Empty job ID or command, or command not found     | `InvalidArgument`    |
| `StopJob` on a job that has already ended         | `FailedPrecondition` |
| `StartJob` while the server is shutting down      | `Unavailable`        |
| `StopJob` waited 10s without the job ending       | `DeadlineExceeded`   |
| Anything unexpected (details are logged)          | `Internal`           |

## Testing

```sh
make test         # on Linux
make test-linux   # on other platforms, in a golang:1.27 container w/ Docker
```
