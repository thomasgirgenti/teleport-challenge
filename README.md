# teleport-challenge
Teleport systems take-home challenge

## Structure
This repo is split into 3 main subdirectories:
1. **core**
   - worker library with methods to start/stop/query status of a job
2. **server**
   - [gRPC](https://grpc.io) API to start/stop/get status/stream output of a job
3. **cli** 
   - CLI to connect to worker service and start, stop, get status, and stream output of a job

Binaries live under `cmd/`: `workerd` (the server), `jobctl` (the CLI), and `certgen` (dev certs, run with `make certs`).
`make build` builds them all into `bin/`.

## Running on macOS with Docker
`workerd` only runs on Linux, so on a Mac run it in a container and connect to it with a native `jobctl`. Requires Docker Desktop (or another Docker engine) and Go.

```sh
make certs              # dev CA, server cert, and client certs in certs/
make build-native-cli   # bin/jobctl for macOS
make docker-run         # builds the workerd image and serves on 127.0.0.1:50051, in the foreground
```

Then, in a second terminal:

```sh
bin/jobctl start bash -c 'for i in 1 2 3; do echo "tick $i"; sleep 1; done'
bin/jobctl logs <job-id>
bin/jobctl status <job-id>
```

Stop the server with Ctrl-C in the first terminal, or `make docker-stop`. Either way it kills every job before exiting.

- The certs are mounted read-only into the container, never copied into the image. The server loads them at startup, so restart the container after re-running `make certs`.
- The port is published on `127.0.0.1` only, so the server isn't reachable from other machines.
- Jobs run inside the container as root, with the tools from the `debian:trixie` image (`bash`, coreutils, etc.).
- `make build` writes *Linux* binaries to `bin/`, overwriting `bin/jobctl`. Run `make build-native-cli` again afterwards.

See the [CLI README](cli/README.md) for every command and flag.

## Rough notes from kickoff call
- job output should be stored in memory, assume unlimited memory on worker machine
- include bootstrap script to gen certs for mTLS auth
- streaming output should function similarly to `docker logs -f`
- client implements all business logic. server is a thin wrapper w/ auth.
- lean on go std lib (crypto, tls, grpc, etc), should use pretty much no 3rd party deps
- each component should be buildable locally (with documented instructions)
- Design doc should be highly technically detailed and cover:
  - how all reqs will be satisfied
  - in-depth security section, documenting _how_ auth will be implemented
  - complete API design
  - CLI examples
  - how clients discover new output
  - possible errors and how they will be handled 
  - required dependencies (crypto, tls, grpc, cli?, etc)
  - data struct for storing output in memory
- Stop command should immediately kill the child process with SIGKILL
- Server shutdown should immediately stop all child processess with SIGKILL
