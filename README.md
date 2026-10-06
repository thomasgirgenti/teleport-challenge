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
