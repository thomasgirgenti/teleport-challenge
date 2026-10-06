# Teleport Server
[gRPC](https://grpc.io) API to start/stop/get status/stream output of a job.
* Use mTLS authentication and verify client certificate. Set up strong set of
  cipher suites for TLS and good crypto setup for certificates. Do not use any
  other authentication protocols on top of mTLS.
* Use a simple authorization scheme.