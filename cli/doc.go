// Package cli implements jobctl, the command line client for the worker's gRPC API.
//
// It authenticates to the server with an mTLS client certificate (TLS 1.3 only) and can start, stop, get the status of, and stream the output of jobs the caller owns.
package cli
