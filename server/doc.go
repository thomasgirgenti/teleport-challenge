// Package server serves the WorkerService gRPC API on top of the core job library.
//
// Clients authenticate with mTLS (TLS 1.3 only).
// The username is the common name of the client's verified certificate, read once per connection during the TLS handshake.
// A job is owned by the user who started it, and every other user gets NotFound for it.
package server
