//go:build linux

package server

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	pb "github.com/thomasgirgenti/teleport-challenge/gen/worker/v1"
)

func TestUserFromCert(t *testing.T) {
	tests := []struct {
		name    string
		cn      string
		want    string
		wantErr bool
	}{
		{"common name", "alice", "alice", false},
		{"empty common name", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := &x509.Certificate{Subject: pkix.Name{CommonName: tt.cn}}
			got, err := userFromCert(cert)
			if got != tt.want || (err != nil) != tt.wantErr {
				t.Fatalf("userFromCert = %q, %v; want %q, error %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestHandshake(t *testing.T) {
	ts := startTestServer(t)
	p := ts.pki

	tls12 := ts.clientTLSConfig(p.alice)
	tls12.MinVersion, tls12.MaxVersion = tls.VersionTLS12, tls.VersionTLS12

	tests := []struct {
		name string
		cfg  *tls.Config
		want codes.Code
	}{
		// NotFound means the connection was accepted and the RPC ran
		{"trusted client", ts.clientTLSConfig(p.alice), codes.NotFound},
		{"untrusted CA", ts.clientTLSConfig(p.eve), codes.Unavailable},
		{"no client cert", ts.clientTLSConfig(nil), codes.Unavailable},
		{"empty common name", ts.clientTLSConfig(p.emptyCN), codes.Unavailable},
		{"cert not valid for client auth", ts.clientTLSConfig(p.serverEKU), codes.Unavailable},
		{"TLS 1.2", tls12, codes.Unavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ts.clientWithConfig(t, tt.cfg)
			_, err := c.GetJobStatus(t.Context(), pb.GetJobStatusRequest_builder{JobId: proto.String("unknown")}.Build())
			wantCode(t, err, tt.want)
		})
	}
}
