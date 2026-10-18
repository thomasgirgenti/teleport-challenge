package cli

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	// serverName is the name the server's cert must be valid for.
	// TODO: Derive it from the address once the server can be configured to listen elsewhere.
	serverName = "localhost"

	// The client pings an idle connection after keepaliveTime and closes it if no ack arrives within keepaliveTimeout.
	// keepaliveTime must stay above the server's keepalive enforcement minimum (20s), or the server drops the connection.
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second
)

// tlsConfig returns the client's mTLS config: TLS 1.3 only, presenting the client cert and trusting only the server CA
func tlsConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load client key pair: %w", err)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read server CA: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("read server CA: no certificates found in %s", caFile)
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &cert, nil
		},
		RootCAs:    rootCAs,
		ServerName: serverName,
	}, nil
}

// dial returns a client connection to addr. It doesn't connect yet; the first RPC does.
func dial(addr string, cfg *tls.Config) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(credentials.NewTLS(cfg)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
			// A log stream can be idle for a long time while its job prints nothing
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	return conn, nil
}
