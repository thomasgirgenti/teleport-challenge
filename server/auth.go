package server

import (
	"context"
	"crypto/x509"
	"errors"
	"net"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// authInfo is the AuthInfo attached to every connection the server accepts.
type authInfo struct {
	credentials.TLSInfo
	User string
}

// identityCreds wraps TLS transport credentials and reads the client's identity during connection handshake
type identityCreds struct {
	credentials.TransportCredentials
}

// ServerHandshake runs the TLS handshake, which verifies the client cert then reads the user from the verified cert
func (c identityCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil {
		return nil, nil, err
	}

	tlsInfo, ok := info.(credentials.TLSInfo)
	if !ok {
		conn.Close()
		return nil, nil, errors.New("unexpected auth info type")
	}
	// RequireAndVerifyClientCert guarantees a verified chain after a successful handshake; check anyway rather than panic
	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		conn.Close()
		return nil, nil, errors.New("no verified client certificate")
	}
	user, err := userFromCert(tlsInfo.State.VerifiedChains[0][0])
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, authInfo{TLSInfo: tlsInfo, User: user}, nil
}

// Clone must return the wrapper, or the embedded Clone would drop it
func (c identityCreds) Clone() credentials.TransportCredentials {
	return identityCreds{TransportCredentials: c.TransportCredentials.Clone()}
}

// userFromCert returns the username (read from common name) of a verified client cert
func userFromCert(cert *x509.Certificate) (string, error) {
	if cert.Subject.CommonName == "" {
		return "", errors.New("client certificate has an empty common name")
	}
	return cert.Subject.CommonName, nil
}

// userFrom returns the user authenticated on the connection the RPC arrived on
func userFrom(ctx context.Context) (string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", false
	}
	ai, ok := p.AuthInfo.(authInfo)
	if !ok || ai.User == "" {
		return "", false
	}
	return ai.User, true
}
