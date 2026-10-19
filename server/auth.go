package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// authInfo is the AuthInfo attached to every connection the server accepts.
type authInfo struct {
	credentials.TLSInfo
	User string
}

// identityCreds are server-only transport credentials: they run the TLS handshake, then read the client's identity from its verified cert
type identityCreds struct {
	cfg *tls.Config
}

// newIdentityCreds returns credentials that serve cfg
// Rejecting a cert w/o user inside the handshake sends the client a bad_certificate alert, rather than just closing the connection
func newIdentityCreds(cfg *tls.Config) *identityCreds {
	cfg = cfg.Clone()
	cfg.NextProtos = []string{"h2"}
	verify := cfg.VerifyConnection
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		if verify != nil {
			if err := verify(cs); err != nil {
				return err
			}
		}
		_, err := userFromState(cs)
		return err
	}
	return &identityCreds{cfg: cfg}
}

// ServerHandshake runs the TLS handshake then reads the user from the verified cert
func (c *identityCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn := tls.Server(raw, c.cfg)
	if err := conn.Handshake(); err != nil {
		conn.Close()
		return nil, nil, err
	}
	state := conn.ConnectionState()

	if state.NegotiatedProtocol != "h2" {
		conn.Close()
		return nil, nil, fmt.Errorf("client did not negotiate h2 (got %q)", state.NegotiatedProtocol)
	}

	// VerifyConnection already rejected a cert without a user during the handshake
	user, err := userFromState(state)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, authInfo{
		TLSInfo: credentials.TLSInfo{
			State:          state,
			CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
		},
		User: user,
	}, nil
}

// ClientHandshake only satisfies credentials.TransportCredentials. The server never calls it
func (*identityCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("identityCreds is server-only")
}

func (*identityCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (c *identityCreds) Clone() credentials.TransportCredentials {
	return &identityCreds{cfg: c.cfg.Clone()}
}

func (*identityCreds) OverrideServerName(string) error {
	return nil
}

// userFromState returns the username of the client's verified cert
func userFromState(cs tls.ConnectionState) (string, error) {
	// RequireAndVerifyClientCert guarantees a verified chain, but check anyway rather than panic
	if len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
		return "", errors.New("no verified client certificate")
	}
	return userFromCert(cs.VerifiedChains[0][0])
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
