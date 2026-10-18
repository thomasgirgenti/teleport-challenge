// Package pki generates the ECDSA P-256 certificates used for mTLS between the worker server and its clients.
//
// It is a development PKI: CAs are created offline and nothing is revoked or rotated.
// TODO: In production, issue short-lived certs from an online CA with revocation and rotation.
package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

const (
	caValidity   = 365 * 24 * time.Hour
	leafValidity = 90 * 24 * time.Hour

	// clockSkew backdates NotBefore so a peer with a slightly slow clock still accepts a fresh cert
	clockSkew = time.Minute
)

// Cert is a certificate and its private key
type Cert struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
}

// NewCA returns a self-signed CA certificate that can sign leaf certificates but not intermediate CAs
func NewCA(cn string) (*Cert, error) {
	return newCert(&x509.Certificate{
		Subject:               pkix.Name{CommonName: cn},
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}, caValidity, nil)
}

// NewServerCert returns a server leaf certificate signed by ca, valid for localhost
func NewServerCert(ca *Cert) (*Cert, error) {
	return newCert(&x509.Certificate{
		Subject:     pkix.Name{CommonName: "localhost"},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, leafValidity, ca)
}

// NewClientCert returns a client leaf certificate signed by ca. The server uses cn as the client's username.
func NewClientCert(ca *Cert, cn string) (*Cert, error) {
	return newCert(&x509.Certificate{
		Subject:     pkix.Name{CommonName: cn},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, leafValidity, ca)
}

// NewLeaf returns a leaf certificate built from template and signed by ca.
// It fills in the serial number, validity, and key; everything else is taken from template.
// Prefer NewServerCert and NewClientCert, this is for tests that need unusual certs.
func NewLeaf(ca *Cert, template *x509.Certificate) (*Cert, error) {
	return newCert(template, leafValidity, ca)
}

// newCert generates a key and signs a certificate for it with parent's key, or self-signs it if parent is nil
func newCert(template *x509.Certificate, validity time.Duration, parent *Cert) (*Cert, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	// 128 random bits, as recommended by the CA/Browser Forum baseline requirements (at least 64)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}

	tmpl := *template
	now := time.Now()
	tmpl.SerialNumber = serial
	tmpl.NotBefore = now.Add(-clockSkew)
	tmpl.NotAfter = now.Add(validity)

	issuer, signer := &tmpl, key
	if parent != nil {
		issuer, signer = parent.Cert, parent.Key
	}
	// Signed with ECDSA-SHA256, the default for a P-256 key
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, issuer, &key.PublicKey, signer)
	if err != nil {
		return nil, fmt.Errorf("create certificate %q: %w", tmpl.Subject.CommonName, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse certificate %q: %w", tmpl.Subject.CommonName, err)
	}
	return &Cert{Cert: cert, Key: key}, nil
}

// CertPEM returns the PEM-encoded certificate
func (c *Cert) CertPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Cert.Raw})
}

// KeyPEM returns the PEM-encoded PKCS #8 private key
func (c *Cert) KeyPEM() ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(c.Key)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// TLSCertificate returns the certificate and key for use in a tls.Config
func (c *Cert) TLSCertificate() tls.Certificate {
	return tls.Certificate{
		Certificate: [][]byte{c.Cert.Raw},
		PrivateKey:  c.Key,
		Leaf:        c.Cert,
	}
}

// Pool returns a cert pool that trusts the given CAs
func Pool(cas ...*Cert) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, ca := range cas {
		pool.AddCert(ca.Cert)
	}
	return pool
}
