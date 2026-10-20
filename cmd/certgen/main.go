// Command certgen writes a development PKI for the worker server and its clients to the certs directory.
//
// Re-running it replaces every cert and key. CA private keys are never written to disk:
// nothing issues certs after bootstrap, so there is no reason to keep them.
// TODO: In production, issue short-lived certs from an online CA with revocation and rotation
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/thomasgirgenti/teleport-challenge/internal/pki"
)

// TODO: Add configuration system
const outDir = "certs"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "certgen: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// Separate CAs, so a stolen client key can never be used to impersonate the server
	serverCA, err := pki.NewCA("worker-server-ca")
	if err != nil {
		return err
	}
	clientCA, err := pki.NewCA("worker-client-ca")
	if err != nil {
		return err
	}
	// eve's CA is trusted by nobody; it is only used to demo a rejected client
	untrustedCA, err := pki.NewCA("untrusted-ca")
	if err != nil {
		return err
	}

	if err := writeCert("server-ca", serverCA, false); err != nil {
		return err
	}
	if err := writeCert("client-ca", clientCA, false); err != nil {
		return err
	}

	server, err := pki.NewServerCert(serverCA)
	if err != nil {
		return err
	}
	if err := writeCert("server", server, true); err != nil {
		return err
	}

	clients := []struct {
		name string
		ca   *pki.Cert
	}{
		{"alice", clientCA},
		{"bob", clientCA},
		{"eve", untrustedCA},
	}
	for _, c := range clients {
		cert, err := pki.NewClientCert(c.ca, c.name)
		if err != nil {
			return err
		}
		if err := writeCert(c.name, cert, true); err != nil {
			return err
		}
	}

	fmt.Printf("wrote certificates to %s/\n", outDir)
	return nil
}

// writeCert writes <name>.crt and, if withKey is set, <name>.key readable only by the owner
func writeCert(name string, c *pki.Cert, withKey bool) error {
	if err := os.WriteFile(filepath.Join(outDir, name+".crt"), c.CertPEM(), 0o644); err != nil {
		return err
	}
	if !withKey {
		return nil
	}
	keyPEM, err := c.KeyPEM()
	if err != nil {
		return err
	}
	return writePrivate(filepath.Join(outDir, name+".key"), keyPEM)
}

// writePrivate writes data to path with 0600 permissions.
// os.WriteFile only applies its permissions when it creates the file, so an existing file is removed first.
func writePrivate(path string, data []byte) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
