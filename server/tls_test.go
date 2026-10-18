package server

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/thomasgirgenti/teleport-challenge/internal/pki"
)

func TestTLSConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	ca, err := pki.NewCA("test-ca")
	if err != nil {
		t.Fatal(err)
	}
	server, err := pki.NewServerCert(ca)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := server.KeyPEM()
	if err != nil {
		t.Fatal(err)
	}
	certFile := write("server.crt", server.CertPEM())
	keyFile := write("server.key", keyPEM)
	caFile := write("ca.crt", ca.CertPEM())
	notPEM := write("not-pem.crt", []byte("not a certificate"))

	t.Run("valid", func(t *testing.T) {
		cfg, err := TLSConfig(certFile, keyFile, caFile)
		if err != nil {
			t.Fatalf("TLSConfig: %v", err)
		}
		if cfg.MinVersion != tls.VersionTLS13 || cfg.ClientAuth != tls.RequireAndVerifyClientCert {
			t.Fatalf("MinVersion, ClientAuth = %x, %v; want TLS 1.3, RequireAndVerifyClientCert", cfg.MinVersion, cfg.ClientAuth)
		}
	})

	errTests := []struct {
		name                     string
		certFile, keyFile, caCrt string
	}{
		{"missing cert", filepath.Join(dir, "missing.crt"), keyFile, caFile},
		{"key doesn't match cert", caFile, keyFile, caFile},
		{"missing client CA", certFile, keyFile, filepath.Join(dir, "missing.crt")},
		{"client CA without certs", certFile, keyFile, notPEM},
	}
	for _, tt := range errTests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := TLSConfig(tt.certFile, tt.keyFile, tt.caCrt); err == nil {
				t.Fatal("TLSConfig succeeded, want error")
			}
		})
	}
}
