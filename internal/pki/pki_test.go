package pki

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
)

func TestLeafCertsVerify(t *testing.T) {
	ca := mustCert(t)(NewCA("test-ca"))
	tests := []struct {
		name    string
		cert    *Cert
		usage   x509.ExtKeyUsage
		dnsName string
		wantCN  string
	}{
		{"server", mustCert(t)(NewServerCert(ca)), x509.ExtKeyUsageServerAuth, "localhost", "localhost"},
		{"client", mustCert(t)(NewClientCert(ca, "alice")), x509.ExtKeyUsageClientAuth, "", "alice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := x509.VerifyOptions{
				Roots:     Pool(ca),
				KeyUsages: []x509.ExtKeyUsage{tt.usage},
				DNSName:   tt.dnsName,
			}
			if _, err := tt.cert.Cert.Verify(opts); err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if cn := tt.cert.Cert.Subject.CommonName; cn != tt.wantCN {
				t.Errorf("CN = %q, want %q", cn, tt.wantCN)
			}
			if tt.cert.Cert.SignatureAlgorithm != x509.ECDSAWithSHA256 {
				t.Errorf("signature algorithm = %v, want ECDSAWithSHA256", tt.cert.Cert.SignatureAlgorithm)
			}
		})
	}
}

func TestLeafRejectedForWrongUsage(t *testing.T) {
	ca := mustCert(t)(NewCA("test-ca"))
	client := mustCert(t)(NewClientCert(ca, "alice"))

	// A client cert must not authenticate a server
	opts := x509.VerifyOptions{Roots: Pool(ca), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if _, err := client.Cert.Verify(opts); err == nil {
		t.Fatal("client cert verified for ServerAuth, want error")
	}
}

func TestLeafRejectedByOtherCA(t *testing.T) {
	ca := mustCert(t)(NewCA("test-ca"))
	other := mustCert(t)(NewCA("other-ca"))
	client := mustCert(t)(NewClientCert(ca, "alice"))

	opts := x509.VerifyOptions{Roots: Pool(other), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if _, err := client.Cert.Verify(opts); err == nil {
		t.Fatal("cert verified against a CA that didn't sign it, want error")
	}
}

func TestCAs(t *testing.T) {
	ca := mustCert(t)(NewCA("test-ca"))
	if !ca.Cert.IsCA || !ca.Cert.MaxPathLenZero || ca.Cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Fatalf("CA cert: IsCA = %v, MaxPathLenZero = %v, KeyUsage = %v", ca.Cert.IsCA, ca.Cert.MaxPathLenZero, ca.Cert.KeyUsage)
	}

	leaf := mustCert(t)(NewClientCert(ca, "alice"))
	if leaf.Cert.IsCA || leaf.Cert.KeyUsage&x509.KeyUsageCertSign != 0 {
		t.Fatalf("leaf cert can sign certificates: IsCA = %v, KeyUsage = %v", leaf.Cert.IsCA, leaf.Cert.KeyUsage)
	}
}

func TestPEMRoundTrip(t *testing.T) {
	ca := mustCert(t)(NewCA("test-ca"))
	keyPEM, err := ca.KeyPEM()
	if err != nil {
		t.Fatal(err)
	}
	// X509KeyPair parses both PEM blocks and checks that the key matches the certificate
	if _, err := tls.X509KeyPair(ca.CertPEM(), keyPEM); err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
}

func mustCert(t *testing.T) func(*Cert, error) *Cert {
	return func(c *Cert, err error) *Cert {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
}
