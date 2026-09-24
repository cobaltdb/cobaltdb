package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeMTLSTestPair writes a minimal self-signed cert/key pair (any parseable
// PEM works for LoadTLSConfig; AppendCertsFromPEM only needs valid DER).
func writeMTLSTestPair(t *testing.T, dir, base string) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"tls-mtls-test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("certgen: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("keyenc: %v", err)
	}
	certPath := filepath.Join(dir, base+".crt")
	keyPath := filepath.Join(dir, base+".key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// TestLoadTLSConfigEnforcesClientAuthWithCA pins the mTLS contract: a
// configured CA means client certificates are the authentication layer.
//
// Regression: LoadTLSConfig applied config.ClientAuth verbatim, so a server
// configured with CAFile but no explicit ClientAuth (the DefaultTLSConfig
// posture: NoClientCert) built a tls.Config whose ClientCAs pool was
// decorative — client certificates were never requested or verified, and
// operators relying on mTLS had no client authentication. The verified
// sibling pkg/replication/tls.go enforces RequireAndVerifyClientCert when a
// CA is configured; the server must match.
func TestLoadTLSConfigEnforcesClientAuthWithCA(t *testing.T) {
	dir := t.TempDir()
	serverCert, serverKey := writeMTLSTestPair(t, dir, "server")
	caCert, _ := writeMTLSTestPair(t, dir, "ca")

	tlsConf, err := LoadTLSConfig(&TLSConfig{
		Enabled:  true,
		CertFile: serverCert,
		KeyFile:  serverKey,
		CAFile:   caCert,
	})
	if err != nil {
		t.Fatalf("LoadTLSConfig with CA: %v", err)
	}
	if tlsConf.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("CA configured: ClientAuth = %v, want RequireAndVerifyClientCert", tlsConf.ClientAuth)
	}
	if tlsConf.ClientCAs == nil {
		t.Fatal("CA configured: ClientCAs pool is nil")
	}

	// Control: without a CA, the operator's ClientAuth is honored as-is.
	noCACfg, err := LoadTLSConfig(&TLSConfig{
		Enabled:  true,
		CertFile: serverCert,
		KeyFile:  serverKey,
	})
	if err != nil {
		t.Fatalf("LoadTLSConfig without CA: %v", err)
	}
	if noCACfg.ClientAuth != tls.NoClientCert {
		t.Fatalf("no CA: ClientAuth = %v, want NoClientCert", noCACfg.ClientAuth)
	}

	// Control: InsecureSkipVerify stays rejected.
	if _, err := LoadTLSConfig(&TLSConfig{
		Enabled: true, CertFile: serverCert, KeyFile: serverKey,
		CAFile: caCert, InsecureSkipVerify: true,
	}); err == nil {
		t.Fatal("InsecureSkipVerify accepted")
	}
}
