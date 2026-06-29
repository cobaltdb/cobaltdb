package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

type shortServerWriteConn struct {
	limit int
}

func (c *shortServerWriteConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *shortServerWriteConn) Close() error                     { return nil }
func (c *shortServerWriteConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *shortServerWriteConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *shortServerWriteConn) SetDeadline(time.Time) error      { return nil }
func (c *shortServerWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (c *shortServerWriteConn) SetWriteDeadline(time.Time) error { return nil }

func (c *shortServerWriteConn) Write(p []byte) (int, error) {
	if len(p) > c.limit {
		return c.limit, nil
	}
	return len(p), nil
}

func generateTestCertHelper(t *testing.T) (string, string) {
	t.Helper()
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "test.crt")
	keyFile := filepath.Join(tmpDir, "test.key")
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"Test"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	certOut, _ := os.Create(certFile)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certOut.Close()
	keyBytes, _ := x509.MarshalECPrivateKey(priv)
	keyOut, _ := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	keyOut.Close()
	return certFile, keyFile
}

// ============ TLS TESTS ============

func TestDefaultTLSConfigCov(t *testing.T) {
	cfg := DefaultTLSConfig()
	if cfg.Enabled {
		t.Error("expected disabled")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Error("bad min")
	}
	if cfg.MaxVersion != tls.VersionTLS13 {
		t.Error("bad max")
	}
	if len(cfg.CipherSuites) == 0 {
		t.Error("no ciphers")
	}
	if cfg.SelfSignedOrg != "CobaltDB" {
		t.Error("bad org")
	}
}

func TestLoadTLSConfigDisabledCov(t *testing.T) {
	tc, err := LoadTLSConfig(&TLSConfig{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if tc != nil {
		t.Error("expected nil")
	}
}

func TestLoadTLSConfigRejectsInsecureSkipVerify(t *testing.T) {
	tc, err := LoadTLSConfig(&TLSConfig{Enabled: false, InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("disabled TLS should ignore InsecureSkipVerify: %v", err)
	}
	if tc != nil {
		t.Fatal("expected nil TLS config when disabled")
	}

	_, err = LoadTLSConfig(&TLSConfig{Enabled: true, InsecureSkipVerify: true})
	if !errors.Is(err, ErrInsecureTLS) {
		t.Fatalf("expected ErrInsecureTLS, got %v", err)
	}
}

func TestLoadTLSConfigNoCertCov(t *testing.T) {
	_, err := LoadTLSConfig(&TLSConfig{Enabled: true})
	if !errors.Is(err, ErrInvalidCert) {
		t.Errorf("got %v", err)
	}
}

func TestLoadTLSConfigBadFileCov(t *testing.T) {
	_, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: "/no.pem", KeyFile: "/no.key"})
	if err == nil {
		t.Error("expected error")
	}
}

func TestLoadTLSConfigValidCov(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	tc, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatal(err)
	}
	if tc == nil {
		t.Fatal("nil")
	}
}

func TestLoadTLSConfigRejectsUnsafeTLSFiles(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	keyLink := filepath.Join(t.TempDir(), "server-key-link.pem")
	if err := os.Symlink(kf, keyLink); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	_, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: keyLink})
	if err == nil {
		t.Fatal("expected symlink key file to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}

	_, err = LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf, CAFile: t.TempDir()})
	if err == nil {
		t.Fatal("expected directory CA file to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestLoadTLSConfigRejectsOversizedTLSFile(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	if err := os.Truncate(cf, maxTLSFileBytes+1); err != nil {
		t.Fatalf("truncate cert file: %v", err)
	}

	_, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf})
	if err == nil {
		t.Fatal("expected oversized TLS file to be rejected")
	}
	if !strings.Contains(err.Error(), "TLS file too large") {
		t.Fatalf("expected oversized TLS file rejection, got %v", err)
	}
}

func TestLoadTLSConfigRestrictsPrivateKeyPermissions(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	if err := os.Chmod(kf, 0644); err != nil {
		t.Fatalf("chmod key: %v", err)
	}

	if _, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf}); err != nil {
		t.Fatalf("LoadTLSConfig failed: %v", err)
	}
	info, err := os.Stat(kf)
	if err != nil {
		t.Fatalf("stat key: %v", err)
	}
	if info.Mode().Perm() != tlsKeyFilePerm {
		t.Fatalf("key permissions = %v, want %v", info.Mode().Perm(), tlsKeyFilePerm)
	}
}

func TestLoadTLSConfigAppliesProductionDefaults(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	tc, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf})
	if err != nil {
		t.Fatal(err)
	}
	if tc.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected TLS 1.2 minimum, got %x", tc.MinVersion)
	}
	if tc.MaxVersion != tls.VersionTLS13 {
		t.Fatalf("expected TLS 1.3 maximum, got %x", tc.MaxVersion)
	}
	if len(tc.CipherSuites) == 0 {
		t.Fatal("expected default cipher suites")
	}
}

func TestLoadTLSConfigCopiesCipherSuites(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	cipherSuites := []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}

	tc, err := LoadTLSConfig(&TLSConfig{
		Enabled:      true,
		CertFile:     cf,
		KeyFile:      kf,
		CipherSuites: cipherSuites,
	})
	if err != nil {
		t.Fatal(err)
	}

	cipherSuites[0] = tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384

	if got, want := tc.CipherSuites[0], uint16(tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256); got != want {
		t.Fatalf("TLS config cipher suites was mutated through caller slice: got %x, want %x", got, want)
	}
}

func TestLoadTLSConfigRejectsWeakCipherSuites(t *testing.T) {
	tests := []struct {
		name  string
		suite uint16
		want  string
	}{
		{name: "cbc", suite: tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, want: "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"},
		{name: "static-rsa", suite: tls.TLS_RSA_WITH_AES_128_GCM_SHA256, want: "TLS_RSA_WITH_AES_128_GCM_SHA256"},
		{name: "unknown", suite: 0xffff, want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadTLSConfig(&TLSConfig{
				Enabled:      true,
				CipherSuites: []uint16{tt.suite},
			})
			if !errors.Is(err, ErrInsecureTLS) {
				t.Fatalf("expected ErrInsecureTLS, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error to mention %q, got %v", tt.want, err)
			}
		})
	}
}

func TestLoadTLSConfigRaisesWeakMinimumVersion(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	tc, err := LoadTLSConfig(&TLSConfig{
		Enabled:    true,
		CertFile:   cf,
		KeyFile:    kf,
		MinVersion: tls.VersionTLS10,
		MaxVersion: tls.VersionTLS10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tc.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected weak minimum version to be raised, got %x", tc.MinVersion)
	}
	if tc.MaxVersion != tls.VersionTLS12 {
		t.Fatalf("expected max version to follow raised minimum, got %x", tc.MaxVersion)
	}
}

func TestSendMessageRejectsShortWrite(t *testing.T) {
	client := &ClientConn{
		Conn:   &shortServerWriteConn{limit: 4},
		Server: &Server{writeTimeout: time.Second},
	}

	err := client.sendMessage(wire.MsgPong)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("sendMessage short write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestWriteServerFullRejectsShortWrite(t *testing.T) {
	writer := &shortServerWriteConn{limit: 3}

	n, err := writeServerFull(writer, []byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeServerFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 3 {
		t.Fatalf("writeServerFull wrote %d bytes, want 3", n)
	}
}

func TestLoadTLSConfigWithCACov(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	tc, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf, CAFile: cf})
	if err != nil {
		t.Fatal(err)
	}
	if tc.ClientCAs == nil {
		t.Error("no CAs")
	}
}

func TestLoadTLSConfigBadCACov(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	_, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf, CAFile: "/no.pem"})
	if err == nil {
		t.Error("expected error")
	}
}

func TestLoadTLSConfigInvalidCACov(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	bad := filepath.Join(t.TempDir(), "bad.pem")
	os.WriteFile(bad, []byte("junk"), 0644)
	_, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf, CAFile: bad})
	if err == nil {
		t.Error("expected error")
	}
}

func TestVerifyCertExpiredCov(t *testing.T) {
	td := t.TempDir()
	cf, kf := filepath.Join(td, "e.crt"), filepath.Join(td, "e.key")
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ser, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tm := x509.Certificate{
		SerialNumber: ser,
		Subject:      pkix.Name{Organization: []string{"T"}},
		NotBefore:    time.Now().Add(-48 * time.Hour),
		NotAfter:     time.Now().Add(-24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, _ := x509.CreateCertificate(rand.Reader, &tm, &tm, &priv.PublicKey, priv)
	co, _ := os.Create(cf)
	pem.Encode(co, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	co.Close()
	kb, _ := x509.MarshalECPrivateKey(priv)
	ko, _ := os.OpenFile(kf, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	pem.Encode(ko, &pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
	ko.Close()
	_, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf})
	if !errors.Is(err, ErrCertExpired) {
		t.Errorf("got %v", err)
	}
}

func TestVerifyCertNotYetValidCov(t *testing.T) {
	td := t.TempDir()
	cf, kf := filepath.Join(td, "f.crt"), filepath.Join(td, "f.key")
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ser, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tm := x509.Certificate{
		SerialNumber: ser,
		Subject:      pkix.Name{Organization: []string{"T"}},
		NotBefore:    time.Now().Add(24 * time.Hour),
		NotAfter:     time.Now().Add(48 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, _ := x509.CreateCertificate(rand.Reader, &tm, &tm, &priv.PublicKey, priv)
	co, _ := os.Create(cf)
	pem.Encode(co, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	co.Close()
	kb, _ := x509.MarshalECPrivateKey(priv)
	ko, _ := os.OpenFile(kf, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	pem.Encode(ko, &pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
	ko.Close()
	_, err := LoadTLSConfig(&TLSConfig{Enabled: true, CertFile: cf, KeyFile: kf})
	if !errors.Is(err, ErrCertNotYetValid) {
		t.Errorf("got %v", err)
	}
}

func TestIsTLSEnabledCov(t *testing.T) {
	if IsTLSEnabled(nil) {
		t.Error("nil")
	}
	if IsTLSEnabled(&TLSConfig{Enabled: false}) {
		t.Error("false")
	}
	if !IsTLSEnabled(&TLSConfig{Enabled: true}) {
		t.Error("true")
	}
}

func TestGetTLSListenerCov(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	if GetTLSListener(ln, nil) != ln {
		t.Error("nil should return same")
	}
	if GetTLSListener(ln, &tls.Config{}) == ln {
		t.Error("should wrap")
	}
}

func TestGetCipherSuiteNameCov(t *testing.T) {
	tests := []struct {
		id   uint16
		want string
	}{
		{tls.TLS_RSA_WITH_AES_128_CBC_SHA, "TLS_RSA_WITH_AES_128_CBC_SHA"},
		{tls.TLS_RSA_WITH_AES_256_CBC_SHA, "TLS_RSA_WITH_AES_256_CBC_SHA"},
		{tls.TLS_RSA_WITH_AES_128_GCM_SHA256, "TLS_RSA_WITH_AES_128_GCM_SHA256"},
		{tls.TLS_RSA_WITH_AES_256_GCM_SHA384, "TLS_RSA_WITH_AES_256_GCM_SHA384"},
		{tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"},
		{tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"},
		{tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"},
		{tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"},
		{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"},
		{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"},
		{tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"},
		{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"},
		{tls.TLS_AES_128_GCM_SHA256, "TLS_AES_128_GCM_SHA256"},
		{tls.TLS_AES_256_GCM_SHA384, "TLS_AES_256_GCM_SHA384"},
		{tls.TLS_CHACHA20_POLY1305_SHA256, "TLS_CHACHA20_POLY1305_SHA256"},
		{0xFFFF, "Unknown(65535)"},
	}
	for _, tc := range tests {
		if g := GetCipherSuiteName(tc.id); g != tc.want {
			t.Errorf("%d: %q!=%q", tc.id, g, tc.want)
		}
	}
}

func TestGetTLSVersionNameCov(t *testing.T) {
	tests := []struct {
		v    uint16
		want string
	}{
		{0x0300, "SSLv3"},
		{tls.VersionTLS10, "TLS 1.0"},
		{tls.VersionTLS11, "TLS 1.1"},
		{tls.VersionTLS12, "TLS 1.2"},
		{tls.VersionTLS13, "TLS 1.3"},
		{0xFFFF, "Unknown(65535)"},
	}
	for _, tc := range tests {
		if g := GetTLSVersionName(tc.v); g != tc.want {
			t.Errorf("%d: %q!=%q", tc.v, g, tc.want)
		}
	}
}

func TestGenSelfSignedCertCov(t *testing.T) {
	old, _ := os.Getwd()
	os.Chdir(t.TempDir())
	defer os.Chdir(old)
	cfg := &TLSConfig{Enabled: true, GenerateSelfSigned: true, SelfSignedOrg: "T", SelfSignedValidDays: 1}
	tc, err := LoadTLSConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if tc == nil {
		t.Fatal("nil")
	}
	// Reload should reuse existing certs
	cfg2 := &TLSConfig{Enabled: true, GenerateSelfSigned: true, SelfSignedOrg: "T", SelfSignedValidDays: 1}
	tc2, err := LoadTLSConfig(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	if tc2 == nil {
		t.Fatal("nil2")
	}
}

func TestGenClientCertBadCov(t *testing.T) {
	_, _, err := GenerateClientCert("/no.crt", "/no.key", "c", 1)
	if err == nil {
		t.Error("expected error")
	}
}

func TestGenClientCertBadPEMCov(t *testing.T) {
	td := t.TempDir()
	cf, kf := filepath.Join(td, "c.crt"), filepath.Join(td, "c.key")
	os.WriteFile(cf, []byte("junk"), 0644)
	os.WriteFile(kf, []byte("junk"), 0644)
	_, _, err := GenerateClientCert(cf, kf, "c", 1)
	if err == nil {
		t.Error("expected error")
	}
}

func TestGenClientCertValidCov(t *testing.T) {
	cf, kf := generateTestCertHelper(t)
	cp, kp, err := GenerateClientCert(cf, kf, "tc", 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(cp) == 0 || len(kp) == 0 {
		t.Error("empty")
	}
}

// ============ PRODUCTION HTTP HANDLER TESTS ============

func newTPServer(t *testing.T) *ProductionServer {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	cfg := &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout:      100 * time.Millisecond,
			DrainTimeout:         50 * time.Millisecond,
			HealthCheckInterval:  500 * time.Millisecond,
			StartupTimeout:       1 * time.Second,
			EnableSignalHandling: false,
		},
		EnableCircuitBreaker: true,
		CircuitBreaker:       engine.DefaultCircuitBreakerConfig(),
		EnableHealthServer:   false,
	}
	ps := NewProductionServer(db, cfg)
	if err := ps.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ps.Stop() })
	return ps
}

func TestHealthzHandlerCov(t *testing.T) {
	ps := newTPServer(t)
	h := ps.healthzHandler()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != 200 {
		t.Errorf("got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "running") {
		t.Error("no running")
	}
	w2 := httptest.NewRecorder()
	h(w2, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if w2.Code != 405 {
		t.Errorf("got %d", w2.Code)
	}
}

func TestStatsHandlerCov(t *testing.T) {
	ps := newTPServer(t)
	h := ps.statsHandler()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if w.Code != 200 {
		t.Errorf("got %d", w.Code)
	}
	w2 := httptest.NewRecorder()
	h(w2, httptest.NewRequest(http.MethodPost, "/stats", nil))
	if w2.Code != 405 {
		t.Errorf("got %d", w2.Code)
	}
}

func TestCBHandlerCov(t *testing.T) {
	ps := newTPServer(t)
	h := ps.circuitBreakerHandler()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/cb", nil))
	if w.Code != 200 {
		t.Errorf("got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "enabled") {
		t.Error("no enabled")
	}
	w2 := httptest.NewRecorder()
	h(w2, httptest.NewRequest(http.MethodPost, "/cb", nil))
	if w2.Code != 405 {
		t.Errorf("got %d", w2.Code)
	}
}

func TestCBHandlerDisabledCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	cfg := &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout: 100 * time.Millisecond, DrainTimeout: 50 * time.Millisecond,
			HealthCheckInterval: 500 * time.Millisecond, StartupTimeout: time.Second,
			EnableSignalHandling: false,
		},
		EnableCircuitBreaker: false, EnableHealthServer: false,
	}
	ps := NewProductionServer(db, cfg)
	ps.Start()
	defer ps.Stop()
	h := ps.circuitBreakerHandler()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/cb", nil))
	if w.Code != 503 {
		t.Errorf("got %d", w.Code)
	}
}

func TestRateLimitsHandlerCov(t *testing.T) {
	ps := newTPServer(t)
	h := ps.rateLimitsHandler()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/rl", nil))
	if w.Code != 503 {
		t.Errorf("got %d", w.Code)
	}
	w2 := httptest.NewRecorder()
	h(w2, httptest.NewRequest(http.MethodPost, "/rl", nil))
	if w2.Code != 405 {
		t.Errorf("got %d", w2.Code)
	}
}

func TestLoopbackOnlyCov(t *testing.T) {
	ps := newTPServer(t)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := ps.loopbackOnly(inner)

	r1 := httptest.NewRequest(http.MethodGet, "/t", nil)
	r1.RemoteAddr = "127.0.0.1:1"
	w1 := httptest.NewRecorder()
	h(w1, r1)
	if w1.Code != 200 {
		t.Errorf("got %d", w1.Code)
	}

	r2 := httptest.NewRequest(http.MethodGet, "/t", nil)
	r2.RemoteAddr = "[::1]:1"
	w2 := httptest.NewRecorder()
	h(w2, r2)
	if w2.Code != 200 {
		t.Errorf("got %d", w2.Code)
	}

	r3 := httptest.NewRequest(http.MethodGet, "/t", nil)
	r3.RemoteAddr = "10.0.0.1:1"
	w3 := httptest.NewRecorder()
	h(w3, r3)
	if w3.Code != 403 {
		t.Errorf("got %d", w3.Code)
	}

	r4 := httptest.NewRequest(http.MethodGet, "/t", nil)
	r4.RemoteAddr = "127.0.0.2:1"
	w4 := httptest.NewRecorder()
	h(w4, r4)
	if w4.Code != 200 {
		t.Errorf("got %d", w4.Code)
	}

	r5 := httptest.NewRequest(http.MethodGet, "/t", nil)
	r5.RemoteAddr = "not-a-host-port"
	w5 := httptest.NewRecorder()
	h(w5, r5)
	if w5.Code != 403 {
		t.Errorf("got %d", w5.Code)
	}
}

func TestAuthRequiredHandlerCov(t *testing.T) {
	ps := newTPServer(t)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := ps.authRequiredHandler(inner)

	r1 := httptest.NewRequest(http.MethodGet, "/t", nil)
	r1.RemoteAddr = "127.0.0.1:1"
	w1 := httptest.NewRecorder()
	h(w1, r1)
	if w1.Code != http.StatusServiceUnavailable {
		t.Errorf("got %d", w1.Code)
	}

	ps.SetAdminToken("secret-token")

	r2 := httptest.NewRequest(http.MethodGet, "/t", nil)
	r2.RemoteAddr = "[::1]:1"
	w2 := httptest.NewRecorder()
	h(w2, r2)
	if w2.Code != http.StatusUnauthorized {
		t.Errorf("got %d", w2.Code)
	}

	rValid := httptest.NewRequest(http.MethodGet, "/t", nil)
	rValid.RemoteAddr = "[::1]:1"
	rValid.Header.Set("Authorization", "Bearer secret-token")
	wValid := httptest.NewRecorder()
	h(wValid, rValid)
	if wValid.Code != http.StatusOK {
		t.Errorf("got %d", wValid.Code)
	}

	r3 := httptest.NewRequest(http.MethodGet, "/t", nil)
	r3.RemoteAddr = "10.0.0.1:1"
	r3.Header.Set("Authorization", "Bearer secret-token")
	w3 := httptest.NewRecorder()
	h(w3, r3)
	if w3.Code != http.StatusForbidden {
		t.Errorf("got %d", w3.Code)
	}
}

func TestHealthHandlerPostCov(t *testing.T) {
	ps := newTPServer(t)
	h := ps.healthHandler()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodPost, "/h", nil))
	if w.Code != 405 {
		t.Errorf("got %d", w.Code)
	}
}

func TestReadyHandlerAllCov(t *testing.T) {
	ps := newTPServer(t)
	h := ps.readyHandler()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/r", nil))
	if w.Code != 200 {
		t.Errorf("got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("ready Content-Type = %q, want application/json", got)
	}
	w2 := httptest.NewRecorder()
	h(w2, httptest.NewRequest(http.MethodPost, "/r", nil))
	if w2.Code != 405 {
		t.Errorf("got %d", w2.Code)
	}
}

func TestReadyHandlerNotRunCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	cfg := &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout: 100 * time.Millisecond, DrainTimeout: 50 * time.Millisecond,
			HealthCheckInterval: 500 * time.Millisecond, StartupTimeout: time.Second,
			EnableSignalHandling: false,
		},
		EnableHealthServer: false,
	}
	ps := NewProductionServer(db, cfg)
	h := ps.readyHandler()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/r", nil))
	if w.Code != 503 {
		t.Errorf("got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("ready Content-Type = %q, want application/json", got)
	}
}

// ============ SERVER.GO TESTS ============

func TestGetAuthenticatorCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	if s.GetAuthenticator() == nil {
		t.Error("nil")
	}
}

func TestSetSQLProtectorCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	sp := NewSQLProtector(DefaultSQLProtectionConfig())
	s.SetSQLProtector(sp)
	if s.sqlProtector != sp {
		t.Error("not set")
	}
}

func TestClientCountCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	if s.ClientCount() != 0 {
		t.Error("not 0")
	}
}

func TestSanitizeErrorCov(t *testing.T) {
	tests := []struct{ i, w string }{
		{"at /x", "at (internal error)"},
		{"at C:\\x", "at (internal error)"},
		{"at D:\\x", "at (internal error)"},
		{"ok", "ok"},
	}
	for _, tc := range tests {
		if g := sanitizeError(fmt.Errorf("%s", tc.i)); g != tc.w {
			t.Errorf("%q->%q want %q", tc.i, g, tc.w)
		}
	}
}

func TestNewServerAuthCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, err := New(ps, &Config{AuthEnabled: true, DefaultAdminUser: "admin", DefaultAdminPass: "Str0ng!Pass#2024"})
	if err != nil {
		t.Fatal(err)
	}
	if !s.auth.IsEnabled() {
		t.Error("not enabled")
	}
}

func TestNewServerTimeoutCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, &Config{ReadTimeout: 10, WriteTimeout: 5})
	if s.readTimeout != 10*time.Second {
		t.Error("rd")
	}
	if s.writeTimeout != 5*time.Second {
		t.Error("wr")
	}
}

func TestHandleMsgPrepareCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{ID: 1, Conn: c1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	p, _ := wire.Encode(&wire.PrepareMessage{SQL: "SELECT 1"})
	if _, ok := cl.handleMessage(wire.MsgPrepare, p).(*wire.OKMessage); !ok {
		t.Error("not OK")
	}
	cl.authed = false
	if em, ok := cl.handleMessage(wire.MsgPrepare, p).(*wire.ErrorMessage); !ok || em.Code != 6 {
		t.Error("auth")
	}
	cl.authed = true
	if em, ok := cl.handleMessage(wire.MsgPrepare, []byte{0xFF}).(*wire.ErrorMessage); !ok || em.Code != 2 {
		t.Error("decode")
	}
}

func TestWireQueryRejectsOversizedSQL(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	cl := &ClientConn{ID: 1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	payload, err := wire.Encode(&wire.QueryMessage{SQL: strings.Repeat("x", maxWireSQLBytes+1)})
	if err != nil {
		t.Fatalf("encode query: %v", err)
	}

	res := cl.handleMessage(wire.MsgQuery, payload)
	em, ok := res.(*wire.ErrorMessage)
	if !ok {
		t.Fatalf("expected ErrorMessage, got %T", res)
	}
	if em.Code != 9 || !strings.Contains(em.Message, "too large") {
		t.Fatalf("unexpected error: code=%d message=%q", em.Code, em.Message)
	}
}

func TestWirePrepareRejectsOversizedSQL(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{ID: 1, Conn: c1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	payload, err := wire.Encode(&wire.PrepareMessage{SQL: strings.Repeat("x", maxWireSQLBytes+1)})
	if err != nil {
		t.Fatalf("encode prepare: %v", err)
	}

	res := cl.handleMessage(wire.MsgPrepare, payload)
	em, ok := res.(*wire.ErrorMessage)
	if !ok {
		t.Fatalf("expected ErrorMessage, got %T", res)
	}
	if em.Code != 9 || !strings.Contains(em.Message, "too large") {
		t.Fatalf("unexpected error: code=%d message=%q", em.Code, em.Message)
	}
}

func TestWirePrepareRejectsTooManyPreparedStatements(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{
		ID:            1,
		Conn:          c1,
		Server:        s,
		authed:        true,
		preparedStmts: make(map[uint32]*preparedStmt, maxWirePreparedStmts),
	}
	for i := 0; i < maxWirePreparedStmts; i++ {
		cl.preparedStmts[uint32(i+1)] = &preparedStmt{sql: "SELECT 1"}
	}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	payload, err := wire.Encode(&wire.PrepareMessage{SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("encode prepare: %v", err)
	}

	res := cl.handleMessage(wire.MsgPrepare, payload)
	em, ok := res.(*wire.ErrorMessage)
	if !ok {
		t.Fatalf("expected ErrorMessage, got %T", res)
	}
	if em.Code != 9 || !strings.Contains(em.Message, "too many prepared statements") {
		t.Fatalf("unexpected error: code=%d message=%q", em.Code, em.Message)
	}
}

func TestWirePrepareRejectsInvalidSQLWithoutRegistering(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	cl := &ClientConn{ID: 1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	payload, err := wire.Encode(&wire.PrepareMessage{SQL: "INVALID SQL !!!"})
	if err != nil {
		t.Fatalf("encode prepare: %v", err)
	}

	res := cl.handleMessage(wire.MsgPrepare, payload)
	em, ok := res.(*wire.ErrorMessage)
	if !ok {
		t.Fatalf("expected ErrorMessage, got %T", res)
	}
	if em.Code != 4 {
		t.Fatalf("error code = %d, want 4", em.Code)
	}
	if len(cl.preparedStmts) != 0 {
		t.Fatalf("invalid SQL registered prepared statements: %d", len(cl.preparedStmts))
	}
}

func TestWirePrepareDoesNotExecuteDDL(t *testing.T) {
	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	cl := &ClientConn{ID: 1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(ctx)
	defer cl.cancel()

	payload, err := wire.Encode(&wire.PrepareMessage{SQL: "CREATE TABLE prepare_side_effect (id INTEGER)"})
	if err != nil {
		t.Fatalf("encode prepare: %v", err)
	}

	res := cl.handleMessage(wire.MsgPrepare, payload)
	if ok, isOK := res.(*wire.OKMessage); !isOK || ok.StmtID == 0 {
		t.Fatalf("expected OK with StmtID, got %T %v", res, res)
	}
	if _, err := db.Query(ctx, "SELECT * FROM prepare_side_effect"); err == nil {
		t.Fatal("prepared DDL was executed during prepare")
	}
}

func TestWireQueryRejectsOversizedResultSet(t *testing.T) {
	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE result_limit_test (id INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i <= maxWireResultRows; i++ {
		if _, err := db.Exec(ctx, "INSERT INTO result_limit_test (id) VALUES (?)", i); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	ps := NewProductionServer(db, DefaultProductionConfig())

	s, _ := New(ps, DefaultConfig())
	cl := &ClientConn{ID: 1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(ctx)
	defer cl.cancel()

	payload, err := wire.Encode(&wire.QueryMessage{SQL: "SELECT id FROM result_limit_test"})
	if err != nil {
		t.Fatalf("encode query: %v", err)
	}

	res := cl.handleMessage(wire.MsgQuery, payload)
	em, ok := res.(*wire.ErrorMessage)
	if !ok {
		t.Fatalf("expected ErrorMessage, got %T", res)
	}
	if em.Code != 9 || !strings.Contains(em.Message, "result set too large") {
		t.Fatalf("unexpected error: code=%d message=%q", em.Code, em.Message)
	}
}

func TestWireQueryRejectsOversizedResultValue(t *testing.T) {
	ctx := context.Background()
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	if _, err := db.Exec(ctx, "CREATE TABLE result_value_limit_test (payload TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO result_value_limit_test (payload) VALUES (?)", strings.Repeat("x", maxWireResultValueBytes+1)); err != nil {
		t.Fatalf("insert oversized value: %v", err)
	}

	ps := NewProductionServer(db, DefaultProductionConfig())

	s, _ := New(ps, DefaultConfig())
	cl := &ClientConn{ID: 1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(ctx)
	defer cl.cancel()

	payload, err := wire.Encode(&wire.QueryMessage{SQL: "SELECT payload FROM result_value_limit_test"})
	if err != nil {
		t.Fatalf("encode query: %v", err)
	}

	res := cl.handleMessage(wire.MsgQuery, payload)
	em, ok := res.(*wire.ErrorMessage)
	if !ok {
		t.Fatalf("expected ErrorMessage, got %T", res)
	}
	if em.Code != 9 || !strings.Contains(em.Message, "result value too large") {
		t.Fatalf("unexpected error: code=%d message=%q", em.Code, em.Message)
	}
}

func TestWireQueryRejectsInvalidParams(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	cl := &ClientConn{ID: 1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	tests := []struct {
		name    string
		params  []interface{}
		message string
	}{
		{
			name:    "too many",
			params:  make([]interface{}, maxWireParams+1),
			message: "too many parameters",
		},
		{
			name:    "large string",
			params:  []interface{}{strings.Repeat("x", maxWireParamBytes+1)},
			message: "parameter too large",
		},
		{
			name:    "nested",
			params:  []interface{}{[]interface{}{1}},
			message: "unsupported parameter type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := wire.Encode(&wire.QueryMessage{SQL: "SELECT 1", Params: tt.params})
			if err != nil {
				t.Fatalf("encode query: %v", err)
			}

			res := cl.handleMessage(wire.MsgQuery, payload)
			em, ok := res.(*wire.ErrorMessage)
			if !ok {
				t.Fatalf("expected ErrorMessage, got %T", res)
			}
			if em.Code != 9 || !strings.Contains(em.Message, tt.message) {
				t.Fatalf("unexpected error: code=%d message=%q", em.Code, em.Message)
			}
		})
	}
}

func TestWireExecuteRejectsInvalidParams(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{ID: 1, Conn: c1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	prep, err := wire.Encode(&wire.PrepareMessage{SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("encode prepare: %v", err)
	}
	res := cl.handleMessage(wire.MsgPrepare, prep)
	okMsg, ok := res.(*wire.OKMessage)
	if !ok {
		t.Fatalf("expected OKMessage, got %T", res)
	}

	exec, err := wire.Encode(&wire.ExecuteMessage{
		StmtID: okMsg.StmtID,
		Params: []interface{}{
			strings.Repeat("x", maxWireParamBytes+1),
		},
	})
	if err != nil {
		t.Fatalf("encode execute: %v", err)
	}

	res = cl.handleMessage(wire.MsgExecute, exec)
	em, ok := res.(*wire.ErrorMessage)
	if !ok {
		t.Fatalf("expected ErrorMessage, got %T", res)
	}
	if em.Code != 9 || !strings.Contains(em.Message, "parameter too large") {
		t.Fatalf("unexpected error: code=%d message=%q", em.Code, em.Message)
	}
}

func TestHandleMsgExecuteCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{ID: 1, Conn: c1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	p, _ := wire.Encode(&wire.ExecuteMessage{StmtID: 1})
	if em, ok := cl.handleMessage(wire.MsgExecute, p).(*wire.ErrorMessage); !ok || em.Code != 4 {
		t.Error("exec")
	}
	cl.authed = false
	if em, ok := cl.handleMessage(wire.MsgExecute, p).(*wire.ErrorMessage); !ok || em.Code != 6 {
		t.Error("auth")
	}
	cl.authed = true
	if em, ok := cl.handleMessage(wire.MsgExecute, []byte{0xFF}).(*wire.ErrorMessage); !ok || em.Code != 2 {
		t.Error("decode")
	}
}

func TestPreparedStatementHappyPath(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{ID: 1, Conn: c1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	// Prepare a statement
	prep, _ := wire.Encode(&wire.PrepareMessage{SQL: "SELECT 1 AS one"})
	res := cl.handleMessage(wire.MsgPrepare, prep)
	ok, isOK := res.(*wire.OKMessage)
	if !isOK || ok.StmtID == 0 {
		t.Fatalf("expected OK with StmtID, got %T %v", res, res)
	}
	stmtID := ok.StmtID

	// Execute the prepared statement
	exec, _ := wire.Encode(&wire.ExecuteMessage{StmtID: stmtID})
	res2 := cl.handleMessage(wire.MsgExecute, exec)
	rm, isResult := res2.(*wire.ResultMessage)
	if !isResult {
		t.Fatalf("expected ResultMessage, got %T %v", res2, res2)
	}
	if len(rm.Columns) != 1 || rm.Columns[0] != "one" {
		t.Errorf("expected column 'one', got %v", rm.Columns)
	}
	if len(rm.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rm.Rows))
	}

	// Execute with non-existent stmt ID should fail
	execBad, _ := wire.Encode(&wire.ExecuteMessage{StmtID: 999})
	res3 := cl.handleMessage(wire.MsgExecute, execBad)
	if em, ok := res3.(*wire.ErrorMessage); !ok || em.Code != 4 {
		t.Errorf("expected error code 4 for missing stmt, got %T %v", res3, res3)
	}
}

func TestHandleQuerySQLProtCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	sp := NewSQLProtector(&SQLProtectionConfig{
		Enabled: true, BlockOnDetection: true, MaxQueryLength: 10000,
		MaxORConditions: 10, MaxUNIONCount: 5, SuspiciousThreshold: 1,
	})
	s.SetSQLProtector(sp)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{ID: 1, Conn: c1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()

	q := &wire.QueryMessage{SQL: "' OR '1'='1; SELECT * FROM u"}
	if em, ok := cl.handleQuery(cl.ctx, q).(*wire.ErrorMessage); !ok || em.Code != 9 {
		t.Error("sql prot")
	}
}

func TestHandleQueryPermCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, &Config{AuthEnabled: true, DefaultAdminUser: "admin", DefaultAdminPass: "Str0ng!Pass#2024"})
	s.auth.CreateUser("r", "Str0ng!Pass#2024", false)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{ID: 1, Conn: c1, Server: s, authed: true, username: "r"}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()
	if em, ok := cl.handleQuery(cl.ctx, &wire.QueryMessage{SQL: "SELECT 1"}).(*wire.ErrorMessage); !ok || em.Code != 8 {
		t.Error("perm")
	}
}

func TestHandleQueryPrefixesCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	ps := NewProductionServer(db, DefaultProductionConfig())
	s, _ := New(ps, DefaultConfig())
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	cl := &ClientConn{ID: 1, Conn: c1, Server: s, authed: true}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	defer cl.cancel()
	_ = cl.handleQuery(cl.ctx, &wire.QueryMessage{SQL: "WITH c AS (SELECT 1) SELECT * FROM c"})
	_ = cl.handleQuery(cl.ctx, &wire.QueryMessage{SQL: "SHOW TABLES"})
	_ = cl.handleQuery(cl.ctx, &wire.QueryMessage{SQL: "EXPLAIN SELECT 1"})
	_ = cl.handleQuery(cl.ctx, &wire.QueryMessage{SQL: "DESCRIBE t"})
}

// ============ LIFECYCLE STATE STRING TEST ============

func TestLifecycleStateAllCov(t *testing.T) {
	tests := []struct {
		s LifecycleState
		w string
	}{
		{StateInitializing, "initializing"},
		{StateStarting, "starting"},
		{StateRunning, "running"},
		{StateDraining, "draining"},
		{StateShuttingDown, "shutting_down"},
		{StateStopped, "stopped"},
		{99, "unknown"},
	}
	for _, tc := range tests {
		if g := tc.s.String(); g != tc.w {
			t.Errorf("%d: %q!=%q", tc.s, g, tc.w)
		}
	}
}

// ============ RATE LIMITER AllowN PER-CLIENT TEST ============

func TestRLAllowNClientCov(t *testing.T) {
	rl := NewRateLimiter(&RateLimiterConfig{
		RPS: 1000, Burst: 100, PerClient: true,
		CleanupInterval: 5 * time.Minute, MaxClients: 100,
	})
	defer rl.Stop()
	if !rl.AllowN("c1", 1) {
		t.Error("1")
	}
	if rl.AllowN("c2", 999999) {
		t.Error("big")
	}
}

// ============ ADMIN SERVER ADDITIONAL TESTS ============

func TestProdServerRLSQLCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	cfg := &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout: 100 * time.Millisecond, DrainTimeout: 50 * time.Millisecond,
			HealthCheckInterval: 500 * time.Millisecond, StartupTimeout: time.Second,
			EnableSignalHandling: false,
		},
		EnableRateLimiter: true, EnableSQLProtection: true, EnableHealthServer: false,
	}
	ps := NewProductionServer(db, cfg)
	if ps.RateLimiter == nil {
		t.Error("rl")
	}
	if ps.SQLProtector == nil {
		t.Error("sp")
	}
	ps.RateLimiter.Stop()
}

func TestProdServerNilCfgCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	if ps := NewProductionServer(db, nil); ps.Config == nil {
		t.Error("nil")
	}
}

func TestProdServerStartTwiceCov(t *testing.T) {
	ps := newTPServer(t)
	if err := ps.Start(); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Error(err)
	}
}

func TestProdServerStopNotRunCov(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	defer db.Close()
	cfg := &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout: 100 * time.Millisecond, DrainTimeout: 50 * time.Millisecond,
			HealthCheckInterval: 500 * time.Millisecond, StartupTimeout: time.Second,
			EnableSignalHandling: false,
		},
		EnableHealthServer: false,
	}
	if err := NewProductionServer(db, cfg).Stop(); err != nil {
		t.Error(err)
	}
}

func TestSuspiciousCommentsCov(t *testing.T) {
	if !hasSuspiciousComments("SELECT /* x") {
		t.Error("unmatched")
	}
	if !hasSuspiciousComments("SELECT /*!50000 1 */") {
		t.Error("mysql")
	}
	if hasSuspiciousComments("SELECT 1") {
		t.Error("clean")
	}
	if hasSuspiciousComments("SELECT /* ok */ 1") {
		t.Error("balanced")
	}
}
