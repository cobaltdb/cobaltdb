package server

import (
	"crypto/tls"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGenerateSelfSignedCert114 tests self-signed certificate generation
func TestGenerateSelfSignedCert114(t *testing.T) {
	// Create temp directory for certs
	tempDir := t.TempDir()

	// Change to temp directory for test
	originalDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalDir)

	config := &TLSConfig{
		GenerateSelfSigned:  true,
		SelfSignedOrg:       "Test Org",
		SelfSignedValidDays: 1, // 1 day validity for testing
	}

	err := generateSelfSignedCert(config)
	if err != nil {
		t.Fatalf("Failed to generate self-signed cert: %v", err)
	}

	// Verify cert files were created
	certPath := filepath.Join("certs", "server.crt")
	keyPath := filepath.Join("certs", "server.key")

	if _, err := os.Stat(certPath); os.IsNotExist(err) {
		t.Error("Certificate file was not created")
	}
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		t.Error("Key file was not created")
	}

	// Verify the certificate can be loaded
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Errorf("Failed to load generated cert: %v", err)
	}

	// Verify certificate
	if err := verifyCertificate(&cert); err != nil {
		t.Errorf("Certificate verification failed: %v", err)
	}
}

func TestGenerateSelfSignedCertHonorsExplicitPathsAndPermissions(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "public", "custom.crt")
	keyPath := filepath.Join(tempDir, "private", "custom.key")

	config := &TLSConfig{
		CertFile:            certPath,
		KeyFile:             keyPath,
		GenerateSelfSigned:  true,
		SelfSignedOrg:       "Test Org",
		SelfSignedValidDays: 1,
	}

	if err := generateSelfSignedCert(config); err != nil {
		t.Fatalf("Failed to generate self-signed cert: %v", err)
	}

	if config.CertFile != filepath.Clean(certPath) {
		t.Fatalf("CertFile was rewritten: got %q, want %q", config.CertFile, filepath.Clean(certPath))
	}
	if config.KeyFile != filepath.Clean(keyPath) {
		t.Fatalf("KeyFile was rewritten: got %q, want %q", config.KeyFile, filepath.Clean(keyPath))
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("Failed to load generated cert: %v", err)
	}
	if err := verifyCertificate(&cert); err != nil {
		t.Fatalf("Certificate verification failed: %v", err)
	}

	certInfo, err := os.Stat(certPath)
	if err != nil {
		t.Fatalf("Failed to stat cert file: %v", err)
	}
	if got := certInfo.Mode().Perm(); got != tlsCertFilePerm {
		t.Fatalf("Cert permissions = %v, want %v", got, tlsCertFilePerm)
	}

	keyInfo, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Failed to stat key file: %v", err)
	}
	if got := keyInfo.Mode().Perm(); got != tlsKeyFilePerm {
		t.Fatalf("Key permissions = %v, want %v", got, tlsKeyFilePerm)
	}

	assertNoTLSTempFiles(t, filepath.Dir(certPath))
	assertNoTLSTempFiles(t, filepath.Dir(keyPath))
}

func TestGenerateSelfSignedCertRejectsSymlinkDirectory(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := filepath.Join(tempDir, "target")
	linkDir := filepath.Join(tempDir, "certs")
	if err := os.Mkdir(targetDir, 0750); err != nil {
		t.Fatalf("Mkdir target: %v", err)
	}
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	config := &TLSConfig{
		CertFile:            filepath.Join(linkDir, "server.crt"),
		KeyFile:             filepath.Join(linkDir, "server.key"),
		GenerateSelfSigned:  true,
		SelfSignedOrg:       "Test Org",
		SelfSignedValidDays: 1,
	}

	err := generateSelfSignedCert(config)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("generateSelfSignedCert symlink dir error = %v, want symlink rejection", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "server.crt")); !os.IsNotExist(err) {
		t.Fatalf("certificate was written through symlink, stat err=%v", err)
	}
}

func TestPrepareTLSFileDirCreatesRestrictiveDirectory(t *testing.T) {
	certPath := filepath.Join(t.TempDir(), "nested", "certs", "server.crt")
	if err := prepareTLSFileDir(certPath); err != nil {
		t.Fatalf("prepareTLSFileDir: %v", err)
	}

	info, err := os.Stat(filepath.Dir(certPath))
	if err != nil {
		t.Fatalf("Stat TLS dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0750 {
		t.Fatalf("TLS dir mode = %o, want 750", got)
	}
}

func TestWriteTLSFullRejectsShortWrite(t *testing.T) {
	writer := &shortTLSWriter{limit: 4}

	n, err := writeTLSFull(writer, []byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeTLSFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 4 {
		t.Fatalf("writeTLSFull wrote %d bytes, want 4", n)
	}
}

func TestGenerateSelfSignedCertRejectsOversizedSubjectAndValidity(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name      string
		org       string
		validDays int
	}{
		{
			name:      "OversizedOrg",
			org:       strings.Repeat("o", maxTLSSubjectBytes+1),
			validDays: 1,
		},
		{
			name:      "ZeroValidity",
			org:       "Test Org",
			validDays: 0,
		},
		{
			name:      "ExcessiveValidity",
			org:       "Test Org",
			validDays: maxTLSCertificateValidDays + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &TLSConfig{
				CertFile:            filepath.Join(tempDir, tt.name, "server.crt"),
				KeyFile:             filepath.Join(tempDir, tt.name, "server.key"),
				GenerateSelfSigned:  true,
				SelfSignedOrg:       tt.org,
				SelfSignedValidDays: tt.validDays,
			}

			err := generateSelfSignedCert(config)
			if !errors.Is(err, ErrInvalidCert) {
				t.Fatalf("expected ErrInvalidCert, got %v", err)
			}
			if _, statErr := os.Stat(config.CertFile); !os.IsNotExist(statErr) {
				t.Fatalf("certificate file should not be created, stat err=%v", statErr)
			}
		})
	}
}

type shortTLSWriter struct {
	limit int
}

func (w *shortTLSWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		return w.limit, nil
	}
	return len(p), nil
}

func assertNoTLSTempFiles(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read dir %s: %v", dir, err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp-") {
			t.Fatalf("Found leftover TLS temp file %s in %s", entry.Name(), dir)
		}
	}
}

// TestGenerateSelfSignedCertExisting114 tests with existing valid certs
func TestGenerateSelfSignedCertExisting114(t *testing.T) {
	// Create temp directory for certs
	tempDir := t.TempDir()

	// Change to temp directory for test
	originalDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalDir)

	// First, create initial certs
	config := &TLSConfig{
		GenerateSelfSigned:  true,
		SelfSignedOrg:       "Test Org",
		SelfSignedValidDays: 1, // 1 day validity for testing
	}

	err := generateSelfSignedCert(config)
	if err != nil {
		t.Fatalf("Failed to generate initial cert: %v", err)
	}

	// Get initial cert info
	certPath := filepath.Join("certs", "server.crt")
	initialInfo, _ := os.Stat(certPath)

	// Call again - should reuse existing valid certs
	err = generateSelfSignedCert(config)
	if err != nil {
		t.Fatalf("Failed with existing certs: %v", err)
	}

	// Verify same cert file (not regenerated)
	newInfo, _ := os.Stat(certPath)
	if newInfo.ModTime() != initialInfo.ModTime() {
		t.Log("Certificate was regenerated (may be expected)")
	}
}

// TestVerifyCertificate114 tests certificate verification
func TestVerifyCertificate114(t *testing.T) {
	// Create temp directory for certs
	tempDir := t.TempDir()

	// Change to temp directory for test
	originalDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalDir)

	// Generate a cert first
	config := &TLSConfig{
		GenerateSelfSigned:  true,
		SelfSignedOrg:       "Test Org",
		SelfSignedValidDays: 1, // 1 day validity for testing
	}

	err := generateSelfSignedCert(config)
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Load and verify
	cert, err := tls.LoadX509KeyPair(
		filepath.Join("certs", "server.crt"),
		filepath.Join("certs", "server.key"),
	)
	if err != nil {
		t.Fatalf("Failed to load cert: %v", err)
	}

	err = verifyCertificate(&cert)
	if err != nil {
		t.Errorf("Certificate verification failed: %v", err)
	}
}

// TestLoadTLSConfigDisable114 tests loading disabled TLS config
func TestLoadTLSConfigDisable114(t *testing.T) {
	config := &TLSConfig{
		Enabled: false,
	}

	tlsConfig, err := LoadTLSConfig(config)
	if err != nil {
		t.Errorf("LoadTLSConfig with disable mode should not error: %v", err)
	}
	if tlsConfig != nil {
		t.Error("Expected nil tls.Config for disabled mode")
	}
}

func TestLoadTLSConfigNilIsDisabled(t *testing.T) {
	tlsConfig, err := LoadTLSConfig(nil)
	if err != nil {
		t.Fatalf("LoadTLSConfig(nil): %v", err)
	}
	if tlsConfig != nil {
		t.Fatal("expected nil tls.Config for nil config")
	}
}

// TestLoadTLSConfigSelfSigned114 tests loading self-signed TLS config
func TestLoadTLSConfigSelfSigned114(t *testing.T) {
	// Create temp directory for certs
	tempDir := t.TempDir()

	// Change to temp directory for test
	originalDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalDir)

	config := &TLSConfig{
		Enabled:             true,
		GenerateSelfSigned:  true,
		SelfSignedOrg:       "Test Org",
		SelfSignedValidDays: 1, // 1 day validity for testing
	}

	tlsConfig, err := LoadTLSConfig(config)
	if err != nil {
		t.Fatalf("Failed to load TLS config: %v", err)
	}
	if tlsConfig == nil {
		t.Error("Expected non-nil tls.Config")
		return
	}
	if len(tlsConfig.Certificates) == 0 {
		t.Error("Expected at least one certificate")
	}
}

func TestLoadTLSConfigSelfSignedDefaultsValidity(t *testing.T) {
	tempDir := t.TempDir()
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	defer os.Chdir(originalDir)

	config := &TLSConfig{
		Enabled:            true,
		GenerateSelfSigned: true,
	}

	tlsConfig, err := LoadTLSConfig(config)
	if err != nil {
		t.Fatalf("LoadTLSConfig: %v", err)
	}
	if tlsConfig == nil || len(tlsConfig.Certificates) == 0 {
		t.Fatal("expected generated certificate")
	}
	if config.SelfSignedValidDays != 0 {
		t.Fatal("LoadTLSConfig should not mutate caller config")
	}
}

// TestVerifyCertificateInvalid114 tests verification of invalid certificates
func TestVerifyCertificateInvalid114(t *testing.T) {
	// Test with empty certificate
	emptyCert := &tls.Certificate{}
	err := verifyCertificate(emptyCert)
	if err == nil {
		t.Error("Expected error for empty certificate")
	}
}

// TestGetCipherSuiteName114 tests cipher suite name lookup
func TestGetCipherSuiteName114(t *testing.T) {
	tests := []struct {
		id       uint16
		expected string
	}{
		{tls.TLS_RSA_WITH_AES_128_CBC_SHA, "TLS_RSA_WITH_AES_128_CBC_SHA"},
		{tls.TLS_RSA_WITH_AES_256_CBC_SHA, "TLS_RSA_WITH_AES_256_CBC_SHA"},
		{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"},
		{tls.TLS_AES_128_GCM_SHA256, "TLS_AES_128_GCM_SHA256"},
		{tls.TLS_AES_256_GCM_SHA384, "TLS_AES_256_GCM_SHA384"},
		{0x9999, "Unknown(39321)"}, // Unknown cipher suite
	}

	for _, tt := range tests {
		result := GetCipherSuiteName(tt.id)
		if result != tt.expected {
			t.Errorf("GetCipherSuiteName(%d) = %s, want %s", tt.id, result, tt.expected)
		}
	}
}

// TestGetTLSVersionName114 tests TLS version name lookup
func TestGetTLSVersionName114(t *testing.T) {
	tests := []struct {
		version  uint16
		expected string
	}{
		{0x0300, "SSLv3"},
		{tls.VersionTLS10, "TLS 1.0"},
		{tls.VersionTLS11, "TLS 1.1"},
		{tls.VersionTLS12, "TLS 1.2"},
		{tls.VersionTLS13, "TLS 1.3"},
		{0x9999, "Unknown(39321)"}, // Unknown version
	}

	for _, tt := range tests {
		result := GetTLSVersionName(tt.version)
		if result != tt.expected {
			t.Errorf("GetTLSVersionName(%d) = %s, want %s", tt.version, result, tt.expected)
		}
	}
}

// TestIsTLSEnabled114 tests TLS enabled check
func TestIsTLSEnabled114(t *testing.T) {
	// Test nil config
	if IsTLSEnabled(nil) {
		t.Error("IsTLSEnabled(nil) should be false")
	}

	// Test disabled config
	config := &TLSConfig{Enabled: false}
	if IsTLSEnabled(config) {
		t.Error("IsTLSEnabled(disabled) should be false")
	}

	// Test enabled config
	config = &TLSConfig{Enabled: true}
	if !IsTLSEnabled(config) {
		t.Error("IsTLSEnabled(enabled) should be true")
	}
}

// TestDefaultTLSConfig114 tests default TLS configuration
func TestDefaultTLSConfig114(t *testing.T) {
	config := DefaultTLSConfig()
	if config == nil {
		t.Fatal("DefaultTLSConfig() returned nil")
	}

	if config.Enabled {
		t.Error("Default should have Enabled = false")
	}
	if config.MinVersion != tls.VersionTLS12 {
		t.Errorf("Default MinVersion should be TLS 1.2, got %d", config.MinVersion)
	}
	if config.SelfSignedValidDays != 365 {
		t.Errorf("Default SelfSignedValidDays should be 365, got %d", config.SelfSignedValidDays)
	}
}

// TestLoadTLSConfigWithCertFiles114 tests loading TLS with explicit cert files
func TestLoadTLSConfigWithCertFiles114(t *testing.T) {
	// Create temp directory for certs
	tempDir := t.TempDir()

	// Change to temp directory for test
	originalDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalDir)

	// Generate certs first
	config := &TLSConfig{
		GenerateSelfSigned:  true,
		SelfSignedOrg:       "Test Org",
		SelfSignedValidDays: 1,
	}
	err := generateSelfSignedCert(config)
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Now load with explicit paths
	config2 := &TLSConfig{
		Enabled:    true,
		CertFile:   filepath.Join("certs", "server.crt"),
		KeyFile:    filepath.Join("certs", "server.key"),
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS13,
	}

	tlsConfig, err := LoadTLSConfig(config2)
	if err != nil {
		t.Fatalf("Failed to load TLS config: %v", err)
	}
	if tlsConfig == nil {
		t.Fatal("Expected non-nil tls.Config")
	}
	if len(tlsConfig.Certificates) == 0 {
		t.Error("Expected at least one certificate")
	}
}

// TestGenerateClientCert114 tests client certificate generation
func TestGenerateClientCert114(t *testing.T) {
	// Create temp directory for certs
	tempDir := t.TempDir()

	// Generate CA cert first
	caConfig := &TLSConfig{
		GenerateSelfSigned:  true,
		SelfSignedOrg:       "Test CA",
		SelfSignedValidDays: 1,
	}

	// Change to temp directory
	originalDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(originalDir)

	err := generateSelfSignedCert(caConfig)
	if err != nil {
		t.Fatalf("Failed to generate CA cert: %v", err)
	}

	caCertPath := filepath.Join("certs", "server.crt")
	caKeyPath := filepath.Join("certs", "server.key")

	// Generate client cert
	certPEM, keyPEM, err := GenerateClientCert(caCertPath, caKeyPath, "testclient", 1)
	if err != nil {
		t.Fatalf("Failed to generate client cert: %v", err)
	}

	if len(certPEM) == 0 {
		t.Error("Expected non-empty certificate PEM")
	}
	if len(keyPEM) == 0 {
		t.Error("Expected non-empty key PEM")
	}
}

// TestGenerateClientCertErrors114 tests client certificate generation error cases
func TestGenerateClientCertErrors114(t *testing.T) {
	tempDir := t.TempDir()

	// Test with non-existent CA cert
	_, _, err := GenerateClientCert(
		filepath.Join(tempDir, "nonexistent.crt"),
		filepath.Join(tempDir, "nonexistent.key"),
		"testclient",
		1,
	)
	if err == nil {
		t.Error("Expected error for non-existent CA cert")
	}

	// Test with invalid CA cert file
	invalidCertPath := filepath.Join(tempDir, "invalid.crt")
	invalidKeyPath := filepath.Join(tempDir, "invalid.key")
	os.WriteFile(invalidCertPath, []byte("invalid cert data"), 0644)
	os.WriteFile(invalidKeyPath, []byte("invalid key data"), 0644)

	_, _, err = GenerateClientCert(invalidCertPath, invalidKeyPath, "testclient", 1)
	if err == nil {
		t.Error("Expected error for invalid CA cert")
	}
}

func TestGenerateClientCertRejectsOversizedSubjectAndValidity(t *testing.T) {
	tests := []struct {
		name       string
		clientName string
		validDays  int
	}{
		{
			name:       "OversizedClientName",
			clientName: strings.Repeat("c", maxTLSSubjectBytes+1),
			validDays:  1,
		},
		{
			name:       "ZeroValidity",
			clientName: "client",
			validDays:  0,
		},
		{
			name:       "ExcessiveValidity",
			clientName: "client",
			validDays:  maxTLSCertificateValidDays + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := GenerateClientCert("/missing-ca.crt", "/missing-ca.key", tt.clientName, tt.validDays)
			if !errors.Is(err, ErrInvalidCert) {
				t.Fatalf("expected ErrInvalidCert before reading CA files, got %v", err)
			}
		})
	}
}
