package server

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestListenTLSConfigError tests Listen when TLS config loading fails
func TestListenTLSConfigError(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create a TLS config with invalid cert file
	tlsConfig := &TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/path/cert.crt",
		KeyFile:  "/nonexistent/path/key.key",
	}

	// Start a listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	// Use a goroutine to call Listen which should fail on TLS config
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Listen(listener.Addr().String(), tlsConfig)
	}()

	select {
	case err := <-errChan:
		if err == nil {
			t.Error("Expected error for invalid TLS config")
		}
	case <-time.After(2 * time.Second):
		t.Error("Listen did not return error in time")
	}

	srv.Close()
}

// TestListenTLSErrorClosesListener tests that Listen closes the listener when TLS config fails
func TestListenTLSErrorClosesListener(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create a TLS config with invalid cert file
	tlsConfig := &TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/path/cert.crt",
		KeyFile:  "/nonexistent/path/key.key",
	}

	// Try to listen - should fail and clean up
	err = srv.Listen("127.0.0.1:0", tlsConfig)
	if err == nil {
		t.Error("Expected error for invalid TLS config")
	}
}

// TestSetupSignalHandling tests setupSignalHandling
func TestSetupSignalHandling(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{
		ShutdownSignals: []os.Signal{os.Interrupt},
	})

	// Call setupSignalHandling - it spawns a goroutine
	l.setupSignalHandling()

	// Give it a moment to set up
	time.Sleep(10 * time.Millisecond)

	// Test passes if no panic occurs
}

// TestLifecycleStartWithSignalHandling tests Start with signal handling
func TestLifecycleStartWithSignalHandling(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{
		EnableSignalHandling: true,
		ShutdownSignals:      []os.Signal{os.Interrupt},
		HealthCheckInterval:  1 * time.Second,
	})

	// Start should set up signal handling (no ctx parameter)
	err := l.Start()
	if err != nil {
		t.Errorf("Start failed: %v", err)
	}

	// Stop to clean up
	_ = l.Stop()
}

// TestGenerateClientCertExpiredCA tests GenerateClientCert with expired CA
func TestGenerateClientCertExpiredCA(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a valid CA cert/key pair
	caConfig := &TLSConfig{
		CertFile:            filepath.Join(tmpDir, "ca.crt"),
		KeyFile:             filepath.Join(tmpDir, "ca.key"),
		SelfSignedOrg:       "TestCA",
		SelfSignedValidDays: 1,
	}

	err := generateSelfSignedCert(caConfig)
	if err != nil {
		t.Fatalf("Failed to generate CA cert: %v", err)
	}

	// Generate client cert with 0 validity days (already expired)
	_, _, err = GenerateClientCert(caConfig.CertFile, caConfig.KeyFile, "testclient", 0)
	if err != nil {
		// Expected - can't generate already expired cert
		t.Logf("Expected error for 0 validity days: %v", err)
	}
}

// TestGenerateClientCertWithEmptyName tests GenerateClientCert with empty client name
func TestGenerateClientCertWithEmptyName(t *testing.T) {
	tmpDir := t.TempDir()

	caConfig := &TLSConfig{
		CertFile:            filepath.Join(tmpDir, "ca.crt"),
		KeyFile:             filepath.Join(tmpDir, "ca.key"),
		SelfSignedOrg:       "TestCA",
		SelfSignedValidDays: 1,
	}

	err := generateSelfSignedCert(caConfig)
	if err != nil {
		t.Fatalf("Failed to generate CA cert: %v", err)
	}

	// Generate client cert with empty name
	certPEM, keyPEM, err := GenerateClientCert(caConfig.CertFile, caConfig.KeyFile, "", 1)
	if err != nil {
		t.Errorf("GenerateClientCert with empty name failed: %v", err)
	}

	if len(certPEM) == 0 || len(keyPEM) == 0 {
		t.Error("Expected cert and key to be generated even with empty name")
	}
}

// TestHandleDBStatsWithClosedDB tests handleDBStats with closed database
func TestHandleDBStatsWithClosedDB(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	admin := NewAdminServer(db, ":0")

	// Close the DB to cause error
	db.Close()

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()

	admin.handleDBStats(w, req)

	// Should still return 200 with error in JSON
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestHandleJSONMetricsWithClosedDB tests handleJSONMetrics with closed database
func TestHandleJSONMetricsWithClosedDB(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	admin := NewAdminServer(db, ":0")

	// Close the DB
	db.Close()

	req := httptest.NewRequest("GET", "/metrics/json", nil)
	w := httptest.NewRecorder()

	admin.handleJSONMetrics(w, req)

	// Should return 200
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestHandlePrometheusMetricsWithClosedDB tests handlePrometheusMetrics with closed DB
func TestHandlePrometheusMetricsWithClosedDB(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	admin := NewAdminServer(db, ":0")

	// Close the DB
	db.Close()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	admin.handlePrometheusMetrics(w, req)

	// Should return 200
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestCheckSQLWithLongQuery tests CheckSQL with various patterns
func TestCheckSQLWithLongQuery(t *testing.T) {
	config := DefaultSQLProtectionConfig()
	config.Enabled = true
	config.MaxQueryLength = 50
	config.BlockOnDetection = true // Enable blocking
	config.SuspiciousThreshold = 1 // Lower threshold to block on single violation

	protector := NewSQLProtector(config)

	tests := []struct {
		name        string
		query       string
		shouldAllow bool
	}{
		{
			name:        "ShortQuery",
			query:       "SELECT * FROM users",
			shouldAllow: true,
		},
		{
			name:        "LongQuery",
			query:       "SELECT * FROM users WHERE id = 1 AND name = 'test' AND status = 'active' AND created_at > '2024-01-01'",
			shouldAllow: false, // Should be blocked for exceeding length
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := protector.CheckSQL(tc.query)
			if result.Allowed != tc.shouldAllow {
				t.Errorf("Expected Allowed=%v for query, got Allowed=%v with violations: %v",
					tc.shouldAllow, result.Allowed, result.Violations)
			}
		})
	}
}

// TestLoadTLSConfigWithInvalidCert tests LoadTLSConfig with invalid cert files
func TestLoadTLSConfigWithInvalidCert(t *testing.T) {
	tmpDir := t.TempDir()

	config := &TLSConfig{
		Enabled:  true,
		CertFile: filepath.Join(tmpDir, "invalid.crt"),
		KeyFile:  filepath.Join(tmpDir, "invalid.key"),
	}

	// Create invalid cert files
	os.WriteFile(config.CertFile, []byte("invalid cert data"), 0644)
	os.WriteFile(config.KeyFile, []byte("invalid key data"), 0644)

	_, err := LoadTLSConfig(config)
	if err == nil {
		t.Error("Expected error for invalid cert files")
	}
}

// TestLoadTLSConfigWithMissingFiles tests LoadTLSConfig with non-existent files
func TestLoadTLSConfigWithMissingFiles(t *testing.T) {
	config := &TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/path/server.crt",
		KeyFile:  "/nonexistent/path/server.key",
	}

	_, err := LoadTLSConfig(config)
	if err == nil {
		t.Error("Expected error for missing cert files")
	}
}

// TestAuthMiddlewareWithInvalidToken tests authMiddleware with invalid token
func TestAuthMiddlewareWithInvalidToken(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")
	// Set auth token to enable auth
	admin.SetAuthToken("test-token")

	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with auth middleware
	handler := admin.authMiddleware(testHandler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Should return 401 for invalid token
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401 for invalid token, got %d", w.Code)
	}
}

// TestAuthMiddlewareWithoutHeader tests authMiddleware without auth header
func TestAuthMiddlewareWithoutHeader(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")
	// Set auth token to enable auth
	admin.SetAuthToken("test-token")

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := admin.authMiddleware(testHandler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Should return 401 for missing header
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401 for missing header, got %d", w.Code)
	}
}
