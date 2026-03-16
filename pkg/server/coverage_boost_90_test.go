package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestLiveCheckStopped tests LiveCheck when stopped
func TestLiveCheckStopped(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})
	l.state = LifecycleState(StateStopped)

	req := httptest.NewRequest("GET", "/live", nil)
	w := httptest.NewRecorder()

	handler := l.LiveCheck()
	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503 when stopped, got %d", w.Code)
	}
}

// TestLiveCheckRunning tests LiveCheck when running
func TestLiveCheckRunning(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})
	l.state = LifecycleState(StateRunning)

	req := httptest.NewRequest("GET", "/live", nil)
	w := httptest.NewRecorder()

	handler := l.LiveCheck()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 when running, got %d", w.Code)
	}
}

// TestReadyCheckRunning tests ReadyCheck when running
func TestReadyCheckRunning(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})
	l.state = LifecycleState(StateRunning)

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	handler := l.ReadyCheck()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 when running, got %d", w.Code)
	}
}

// TestReadyCheckInitializing tests ReadyCheck when initializing
func TestReadyCheckInitializing(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})
	l.state = LifecycleState(StateInitializing)

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	handler := l.ReadyCheck()
	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503 when initializing, got %d", w.Code)
	}
}

// TestHandleDBStatsSuccess tests handleDBStats with valid DB
func TestHandleDBStatsSuccess(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()

	admin.handleDBStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}
}

// TestHandleJSONMetricsSuccess tests handleJSONMetrics with valid DB
func TestHandleJSONMetricsSuccess(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/metrics/json", nil)
	w := httptest.NewRecorder()

	admin.handleJSONMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestGenerateSelfSignedCertSuccess tests generating self-signed cert
func TestGenerateSelfSignedCertSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	certDir := filepath.Join(tmpDir, "certs")

	config := &TLSConfig{
		CertFile:            filepath.Join(certDir, "server.crt"),
		KeyFile:             filepath.Join(certDir, "server.key"),
		SelfSignedOrg:       "TestOrg",
		SelfSignedValidDays: 1,
	}

	err := generateSelfSignedCert(config)
	if err != nil {
		t.Errorf("generateSelfSignedCert failed: %v", err)
	}

	// Check that cert file was created
	if _, err := os.Stat(config.CertFile); os.IsNotExist(err) {
		t.Error("Certificate file was not created")
	}
}

// TestGenerateClientCertInvalidCA tests GenerateClientCert with invalid CA
func TestGenerateClientCertInvalidCA(t *testing.T) {
	tmpDir := t.TempDir()

	_, _, err := GenerateClientCert(
		filepath.Join(tmpDir, "nonexistent-ca.crt"),
		filepath.Join(tmpDir, "nonexistent-ca.key"),
		"testuser",
		1,
	)
	if err == nil {
		t.Error("Expected error for non-existent CA")
	}
}

// TestFormatFloatPrecision tests formatFloat function
func TestFormatFloatPrecision(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{3.14159, 3.14},
		{2.71828, 2.72},
		{100.999, 101.0},
		{0.0, 0.0},
	}

	for _, tc := range tests {
		result := formatFloat(tc.input)
		if result != tc.expected {
			t.Errorf("formatFloat(%f) = %f, expected %f", tc.input, result, tc.expected)
		}
	}
}

// TestHandlePrometheusMetricsNilCollector tests handlePrometheusMetrics with nil collector
func TestHandlePrometheusMetricsNilCollector(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	admin.handlePrometheusMetrics(w, req)

	// Should return 200 even if collector is nil
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestHandleHealthWithDB2 tests handleHealth with database
func TestHandleHealthWithDB2(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	admin.handleHealth(w, req)

	if w.Code != http.StatusOK && w.Code != http.StatusServiceUnavailable {
		t.Errorf("Unexpected status: %d", w.Code)
	}
}

// TestNewSQLProtectorDefaultConfig tests NewSQLProtector with default config
func TestNewSQLProtectorDefaultConfig(t *testing.T) {
	config := DefaultSQLProtectionConfig()
	config.Enabled = true
	config.MaxQueryLength = 1000

	protector := NewSQLProtector(config)
	if protector == nil {
		t.Fatal("NewSQLProtector returned nil")
	}

	if !protector.config.Enabled {
		t.Error("Protector should be enabled")
	}
}

// TestLifecycleStateString tests state string representations
func TestLifecycleStateString(t *testing.T) {
	tests := []struct {
		state LifecycleState
		name  string
	}{
		{StateInitializing, "initializing"},
		{StateRunning, "running"},
		{StateDraining, "draining"},
		{StateStopped, "stopped"},
	}

	for _, tc := range tests {
		result := tc.state.String()
		if result != tc.name {
			t.Errorf("State(%d).String() = %q, expected %q", tc.state, result, tc.name)
		}
	}
}
