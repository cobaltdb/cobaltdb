package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestHandleDBStatsErrorPath tests handleDBStats when Stats returns error
func TestHandleDBStatsErrorPath(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()

	admin.handleDBStats(w, req)

	// Should return 200 even if there's an error (error is in JSON response)
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestHandleJSONMetricsWithCollector tests handleJSONMetrics with metrics collector
func TestHandleJSONMetricsWithCollector(t *testing.T) {
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

// TestHandleJSONMetricsErrorPaths tests handleJSONMetrics error paths
func TestHandleJSONMetricsErrorPaths(t *testing.T) {
	// Test with nil DB
	t.Run("NilDB", func(t *testing.T) {
		admin := NewAdminServer(nil, ":0")
		req := httptest.NewRequest("GET", "/metrics/json", nil)
		w := httptest.NewRecorder()

		admin.handleJSONMetrics(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})

	// Test with DB but no collector
	t.Run("NoCollector", func(t *testing.T) {
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
	})
}

// TestLiveCheckAllStates tests LiveCheck with all lifecycle states
func TestLiveCheckAllStates(t *testing.T) {
	states := []LifecycleState{
		StateInitializing,
		StateStarting,
		StateRunning,
		StateDraining,
		StateShuttingDown,
		StateStopped,
	}

	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			l := NewLifecycle(&LifecycleConfig{})
			l.state = state

			req := httptest.NewRequest("GET", "/live", nil)
			w := httptest.NewRecorder()

			handler := l.LiveCheck()
			handler(w, req)

			// Only stopped should return 503, others should return 200
			if state == StateStopped {
				if w.Code != http.StatusServiceUnavailable {
					t.Errorf("Expected status 503 for stopped state, got %d", w.Code)
				}
			} else {
				if w.Code != http.StatusOK {
					t.Errorf("Expected status 200 for state %s, got %d", state.String(), w.Code)
				}
			}
		})
	}
}

// TestReadyCheckAllStates tests ReadyCheck with all lifecycle states
func TestReadyCheckAllStates(t *testing.T) {
	states := []LifecycleState{
		StateInitializing,
		StateStarting,
		StateRunning,
		StateDraining,
		StateShuttingDown,
		StateStopped,
	}

	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			l := NewLifecycle(&LifecycleConfig{})
			l.state = state

			req := httptest.NewRequest("GET", "/ready", nil)
			w := httptest.NewRecorder()

			handler := l.ReadyCheck()
			handler(w, req)

			// Only running should return 200
			if state == StateRunning {
				if w.Code != http.StatusOK {
					t.Errorf("Expected status 200 for running state, got %d", w.Code)
				}
			} else {
				if w.Code != http.StatusServiceUnavailable {
					t.Errorf("Expected status 503 for state %s, got %d", state.String(), w.Code)
				}
			}
		})
	}
}

// TestGenerateSelfSignedCertExisting tests generateSelfSignedCert with existing cert
func TestGenerateSelfSignedCertExisting(t *testing.T) {
	tmpDir := t.TempDir()
	certDir := filepath.Join(tmpDir, "certs")

	config := &TLSConfig{
		CertFile:            filepath.Join(certDir, "server.crt"),
		KeyFile:             filepath.Join(certDir, "server.key"),
		SelfSignedOrg:       "TestOrg",
		SelfSignedValidDays: 1,
	}

	// First call to create cert
	err := generateSelfSignedCert(config)
	if err != nil {
		t.Fatalf("First generateSelfSignedCert failed: %v", err)
	}

	// Second call should reuse existing valid cert
	err = generateSelfSignedCert(config)
	if err != nil {
		t.Errorf("Second generateSelfSignedCert failed: %v", err)
	}
}

// TestGenerateSelfSignedCertInvalidReplacement tests generateSelfSignedCert replacing invalid cert
func TestGenerateSelfSignedCertInvalidReplacement(t *testing.T) {
	tmpDir := t.TempDir()
	certDir := filepath.Join(tmpDir, "certs")

	config := &TLSConfig{
		CertFile:            filepath.Join(certDir, "server.crt"),
		KeyFile:             filepath.Join(certDir, "server.key"),
		SelfSignedOrg:       "TestOrg",
		SelfSignedValidDays: 1,
	}

	// Create certs directory
	os.MkdirAll(certDir, 0750)

	// Create invalid cert file
	os.WriteFile(config.CertFile, []byte("invalid cert"), 0644)
	os.WriteFile(config.KeyFile, []byte("invalid key"), 0644)

	// Should regenerate cert when existing is invalid
	err := generateSelfSignedCert(config)
	if err != nil {
		t.Errorf("generateSelfSignedCert with invalid existing cert failed: %v", err)
	}
}

// TestGenerateClientCertWithInvalidCA tests GenerateClientCert with various invalid inputs
func TestGenerateClientCertWithInvalidCA(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name       string
		caCertFile string
		caKeyFile  string
		clientName string
	}{
		{
			name:       "NonExistentCA",
			caCertFile: filepath.Join(tmpDir, "nonexistent.crt"),
			caKeyFile:  filepath.Join(tmpDir, "nonexistent.key"),
			clientName: "testuser",
		},
		{
			name:       "InvalidCACert",
			caCertFile: filepath.Join(tmpDir, "invalid.crt"),
			caKeyFile:  filepath.Join(tmpDir, "invalid.key"),
			clientName: "testuser",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "InvalidCACert" {
				// Create invalid cert files
				os.WriteFile(tc.caCertFile, []byte("invalid"), 0644)
				os.WriteFile(tc.caKeyFile, []byte("invalid"), 0644)
				defer os.Remove(tc.caCertFile)
				defer os.Remove(tc.caKeyFile)
			}

			_, _, err := GenerateClientCert(tc.caCertFile, tc.caKeyFile, tc.clientName, 1)
			if err == nil {
				t.Error("Expected error for invalid CA")
			}
		})
	}
}

// TestHandlePrometheusMetricsAllPaths tests handlePrometheusMetrics with all paths
func TestHandlePrometheusMetricsAllPaths(t *testing.T) {
	t.Run("NilDB", func(t *testing.T) {
		admin := NewAdminServer(nil, ":0")
		req := httptest.NewRequest("GET", "/metrics", nil)
		w := httptest.NewRecorder()

		admin.handlePrometheusMetrics(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})

	t.Run("WithDBNoCollector", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		admin := NewAdminServer(db, ":0")
		req := httptest.NewRequest("GET", "/metrics", nil)
		w := httptest.NewRecorder()

		admin.handlePrometheusMetrics(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})
}

// TestHandleHealthAllStates tests handleHealth with different states
func TestHandleHealthAllStates(t *testing.T) {
	t.Run("NilDB", func(t *testing.T) {
		admin := NewAdminServer(nil, ":0")
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()

		admin.handleHealth(w, req)

		// Returns 200 with "degraded" status when DB is nil (not 503)
		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})

	t.Run("WithDB", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		admin := NewAdminServer(db, ":0")
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()

		admin.handleHealth(w, req)

		// Should return 200 or 503 depending on health
		if w.Code != http.StatusOK && w.Code != http.StatusServiceUnavailable {
			t.Errorf("Unexpected status: %d", w.Code)
		}
	})
}

// TestHandleReadyAllStates tests handleReady with different states
func TestHandleReadyAllStates(t *testing.T) {
	t.Run("NilDB", func(t *testing.T) {
		admin := NewAdminServer(nil, ":0")
		req := httptest.NewRequest("GET", "/ready", nil)
		w := httptest.NewRecorder()

		admin.handleReady(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("Expected status 503, got %d", w.Code)
		}
	})

	t.Run("WithDB", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		admin := NewAdminServer(db, ":0")
		req := httptest.NewRequest("GET", "/ready", nil)
		w := httptest.NewRecorder()

		admin.handleReady(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
	})
}
