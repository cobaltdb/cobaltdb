package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// ============================================================
// Admin Server Handler Tests
// ============================================================

func TestAdminHandleMetricsCov(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "")

	// Test metrics endpoint
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	admin.handleMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	if !containsStr(w.Body.String(), "CobaltDB Metrics") {
		t.Error("expected metrics header in response")
	}
}

func TestAdminHandleMetricsWithNilDBCov(t *testing.T) {
	admin := NewAdminServer(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	admin.handleMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestAdminHandleDBStatsCov(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a table to have some stats
	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test_stats (id INTEGER PRIMARY KEY)")

	admin := NewAdminServer(db, "")

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()
	admin.handleDBStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to parse stats: %v", err)
	}
}

func TestAdminHandleJSONMetricsCov(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "")

	req := httptest.NewRequest(http.MethodGet, "/metrics/json", nil)
	w := httptest.NewRecorder()
	admin.handleJSONMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var metrics map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &metrics); err != nil {
		t.Errorf("failed to parse JSON metrics: %v", err)
	}
}

func TestAdminHandleReadyCov(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "")

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	admin.handleReady(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp["status"] != "ready" {
		t.Errorf("expected status 'ready', got %s", resp["status"])
	}
}

func TestAdminHandleReadyWithNilDBCov(t *testing.T) {
	admin := NewAdminServer(nil, "")

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	admin.handleReady(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", w.Code)
	}
}

// ============================================================
// SQL Protection Tests
// ============================================================

func TestSQLProtectorCheckCov(t *testing.T) {
	protector := NewSQLProtector(DefaultSQLProtectionConfig())
	if protector == nil {
		t.Fatal("expected protector to be created")
	}

	// Test allowed query
	result := protector.CheckSQL("SELECT * FROM users WHERE id = 1")
	if !result.Allowed {
		t.Errorf("expected query to be allowed, got violations: %v", result.Violations)
	}
}

// ============================================================
// Lifecycle Tests
// ============================================================

func TestLiveCheckHandlerCov(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	cfg.EnableSignalHandling = false
	lc := NewLifecycle(cfg)

	handler := lc.LiveCheck()

	// Test live check
	req := httptest.NewRequest(http.MethodGet, "/live", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

// ============================================================
// Helper Functions
// ============================================================

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAtStr(s, substr, 0))
}

func containsAtStr(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
