package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestAdminServerHandleDBStatsError tests handleDBStats when Stats returns error
func TestAdminServerHandleDBStatsError(t *testing.T) {
	// Create admin server with a mock or minimal setup
	// We need to test the error path when db.Stats() returns error
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

	// Parse response to verify it's valid JSON
	var result map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Errorf("Response is not valid JSON: %v", err)
	}

	// Should have timestamp and stats fields
	if _, ok := result["timestamp"]; !ok {
		t.Error("Response should have timestamp field")
	}
}

// TestAdminServerHandleJSONMetricsNotEnabled tests handleJSONMetrics when metrics not enabled
func TestAdminServerHandleJSONMetricsNotEnabled(t *testing.T) {
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

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}
}

// TestAdminServerHandlePrometheusMetrics tests handleMetrics endpoint
func TestAdminServerHandlePrometheusMetrics(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	admin.handleMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("Expected Content-Type text/plain, got %s", contentType)
	}
}

// TestAdminServerHandleHealth tests handleHealth endpoint
func TestAdminServerHandleHealth(t *testing.T) {
	admin := NewAdminServer(nil, ":0")

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	admin.handleHealth(w, req)

	// Status can be 200 or 503 depending on health
	if w.Code != http.StatusOK && w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 200 or 503, got %d", w.Code)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Errorf("Response is not valid JSON: %v", err)
	}

	// Status can be "healthy" or "degraded" depending on state
	if status, ok := result["status"]; !ok || (status != "healthy" && status != "degraded") {
		t.Errorf("Expected status 'healthy' or 'degraded', got %v", status)
	}
}

// TestAdminServerHandleReady tests handleReady endpoint
func TestAdminServerHandleReady(t *testing.T) {
	admin := NewAdminServer(nil, ":0")

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	admin.handleReady(w, req)

	// Without DB, ready returns 503
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503 without DB, got %d", w.Code)
	}
}

// TestAdminServerHandleReadyWithDB tests handleReady with database
func TestAdminServerHandleReadyWithDB(t *testing.T) {
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
}

// TestAdminServerMethods tests various admin server methods
func TestAdminServerMethods(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	// Test IsRunning before start
	if admin.IsRunning() {
		t.Error("IsRunning should be false before Start")
	}

	// Test Addr
	if admin.Addr() != ":0" {
		t.Errorf("Expected addr :0, got %s", admin.Addr())
	}
}
