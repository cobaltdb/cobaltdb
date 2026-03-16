package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestAdminServerHandleDBStatsNilDB tests handleDBStats with nil database
func TestAdminServerHandleDBStatsNilDB(t *testing.T) {
	admin := NewAdminServer(nil, ":0")

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()

	admin.handleDBStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	body := w.Body.String()
	if body == "" {
		t.Error("Expected non-empty response body")
	}
}

// TestAdminServerHandleJSONMetricsNilDB tests handleJSONMetrics with nil database
func TestAdminServerHandleJSONMetricsNilDB(t *testing.T) {
	admin := NewAdminServer(nil, ":0")

	req := httptest.NewRequest("GET", "/metrics/json", nil)
	w := httptest.NewRecorder()

	admin.handleJSONMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	body := w.Body.String()
	if body == "" {
		t.Error("Expected non-empty response body")
	}
}

// TestAdminServerHandleDBStatsWithDB tests handleDBStats with valid database
func TestAdminServerHandleDBStatsWithDB(t *testing.T) {
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

// TestAdminServerHandleJSONMetricsWithDB tests handleJSONMetrics with valid database
func TestAdminServerHandleJSONMetricsWithDB(t *testing.T) {
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

// TestAdminServerHandleStatsRedirect tests handleStats which calls handleDBStats
func TestAdminServerHandleStatsRedirect(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/dbstats", nil)
	w := httptest.NewRecorder()

	admin.handleStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestAdminServerIsRunningAndAddr tests IsRunning and Addr methods
func TestAdminServerIsRunningAndAddr(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true})
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	if admin.IsRunning() {
		t.Error("Admin server should not be running initially")
	}

	// Addr should return the configured address even when not running
	addr := admin.Addr()
	if addr != ":0" {
		t.Errorf("Expected addr :0, got %s", addr)
	}
}

// TestAdminServerHandleSystem tests handleSystem endpoint
func TestAdminServerHandleSystem(t *testing.T) {
	admin := NewAdminServer(nil, ":0")

	req := httptest.NewRequest("GET", "/system", nil)
	w := httptest.NewRecorder()

	admin.handleSystem(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}
}
