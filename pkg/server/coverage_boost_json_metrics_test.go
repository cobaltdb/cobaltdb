package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestHandleJSONMetricsNilDB tests handleJSONMetrics when db is nil
func TestHandleJSONMetricsNilDB(t *testing.T) {
	admin := NewAdminServer(nil, ":0")

	req := httptest.NewRequest("GET", "/metrics/json", nil)
	w := httptest.NewRecorder()

	admin.handleJSONMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Errorf("Response is not valid JSON: %v", err)
	}

	if result["error"] != "database not initialized" {
		t.Errorf("Expected 'database not initialized' error, got %v", result["error"])
	}
}

// TestHandleJSONMetricsNoCollector tests handleJSONMetrics when metrics collector is nil or disabled
func TestHandleJSONMetricsNoCollector(t *testing.T) {
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

	// Response may have metrics or error depending on configuration
	var result map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		// Try string map for error case
		var errResult map[string]string
		if err2 := json.Unmarshal(w.Body.Bytes(), &errResult); err2 != nil {
			t.Errorf("Response is not valid JSON: %v", err)
		}
	}
}

// TestHandleJSONMetricsWithCollector tests handleJSONMetrics with metrics collector
func TestHandleJSONMetricsWithCollector(t *testing.T) {
	// Open with WAL to enable metrics
	dir := t.TempDir()
	dbPath := dir + "/test.db"
	db, err := engine.Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/metrics/json", nil)
	w := httptest.NewRecorder()

	admin.handleJSONMetrics(w, req)

	// Response may vary depending on metrics configuration
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}
}
