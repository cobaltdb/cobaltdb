package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestHandleDBStatsWithNilDB tests handleDBStats when db is nil
func TestHandleDBStatsWithNilDB(t *testing.T) {
	admin := NewAdminServer(nil, ":0")

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()

	admin.handleDBStats(w, req)

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

// TestHandleDBStatsWithDB tests handleDBStats with valid database
func TestHandleDBStatsWithDB(t *testing.T) {
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

	var result map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Errorf("Response is not valid JSON: %v", err)
	}

	if _, ok := result["timestamp"]; !ok {
		t.Error("Response should have timestamp field")
	}

	if _, ok := result["stats"]; !ok {
		t.Error("Response should have stats field")
	}
}

// TestHandleSystem tests handleSystem endpoint
func TestHandleSystem(t *testing.T) {
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

	var result map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Errorf("Response is not valid JSON: %v", err)
	}

	if _, ok := result["timestamp"]; !ok {
		t.Error("Response should have timestamp field")
	}

	if _, ok := result["go"]; !ok {
		t.Error("Response should have go field")
	}

	if _, ok := result["memory"]; !ok {
		t.Error("Response should have memory field")
	}
}

// TestHandleStatsRedirectsToDBStats tests that handleStats calls handleDBStats
func TestHandleStatsRedirectsToDBStats(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":0")

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()

	admin.handleStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Errorf("Response is not valid JSON: %v", err)
	}

	if _, ok := result["timestamp"]; !ok {
		t.Error("Response should have timestamp field")
	}
}

// TestFormatFloat tests formatFloat helper function
func TestFormatFloat(t *testing.T) {
	result := formatFloat(3.14159265)
	if result != 3.14 {
		t.Errorf("Expected 3.14, got %f", result)
	}

	result = formatFloat(123.456)
	if result != 123.46 {
		t.Errorf("Expected 123.46, got %f", result)
	}
}

// TestAdminServerIsRunning tests IsRunning method
func TestAdminServerIsRunning(t *testing.T) {
	admin := NewAdminServer(nil, ":0")

	// Should be false before start
	if admin.IsRunning() {
		t.Error("IsRunning should be false before Start")
	}
}

// TestAdminServerAddr tests Addr method
func TestAdminServerAddr(t *testing.T) {
	admin := NewAdminServer(nil, ":8080")

	if admin.Addr() != ":8080" {
		t.Errorf("Expected addr :8080, got %s", admin.Addr())
	}
}
