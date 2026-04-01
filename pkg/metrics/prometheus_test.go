package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPrometheusMetricsHandler tests the Prometheus HTTP handler
func TestPrometheusMetricsHandler(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Get the handler
	handler := pm.Handler()

	// Test GET request
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("Expected specific Content-Type, got %s", contentType)
	}

	// Test POST request (should be rejected)
	req2 := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	w2 := httptest.NewRecorder()
	handler(w2, req2)

	resp2 := w2.Result()
	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405 for POST, got %d", resp2.StatusCode)
	}
}

// TestGetPrometheusHandler tests the global Prometheus handler
func TestGetPrometheusHandler(t *testing.T) {
	handler := GetPrometheusHandler()
	if handler == nil {
		t.Error("GetPrometheusHandler returned nil")
	}

	// Test the handler
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

// TestWriteTransactionMetrics tests writing transaction metrics
func TestWriteTransactionMetrics(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Record some transaction metrics first
	GetTransactionMetrics().RecordTxnStart()

	// Create a response writer
	w := httptest.NewRecorder()

	// Write transaction metrics
	pm.writeTransactionMetrics(w)

	// Check output contains expected metrics
	output := w.Body.String()
	if output == "" {
		t.Error("writeTransactionMetrics produced no output")
	}
}

// TestWriteSystemMetrics tests writing system metrics
func TestWriteSystemMetrics(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Create a response writer
	w := httptest.NewRecorder()

	// Write system metrics
	pm.writeSystemMetrics(w)

	// Check output contains expected metrics
	output := w.Body.String()
	if output == "" {
		t.Error("writeSystemMetrics produced no output")
	}

	// Should contain uptime
	if !strings.Contains(output, "cobaltdb_uptime_seconds") {
		t.Error("Output should contain uptime metric")
	}

	// Should contain goroutines
	if !strings.Contains(output, "cobaltdb_go_goroutines") {
		t.Error("Output should contain goroutines metric")
	}
}

// TestWriteQueryMetrics tests writing query metrics
func TestWriteQueryMetrics(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Create a response writer
	w := httptest.NewRecorder()

	// Write query metrics
	pm.writeQueryMetrics(w)

	// Output may be empty if slow query log is not initialized
	_ = w.Body.String()
}

// TestWriteStorageMetrics tests writing storage metrics
func TestWriteStorageMetrics(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Create a response writer
	w := httptest.NewRecorder()

	// Write storage metrics
	pm.writeStorageMetrics(w)

	// Check output contains expected metrics
	output := w.Body.String()
	if output == "" {
		t.Error("writeStorageMetrics produced no output")
	}
}
