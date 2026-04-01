package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestTransactionMetricsHandler tests the transaction metrics HTTP handler
func TestTransactionMetricsHandler(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	config := &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout:      1 * time.Second,
			DrainTimeout:         100 * time.Millisecond,
			HealthCheckInterval:  500 * time.Millisecond,
			StartupTimeout:       1 * time.Second,
			EnableSignalHandling: false,
		},
		HealthAddr:         ":18421",
		EnableHealthServer: true,
	}

	ps := NewProductionServer(db, config)

	// Get the handler
	handler := ps.transactionMetricsHandler()

	// Test GET request
	req := httptest.NewRequest(http.MethodGet, "/transaction-metrics", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", contentType)
	}

	// Test POST request (should be rejected)
	req2 := httptest.NewRequest(http.MethodPost, "/transaction-metrics", nil)
	w2 := httptest.NewRecorder()
	handler(w2, req2)

	resp2 := w2.Result()
	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405 for POST, got %d", resp2.StatusCode)
	}
}
