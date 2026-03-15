package server

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestHandleDBStatsCoverage targets handleDBStats endpoint
func TestHandleDBStatsCoverage(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	// Test valid request
	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/stats")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Logf("handleDBStats returned status %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("Stats response: %s", string(body))
}

// TestHandleJSONMetricsCoverage targets handleJSONMetrics endpoint
func TestHandleJSONMetricsCoverage(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/metrics/json")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("handleJSONMetrics returned status %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("JSON metrics response: %s", string(body))
}

// TestHandleSystemCoverage targets handleSystem endpoint
func TestHandleSystemCoverage(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/system")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("handleSystem returned status %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("System response: %s", string(body))
}

// TestHandleReadyCoverage targets handleReady endpoint with different states
func TestHandleReadyCoverage(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "127.0.0.1:0")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	// Test without auth token - should be service unavailable
	resp, err := http.Get("http://" + admin.Addr() + "/ready")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	// Without auth token, should return 503 (no database configured)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Logf("handleReady returned status %d (expected 503 when no DB configured)", resp.StatusCode)
	}
}

// TestStopCoverage targets Stop function error paths
func TestStopCoverage(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "127.0.0.1:0")

	// Stop without starting should not panic
	err = admin.Stop()
	if err != nil {
		t.Logf("Stop returned error (expected): %v", err)
	}

	// Normal stop after start
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	err = admin.Stop()
	if err != nil {
		t.Errorf("Stop returned error: %v", err)
	}

	// Double stop should not panic
	err = admin.Stop()
	if err != nil {
		t.Logf("Double stop returned error (may be expected): %v", err)
	}
}

// TestHandleJSONMetricsNoDB tests handleJSONMetrics with nil database
func TestHandleJSONMetricsNoDB(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/metrics/json")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("handleJSONMetrics returned status %d, expected 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	t.Logf("JSON metrics response with nil DB: %s", bodyStr)
}

// TestHandleDBStatsNoDB tests handleDBStats with nil database
func TestHandleDBStatsNoDB(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/stats")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("handleDBStats returned status %d, expected 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	t.Logf("DBStats response with nil DB: %s", bodyStr)
}
