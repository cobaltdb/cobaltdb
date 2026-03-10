package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func doAuthGet(t *testing.T, token, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("Failed to build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to execute request: %v", err)
	}
	return resp
}

func TestAdminServerHealth(t *testing.T) {
	// Create in-memory database
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create admin server
	admin := NewAdminServer(db, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Test health endpoint
	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/health")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var health map[string]interface{}
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("Failed to parse health response: %v", err)
	}

	if health["status"] != "healthy" {
		t.Errorf("Expected status healthy, got %v", health["status"])
	}
}

func TestAdminServerReady(t *testing.T) {
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

	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/ready")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestAdminServerMetricsJSON(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Execute a query to generate metrics
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO test VALUES (1, 'test')")
	_, _ = db.Query(ctx, "SELECT * FROM test")

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
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var metrics map[string]interface{}
	if err := json.Unmarshal(body, &metrics); err != nil {
		t.Fatalf("Failed to parse metrics: %v", err)
	}

	// Check required fields exist
	if _, ok := metrics["queries"]; !ok {
		t.Error("Missing queries field in metrics")
	}
	if _, ok := metrics["connections"]; !ok {
		t.Error("Missing connections field in metrics")
	}
}

func TestAdminServerMetricsPrometheus(t *testing.T) {
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

	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/metrics/prometheus")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "text/plain; version=0.0.4" {
		t.Errorf("Expected text/plain content type, got %s", contentType)
	}

	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Error("Expected non-empty prometheus metrics")
	}

	// Check for expected metrics
	bodyStr := string(body)
	if !contains(bodyStr, "cobaltdb_queries_total") {
		t.Error("Missing cobaltdb_queries_total metric")
	}
	if !contains(bodyStr, "cobaltdb_connections_active") {
		t.Error("Missing cobaltdb_connections_active metric")
	}
}

func TestAdminServerDBStats(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create some tables
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE test1 (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "CREATE TABLE test2 (id INTEGER PRIMARY KEY)")

	admin := NewAdminServer(db, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/stats")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("Failed to parse stats: %v", err)
	}

	if _, ok := result["stats"]; !ok {
		t.Error("Missing stats field")
	}
}

func TestAdminServerSystem(t *testing.T) {
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
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var info map[string]interface{}
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("Failed to parse system info: %v", err)
	}

	if _, ok := info["go"]; !ok {
		t.Error("Missing go field")
	}
	if _, ok := info["memory"]; !ok {
		t.Error("Missing memory field")
	}
}

func TestAdminServerAuth(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "127.0.0.1:0")
	admin.SetAuthToken("secret-token")

	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	// Request without auth should fail
	resp, err := http.Get("http://" + admin.Addr() + "/health")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected 401 without auth, got %d", resp.StatusCode)
	}

	// Request with correct auth should succeed
	req, _ := http.NewRequest(http.MethodGet, "http://"+admin.Addr()+"/health", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 with auth, got %d", resp.StatusCode)
	}
}

func TestAdminServerCORS(t *testing.T) {
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

	req, _ := http.NewRequest("OPTIONS", "http://"+admin.Addr()+"/health", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to make OPTIONS request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for OPTIONS, got %d", resp.StatusCode)
	}

	// Check CORS headers - wildcard origin removed for security, but methods header should exist
	if resp.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Error("Missing CORS Allow-Methods header")
	}
}

func TestAdminServerDisabledWithoutToken(t *testing.T) {
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

	resp, err := http.Get("http://" + admin.Addr() + "/health")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when admin token is not configured, got %d", resp.StatusCode)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
