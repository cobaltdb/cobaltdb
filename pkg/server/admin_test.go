package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestAdminServerSetsProductionHeaderLimit(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer admin.Stop()

	if admin.server == nil {
		t.Fatal("admin server was not initialized")
	}
	if admin.server.MaxHeaderBytes != productionHTTPMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", admin.server.MaxHeaderBytes, productionHTTPMaxHeaderBytes)
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

func TestAdminServerSetAuthTokenStoresDigestOnly(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("secret-token")

	if !admin.authTokenConfigured {
		t.Fatal("admin token should be configured")
	}
	if admin.authTokenDigest != adminTokenDigest("secret-token") {
		t.Fatal("admin token digest not stored")
	}
	if string(admin.authTokenDigest[:]) == "secret-token" {
		t.Fatal("admin token should not be stored as raw bytes")
	}

	handler := admin.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	validReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	validReq.Header.Set("Authorization", "Bearer secret-token")
	validRec := httptest.NewRecorder()
	handler.ServeHTTP(validRec, validReq)
	if validRec.Code != http.StatusNoContent {
		t.Fatalf("valid token status = %d, want %d", validRec.Code, http.StatusNoContent)
	}

	wrongReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	wrongReq.Header.Set("Authorization", "Bearer secret-token-extra")
	wrongRec := httptest.NewRecorder()
	handler.ServeHTTP(wrongRec, wrongReq)
	if wrongRec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want %d", wrongRec.Code, http.StatusUnauthorized)
	}
}

func TestAdminServerAuthMiddlewareRejectsOversizedAuthorization(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("secret-token")
	called := false
	handler := admin.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", maxAdminTokenBytes+1))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("oversized token status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("oversized token reached admin handler")
	}

	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Authorization", strings.Repeat("x", maxAdminAuthorizationHeaderBytes+1))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("oversized header status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("oversized Authorization header reached admin handler")
	}
}

func TestAdminServerSetAuthTokenEmptyDisablesAPI(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("secret-token")
	admin.SetAuthToken("")

	if admin.authTokenConfigured {
		t.Fatal("empty token should clear admin token configuration")
	}
	if admin.authTokenDigest != ([sha256.Size]byte{}) {
		t.Fatal("empty token should clear admin token digest")
	}

	handler := admin.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestAdminServerSetAuthTokenOversizedDisablesAPI(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken(strings.Repeat("x", maxAdminTokenBytes+1))

	if admin.authTokenConfigured {
		t.Fatal("oversized token should not configure admin token")
	}
	if admin.authTokenDigest != ([sha256.Size]byte{}) {
		t.Fatal("oversized token should clear admin token digest")
	}

	handler := admin.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("oversized configured token should not enable admin handler")
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", maxAdminTokenBytes+1))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestAdminServerAuthMiddlewareRejectsNonGETMethods(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("secret-token")
	called := false

	handler := admin.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if called {
		t.Fatal("non-GET request reached admin handler")
	}

	optionsReq := httptest.NewRequest(http.MethodOptions, "/health", nil)
	optionsRec := httptest.NewRecorder()
	handler.ServeHTTP(optionsRec, optionsReq)
	if optionsRec.Code != http.StatusOK {
		t.Fatalf("OPTIONS status = %d, want %d", optionsRec.Code, http.StatusOK)
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
