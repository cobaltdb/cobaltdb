package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

var (
	errTemporary = errors.New("temporary")
	errPermanent = errors.New("permanent")
)

func TestProductionServerBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
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
		CircuitBreaker:       engine.DefaultCircuitBreakerConfig(),
		Retry:                engine.DefaultRetryConfig(),
		HealthAddr:           ":18420",
		EnableCircuitBreaker: true,
		EnableRetry:          true,
		EnableHealthServer:   true,
	}

	ps := NewProductionServer(db, config)

	// Start the server
	if err := ps.Start(); err != nil {
		t.Fatalf("failed to start production server: %v", err)
	}

	// Check it's running
	if !ps.IsRunning() {
		t.Error("expected server to be running")
	}

	// Check health
	if !ps.IsHealthy() {
		t.Error("expected server to be healthy")
	}

	time.Sleep(100 * time.Millisecond) // Give health server time to start

	// Test health endpoint
	resp, err := http.Get("http://localhost:18420/health")
	if err != nil {
		t.Logf("health endpoint not accessible (may need more time): %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
	}

	// Test ready endpoint
	resp, err = http.Get("http://localhost:18420/ready")
	if err != nil {
		t.Logf("ready endpoint not accessible: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200 for ready, got %d", resp.StatusCode)
		}
	}

	// Stop the server
	if err := ps.Stop(); err != nil {
		t.Fatalf("failed to stop server: %v", err)
	}

	// Check it's stopped
	if ps.IsRunning() {
		t.Error("expected server to not be running after stop")
	}
}

func TestProductionHealthServerSetsHeaderLimit(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	config := DefaultProductionConfig()
	config.HealthAddr = "127.0.0.1:0"
	config.Lifecycle.EnableSignalHandling = false
	ps := NewProductionServer(db, config)
	if err := ps.Start(); err != nil {
		t.Fatalf("failed to start production server: %v", err)
	}
	defer ps.Stop()

	if ps.healthServer == nil {
		t.Fatal("health server was not initialized")
	}
	if ps.healthServer.MaxHeaderBytes != productionHTTPMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", ps.healthServer.MaxHeaderBytes, productionHTTPMaxHeaderBytes)
	}
}

func TestProductionHealthHandlersSetJSONContentType(t *testing.T) {
	ps := NewProductionServer(nil, &ProductionConfig{EnableHealthServer: false})

	for _, tt := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{name: "health", handler: ps.healthHandler()},
		{name: "ready", handler: ps.readyHandler()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/"+tt.name, nil)
			rec := httptest.NewRecorder()

			tt.handler(rec, req)

			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", got)
			}
		})
	}
}

func TestProductionServerStartReturnsHealthBindError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	config := DefaultProductionConfig()
	config.HealthAddr = listener.Addr().String()
	config.Lifecycle.EnableSignalHandling = false

	ps := NewProductionServer(db, config)
	err = ps.Start()
	if err == nil {
		t.Fatal("expected health bind error")
	}
	if !strings.Contains(err.Error(), "failed to start health server") {
		t.Fatalf("expected health server error, got %v", err)
	}
	if ps.IsRunning() {
		t.Fatal("server should not remain running after health bind failure")
	}
}

func TestProductionServerWithRetry(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	config := DefaultProductionConfig()
	config.EnableHealthServer = false // Don't start HTTP server for this test
	config.Lifecycle.EnableSignalHandling = false

	ps := NewProductionServer(db, config)

	if err := ps.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer ps.Stop()

	// Test ExecuteWithRetry
	var callCount int
	err = ps.ExecuteWithRetry(context.Background(), func() error {
		callCount++
		if callCount < 2 {
			return fmt.Errorf("temporary error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

func TestProductionPrometheusMetricsRemoteAccess(t *testing.T) {
	tests := []struct {
		name               string
		allowRemoteMetrics bool
		adminToken         string
		authHeader         string
		wantStatus         int
	}{
		{
			name:               "RemoteDeniedByDefault",
			allowRemoteMetrics: false,
			wantStatus:         http.StatusForbidden,
		},
		{
			name:               "RemoteDisabledWithoutAdminToken",
			allowRemoteMetrics: true,
			wantStatus:         http.StatusServiceUnavailable,
		},
		{
			name:               "RemoteRequiresAuthorization",
			allowRemoteMetrics: true,
			adminToken:         "secret-token",
			wantStatus:         http.StatusUnauthorized,
		},
		{
			name:               "RemoteRejectsWrongAuthorization",
			allowRemoteMetrics: true,
			adminToken:         "secret-token",
			authHeader:         "Bearer wrong-token",
			wantStatus:         http.StatusUnauthorized,
		},
		{
			name:               "RemoteAllowedWithAdminToken",
			allowRemoteMetrics: true,
			adminToken:         "secret-token",
			authHeader:         "Bearer secret-token",
			wantStatus:         http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := NewProductionServer(nil, &ProductionConfig{
				AllowRemoteMetrics: tt.allowRemoteMetrics,
				AdminToken:         tt.adminToken,
			})
			req := httptest.NewRequest(http.MethodGet, "/metrics/prometheus", nil)
			req.RemoteAddr = "10.0.0.42:49152"
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rr := httptest.NewRecorder()

			ps.prometheusMetricsHandler()(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rr.Code)
			}
		})
	}
}

func TestProductionServerRejectsRemoteMetricsWithoutAdminToken(t *testing.T) {
	ps := NewProductionServer(nil, &ProductionConfig{
		HealthAddr:         "127.0.0.1:0",
		EnableHealthServer: true,
		AllowRemoteMetrics: true,
		Lifecycle: &LifecycleConfig{
			EnableSignalHandling: false,
		},
	})

	err := ps.Start()
	if err == nil {
		_ = ps.Stop()
		t.Fatal("expected remote metrics without admin token to fail startup")
	}
	if !strings.Contains(err.Error(), "remote metrics require an admin token") {
		t.Fatalf("expected remote metrics admin token error, got %v", err)
	}
}

func TestProductionServerConfigIsolation(t *testing.T) {
	config := &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout:      time.Second,
			DrainTimeout:         time.Second,
			HealthCheckInterval:  time.Second,
			StartupTimeout:       time.Second,
			EnableSignalHandling: false,
		},
		CircuitBreaker: &engine.CircuitBreakerConfig{
			MaxFailures:         2,
			MinSuccesses:        1,
			ResetTimeout:        time.Second,
			MaxConcurrency:      4,
			HalfOpenMaxRequests: 1,
		},
		Retry: &engine.RetryConfig{
			MaxAttempts:        2,
			InitialDelay:       time.Millisecond,
			MaxDelay:           time.Second,
			Multiplier:         1.5,
			Jitter:             0.1,
			RetryableErrors:    []error{errTemporary},
			NonRetryableErrors: []error{errPermanent},
		},
		RateLimiter: &RateLimiterConfig{
			RPS:             7,
			Burst:           3,
			PerClient:       true,
			ClientHeader:    "X-Test-Client",
			CleanupInterval: time.Second,
			MaxClients:      5,
		},
		EnableCircuitBreaker: true,
		EnableRateLimiter:    true,
		EnableRetry:          true,
		EnableHealthServer:   false,
	}

	ps := NewProductionServer(nil, config)
	defer ps.RateLimiter.Stop()

	config.Lifecycle.ShutdownTimeout = 99 * time.Second
	config.Lifecycle.ShutdownSignals = append(config.Lifecycle.ShutdownSignals, syscall.SIGHUP)
	config.CircuitBreaker.MaxFailures = 99
	config.RateLimiter.RPS = 99
	config.RateLimiter.ClientHeader = "X-Mutated"
	config.Retry.MaxAttempts = 99
	config.Retry.RetryableErrors[0] = errPermanent
	config.Retry.NonRetryableErrors[0] = errTemporary

	if ps.Config == config {
		t.Fatal("expected production server to store a config copy")
	}
	if ps.Config.Lifecycle.ShutdownTimeout != time.Second {
		t.Fatalf("expected lifecycle config copy, got shutdown timeout %v", ps.Config.Lifecycle.ShutdownTimeout)
	}
	if len(ps.Config.Lifecycle.ShutdownSignals) != 2 {
		t.Fatalf("expected lifecycle signal defaults isolated from caller mutation, got %d", len(ps.Config.Lifecycle.ShutdownSignals))
	}
	if ps.Config.CircuitBreaker.MaxFailures != 2 {
		t.Fatalf("expected circuit breaker config copy, got max failures %d", ps.Config.CircuitBreaker.MaxFailures)
	}
	if ps.Config.RateLimiter.RPS != 7 || ps.Config.RateLimiter.ClientHeader != "X-Test-Client" {
		t.Fatalf("expected rate limiter config copy, got rps=%d header=%q", ps.Config.RateLimiter.RPS, ps.Config.RateLimiter.ClientHeader)
	}
	if ps.Config.Retry.MaxAttempts != 2 {
		t.Fatalf("expected retry config copy, got max attempts %d", ps.Config.Retry.MaxAttempts)
	}
	if !errors.Is(ps.Config.Retry.RetryableErrors[0], errTemporary) {
		t.Fatal("expected retryable errors slice copy")
	}
	if !errors.Is(ps.Config.Retry.NonRetryableErrors[0], errPermanent) {
		t.Fatal("expected non-retryable errors slice copy")
	}
}

func TestProductionServerLoggerDoesNotMutateCallerLifecycle(t *testing.T) {
	log := logger.New(logger.InfoLevel, io.Discard)
	config := &ProductionConfig{
		Lifecycle:          nil,
		EnableHealthServer: false,
		Logger:             log,
	}

	ps := NewProductionServer(nil, config)
	if config.Lifecycle != nil {
		t.Fatal("expected caller lifecycle config to remain nil")
	}
	if ps.Config.Lifecycle == nil || ps.Config.Lifecycle.Logger != log {
		t.Fatal("expected logger to be applied to cloned lifecycle config")
	}
}

func TestProductionServerStoresAdminTokenDigest(t *testing.T) {
	config := &ProductionConfig{
		AdminToken:         "secret-token",
		EnableHealthServer: false,
	}
	ps := NewProductionServer(nil, config)

	if config.AdminToken != "secret-token" {
		t.Fatal("constructor mutated caller admin token")
	}
	if ps.Config.AdminToken != "" {
		t.Fatalf("raw admin token retained in production config: %q", ps.Config.AdminToken)
	}
	if !ps.adminTokenSet {
		t.Fatal("expected admin token to be configured")
	}
	if ps.adminTokenDigest != adminTokenDigest("secret-token") {
		t.Fatal("expected admin token digest to match configured token")
	}
	if string(ps.adminTokenDigest[:]) == "secret-token" {
		t.Fatal("raw admin token stored in digest")
	}
}

func TestProductionServerRejectsOversizedConfiguredAdminToken(t *testing.T) {
	config := &ProductionConfig{
		AdminToken:         strings.Repeat("x", maxAdminTokenBytes+1),
		EnableHealthServer: false,
	}
	ps := NewProductionServer(nil, config)

	if config.AdminToken == "" {
		t.Fatal("constructor should not mutate caller admin token")
	}
	if ps.Config.AdminToken != "" {
		t.Fatalf("raw admin token retained in production config: %q", ps.Config.AdminToken)
	}
	if ps.adminTokenSet {
		t.Fatal("oversized admin token should not be configured")
	}
	if ps.adminTokenDigest != ([sha256.Size]byte{}) {
		t.Fatal("oversized admin token should leave empty digest")
	}
}

func TestProductionAuthRequiredHandlerRequiresConfiguredAdminToken(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	ps := NewProductionServer(nil, &ProductionConfig{})
	handler := ps.authRequiredHandler(inner)

	req := httptest.NewRequest(http.MethodPost, "/admin", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing admin token status = %d, want 503", rec.Code)
	}

	ps.SetAdminToken("secret-token")

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing Authorization status = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", maxAdminTokenBytes+1))
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("oversized bearer token status = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", strings.Repeat("x", maxAdminAuthorizationHeaderBytes+1))
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("oversized Authorization status = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid bearer token status = %d, want 200", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "secret-token")
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid plain token status = %d, want 200", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "10.0.0.10:12345"
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("remote token status = %d, want 403", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "127.0.0.2:12345"
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("alternate loopback token status = %d, want 200", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.RemoteAddr = "not-a-host-port"
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("malformed remote address status = %d, want 403", rec.Code)
	}
}

func TestProductionHealthMuxProtectsAdminStatsEndpoint(t *testing.T) {
	ps := NewProductionServer(nil, &ProductionConfig{AdminToken: "secret-token"})
	mux := ps.healthMux()

	req := httptest.NewRequest(http.MethodPost, "/stats", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /stats status = %d, want 405", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid token status = %d, want 200", rec.Code)
	}
}

func TestProductionHealthMuxAppliesRateLimiter(t *testing.T) {
	ps := NewProductionServer(nil, &ProductionConfig{
		EnableRateLimiter: true,
		RateLimiter: &RateLimiterConfig{
			RPS:             1,
			Burst:           1,
			PerClient:       false,
			CleanupInterval: time.Minute,
			MaxClients:      10,
		},
	})
	defer ps.RateLimiter.Stop()
	mux := ps.healthMux()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After header = %q, want 1", rec.Header().Get("Retry-After"))
	}
}

func TestProductionRateLimitsHandlerReturnsStats(t *testing.T) {
	ps := NewProductionServer(nil, &ProductionConfig{
		AdminToken:        "secret-token",
		EnableRateLimiter: true,
		RateLimiter: &RateLimiterConfig{
			RPS:             10,
			Burst:           4,
			PerClient:       false,
			CleanupInterval: time.Minute,
			MaxClients:      10,
		},
	})
	defer ps.RateLimiter.Stop()
	handler := ps.authRequiredHandler(ps.rateLimitsHandler())

	req := httptest.NewRequest(http.MethodGet, "/rate-limits", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rate limits status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"rps":10`) || !strings.Contains(rec.Body.String(), `"burst":4`) {
		t.Fatalf("rate limits response missing stats: %s", rec.Body.String())
	}
}

func TestAdminTokenEqual(t *testing.T) {
	if !adminTokenEqual("secret-token", "secret-token") {
		t.Fatal("matching admin tokens should compare equal")
	}
	tests := []struct {
		name     string
		provided string
		expected string
	}{
		{name: "different content", provided: "secret-tokem", expected: "secret-token"},
		{name: "shorter", provided: "secret", expected: "secret-token"},
		{name: "longer", provided: "secret-token-extra", expected: "secret-token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if adminTokenEqual(tt.provided, tt.expected) {
				t.Fatalf("tokens %q and %q should not compare equal", tt.provided, tt.expected)
			}
		})
	}
}

func TestProductionServerWithCircuitBreaker(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	config := DefaultProductionConfig()
	config.EnableHealthServer = false
	config.Lifecycle.EnableSignalHandling = false

	ps := NewProductionServer(db, config)

	if err := ps.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer ps.Stop()

	// Test ExecuteWithCircuitBreaker
	key := "test-operation"

	// First call should succeed
	err = ps.ExecuteWithCircuitBreaker(key, func() error {
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Multiple failures should trip the circuit
	for i := 0; i < 5; i++ {
		ps.ExecuteWithCircuitBreaker(key, func() error {
			return fmt.Errorf("error %d", i)
		})
	}

	// Circuit should be open now
	cb, exists := ps.CircuitBreakers.Get(key)
	if !exists {
		t.Fatal("circuit breaker should exist")
	}

	if cb.State() != engine.CircuitOpen {
		t.Errorf("expected circuit to be open, got %s", cb.State())
	}
}

func TestProductionServerStats(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	config := DefaultProductionConfig()
	config.EnableHealthServer = false
	config.Lifecycle.EnableSignalHandling = false

	ps := NewProductionServer(db, config)

	stats := ps.GetStats()
	if stats.IsRunning {
		t.Error("expected IsRunning to be false before start")
	}

	if err := ps.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	stats = ps.GetStats()
	if !stats.IsRunning {
		t.Error("expected IsRunning to be true after start")
	}
	if !stats.IsHealthy {
		t.Error("expected IsHealthy to be true")
	}
	if stats.State != "running" {
		t.Errorf("expected state 'running', got %s", stats.State)
	}

	ps.Stop()

	stats = ps.GetStats()
	if stats.IsRunning {
		t.Error("expected IsRunning to be false after stop")
	}
}

func TestProductionServerDisabledFeatures(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
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
		EnableCircuitBreaker: false,
		EnableRetry:          false,
		EnableHealthServer:   false,
	}

	ps := NewProductionServer(db, config)

	if ps.CircuitBreakers != nil {
		t.Error("expected CircuitBreakers to be nil when disabled")
	}

	if err := ps.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer ps.Stop()

	// Retry should still work but without actual retry
	err = ps.ExecuteWithRetry(context.Background(), func() error {
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Circuit breaker should still work but without actual protection
	err = ps.ExecuteWithCircuitBreaker("test", func() error {
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDefaultProductionConfig(t *testing.T) {
	config := DefaultProductionConfig()

	if config.Lifecycle == nil {
		t.Error("expected Lifecycle config")
	}
	if config.CircuitBreaker == nil {
		t.Error("expected CircuitBreaker config")
	}
	if config.Retry == nil {
		t.Error("expected Retry config")
	}
	if config.HealthAddr != "127.0.0.1:8420" {
		t.Errorf("expected HealthAddr 127.0.0.1:8420, got %s", config.HealthAddr)
	}
	if !config.EnableCircuitBreaker {
		t.Error("expected EnableCircuitBreaker to be true")
	}
	if !config.EnableRetry {
		t.Error("expected EnableRetry to be true")
	}
	if !config.EnableHealthServer {
		t.Error("expected EnableHealthServer to be true")
	}
	if config.AdminToken != "" {
		t.Error("expected AdminToken to be empty by default")
	}
}

func BenchmarkProductionServerStartStop(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	config := DefaultProductionConfig()
	config.EnableHealthServer = false
	config.Lifecycle.EnableSignalHandling = false

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps := NewProductionServer(db, config)
		ps.Start()
		ps.Stop()
	}
}

// TestProductionTransactionMetricsHandler exercises the
// transactionMetricsHandler GET/POST contract. The handler must return
// 200 with JSON on GET and reject POST with 405.
func TestProductionTransactionMetricsHandler(t *testing.T) {
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
	handler := ps.transactionMetricsHandler()

	// GET should succeed with JSON.
	req := httptest.NewRequest(http.MethodGet, "/transaction-metrics", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET: expected status 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("GET: expected Content-Type application/json, got %s", got)
	}

	// POST should be rejected.
	req2 := httptest.NewRequest(http.MethodPost, "/transaction-metrics", nil)
	w2 := httptest.NewRecorder()
	handler(w2, req2)

	resp2 := w2.Result()
	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST: expected status 405, got %d", resp2.StatusCode)
	}
}

// TestProductionServerWait covers ProductionServer.Wait: it must
// return after Stop() is called. Ported from the
// coverage_boost_server3_test.go padding file.
func TestProductionServerWait(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfg := &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout:      100 * time.Millisecond,
			DrainTimeout:         50 * time.Millisecond,
			HealthCheckInterval:  500 * time.Millisecond,
			StartupTimeout:       1 * time.Second,
			EnableSignalHandling: false,
		},
		EnableHealthServer: false,
	}

	ps := NewProductionServer(db, cfg)
	if err := ps.Start(); err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	done := make(chan bool)
	go func() {
		ps.Wait()
		done <- true
	}()

	time.Sleep(50 * time.Millisecond)
	go ps.Stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("Wait() did not return after Stop()")
	}
}

// TestLifecycleWaitDirect covers Lifecycle.Wait in isolation from the
// ProductionServer wrapper. Ported from coverage_boost_server3_test.go.
func TestLifecycleWaitDirect(t *testing.T) {
	lc := NewLifecycle(&LifecycleConfig{
		ShutdownTimeout:      100 * time.Millisecond,
		DrainTimeout:         50 * time.Millisecond,
		HealthCheckInterval:  500 * time.Millisecond,
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	})

	if err := lc.Start(); err != nil {
		t.Fatalf("Failed to start lifecycle: %v", err)
	}

	done := make(chan bool)
	go func() {
		lc.Wait()
		done <- true
	}()

	time.Sleep(50 * time.Millisecond)
	go lc.Stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("Lifecycle.Wait() did not return after Stop()")
	}
}
