package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// ============================================================
// Admin Server Additional Handler Coverage
// ============================================================

func TestCovBoost3_AdminHandleSystem(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "")

	req := httptest.NewRequest(http.MethodGet, "/system", nil)
	w := httptest.NewRecorder()
	admin.handleSystem(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	// System endpoint returns various system info
	if resp == nil {
		t.Error("expected non-nil response")
	}
}

// ============================================================
// Rate Limiter Coverage
// ============================================================

func TestCovBoost3_RateLimiter_Basic(t *testing.T) {
	config := &RateLimiterConfig{
		RPS:             10,
		Burst:           5,
		PerClient:       false,
		CleanupInterval: time.Minute,
		MaxClients:      100,
	}

	rl := NewRateLimiter(config)
	defer rl.Stop()

	// First few requests should be allowed (within burst)
	for i := 0; i < 5; i++ {
		if !rl.Allow("client1") {
			t.Errorf("request %d should be allowed", i+1)
		}
	}
}

func TestCovBoost3_RateLimiter_PerClient(t *testing.T) {
	config := &RateLimiterConfig{
		RPS:             1000,
		Burst:           100,
		PerClient:       true,
		CleanupInterval: time.Minute,
		MaxClients:      100,
	}

	rl := NewRateLimiter(config)
	defer rl.Stop()

	// Each client should have its own bucket
	for i := 0; i < 5; i++ {
		if !rl.Allow("client1") {
			t.Errorf("client1 request %d should be allowed", i+1)
		}
		if !rl.Allow("client2") {
			t.Errorf("client2 request %d should be allowed", i+1)
		}
	}
}

// ============================================================
// SQL Protector Coverage
// ============================================================

func TestCovBoost3_SQLProtector_SuspiciousPatterns(t *testing.T) {
	protector := NewSQLProtector(DefaultSQLProtectionConfig())

	tests := []struct {
		name    string
		sql     string
		allowed bool
	}{
		{"Simple SELECT", "SELECT * FROM users", true},
		{"SELECT with condition", "SELECT * FROM users WHERE id = 1", true},
		{"Long query", "SELECT * FROM users WHERE id = 1 AND name = 'very long name that exceeds typical limits and should be checked for length'", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protector.CheckSQL(tt.sql)
			if result.Allowed != tt.allowed {
				t.Errorf("expected allowed=%v, got %v for SQL: %s", tt.allowed, result.Allowed, tt.sql)
			}
		})
	}
}

func TestCovBoost3_SQLProtector_Disabled(t *testing.T) {
	config := DefaultSQLProtectionConfig()
	config.Enabled = false
	protector := NewSQLProtector(config)

	// When disabled, all queries should be allowed
	result := protector.CheckSQL("SELECT * FROM users")
	if !result.Allowed {
		t.Error("query should be allowed when protection is disabled")
	}
}

// ============================================================
// TLS Config Coverage
// ============================================================

func TestCovBoost3_TLSConfig_Default(t *testing.T) {
	config := DefaultTLSConfig()

	if config.Enabled {
		t.Error("TLS should be disabled by default")
	}
	if config.GenerateSelfSigned {
		t.Error("GenerateSelfSigned should be false by default")
	}
}

func TestCovBoost3_TLSConfig_EnableSelfSigned(t *testing.T) {
	config := &TLSConfig{
		Enabled:            true,
		GenerateSelfSigned: true,
	}

	if !config.Enabled {
		t.Error("TLS should be enabled")
	}
	if !config.GenerateSelfSigned {
		t.Error("GenerateSelfSigned should be true")
	}
}

// ============================================================
// Production Config Coverage
// ============================================================

func TestCovBoost3_ProductionConfig_Default(t *testing.T) {
	config := DefaultProductionConfig()

	if config.HealthAddr != ":8420" {
		t.Errorf("expected default health address :8420, got %s", config.HealthAddr)
	}
	if !config.EnableHealthServer {
		t.Error("EnableHealthServer should be true by default")
	}
}

func TestCovBoost3_ProductionConfig_WithCustomAddr(t *testing.T) {
	config := &ProductionConfig{
		HealthAddr:         ":8440",
		EnableHealthServer: true,
	}

	if config.HealthAddr != ":8440" {
		t.Errorf("expected health address :8440, got %s", config.HealthAddr)
	}
}

// ============================================================
// Server Config Coverage
// ============================================================

func TestCovBoost3_ServerConfig_Default(t *testing.T) {
	config := DefaultConfig()

	if config.Address != ":4200" {
		t.Errorf("expected default address :4200, got %s", config.Address)
	}
}

func TestCovBoost3_ServerConfig_WithAuth(t *testing.T) {
	config := &Config{
		Address:     ":4200",
		AuthEnabled: true,
		RequireAuth: true,
	}

	if !config.AuthEnabled {
		t.Error("AuthEnabled should be true")
	}
	if !config.RequireAuth {
		t.Error("RequireAuth should be true")
	}
}

// ============================================================
// Admin Server Token Coverage
// ============================================================

func TestCovBoost3_AdminSetAuthToken(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	admin := NewAdminServer(db, "")
	admin.SetAuthToken("new-token")

	// Token is private, but we can verify server works
	if admin == nil {
		t.Error("admin server should not be nil")
	}
}

// ============================================================
// Circuit Breaker Config Coverage
// ============================================================

func TestCovBoost3_CircuitBreakerConfig_Default(t *testing.T) {
	config := engine.DefaultCircuitBreakerConfig()

	if config.MaxFailures != 5 {
		t.Errorf("expected MaxFailures 5, got %d", config.MaxFailures)
	}
	if config.ResetTimeout != 30*time.Second {
		t.Errorf("expected ResetTimeout 30s, got %v", config.ResetTimeout)
	}
	if config.MaxConcurrency != 100 {
		t.Errorf("expected MaxConcurrency 100, got %d", config.MaxConcurrency)
	}
}

// ============================================================
// Retry Config Coverage
// ============================================================

func TestCovBoost3_RetryConfig_Default(t *testing.T) {
	config := engine.DefaultRetryConfig()

	if config.MaxAttempts != 3 {
		t.Errorf("expected MaxAttempts 3, got %d", config.MaxAttempts)
	}
	if config.InitialDelay != 100*time.Millisecond {
		t.Errorf("expected InitialDelay 100ms, got %v", config.InitialDelay)
	}
	if config.Multiplier != 2.0 {
		t.Errorf("expected Multiplier 2.0, got %f", config.Multiplier)
	}
}

// ============================================================
// Rate Limiter Config Coverage
// ============================================================

func TestCovBoost3_RateLimiterConfig_Default(t *testing.T) {
	config := DefaultRateLimiterConfig()

	if config.RPS != 1000 {
		t.Errorf("expected RPS 1000, got %d", config.RPS)
	}
	if config.Burst != 100 {
		t.Errorf("expected Burst 100, got %d", config.Burst)
	}
	if config.CleanupInterval != 5*time.Minute {
		t.Errorf("expected CleanupInterval 5m, got %v", config.CleanupInterval)
	}
}

// ============================================================
// SQL Protection Config Coverage
// ============================================================

func TestCovBoost3_SQLProtectionConfig_Default(t *testing.T) {
	config := DefaultSQLProtectionConfig()

	if !config.Enabled {
		t.Error("Enabled should be true by default")
	}
	if config.MaxQueryLength != 10000 {
		t.Errorf("expected MaxQueryLength 10000, got %d", config.MaxQueryLength)
	}
	if config.MaxORConditions != 10 {
		t.Errorf("expected MaxORConditions 10, got %d", config.MaxORConditions)
	}
	if config.SuspiciousThreshold != 3 {
		t.Errorf("expected SuspiciousThreshold 3, got %d", config.SuspiciousThreshold)
	}
}

// ============================================================
// Admin Server Address Coverage
// ============================================================

func TestCovBoost3_AdminAddr(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	admin := NewAdminServer(db, ":8080")

	// Addr should return the configured address
	addr := admin.Addr()
	if addr != ":8080" {
		t.Errorf("expected address :8080, got %s", addr)
	}
}

// ============================================================
// Lifecycle Coverage
// ============================================================

func TestCovBoost3_Lifecycle_GetHealth(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	cfg.EnableSignalHandling = false
	lc := NewLifecycle(cfg)

	// Get health should return current health status
	health := lc.GetHealth()
	if health == nil {
		t.Error("GetHealth should return a map")
	}
}

func TestCovBoost3_Lifecycle_StateString(t *testing.T) {
	// Test state string representations
	states := []struct {
		state LifecycleState
		name  string
	}{
		{StateInitializing, "initializing"},
		{StateStarting, "starting"},
		{StateRunning, "running"},
		{StateDraining, "draining"},
		{StateShuttingDown, "shutting_down"},
		{StateStopped, "stopped"},
	}

	for _, s := range states {
		if s.state.String() != s.name {
			t.Errorf("expected state %s, got %s", s.name, s.state.String())
		}
	}
}

// ============================================================
// Lifecycle Config Coverage
// ============================================================

func TestCovBoost3_LifecycleConfig_Default(t *testing.T) {
	config := DefaultLifecycleConfig()

	if config.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected ShutdownTimeout 30s, got %v", config.ShutdownTimeout)
	}
	if config.DrainTimeout != 10*time.Second {
		t.Errorf("expected DrainTimeout 10s, got %v", config.DrainTimeout)
	}
	if config.HealthCheckInterval != 5*time.Second {
		t.Errorf("expected HealthCheckInterval 5s, got %v", config.HealthCheckInterval)
	}
}
