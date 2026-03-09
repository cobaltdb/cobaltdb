package server

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestProductionServerBasic(t *testing.T) {
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

func TestProductionServerWithRetry(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
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

func TestProductionServerWithCircuitBreaker(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
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
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
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
	if config.HealthAddr != ":8420" {
		t.Errorf("expected HealthAddr :8420, got %s", config.HealthAddr)
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
}

func BenchmarkProductionServerStartStop(b *testing.B) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
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
