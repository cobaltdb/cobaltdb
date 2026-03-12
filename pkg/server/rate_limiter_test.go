package server

import (
	"context"
	"testing"
	"time"
)

func TestDefaultRateLimiterConfig(t *testing.T) {
	cfg := DefaultRateLimiterConfig()
	if cfg.RPS != 1000 {
		t.Errorf("expected RPS=1000, got %d", cfg.RPS)
	}
	if cfg.Burst != 100 {
		t.Errorf("expected Burst=100, got %d", cfg.Burst)
	}
	if !cfg.PerClient {
		t.Error("expected PerClient=true")
	}
	if cfg.MaxClients != 10000 {
		t.Errorf("expected MaxClients=10000, got %d", cfg.MaxClients)
	}
}

func TestNewRateLimiter(t *testing.T) {
	rl := NewRateLimiter(nil)
	defer rl.Stop()

	if rl.config.RPS != 1000 {
		t.Errorf("nil config should use defaults, got RPS=%d", rl.config.RPS)
	}
}

func TestRateLimiterAllow(t *testing.T) {
	cfg := &RateLimiterConfig{
		RPS:             100,
		Burst:           10,
		PerClient:       false,
		CleanupInterval: time.Minute,
		MaxClients:      100,
	}
	rl := NewRateLimiter(cfg)
	defer rl.Stop()

	// First 10 requests should be allowed (burst)
	for i := 0; i < 10; i++ {
		if !rl.Allow("") {
			t.Errorf("request %d should be allowed within burst", i)
		}
	}

	// 11th should be rejected (burst exhausted)
	if rl.Allow("") {
		t.Error("request beyond burst should be rejected")
	}
}

func TestRateLimiterAllowPerClient(t *testing.T) {
	cfg := &RateLimiterConfig{
		RPS:             1000,
		Burst:           100,
		PerClient:       true,
		CleanupInterval: time.Minute,
		MaxClients:      100,
	}
	rl := NewRateLimiter(cfg)
	defer rl.Stop()

	// Per-client burst is 1/10th of global = 10
	for i := 0; i < 10; i++ {
		if !rl.Allow("client1") {
			t.Errorf("client1 request %d should be allowed", i)
		}
	}

	// client1 should be throttled
	if rl.Allow("client1") {
		t.Error("client1 beyond burst should be rejected")
	}

	// client2 should still be allowed (separate limiter)
	if !rl.Allow("client2") {
		t.Error("client2 first request should be allowed")
	}
}

func TestRateLimiterAllowN(t *testing.T) {
	cfg := &RateLimiterConfig{
		RPS:             100,
		Burst:           10,
		PerClient:       false,
		CleanupInterval: time.Minute,
		MaxClients:      100,
	}
	rl := NewRateLimiter(cfg)
	defer rl.Stop()

	// AllowN(5) twice should work (burst=10)
	if !rl.AllowN("", 5) {
		t.Error("first AllowN(5) should succeed")
	}
	if !rl.AllowN("", 5) {
		t.Error("second AllowN(5) should succeed")
	}
	// Third should fail
	if rl.AllowN("", 5) {
		t.Error("third AllowN(5) should fail (burst exhausted)")
	}
}

func TestRateLimiterWait(t *testing.T) {
	cfg := &RateLimiterConfig{
		RPS:             1000,
		Burst:           5,
		PerClient:       false,
		CleanupInterval: time.Minute,
		MaxClients:      100,
	}
	rl := NewRateLimiter(cfg)
	defer rl.Stop()

	// Exhaust burst
	for i := 0; i < 5; i++ {
		rl.Allow("")
	}

	// Wait with short timeout should fail
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	err := rl.Wait(ctx, "")
	if err == nil {
		t.Error("expected context deadline error")
	}
}

func TestRateLimiterWaitSuccess(t *testing.T) {
	cfg := &RateLimiterConfig{
		RPS:             10000, // High rate so tokens refill quickly
		Burst:           1,
		PerClient:       false,
		CleanupInterval: time.Minute,
		MaxClients:      100,
	}
	rl := NewRateLimiter(cfg)
	defer rl.Stop()

	// First request uses the token
	rl.Allow("")

	// Wait should succeed quickly with high RPS
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := rl.Wait(ctx, "")
	if err != nil {
		t.Errorf("Wait should succeed with high RPS: %v", err)
	}
}

func TestRateLimiterGetStats(t *testing.T) {
	cfg := &RateLimiterConfig{
		RPS:             500,
		Burst:           50,
		PerClient:       true,
		CleanupInterval: time.Minute,
		MaxClients:      100,
	}
	rl := NewRateLimiter(cfg)
	defer rl.Stop()

	// Generate some client activity
	rl.Allow("client1")
	rl.Allow("client2")

	stats := rl.GetStats()
	if stats.RPS != 500 {
		t.Errorf("expected RPS=500, got %d", stats.RPS)
	}
	if stats.Burst != 50 {
		t.Errorf("expected Burst=50, got %d", stats.Burst)
	}
	if stats.ClientCount != 2 {
		t.Errorf("expected 2 clients, got %d", stats.ClientCount)
	}
}

func TestRateLimiterMaxClients(t *testing.T) {
	cfg := &RateLimiterConfig{
		RPS:             1000,
		Burst:           100,
		PerClient:       true,
		CleanupInterval: time.Minute,
		MaxClients:      3,
	}
	rl := NewRateLimiter(cfg)
	defer rl.Stop()

	// Create max clients
	rl.Allow("c1")
	rl.Allow("c2")
	rl.Allow("c3")

	// 4th client should fall back to global limiter (no panic)
	if !rl.Allow("c4") {
		t.Error("4th client should still be allowed (global fallback)")
	}

	stats := rl.GetStats()
	if stats.ClientCount != 3 {
		t.Errorf("should cap at 3 clients, got %d", stats.ClientCount)
	}
}

func TestRateLimiterStop(t *testing.T) {
	rl := NewRateLimiter(nil)
	// Stop should not panic, even called multiple times
	rl.Stop()
	rl.Stop()
}

func TestRateLimiterCleanup(t *testing.T) {
	cfg := &RateLimiterConfig{
		RPS:             1000,
		Burst:           100,
		PerClient:       true,
		CleanupInterval: 10 * time.Millisecond,
		MaxClients:      100,
	}
	rl := NewRateLimiter(cfg)
	defer rl.Stop()

	// Create a client
	rl.Allow("stale_client")
	stats := rl.GetStats()
	if stats.ClientCount != 1 {
		t.Fatalf("expected 1 client, got %d", stats.ClientCount)
	}

	// Wait for cleanup interval to pass + cleanup to run
	time.Sleep(50 * time.Millisecond)

	stats = rl.GetStats()
	if stats.ClientCount != 0 {
		t.Errorf("stale client should be cleaned up, got %d clients", stats.ClientCount)
	}
}
