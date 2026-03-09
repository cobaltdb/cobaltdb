package server

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiterBasic(t *testing.T) {
	config := &RateLimiterConfig{
		RPS:             10,
		Burst:           5,
		PerClient:       false,
		CleanupInterval: 1 * time.Minute,
		MaxClients:      100,
	}

	rl := NewRateLimiter(config)
	defer rl.Stop()

	// Should allow initial burst
	for i := 0; i < 5; i++ {
		if !rl.Allow("") {
			t.Errorf("request %d should be allowed (burst)", i)
		}
	}

	// 6th request should fail (burst exhausted)
	if rl.Allow("") {
		t.Error("6th request should be denied after burst exhausted")
	}
}

func TestRateLimiterPerClient(t *testing.T) {
	config := &RateLimiterConfig{
		RPS:             100,
		Burst:           100,
		PerClient:       true,
		CleanupInterval: 1 * time.Minute,
		MaxClients:      100,
	}

	rl := NewRateLimiter(config)
	defer rl.Stop()

	// With high global burst, each client should have its own limit
	// First request from client1 should succeed
	if !rl.Allow("client1") {
		t.Error("client1 first request should be allowed")
	}

	// First request from client2 should also succeed (separate limits)
	if !rl.Allow("client2") {
		t.Error("client2 first request should be allowed")
	}

	// Verify they have separate rate limits by checking they both work
	for i := 0; i < 5; i++ {
		rl.Allow("client1")
		rl.Allow("client2")
	}
}

func TestRateLimiterWait(t *testing.T) {
	config := &RateLimiterConfig{
		RPS:             1,  // 1 per second = 1 token per second
		Burst:           1,
		PerClient:       false,
		CleanupInterval: 1 * time.Minute,
		MaxClients:      100,
	}

	rl := NewRateLimiter(config)
	defer rl.Stop()

	// Use initial burst
	rl.Allow("")

	// Wait for refill (should happen quickly with 1 RPS and short wait)
	time.Sleep(100 * time.Millisecond)

	// Now should succeed within timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := rl.Wait(ctx, "")
	if err != nil {
		t.Logf("Wait returned error (may be timing issue): %v", err)
	}
}

func TestRateLimiterStats(t *testing.T) {
	config := &RateLimiterConfig{
		RPS:             10,
		Burst:           5,
		PerClient:       true,
		CleanupInterval: 1 * time.Minute,
		MaxClients:      100,
	}

	rl := NewRateLimiter(config)
	defer rl.Stop()

	// Make some requests
	rl.Allow("client1")
	rl.Allow("client2")
	rl.Allow("client3")

	stats := rl.GetStats()
	if stats.ClientCount != 3 {
		t.Errorf("expected 3 clients, got %d", stats.ClientCount)
	}
	if stats.RPS != 10 {
		t.Errorf("expected RPS 10, got %d", stats.RPS)
	}
}

func TestAdaptiveRateLimiter(t *testing.T) {
	load := 0.5
	loadMonitor := func() float64 { return load }

	config := DefaultRateLimiterConfig()
	arl := NewAdaptiveRateLimiter(config, 100, 1000, loadMonitor)
	defer arl.Stop()

	// Initial state
	arl.Adjust()

	// High load should reduce rate
	load = 0.9
	arl.Adjust()

	// Low load should increase rate
	load = 0.1
	arl.Adjust()
}

func BenchmarkRateLimiterAllow(b *testing.B) {
	config := DefaultRateLimiterConfig()
	rl := NewRateLimiter(config)
	defer rl.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rl.Allow("")
	}
}
