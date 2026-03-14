package server

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestStateChangeCallback targets OnStateChange callback
func TestStateChangeCallback(t *testing.T) {
	config := DefaultLifecycleConfig()
	lc := NewLifecycle(config)

	callbackInvoked := false
	lc.OnStateChange(StateRunning, func() {
		callbackInvoked = true
	})

	component := &testComponent{name: "test3"}
	lc.RegisterComponent(component)

	if err := lc.Start(); err != nil {
		t.Fatalf("Failed to start lifecycle: %v", err)
	}

	// Give time for state change
	time.Sleep(100 * time.Millisecond)

	if err := lc.Stop(); err != nil {
		t.Fatalf("Failed to stop lifecycle: %v", err)
	}

	t.Logf("Callback invoked: %v", callbackInvoked)
}

// TestCheckHealthFailure targets checkHealth with failing component
func TestCheckHealthFailure(t *testing.T) {
	config := DefaultLifecycleConfig()
	lc := NewLifecycle(config)

	// Register unhealthy component
	unhealthy := &unhealthyComponent{name: "unhealthy"}
	lc.RegisterComponent(unhealthy)

	if err := lc.Start(); err != nil {
		t.Fatalf("Failed to start lifecycle: %v", err)
	}

	// Wait for health check to run
	time.Sleep(150 * time.Millisecond)

	if lc.IsHealthy() {
		t.Error("Expected lifecycle to be unhealthy")
	}

	health := lc.GetHealth()
	hasUnhealthy := false
	for _, status := range health {
		if !status.Healthy {
			hasUnhealthy = true
			break
		}
	}
	if !hasUnhealthy {
		t.Error("Expected health to report at least one unhealthy component")
	}

	lc.Stop()
}

// TestLiveCheckHandler targets LiveCheck HTTP handler
func TestLiveCheckHandler(t *testing.T) {
	config := DefaultLifecycleConfig()
	lc := NewLifecycle(config)

	component := &testComponent{name: "test"}
	lc.RegisterComponent(component)

	if err := lc.Start(); err != nil {
		t.Fatalf("Failed to start lifecycle: %v", err)
	}

	// Wait for startup
	time.Sleep(100 * time.Millisecond)

	// LiveCheck returns a handler - just verify it doesn't panic
	handler := lc.LiveCheck()
	if handler == nil {
		t.Error("LiveCheck should return a handler")
	}

	lc.Stop()
}

// TestGracefulShutdownTimeout targets timeout path in shutdown
func TestGracefulShutdownTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Signal handling tests skipped on Windows")
	}

	config := DefaultLifecycleConfig()
	config.ShutdownTimeout = 50 * time.Millisecond // Very short timeout

	lc := NewLifecycle(config)

	// Register slow component
	slowComponent := &slowComponent{name: "slow"}
	lc.RegisterComponent(slowComponent)

	if err := lc.Start(); err != nil {
		t.Fatalf("Failed to start lifecycle: %v", err)
	}

	// Stop will trigger timeout
	start := time.Now()
	if err := lc.Stop(); err != nil {
		t.Logf("Stop returned error (expected due to timeout): %v", err)
	}
	elapsed := time.Since(start)

	// Should have timed out quickly
	if elapsed > 200*time.Millisecond {
		t.Errorf("Shutdown took too long: %v", elapsed)
	}
}

// TestSetStateTransitions targets setState with various states
func TestSetStateTransitions(t *testing.T) {
	config := DefaultLifecycleConfig()
	lc := NewLifecycle(config)

	// Test all state transitions
	states := []LifecycleState{
		StateInitializing,
		StateStarting,
		StateRunning,
		StateDraining,
		StateShuttingDown,
		StateStopped,
	}

	for _, state := range states {
		lc.setState(state)
		if lc.State() != state {
			t.Errorf("Expected state %v, got %v", state, lc.State())
		}
	}
}

// TestRegisterHealthCheck targets health check registration
func TestRegisterHealthCheck(t *testing.T) {
	config := DefaultLifecycleConfig()
	lc := NewLifecycle(config)

	lc.RegisterHealthCheck("test-check", func() HealthStatus {
		return HealthStatus{Healthy: true}
	})

	component := &testComponent{name: "test"}
	lc.RegisterComponent(component)

	if err := lc.Start(); err != nil {
		t.Fatalf("Failed to start lifecycle: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	health := lc.GetHealth()
	if _, ok := health["test-check"]; !ok {
		t.Error("Expected registered health check to appear in health status")
	}

	lc.Stop()
}

// Helper types
type slowComponent struct {
	name string
}

func (c *slowComponent) Name() string                     { return c.name }
func (c *slowComponent) Start(ctx context.Context) error  { return nil }
func (c *slowComponent) Health() HealthStatus             { return HealthStatus{Healthy: true} }
func (c *slowComponent) Stop(ctx context.Context) error {
	time.Sleep(500 * time.Millisecond) // Slower than timeout
	return nil
}

type unhealthyComponent struct {
	name string
}

func (c *unhealthyComponent) Name() string                    { return c.name }
func (c *unhealthyComponent) Start(ctx context.Context) error { return nil }
func (c *unhealthyComponent) Stop(ctx context.Context) error  { return nil }
func (c *unhealthyComponent) Health() HealthStatus {
	return HealthStatus{Healthy: false, Message: "unhealthy"}
}

type testComponent struct {
	name string
}

func (c *testComponent) Name() string                     { return c.name }
func (c *testComponent) Start(ctx context.Context) error  { return nil }
func (c *testComponent) Stop(ctx context.Context) error   { return nil }
func (c *testComponent) Health() HealthStatus             { return HealthStatus{Healthy: true} }
