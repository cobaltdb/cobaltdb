package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// MockComponent is a test component that tracks lifecycle calls
type MockComponent struct {
	name          string
	startErr      error
	stopErr       error
	healthy       bool
	startCalled   atomic.Bool
	stopCalled    atomic.Bool
	healthMessage string
}

func (m *MockComponent) Name() string {
	return m.name
}

func (m *MockComponent) Start(ctx context.Context) error {
	m.startCalled.Store(true)
	return m.startErr
}

func (m *MockComponent) Stop(ctx context.Context) error {
	m.stopCalled.Store(true)
	return m.stopErr
}

func (m *MockComponent) Health() HealthStatus {
	return HealthStatus{
		Healthy: m.healthy,
		Message: m.healthMessage,
	}
}

func TestLifecycleBasic(t *testing.T) {
	config := &LifecycleConfig{
		ShutdownTimeout:      1 * time.Second,
		DrainTimeout:         100 * time.Millisecond,
		HealthCheckInterval:  100 * time.Millisecond,
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	}

	lifecycle := NewLifecycle(config)

	comp := &MockComponent{
		name:          "test-component",
		healthy:       true,
		healthMessage: "test message",
	}
	lifecycle.RegisterComponent(comp)

	// Initial state
	if lifecycle.State() != StateInitializing {
		t.Errorf("expected initial state to be Initializing, got %s", lifecycle.State())
	}

	// Start
	if err := lifecycle.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	if lifecycle.State() != StateRunning {
		t.Errorf("expected state to be Running, got %s", lifecycle.State())
	}

	if !comp.startCalled.Load() {
		t.Error("expected Start to be called on component")
	}

	// Health check
	if !lifecycle.IsHealthy() {
		t.Error("expected lifecycle to be healthy")
	}

	health := lifecycle.GetHealth()
	if len(health) != 1 {
		t.Errorf("expected 1 health entry, got %d", len(health))
	}

	if status, ok := health["test-component"]; !ok {
		t.Error("expected health for test-component")
	} else if !status.Healthy {
		t.Error("expected component to be healthy")
	}

	// Stop
	if err := lifecycle.Stop(); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}

	if lifecycle.State() != StateStopped {
		t.Errorf("expected state to be Stopped, got %s", lifecycle.State())
	}

	if !comp.stopCalled.Load() {
		t.Error("expected Stop to be called on component")
	}
}

func TestLifecycleStartFailure(t *testing.T) {
	config := &LifecycleConfig{
		ShutdownTimeout:      1 * time.Second,
		DrainTimeout:         100 * time.Millisecond,
		HealthCheckInterval:  100 * time.Millisecond,
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	}

	lifecycle := NewLifecycle(config)

	comp1 := &MockComponent{name: "comp1", healthy: true}
	comp2 := &MockComponent{
		name:     "comp2",
		startErr: errors.New("start failed"),
		healthy:  true,
	}

	lifecycle.RegisterComponent(comp1)
	lifecycle.RegisterComponent(comp2)

	err := lifecycle.Start()
	if err == nil {
		t.Fatal("expected start to fail")
	}

	// comp1's Stop should be called to clean up
	if !comp1.stopCalled.Load() {
		t.Error("expected Stop to be called on comp1 for cleanup")
	}
}

func TestLifecycleStateHooks(t *testing.T) {
	config := &LifecycleConfig{
		ShutdownTimeout:      1 * time.Second,
		DrainTimeout:         100 * time.Millisecond,
		HealthCheckInterval:  1 * time.Second, // Long interval so it doesn't interfere
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	}

	lifecycle := NewLifecycle(config)

	var runningCalled atomic.Bool
	lifecycle.OnStateChange(StateRunning, func() {
		runningCalled.Store(true)
	})

	var stoppedCalled atomic.Bool
	lifecycle.OnStateChange(StateStopped, func() {
		stoppedCalled.Store(true)
	})

	comp := &MockComponent{name: "test", healthy: true}
	lifecycle.RegisterComponent(comp)

	lifecycle.Start()
	time.Sleep(100 * time.Millisecond) // Wait for hook

	if !runningCalled.Load() {
		t.Error("expected running hook to be called")
	}

	lifecycle.Stop()
	time.Sleep(100 * time.Millisecond) // Wait for hook

	if !stoppedCalled.Load() {
		t.Error("expected stopped hook to be called")
	}
}

func TestLifecycleHealthCheck(t *testing.T) {
	config := &LifecycleConfig{
		ShutdownTimeout:      1 * time.Second,
		DrainTimeout:         100 * time.Millisecond,
		HealthCheckInterval:  50 * time.Millisecond,
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	}

	lifecycle := NewLifecycle(config)

	healthyComp := &MockComponent{
		name:    "healthy",
		healthy: true,
	}
	unhealthyComp := &MockComponent{
		name:    "unhealthy",
		healthy: false,
		healthMessage: "unhealthy for testing",
	}

	lifecycle.RegisterComponent(healthyComp)
	lifecycle.RegisterComponent(unhealthyComp)

	lifecycle.Start()
	time.Sleep(200 * time.Millisecond) // Wait for health checks
	lifecycle.Stop()

	if lifecycle.IsHealthy() {
		t.Error("expected lifecycle to be unhealthy with unhealthy component")
	}
}

func TestLifecycleWait(t *testing.T) {
	config := &LifecycleConfig{
		ShutdownTimeout:      1 * time.Second,
		DrainTimeout:         100 * time.Millisecond,
		HealthCheckInterval:  1 * time.Second,
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	}

	lifecycle := NewLifecycle(config)
	comp := &MockComponent{name: "test", healthy: true}
	lifecycle.RegisterComponent(comp)

	lifecycle.Start()

	done := make(chan bool)
	go func() {
		lifecycle.Wait()
		done <- true
	}()

	// Stop should trigger Wait to return
	go func() {
		time.Sleep(100 * time.Millisecond)
		lifecycle.Stop()
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("Wait did not return after Stop")
	}
}

func TestLifecycleRegisterHealthCheck(t *testing.T) {
	lifecycle := NewLifecycle(nil)

	healthy := true
	lifecycle.RegisterHealthCheck("custom", func() HealthStatus {
		if healthy {
			return HealthStatus{Healthy: true, Message: "ok"}
		}
		return HealthStatus{Healthy: false, Message: "not ok"}
	})

	health := lifecycle.GetHealth()
	if status, ok := health["custom"]; !ok {
		t.Error("expected custom health check")
	} else if !status.Healthy {
		t.Error("expected custom to be healthy")
	}

	// Make it unhealthy
	healthy = false
	health = lifecycle.GetHealth()
	if status := health["custom"]; status.Healthy {
		t.Error("expected custom to be unhealthy")
	}
}

func TestDBComponent(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	comp := NewDBComponent("test-db", db)

	// Name
	if comp.Name() != "test-db" {
		t.Errorf("Expected name 'test-db', got %q", comp.Name())
	}

	// Health before start
	health := comp.Health()
	if !health.Healthy {
		t.Errorf("Expected healthy, got: %s", health.Message)
	}

	// Start
	if err := comp.Start(context.Background()); err != nil {
		t.Errorf("Start failed: %v", err)
	}

	// Health after start
	health = comp.Health()
	if !health.Healthy {
		t.Errorf("Expected healthy after start, got: %s", health.Message)
	}

	// Stop
	if err := comp.Stop(context.Background()); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

// TrackingComponent wraps a component and tracks lifecycle calls
type TrackingComponent struct {
	name       string
	startOrder *[]string
	stopOrder  *[]string
	mu         *sync.Mutex
	healthy    bool
}

func (t *TrackingComponent) Name() string {
	return t.name
}

func (t *TrackingComponent) Start(ctx context.Context) error {
	t.mu.Lock()
	*t.startOrder = append(*t.startOrder, t.name)
	t.mu.Unlock()
	return nil
}

func (t *TrackingComponent) Stop(ctx context.Context) error {
	t.mu.Lock()
	*t.stopOrder = append(*t.stopOrder, t.name)
	t.mu.Unlock()
	return nil
}

func (t *TrackingComponent) Health() HealthStatus {
	return HealthStatus{Healthy: t.healthy, Message: "ok"}
}

func TestMultipleComponents(t *testing.T) {
	config := &LifecycleConfig{
		ShutdownTimeout:      1 * time.Second,
		DrainTimeout:         50 * time.Millisecond,
		HealthCheckInterval:  1 * time.Second,
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	}

	lifecycle := NewLifecycle(config)

	var startOrder []string
	var stopOrder []string
	var mu sync.Mutex

	for i := 0; i < 3; i++ {
		name := string('A' + byte(i))
		comp := &TrackingComponent{
			name:       name,
			startOrder: &startOrder,
			stopOrder:  &stopOrder,
			mu:         &mu,
			healthy:    true,
		}
		lifecycle.RegisterComponent(comp)
	}

	lifecycle.Start()
	lifecycle.Stop()

	// Components should start in order A, B, C
	expectedStart := []string{"A", "B", "C"}
	if len(startOrder) != 3 {
		t.Fatalf("expected 3 start calls, got %d", len(startOrder))
	}
	for i, name := range startOrder {
		if name != expectedStart[i] {
			t.Errorf("start order: expected %s at position %d, got %s", expectedStart[i], i, name)
		}
	}

	// Components should stop in reverse order C, B, A
	expectedStop := []string{"C", "B", "A"}
	if len(stopOrder) != 3 {
		t.Fatalf("expected 3 stop calls, got %d", len(stopOrder))
	}
	for i, name := range stopOrder {
		if name != expectedStop[i] {
			t.Errorf("stop order: expected %s at position %d, got %s", expectedStop[i], i, name)
		}
	}
}

func BenchmarkLifecycleStartStop(b *testing.B) {
	config := &LifecycleConfig{
		ShutdownTimeout:      1 * time.Second,
		DrainTimeout:         1 * time.Millisecond,
		HealthCheckInterval:  1 * time.Hour, // Disable health checks
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lifecycle := NewLifecycle(config)
		comp := &MockComponent{name: "bench", healthy: true}
		lifecycle.RegisterComponent(comp)

		lifecycle.Start()
		lifecycle.Stop()
	}
}
