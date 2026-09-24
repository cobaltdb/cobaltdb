package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type lcTestComp struct {
	name       string
	startDelay time.Duration
	startErr   error
	started    atomic.Bool
	stopped    atomic.Bool
}

func (c *lcTestComp) Name() string { return c.name }

func (c *lcTestComp) Start(ctx context.Context) error {
	if c.startDelay > 0 {
		select {
		case <-time.After(c.startDelay):
		case <-ctx.Done():
			// Mirror a component honoring its context: return after the
			// delay so the timeout fires at the NEXT component's iteration.
		}
	}
	if c.startErr != nil {
		return c.startErr
	}
	c.started.Store(true)
	return nil
}

func (c *lcTestComp) Stop(ctx context.Context) error {
	c.stopped.Store(true)
	return nil
}

func (c *lcTestComp) Health() HealthStatus { return HealthStatus{Healthy: true} }

func lcTestConfig() *LifecycleConfig {
	cfg := DefaultLifecycleConfig()
	cfg.EnableSignalHandling = false
	return cfg
}

// TestStartTimeoutRollsBackStartedComponents pins the contract that a failed
// Start rolls back already-started components on BOTH failure paths.
//
// Regression: Start's component-error path stopped already-started components
// ("Stop already started components"), but its ctx.Done branch returned the
// startup-timeout error WITHOUT any rollback — a timed-out startup left
// half-started components running and the lifecycle wedged in StateStarting.
func TestStartTimeoutRollsBackStartedComponents(t *testing.T) {
	cfg := lcTestConfig()
	cfg.StartupTimeout = 100 * time.Millisecond
	l := NewLifecycle(cfg)
	slow := &lcTestComp{name: "slow", startDelay: 300 * time.Millisecond}
	fast := &lcTestComp{name: "fast"}
	l.RegisterComponent(slow)
	l.RegisterComponent(fast)

	err := l.Start()
	if err == nil {
		t.Fatal("expected a startup timeout error, got nil")
	}
	if !slow.started.Load() {
		t.Fatal("harness broken: slow component not started")
	}
	// The timeout fired at the fast component's iteration (its Start is never
	// invoked on this path — the select's Done branch precedes it), so the
	// rollback question is solely whether the slow component was stopped.
	if !slow.stopped.Load() {
		t.Fatalf("startup timeout left %q running (started=%v stopped=%v) — rollback skipped on the timeout path",
			slow.name, slow.started.Load(), slow.stopped.Load())
	}
}

// TestStartErrorRollsBackStartedComponents is the control pinning the
// pre-existing component-error rollback this contract extends.
func TestStartErrorRollsBackStartedComponents(t *testing.T) {
	cfg := lcTestConfig()
	cfg.StartupTimeout = 2 * time.Second
	l := NewLifecycle(cfg)
	ok1 := &lcTestComp{name: "ok1"}
	bad2 := &lcTestComp{name: "bad2", startErr: context.DeadlineExceeded}
	l.RegisterComponent(ok1)
	l.RegisterComponent(bad2)

	if err := l.Start(); err == nil {
		t.Fatal("expected a component error, got nil")
	}
	if !ok1.stopped.Load() {
		t.Fatal("component-error path did not roll back ok1")
	}
}

// TestCleanStartStartsAllComponents is the control for the happy path.
func TestCleanStartStartsAllComponents(t *testing.T) {
	cfg := lcTestConfig()
	l := NewLifecycle(cfg)
	c1 := &lcTestComp{name: "c1"}
	c2 := &lcTestComp{name: "c2"}
	l.RegisterComponent(c1)
	l.RegisterComponent(c2)

	if err := l.Start(); err != nil {
		t.Fatalf("clean start failed: %v", err)
	}
	if !c1.started.Load() || !c2.started.Load() {
		t.Fatal("clean start did not start all components")
	}
}
