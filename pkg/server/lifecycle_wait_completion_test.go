package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// waitProbeComponent records whether its Stop has run.
type waitProbeComponent struct {
	stopped atomic.Bool
}

func (c *waitProbeComponent) Name() string                    { return "wait-probe" }
func (c *waitProbeComponent) Start(ctx context.Context) error { return nil }
func (c *waitProbeComponent) Stop(ctx context.Context) error {
	c.stopped.Store(true)
	return nil
}
func (c *waitProbeComponent) Health() HealthStatus {
	return HealthStatus{Healthy: true}
}

// Regression: Lifecycle.Wait must block until the stop sequence completes.
// It used to block on shutdownCh, which Stop closes as its first action, so
// Wait returned while draining and component stops were still in flight.
// cmd/cobaltdb-server relies on the documented contract ("Wait blocks until
// the server is stopped"): main blocks on Wait and then closes the database,
// which must happen after the components have stopped — otherwise db.Close()
// races servers that are still serving and process exit truncates the
// graceful component stops.
func TestLifecycleWaitBlocksUntilStopCompletes(t *testing.T) {
	config := &LifecycleConfig{
		ShutdownTimeout:      2 * time.Second,
		DrainTimeout:         150 * time.Millisecond,
		HealthCheckInterval:  time.Hour,
		StartupTimeout:       1 * time.Second,
		EnableSignalHandling: false,
	}

	lifecycle := NewLifecycle(config)
	comp := &waitProbeComponent{}
	lifecycle.RegisterComponent(comp)

	if err := lifecycle.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	go func() { _ = lifecycle.Stop() }()

	lifecycle.Wait()

	if !comp.stopped.Load() {
		t.Fatalf("Wait returned while shutdown was still in progress: component Stop had not run (state=%v)", lifecycle.State())
	}
	if lifecycle.State() != StateStopped {
		t.Fatalf("Wait returned before StateStopped (state=%v)", lifecycle.State())
	}
}
