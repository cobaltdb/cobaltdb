package server

import (
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestProductionServerWait tests the Wait method
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

// TestLifecycleWait tests Lifecycle.Wait directly
func TestLifecycleWaitCoverage(t *testing.T) {
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
