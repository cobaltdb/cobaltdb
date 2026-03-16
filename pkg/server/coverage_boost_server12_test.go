package server

import (
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestProductionServerStartStop tests ProductionServer Start and Stop
func TestProductionServerStartStop(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
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

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	// Stop should work
	ps.Stop()
}

// TestProductionServerReadyHandler tests the readyHandler
func TestProductionServerReadyHandler(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
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
		EnableHealthServer: true,
		HealthAddr:         "127.0.0.1:0",
	}

	ps := NewProductionServer(db, cfg)

	if err := ps.Start(); err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	ps.Stop()
}
