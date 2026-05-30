package integration

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/scheduler"
)

func TestSchedulerAutoVacuumJob(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scheduler_av.db")

	ctx := context.Background()
	opts := &engine.Options{
		CoreStorage: engine.CoreStorage{InMemory: false},
		Maintenance: engine.MaintenanceConfig{
			EnableAutoVacuum:    true,
			AutoVacuumInterval:  100 * time.Millisecond,
			AutoVacuumThreshold: 0.15,
		},
		Scheduler: engine.SchedulerConfig{
			TickInterval: 50 * time.Millisecond,
		},
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE sched_test (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 1; i <= 100; i++ {
		_, _ = db.Exec(ctx, `INSERT INTO sched_test VALUES (?, ?)`, i, "data")
	}
	_, _ = db.Exec(ctx, `DELETE FROM sched_test WHERE id > 50`)

	time.Sleep(400 * time.Millisecond)

	ratio := db.GetCatalog().GetDeadTupleRatio("sched_test")
	if ratio >= 0.15 {
		t.Fatalf("Auto-vacuum did not run: ratio=%.2f", ratio)
	}
}

func TestSchedulerCustomJob(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scheduler_custom.db")

	var count atomic.Int32
	opts := &engine.Options{
		CoreStorage: engine.CoreStorage{InMemory: false},
		Maintenance: engine.MaintenanceConfig{
			EnableAutoVacuum: false,
		},
		Scheduler: engine.SchedulerConfig{
			TickInterval: 50 * time.Millisecond,
		},
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Register a custom job
	customJob := &scheduler.Job{
		ID:       "custom-counter",
		Name:     "Counter",
		Type:     scheduler.JobTypeCustom,
		Interval: 100 * time.Millisecond,
		Enabled:  true,
		Fn: func(ctx context.Context) error {
			count.Add(1)
			return nil
		},
	}

	sched := db.GetScheduler()
	if sched == nil {
		t.Fatal("Scheduler is nil")
	}

	if err := sched.Register(customJob); err != nil {
		t.Fatalf("Failed to register custom job: %v", err)
	}

	waitForIntegrationSchedulerCount(t, &count, 3, time.Second)

	// Disable job and verify it stops
	if !sched.Disable("custom-counter") {
		t.Fatal("failed to disable custom job")
	}
	before := count.Load()
	time.Sleep(300 * time.Millisecond)
	if count.Load() != before {
		t.Fatalf("Expected no runs while disabled, got %d", count.Load())
	}

	// Trigger manually
	if err := sched.Trigger("custom-counter"); err != nil {
		t.Fatalf("Trigger failed: %v", err)
	}
	if count.Load() != before+1 {
		t.Fatalf("Expected one triggered run, got %d", count.Load())
	}
}

func TestSchedulerDisableScheduler(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scheduler_off.db")

	opts := &engine.Options{
		CoreStorage: engine.CoreStorage{InMemory: false},
		Maintenance: engine.MaintenanceConfig{
			EnableAutoVacuum: false,
		},
		Scheduler: engine.SchedulerConfig{
			TickInterval: 0, // disabled
		},
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if db.GetScheduler() != nil {
		t.Fatal("Expected nil scheduler when disabled")
	}
}

func waitForIntegrationSchedulerCount(t *testing.T, count *atomic.Int32, want int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if count.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Expected at least %d runs, got %d", want, count.Load())
}
