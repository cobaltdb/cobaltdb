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
		InMemory:              false,
		EnableAutoVacuum:      true,
		AutoVacuumInterval:    100 * time.Millisecond,
		AutoVacuumThreshold:   0.15,
		SchedulerTickInterval: 50 * time.Millisecond,
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
		InMemory:              false,
		EnableScheduler:       true,
		EnableAutoVacuum:      false,
		SchedulerTickInterval: 50 * time.Millisecond,
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

	time.Sleep(350 * time.Millisecond)

	if count.Load() < 2 {
		t.Fatalf("Expected at least 2 runs, got %d", count.Load())
	}

	// Disable job and verify it stops
	sched.Disable("custom-counter")
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
		InMemory:         false,
		EnableScheduler:  false,
		EnableAutoVacuum: false,
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
