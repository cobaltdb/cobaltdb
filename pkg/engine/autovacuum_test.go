package engine

import (
	"context"
	"testing"
	"time"
)

func TestDefaultAutoVacuumConfig(t *testing.T) {
	config := DefaultAutoVacuumConfig()

	if !config.Enabled {
		t.Error("Expected auto-vacuum to be enabled by default")
	}

	if config.VacuumThreshold != 50 {
		t.Errorf("Expected vacuum threshold 50, got %d", config.VacuumThreshold)
	}

	if config.VacuumScaleFactor != 0.2 {
		t.Errorf("Expected vacuum scale factor 0.2, got %f", config.VacuumScaleFactor)
	}

	if config.MaxWorkers != 3 {
		t.Errorf("Expected max workers 3, got %d", config.MaxWorkers)
	}
}

func TestTableStatsNeedsVacuum(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	stats := &TableStats{
		TableName:  "test_table",
		RelTuples:  1000,
		DeadTuples: 0,
	}

	// Should not need vacuum initially
	if stats.NeedsVacuum(config) {
		t.Error("Should not need vacuum initially")
	}

	// Add enough dead tuples to trigger vacuum
	// Threshold = 50 + 0.2 * 1000 = 250
	stats.UpdateDeadTuples(300)
	if !stats.NeedsVacuum(config) {
		t.Error("Should need vacuum after adding dead tuples")
	}
}

func TestTableStatsNeedsAnalyze(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	stats := &TableStats{
		TableName: "test_table",
		RelTuples: 1000,
	}

	// Should not need analyze initially
	if stats.NeedsAnalyze(config) {
		t.Error("Should not need analyze initially")
	}

	// Add enough modifications to trigger analyze
	// Threshold = 50 + 0.1 * 1000 = 150
	stats.UpdateModifications(200)
	if !stats.NeedsAnalyze(config) {
		t.Error("Should need analyze after modifications")
	}
}

func TestTableStatsNeedsFreeze(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	stats := &TableStats{
		TableName: "test_table",
		FrozenXID: 100,
	}

	currentXID := int64(200)

	// Should not need freeze (age = 100 < 150000000)
	if stats.NeedsFreeze(config, currentXID) {
		t.Error("Should not need freeze with low age")
	}

	// Set high age
	stats.FrozenXID = currentXID - int64(config.VacuumFreezeTableAge) - 1
	if !stats.NeedsFreeze(config, currentXID) {
		t.Error("Should need freeze with high age")
	}
}

func TestTableStatsRecordVacuum(t *testing.T) {
	stats := &TableStats{
		TableName:                "test_table",
		DeadTuples:               100,
		ModificationsSinceVacuum: 50,
	}

	duration := 100 * time.Millisecond
	stats.RecordVacuum(duration)

	if stats.DeadTuples != 0 {
		t.Errorf("Expected dead tuples to be reset, got %d", stats.DeadTuples)
	}

	if stats.ModificationsSinceVacuum != 0 {
		t.Errorf("Expected modifications to be reset, got %d", stats.ModificationsSinceVacuum)
	}

	if stats.VacuumCount != 1 {
		t.Errorf("Expected vacuum count 1, got %d", stats.VacuumCount)
	}

	if stats.LastVacuumDuration != duration {
		t.Errorf("Expected duration %v, got %v", duration, stats.LastVacuumDuration)
	}
}

func TestTableStatsRecordAnalyze(t *testing.T) {
	stats := &TableStats{
		TableName:                 "test_table",
		ModificationsSinceAnalyze: 100,
	}

	duration := 50 * time.Millisecond
	stats.RecordAnalyze(duration)

	if stats.ModificationsSinceAnalyze != 0 {
		t.Errorf("Expected modifications to be reset, got %d", stats.ModificationsSinceAnalyze)
	}

	if stats.AnalyzeCount != 1 {
		t.Errorf("Expected analyze count 1, got %d", stats.AnalyzeCount)
	}
}

func TestAutoVacuumRegisterTable(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false // Don't start the daemon
	av := NewAutoVacuum(config, nil)

	av.RegisterTable("users", 10000)

	stats, ok := av.GetStats()["users"]
	if !ok {
		t.Fatal("Expected users table to be registered")
	}

	stats.mu.RLock()
	relTuples := stats.RelTuples
	stats.mu.RUnlock()

	if relTuples != 10000 {
		t.Errorf("Expected 10000 rows, got %d", relTuples)
	}
}

func TestAutoVacuumUnregisterTable(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false
	av := NewAutoVacuum(config, nil)

	av.RegisterTable("users", 1000)
	av.UnregisterTable("users")

	_, ok := av.GetStats()["users"]
	if ok {
		t.Error("Expected users table to be unregistered")
	}
}

func TestAutoVacuumTrackModification(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false
	av := NewAutoVacuum(config, nil)

	av.RegisterTable("users", 1000)
	av.TrackModification("users", 5, 3, 2) // 5 deleted, 3 updated, 2 inserted

	stats := av.getTableStats("users")

	stats.mu.RLock()
	deadTuples := stats.DeadTuples
	modifications := stats.ModificationsSinceAnalyze
	stats.mu.RUnlock()

	if deadTuples != 8 { // deleted + updated
		t.Errorf("Expected 8 dead tuples, got %d", deadTuples)
	}

	if modifications != 10 { // total changes
		t.Errorf("Expected 10 modifications, got %d", modifications)
	}
}

func TestAutoVacuumVacuumTable(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false
	av := NewAutoVacuum(config, nil)

	av.RegisterTable("users", 1000)

	ctx := context.Background()
	err := av.VacuumTable(ctx, "users")
	if err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}

	stats := av.getTableStats("users")

	stats.mu.RLock()
	vacuumCount := stats.VacuumCount
	stats.mu.RUnlock()

	if vacuumCount != 1 {
		t.Errorf("Expected vacuum count 1, got %d", vacuumCount)
	}
}

func TestAutoVacuumAnalyzeTable(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false
	av := NewAutoVacuum(config, nil)

	av.RegisterTable("users", 1000)

	ctx := context.Background()
	err := av.AnalyzeTable(ctx, "users")
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	stats := av.getTableStats("users")

	stats.mu.RLock()
	analyzeCount := stats.AnalyzeCount
	stats.mu.RUnlock()

	if analyzeCount != 1 {
		t.Errorf("Expected analyze count 1, got %d", analyzeCount)
	}
}

func TestAutoVacuumVacuumCancellation(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false
	av := NewAutoVacuum(config, nil)

	av.RegisterTable("users", 1000)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := av.VacuumTable(ctx, "users")
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestAutoVacuumDisabled(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false
	config.TrackCounts = false
	av := NewAutoVacuum(config, nil)

	av.RegisterTable("users", 1000)

	// Track modification should be no-op when disabled
	av.TrackModification("users", 100, 100, 100)

	stats := av.getTableStats("users")

	stats.mu.RLock()
	modifications := stats.ModificationsSinceVacuum
	stats.mu.RUnlock()

	if modifications != 0 {
		t.Error("Expected modifications to be 0 when tracking is disabled")
	}
}

func TestVacuumWorkerPool(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false
	av := NewAutoVacuum(config, nil)
	av.workers.start(av)

	// Submit some jobs
	av.RegisterTable("table1", 1000)
	av.RegisterTable("table2", 1000)

	// Manually trigger vacuum through worker pool
	job1 := vacuumJob{tableName: "table1", jobType: "vacuum", stats: av.getTableStats("table1")}
	job2 := vacuumJob{tableName: "table2", jobType: "analyze", stats: av.getTableStats("table2")}

	av.workers.submit(job1)
	av.workers.submit(job2)

	// Give workers time to process
	time.Sleep(100 * time.Millisecond)

	av.workers.stop()
}

func TestAutoVacuumStartStop(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.AutoVacuumInterval = 100 * time.Millisecond
	av := NewAutoVacuum(config, nil)

	// Start
	av.Start()
	if !av.running.Load() {
		t.Error("Expected auto-vacuum to be running")
	}

	// Let it run for a bit
	time.Sleep(200 * time.Millisecond)

	// Stop
	av.Stop()
	if av.running.Load() {
		t.Error("Expected auto-vacuum to be stopped")
	}
}

func TestAutoVacuumMetrics(t *testing.T) {
	config := DefaultAutoVacuumConfig()
	config.Enabled = false
	av := NewAutoVacuum(config, nil)

	metrics := av.GetMetrics()

	if metrics.CurrentWorkers != config.MaxWorkers {
		t.Errorf("Expected %d workers, got %d", config.MaxWorkers, metrics.CurrentWorkers)
	}
}

func TestVacuumProgress(t *testing.T) {
	progress := &VacuumProgress{
		TableName:       "users",
		Phase:           "scanning",
		HeapBlksTotal:   100,
		HeapBlksScanned: 50,
		StartTime:       time.Now(),
	}

	if progress.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", progress.TableName)
	}

	if progress.Phase != "scanning" {
		t.Errorf("Expected phase 'scanning', got '%s'", progress.Phase)
	}
}

func TestVacuumReport(t *testing.T) {
	report := &VacuumReport{
		TableName:         "users",
		Duration:          100 * time.Millisecond,
		PagesScanned:      100,
		PagesVacuumed:     95,
		TuplesDeleted:     50,
		TuplesFrozen:      10,
		IndexPagesCleaned: 20,
	}

	if report.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", report.TableName)
	}

	if report.PagesScanned != 100 {
		t.Errorf("Expected 100 pages scanned, got %d", report.PagesScanned)
	}
}
