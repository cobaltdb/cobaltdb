package storage

import (
	"os"
	"testing"
	"time"
)

func TestGroupCommitBasic(t *testing.T) {
	tmpFile := "/tmp/test_group_commit_wal.db"
	defer os.Remove(tmpFile)

	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Create group committer
	config := &GroupCommitConfig{
		Enabled:      true,
		MaxWaitTime:  10 * time.Millisecond,
		MaxBatchSize: 100,
		MinBatchSize: 5,
	}
	gc := NewGroupCommitter(wal, config)
	defer gc.Stop()

	// Submit a single commit
	if err := gc.SubmitCommit(1); err != nil {
		t.Errorf("Failed to submit commit: %v", err)
	}

	// Check stats
	stats := gc.GetStats()
	if !stats.Enabled {
		t.Error("Group commit should be enabled")
	}
}

func TestGroupCommitDisabled(t *testing.T) {
	tmpFile := "/tmp/test_group_commit_disabled.db"
	defer os.Remove(tmpFile)

	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Create group committer with disabled config
	config := &GroupCommitConfig{
		Enabled: false,
	}
	gc := NewGroupCommitter(wal, config)
	defer gc.Stop()

	// Submit commit (should work but without batching)
	if err := gc.SubmitCommit(1); err != nil {
		t.Errorf("Failed to submit commit: %v", err)
	}
}

func TestGroupCommitTimeout(t *testing.T) {
	tmpFile := "/tmp/test_group_commit_timeout.db"
	defer os.Remove(tmpFile)

	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Create group committer with short timeout
	config := &GroupCommitConfig{
		Enabled:      true,
		MaxWaitTime:  50 * time.Millisecond,
		MaxBatchSize: 100,
		MinBatchSize: 50, // High min batch size to force timeout
	}
	gc := NewGroupCommitter(wal, config)
	defer gc.Stop()

	// Submit 3 commits synchronously (won't reach MinBatchSize, so timeout will trigger)
	for i := 0; i < 3; i++ {
		if err := gc.SubmitCommit(uint64(i + 1)); err != nil {
			t.Logf("Commit error: %v", err)
		}
	}

	// Wait for timeout to trigger flush
	time.Sleep(100 * time.Millisecond)

	// Check stats
	stats := gc.GetStats()
	if stats.TotalCommits != 3 {
		t.Errorf("Expected 3 commits, got %d", stats.TotalCommits)
	}
}

func TestWALWithGroupCommit(t *testing.T) {
	tmpFile := "/tmp/test_wal_gc.db"
	defer os.Remove(tmpFile)

	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Wrap with group commit
	config := &GroupCommitConfig{
		Enabled:      true,
		MaxWaitTime:  10 * time.Millisecond,
		MaxBatchSize: 100,
		MinBatchSize: 5,
	}
	walGC := NewWALWithGroupCommit(wal, config)

	// Append a commit record
	record := &WALRecord{
		TxnID: 1,
		Type:  WALCommit,
	}

	if err := walGC.AppendCommit(record); err != nil {
		t.Fatalf("Failed to append commit: %v", err)
	}

	// Close
	if err := walGC.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Check stats
	stats := walGC.GetGroupCommitStats()
	if stats.TotalCommits != 1 {
		t.Errorf("Expected 1 commit, got %d", stats.TotalCommits)
	}
}

func TestGroupCommitStats(t *testing.T) {
	tmpFile := "/tmp/test_gc_stats.db"
	defer os.Remove(tmpFile)

	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	config := DefaultGroupCommitConfig()
	gc := NewGroupCommitter(wal, config)
	defer gc.Stop()

	// Get initial stats
	stats := gc.GetStats()
	if stats.Enabled != true {
		t.Error("Group commit should be enabled")
	}
	if stats.MaxWaitTime != 5*time.Millisecond {
		t.Errorf("Expected 5ms wait time, got %v", stats.MaxWaitTime)
	}
}
