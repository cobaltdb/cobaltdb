package metrics

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSlowQueryLogBasic(t *testing.T) {
	sql := NewSlowQueryLog(true, 100*time.Millisecond, 100, "")

	// Log a slow query
	sql.Log("SELECT * FROM users", 200*time.Millisecond, 0, 10)

	entries := sql.GetEntries(10)
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].SQL != "SELECT * FROM users" {
		t.Errorf("Expected SQL 'SELECT * FROM users', got '%s'", entries[0].SQL)
	}

	if entries[0].Duration != 200*time.Millisecond {
		t.Errorf("Expected duration 200ms, got %v", entries[0].Duration)
	}
}

func TestSlowQueryLogThreshold(t *testing.T) {
	sql := NewSlowQueryLog(true, 100*time.Millisecond, 100, "")

	// Log a fast query (should not be recorded)
	sql.Log("SELECT 1", 50*time.Millisecond, 0, 1)

	// Log a slow query (should be recorded)
	sql.Log("SELECT * FROM large_table", 150*time.Millisecond, 0, 100)

	entries := sql.GetEntries(10)
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry (only slow query), got %d", len(entries))
	}

	if entries[0].SQL != "SELECT * FROM large_table" {
		t.Errorf("Expected slow query SQL, got '%s'", entries[0].SQL)
	}
}

func TestSlowQueryLogDisabled(t *testing.T) {
	sql := NewSlowQueryLog(false, 100*time.Millisecond, 100, "")

	// Try to log when disabled
	sql.Log("SELECT * FROM users", 500*time.Millisecond, 0, 10)

	entries := sql.GetEntries(10)
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries when disabled, got %d", len(entries))
	}
}

func TestSlowQueryLogMaxEntries(t *testing.T) {
	sql := NewSlowQueryLog(true, 1*time.Millisecond, 5, "")

	// Log more entries than max
	for i := 0; i < 10; i++ {
		sql.Log("SELECT * FROM users", 10*time.Millisecond, 0, 1)
	}

	entries := sql.GetEntries(10)
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries (max), got %d", len(entries))
	}
}

func TestSlowQueryLogFile(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "slow_queries.log")

	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, logFile)

	// Log some queries
	sql.Log("SELECT * FROM users", 100*time.Millisecond, 0, 10)
	sql.Log("UPDATE users SET name = 'test'", 200*time.Millisecond, 5, 0)

	// Give file write time
	time.Sleep(10 * time.Millisecond)

	// Check file exists and contains entries
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if len(data) == 0 {
		t.Error("Log file is empty")
	}
}

func TestSlowQueryLogStats(t *testing.T) {
	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, "")

	// Log some queries
	sql.Log("SELECT 1", 100*time.Millisecond, 0, 1)
	sql.Log("SELECT 2", 200*time.Millisecond, 0, 1)
	sql.Log("SELECT 3", 300*time.Millisecond, 0, 1)

	total, avg := sql.GetStats()
	if total != 3 {
		t.Errorf("Expected total 3, got %d", total)
	}

	expectedAvg := 200 * time.Millisecond
	if avg != expectedAvg {
		t.Errorf("Expected avg %v, got %v", expectedAvg, avg)
	}
}

func TestSlowQueryLogClear(t *testing.T) {
	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, "")

	sql.Log("SELECT * FROM users", 100*time.Millisecond, 0, 10)

	if len(sql.GetEntries(10)) != 1 {
		t.Error("Expected 1 entry before clear")
	}

	sql.Clear()

	if len(sql.GetEntries(10)) != 0 {
		t.Error("Expected 0 entries after clear")
	}
}

func TestSlowQueryLogEnableDisable(t *testing.T) {
	sql := NewSlowQueryLog(false, 100*time.Millisecond, 100, "")

	if sql.IsEnabled() {
		t.Error("Expected disabled initially")
	}

	sql.Enable()

	if !sql.IsEnabled() {
		t.Error("Expected enabled after Enable()")
	}

	sql.Disable()

	if sql.IsEnabled() {
		t.Error("Expected disabled after Disable()")
	}
}

func TestSlowQueryLogSetThreshold(t *testing.T) {
	sql := NewSlowQueryLog(true, 100*time.Millisecond, 100, "")

	// Change threshold
	sql.SetThreshold(50 * time.Millisecond)

	// Log query that was previously above threshold but now below
	sql.Log("SELECT * FROM users", 75*time.Millisecond, 0, 10)

	entries := sql.GetEntries(10)
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry with new threshold, got %d", len(entries))
	}
}

func TestSlowQueryLogRows(t *testing.T) {
	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, "")

	sql.Log("INSERT INTO users VALUES (1)", 100*time.Millisecond, 1, 0)
	sql.Log("SELECT * FROM users", 200*time.Millisecond, 0, 50)

	entries := sql.GetEntries(10)
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	if entries[0].RowsAffected != 1 {
		t.Errorf("Expected RowsAffected 1, got %d", entries[0].RowsAffected)
	}

	if entries[1].RowsReturned != 50 {
		t.Errorf("Expected RowsReturned 50, got %d", entries[1].RowsReturned)
	}
}
