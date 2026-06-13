package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

func TestSlowQueryLogNegativeMaxEntriesDoesNotPanic(t *testing.T) {
	sql := NewSlowQueryLog(true, 1*time.Millisecond, -1, "")

	sql.Log("SELECT * FROM users", 10*time.Millisecond, 0, 1)

	if entries := sql.GetEntries(10); len(entries) != 0 {
		t.Fatalf("expected negative maxEntries to retain no entries, got %d", len(entries))
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

	info, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("Failed to stat log file: %v", err)
	}
	if info.Mode().Perm() != slowQueryLogFilePerm {
		t.Errorf("Expected log file permissions %o, got %o", slowQueryLogFilePerm, info.Mode().Perm())
	}
	if err := sql.LastWriteError(); err != nil {
		t.Fatalf("expected no slow query log write error, got %v", err)
	}
}

func TestSlowQueryEntryJSONUsesMilliseconds(t *testing.T) {
	entry := SlowQueryEntry{
		Timestamp:    time.Unix(1700000000, 0).UTC(),
		SQL:          "SELECT pg_sleep(1)",
		Duration:     1500 * time.Millisecond,
		RowsReturned: 1,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal slow query entry: %v", err)
	}
	if !strings.Contains(string(data), `"duration_ms":1500`) {
		t.Fatalf("expected duration_ms to be encoded as milliseconds, got %s", data)
	}

	var decoded SlowQueryEntry
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal slow query entry: %v", err)
	}
	if decoded.Duration != 1500*time.Millisecond {
		t.Fatalf("decoded duration = %v, want 1500ms", decoded.Duration)
	}
}

func TestSlowQueryLogFileDurationIsMilliseconds(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "slow_queries.log")
	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, logFile)

	sql.Log("SELECT * FROM users", 250*time.Millisecond, 0, 10)
	if err := sql.LastWriteError(); err != nil {
		t.Fatalf("expected no slow query log write error, got %v", err)
	}

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read slow query log: %v", err)
	}
	if !strings.Contains(string(data), `"duration_ms":250`) {
		t.Fatalf("expected file duration_ms to be milliseconds, got %s", data)
	}
}

func TestSlowQueryLogTruncatesOversizedSQLInMemoryAndFile(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "slow_queries.log")
	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, logFile)
	oversized := strings.Repeat("x", maxSlowQuerySQLBytes+128)

	sql.Log(oversized, 100*time.Millisecond, 0, 1)

	entries := sql.GetEntries(1)
	if len(entries) != 1 {
		t.Fatalf("expected one slow query entry, got %d", len(entries))
	}
	if len(entries[0].SQL) != maxSlowQuerySQLBytes {
		t.Fatalf("in-memory SQL length = %d, want %d", len(entries[0].SQL), maxSlowQuerySQLBytes)
	}
	if entries[0].SQL != oversized[:maxSlowQuerySQLBytes] {
		t.Fatal("in-memory SQL was not truncated deterministically")
	}
	if err := sql.LastWriteError(); err != nil {
		t.Fatalf("expected no slow query log write error, got %v", err)
	}

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read slow query log: %v", err)
	}
	var logged SlowQueryEntry
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &logged); err != nil {
		t.Fatalf("decode slow query log entry: %v", err)
	}
	if len(logged.SQL) != maxSlowQuerySQLBytes {
		t.Fatalf("logged SQL length = %d, want %d", len(logged.SQL), maxSlowQuerySQLBytes)
	}
}

func TestSlowQueryLogRestrictsExistingFilePermissions(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "slow_queries.log")
	if err := os.WriteFile(logFile, []byte{}, 0644); err != nil {
		t.Fatalf("write existing log file: %v", err)
	}

	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, logFile)
	sql.Log("SELECT * FROM users", 100*time.Millisecond, 0, 10)

	if err := sql.LastWriteError(); err != nil {
		t.Fatalf("expected no slow query log write error, got %v", err)
	}
	info, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("stat log file: %v", err)
	}
	if info.Mode().Perm() != slowQueryLogFilePerm {
		t.Fatalf("log file permissions = %v, want %v", info.Mode().Perm(), slowQueryLogFilePerm)
	}
}

func TestSlowQueryLogRejectsSymlinkFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.log")
	link := filepath.Join(dir, "link.log")
	if err := os.WriteFile(target, []byte{}, slowQueryLogFilePerm); err != nil {
		t.Fatalf("write target log: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, link)
	sql.Log("SELECT * FROM users", 100*time.Millisecond, 0, 10)

	err := sql.LastWriteError()
	if err == nil {
		t.Fatal("expected symlink slow query log write error")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestSlowQueryLogRejectsSymlinkParentComponent(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "target")
	if err := os.Mkdir(targetDir, 0750); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	linkDir := filepath.Join(dir, "link")
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	logFile := filepath.Join(linkDir, "nested", "slow.log")
	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, logFile)
	sql.Log("SELECT * FROM users", 100*time.Millisecond, 0, 10)

	err := sql.LastWriteError()
	if err == nil {
		t.Fatal("expected symlink parent slow query log write error")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(targetDir, "nested", "slow.log")); !os.IsNotExist(statErr) {
		t.Fatalf("slow query log should not be created through symlink parent, stat err=%v", statErr)
	}
}

func TestSlowQueryLogRejectsNonRegularFile(t *testing.T) {
	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, t.TempDir())
	sql.Log("SELECT * FROM users", 100*time.Millisecond, 0, 10)

	err := sql.LastWriteError()
	if err == nil {
		t.Fatal("expected directory slow query log write error")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestSlowQueryLogRecordsFileWriteError(t *testing.T) {
	tempDir := t.TempDir()
	parentFile := filepath.Join(tempDir, "not-a-dir")
	if err := os.WriteFile(parentFile, []byte("file"), 0600); err != nil {
		t.Fatalf("write parent file: %v", err)
	}

	sql := NewSlowQueryLog(true, 1*time.Millisecond, 100, filepath.Join(parentFile, "slow.log"))
	sql.Log("SELECT * FROM users", 100*time.Millisecond, 0, 10)

	err := sql.LastWriteError()
	if err == nil {
		t.Fatal("expected slow query log write error")
	}
	if !strings.Contains(err.Error(), "create slow query log directory") {
		t.Fatalf("expected directory creation error, got %v", err)
	}
	entries := sql.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("expected in-memory entry despite file error, got %d", len(entries))
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
