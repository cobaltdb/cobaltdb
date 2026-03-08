package engine

import (
	"strings"
	"testing"
	"time"
)

func TestSlowQueryLogBasic(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 10 * time.Millisecond
	config.MaxEntries = 10

	sql := NewSlowQueryLog(config)

	// Log a slow query
	sql.Log("SELECT * FROM users", nil, 100*time.Millisecond, 0, 10)

	// Check stats
	stats := sql.GetStats()
	if stats.TotalLogged != 1 {
		t.Errorf("Expected 1 logged query, got %d", stats.TotalLogged)
	}

	// Get entries
	entries := sql.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].SQL != "SELECT * FROM users" {
		t.Errorf("Expected SQL 'SELECT * FROM users', got '%s'", entries[0].SQL)
	}

	if entries[0].Duration != 100*time.Millisecond {
		t.Errorf("Expected duration 100ms, got %v", entries[0].Duration)
	}
}

func TestSlowQueryLogThreshold(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 100 * time.Millisecond

	sql := NewSlowQueryLog(config)

	// Log a query below threshold
	sql.Log("SELECT * FROM users", nil, 10*time.Millisecond, 0, 10)

	stats := sql.GetStats()
	if stats.TotalLogged != 0 {
		t.Error("Expected query below threshold to not be logged")
	}

	// Log a query above threshold
	sql.Log("SELECT * FROM users", nil, 200*time.Millisecond, 0, 10)

	stats = sql.GetStats()
	if stats.TotalLogged != 1 {
		t.Error("Expected query above threshold to be logged")
	}
}

func TestSlowQueryLogDisabled(t *testing.T) {
	config := &SlowQueryConfig{
		Enabled:   false,
		Threshold: 10 * time.Millisecond,
	}

	sql := NewSlowQueryLog(config)

	sql.Log("SELECT * FROM users", nil, 500*time.Millisecond, 0, 10)

	stats := sql.GetStats()
	if stats.TotalLogged != 0 {
		t.Error("Expected no queries logged when disabled")
	}
}

func TestSlowQueryLogSampling(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond
	config.SampleRate = 0.0 // Don't sample any

	sql := NewSlowQueryLog(config)

	sql.Log("SELECT * FROM users", nil, 500*time.Millisecond, 0, 10)

	stats := sql.GetStats()
	if stats.TotalSkipped != 1 {
		t.Errorf("Expected 1 skipped query, got %d", stats.TotalSkipped)
	}

	if stats.TotalLogged != 0 {
		t.Error("Expected no queries logged with 0 sample rate")
	}
}

func TestSlowQueryLogBufferLimit(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond
	config.MaxEntries = 5

	sql := NewSlowQueryLog(config)

	// Log more queries than buffer size
	for i := 0; i < 10; i++ {
		sql.Log("SELECT * FROM users", nil, 500*time.Millisecond, 0, 10)
		time.Sleep(1 * time.Millisecond) // Ensure different timestamps
	}

	stats := sql.GetStats()
	if stats.TotalLogged != 10 {
		t.Errorf("Expected 10 logged queries, got %d", stats.TotalLogged)
	}

	// Buffer should only have MaxEntries
	entries := sql.GetEntries(100)
	if len(entries) > config.MaxEntries {
		t.Errorf("Expected at most %d entries in buffer, got %d", config.MaxEntries, len(entries))
	}
}

func TestSlowQueryLogGetEntriesLimit(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond
	config.MaxEntries = 10

	sql := NewSlowQueryLog(config)

	// Log 5 queries
	for i := 0; i < 5; i++ {
		sql.Log("SELECT * FROM users", nil, 500*time.Millisecond, 0, 10)
		time.Sleep(1 * time.Millisecond)
	}

	// Get only 3
	entries := sql.GetEntries(3)
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}
}

func TestSlowQueryLogTopSlowQueries(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond
	config.MaxEntries = 10

	sql := NewSlowQueryLog(config)

	// Log queries with different durations
	sql.Log("SELECT * FROM slow", nil, 500*time.Millisecond, 0, 10)
	sql.Log("SELECT * FROM medium", nil, 200*time.Millisecond, 0, 10)
	sql.Log("SELECT * FROM fast", nil, 100*time.Millisecond, 0, 10)

	// Get top 2
	top := sql.GetTopSlowQueries(2)
	if len(top) != 2 {
		t.Fatalf("Expected 2 top queries, got %d", len(top))
	}

	// First should be the slowest
	if !strings.Contains(top[0].SQL, "slow") {
		t.Errorf("Expected slowest query first, got: %s", top[0].SQL)
	}
}

func TestSlowQueryLogClear(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond

	sql := NewSlowQueryLog(config)

	sql.Log("SELECT * FROM users", nil, 500*time.Millisecond, 0, 10)

	stats := sql.GetStats()
	if stats.TotalLogged != 1 {
		t.Error("Expected 1 logged query")
	}

	sql.Clear()

	stats = sql.GetStats()
	if stats.TotalLogged != 0 {
		t.Error("Expected 0 logged queries after clear")
	}

	entries := sql.GetEntries(10)
	if len(entries) != 0 {
		t.Error("Expected 0 entries after clear")
	}
}

func TestSlowQueryLogCallback(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond

	sql := NewSlowQueryLog(config)

	var callbackCalled bool
	var callbackEntry *SlowQueryEntry

	sql.SetCallback(func(entry *SlowQueryEntry) {
		callbackCalled = true
		callbackEntry = entry
	})

	sql.Log("SELECT * FROM users", nil, 500*time.Millisecond, 0, 10)

	if !callbackCalled {
		t.Error("Expected callback to be called")
	}

	if callbackEntry == nil {
		t.Fatal("Expected callback entry to be set")
	}

	if callbackEntry.SQL != "SELECT * FROM users" {
		t.Errorf("Expected SQL in callback, got: %s", callbackEntry.SQL)
	}
}

func TestQueryProfile(t *testing.T) {
	profile := NewQueryProfile("SELECT * FROM users")

	// Simulate parse phase
	profile.StartPhase("parse")
	time.Sleep(5 * time.Millisecond)
	profile.EndPhase()

	// Simulate plan phase
	profile.StartPhase("plan")
	time.Sleep(5 * time.Millisecond)
	profile.EndPhase()

	// Check phases
	if profile.GetPhase("parse") == 0 {
		t.Error("Expected parse time to be recorded")
	}

	if profile.GetPhase("plan") == 0 {
		t.Error("Expected plan time to be recorded")
	}

	// Total time should be at least 10ms
	if profile.TotalTime() < 10*time.Millisecond {
		t.Error("Expected total time to be at least 10ms")
	}
}

func TestQueryProfileWithoutEndPhase(t *testing.T) {
	profile := NewQueryProfile("SELECT * FROM users")

	profile.StartPhase("parse")
	// Don't call EndPhase

	// Start new phase
	profile.StartPhase("plan")

	// Parse phase should not be recorded since EndPhase wasn't called
	if profile.GetPhase("parse") != 0 {
		t.Error("Expected parse time to be 0 when EndPhase not called")
	}
}

func TestSlowQueryLogWithProfile(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond
	config.LogToStdout = false

	sql := NewSlowQueryLog(config)

	profile := NewQueryProfile("SELECT * FROM users")

	profile.StartPhase("parse")
	time.Sleep(2 * time.Millisecond)
	profile.EndPhase()

	profile.StartPhase("plan")
	time.Sleep(2 * time.Millisecond)
	profile.EndPhase()

	profile.StartPhase("execute")
	time.Sleep(2 * time.Millisecond)
	profile.EndPhase()

	sql.LogWithProfile(profile, 0, 10)

	entries := sql.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].ParseTime == 0 {
		t.Error("Expected parse time to be recorded")
	}

	if entries[0].PlanTime == 0 {
		t.Error("Expected plan time to be recorded")
	}

	if entries[0].ExecuteTime == 0 {
		t.Error("Expected execute time to be recorded")
	}
}

func TestSlowQueryLogWithProfileNotSlow(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Second // Very high threshold

	sql := NewSlowQueryLog(config)

	profile := NewQueryProfile("SELECT * FROM users")
	profile.StartPhase("execute")
	time.Sleep(1 * time.Millisecond)
	profile.EndPhase()

	sql.LogWithProfile(profile, 0, 10)

	stats := sql.GetStats()
	if stats.TotalLogged != 0 {
		t.Error("Expected query below threshold to not be logged")
	}
}

func TestSlowQueryAnalyzer(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond

	sql := NewSlowQueryLog(config)
	analyzer := NewSlowQueryAnalyzer(sql)

	// Log some queries
	sql.Log("SELECT * FROM users", nil, 100*time.Millisecond, 0, 10)
	sql.Log("SELECT * FROM orders", nil, 200*time.Millisecond, 0, 10)

	report := analyzer.AnalyzePatterns()

	if report.TotalQueries != 2 {
		t.Errorf("Expected 2 queries in report, got %d", report.TotalQueries)
	}

	if report.AvgDuration == 0 {
		t.Error("Expected non-zero average duration")
	}

	if report.MaxDuration != 200*time.Millisecond {
		t.Errorf("Expected max duration 200ms, got %v", report.MaxDuration)
	}
}

func TestSlowQueryAnalyzerEmpty(t *testing.T) {
	config := DefaultSlowQueryConfig()

	sql := NewSlowQueryLog(config)
	analyzer := NewSlowQueryAnalyzer(sql)

	report := analyzer.AnalyzePatterns()

	if report.TotalQueries != 0 {
		t.Errorf("Expected 0 queries, got %d", report.TotalQueries)
	}
}

func TestDefaultSlowQueryConfig(t *testing.T) {
	config := DefaultSlowQueryConfig()

	if !config.Enabled {
		t.Error("Expected enabled by default")
	}

	if config.Threshold != 1*time.Second {
		t.Errorf("Expected threshold 1s, got %v", config.Threshold)
	}

	if config.MaxEntries != 1000 {
		t.Errorf("Expected max entries 1000, got %d", config.MaxEntries)
	}

	if config.SampleRate != 1.0 {
		t.Errorf("Expected sample rate 1.0, got %f", config.SampleRate)
	}
}

func TestSlowQueryLogNilConfig(t *testing.T) {
	// Should use default config when nil
	sql := NewSlowQueryLog(nil)

	if sql.config == nil {
		t.Fatal("Expected config to be set")
	}

	if !sql.config.Enabled {
		t.Error("Expected enabled by default")
	}
}

func TestSlowQueryLogRowsData(t *testing.T) {
	config := DefaultSlowQueryConfig()
	config.Threshold = 1 * time.Millisecond

	sql := NewSlowQueryLog(config)

	args := []interface{}{1, "test", true}
	sql.Log("SELECT * FROM users WHERE id = ?", args, 500*time.Millisecond, 5, 10)

	entries := sql.GetEntries(10)
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].RowsAffected != 5 {
		t.Errorf("Expected 5 rows affected, got %d", entries[0].RowsAffected)
	}

	if entries[0].RowsReturned != 10 {
		t.Errorf("Expected 10 rows returned, got %d", entries[0].RowsReturned)
	}

	if len(entries[0].Args) != 3 {
		t.Errorf("Expected 3 args, got %d", len(entries[0].Args))
	}
}

func TestExtractQueryPattern(t *testing.T) {
	// Short SQL
	pattern := extractQueryPattern("SELECT * FROM users")
	if pattern != "SELECT * FROM users" {
		t.Errorf("Expected full SQL, got: %s", pattern)
	}

	// Long SQL
	longSQL := "SELECT * FROM users WHERE id = 1 AND name = 'test' AND status = 'active' AND created_at > '2024-01-01'"
	pattern = extractQueryPattern(longSQL)
	if !strings.HasSuffix(pattern, "...") {
		t.Error("Expected long SQL to be truncated")
	}
}
