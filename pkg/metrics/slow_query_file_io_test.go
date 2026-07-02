package metrics

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func readSlowQueryLogEntries(t *testing.T, path string) []SlowQueryEntry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	defer f.Close()
	var entries []SlowQueryEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e SlowQueryEntry
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return entries
}

// TestSlowQueryLogPersistentHandleDurability verifies that with the buffered
// persistent-handle implementation every logged entry is eventually durable
// and Close flushes everything.
func TestSlowQueryLogPersistentHandleDurability(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "slow.log")

	s := NewSlowQueryLog(true, time.Millisecond, 1000, logFile)

	const n = 200 // crosses the fsync batch size
	for i := 0; i < n; i++ {
		s.Log(fmt.Sprintf("SELECT %d", i), 5*time.Millisecond, 0, int64(i))
	}
	if err := s.LastWriteError(); err != nil {
		t.Fatalf("write error: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	entries := readSlowQueryLogEntries(t, logFile)
	if len(entries) != n {
		t.Fatalf("expected %d durable entries after Close, got %d", n, len(entries))
	}
	for i, e := range entries {
		if want := fmt.Sprintf("SELECT %d", i); e.SQL != want {
			t.Fatalf("entry %d: got %q want %q (ordering/corruption)", i, e.SQL, want)
		}
	}

	// Close is idempotent; Log after Close records an error but does not panic.
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	s.Log("SELECT after close", 5*time.Millisecond, 0, 0)
	if err := s.LastWriteError(); err == nil {
		t.Fatal("expected write error after Close")
	}
}

// TestSlowQueryLogConcurrentLogging verifies file logging is safe under
// concurrency and loses nothing. Run with -race.
func TestSlowQueryLogConcurrentLogging(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "slow-concurrent.log")

	s := NewSlowQueryLog(true, time.Millisecond, 10000, logFile)

	const goroutines = 8
	const perG = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				s.Log(fmt.Sprintf("SELECT g%d_%d", g, i), 5*time.Millisecond, 0, 0)
			}
		}(g)
	}
	wg.Wait()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	entries := readSlowQueryLogEntries(t, logFile)
	if len(entries) != goroutines*perG {
		t.Fatalf("expected %d entries, got %d", goroutines*perG, len(entries))
	}
}

// TestSlowQueryLogRotationReopens verifies that after the file is rotated
// away, logging continues into a fresh file at the configured path.
func TestSlowQueryLogRotationReopens(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "slow-rotate.log")

	s := NewSlowQueryLog(true, time.Millisecond, 1000, logFile)
	defer s.Close()

	// Write enough entries to force an fsync (rotation is detected at sync).
	for i := 0; i < slowQueryLogSyncEveryN; i++ {
		s.Log("SELECT before", 5*time.Millisecond, 0, 0)
	}
	// Rotate.
	if err := os.Rename(logFile, logFile+".1"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	// Trigger another sync cycle so rotation is detected, then keep logging.
	for i := 0; i < 2*slowQueryLogSyncEveryN; i++ {
		s.Log("SELECT after", 5*time.Millisecond, 0, 0)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := os.Stat(logFile); err != nil {
		t.Fatalf("expected a fresh log file after rotation: %v", err)
	}
	entries := readSlowQueryLogEntries(t, logFile)
	if len(entries) == 0 {
		t.Fatal("expected entries in the reopened log file")
	}
	for _, e := range entries {
		if e.SQL != "SELECT after" {
			t.Fatalf("unexpected entry in reopened file: %q", e.SQL)
		}
	}
}
