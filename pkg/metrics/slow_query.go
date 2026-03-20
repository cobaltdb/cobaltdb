package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SlowQueryEntry represents a single slow query log entry
type SlowQueryEntry struct {
	Timestamp   time.Time     `json:"timestamp"`
	SQL         string        `json:"sql"`
	Duration    time.Duration `json:"duration_ms"`
	RowsAffected int64        `json:"rows_affected,omitempty"`
	RowsReturned int64        `json:"rows_returned,omitempty"`
}

// SlowQueryLog manages slow query logging
type SlowQueryLog struct {
	enabled   bool
	threshold time.Duration
	maxEntries int
	entries   []SlowQueryEntry
	mu        sync.RWMutex
	logFile   string
}

// NewSlowQueryLog creates a new slow query logger
func NewSlowQueryLog(enabled bool, threshold time.Duration, maxEntries int, logFile string) *SlowQueryLog {
	return &SlowQueryLog{
		enabled:    enabled,
		threshold:  threshold,
		maxEntries: maxEntries,
		entries:    make([]SlowQueryEntry, 0),
		logFile:    logFile,
	}
}

// Log logs a slow query if it exceeds the threshold
func (s *SlowQueryLog) Log(sql string, duration time.Duration, rowsAffected, rowsReturned int64) {
	if !s.enabled {
		return
	}

	if duration < s.threshold {
		return
	}

	entry := SlowQueryEntry{
		Timestamp:    time.Now().UTC(),
		SQL:          sql,
		Duration:     duration,
		RowsAffected: rowsAffected,
		RowsReturned: rowsReturned,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Add to in-memory buffer
	s.entries = append(s.entries, entry)

	// Trim if exceeds max entries
	if len(s.entries) > s.maxEntries {
		s.entries = s.entries[len(s.entries)-s.maxEntries:]
	}

	// Write to log file if configured
	if s.logFile != "" {
		s.writeToFile(entry)
	}
}

// writeToFile appends the entry to the log file
func (s *SlowQueryLog) writeToFile(entry SlowQueryEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	// Ensure directory exists
	dir := filepath.Dir(s.logFile)
	if dir != "" && dir != "." {
		os.MkdirAll(dir, 0755)
	}

	f, err := os.OpenFile(s.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	_, _ = fmt.Fprintf(f, "%s\n", data)
}

// GetEntries returns a copy of recent slow query entries
func (s *SlowQueryLog) GetEntries(limit int) []SlowQueryEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 || limit > len(s.entries) {
		limit = len(s.entries)
	}

	result := make([]SlowQueryEntry, limit)
	start := len(s.entries) - limit
	copy(result, s.entries[start:])
	return result
}

// GetStats returns slow query statistics
func (s *SlowQueryLog) GetStats() (total int, avgDuration time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total = len(s.entries)
	if total == 0 {
		return 0, 0
	}

	var totalDuration time.Duration
	for _, e := range s.entries {
		totalDuration += e.Duration
	}

	return total, totalDuration / time.Duration(total)
}

// Clear clears all entries
func (s *SlowQueryLog) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make([]SlowQueryEntry, 0)
}

// SetThreshold updates the threshold dynamically
func (s *SlowQueryLog) SetThreshold(threshold time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threshold = threshold
}

// IsEnabled returns whether slow query logging is enabled
func (s *SlowQueryLog) IsEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled
}

// Enable enables slow query logging
func (s *SlowQueryLog) Enable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enabled = true
}

// Disable disables slow query logging
func (s *SlowQueryLog) Disable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enabled = false
}
