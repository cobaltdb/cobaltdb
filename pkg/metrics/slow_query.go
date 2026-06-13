package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	slowQueryLogDirPerm  = 0750
	slowQueryLogFilePerm = 0600
	maxSlowQuerySQLBytes = 10000
)

// SlowQueryEntry represents a single slow query log entry
type SlowQueryEntry struct {
	Timestamp    time.Time     `json:"timestamp"`
	SQL          string        `json:"sql"`
	Duration     time.Duration `json:"duration_ms"`
	RowsAffected int64         `json:"rows_affected,omitempty"`
	RowsReturned int64         `json:"rows_returned,omitempty"`
}

type slowQueryEntryJSON struct {
	Timestamp    time.Time `json:"timestamp"`
	SQL          string    `json:"sql"`
	DurationMS   int64     `json:"duration_ms"`
	RowsAffected int64     `json:"rows_affected,omitempty"`
	RowsReturned int64     `json:"rows_returned,omitempty"`
}

func (e SlowQueryEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(slowQueryEntryJSON{
		Timestamp:    e.Timestamp,
		SQL:          e.SQL,
		DurationMS:   e.Duration.Milliseconds(),
		RowsAffected: e.RowsAffected,
		RowsReturned: e.RowsReturned,
	})
}

func (e *SlowQueryEntry) UnmarshalJSON(data []byte) error {
	var entry slowQueryEntryJSON
	if err := json.Unmarshal(data, &entry); err != nil {
		return err
	}
	e.Timestamp = entry.Timestamp
	e.SQL = entry.SQL
	e.Duration = time.Duration(entry.DurationMS) * time.Millisecond
	e.RowsAffected = entry.RowsAffected
	e.RowsReturned = entry.RowsReturned
	return nil
}

// SlowQueryLog manages slow query logging
type SlowQueryLog struct {
	enabled      bool
	threshold    time.Duration
	maxEntries   int
	entries      []SlowQueryEntry
	mu           sync.RWMutex
	logFile      string
	lastWriteErr error
}

// NewSlowQueryLog creates a new slow query logger
func NewSlowQueryLog(enabled bool, threshold time.Duration, maxEntries int, logFile string) *SlowQueryLog {
	if maxEntries < 0 {
		maxEntries = 0
	}
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
		SQL:          truncateSlowQuerySQL(sql),
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
		s.lastWriteErr = s.writeToFile(entry)
	}
}

// writeToFile appends the entry to the log file
func (s *SlowQueryLog) writeToFile(entry SlowQueryEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal slow query entry: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(s.logFile)
	if dir != "" && dir != "." {
		if err := rejectSlowQueryLogDirSymlinks(dir); err != nil {
			return err
		}
		if err := os.MkdirAll(dir, slowQueryLogDirPerm); err != nil {
			return fmt.Errorf("create slow query log directory: %w", err)
		}
		if err := rejectSlowQueryLogDirSymlinks(dir); err != nil {
			return err
		}
	}

	f, created, err := openSlowQueryLogFile(s.logFile)
	if err != nil {
		return fmt.Errorf("open slow query log file: %w", err)
	}

	if _, err := fmt.Fprintf(f, "%s\n", data); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			return fmt.Errorf("write slow query log entry: %w; close failed: %v", err, closeErr)
		}
		return fmt.Errorf("write slow query log entry: %w", err)
	}
	if err := f.Sync(); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			return fmt.Errorf("sync slow query log file: %w; close failed: %v", err, closeErr)
		}
		return fmt.Errorf("sync slow query log file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close slow query log file: %w", err)
	}
	if created {
		if err := syncSlowQueryLogParentDir(s.logFile); err != nil {
			return fmt.Errorf("sync slow query log directory: %w", err)
		}
	}
	return nil
}

func truncateSlowQuerySQL(sql string) string {
	if len(sql) <= maxSlowQuerySQLBytes {
		return sql
	}
	return sql[:maxSlowQuerySQLBytes]
}

func openSlowQueryLogFile(path string) (*os.File, bool, error) {
	cleanPath := filepath.Clean(path)
	if err := rejectSlowQueryLogDirSymlinks(filepath.Dir(cleanPath)); err != nil {
		return nil, false, err
	}
	info, statErr := os.Lstat(cleanPath)
	created := os.IsNotExist(statErr)
	if statErr != nil && !created {
		return nil, false, statErr
	}
	if !created {
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, false, fmt.Errorf("slow query log file must not be a symlink: %s", cleanPath)
		}
		if !info.Mode().IsRegular() {
			return nil, false, fmt.Errorf("slow query log file must be a regular file: %s", cleanPath)
		}
	}

	f, err := os.OpenFile(cleanPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, slowQueryLogFilePerm)
	if err != nil {
		return nil, false, err
	}
	if err := f.Chmod(slowQueryLogFilePerm); err != nil {
		closeErr := f.Close()
		if closeErr != nil {
			return nil, false, fmt.Errorf("chmod slow query log file: %w; close failed: %v", err, closeErr)
		}
		return nil, false, fmt.Errorf("chmod slow query log file: %w", err)
	}
	openedInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, false, err
	}
	if !openedInfo.Mode().IsRegular() {
		_ = f.Close()
		return nil, false, fmt.Errorf("slow query log file must be a regular file: %s", cleanPath)
	}
	if !created && !os.SameFile(info, openedInfo) {
		_ = f.Close()
		return nil, false, fmt.Errorf("slow query log file changed while opening: %s", cleanPath)
	}
	return f, created, nil
}

func rejectSlowQueryLogDirSymlinks(path string) error {
	path = filepath.Clean(path)
	if path == "." || path == string(os.PathSeparator) {
		return nil
	}

	current := "."
	if filepath.IsAbs(path) {
		current = string(os.PathSeparator)
		path = strings.TrimPrefix(path, string(os.PathSeparator))
	}

	for _, part := range strings.Split(path, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to stat slow query log directory component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("slow query log directory component must not be a symlink: %s", current)
		}
	}
	return nil
}

func syncSlowQueryLogParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

// LastWriteError returns the last file logging error, if any.
func (s *SlowQueryLog) LastWriteError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastWriteErr
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
