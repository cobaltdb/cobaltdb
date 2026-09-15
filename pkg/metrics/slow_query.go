package metrics

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	slowQueryLogFilePerm = 0600
	maxSlowQuerySQLBytes = 10000
)

var slowQueryLogDirPerm = os.FileMode(0750)

// OS seam variables for deterministic test injection.
var slowQueryOpenFile = os.OpenFile
var slowQueryLstat = os.Lstat
var slowQueryMkdirAll = os.MkdirAll
var slowQuerySameFile = os.SameFile

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

// slowQueryLogSyncEveryN is the number of file-logged entries after which the
// log file is fsynced.
const slowQueryLogSyncEveryN = 64

// slowQueryLogSyncInterval is the maximum time file-logged entries may remain
// un-fsynced (checked on the next write).
const slowQueryLogSyncInterval = time.Second

// SlowQueryLog manages slow query logging.
//
// enabled and thresholdNanos are accessed atomically: Log() reads them on the
// per-query hot path without taking the mutex, while Enable/Disable/SetThreshold
// mutate them concurrently. The mutex (s.mu) guards only the in-memory entries
// buffer; all file IO happens under a separate fileMu so slow disk syncs never
// block readers of the in-memory buffer or other Log() bookkeeping.
//
// File IO strategy: the log file is opened once (with the same symlink-safety
// validation as before), kept open, and written through a buffered writer that
// is flushed to the OS on every entry and fsynced every slowQueryLogSyncEveryN
// entries or slowQueryLogSyncInterval, whichever comes first, and on Close.
// External rotation (file removed/renamed) is detected at sync time and the
// handle is reopened on the next write.
type SlowQueryLog struct {
	enabled        atomic.Bool
	thresholdNanos atomic.Int64
	maxEntries     int
	entries        []SlowQueryEntry
	mu             sync.RWMutex
	logFile        string

	// File-write state, guarded by fileMu (never held together with s.mu).
	fileMu       sync.Mutex
	file         *os.File
	writer       *bufio.Writer
	fileInfo     os.FileInfo // Lstat at open time, for rotation detection
	pendingSync  int         // entries written since last fsync
	lastSyncAt   time.Time
	lastWriteErr error
	fileClosed   bool
}

// NewSlowQueryLog creates a new slow query logger
func NewSlowQueryLog(enabled bool, threshold time.Duration, maxEntries int, logFile string) *SlowQueryLog {
	if maxEntries < 0 {
		maxEntries = 0
	}
	s := &SlowQueryLog{
		maxEntries: maxEntries,
		entries:    make([]SlowQueryEntry, 0),
		logFile:    logFile,
	}
	s.enabled.Store(enabled)
	s.thresholdNanos.Store(int64(threshold))
	return s
}

// Log logs a slow query if it exceeds the threshold
func (s *SlowQueryLog) Log(sql string, duration time.Duration, rowsAffected, rowsReturned int64) {
	if !s.enabled.Load() {
		return
	}

	if duration < time.Duration(s.thresholdNanos.Load()) {
		return
	}

	entry := SlowQueryEntry{
		Timestamp:    time.Now().UTC(),
		SQL:          truncateSlowQuerySQL(sql),
		Duration:     duration,
		RowsAffected: rowsAffected,
		RowsReturned: rowsReturned,
	}

	// In-memory buffer under s.mu only — no file IO inside this lock.
	s.mu.Lock()
	s.entries = append(s.entries, entry)
	if len(s.entries) > s.maxEntries {
		s.entries = s.entries[len(s.entries)-s.maxEntries:]
	}
	s.mu.Unlock()

	// File write under the dedicated file mutex.
	if s.logFile != "" {
		s.fileMu.Lock()
		s.lastWriteErr = s.writeToFileLocked(entry)
		s.fileMu.Unlock()
	}
}

// Close flushes and fsyncs any buffered log entries and closes the log file
// handle. Close is idempotent. Log calls after Close record an error instead
// of writing (the in-memory buffer keeps working).
func (s *SlowQueryLog) Close() error {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()

	if s.fileClosed {
		return nil
	}
	s.fileClosed = true

	if s.file == nil {
		return nil
	}
	var errs []error
	if s.writer != nil {
		if err := s.writer.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("flush slow query log: %w", err))
		}
	}
	if err := s.file.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("sync slow query log: %w", err))
	}
	if err := s.file.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close slow query log: %w", err))
	}
	s.file = nil
	s.writer = nil
	s.fileInfo = nil
	err := errors.Join(errs...)
	if err != nil {
		s.lastWriteErr = err
	}
	return err
}

// writeToFileLocked appends the entry to the log file through the persistent
// buffered writer. Caller must hold s.fileMu.
func (s *SlowQueryLog) writeToFileLocked(entry SlowQueryEntry) error {
	if s.fileClosed {
		return fmt.Errorf("slow query log file is closed")
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal slow query entry: %w", err)
	}

	if err := s.ensureFileOpenLocked(); err != nil {
		return err
	}

	if _, err := s.writer.Write(append(data, '\n')); err != nil {
		s.closeFileLocked() // force reopen (and revalidation) on next write
		return fmt.Errorf("write slow query log entry: %w", err)
	}
	// Flush to the OS on every entry so records survive a process crash; the
	// expensive fsync is batched below.
	if err := s.writer.Flush(); err != nil {
		s.closeFileLocked()
		return fmt.Errorf("flush slow query log entry: %w", err)
	}

	s.pendingSync++
	if s.pendingSync >= slowQueryLogSyncEveryN || time.Since(s.lastSyncAt) >= slowQueryLogSyncInterval {
		if err := s.file.Sync(); err != nil {
			s.closeFileLocked()
			return fmt.Errorf("sync slow query log file: %w", err)
		}
		s.pendingSync = 0
		s.lastSyncAt = time.Now()
		// Rotation detection: if the path no longer refers to our open file
		// (rotated/removed), close so the next write reopens and revalidates.
		if info, statErr := slowQueryLstat(s.logFile); statErr != nil || !slowQuerySameFile(info, s.fileInfo) {
			s.closeFileLocked()
		}
	}
	return nil
}

// ensureFileOpenLocked opens the log file (validating symlink safety) if it
// is not already open. Caller must hold s.fileMu.
func (s *SlowQueryLog) ensureFileOpenLocked() error {
	if s.file != nil {
		return nil
	}

	// Ensure directory exists
	dir := filepath.Dir(s.logFile)
	if dir != "" && dir != "." {
		if err := rejectSlowQueryLogDirSymlinks(dir); err != nil {
			return err
		}
		if err := slowQueryMkdirAll(dir, slowQueryLogDirPerm); err != nil {
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
	if created {
		if err := syncSlowQueryLogParentDir(s.logFile); err != nil {
			_ = f.Close()
			return fmt.Errorf("sync slow query log directory: %w", err)
		}
	}
	info, err := slowQueryLstat(s.logFile)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat slow query log file: %w", err)
	}

	s.file = f
	s.writer = bufio.NewWriter(f)
	s.fileInfo = info
	s.pendingSync = 0
	s.lastSyncAt = time.Now()
	return nil
}

// closeFileLocked closes the persistent handle (best effort) so the next
// write reopens and revalidates the path. Caller must hold s.fileMu.
func (s *SlowQueryLog) closeFileLocked() {
	if s.writer != nil {
		_ = s.writer.Flush()
	}
	if s.file != nil {
		_ = s.file.Close()
	}
	s.file = nil
	s.writer = nil
	s.fileInfo = nil
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

	f, err := slowQueryOpenFile(cleanPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, slowQueryLogFilePerm)
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
		info, err := slowQueryLstat(current)
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
	if err := rejectSlowQueryLogDirSymlinks(dir); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("slow query log directory must not be a symlink: %s", dir)
	}
	if !info.IsDir() {
		return fmt.Errorf("slow query log directory must be a directory: %s", dir)
	}
	file, err := os.Open(dir) // #nosec G304 -- directory path is derived from a validated slow-query log path and checked against symlink swaps before use.
	if err != nil {
		return err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return err
	}
	if !openedInfo.IsDir() {
		return fmt.Errorf("slow query log directory must be a directory: %s", dir)
	}
	if !os.SameFile(info, openedInfo) {
		return fmt.Errorf("slow query log directory changed while syncing: %s", dir)
	}
	return file.Sync()
}

// LastWriteError returns the last file logging error, if any.
func (s *SlowQueryLog) LastWriteError() error {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
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
	s.thresholdNanos.Store(int64(threshold))
}

// IsEnabled returns whether slow query logging is enabled
func (s *SlowQueryLog) IsEnabled() bool {
	return s.enabled.Load()
}

// Enable enables slow query logging
func (s *SlowQueryLog) Enable() {
	s.enabled.Store(true)
}

// Disable disables slow query logging
func (s *SlowQueryLog) Disable() {
	s.enabled.Store(false)
}
