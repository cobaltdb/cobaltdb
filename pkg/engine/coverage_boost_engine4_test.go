package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestDBWithQueryCacheOptions tests database with query cache options
func TestDBWithQueryCacheOptions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_cache.db")

	opts := &Options{
		EnableQueryCache: true,
		QueryCacheSize:   1024 * 1024,
		QueryCacheTTL:    time.Minute,
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database with query cache: %v", err)
	}

	// Execute queries to test cache
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO cache_test VALUES (1, 'test')")
	_, _ = db.Query(ctx, "SELECT * FROM cache_test WHERE id = 1")

	db.Close()
}

// TestDBWithSlowQueryLog tests database with slow query log enabled
func TestDBWithSlowQueryLog(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_slow.db")

	opts := &Options{
		EnableSlowQueryLog:  true,
		SlowQueryThreshold:  time.Millisecond,
		SlowQueryMaxEntries: 100,
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database with slow query log: %v", err)
	}

	// Execute queries
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE slow_test (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "INSERT INTO slow_test VALUES (1)")

	db.Close()
}

// TestDBWithMaxConnections tests database with max connections
func TestDBWithMaxConnections(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_maxconn.db")

	opts := &Options{
		MaxConnections:    10,
		ConnectionTimeout: 5 * time.Second,
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database with max connections: %v", err)
	}

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE conn_test (id INTEGER PRIMARY KEY)")

	db.Close()
}

// TestDBWithQueryTimeout tests database with query timeout
func TestDBWithQueryTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_timeout.db")

	opts := &Options{
		QueryTimeout: 30 * time.Second,
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database with query timeout: %v", err)
	}

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE timeout_test (id INTEGER PRIMARY KEY)")

	db.Close()
}

// TestDBWithBackupOptions tests database with backup options
func TestDBWithBackupOptions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_backup.db")
	backupDir := filepath.Join(tmpDir, "backups")

	opts := &Options{
		BackupDir:              backupDir,
		BackupRetention:        24 * time.Hour,
		MaxBackups:             5,
		BackupCompressionLevel: 6,
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database with backup options: %v", err)
	}

	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE backup_test (id INTEGER PRIMARY KEY)")

	db.Close()
}

// TestDBGetCurrentLSN tests GetCurrentLSN method
func TestDBGetCurrentLSN2(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_lsn.db")

	opts := &Options{
		WALEnabled: true,
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Get LSN
	lsn := db.GetCurrentLSN()
	t.Logf("Current LSN: %d", lsn)

	db.Close()
}

// TestDBGetWALPath tests GetWALPath method
func TestDBGetWALPath2(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_wal_path.db")

	opts := &Options{
		WALEnabled: true,
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Get WAL path (may return empty if WAL is not yet created)
	walPath := db.GetWALPath()
	t.Logf("WAL path: %s", walPath)

	db.Close()
}

// TestDBCloseTwice tests closing database multiple times
func TestDBCloseTwice2(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Close should not panic when called multiple times
	db.Close()
	db.Close()
}

// TestDBWithSyncMode tests database with different sync modes
func TestDBWithSyncMode(t *testing.T) {
	tmpDir := t.TempDir()

	syncModes := []SyncMode{SyncOff, SyncNormal, SyncFull}

	for i, mode := range syncModes {
		opts := &Options{
			SyncMode: mode,
		}

		dbPathMode := filepath.Join(tmpDir, "test_sync_"+string(rune('0'+i))+".db")
		db, err := Open(dbPathMode, opts)
		if err != nil {
			t.Fatalf("Failed to open database with sync mode %d: %v", mode, err)
		}

		ctx := context.Background()
		_, _ = db.Exec(ctx, "CREATE TABLE sync_test (id INTEGER PRIMARY KEY)")

		db.Close()
	}
}
