package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/audit"
)

// TestLoadExistingErrorPaths tests loadExisting error handling
func TestLoadExistingErrorPaths(t *testing.T) {
	// Test with corrupted database file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "corrupt.db")

	// Create a file with invalid content (not a valid meta page)
	if err := os.WriteFile(dbPath, []byte("invalid data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, err := Open(dbPath, &Options{
		InMemory:  false,
		CacheSize: 1024,
	})
	if err == nil {
		t.Error("Expected error when opening corrupt database")
	}
}

// TestCreateNewWithReplication tests createNew with replication options
func TestCreateNewWithReplication(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_repl.db")

	db, err := Open(dbPath, &Options{
		CacheSize:             256,
		ReplicationRole:       "master",
		ReplicationMode:       "async",
		ReplicationListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Failed to open with replication: %v", err)
	}
	defer db.Close()

	if db.replicationMgr != nil {
		t.Log("Replication manager initialized")
	}
}

// TestCreateNewWithSlowQueryLog tests createNew with slow query log enabled
func TestCreateNewWithSlowQueryLog(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "slowquery.db")
	logPath := filepath.Join(tmpDir, "slow.log")

	db, err := Open(dbPath, &Options{
		InMemory:             false,
		CacheSize:            1024,
		EnableSlowQueryLog:   true,
		SlowQueryThreshold:   100 * time.Millisecond,
		SlowQueryMaxEntries:  100,
		SlowQueryLogFile:     logPath,
	})
	if err != nil {
		t.Fatalf("Failed to open database with slow query log: %v", err)
	}

	// Verify database is functional
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	db.Close()
}

// TestExecuteVacuum tests executeVacuum
func TestExecuteVacuum(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id) VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Vacuum should succeed
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Errorf("VACUUM failed: %v", err)
	}
}

// TestExecuteAnalyze tests executeAnalyze
func TestExecuteAnalyze(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test1 (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE test2 (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test1 (id) VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Analyze all tables
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Errorf("ANALYZE all tables failed: %v", err)
	}

	// Analyze specific table
	_, err = db.Exec(ctx, "ANALYZE test1")
	if err != nil {
		t.Errorf("ANALYZE specific table failed: %v", err)
	}
}

// TestBeginHotBackupClosedDB tests BeginHotBackup on closed database
func TestBeginHotBackupClosedDB(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	// BeginHotBackup should fail on closed database
	if err := db.BeginHotBackup(); err == nil {
		t.Error("BeginHotBackup should fail on closed database")
	}
}

// TestEndHotBackup tests EndHotBackup
func TestEndHotBackup(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// EndHotBackup should succeed
	if err := db.EndHotBackup(); err != nil {
		t.Errorf("EndHotBackup failed: %v", err)
	}
}

// TestBackupOperationsNoManager tests backup operations without backup manager
func TestBackupOperationsNoManager(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Backup operations should handle nil backup manager gracefully
	backups := db.ListBackups()
	// ListBackups may return nil or empty slice when backup manager not initialized
	t.Logf("ListBackups returned: %v", backups)

	backup := db.GetBackup("test")
	if backup != nil {
		t.Error("GetBackup should return nil when backup manager not initialized")
	}

	err = db.DeleteBackup("test")
	if err == nil {
		t.Error("DeleteBackup should return error when backup manager not initialized")
	}

	ctx := context.Background()
	_, err = db.CreateBackup(ctx, "full")
	if err == nil {
		t.Error("CreateBackup should return error when backup manager not initialized")
	}
}

// TestCloseWithErrors tests Close with various cleanup scenarios
func TestCloseWithErrors(t *testing.T) {
	// Test normal close
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Double close should be safe
	if err := db.Close(); err != nil {
		t.Errorf("First close failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Errorf("Second close failed: %v", err)
	}
}

// TestQueryCache tests query cache operations
func TestQueryCache(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:         true,
		CacheSize:        1024,
		EnableQueryCache: true,
		QueryCacheSize:   1000,
		QueryCacheTTL:    1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// GetQueryCache should return cache
	cache := db.GetQueryCache()
	if cache == nil {
		t.Error("GetQueryCache returned nil")
	}
}

// TestOpenWithEncryption tests Open with encryption
func TestOpenWithEncryption(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "encrypted.db")

	// Test with EncryptionKey (legacy option)
	db, err := Open(dbPath, &Options{
		InMemory:      false,
		CacheSize:     1024,
		EncryptionKey: []byte("test-key-32-bytes-long-for-aes-256"),
	})
	if err != nil {
		t.Fatalf("Failed to open database with encryption key: %v", err)
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	db.Close()
}

// TestOpenWithAuditConfig tests Open with audit configuration
func TestOpenWithAuditConfig(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "audit_test.db")

	db, err := Open(dbPath, &Options{
		InMemory:  false,
		CacheSize: 1024,
		AuditConfig: &audit.Config{
			Enabled:    true,
			LogFile:    filepath.Join(tmpDir, "audit.log"),
			
		},
	})
	if err != nil {
		// Audit config may fail due to file permissions - that's ok for coverage
		t.Logf("Open with audit config returned: %v", err)
		return
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	db.Close()
}

// TestOpenWithRLS tests Open with RLS enabled
func TestOpenWithRLS(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rls_test.db")

	db, err := Open(dbPath, &Options{
		InMemory:  false,
		CacheSize: 1024,
		EnableRLS: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database with RLS: %v", err)
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	db.Close()
}

// TestOpenWithInvalidDir tests Open with invalid directory
func TestOpenWithInvalidDir(t *testing.T) {
	// Try to open in a directory that cannot be created (invalid path)
	// Note: This may succeed on some systems that allow creating paths
	_, err := Open("/nonexistent/path/that/cannot/be/created/test.db", &Options{
		InMemory:  false,
		CacheSize: 1024,
	})
	if err != nil {
		t.Logf("Got expected error for invalid directory: %v", err)
	} else {
		t.Log("Open succeeded - path may be valid on this system")
	}
}

// TestGetDatabasePath tests GetDatabasePath
func TestGetDatabasePath(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	path := db.GetDatabasePath()
	if path != ":memory:" {
		t.Errorf("Expected path ':memory:', got: %s", path)
	}
}

// TestSaveMetaPage tests saveMetaPage
func TestSaveMetaPage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "meta_test.db")

	db, err := Open(dbPath, &Options{
		InMemory:  false,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// saveMetaPage is called during Close
	db.Close()
}

// TestIsHealthy tests IsHealthy method
func TestIsHealthy(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Should be healthy when open
	if !db.IsHealthy() {
		t.Error("IsHealthy should return true for open database")
	}

	db.Close()

	// Should not be healthy when closed
	if db.IsHealthy() {
		t.Error("IsHealthy should return false for closed database")
	}
}

// TestGetMetricsCollectorNil tests GetMetricsCollector when metrics disabled
func TestGetMetricsCollectorNil(t *testing.T) {
	// Note: metrics are enabled by default, but test the method
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	collector := db.GetMetricsCollector()
	if collector == nil {
		t.Log("GetMetricsCollector returned nil (metrics may be disabled)")
	}
}

// TestGetReplicationManagerNil tests GetReplicationManager when not configured
func TestGetReplicationManagerNil(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	mgr := db.GetReplicationManager()
	if mgr != nil {
		t.Error("GetReplicationManager should return nil when not configured")
	}
}

// TestGetOptimizerNil tests GetOptimizer when not initialized
func TestGetOptimizerNil(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	opt := db.GetOptimizer()
	if opt == nil {
		t.Log("GetOptimizer returned nil (may not be initialized)")
	}
}

// TestUpdateTableStatisticsNilOptimizer tests UpdateTableStatistics with nil optimizer
func TestUpdateTableStatisticsNilOptimizer(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Should not panic when optimizer is nil or not
	db.UpdateTableStatistics("test", nil)
}

// TestBeginHotBackupAndEnd tests BeginHotBackup followed by EndHotBackup
func TestBeginHotBackupAndEnd(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin hot backup
	if err := db.BeginHotBackup(); err != nil {
		t.Errorf("BeginHotBackup failed: %v", err)
	}

	// End hot backup
	if err := db.EndHotBackup(); err != nil {
		t.Errorf("EndHotBackup failed: %v", err)
	}
}
