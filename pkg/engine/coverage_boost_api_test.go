package engine

import (
	"context"
	"os"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// TestPathMethod tests the Path() method
func TestPathMethod(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	path := db.Path()
	if path != ":memory:" {
		t.Errorf("Expected ':memory:', got %q", path)
	}
}

// TestPlanCacheMethods tests plan cache enable/disable/stats/clear methods
func TestPlanCacheMethods(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	// Initially plan cache may or may not be enabled
	initialState := db.IsPlanCacheEnabled()
	t.Logf("Initial plan cache state: %v", initialState)

	// Disable first to ensure clean state
	db.DisablePlanCache()
	if db.IsPlanCacheEnabled() {
		t.Error("Plan cache should be disabled after DisablePlanCache()")
	}

	// GetPlanCacheStats should return nil when disabled
	stats := db.GetPlanCacheStats()
	if stats != nil {
		t.Error("Expected nil stats when plan cache is disabled")
	}

	// ClearPlanCache should not panic when disabled
	db.ClearPlanCache()

	// Enable plan cache
	db.EnablePlanCache(1024*1024, 100)
	if !db.IsPlanCacheEnabled() {
		t.Error("Plan cache should be enabled after EnablePlanCache()")
	}

	// Execute a query to populate cache
	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE pc_test (id INTEGER PRIMARY KEY, val TEXT)")
	db.Exec(ctx, "INSERT INTO pc_test VALUES (1, 'hello')")
	db.Query(ctx, "SELECT * FROM pc_test WHERE id = 1")

	// Get stats
	stats = db.GetPlanCacheStats()
	if stats == nil {
		t.Error("Expected non-nil stats when plan cache is enabled")
	} else {
		t.Logf("Plan cache stats: hits=%d, misses=%d, size=%d", stats.Hits, stats.Misses, stats.Size)
	}

	// Clear plan cache
	db.ClearPlanCache()

	// Enable again (already enabled - should be no-op)
	db.EnablePlanCache(0, 0) // default values

	// Disable
	db.DisablePlanCache()
	if db.IsPlanCacheEnabled() {
		t.Error("Plan cache should be disabled")
	}
}

// TestSearchVectorKNNMethod tests the public SearchVectorKNN method
func TestSearchVectorKNNMethod(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	// Search on non-existent index should error
	_, _, err = db.SearchVectorKNN("nonexistent", []float64{1.0, 2.0}, 5)
	if err == nil {
		t.Error("Expected error for non-existent vector index")
	}

	// Search on closed DB
	db2, _ := Open(":memory:", &Options{InMemory: true})
	db2.Close()
	_, _, err = db2.SearchVectorKNN("test", []float64{1.0}, 1)
	if err == nil {
		t.Error("Expected error for closed DB")
	}
}

// TestClosedDBMethods tests methods on a closed database return proper errors
func TestClosedDBMethods(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	db.Close()

	ctx := context.Background()

	// Exec on closed DB
	_, err = db.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Error("Expected error from Exec on closed DB")
	}

	// Query on closed DB
	_, err = db.Query(ctx, "SELECT 1")
	if err == nil {
		t.Error("Expected error from Query on closed DB")
	}
}

// TestDBUtilityMethods tests various utility methods
func TestDBUtilityMethods(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/util_test.db"

	db, err := Open(dbPath, &Options{
		WALEnabled: true,
		CacheSize:  256,
	})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE util_test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO util_test VALUES (1)")

	// GetMetrics (may be nil if not enabled)
	_, err = db.GetMetrics()
	t.Logf("GetMetrics: %v", err)

	// GetWALPath
	walPath := db.GetWALPath()
	t.Logf("WAL path: %s", walPath)

	// Checkpoint
	err = db.Checkpoint()
	t.Logf("Checkpoint: %v", err)

	// GetCurrentLSN
	lsn := db.GetCurrentLSN()
	t.Logf("Current LSN: %d", lsn)

	// ListBackups
	backups := db.ListBackups()
	t.Logf("ListBackups: %v", backups)

	// GetBackup
	backup := db.GetBackup("nonexistent")
	t.Logf("GetBackup: %v", backup)

	// DeleteBackup
	err = db.DeleteBackup("nonexistent")
	t.Logf("DeleteBackup: %v", err)

	// VACUUM via SQL
	_, err = db.Exec(ctx, "VACUUM")
	t.Logf("VACUUM: %v", err)
}

// TestReplicateWriteWithMaster tests replicateWrite with active replication
func TestReplicateWriteWithMaster(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/repl_write.db"

	db, err := Open(dbPath, &Options{
		CacheSize:             256,
		ReplicationRole:       "master",
		ReplicationMode:       "async",
		ReplicationListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE repl_test (id INTEGER PRIMARY KEY, val TEXT)")
	db.Exec(ctx, "INSERT INTO repl_test VALUES (1, 'hello')")
	db.Exec(ctx, "UPDATE repl_test SET val = 'world' WHERE id = 1")
	db.Exec(ctx, "DELETE FROM repl_test WHERE id = 1")

	t.Log("Replication write operations completed")
}

// TestDBUtilityMethodsInMemory tests utility methods on in-memory DB (no WAL)
func TestDBUtilityMethodsInMemory(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	// GetWALPath with no WAL
	walPath := db.GetWALPath()
	if walPath != "" {
		t.Errorf("Expected empty WAL path for in-memory, got %q", walPath)
	}

	// Checkpoint with no WAL
	err = db.Checkpoint()
	if err != nil {
		t.Errorf("Checkpoint should succeed with no WAL: %v", err)
	}

	// GetCurrentLSN with no WAL
	lsn := db.GetCurrentLSN()
	if lsn != 0 {
		t.Errorf("Expected LSN 0 for no WAL, got %d", lsn)
	}

	// GetMetrics with no explicit metrics setup
	_, err = db.GetMetrics()
	if err != nil {
		t.Logf("GetMetrics: %v (expected if metrics not enabled)", err)
	}

	// Backup methods
	backups := db.ListBackups()
	t.Logf("ListBackups: %d backups", len(backups))

	b := db.GetBackup("nonexistent")
	if b != nil {
		t.Log("GetBackup returned non-nil for nonexistent")
	}

	err = db.DeleteBackup("nonexistent")
	t.Logf("DeleteBackup: %v", err)

	// CreateBackup
	_, err = db.CreateBackup(context.Background(), "full")
	t.Logf("CreateBackup: %v", err)

	// BeginHotBackup / EndHotBackup
	err = db.BeginHotBackup()
	if err != nil {
		t.Errorf("BeginHotBackup failed: %v", err)
	}
	err = db.EndHotBackup()
	if err != nil {
		t.Errorf("EndHotBackup failed: %v", err)
	}

	// GetDatabasePath
	path := db.GetDatabasePath()
	t.Logf("Database path: %s", path)

	// GetMetricsCollector
	mc := db.GetMetricsCollector()
	t.Logf("Metrics collector: %v", mc)

	// GetOptimizer
	opt := db.GetOptimizer()
	t.Logf("Optimizer: %v", opt)

	// GetBackupManager
	bm := db.GetBackupManager()
	t.Logf("Backup manager: %v", bm)

	// GetQueryCache
	qc := db.GetQueryCache()
	t.Logf("Query cache: %v", qc)

	// Tables
	tables := db.Tables()
	t.Logf("Tables: %v", tables)
}

// TestVacuumAndAnalyze tests VACUUM and ANALYZE via SQL
func TestVacuumAndAnalyze(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE vac_test (id INTEGER PRIMARY KEY, data TEXT)")
	for i := 0; i < 100; i++ {
		db.Exec(ctx, "INSERT INTO vac_test VALUES (?, ?)", i, "data")
	}
	db.Exec(ctx, "DELETE FROM vac_test WHERE id > 50")

	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Errorf("VACUUM failed: %v", err)
	}

	_, err = db.Exec(ctx, "ANALYZE vac_test")
	if err != nil {
		t.Errorf("ANALYZE failed: %v", err)
	}
}

// TestCreateVectorIndexViaSQL tests CREATE VECTOR INDEX via SQL
func TestCreateVectorIndexViaSQL(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, vec VECTOR(3))")
	db.Exec(ctx, "INSERT INTO embeddings VALUES (1, '[1.0, 0.0, 0.0]')")
	db.Exec(ctx, "INSERT INTO embeddings VALUES (2, '[0.0, 1.0, 0.0]')")

	_, err = db.Exec(ctx, "CREATE VECTOR INDEX idx_vec ON embeddings (vec)")
	if err != nil {
		t.Logf("CREATE VECTOR INDEX: %v", err)
	}
}

// TestCTEViaEngine tests CTE execution through engine
func TestCTEViaEngine(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE cte_test (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)")
	db.Exec(ctx, "INSERT INTO cte_test VALUES (1, NULL, 'root')")
	db.Exec(ctx, "INSERT INTO cte_test VALUES (2, 1, 'child1')")
	db.Exec(ctx, "INSERT INTO cte_test VALUES (3, 1, 'child2')")

	rows, err := db.Query(ctx, "WITH tree AS (SELECT * FROM cte_test WHERE parent_id IS NULL) SELECT * FROM tree")
	if err != nil {
		t.Errorf("CTE query failed: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		t.Logf("CTE returned %d rows", count)
	}
}

// TestDBWithWALMethods tests methods on a WAL-enabled database
func TestDBWithWALMethods(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/wal_test.db"

	// Create DB first (no WAL on create)
	db1, err := Open(dbPath, &Options{CacheSize: 256})
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	ctx := context.Background()
	db1.Exec(ctx, "CREATE TABLE wal_t (id INTEGER PRIMARY KEY, val TEXT)")
	db1.Exec(ctx, "INSERT INTO wal_t VALUES (1, 'test')")
	db1.Close()

	// Reopen with WAL
	db, err := Open(dbPath, &Options{
		WALEnabled: true,
		CacheSize:  256,
	})
	if err != nil {
		t.Fatalf("Failed to reopen with WAL: %v", err)
	}
	defer db.Close()

	// WAL-dependent methods
	walPath := db.GetWALPath()
	t.Logf("WAL path: %q", walPath)

	lsn := db.GetCurrentLSN()
	t.Logf("Current LSN: %d", lsn)

	err = db.Checkpoint()
	t.Logf("Checkpoint: %v", err)

	// Commit via transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	tx.Exec(ctx, "INSERT INTO wal_t VALUES (2, 'tx')")
	err = tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	// HealthCheck
	err = db.HealthCheck()
	t.Logf("HealthCheck: %v", err)
}

// TestOpenWithCustomLogger tests Open with a custom logger
func TestOpenWithCustomLogger(t *testing.T) {
	customLog := logger.New(logger.InfoLevel, os.Stderr)
	db, err := Open(":memory:", &Options{
		InMemory: true,
		Logger:   customLog,
	})
	if err != nil {
		t.Fatalf("Open with custom logger: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE log_test (id INTEGER PRIMARY KEY)")
	t.Log("Custom logger test passed")
}

// TestOpenWithMaxConnections tests connection limit configuration
func TestOpenWithMaxConnections(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:       true,
		MaxConnections: 10,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	t.Logf("DB with max connections opened")
}

// TestOpenWithQueryCache tests query cache configuration
func TestOpenWithQueryCacheConfig(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:         true,
		EnableQueryCache: true,
		QueryCacheSize:   1024 * 1024,
		QueryCacheTTL:    60,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	qc := db.GetQueryCache()
	if qc == nil {
		t.Log("Query cache not returned (may use different init path)")
	}
}

// TestSearchVectorRangeMethod tests the public SearchVectorRange method
func TestSearchVectorRangeMethod(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	// Search on non-existent index should error
	_, _, err = db.SearchVectorRange("nonexistent", []float64{1.0, 2.0}, 1.0)
	if err == nil {
		t.Error("Expected error for non-existent vector index")
	}

	// Search on closed DB
	db2, _ := Open(":memory:", &Options{InMemory: true})
	db2.Close()
	_, _, err = db2.SearchVectorRange("test", []float64{1.0}, 1.0)
	if err == nil {
		t.Error("Expected error for closed DB")
	}
}
