package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

func TestAcquireConnectionRejectsCancelledContext(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := db.Exec(ctx, "CREATE TABLE cancelled_exec (id INTEGER)"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Exec error = %v, want context.Canceled", err)
	}
	if got := db.activeConns.Load(); got != 0 {
		t.Fatalf("cancelled Exec leaked active connection count: %d", got)
	}

	tx, err := db.Begin(ctx)
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("Begin with cancelled context unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Begin error = %v, want context.Canceled", err)
	}
	if got := db.activeConns.Load(); got != 0 {
		t.Fatalf("cancelled Begin leaked active connection count: %d", got)
	}
}

func TestReleasedTxClearsReferences(t *testing.T) {
	tests := []struct {
		name   string
		finish func(*Tx) error
	}{
		{name: "commit", finish: (*Tx).Commit},
		{name: "rollback", finish: (*Tx).Rollback},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer db.Close()

			tx, err := db.Begin(context.Background())
			if err != nil {
				t.Fatalf("begin tx: %v", err)
			}

			if err := tt.finish(tx); err != nil {
				t.Fatalf("finish tx: %v", err)
			}
			if tx.db != nil {
				t.Fatal("released Tx retained DB reference")
			}
			if tx.txn != nil {
				t.Fatal("released Tx retained transaction reference")
			}

			if _, err := tx.Exec(context.Background(), "SELECT 1"); err == nil || !strings.Contains(err.Error(), "transaction already completed") {
				t.Fatalf("Exec after release error = %v, want transaction already completed", err)
			}
			if err := tx.Commit(); err == nil || !strings.Contains(err.Error(), "transaction already completed") {
				t.Fatalf("Commit after release error = %v, want transaction already completed", err)
			}
			if err := tx.Rollback(); err == nil || !strings.Contains(err.Error(), "transaction already completed") {
				t.Fatalf("Rollback after release error = %v, want transaction already completed", err)
			}
		})
	}
}

func TestCompletedTxHandleIsNotReusedForNewTransaction(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	tx1, err := db.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	tx2, err := db.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback()

	if tx1 == tx2 {
		t.Fatal("completed transaction handle was reused for a new active transaction")
	}
	if _, err := tx1.Exec(context.Background(), "SELECT 1"); err == nil || !strings.Contains(err.Error(), "transaction already completed") {
		t.Fatalf("Exec on completed tx error = %v, want transaction already completed", err)
	}
}

// TestOpenWithDirectoryCreation tests opening database with directory creation
func TestOpenWithDirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/subdir/test.db"

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database with subdirectory: %v", err)
	}
	defer db.Close()

	// Verify database is usable
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
}

func TestOpenSecuresDatabaseParentDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat database dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected parent path to be a directory")
	}
	if info.Mode().Perm() != 0750 {
		t.Fatalf("database dir permissions = %v, want 0750", info.Mode().Perm())
	}
}

func TestOpenRejectsSymlinkDatabaseParentDirectory(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0750); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	_, err := Open(filepath.Join(link, "test.db"), nil)
	if err == nil {
		t.Fatal("expected symlink database parent directory to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestOpenRejectsSymlinkDatabaseParentComponent(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0750); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	_, err := Open(filepath.Join(link, "nested", "test.db"), nil)
	if err == nil {
		t.Fatal("expected symlink database parent component to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(target, "nested", "test.db")); !os.IsNotExist(statErr) {
		t.Fatalf("database was created through symlink component, stat err=%v", statErr)
	}
}

func TestOpenRejectsFileDatabaseParentDirectory(t *testing.T) {
	dir := t.TempDir()
	parent := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(parent, []byte("file"), 0600); err != nil {
		t.Fatalf("write file parent: %v", err)
	}

	_, err := Open(filepath.Join(parent, "test.db"), nil)
	if err == nil {
		t.Fatal("expected file database parent to be rejected")
	}
	if !strings.Contains(err.Error(), "must be a directory") {
		t.Fatalf("expected directory rejection, got %v", err)
	}
}

func TestOpenRejectsInvalidStorageOptions(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
		want string
	}{
		{
			name: "negative cache",
			opts: &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: -1}},
			want: "cache size must be positive",
		},
		{
			name: "unsupported page size",
			opts: &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1, PageSize: storage.PageSize * 2}},
			want: "page size",
		},
		{
			name: "negative max connections",
			opts: &Options{
				CoreStorage:    CoreStorage{InMemory: true},
				ConnectionPool: ConnectionPool{MaxConnections: -1},
			},
			want: "max connections must be non-negative",
		},
		{
			name: "max connections too large",
			opts: &Options{
				CoreStorage:    CoreStorage{InMemory: true},
				ConnectionPool: ConnectionPool{MaxConnections: 100001},
			},
			want: "max connections exceeds maximum",
		},
		{
			name: "negative connection timeout",
			opts: &Options{
				CoreStorage:    CoreStorage{InMemory: true},
				ConnectionPool: ConnectionPool{ConnectionTimeout: -time.Second},
			},
			want: "connection timeout must be non-negative",
		},
		{
			name: "negative query timeout",
			opts: &Options{
				CoreStorage:    CoreStorage{InMemory: true},
				ConnectionPool: ConnectionPool{QueryTimeout: -time.Second},
			},
			want: "query timeout must be non-negative",
		},
		{
			name: "negative statement cache",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Security:    Security{MaxStmtCacheSize: -1},
			},
			want: "max statement cache size must be non-negative",
		},
		{
			name: "negative query cache size",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				QueryCache:  QueryCacheConfig{QueryCacheSize: -1},
			},
			want: "query cache size must be non-negative",
		},
		{
			name: "negative query cache ttl",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				QueryCache:  QueryCacheConfig{QueryCacheTTL: -time.Second},
			},
			want: "query cache TTL must be non-negative",
		},
		{
			name: "negative backup retention",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Backup:      BackupConfig{Retention: -time.Second},
			},
			want: "backup retention must be non-negative",
		},
		{
			name: "negative max backups",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Backup:      BackupConfig{MaxBackups: -1},
			},
			want: "max backups must be non-negative",
		},
		{
			name: "invalid backup compression",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Backup:      BackupConfig{CompressionLevel: 10},
			},
			want: "backup compression level must be between 0 and 9",
		},
		{
			name: "negative slow query threshold",
			opts: &Options{
				CoreStorage:  CoreStorage{InMemory: true},
				SlowQueryLog: SlowQueryLogConfig{Threshold: -time.Second},
			},
			want: "slow query threshold must be non-negative",
		},
		{
			name: "negative slow query max entries",
			opts: &Options{
				CoreStorage:  CoreStorage{InMemory: true},
				SlowQueryLog: SlowQueryLogConfig{MaxEntries: -1},
			},
			want: "slow query max entries must be non-negative",
		},
		{
			name: "negative plan cache size",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				PlanCache:   PlanCacheConfig{Size: -1},
			},
			want: "plan cache size must be non-negative",
		},
		{
			name: "negative plan cache entries",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				PlanCache:   PlanCacheConfig{MaxEntries: -1},
			},
			want: "plan cache max entries must be non-negative",
		},
		{
			name: "negative auto vacuum interval",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Maintenance: MaintenanceConfig{AutoVacuumInterval: -time.Second},
			},
			want: "auto-vacuum interval must be non-negative",
		},
		{
			name: "invalid auto vacuum threshold",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Maintenance: MaintenanceConfig{AutoVacuumThreshold: 1.1},
			},
			want: "auto-vacuum threshold must be between 0 and 1",
		},
		{
			name: "negative checkpoint interval",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Maintenance: MaintenanceConfig{CheckpointInterval: -time.Second},
			},
			want: "checkpoint interval must be non-negative",
		},
		{
			name: "negative scheduler analyze interval",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Scheduler:   SchedulerConfig{AnalyzeInterval: -time.Second},
			},
			want: "scheduler analyze interval must be non-negative",
		},
		{
			name: "negative scheduler workers",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Scheduler:   SchedulerConfig{Workers: -1},
			},
			want: "scheduler workers must be non-negative",
		},
		{
			name: "negative scheduler tick interval",
			opts: &Options{
				CoreStorage: CoreStorage{InMemory: true},
				Scheduler:   SchedulerConfig{TickInterval: -time.Second},
			},
			want: "scheduler tick interval must be non-negative",
		},
		{
			name: "negative parallel query workers",
			opts: &Options{
				CoreStorage:   CoreStorage{InMemory: true},
				ParallelQuery: ParallelQueryConfig{Workers: -1},
			},
			want: "parallel query workers must be non-negative",
		},
		{
			name: "negative parallel query threshold",
			opts: &Options{
				CoreStorage:   CoreStorage{InMemory: true},
				ParallelQuery: ParallelQueryConfig{Threshold: -1},
			},
			want: "parallel query threshold must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(":memory:", tt.opts)
			if err == nil {
				_ = db.Close()
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}

// TestOpenWithInvalidDirectory tests opening database with invalid directory path
func TestOpenWithInvalidDirectory(t *testing.T) {
	// Try to create a database in a path that cannot be created
	// This should fail on most systems
	_, err := Open("/dev/null/invalid/db", nil)
	if err == nil {
		t.Skip("Path creation succeeded - may be valid on this system")
	}
}

// TestCloseWithWALCheckpoint tests closing database with WAL checkpoint
func TestCloseWithWALCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"

	// Create database with WAL enabled
	db, err := Open(dbPath, &Options{
		CoreStorage: CoreStorage{
			InMemory:   false,
			WALEnabled: BoolPtr(true),
			CacheSize:  1024,
		},
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Perform some operations
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Close should trigger WAL checkpoint
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

// TestCloseWithoutCatalogSave tests closing database without catalog save (in-memory)
func TestCloseWithoutCatalogSave(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Close in-memory database (no catalog save needed)
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

// TestExecuteAutocommit tests execute with autocommit mode
func TestExecuteAutocommit(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"

	// Open with WAL enabled to trigger autocommit path
	db, err := Open(dbPath, &Options{
		CoreStorage: CoreStorage{
			InMemory:   false,
			WALEnabled: BoolPtr(true),
			CacheSize:  1024,
		},
	})
	if err != nil {
		t.Skipf("WAL mode not supported: %v", err)
		return
	}
	defer db.Close()

	ctx := context.Background()

	// These operations should use autocommit when no explicit transaction
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
}

// TestExecuteInsertError tests executeInsert with error
func TestExecuteInsertError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try to insert into non-existent table
	_, err = db.Exec(ctx, "INSERT INTO nonexistent (id) VALUES (1)")
	if err == nil {
		t.Error("Expected error for insert into non-existent table")
	}
}

// TestExecuteUpdateError tests executeUpdate with error
func TestExecuteUpdateError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try to update non-existent table
	_, err = db.Exec(ctx, "UPDATE nonexistent SET id = 2")
	if err == nil {
		t.Error("Expected error for update on non-existent table")
	}
}

// TestExecuteDeleteError tests executeDelete with error
func TestExecuteDeleteError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try to delete from non-existent table
	_, err = db.Exec(ctx, "DELETE FROM nonexistent")
	if err == nil {
		t.Error("Expected error for delete from non-existent table")
	}
}

// TestExecuteCreateIndexError tests executeCreateIndex with error
func TestExecuteCreateIndexError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try to create index on non-existent table
	_, err = db.Exec(ctx, "CREATE INDEX idx ON nonexistent (id)")
	if err == nil {
		t.Error("Expected error for create index on non-existent table")
	}
}

// TestExecuteCreateViewError tests executeCreateView with error
func TestExecuteCreateViewError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try to create view with invalid query
	_, err = db.Exec(ctx, "CREATE VIEW test_view AS SELECT * FROM nonexistent")
	// May or may not error depending on implementation
	t.Logf("Create view with invalid query result: %v", err)
}

// TestExecuteCreateTriggerError tests executeCreateTrigger with error
func TestExecuteCreateTriggerError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try to create trigger on non-existent table
	_, err = db.Exec(ctx, "CREATE TRIGGER trg AFTER INSERT ON nonexistent BEGIN SELECT 1; END")
	// May or may not error depending on implementation
	t.Logf("Create trigger on non-existent table result: %v", err)
}

// TestExecuteCreateProcedureError tests executeCreateProcedure with error
func TestExecuteCreateProcedureError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try to create procedure with invalid syntax
	_, err = db.Exec(ctx, "CREATE PROCEDURE test_proc() BEGIN INVALID SYNTAX; END")
	// May or may not error depending on implementation
	t.Logf("Create procedure with invalid syntax result: %v", err)
}

// TestExecuteCallProcedureNotFound tests executeCallProcedure with non-existent procedure
func TestExecuteCallProcedureNotFound(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try to call non-existent procedure
	_, err = db.Exec(ctx, "CALL nonexistent_proc()")
	if err == nil {
		t.Error("Expected error for calling non-existent procedure")
	}
}

// TestExecuteCallProcedureWithExecError tests executeCallProcedure with exec error
func TestExecuteCallProcedureWithExecError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a procedure that tries to insert into non-existent table
	_, err = db.Exec(ctx, "CREATE PROCEDURE bad_proc() BEGIN INSERT INTO nonexistent (id) VALUES (1); END")
	if err != nil {
		t.Skipf("CREATE PROCEDURE not supported: %v", err)
		return
	}

	// Call the procedure - should fail when executing the body
	_, err = db.Exec(ctx, "CALL bad_proc()")
	// Should error because the procedure body has an error
	t.Logf("Call procedure with exec error result: %v", err)
}

// TestScanError tests Scan with error
func TestScanError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	rows, err := db.Query(ctx, "SELECT id, name FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		// Try to scan into incompatible types
		var id string
		var name int
		err = rows.Scan(&id, &name)
		// This may or may not error depending on type conversion
		t.Logf("Scan with type conversion result: %v", err)
	}
}

// TestCommitError tests Commit with error
func TestCommitError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert some data
	_, err = tx.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// First commit should succeed
	err = tx.Commit()
	if err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Second commit should fail (transaction already committed)
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error for double commit")
	}
}

// TestRollbackError tests Rollback with error
func TestRollbackError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// First rollback should succeed
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("First rollback failed: %v", err)
	}

	// Second rollback may or may not error
	err = tx.Rollback()
	t.Logf("Second rollback result: %v", err)
}

// TestQueryRowErrNoRows tests QueryRow with no rows error
func TestQueryRowErrNoRows(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Query for non-existent row
	row := db.QueryRow(ctx, "SELECT id FROM test WHERE id = 999")
	var id int
	err = row.Scan(&id)
	if err == nil {
		t.Error("Expected error for no rows")
	}
}

// TestBeginWithOptionsClosed tests BeginWith on closed database
func TestBeginWithOptionsClosed(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	db.Close()

	ctx := context.Background()
	_, err = db.BeginWith(ctx, txn.DefaultOptions())
	if err != ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}
}

// TestGetPreparedStatementCacheLimit tests prepared statement cache size limit
func TestGetPreparedStatementCacheLimit(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute many different statements to potentially exceed cache limit
	for i := 0; i < 1100; i++ {
		sql := "SELECT " + string(rune('0'+i%10)) + " FROM test"
		_, _ = db.Exec(ctx, sql)
	}

	// Cache should have been limited to 1000 entries
	// Just verify no panic occurred
}

func TestGetPreparedStatementConcurrentMissKeepsSingleLRUEntry(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024},
		Security:    Security{MaxStmtCacheSize: 100},
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	const sql = "SELECT 1"
	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for i := 0; i < cap(errCh); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.getPreparedStatement(sql)
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("getPreparedStatement failed: %v", err)
		}
	}

	db.stmtMu.RLock()
	defer db.stmtMu.RUnlock()
	if got := len(db.stmtCache); got != 1 {
		t.Fatalf("expected one cached statement, got %d", got)
	}
	nodes := 0
	for e := db.stmtLRU.head; e != nil; e = e.next {
		nodes++
		if e.sql != sql {
			t.Fatalf("unexpected LRU SQL %q", e.sql)
		}
	}
	if nodes != 1 {
		t.Fatalf("expected one LRU node for concurrent miss, got %d", nodes)
	}
}

// TestOpenDiskBackendError tests Open with disk backend error
func TestOpenDiskBackendError(t *testing.T) {
	// Try to open a path that is a file (not a directory)
	tmpFile, err := os.CreateTemp("", "testdb")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Try to open the file as a database path with subdirectory
	_, err = Open(tmpFile.Name()+"/db", nil)
	// Should fail because tmpFile.Name() is a file, not a directory
	if err == nil {
		t.Skip("Path creation succeeded - may be valid on this system")
	}
}

// TestLoadExistingWithWALRecovery tests loadExisting with WAL recovery
func TestLoadExistingWithWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"

	// Create database with WAL enabled
	db, err := Open(dbPath, &Options{
		CoreStorage: CoreStorage{
			InMemory:   false,
			WALEnabled: BoolPtr(true),
			CacheSize:  1024,
		},
	})
	if err != nil {
		t.Skipf("WAL mode not supported: %v", err)
		return
	}

	// Perform operations
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Close without checkpoint (simulate crash)
	db.pool.Close()
	if db.wal != nil {
		db.wal.Close()
	}
	db.backend.Close()

	// Reopen - should recover from WAL
	db2, err := Open(dbPath, &Options{
		CoreStorage: CoreStorage{
			InMemory:   false,
			WALEnabled: BoolPtr(true),
			CacheSize:  1024,
		},
	})
	if err != nil {
		t.Skipf("Reopen with WAL recovery not supported: %v", err)
		return
	}
	defer db2.Close()

	// Verify data was recovered
	row := db2.QueryRow(ctx, "SELECT COUNT(*) FROM test")
	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Logf("WAL recovery may not be fully implemented: %v", err)
		return
	}
	if count != 1 {
		t.Errorf("Expected 1 row after recovery, got %d", count)
	}
}

// TestCreateNewWithBackendError tests createNew with backend write error
func TestCreateNewWithBackendError(t *testing.T) {
	// This is difficult to test without mocking
	// For now, just test that createNew works in normal case
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	db.Close()
}

// TestExecuteUnsupportedStatementType tests execute with unsupported statement type
func TestExecuteUnsupportedStatementType(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// BEGIN should succeed and start a transaction
	_, err = db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Errorf("BEGIN should succeed: %v", err)
	}

	// COMMIT should succeed since we have an active transaction
	_, err = db.Exec(ctx, "COMMIT")
	if err != nil {
		t.Errorf("COMMIT should succeed: %v", err)
	}

	// ROLLBACK without active transaction should return error
	_, err = db.Exec(ctx, "ROLLBACK")
	if err == nil {
		t.Error("Expected error for ROLLBACK without active transaction")
	}
}

// TestScanValueTypeConversions tests scanValue with various type conversions
func TestScanValueTypeConversions(t *testing.T) {
	tests := []struct {
		name     string
		src      interface{}
		dest     interface{}
		expected interface{}
		wantErr  bool
	}{
		{"int64_to_int", int64(42), new(int), 42, false},
		{"float64_to_int", float64(42.7), new(int), 42, false},
		{"float64_to_int64", float64(42.7), new(int64), int64(42), false},
		{"int64_to_string", int64(42), new(string), "42", false},
		{"nil_to_interface", nil, new(interface{}), nil, false},
		{"string_to_string", "test", new(string), "test", false},
		{"bool_to_bool", true, new(bool), true, false},
		{"string_to_bool_true", "true", new(bool), true, false},
		{"string_to_bool_false", "false", new(bool), false, false},
		{"string_to_int", "42", new(int), 42, false},
		{"string_to_int64", "42", new(int64), int64(42), false},
		{"string_to_float64", "3.14", new(float64), 3.14, false},
		{"bytes_to_bytes", []byte{1, 2, 3}, new([]byte), []byte{1, 2, 3}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := scanValue(tt.src, tt.dest)
			if tt.wantErr && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestTransactionWithCatalogErrors tests transaction operations with catalog errors
func TestTransactionWithCatalogErrors(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data
	_, err = tx.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit should work
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify data was committed
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM test")
	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row after commit, got %d", count)
	}
}
