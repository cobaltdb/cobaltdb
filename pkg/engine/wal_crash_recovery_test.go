package engine

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/backup"
)

func durabilityTestOptions() *Options {
	return &Options{
		CacheSize:             128,
		WALEnabled:            BoolPtr(true),
		SyncMode:              SyncFull,
		EnableAutoCheckpoint:  false,
		EnableAutoVacuum:      false,
		EnableScheduler:       false,
		ParallelWorkers:       1,
		ParallelThreshold:     1000,
		SchedulerTickInterval: 0,
	}
}

func TestWALRecoversCommittedWritesAfterProcessExit(t *testing.T) {
	if os.Getenv("COBALTDB_WAL_CRASH_HELPER") == "1" {
		runWALCrashWriter(t)
		os.Exit(0)
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	ctx := context.Background()

	db, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open setup db: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE durable (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE INDEX idx_durable_name ON durable(name)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint setup db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close setup db: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestWALRecoversCommittedWritesAfterProcessExit")
	cmd.Env = append(os.Environ(),
		"COBALTDB_WAL_CRASH_HELPER=1",
		"COBALTDB_WAL_CRASH_DB="+dbPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crash helper failed: %v\n%s", err, out)
	}

	recovered, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open recovered db: %v", err)
	}
	defer recovered.Close()

	assertScalar(t, recovered, "SELECT COUNT(*) FROM durable", int64(2))
	assertScalar(t, recovered, "SELECT SUM(score) FROM durable", float64(35))
	assertScalar(t, recovered, "SELECT score FROM durable WHERE name = 'beta'", int64(25))
}

func TestWALCrashRecoveryIgnoresOpenTransaction(t *testing.T) {
	if os.Getenv("COBALTDB_WAL_OPEN_TX_HELPER") == "1" {
		runWALOpenTransactionWriter(t)
		os.Exit(0)
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "open_tx.db")
	ctx := context.Background()

	db, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open setup db: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts VALUES (1, 100)"); err != nil {
		t.Fatalf("insert account 1: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts VALUES (2, 200)"); err != nil {
		t.Fatalf("insert account 2: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint setup db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close setup db: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestWALCrashRecoveryIgnoresOpenTransaction")
	cmd.Env = append(os.Environ(),
		"COBALTDB_WAL_OPEN_TX_HELPER=1",
		"COBALTDB_WAL_CRASH_DB="+dbPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("open transaction helper failed: %v\n%s", err, out)
	}

	recovered, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open recovered db: %v", err)
	}
	defer recovered.Close()

	assertScalar(t, recovered, "SELECT COUNT(*) FROM accounts", int64(3))
	assertScalar(t, recovered, "SELECT SUM(balance) FROM accounts", float64(600))
	assertScalar(t, recovered, "SELECT balance FROM accounts WHERE id = 1", int64(100))
	assertScalar(t, recovered, "SELECT balance FROM accounts WHERE id = 2", int64(200))
	assertScalar(t, recovered, "SELECT COUNT(*) FROM accounts WHERE id = 4", int64(0))
}

func runWALCrashWriter(t *testing.T) {
	t.Helper()

	dbPath := os.Getenv("COBALTDB_WAL_CRASH_DB")
	if dbPath == "" {
		t.Fatal("COBALTDB_WAL_CRASH_DB is required")
	}

	db, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open crash writer db: %v", err)
	}

	ctx := context.Background()
	statements := []string{
		"INSERT INTO durable (id, name, score) VALUES (1, 'alpha', 10)",
		"INSERT INTO durable (id, name, score) VALUES (2, 'beta', 20)",
		"INSERT INTO durable (id, name, score) VALUES (3, 'gamma', 30)",
		"UPDATE durable SET score = 25 WHERE id = 2",
		"DELETE FROM durable WHERE id = 3",
	}
	for _, stmt := range statements {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	// Intentionally do not call db.Close or Checkpoint. os.Exit below simulates a
	// process crash after committed WAL writes but before dirty page flushing.
}

func runWALOpenTransactionWriter(t *testing.T) {
	t.Helper()

	dbPath := os.Getenv("COBALTDB_WAL_CRASH_DB")
	if dbPath == "" {
		t.Fatal("COBALTDB_WAL_CRASH_DB is required")
	}

	db, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open crash writer db: %v", err)
	}

	ctx := context.Background()
	if _, err := db.Exec(ctx, "INSERT INTO accounts VALUES (3, 300)"); err != nil {
		t.Fatalf("insert committed account: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin open transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = 999 WHERE id = 1"); err != nil {
		t.Fatalf("update in open transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM accounts WHERE id = 2"); err != nil {
		t.Fatalf("delete in open transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO accounts VALUES (4, 400)"); err != nil {
		t.Fatalf("insert in open transaction: %v", err)
	}

	// Intentionally leave tx and db open. The parent process verifies that only
	// the committed autocommit statement is replayed after the simulated crash.
}

func TestIncrementalBackupRestoreOpensAsDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "source.db")
	restorePath := filepath.Join(dir, "restore", "restored.db")
	ctx := context.Background()

	db, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts VALUES (1, 'alice', 100)"); err != nil {
		t.Fatalf("insert initial row: %v", err)
	}

	cfg := backup.DefaultConfig()
	cfg.BackupDir = filepath.Join(dir, "backups")
	cfg.CompressionLevel = 0
	cfg.Verify = true
	manager := backup.NewManager(cfg, db)

	full, err := manager.CreateBackup(ctx, backup.TypeFull)
	if err != nil {
		t.Fatalf("create full backup: %v", err)
	}
	if full.ParentID != "" {
		t.Fatalf("full backup should not have parent, got %q", full.ParentID)
	}

	if _, err := db.Exec(ctx, "UPDATE accounts SET balance = 125 WHERE id = 1"); err != nil {
		t.Fatalf("update row: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts VALUES (2, 'bob', 50)"); err != nil {
		t.Fatalf("insert second row: %v", err)
	}

	incremental, err := manager.CreateBackup(ctx, backup.TypeIncremental)
	if err != nil {
		t.Fatalf("create incremental backup: %v", err)
	}
	if incremental.ParentID != full.ID {
		t.Fatalf("incremental parent = %q, want %q", incremental.ParentID, full.ID)
	}

	if err := manager.Restore(ctx, incremental.ID, restorePath); err != nil {
		t.Fatalf("restore incremental backup: %v", err)
	}

	restored, err := Open(restorePath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer restored.Close()

	assertScalar(t, restored, "SELECT COUNT(*) FROM accounts", int64(2))
	assertScalar(t, restored, "SELECT SUM(balance) FROM accounts", float64(175))
	assertScalar(t, restored, "SELECT balance FROM accounts WHERE owner = 'alice'", int64(125))
}

func assertScalar(t *testing.T, db *DB, query string, want interface{}) {
	t.Helper()

	row := db.QueryRow(context.Background(), query)
	var got interface{}
	if err := row.Scan(&got); err != nil {
		t.Fatalf("scan %q: %v", query, err)
	}
	if normalizeScalar(got) != normalizeScalar(want) {
		t.Fatalf("%s = %v (%T), want %v (%T)", query, got, got, want, want)
	}
}

func normalizeScalar(v interface{}) interface{} {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float32:
		return float64(x)
	default:
		return v
	}
}
