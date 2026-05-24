package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// TestACID_RollbackConsistency verifies that rolled-back changes are invisible.
func TestACID_RollbackConsistency(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "acid.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert committed row
	if _, err := db.Exec(ctx, "INSERT INTO t (id, val) VALUES (1, 'committed')"); err != nil {
		t.Fatalf("insert committed: %v", err)
	}

	// Start transaction, insert, rollback
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, val) VALUES (2, 'rolled-back')"); err != nil {
		t.Fatalf("insert rollback: %v", err)
	}
	if _, err := db.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Only committed row should be visible
	rows, err := db.Query(ctx, "SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1], got %v", ids)
	}
}

// TestACID_CommitDurability verifies committed data survives close/reopen.
func TestACID_CommitDurability(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "durability.db")

	db, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, val) VALUES (42, 'persist')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify
	db2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(ctx, "SELECT val FROM t WHERE id = 42")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected row after reopen")
	}
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != "persist" {
		t.Fatalf("expected 'persist', got %q", val)
	}
}

// TestStress_ConcurrentReadsWrites runs concurrent writers and readers for 3 seconds.
func TestStress_ConcurrentReadsWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "stress.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE counters (id INT PRIMARY KEY, count INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO counters (id, count) VALUES (%d, 0)", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	var wg sync.WaitGroup
	duration := 3 * time.Second
	deadline := time.Now().Add(duration)

	errors := make(chan error, 100)

	// Writers
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				_, err := db.Exec(ctx, fmt.Sprintf("UPDATE counters SET count = count + 1 WHERE id = %d", id%5))
				if err != nil {
					errors <- fmt.Errorf("writer %d: %w", id, err)
					return
				}
			}
		}(w)
	}

	// Readers
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				rows, err := db.Query(ctx, "SELECT count FROM counters WHERE id = 0")
				if err != nil {
					errors <- fmt.Errorf("reader %d query: %w", id, err)
					return
				}
				for rows.Next() {
					var c int
					if err := rows.Scan(&c); err != nil {
						errors <- fmt.Errorf("reader %d scan: %w", id, err)
						rows.Close()
						return
					}
				}
				rows.Close()
			}
		}(r)
	}

	wg.Wait()
	close(errors)

	for e := range errors {
		t.Error(e)
	}
}

func TestProductionSoakBoundedCheckpointBackupReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "soak.db")
	opts := durabilityTestOptions()
	opts.BackupDir = filepath.Join(dir, "backups")
	opts.MaxBackups = 4
	opts.BackupCompressionLevel = 0

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := db.Exec(ctx, "CREATE TABLE ledger (id INTEGER PRIMARY KEY, account_id INTEGER, amount INTEGER)"); err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)"); err != nil {
		t.Fatalf("create accounts: %v", err)
	}

	const workers = 4
	const iterations = 25
	for i := 0; i < workers; i++ {
		if _, err := db.Exec(ctx, "INSERT INTO accounts VALUES (?, 0)", i); err != nil {
			t.Fatalf("seed account %d: %v", i, err)
		}
	}

	errs := make(chan error, workers*iterations+64)
	var workload sync.WaitGroup
	stopCheckpoint := make(chan struct{})
	var checkpointWG sync.WaitGroup

	checkpointWG.Add(1)
	go func() {
		defer checkpointWG.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCheckpoint:
				return
			case <-ticker.C:
				if err := db.Checkpoint(); err != nil && !errors.Is(err, ErrDatabaseClosed) {
					errs <- fmt.Errorf("checkpoint: %w", err)
					return
				}
			}
		}
	}()

	for worker := 0; worker < workers; worker++ {
		workload.Add(1)
		go func(workerID int) {
			defer workload.Done()
			amount := workerID + 1
			for i := 0; i < iterations; i++ {
				id := workerID*iterations + i + 1
				tx, err := db.Begin(ctx)
				if err != nil {
					errs <- fmt.Errorf("worker %d begin: %w", workerID, err)
					return
				}
				if _, err := tx.Exec(ctx, "INSERT INTO ledger VALUES (?, ?, ?)", id, workerID, amount); err != nil {
					_ = tx.Rollback()
					errs <- fmt.Errorf("worker %d insert %d: %w", workerID, id, err)
					return
				}
				if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance + ? WHERE id = ?", amount, workerID); err != nil {
					_ = tx.Rollback()
					errs <- fmt.Errorf("worker %d update %d: %w", workerID, id, err)
					return
				}
				if err := tx.Commit(); err != nil {
					errs <- fmt.Errorf("worker %d commit %d: %w", workerID, id, err)
					return
				}
			}
		}(worker)
	}

	for reader := 0; reader < 2; reader++ {
		workload.Add(1)
		go func(readerID int) {
			defer workload.Done()
			for i := 0; i < iterations; i++ {
				row := db.QueryRow(ctx, "SELECT COUNT(*) FROM ledger")
				var count int64
				if err := row.Scan(&count); err != nil {
					errs <- fmt.Errorf("reader %d count: %w", readerID, err)
					return
				}
				row = db.QueryRow(ctx, "SELECT SUM(balance) FROM accounts")
				var total float64
				if err := row.Scan(&total); err != nil && !strings.Contains(err.Error(), "cannot scan <nil>") {
					errs <- fmt.Errorf("reader %d sum: %w", readerID, err)
					return
				}
			}
		}(reader)
	}

	workload.Wait()
	close(stopCheckpoint)
	checkpointWG.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	expectedRows := int64(workers * iterations)
	expectedTotal := float64(iterations * (workers * (workers + 1) / 2))
	assertScalar(t, db, "SELECT COUNT(*) FROM ledger", expectedRows)
	assertScalar(t, db, "SELECT SUM(amount) FROM ledger", expectedTotal)
	assertScalar(t, db, "SELECT SUM(balance) FROM accounts", expectedTotal)

	b, err := db.CreateBackup(ctx, "full")
	if err != nil {
		t.Fatalf("create backup: %v", err)
	}
	restorePath := filepath.Join(dir, "restore", "soak-restored.db")
	if err := db.backupMgr.Restore(ctx, b.ID, restorePath); err != nil {
		t.Fatalf("restore backup: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	reopened, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("reopen source: %v", err)
	}
	defer reopened.Close()
	assertScalar(t, reopened, "SELECT COUNT(*) FROM ledger", expectedRows)
	assertScalar(t, reopened, "SELECT SUM(balance) FROM accounts", expectedTotal)

	restored, err := Open(restorePath, opts)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	assertScalar(t, restored, "SELECT COUNT(*) FROM ledger", expectedRows)
	assertScalar(t, restored, "SELECT SUM(amount) FROM ledger", expectedTotal)
	assertScalar(t, restored, "SELECT SUM(balance) FROM accounts", expectedTotal)
}

// TestStress_Extended runs concurrent writers and readers for 60 seconds.
func TestStress_Extended(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping extended stress test in short mode")
	}

	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "stress_ext.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE counters (id INT PRIMARY KEY, count INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO counters (id, count) VALUES (%d, 0)", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	var wg sync.WaitGroup
	duration := 10 * time.Second
	deadline := time.Now().Add(duration)

	errors := make(chan error, 1000)

	// Writers
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				_, err := db.Exec(ctx, fmt.Sprintf("UPDATE counters SET count = count + 1 WHERE id = %d", id%5))
				if err != nil {
					errors <- fmt.Errorf("writer %d: %w", id, err)
					return
				}
			}
		}(w)
	}

	// Readers
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				rows, err := db.Query(ctx, "SELECT count FROM counters WHERE id = 0")
				if err != nil {
					errors <- fmt.Errorf("reader %d query: %w", id, err)
					return
				}
				for rows.Next() {
					var c int
					if err := rows.Scan(&c); err != nil {
						errors <- fmt.Errorf("reader %d scan: %w", id, err)
						rows.Close()
						return
					}
				}
				rows.Close()
			}
		}(r)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(120 * time.Second):
		t.Fatal("TestStress_Extended timed out waiting for goroutines")
	}
	close(errors)

	for e := range errors {
		t.Error(e)
	}
}

// TestDeadlockDetection verifies that conflicting transactions are resolved.
func TestDeadlockDetection(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "deadlock.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 100)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Start two transactions that update rows in opposite order.
	// With deadlock detection, one should be aborted automatically.
	var wg sync.WaitGroup
	wg.Add(2)

	results := make(chan error, 2)

	go func() {
		defer wg.Done()
		_, err := db.Exec(ctx, "BEGIN")
		if err != nil {
			results <- err
			return
		}
		_, err = db.Exec(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = 1")
		if err != nil {
			results <- err
			return
		}
		// Small sleep to increase chance of overlap
		time.Sleep(20 * time.Millisecond)
		_, err = db.Exec(ctx, "UPDATE accounts SET balance = balance + 10 WHERE id = 2")
		if err != nil {
			results <- err
			return
		}
		_, err = db.Exec(ctx, "COMMIT")
		results <- err
	}()

	go func() {
		defer wg.Done()
		_, err := db.Exec(ctx, "BEGIN")
		if err != nil {
			results <- err
			return
		}
		_, err = db.Exec(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = 2")
		if err != nil {
			results <- err
			return
		}
		time.Sleep(20 * time.Millisecond)
		_, err = db.Exec(ctx, "UPDATE accounts SET balance = balance + 10 WHERE id = 1")
		if err != nil {
			results <- err
			return
		}
		_, err = db.Exec(ctx, "COMMIT")
		results <- err
	}()

	wg.Wait()
	close(results)

	success := 0
	failure := 0
	for err := range results {
		if err == nil {
			success++
		} else {
			failure++
		}
	}
	// At least one should succeed; deadlock detection may abort one.
	if success == 0 {
		t.Fatal("expected at least one transaction to commit")
	}
	if success+failure != 2 {
		t.Fatalf("unexpected result count: success=%d failure=%d", success, failure)
	}
}

// TestBackupRestoreRoundTrip creates a backup and restores it to a new database.
func TestBackupRestoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	restorePath := filepath.Join(dir, "restored.db")

	db, err := Open(srcPath, nil)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO items (id, name) VALUES (1, 'apple'), (2, 'banana')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	backupDir := filepath.Join(dir, "backups")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatalf("mkdir backups: %v", err)
	}

	b, err := db.CreateBackup(ctx, "full")
	if err != nil {
		t.Fatalf("create backup: %v", err)
	}

	// Restore before closing source DB so backup manager is still valid
	tmpMgr := db.backupMgr
	if tmpMgr == nil {
		t.Skip("backup manager not initialized")
	}
	if err := tmpMgr.Restore(ctx, b.ID, restorePath); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify backup and restored files exist and have non-zero size
	if fi, err := os.Stat(b.Destination); err != nil || fi.Size() == 0 {
		t.Fatalf("backup file missing or empty: %v", err)
	}
	if fi, err := os.Stat(restorePath); err != nil || fi.Size() == 0 {
		t.Fatalf("restored file missing or empty: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	// Reopen restored database and verify data
	db2, err := Open(restorePath, nil)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(ctx, "SELECT id, name FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("query restored: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id   int
		name string
	}{{1, "apple"}, {2, "banana"}}
	for i, exp := range expected {
		if !rows.Next() {
			t.Fatalf("expected row %d", i)
		}
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if id != exp.id || name != exp.name {
			t.Fatalf("row %d: expected (%d,%s), got (%d,%s)", i, exp.id, exp.name, id, name)
		}
	}
	if rows.Next() {
		t.Fatal("unexpected extra row")
	}
}

// TestStress_ConcurrentExplicitTransactions_MultiTable validates that explicit
// transactions on independent tables commit in parallel without conflicts.
func TestStress_ConcurrentExplicitTransactions_MultiTable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "mvcc_multi.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE ta (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
		t.Fatalf("create table ta: %v", err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE tb (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
		t.Fatalf("create table tb: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO ta VALUES (%d, 0)", i)); err != nil {
			t.Fatalf("insert ta: %v", err)
		}
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO tb VALUES (%d, 0)", i)); err != nil {
			t.Fatalf("insert tb: %v", err)
		}
	}

	workers := 4
	iterations := 25

	var successCount int64
	var conflictCount int64
	var otherErrCount int64

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			table := "ta"
			if id%2 == 1 {
				table = "tb"
			}
			rowID := id / 2 // workers 0,2 -> ta rows 0,1 ; workers 1,3 -> tb rows 0,1
			for j := 0; j < iterations; {
				tx, err := db.Begin(ctx)
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Begin error: %v", id, err)
					return
				}
				_, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET value = value + 1 WHERE id = %d", table, rowID))
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Update error: %v", id, err)
					tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
					if errors.Is(err, txn.ErrConflict) {
						atomic.AddInt64(&conflictCount, 1)
						continue
					}
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Commit error: %v", id, err)
					return
				}
				atomic.AddInt64(&successCount, 1)
				j++
			}
		}(i)
	}
	wg.Wait()

	if otherErrCount > 0 {
		t.Fatalf("unexpected errors: %d", otherErrCount)
	}
	if successCount != int64(workers*iterations) {
		t.Fatalf("expected %d successes, got %d (conflicts=%d)", workers*iterations, successCount, conflictCount)
	}

	for _, table := range []string{"ta", "tb"} {
		for rowID := 0; rowID < 2; rowID++ {
			var value int
			r := db.QueryRow(ctx, fmt.Sprintf("SELECT value FROM %s WHERE id = %d", table, rowID))
			if err := r.Scan(&value); err != nil {
				t.Fatalf("read %s[%d]: %v", table, rowID, err)
			}
			expected := iterations // one worker per row
			if value != expected {
				t.Fatalf("lost update in %s[%d]: expected %d, got %d", table, rowID, expected, value)
			}
		}
	}
	t.Logf("success=%d conflicts=%d", successCount, conflictCount)
}

// TestStress_ConcurrentExplicitTransactions_Conflict validates MVCC conflict
// detection when two explicit transactions compete on the same row.
func TestStress_ConcurrentExplicitTransactions_Conflict(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "mvcc_conflict.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO counter VALUES (1, 0)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	workers := 2
	iterations := 10

	var successCount int64
	var conflictCount int64
	var otherErrCount int64

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; {
				tx, err := db.Begin(ctx)
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Begin error: %v", id, err)
					return
				}
				_, err = tx.Exec(ctx, "UPDATE counter SET value = value + 1 WHERE id = 1")
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Update error: %v", id, err)
					tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
					if errors.Is(err, txn.ErrConflict) {
						atomic.AddInt64(&conflictCount, 1)
						continue
					}
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Commit error: %v", id, err)
					return
				}
				atomic.AddInt64(&successCount, 1)
				j++
			}
		}(i)
	}
	wg.Wait()

	var value int
	r := db.QueryRow(ctx, "SELECT value FROM counter WHERE id = 1")
	if err := r.Scan(&value); err != nil {
		t.Fatalf("final read: %v", err)
	}
	expected := workers * iterations
	if value != expected {
		t.Fatalf("lost update: expected value=%d, got value=%d (success=%d conflicts=%d other=%d)",
			expected, value, successCount, conflictCount, otherErrCount)
	}
	if otherErrCount > 0 {
		t.Fatalf("unexpected errors: %d", otherErrCount)
	}
	if conflictCount == 0 {
		t.Logf("warning: zero conflicts detected — concurrency may be lower than expected")
	}
	t.Logf("success=%d conflicts=%d value=%d", successCount, conflictCount, value)
}

// TestStress_ConcurrentExplicitTransactions_Insert validates that concurrent
// INSERT operations with auto-increment primary keys produce no lost rows or
// duplicate keys under buffered write mode.
func TestStress_ConcurrentExplicitTransactions_Insert(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "insert.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	workers := 4
	iterations := 25

	var successCount int64
	var otherErrCount int64

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				tx, err := db.Begin(ctx)
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Begin error: %v", id, err)
					return
				}
				_, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO items (name) VALUES ('worker-%d-row-%d')", id, j))
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Insert error: %v", id, err)
					tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Commit error: %v", id, err)
					return
				}
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if otherErrCount > 0 {
		t.Fatalf("unexpected errors: %d", otherErrCount)
	}
	if successCount != int64(workers*iterations) {
		t.Fatalf("expected %d successes, got %d", workers*iterations, successCount)
	}

	var count int
	r := db.QueryRow(ctx, "SELECT COUNT(*) FROM items")
	if err := r.Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != workers*iterations {
		t.Fatalf("expected %d rows, got %d", workers*iterations, count)
	}

	var maxID int
	r = db.QueryRow(ctx, "SELECT MAX(id) FROM items")
	if err := r.Scan(&maxID); err != nil {
		t.Fatalf("max id: %v", err)
	}
	if maxID != workers*iterations {
		t.Fatalf("expected max id %d, got %d", workers*iterations, maxID)
	}
	t.Logf("inserted=%d max_id=%d", count, maxID)
}

// TestStress_ConcurrentExplicitTransactions_Delete validates that concurrent
// DELETE operations on distinct rows remove exactly the intended rows without
// conflicts or lost updates.
func TestStress_ConcurrentExplicitTransactions_Delete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "delete.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE del_items (id INTEGER PRIMARY KEY, flag TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	workers := 4
	iterations := 25
	totalRows := workers * iterations

	// Seed rows
	for i := 0; i < totalRows; i++ {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO del_items VALUES (%d, 'row-%d')", i, i)); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}

	var successCount int64
	var conflictCount int64
	var otherErrCount int64

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; {
				tx, err := db.Begin(ctx)
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Begin error: %v", id, err)
					return
				}
				rowID := id*iterations + j
				_, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM del_items WHERE id = %d", rowID))
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Delete error: %v", id, err)
					tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
					if errors.Is(err, txn.ErrConflict) {
						atomic.AddInt64(&conflictCount, 1)
						continue
					}
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Commit error: %v", id, err)
					return
				}
				atomic.AddInt64(&successCount, 1)
				j++
			}
		}(i)
	}
	wg.Wait()

	if otherErrCount > 0 {
		t.Fatalf("unexpected errors: %d", otherErrCount)
	}
	if successCount != int64(totalRows) {
		t.Fatalf("expected %d successes, got %d (conflicts=%d)", totalRows, successCount, conflictCount)
	}

	var count int
	r := db.QueryRow(ctx, "SELECT COUNT(*) FROM del_items")
	if err := r.Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows, got %d", count)
	}
	t.Logf("deleted=%d conflicts=%d remaining=%d", successCount, conflictCount, count)
}

// TestStress_ConcurrentExplicitTransactions_Mixed runs a mixed workload of
// INSERT, UPDATE, and DELETE across multiple workers to validate the buffered
// write path under realistic contention.
func TestStress_ConcurrentExplicitTransactions_Mixed(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "mixed.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, amount INTEGER, status TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// Seed some rows for UPDATE and DELETE
	for i := 0; i < 20; i++ {
		if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO ledger (amount, status) VALUES (%d, 'active')", i*10)); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}

	workers := 4
	iterations := 15

	var insertCount int64
	var updateCount int64
	var deleteCount int64
	var conflictCount int64
	var otherErrCount int64

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; {
				tx, err := db.Begin(ctx)
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					return
				}

				var res Result
				switch j % 3 {
				case 0: // INSERT
					res, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO ledger (amount, status) VALUES (%d, 'new')", id*1000+j))
					if err == nil && res.RowsAffected > 0 {
						atomic.AddInt64(&insertCount, 1)
					}
				case 1: // UPDATE
					rowID := (id+j)%20 + 1
					res, err = tx.Exec(ctx, fmt.Sprintf("UPDATE ledger SET amount = amount + 1 WHERE id = %d", rowID))
					if err == nil && res.RowsAffected > 0 {
						atomic.AddInt64(&updateCount, 1)
					}
				case 2: // DELETE
					rowID := 20 + (id*iterations+j)%10
					res, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM ledger WHERE id = %d", rowID))
					if err == nil && res.RowsAffected > 0 {
						atomic.AddInt64(&deleteCount, 1)
					}
				}

				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d op error: %v", id, err)
					tx.Rollback()
					return
				}

				if err := tx.Commit(); err != nil {
					if errors.Is(err, txn.ErrConflict) {
						atomic.AddInt64(&conflictCount, 1)
						continue
					}
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Commit error: %v", id, err)
					return
				}
				j++
			}
		}(i)
	}
	wg.Wait()

	if otherErrCount > 0 {
		t.Fatalf("unexpected errors: %d", otherErrCount)
	}

	// Consistency checks (exact row count is nondeterministic due to
	// interleaved INSERT/DELETE races, so we verify structural integrity).
	var remaining int
	r := db.QueryRow(ctx, "SELECT COUNT(*) FROM ledger")
	if err := r.Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining < 0 {
		t.Fatalf("negative row count: %d", remaining)
	}

	// Verify no duplicate IDs and no soft-deleted rows are visible
	rows, err := db.Query(ctx, "SELECT id, amount, status FROM ledger ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	seenIDs := make(map[int]bool)
	for rows.Next() {
		var id int
		var amount int
		var status string
		if err := rows.Scan(&id, &amount, &status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if seenIDs[id] {
			t.Fatalf("duplicate row id: %d", id)
		}
		seenIDs[id] = true
		if status != "active" && status != "new" {
			t.Fatalf("unexpected status for id %d: %q", id, status)
		}
	}
	if len(seenIDs) != remaining {
		t.Fatalf("row count mismatch: COUNT(*)=%d but scanned %d rows", remaining, len(seenIDs))
	}

	t.Logf("inserts=%d updates=%d deletes=%d conflicts=%d remaining=%d",
		insertCount, updateCount, deleteCount, conflictCount, remaining)
}

// TestStress_ConcurrentExplicitTransactions_InsertConflict validates MVCC
// conflict detection when multiple transactions attempt to INSERT the same
// explicit primary key.
func TestStress_ConcurrentExplicitTransactions_InsertConflict(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "insert_conflict.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE kv (id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	workers := 4
	iterations := 10

	var successCount int64
	var conflictCount int64
	var otherErrCount int64

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; {
				tx, err := db.Begin(ctx)
				if err != nil {
					atomic.AddInt64(&otherErrCount, 1)
					return
				}
				// All workers compete for the same 5 keys (1-5)
				key := (j % 5) + 1
				_, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO kv VALUES (%d, 'worker-%d-%d')", key, id, j))
				if err != nil {
					// Another transaction already inserted this key; treat as conflict
					// and move to the next key.
					atomic.AddInt64(&conflictCount, 1)
					tx.Rollback()
					j++
					continue
				}
				if err := tx.Commit(); err != nil {
					if errors.Is(err, txn.ErrConflict) {
						atomic.AddInt64(&conflictCount, 1)
						continue
					}
					atomic.AddInt64(&otherErrCount, 1)
					t.Logf("worker %d Commit error: %v", id, err)
					return
				}
				atomic.AddInt64(&successCount, 1)
				j++
			}
		}(i)
	}
	wg.Wait()

	if otherErrCount > 0 {
		t.Fatalf("unexpected errors: %d", otherErrCount)
	}

	// Each of the 5 keys should have exactly one successful insert
	var count int
	r := db.QueryRow(ctx, "SELECT COUNT(*) FROM kv")
	if err := r.Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 rows (one per key), got %d", count)
	}

	// Verify no duplicate keys
	rows, err := db.Query(ctx, "SELECT id FROM kv")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	seen := make(map[int]bool)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate key: %d", id)
		}
		seen[id] = true
	}

	t.Logf("success=%d conflicts=%d rows=%d", successCount, conflictCount, count)
}

// TestStress_ConcurrentAutocommit_Insert verifies that multiple goroutines can
// safely perform autocommit INSERTs concurrently without lost updates.
func TestStress_ConcurrentAutocommit_Insert(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "autocommit_insert.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE items (id INT PRIMARY KEY, worker INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	workers := 4
	iterations := 25
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := id*iterations + j + 1
				if _, err := db.Exec(ctx, fmt.Sprintf("INSERT INTO items VALUES (%d, %d)", key, id)); err != nil {
					t.Logf("worker %d insert error: %v", id, err)
				}
			}
		}(i)
	}
	wg.Wait()

	var count int
	r := db.QueryRow(ctx, "SELECT COUNT(*) FROM items")
	if err := r.Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	expected := workers * iterations
	if count != expected {
		t.Fatalf("expected %d rows, got %d", expected, count)
	}

	t.Logf("inserted=%d expected=%d", count, expected)
}

// TestStress_ConcurrentAutocommit_Mixed verifies that concurrent autocommit
// INSERT, UPDATE, and DELETE operations maintain structural consistency.
func TestStress_ConcurrentAutocommit_Mixed(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "autocommit_mixed.db"), nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE ledger (id INT PRIMARY KEY, status TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	workers := 4
	iterations := 10
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				switch j % 3 {
				case 0:
					key := id*iterations + j + 1
					db.Exec(ctx, fmt.Sprintf("INSERT INTO ledger VALUES (%d, 'new')", key))
				case 1:
					key := id*iterations + j
					if key > 0 {
						db.Exec(ctx, fmt.Sprintf("UPDATE ledger SET status = 'updated' WHERE id = %d", key))
					}
				case 2:
					key := id*iterations + j - 1
					if key > 0 {
						db.Exec(ctx, fmt.Sprintf("DELETE FROM ledger WHERE id = %d", key))
					}
				}
			}
		}(i)
	}
	wg.Wait()

	// Verify no duplicate IDs and all statuses are valid.
	rows, err := db.Query(ctx, "SELECT id, status FROM ledger")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	seen := make(map[int]bool)
	for rows.Next() {
		var id int
		var status string
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate id: %d", id)
		}
		seen[id] = true
		if status != "new" && status != "updated" {
			t.Fatalf("unexpected status for id %d: %s", id, status)
		}
	}

	t.Logf("remaining rows=%d", len(seen))
}

// BenchmarkConcurrentWriters measures throughput of concurrent INSERT
// operations with varying numbers of goroutines.
func BenchmarkConcurrentWriters(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(filepath.Join(dir, "bench.db"), &Options{
		EnableScheduler:      false,
		EnableAutoCheckpoint: false,
		EnableAutoVacuum:     false,
	})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE bench (id INT PRIMARY KEY, n INT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}

	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				perWorker := 100
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(id, base int) {
						defer wg.Done()
						for j := 0; j < perWorker; j++ {
							key := base + id*perWorker + j
							db.Exec(ctx, "INSERT INTO bench VALUES (?, ?)", key, id)
						}
					}(w, i*workers*perWorker)
				}
				wg.Wait()
			}
			b.ReportMetric(float64(workers*100*b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

// BenchmarkConcurrentWritersBatch measures throughput of concurrent batch
// INSERT operations (multiple rows per statement).
func BenchmarkConcurrentWritersBatch(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(filepath.Join(dir, "bench.db"), &Options{
		EnableScheduler:      false,
		EnableAutoCheckpoint: false,
		EnableAutoVacuum:     false,
	})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE batch (id INT PRIMARY KEY, n INT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}

	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			batchSize := 50
			perWorker := 10 // 10 batches * 50 rows = 500 rows per worker
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(id, base int) {
						defer wg.Done()
						for j := 0; j < perWorker; j++ {
							var values []string
							var args []interface{}
							for k := 0; k < batchSize; k++ {
								key := base + id*perWorker*batchSize + j*batchSize + k
								values = append(values, "(?, ?)")
								args = append(args, key, id)
							}
							sql := "INSERT INTO batch VALUES " + strings.Join(values, ", ")
							db.Exec(ctx, sql, args...)
						}
					}(w, i*workers*perWorker*batchSize)
				}
				wg.Wait()
			}
			b.ReportMetric(float64(workers*perWorker*batchSize*b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}
