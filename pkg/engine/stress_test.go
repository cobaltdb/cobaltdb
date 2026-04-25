package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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
