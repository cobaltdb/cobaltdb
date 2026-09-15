package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestProbeDurabilityReopen verifies committed data survives a clean close and
// reopen, including data written after DDL and inside explicit transactions.
func TestProbeDurabilityReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dur.db")
	ctx := context.Background()

	db, err := engine.Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, ctx, `CREATE TABLE k (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, db, ctx, `INSERT INTO k VALUES (1,'one'),(2,'two')`)

	// Committed transaction must survive.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO k VALUES (3,'three')`); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Rolled back transaction must NOT survive.
	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin2: %v", err)
	}
	if _, err := tx2.Exec(ctx, `INSERT INTO k VALUES (4,'four')`); err != nil {
		t.Fatalf("tx2 insert: %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := engine.Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()

	if got := rowsToStr(t, db2, ctx, `SELECT id, v FROM k ORDER BY id`); got != "1|one;2|two;3|three" {
		t.Errorf("after reopen: got %q, want committed rows only", got)
	}
}

// TestProbeDurabilityNoClose simulates an unclean shutdown: the process drops
// the handle without Close(), then the database is reopened and must replay
// committed writes from the WAL.
func TestProbeDurabilityNoClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.db")
	ctx := context.Background()

	func() {
		db, err := engine.Open(path, nil)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		mustExec(t, db, ctx, `CREATE TABLE c (id INT PRIMARY KEY, v TEXT)`)
		for i := 1; i <= 20; i++ {
			if _, err := db.Exec(ctx, `INSERT INTO c VALUES (?,?)`, i, fmt.Sprintf("v%d", i)); err != nil {
				t.Fatalf("insert %d: %v", i, err)
			}
		}
		// Deliberately no Close(): emulate an abrupt process exit.
	}()

	db2, err := engine.Open(path, nil)
	if err != nil {
		t.Fatalf("reopen after unclean shutdown: %v", err)
	}
	defer func() { _ = db2.Close() }()

	var n int
	if err := db2.QueryRow(ctx, `SELECT COUNT(*) FROM c`).Scan(&n); err != nil {
		t.Fatalf("count after recovery: %v", err)
	}
	if n != 20 {
		t.Errorf("after unclean shutdown recovered %d rows, want 20", n)
	}
}

// TestProbeConcurrentWritesNoLostUpdate runs concurrent single-statement
// read-modify-writes, the pattern CLAUDE.md documents as safe, and checks the
// final total is exact.
func TestProbeConcurrentWritesNoLostUpdate(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE ctr (id INT PRIMARY KEY, n INT)`)
	mustExec(t, db, ctx, `INSERT INTO ctr VALUES (1,0)`)

	const workers, incs = 8, 50
	var wg sync.WaitGroup
	errCh := make(chan error, workers*incs)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < incs; i++ {
				// Retry on write-write conflict, which is the documented
				// contract for concurrent RMW.
				var lastErr error
				for attempt := 0; attempt < 100; attempt++ {
					_, err := db.Exec(ctx, `UPDATE ctr SET n = n + 1 WHERE id = 1`)
					if err == nil {
						lastErr = nil
						break
					}
					lastErr = err
				}
				if lastErr != nil {
					errCh <- lastErr
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("update failed after retries: %v", err)
	}

	var got int
	if err := db.QueryRow(ctx, `SELECT n FROM ctr WHERE id = 1`).Scan(&got); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if want := workers * incs; got != want {
		t.Errorf("counter = %d, want %d (lost %d updates)", got, want, want-got)
	}
}

// TestProbeConcurrentInsertsUnique checks that concurrent inserts preserve
// primary key uniqueness and that every acknowledged insert is durable.
func TestProbeConcurrentInsertsUnique(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE ins (id INT PRIMARY KEY, w INT)`)

	const workers, per = 8, 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := w*per + i
				var err error
				for attempt := 0; attempt < 100; attempt++ {
					_, err = db.Exec(ctx, `INSERT INTO ins VALUES (?,?)`, id, w)
					if err == nil {
						break
					}
				}
				if err == nil {
					mu.Lock()
					ok++
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()

	var n int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM ins`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != ok {
		t.Errorf("acknowledged %d inserts but table holds %d rows", ok, n)
	}
	if ok != workers*per {
		t.Errorf("only %d/%d inserts succeeded", ok, workers*per)
	}
}
