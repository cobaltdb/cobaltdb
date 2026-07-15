package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// concurrentCheckpointTrial runs one crash trial: a writer commits n rows while a
// checkpointer runs concurrently, then the DB is abandoned (crash) and reopened.
// Returns the sorted list of committed-but-lost row ids (empty == durable).
func concurrentCheckpointTrial(t *testing.T, dbPath string, n int) []int {
	t.Helper()
	ctx := context.Background()

	db1, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	caExec(t, db1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	if err := db1.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= n; i++ {
			if _, err := db2.Exec(ctx, "INSERT INTO t VALUES (?,?)", i, i*10); err != nil {
				t.Errorf("insert %d: %v", i, err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ { // tight checkpoint loop overlapping every commit
			if err := db2.Checkpoint(); err != nil {
				t.Errorf("concurrent checkpoint: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	// abandon db2 (no close): every committed insert is fsynced, so all must survive.

	db3, err := Open(dbPath, durabilityTestOptions())
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer db3.Close()

	seen := map[int64]bool{}
	rows, err := db3.Query(ctx, "SELECT id, v FROM t")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, v int64
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %d after recovery", id)
		}
		seen[id] = true
		if v != id*10 {
			t.Fatalf("corrupt row id=%d v=%d want %d", id, v, id*10)
		}
	}
	var missing []int
	for i := 1; i <= n; i++ {
		if !seen[int64(i)] {
			missing = append(missing, i)
		}
	}
	return missing
}

// TestCrashAudit_ConcurrentCheckpoint asserts that a checkpoint running
// concurrently with a committing writer never drops an acknowledged committed
// write on a subsequent crash. Runs several independent trials because the loss
// is a timing race (checkpoint truncating the WAL between a commit's durable WAL
// append and its later page-apply).
func TestCrashAudit_ConcurrentCheckpoint(t *testing.T) {
	const trials = 6
	const n = 150
	for trial := 0; trial < trials; trial++ {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "concck.db")
		if lost := concurrentCheckpointTrial(t, dbPath, n); len(lost) > 0 {
			t.Fatalf("trial %d: %d committed rows LOST after crash (checkpoint/commit race): %v",
				trial, len(lost), lost)
		}
	}
}

func caExec(t *testing.T, db *DB, stmts ...string) {
	t.Helper()
	ctx := context.Background()
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// TestCrashAudit_SQLTxnCommitCheckpointRace verifies durability of SQL
// BEGIN/COMMIT explicit transactions racing a checkpoint. The SQL COMMIT path
// (used by the MySQL wire server and CLI) applied buffered writes (WAL-append →
// page-apply) without flushMu, so a concurrent checkpoint could truncate the WAL
// before the committed rows' pages were applied, losing acknowledged rows on a
// crash. COMMIT now holds flushMu.RLock across the flush+commit.
func TestCrashAudit_SQLTxnCommitCheckpointRace(t *testing.T) {
	ctx := context.Background()
	const N = 100
	for trial := 0; trial < 6; trial++ {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "a.db")
		db, err := Open(dbPath, &Options{})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		caExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")

		var wg sync.WaitGroup
		stop := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = db.Checkpoint()
				}
			}
		}()
		for i := 1; i <= N; i++ {
			caExec(t, db, "BEGIN")
			caExec(t, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
			caExec(t, db, "COMMIT")
		}
		close(stop)
		wg.Wait()

		// Abandon without Close (models a crash: dirty pages lost, WAL survives).
		db2, err := Open(dbPath, &Options{})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		var c int64
		r, err := db2.Query(ctx, "SELECT COUNT(*) FROM t")
		if err == nil && r.Next() {
			_ = r.Scan(&c)
			r.Close()
		}
		db2.Close()
		if c != N {
			t.Fatalf("trial %d: %d/%d committed rows survived after crash (SQL COMMIT/checkpoint race)", trial, c, N)
		}
	}
}
