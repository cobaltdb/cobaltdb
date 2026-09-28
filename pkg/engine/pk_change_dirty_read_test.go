package engine

import (
	"context"
	"testing"
)

// Regression for the Phase-2 direct-path class (refactor.md §1.16):
// PK-changing SET-clause updates used to bypass the pending-write buffer,
// so the old key disappeared from the shared B-tree and the new key
// appeared in it — both uncommitted — while the transaction was still open.
// Under Phase 2 the rekey is deferred to commit (new-key live write +
// old-key tombstone), so concurrent readers see the row at its old key
// until COMMIT.
func TestPKChangeUpdateNoDirtyRead(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	exec := func(sql string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	exec(`CREATE TABLE pkr (id INTEGER PRIMARY KEY, tag TEXT)`)
	exec(`INSERT INTO pkr VALUES (1, 'committed')`)

	// PROBE 1: uncommitted PK move must be invisible to a concurrent SELECT.
	updateDone := make(chan error)
	readDone := make(chan struct{})
	commitDone := make(chan error)
	go func() {
		if _, err := db.Exec(ctx, "BEGIN"); err != nil {
			updateDone <- err
			return
		}
		if _, err := db.Exec(ctx, "UPDATE pkr SET id = 10, tag = 'moved' WHERE id = 1"); err != nil {
			updateDone <- err
			return
		}
		close(updateDone)
		<-readDone
		if _, err := db.Exec(ctx, "COMMIT"); err != nil {
			commitDone <- err
			return
		}
		close(commitDone)
	}()
	if err := <-updateDone; err != nil {
		t.Fatalf("writer goroutine: %v", err)
	}
	rows, err := db.Query(ctx, "SELECT id, tag FROM pkr ORDER BY id")
	if err != nil {
		t.Fatalf("concurrent query: %v", err)
	}
	var observed [][2]interface{}
	for rows.Next() {
		var id, tag interface{}
		if err := rows.Scan(&id, &tag); err != nil {
			t.Fatalf("scan: %v", err)
		}
		observed = append(observed, [2]interface{}{id, tag})
	}
	rows.Close()
	close(readDone)
	if err := <-commitDone; err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(observed) != 1 || toIntAny(observed[0][0]) != 1 || observed[0][1] != "committed" {
		t.Fatalf("DIRTY READ on PK-changing update: concurrent SELECT observed %v while the rekey 1→10 was still uncommitted; the old-key delete must be deferred to COMMIT (refactor.md §1.16 Phase 2)", observed)
	}

	// Post-commit: the row must exist only at its new key.
	rows2, err := db.Query(ctx, "SELECT id, tag FROM pkr ORDER BY id")
	if err != nil {
		t.Fatalf("post-commit query: %v", err)
	}
	var after [][2]interface{}
	for rows2.Next() {
		var id, tag interface{}
		_ = rows2.Scan(&id, &tag)
		after = append(after, [2]interface{}{id, tag})
	}
	rows2.Close()
	if len(after) != 1 || toIntAny(after[0][0]) != 10 || after[0][1] != "moved" {
		t.Fatalf("post-commit: observed %v, want exactly [10 moved]", after)
	}

	// PROBE 2: rekeying onto an existing key is rejected at statement time.
	if _, err := db.Exec(ctx, "INSERT INTO pkr VALUES (20, 'occupied')"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE pkr SET id = 20 WHERE id = 10"); err == nil {
		_, _ = db.Exec(ctx, "ROLLBACK")
		t.Fatalf("rekey onto an existing key was accepted; want PRIMARY KEY constraint failure")
	} else {
		_, _ = db.Exec(ctx, "ROLLBACK")
	}

	// PROBE 3: rollback of a rekey leaves the row at its original key.
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE pkr SET id = 30 WHERE id = 10"); err != nil {
		t.Fatalf("rekey in txn: %v", err)
	}
	if _, err := db.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	rows3, err := db.Query(ctx, "SELECT id FROM pkr ORDER BY id")
	if err != nil {
		t.Fatalf("post-rollback query: %v", err)
	}
	var ids []interface{}
	for rows3.Next() {
		var id interface{}
		_ = rows3.Scan(&id)
		ids = append(ids, id)
	}
	rows3.Close()
	if len(ids) != 2 || toIntAny(ids[0]) != 10 || toIntAny(ids[1]) != 20 {
		t.Fatalf("post-rollback: ids = %v, want [10 20] (rekey rolled back cleanly)", ids)
	}
}

func toIntAny(v interface{}) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case int:
		return n
	case float64:
		return int(n)
	}
	return -1
}
