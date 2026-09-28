package engine

import (
	"context"
	"testing"
)

// Regression for the Phase-3 direct-path class (refactor.md §1.16):
// partitioned-table writes used to bypass the pending-write buffer (the
// table.Partition == nil gating term), landing immediately in the
// per-partition B-trees — so a concurrent SELECT observed uncommitted
// inserts and updates. Under Phase 3 the writes buffer keyed by their
// partition tree and become visible only at COMMIT.
func TestPartitionedTableNoDirtyRead(t *testing.T) {
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
	exec(`CREATE TABLE pm (id INTEGER PRIMARY KEY, region INTEGER, tag TEXT) PARTITION BY RANGE (region) (PARTITION p0 VALUES LESS THAN (10), PARTITION pmax VALUES LESS THAN (MAXVALUE))`)
	exec(`INSERT INTO pm VALUES (1, 5, 'committed')`)

	// PROBE 1: an uncommitted INSERT must be invisible to a concurrent SELECT.
	insertDone := make(chan error)
	readDone := make(chan struct{})
	commitDone := make(chan error)
	go func() {
		if _, err := db.Exec(ctx, "BEGIN"); err != nil {
			insertDone <- err
			return
		}
		if _, err := db.Exec(ctx, "INSERT INTO pm VALUES (2, 7, 'uncommitted')"); err != nil {
			insertDone <- err
			return
		}
		close(insertDone)
		<-readDone
		if _, err := db.Exec(ctx, "COMMIT"); err != nil {
			commitDone <- err
			return
		}
		close(commitDone)
	}()
	if err := <-insertDone; err != nil {
		t.Fatalf("writer goroutine: %v", err)
	}
	selectRows := func() [][2]interface{} {
		rows, err := db.Query(ctx, "SELECT id, tag FROM pm ORDER BY id")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		var out [][2]interface{}
		for rows.Next() {
			var id, tag interface{}
			if err := rows.Scan(&id, &tag); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, [2]interface{}{id, tag})
		}
		rows.Close()
		return out
	}
	observed := selectRows()
	close(readDone)
	if err := <-commitDone; err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(observed) != 1 || toIntAny(observed[0][0]) != 1 || observed[0][1] != "committed" {
		t.Fatalf("DIRTY READ on partitioned INSERT: concurrent SELECT observed %v while the insert was still uncommitted; partitioned writes must buffer and become visible only at COMMIT (refactor.md §1.16 Phase 3)", observed)
	}
	after := selectRows()
	if len(after) != 2 || toIntAny(after[1][0]) != 2 || after[1][1] != "uncommitted" {
		t.Fatalf("post-commit: observed %v, want the committed rows [1 committed] [2 uncommitted]", after)
	}

	// PROBE 2: an uncommitted UPDATE must be equally invisible.
	updateDone := make(chan error)
	readDone2 := make(chan struct{})
	commitDone2 := make(chan error)
	go func() {
		if _, err := db.Exec(ctx, "BEGIN"); err != nil {
			updateDone <- err
			return
		}
		if _, err := db.Exec(ctx, "UPDATE pm SET tag = 'moved' WHERE id = 1"); err != nil {
			updateDone <- err
			return
		}
		close(updateDone)
		<-readDone2
		if _, err := db.Exec(ctx, "COMMIT"); err != nil {
			commitDone2 <- err
			return
		}
		close(commitDone2)
	}()
	if err := <-updateDone; err != nil {
		t.Fatalf("update goroutine: %v", err)
	}
	observed2 := selectRows()
	close(readDone2)
	if err := <-commitDone2; err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	if len(observed2) != 2 || observed2[0][1] != "committed" {
		t.Fatalf("DIRTY READ on partitioned UPDATE: concurrent SELECT observed %v while the update was still uncommitted", observed2)
	}
	after2 := selectRows()
	if len(after2) != 2 || after2[0][1] != "moved" {
		t.Fatalf("post-commit update: observed %v, want tag 'moved' on row 1", after2)
	}

	// PROBE 3 (guard): rollback of a partitioned insert leaves no row.
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin 3: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO pm VALUES (3, 8, 'gone')"); err != nil {
		t.Fatalf("insert 3: %v", err)
	}
	if _, err := db.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	final := selectRows()
	if len(final) != 2 {
		t.Fatalf("post-rollback: %d rows %v, want 2 (rollback left no residue)", len(final), final)
	}
}
