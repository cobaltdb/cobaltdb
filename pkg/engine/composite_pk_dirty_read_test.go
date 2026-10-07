package engine

import (
	"context"
	"testing"
)

// Regression for the composite-PK class of refactor.md §1.16 (round 26).
// PK-touching SET clauses on composite primary keys used to
//
//	(a) bypass the pending-write buffer inside explicit transactions, so a
//	    concurrent SELECT observed the uncommitted rekey (dirty read), and
//	(b) miskey rows on the autocommit direct path: change detection compared
//	    only the FIRST PK column and the new key was derived from that single
//	    column — a second-column change updated the row in place under its
//	    stale key (key/content desync: the live tuple was re-insertable as a
//	    duplicate) and a first-column change landed under a foreign
//	    single-column key.
//
// The fix generalizes the Phase-2 deferred rekey (new-key live write +
// old-key tombstone, applied at commit) and the direct path's key recipe to
// buildCompositePK over ALL PK columns.
func TestCompositePKChangeUpdateNoDirtyRead(t *testing.T) {
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
	exec(`CREATE TABLE comp (a INTEGER, b TEXT, c INTEGER, PRIMARY KEY (a, b))`)
	exec(`INSERT INTO comp VALUES (1, 'committed', 10)`)

	// PROBE: an uncommitted composite-PK move must be invisible to a
	// concurrent SELECT until COMMIT.
	updateDone := make(chan error)
	readDone := make(chan struct{})
	commitDone := make(chan error)
	go func() {
		if _, err := db.Exec(ctx, "BEGIN"); err != nil {
			updateDone <- err
			return
		}
		if _, err := db.Exec(ctx, "UPDATE comp SET b = 'moved' WHERE a = 1"); err != nil {
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
	rows, err := db.Query(ctx, "SELECT a, b, c FROM comp WHERE a = 1")
	if err != nil {
		t.Fatalf("concurrent query: %v", err)
	}
	var observed [][]interface{}
	for rows.Next() {
		var a, b, c interface{}
		if err := rows.Scan(&a, &b, &c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		observed = append(observed, []interface{}{a, b, c})
	}
	rows.Close()
	close(readDone)
	if err := <-commitDone; err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(observed) != 1 {
		t.Fatalf("concurrent SELECT during uncommitted composite-PK rekey returned %v, want exactly one row", observed)
	}
	if observed[0][1] != "committed" {
		t.Fatalf("DIRTY READ on composite-PK update: concurrent SELECT observed %v while the rekey (1,'committed')→(1,'moved') was still uncommitted; the old key must stay visible until COMMIT (refactor.md §1.16)", observed)
	}

	// Post-commit: the row must live at its new composite key and only there.
	rows2, err := db.Query(ctx, "SELECT c FROM comp WHERE a = 1 AND b = 'moved'")
	if err != nil {
		t.Fatalf("post-commit new-tuple query: %v", err)
	}
	var after []interface{}
	for rows2.Next() {
		var c interface{}
		_ = rows2.Scan(&c)
		after = append(after, c)
	}
	rows2.Close()
	if len(after) != 1 || toIntAny(after[0]) != 10 {
		t.Fatalf("post-commit keyed lookup by the new tuple (1,'moved') returned %v, want [10]", after)
	}
	rows3, err := db.Query(ctx, "SELECT c FROM comp WHERE a = 1 AND b = 'committed'")
	if err != nil {
		t.Fatalf("post-commit old-tuple query: %v", err)
	}
	var stale int
	for rows3.Next() {
		stale++
	}
	rows3.Close()
	if stale != 0 {
		t.Fatalf("post-commit: old tuple (1,'committed') still resolves %d rows, want 0", stale)
	}
	// The live tuple owns the row: re-inserting it must be a duplicate PK.
	if _, err := db.Exec(ctx, "INSERT INTO comp VALUES (1, 'moved', 20)"); err == nil {
		t.Fatalf("re-insert of the live PK tuple (1,'moved') was accepted; want duplicate-PK rejection")
	}
}

func TestCompositePKChangeAutocommitRekeys(t *testing.T) {
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
	exec(`CREATE TABLE comp (a INTEGER, b TEXT, c INTEGER, PRIMARY KEY (a, b))`)

	// Second PK column changes: the row must move to the key derived from the
	// full new tuple.
	exec(`INSERT INTO comp VALUES (1, 'old', 10)`)
	exec(`UPDATE comp SET b = 'new' WHERE a = 1`)
	var got []interface{}
	queryOne := func(sql string) []interface{} {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		var out []interface{}
		for rows.Next() {
			n := len(rows.Columns())
			vals := make([]interface{}, n)
			ptrs := make([]interface{}, n)
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, vals...)
		}
		return out
	}
	got = queryOne(`SELECT c FROM comp WHERE a = 1 AND b = 'new'`)
	if len(got) != 1 || toIntAny(got[0]) != 10 {
		t.Fatalf("keyed lookup by the new tuple (1,'new') returned %v, want [10]", got)
	}
	got = queryOne(`SELECT c FROM comp WHERE a = 1 AND b = 'old'`)
	if len(got) != 0 {
		t.Fatalf("old tuple (1,'old') still visible after the PK change: %v, want no rows", got)
	}
	if _, err := db.Exec(ctx, "INSERT INTO comp VALUES (1, 'new', 20)"); err == nil {
		t.Fatalf("re-insert of the live PK tuple (1,'new') was accepted; want duplicate-PK rejection")
	}

	// First PK column changes: the new key must be the composite key of the
	// new tuple, not a foreign single-column key.
	exec(`INSERT INTO comp VALUES (3, 'keep', 30)`)
	exec(`UPDATE comp SET a = 4 WHERE a = 3`)
	got = queryOne(`SELECT c FROM comp WHERE a = 4 AND b = 'keep'`)
	if len(got) != 1 || toIntAny(got[0]) != 30 {
		t.Fatalf("keyed lookup by the new tuple (4,'keep') returned %v, want [30]", got)
	}
	got = queryOne(`SELECT c FROM comp WHERE a = 3 AND b = 'keep'`)
	if len(got) != 0 {
		t.Fatalf("old tuple (3,'keep') still visible after the PK change: %v, want no rows", got)
	}
	if _, err := db.Exec(ctx, "INSERT INTO comp VALUES (4, 'keep', 40)"); err == nil {
		t.Fatalf("re-insert of the live PK tuple (4,'keep') was accepted; want duplicate-PK rejection")
	}

	// CONTROL: a non-PK update keeps keyed access intact.
	exec(`UPDATE comp SET c = 99 WHERE a = 1 AND b = 'new'`)
	got = queryOne(`SELECT c FROM comp WHERE a = 1 AND b = 'new'`)
	if len(got) != 1 || toIntAny(got[0]) != 99 {
		t.Fatalf("non-PK control update broke keyed access: %v, want [99]", got)
	}
	if _, err := db.Exec(ctx, "INSERT INTO comp VALUES (1, 'new', 5)"); err == nil {
		t.Fatalf("control: duplicate-PK insert of (1,'new') was accepted")
	}
}

func TestCompositePKChangeRollbackClean(t *testing.T) {
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
	exec(`CREATE TABLE comp (a INTEGER, b TEXT, c INTEGER, PRIMARY KEY (a, b))`)
	exec(`INSERT INTO comp VALUES (1, 'keep', 10)`)

	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE comp SET b = 'moved' WHERE a = 1"); err != nil {
		t.Fatalf("rekey in txn: %v", err)
	}
	if _, err := db.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	rows, err := db.Query(ctx, "SELECT a, b, c FROM comp")
	if err != nil {
		t.Fatalf("post-rollback query: %v", err)
	}
	var after [][]interface{}
	for rows.Next() {
		var a, b, c interface{}
		_ = rows.Scan(&a, &b, &c)
		after = append(after, []interface{}{a, b, c})
	}
	rows.Close()
	if len(after) != 1 || toIntAny(after[0][0]) != 1 || after[0][1] != "keep" || toIntAny(after[0][2]) != 10 {
		t.Fatalf("post-rollback: observed %v, want exactly [[1 keep 10]]", after)
	}
	// The rolled-back key must be free again: inserting the rejected tuple works.
	if _, err := db.Exec(ctx, "INSERT INTO comp VALUES (1, 'moved', 20)"); err != nil {
		t.Fatalf("insert of the rolled-back tuple (1,'moved') failed: %v", err)
	}
}
