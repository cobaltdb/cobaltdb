package engine

import (
	"context"
	"testing"
)

// Regression for the JOIN+GROUP BY hash join missing the canonical-collision
// confirmation: executeJoinPass verifies hash-join candidates with
// compareValues (hashJoinKey canonicalizes numeric-looking TEXT — '0123' and
// '123' collide on key "1"), but executeJoinChainForGroupBy's inline hash
// join (the JOIN+GROUP BY path) never re-verified, so canonically colliding
// distinct TEXT keys wrongly joined there and fed phantom rows into GROUP BY
// aggregates. The non-GROUP-BY path (executeJoinPass) correctly returned 0
// rows for the identical join condition.
func TestJoinGroupByHashJoinConfirmsCanonicalCollisions(t *testing.T) {
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()

	execArgs(t, db, `CREATE TABLE a (id TEXT, val INTEGER)`)
	execArgs(t, db, `CREATE TABLE b (id TEXT, tag TEXT)`)
	execArgs(t, db, `INSERT INTO a VALUES ('0123', 1)`)
	execArgs(t, db, `INSERT INTO b VALUES ('123', 'X')`)

	// Control: the same join through executeJoinPass (no GROUP BY) — the
	// compareValues confirmation filters the canonical collision.
	rows, err := db.Query(ctx, "SELECT a.val, b.tag FROM a JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatalf("control query: %v", err)
	}
	controlCount := 0
	for rows.Next() {
		controlCount++
	}
	_ = rows.Close()
	if controlCount != 0 {
		t.Fatalf("control (no GROUP BY): expected 0 rows ('0123' must not join '123'), got %d", controlCount)
	}

	// RED: the identical join through executeJoinChainForGroupBy (GROUP BY
	// present) must not join either — previously the missing confirmation
	// produced a phantom group row.
	gRows, err := db.Query(ctx, "SELECT a.id, COUNT(*) FROM a JOIN b ON a.id = b.id GROUP BY a.id")
	if err != nil {
		t.Fatalf("GROUP BY query: %v", err)
	}
	defer func() { _ = gRows.Close() }()

	groupCount := 0
	for gRows.Next() {
		var id string
		var cnt int
		if err := gRows.Scan(&id, &cnt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		groupCount++
		t.Logf("group: id=%q count=%d", id, cnt)
	}
	if groupCount != 0 {
		t.Fatalf("GROUP BY path joined canonically colliding TEXT keys: got %d group row(s), want 0", groupCount)
	}

	// Sanity: a real key match still joins on the GROUP BY path.
	execArgs(t, db, `INSERT INTO b VALUES ('0123', 'Y')`)
	gRows2, err := db.Query(ctx, "SELECT a.id, b.tag FROM a JOIN b ON a.id = b.id GROUP BY a.id, b.tag")
	if err != nil {
		t.Fatalf("real-match query: %v", err)
	}
	defer func() { _ = gRows2.Close() }()
	saw := false
	for gRows2.Next() {
		var id, tag string
		if err := gRows2.Scan(&id, &tag); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if id == "0123" && tag == "Y" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("real ('0123','Y') join row missing on GROUP BY path")
	}
}
