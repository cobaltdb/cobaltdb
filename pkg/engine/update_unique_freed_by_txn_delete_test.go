package engine

import (
	"context"
	"strings"
	"testing"
)

// TestUpdateUniqueValueFreedByTxnDelete pins the net-effect semantics of the
// buffered UPDATE unique check. Inside one transaction: INSERT a row holding
// unique value 'beta', DELETE it, then UPDATE another row to 'beta'. The last
// pending index op on 'beta' is a DELETE, so the slot is freed and the UPDATE
// must succeed. The check at catalog_update.go now uses indexKeyPendingState,
// whose last-op-wins net effect correctly frees the slot; its deleted
// predecessor indexKeyInPendingWrites reported the slot as taken if ANY
// pending non-delete op matched, spuriously rejecting the UPDATE with
// "UNIQUE constraint failed".
func TestUpdateUniqueValueFreedByTxnDelete(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE u (id INTEGER PRIMARY KEY, code TEXT)`)
	mustExec(t, db, `CREATE UNIQUE INDEX uq_code ON u (code)`)
	mustExec(t, db, `INSERT INTO u VALUES (1, 'alpha')`)
	mustExec(t, db, `INSERT INTO u VALUES (2, 'gamma')`)

	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO u VALUES (3, 'beta')`)
	mustExec(t, db, `DELETE FROM u WHERE id = 3`)
	// The pending 'beta' entry was freed by the DELETE (last op wins), so
	// this UPDATE must succeed.
	mustExec(t, db, `UPDATE u SET code = 'beta' WHERE id = 2`)
	mustExec(t, db, `COMMIT`)

	var code string
	if err := db.QueryRow(ctx, `SELECT code FROM u WHERE id = 2`).Scan(&code); err != nil {
		t.Fatalf("read row 2: %v", err)
	}
	if code != "beta" {
		t.Fatalf("FAIL: UPDATE to freed unique value did not commit: row 2 code=%q, want beta", code)
	}
}

// TestUpdateUniqueValueStillTakenWithoutDelete is the control: the same
// UPDATE with NO freeing DELETE must still be rejected — both the old
// boolean check and the net-state check agree here.
func TestUpdateUniqueValueStillTakenWithoutDelete(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE u2 (id INTEGER PRIMARY KEY, code TEXT)`)
	mustExec(t, db, `CREATE UNIQUE INDEX uq_code2 ON u2 (code)`)
	mustExec(t, db, `INSERT INTO u2 VALUES (1, 'alpha')`)
	mustExec(t, db, `INSERT INTO u2 VALUES (2, 'gamma')`)

	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO u2 VALUES (3, 'beta')`)
	_, err = db.Exec(ctx, `UPDATE u2 SET code = 'beta' WHERE id = 2`)
	if err == nil {
		t.Fatalf("FAIL: UPDATE onto a taken unique value succeeded")
	}
	if !strings.Contains(err.Error(), "UNIQUE constraint failed") {
		t.Fatalf("FAIL: unexpected error %q, want UNIQUE constraint failure", err.Error())
	}
	if _, err := db.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}
