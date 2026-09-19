package engine

import (
	"context"
	"path/filepath"
	"testing"
)

// TestCatalogPersistenceRoundTrip pins the disk-reopen contract verified in
// the new-series round 6 sweep of catalog_maintenance.go: Save persists
// tables (with AutoIncSeq via the atomic MarshalJSON alias), views (skipping
// temporary ones), and triggers; Load restores them fail-closed — a
// successful reopen asserts that every persisted class parsed and
// registered cleanly.
func TestCatalogPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "roundtrip.db")

	db, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE auto_t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`)
	mustExec(t, db, `INSERT INTO auto_t (v) VALUES ('a')`)   // auto id=1, AutoIncSeq=1
	mustExec(t, db, `INSERT INTO auto_t VALUES (5, 'five')`) // explicit id=5 bumps AutoIncSeq to 5
	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, dept INTEGER)`)
	mustExec(t, db, `INSERT INTO t VALUES (1, 10)`)
	mustExec(t, db, `CREATE VIEW v_t AS SELECT id, dept FROM t`)
	mustExec(t, db, `CREATE TEMPORARY VIEW temp_v AS SELECT 1 AS one`)
	mustExec(t, db, `CREATE TABLE fired (tag TEXT)`)
	mustExec(t, db, `CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO fired VALUES ('i'); END`)

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: Load fail-closes on any restore error.
	db2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()

	// AutoIncSeq must survive: the next auto id is 6, not a collision with
	// the existing ids 1 and 5.
	if _, err := db2.Exec(ctx, `INSERT INTO auto_t (v) VALUES ('after')`); err != nil {
		t.Fatalf("FAIL: auto-increment insert after reopen failed (AutoIncSeq lost?): %v", err)
	}
	var id int
	if err := db2.QueryRow(ctx, `SELECT id FROM auto_t WHERE v = 'after'`).Scan(&id); err != nil {
		t.Fatalf("FAIL: reopened auto-increment row not found: %v", err)
	}
	if id != 6 {
		t.Fatalf("FAIL: auto-increment continued at %d after reopen, want 6", id)
	}

	// View survives: querying it works.
	rows, err := db2.Query(ctx, `SELECT dept FROM v_t WHERE id = 1`)
	if err != nil {
		t.Fatalf("FAIL: view did not survive reopen: %v", err)
	}
	var dept int
	if !rows.Next() {
		t.Fatalf("FAIL: view returned no rows")
	}
	if err := rows.Scan(&dept); err != nil {
		t.Fatalf("scan: %v", err)
	}
	rows.Close()
	if dept != 10 {
		t.Fatalf("FAIL: view returned dept=%d, want 10", dept)
	}

	// Temporary view must NOT survive reopen.
	if _, err := db2.Query(ctx, `SELECT one FROM temp_v`); err == nil {
		t.Fatalf("FAIL: temporary view survived reopen")
	}

	// Trigger survives: INSERT into t fires it.
	if _, err := db2.Exec(ctx, `INSERT INTO t VALUES (2, 20)`); err != nil {
		t.Fatalf("FAIL: insert after reopen failed: %v", err)
	}
	var fired int
	if err := db2.QueryRow(ctx, `SELECT COUNT(*) FROM fired`).Scan(&fired); err != nil {
		t.Fatalf("count fired: %v", err)
	}
	if fired != 1 {
		t.Fatalf("FAIL: trigger did not survive reopen (fired=%d)", fired)
	}
}
