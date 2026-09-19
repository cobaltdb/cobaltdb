package engine

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCheckConstraintsSurviveRoundTrip pins the end-to-end persistence of
// CHECK constraints: BETWEEN, bitwise, CASE, and simple comparisons must be
// enforced both before and after a Close/Reopen cycle. Pre-fix, exprToSQL
// corrupted the persisted CheckStr (BETWEEN as a Go struct dump, bitwise
// operators as "?"), so the catalog load failed on reopen and the database
// could not be opened at all.
func TestCheckConstraintsSurviveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testdb")

	db, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mustExec(t, db, "CREATE TABLE bt (x INTEGER CHECK (x BETWEEN 1 AND 5))")
	mustExec(t, db, "CREATE TABLE bit_t (x INTEGER CHECK (x & 1 = 1))")
	mustExec(t, db, "CREATE TABLE cs_t (x INTEGER CHECK (CASE WHEN x > 0 THEN 1 ELSE 0 END = 1))")
	mustExec(t, db, "CREATE TABLE simple (x INTEGER CHECK (x > 0))")

	// Controls: every constraint enforced before the round trip.
	for _, tt := range []struct{ table, values string }{
		{"bt", "(10)"},
		{"bit_t", "(2)"},
		{"cs_t", "(-1)"},
		{"simple", "(-1)"},
	} {
		if _, err := db.Exec(nil, "INSERT INTO "+tt.table+" VALUES "+tt.values); err == nil {
			t.Fatalf("pre-close: INSERT violating %s CHECK unexpectedly succeeded", tt.table)
		}
	}
	if _, err := db.Exec(nil, "INSERT INTO simple VALUES (1)"); err != nil {
		t.Fatalf("pre-close control INSERT failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Round trip: reopen and verify enforcement survived.
	db2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen failed after persisting CHECK constraints: %v", err)
	}
	defer func() { _ = db2.Close() }()

	for _, tt := range []struct{ table, values string }{
		{"bt", "(10)"},
		{"bit_t", "(2)"},
		{"cs_t", "(-1)"},
		{"simple", "(-1)"},
	} {
		if _, err := db2.Exec(nil, "INSERT INTO "+tt.table+" VALUES "+tt.values); err == nil {
			t.Errorf("post-reopen: CHECK on %s silently dropped after reload", tt.table)
		}
	}
	if _, err := db2.Exec(nil, "INSERT INTO simple VALUES (2)"); err != nil {
		t.Fatalf("post-reopen control INSERT failed: %v", err)
	}
	_ = os.RemoveAll(dir) // t.TempDir already cleans; keep tools honest
}
