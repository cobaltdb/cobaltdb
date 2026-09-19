package engine

import (
	"path/filepath"
	"testing"
)

// TestValueToLiteralExprPreservesInt64 pins int64 precision through the
// CREATE TABLE AS SELECT materialization: valueToLiteralExpr (database.go)
// must set NumberLiteral.Raw for integer values so NumberLiteral.Evaluate can
// restore full int64 precision. Pre-fix, scanned int64 values above 2^53 were
// wrapped as float64-only literals and silently rounded (9007199254740993 ->
// 9007199254740992) by the CTAS and INSERT-materialization paths.
func TestValueToLiteralExprPreservesInt64(t *testing.T) {
	const big = int64(9007199254740993) // 2^53 + 1: not representable in float64

	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "testdb"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE src (v INTEGER)")
	mustExec(t, db, "INSERT INTO src VALUES (9007199254740993)")

	// Control: the parser path preserves the big integer exactly.
	var srcVal int64
	if err := db.QueryRow(nil, "SELECT v FROM src").Scan(&srcVal); err != nil {
		t.Fatalf("read source: %v", err)
	}
	if srcVal != big {
		t.Fatalf("source round-trip corrupted: got %d, want %d", srcVal, big)
	}

	// CTAS: the valueToLiteralExpr materialization path.
	mustExec(t, db, "CREATE TABLE dst AS SELECT v FROM src")
	var got int64
	if err := db.QueryRow(nil, "SELECT v FROM dst").Scan(&got); err != nil {
		t.Fatalf("read CTAS table: %v", err)
	}
	if got != big {
		t.Fatalf("CTAS corrupted int64: got %d, want %d", got, big)
	}

	// Small ints are unaffected (float64-exact range).
	mustExec(t, db, "CREATE TABLE dst2 AS SELECT 42 AS v")
	var small int64
	if err := db.QueryRow(nil, "SELECT v FROM dst2").Scan(&small); err != nil {
		t.Fatalf("read small CTAS table: %v", err)
	}
	if small != 42 {
		t.Fatalf("CTAS corrupted small int: got %d, want 42", small)
	}
}
