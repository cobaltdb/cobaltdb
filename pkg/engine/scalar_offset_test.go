package engine

import (
	"context"
	"testing"
)

// Regression for FROM-less SELECT OFFSET: executeScalarSelect handled
// stmt.Limit but never touched stmt.Offset, so OFFSET was silently ignored
// on the FROM-less path — `SELECT 1 OFFSET 1` returned its row instead of
// skipping it. Every other SELECT path (main, fast, applyOffsetLimit)
// applies OFFSET; the scalar path now does too.
func TestScalarSelectWithoutFromAppliesOffset(t *testing.T) {
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()

	rowCount := func(sql string) int {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer func() { _ = rows.Close() }()
		n := 0
		for rows.Next() {
			n++
		}
		return n
	}

	// RED: OFFSET must skip the single FROM-less row.
	if n := rowCount("SELECT 1 OFFSET 1"); n != 0 {
		t.Fatalf("SELECT 1 OFFSET 1 returned %d row(s), want 0 (OFFSET silently ignored)", n)
	}

	// Combined LIMIT + OFFSET: skip past the row.
	if n := rowCount("SELECT 1 LIMIT 1 OFFSET 1"); n != 0 {
		t.Fatalf("SELECT 1 LIMIT 1 OFFSET 1 returned %d row(s), want 0", n)
	}

	// Controls: OFFSET 0 and no OFFSET keep the row.
	if n := rowCount("SELECT 1"); n != 1 {
		t.Fatalf("SELECT 1 returned %d row(s), want 1", n)
	}
	if n := rowCount("SELECT 1 OFFSET 0"); n != 1 {
		t.Fatalf("SELECT 1 OFFSET 0 returned %d row(s), want 1", n)
	}
	if n := rowCount("SELECT 'x' AS s LIMIT 5 OFFSET 0"); n != 1 {
		t.Fatalf("SELECT 'x' LIMIT 5 OFFSET 0 returned %d row(s), want 1", n)
	}
}
