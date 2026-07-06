package engine

import (
	"context"
	"strings"
	"testing"
)

// TestAuditPlaceholderArgCount verifies that a query supplying fewer bind
// arguments than positional `?` placeholders is rejected with a clear error,
// instead of silently binding the missing placeholder to NULL and returning
// wrong results. Extra args stay tolerated (ignored), and `?` inside a string
// literal is not counted.
func TestAuditPlaceholderArgCount(t *testing.T) {
	ctx := context.Background()
	db := mustOpenMem(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, s TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1,10,'a'),(2,20,'b')")

	// Too few args -> error, not silent wrong result.
	if _, err := db.Query(ctx, "SELECT id FROM t WHERE v = ? OR v = ?", 10); err == nil {
		t.Fatal("expected an error for 2 placeholders with 1 arg, got nil")
	} else if !strings.Contains(err.Error(), "placeholder") {
		t.Fatalf("expected a placeholder-count error, got: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE t SET v = ? WHERE id = ?", 5); err == nil {
		t.Fatal("expected an error for 2 placeholders with 1 arg in UPDATE")
	}
	// Zero args with placeholders present -> error.
	if _, err := db.Query(ctx, "SELECT id FROM t WHERE v = ?"); err == nil {
		t.Fatal("expected an error for 1 placeholder with 0 args")
	}

	// Correct arg count works.
	rows, err := db.Query(ctx, "SELECT id FROM t WHERE v = ? OR v = ?", 10, 20)
	if err != nil {
		t.Fatalf("correct arg count failed: %v", err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if n != 2 {
		t.Fatalf("correct arg count returned %d rows, want 2", n)
	}

	// Extra args tolerated (ignored).
	if _, err := db.Query(ctx, "SELECT id FROM t WHERE v = ?", 10, 999); err != nil {
		t.Fatalf("extra args should be tolerated: %v", err)
	}

	// A `?` inside a string literal is not a placeholder.
	if _, err := db.Query(ctx, "SELECT id FROM t WHERE s = 'a?b'"); err != nil {
		t.Fatalf("literal question mark miscounted as placeholder: %v", err)
	}
}
