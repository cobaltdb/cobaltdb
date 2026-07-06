package engine

import (
	"context"
	"strings"
	"testing"
)

// TestAuditNonFiniteFloatRejected verifies that a write producing a non-finite
// float (NaN/±Inf) — reachable from ordinary SQL via arithmetic overflow — is
// rejected at write time with a clear error and leaves the existing row intact
// and readable, rather than silently persisting a "+Inf" that the row decoder
// cannot parse (which previously made the row permanently unreadable).
func TestAuditNonFiniteFloatRejected(t *testing.T) {
	ctx := context.Background()
	db := mustOpenMem(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE f (id INTEGER PRIMARY KEY, v REAL)")
	mustExec(t, db, "INSERT INTO f VALUES (1, 1e308)")

	// Overflow to +Inf must be refused, not stored.
	if _, err := db.Exec(ctx, "UPDATE f SET v = v * 100 WHERE id = 1"); err == nil {
		t.Fatal("expected overflow UPDATE to be rejected, got nil error")
	} else if !strings.Contains(err.Error(), "non-finite") {
		t.Fatalf("expected a non-finite error, got: %v", err)
	}

	// The row must remain readable with its original value (write was atomic).
	rows, err := db.Query(ctx, "SELECT v FROM f WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after rejected UPDATE failed (row corrupted?): %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("row disappeared after rejected UPDATE")
	}
	var v float64
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if v != 1e308 {
		t.Fatalf("original value not preserved: got %v, want 1e308", v)
	}

	// A fresh INSERT of a non-finite value (via overflow inside the statement)
	// is likewise refused rather than corrupting a new row.
	if _, err := db.Exec(ctx, "INSERT INTO f VALUES (2, 1e308)"); err != nil {
		t.Fatalf("insert row 2: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE f SET v = v * v * v WHERE id = 2"); err == nil {
		t.Fatal("expected overflow UPDATE on row 2 to be rejected")
	}
	// Row 2 still readable.
	if _, err := db.Query(ctx, "SELECT v FROM f WHERE id = 2"); err != nil {
		t.Fatalf("SELECT row 2 after rejected UPDATE failed: %v", err)
	}
}
