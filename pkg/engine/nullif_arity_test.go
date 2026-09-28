package engine

import (
	"context"
	"testing"
)

// NULLIF enforces exactly 2 arguments in both evaluation contexts (the
// SELECT-list dispatch and the WHERE/value-expression dispatch). The arity
// guards previously read len(args) < 2, so a 3-argument call like
// NULLIF(1, 2, 3) was silently accepted and evaluated as the 2-argument
// form — a typo'd extra argument went unnoticed, diverging from MySQL
// ("Incorrect parameter count") and from the function's own error message,
// "NULLIF requires 2 arguments".

func nullifArityQuery1(t *testing.T, db *DB, sql string) (interface{}, error) {
	t.Helper()
	rows, err := db.Query(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("query %q: no rows", sql)
	}
	var v interface{}
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan %q: %v", sql, err)
	}
	return v, nil
}

// TestNULLIFValidArity pins the valid 2-argument shapes in both contexts.
func TestNULLIFValidArity(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE nt (v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO nt VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if v, err := nullifArityQuery1(t, db, "SELECT NULLIF(1, 2)"); err != nil || v == nil {
		t.Fatalf("NULLIF(1,2): v=%v err=%v, want 1", v, err)
	}
	if v, err := nullifArityQuery1(t, db, "SELECT NULLIF(2, 2)"); err != nil || v != nil {
		t.Fatalf("NULLIF(2,2): v=%v err=%v, want NULL", v, err)
	}
	if v, err := nullifArityQuery1(t, db, "SELECT NULLIF(NULL, 5)"); err != nil || v != nil {
		t.Fatalf("NULLIF(NULL,5): v=%v err=%v, want NULL", v, err)
	}
	if _, err := nullifArityQuery1(t, db, "SELECT NULLIF(1)"); err == nil {
		t.Fatal("1-arg NULLIF must error")
	}
	if _, err := nullifArityQuery1(t, db, "SELECT v FROM nt WHERE NULLIF(1, 2) = 1"); err != nil {
		t.Fatalf("WHERE NULLIF(1,2): %v", err)
	}
}

// TestNULLIFRejectsOverArity pins the fix: 3+ arguments must fail the
// statement in BOTH evaluation contexts instead of silently dropping the
// extras.
func TestNULLIFRejectsOverArity(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE nt (v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO nt VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if v, err := nullifArityQuery1(t, db, "SELECT NULLIF(1, 2, 3)"); err == nil {
		t.Fatalf("FAIL: SELECT NULLIF(1,2,3) = %v with no error — 3 arguments must be rejected (\"NULLIF requires 2 arguments\"; MySQL: Incorrect parameter count), not silently evaluated as 2-arg", v)
	}
	if _, err := db.Query(ctx, "SELECT v FROM nt WHERE NULLIF(1, 2, 3) = 1"); err == nil {
		t.Fatal("FAIL: WHERE NULLIF(1,2,3) accepted with no error — the value-expression context must enforce the same 2-argument arity as the SELECT-list context")
	}
	if _, err := db.Query(ctx, "SELECT v FROM nt WHERE NULLIF(1, 2, 3, 4) = 1"); err == nil {
		t.Fatal("FAIL: WHERE NULLIF(1,2,3,4) accepted with no error")
	}
}
