package engine

import (
	"context"
	"fmt"
	"testing"
)

// TestBytesLiteralRoundTripMaterialization pins that valueToLiteralExpr
// preserves []byte exactly (rather than rendering the decimal byte list
// "[104 101 108 108 111]"). []byte has no case in the switch, so it fell to
// the default `fmt.Sprintf("%v", val)`, which silently corrupted binary data
// on both of valueToLiteralExpr's call sites:
//
//  1. CTAS  (database_ddl.go: CREATE TABLE dst AS SELECT ...)
//  2. INSERT..SELECT with a conflict clause (database_dml.go executeUpsert's
//     materialized-source branch)
//
// The corruption was silent: no error at DDL time, no error at query time.
// A BLOB column value is scanned back as []byte, so binding a []byte into a
// BLOB column and materializing it through either path reproduced it.
//
// Regression guard: the neighbouring valueToLiteralExpr fix for integer
// precision (r14) is asserted below so this test also catches a regression
// of that earlier fix.
func TestBytesLiteralRoundTripMaterialization(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec := func(sql string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	scanOne := func(sql string) interface{} {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("query %q returned no rows", sql)
		}
		vals := make([]interface{}, 1)
		ptrs := make([]interface{}, 1)
		ptrs[0] = &vals[0]
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan %q: %v", sql, err)
		}
		return vals[0]
	}
	asText := func(v interface{}) string {
		switch t2 := v.(type) {
		case []byte:
			return string(t2)
		case string:
			return t2
		default:
			return fmt.Sprint(v)
		}
	}

	mustExec(`CREATE TABLE src (id INTEGER PRIMARY KEY, data BLOB)`)
	// Bound []byte is what makes the column scan back as []byte.
	mustExec(`INSERT INTO src VALUES (?, ?)`, int64(1), []byte("hello"))

	// Control: the source row itself round-trips.
	if got := scanOne(`SELECT data FROM src`); asText(got) != "hello" {
		t.Fatalf("control: source data = %q (%T), want %q", fmt.Sprint(got), got, "hello")
	}

	// Call site 1 — CTAS materialization.
	mustExec(`CREATE TABLE dst_ctas AS SELECT id, data FROM src`)
	if got := scanOne(`SELECT data FROM dst_ctas`); asText(got) != "hello" {
		t.Fatalf("CTAS: data = %q (%T), want %q", asText(got), got, "hello")
	}

	// Call site 2 — INSERT..SELECT with a conflict clause.
	mustExec(`CREATE TABLE dst_conf (id INTEGER PRIMARY KEY, data BLOB)`)
	mustExec(`INSERT INTO dst_conf VALUES (1, 'starter')`)
	mustExec(`INSERT INTO dst_conf SELECT id, data FROM src WHERE id = 1 ON CONFLICT (id) DO UPDATE SET data = VALUES(data)`)
	if got := scanOne(`SELECT data FROM dst_conf`); asText(got) != "hello" {
		t.Fatalf("conflict materialization: data = %q (%T), want %q", asText(got), got, "hello")
	}

	// Boundary: an empty []byte must stay empty, not become "[]".
	mustExec(`INSERT INTO src VALUES (?, ?)`, int64(2), []byte{})
	mustExec(`CREATE TABLE dst_empty AS SELECT id, data FROM src WHERE id = 2`)
	if got := scanOne(`SELECT data FROM dst_empty`); asText(got) != "" {
		t.Fatalf("empty []byte: data = %q (%T), want empty", asText(got), got)
	}
}
