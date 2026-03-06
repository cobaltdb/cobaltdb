package test

import (
	"fmt"
	"testing"
)

// TestV64UnionDerived tests UNION/UNION ALL inside derived tables (subquery in FROM).
func TestV64UnionDerived(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expected, len(rows))
			return
		}
		pass++
	}

	// Setup
	afExec(t, db, ctx, `CREATE TABLE v64_a (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v64_a VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO v64_a VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO v64_a VALUES (3, 'Carol', 300)")

	afExec(t, db, ctx, `CREATE TABLE v64_b (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v64_b VALUES (1, 'Dave', 400)")
	afExec(t, db, ctx, "INSERT INTO v64_b VALUES (2, 'Eve', 500)")

	// ============================================================
	// === UNION IN DERIVED TABLE ===
	// ============================================================

	// UD1: Simple UNION in derived table
	checkRowCount("UD1 UNION derived",
		`SELECT * FROM (
			SELECT name, val FROM v64_a
			UNION
			SELECT name, val FROM v64_b
		) sub`, 5)

	// UD2: UNION ALL in derived table
	checkRowCount("UD2 UNION ALL derived",
		`SELECT * FROM (
			SELECT name FROM v64_a
			UNION ALL
			SELECT name FROM v64_b
		) sub`, 5)

	// UD3: Aggregate over UNION derived table
	check("UD3 agg UNION derived",
		`SELECT SUM(val) FROM (
			SELECT val FROM v64_a
			UNION ALL
			SELECT val FROM v64_b
		) sub`, 1500)
	// 100+200+300+400+500 = 1500

	// UD4: COUNT over UNION derived table
	check("UD4 COUNT UNION derived",
		`SELECT COUNT(*) FROM (
			SELECT name FROM v64_a
			UNION ALL
			SELECT name FROM v64_b
		) sub`, 5)

	// UD5: UNION with WHERE then aggregate
	check("UD5 UNION WHERE agg",
		`SELECT COUNT(*) FROM (
			SELECT name, val FROM v64_a WHERE val > 150
			UNION ALL
			SELECT name, val FROM v64_b WHERE val > 450
		) sub`, 3)
	// v64_a > 150: Bob(200), Carol(300) = 2; v64_b > 450: Eve(500) = 1; total = 3

	// UD6: UNION deduplication in derived table
	afExec(t, db, ctx, `CREATE TABLE v64_c (id INTEGER PRIMARY KEY, tag TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v64_c VALUES (1, 'x')")
	afExec(t, db, ctx, "INSERT INTO v64_c VALUES (2, 'y')")

	afExec(t, db, ctx, `CREATE TABLE v64_d (id INTEGER PRIMARY KEY, tag TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v64_d VALUES (1, 'y')")
	afExec(t, db, ctx, "INSERT INTO v64_d VALUES (2, 'z')")

	check("UD6 UNION dedup derived",
		`SELECT COUNT(*) FROM (
			SELECT tag FROM v64_c
			UNION
			SELECT tag FROM v64_d
		) sub`, 3)
	// UNION deduplicates: x, y, z = 3

	check("UD7 UNION ALL no dedup",
		`SELECT COUNT(*) FROM (
			SELECT tag FROM v64_c
			UNION ALL
			SELECT tag FROM v64_d
		) sub`, 4)
	// UNION ALL: x, y, y, z = 4

	// UD8: ORDER BY on UNION derived table
	check("UD8 ORDER UNION derived",
		`SELECT name FROM (
			SELECT name, val FROM v64_a
			UNION ALL
			SELECT name, val FROM v64_b
		) sub ORDER BY val DESC LIMIT 1`, "Eve")

	// UD9: MAX over UNION derived table
	check("UD9 MAX UNION derived",
		`SELECT MAX(val) FROM (
			SELECT val FROM v64_a
			UNION ALL
			SELECT val FROM v64_b
		) sub`, 500)

	// UD10: Triple UNION ALL in derived table
	checkRowCount("UD10 triple UNION",
		`SELECT * FROM (
			SELECT name FROM v64_a
			UNION ALL
			SELECT name FROM v64_b
			UNION ALL
			SELECT tag AS name FROM v64_c
		) sub`, 7)
	// 3 + 2 + 2 = 7

	t.Logf("\n=== V64 UNION DERIVED TABLE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
