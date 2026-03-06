package test

import (
	"fmt"
	"testing"
)

// TestV65SetOperations tests INTERSECT, EXCEPT, and complex set operation patterns
// including in CTEs and derived tables.
func TestV65SetOperations(t *testing.T) {
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
	afExec(t, db, ctx, `CREATE TABLE v65_a (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v65_a VALUES (1, 'Alice', 10)")
	afExec(t, db, ctx, "INSERT INTO v65_a VALUES (2, 'Bob', 20)")
	afExec(t, db, ctx, "INSERT INTO v65_a VALUES (3, 'Carol', 30)")
	afExec(t, db, ctx, "INSERT INTO v65_a VALUES (4, 'Dave', 40)")

	afExec(t, db, ctx, `CREATE TABLE v65_b (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v65_b VALUES (1, 'Bob', 20)")
	afExec(t, db, ctx, "INSERT INTO v65_b VALUES (2, 'Carol', 30)")
	afExec(t, db, ctx, "INSERT INTO v65_b VALUES (3, 'Eve', 50)")
	afExec(t, db, ctx, "INSERT INTO v65_b VALUES (4, 'Frank', 60)")

	// ============================================================
	// === TOP-LEVEL INTERSECT ===
	// ============================================================

	// IN1: INTERSECT - common rows
	checkRowCount("IN1 INTERSECT basic",
		`SELECT name, val FROM v65_a
		 INTERSECT
		 SELECT name, val FROM v65_b`, 2)
	// Common: Bob(20), Carol(30) = 2

	// IN2: INTERSECT names only
	checkRowCount("IN2 INTERSECT names",
		`SELECT name FROM v65_a
		 INTERSECT
		 SELECT name FROM v65_b`, 2)

	// ============================================================
	// === TOP-LEVEL EXCEPT ===
	// ============================================================

	// EX1: EXCEPT - left minus right
	checkRowCount("EX1 EXCEPT basic",
		`SELECT name FROM v65_a
		 EXCEPT
		 SELECT name FROM v65_b`, 2)
	// A minus B: Alice, Dave (Bob and Carol in both) = 2

	// EX2: EXCEPT reverse direction
	checkRowCount("EX2 EXCEPT reverse",
		`SELECT name FROM v65_b
		 EXCEPT
		 SELECT name FROM v65_a`, 2)
	// B minus A: Eve, Frank = 2

	// ============================================================
	// === INTERSECT IN CTE ===
	// ============================================================

	// CI1: CTE with INTERSECT
	check("CI1 CTE INTERSECT",
		`WITH common AS (
			SELECT name, val FROM v65_a
			INTERSECT
			SELECT name, val FROM v65_b
		)
		SELECT COUNT(*) FROM common`, 2)

	// CI2: CTE INTERSECT with aggregate
	check("CI2 CTE INTERSECT agg",
		`WITH common AS (
			SELECT name, val FROM v65_a
			INTERSECT
			SELECT name, val FROM v65_b
		)
		SELECT SUM(val) FROM common`, 50)
	// Bob(20) + Carol(30) = 50

	// ============================================================
	// === EXCEPT IN CTE ===
	// ============================================================

	// CE1: CTE with EXCEPT
	check("CE1 CTE EXCEPT",
		`WITH only_a AS (
			SELECT name, val FROM v65_a
			EXCEPT
			SELECT name, val FROM v65_b
		)
		SELECT COUNT(*) FROM only_a`, 2)

	// CE2: CTE EXCEPT with aggregate
	check("CE2 CTE EXCEPT agg",
		`WITH only_a AS (
			SELECT name, val FROM v65_a
			EXCEPT
			SELECT name, val FROM v65_b
		)
		SELECT SUM(val) FROM only_a`, 50)
	// Alice(10) + Dave(40) = 50

	// ============================================================
	// === SET OPERATIONS IN DERIVED TABLES ===
	// ============================================================

	// DI1: INTERSECT in derived table
	check("DI1 derived INTERSECT",
		`SELECT COUNT(*) FROM (
			SELECT name FROM v65_a
			INTERSECT
			SELECT name FROM v65_b
		) sub`, 2)

	// DE1: EXCEPT in derived table
	check("DE1 derived EXCEPT",
		`SELECT COUNT(*) FROM (
			SELECT name FROM v65_a
			EXCEPT
			SELECT name FROM v65_b
		) sub`, 2)

	// ============================================================
	// === COMPLEX SET OPERATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v65_c (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v65_c VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v65_c VALUES (2, 'Eve')")
	afExec(t, db, ctx, "INSERT INTO v65_c VALUES (3, 'Grace')")

	// CS1: UNION then INTERSECT (via CTEs)
	check("CS1 UNION then INTERSECT",
		`WITH ab AS (
			SELECT name FROM v65_a
			UNION
			SELECT name FROM v65_b
		),
		common_with_c AS (
			SELECT name FROM v65_c WHERE name IN (SELECT name FROM ab)
		)
		SELECT COUNT(*) FROM common_with_c`, 2)
	// ab: Alice,Bob,Carol,Dave,Eve,Frank; c: Alice,Eve,Grace → common: Alice, Eve = 2

	// CS2: EXCEPT with WHERE filter
	checkRowCount("CS2 EXCEPT filtered",
		`SELECT name FROM v65_a WHERE val > 15
		 EXCEPT
		 SELECT name FROM v65_b WHERE val > 25`, 2)
	// A filtered: Bob,Carol,Dave; B filtered: Carol,Eve,Frank; EXCEPT: Bob,Dave = 2

	// ============================================================
	// === UNION + INTERSECT CHAINED ===
	// ============================================================

	// CH1: Chained set operations via CTE
	check("CH1 chained CTE sets",
		`WITH step1 AS (
			SELECT name FROM v65_a
			EXCEPT
			SELECT name FROM v65_b
		)
		SELECT COUNT(*) FROM step1`, 2)
	// Alice, Dave only in A

	// ============================================================
	// === EMPTY RESULTS ===
	// ============================================================

	// EM1: INTERSECT with no common rows
	checkRowCount("EM1 INTERSECT empty",
		`SELECT name FROM v65_a WHERE val > 100
		 INTERSECT
		 SELECT name FROM v65_b`, 0)

	// EM2: EXCEPT where all rows match
	checkRowCount("EM2 EXCEPT empty",
		`SELECT name FROM v65_a WHERE name IN ('Bob', 'Carol')
		 EXCEPT
		 SELECT name FROM v65_b WHERE name IN ('Bob', 'Carol')`, 0)

	t.Logf("\n=== V65 SET OPERATIONS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
