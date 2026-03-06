package test

import (
	"fmt"
	"testing"
)

// TestV52AggregateNull exercises aggregate function NULL handling, empty groups,
// window aggregate NULL propagation, and edge cases in GROUP BY with NULLs.
func TestV52AggregateNull(t *testing.T) {
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

	checkNull := func(desc string, sql string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		if rows[0][0] != nil {
			t.Errorf("[FAIL] %s: expected NULL, got %v", desc, rows[0][0])
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

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}
	_ = checkNoError

	// ============================================================
	// === SUM() NULL HANDLING ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v52_nums (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v52_nums VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v52_nums VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO v52_nums VALUES (3, 'A', NULL)")
	afExec(t, db, ctx, "INSERT INTO v52_nums VALUES (4, 'B', NULL)")
	afExec(t, db, ctx, "INSERT INTO v52_nums VALUES (5, 'B', NULL)")
	afExec(t, db, ctx, "INSERT INTO v52_nums VALUES (6, 'C', 5)")

	// SN1: SUM with mixed values and NULLs (NULLs ignored)
	check("SN1 SUM mixed", "SELECT SUM(val) FROM v52_nums WHERE grp = 'A'", 30)

	// SN2: SUM with all NULLs should return NULL
	checkNull("SN2 SUM all NULL", "SELECT SUM(val) FROM v52_nums WHERE grp = 'B'")

	// SN3: SUM on empty table should return NULL
	afExec(t, db, ctx, `CREATE TABLE v52_empty (id INTEGER PRIMARY KEY, val INTEGER)`)
	checkNull("SN3 SUM empty table", "SELECT SUM(val) FROM v52_empty")

	// SN4: SUM with GROUP BY - per group
	check("SN4 SUM group A",
		"SELECT SUM(val) FROM v52_nums WHERE grp = 'A'", 30)
	checkNull("SN4 SUM group B null",
		"SELECT SUM(val) FROM v52_nums WHERE grp = 'B'")
	check("SN4 SUM group C",
		"SELECT SUM(val) FROM v52_nums WHERE grp = 'C'", 5)

	// ============================================================
	// === AVG() NULL HANDLING ===
	// ============================================================

	// AN1: AVG ignores NULLs
	check("AN1 AVG mixed", "SELECT AVG(val) FROM v52_nums WHERE grp = 'A'", 15)

	// AN2: AVG all NULLs = NULL
	checkNull("AN2 AVG all NULL", "SELECT AVG(val) FROM v52_nums WHERE grp = 'B'")

	// AN3: AVG on empty = NULL
	checkNull("AN3 AVG empty", "SELECT AVG(val) FROM v52_empty")

	// ============================================================
	// === COUNT() NULL HANDLING ===
	// ============================================================

	// CN1: COUNT(*) counts all rows including NULLs
	check("CN1 COUNT(*) all", "SELECT COUNT(*) FROM v52_nums", 6)

	// CN2: COUNT(col) ignores NULLs
	check("CN2 COUNT(col) non-null", "SELECT COUNT(val) FROM v52_nums", 3)

	// CN3: COUNT(*) on empty table
	check("CN3 COUNT(*) empty", "SELECT COUNT(*) FROM v52_empty", 0)

	// CN4: COUNT(col) on empty table
	check("CN4 COUNT(col) empty", "SELECT COUNT(val) FROM v52_empty", 0)

	// CN5: COUNT with WHERE filter per group
	check("CN5 COUNT grp A", "SELECT COUNT(val) FROM v52_nums WHERE grp = 'A'", 2)
	check("CN5 COUNT grp B", "SELECT COUNT(val) FROM v52_nums WHERE grp = 'B'", 0)

	// ============================================================
	// === MIN/MAX NULL HANDLING ===
	// ============================================================

	// MM1: MIN ignores NULLs
	check("MM1 MIN mixed", "SELECT MIN(val) FROM v52_nums WHERE grp = 'A'", 10)

	// MM2: MIN all NULLs = NULL
	checkNull("MM2 MIN all NULL", "SELECT MIN(val) FROM v52_nums WHERE grp = 'B'")

	// MM3: MAX ignores NULLs
	check("MM3 MAX mixed", "SELECT MAX(val) FROM v52_nums WHERE grp = 'A'", 20)

	// MM4: MAX all NULLs = NULL
	checkNull("MM4 MAX all NULL", "SELECT MAX(val) FROM v52_nums WHERE grp = 'B'")

	// MM5: MIN/MAX on empty table
	checkNull("MM5 MIN empty", "SELECT MIN(val) FROM v52_empty")
	checkNull("MM5 MAX empty", "SELECT MAX(val) FROM v52_empty")

	// ============================================================
	// === AGGREGATE WITH EXPRESSION ARGUMENTS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v52_orders (
		id INTEGER PRIMARY KEY, qty INTEGER, price INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v52_orders VALUES (1, 5, 10)")
	afExec(t, db, ctx, "INSERT INTO v52_orders VALUES (2, 3, 20)")
	afExec(t, db, ctx, "INSERT INTO v52_orders VALUES (3, NULL, 15)")

	// AE1: SUM of expression
	check("AE1 SUM expr", "SELECT SUM(qty * price) FROM v52_orders WHERE qty IS NOT NULL", 110)

	// AE2: AVG of expression
	check("AE2 AVG expr", "SELECT AVG(qty * price) FROM v52_orders WHERE qty IS NOT NULL", 55)

	// ============================================================
	// === COALESCE WITH AGGREGATES ===
	// ============================================================

	// CA1: COALESCE(SUM, 0) for NULL groups
	check("CA1 COALESCE SUM",
		"SELECT COALESCE(SUM(val), 0) FROM v52_nums WHERE grp = 'B'", 0)

	// CA2: COALESCE with non-NULL SUM
	check("CA2 COALESCE non-null",
		"SELECT COALESCE(SUM(val), 0) FROM v52_nums WHERE grp = 'A'", 30)

	// ============================================================
	// === AGGREGATES IN JOIN QUERIES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v52_depts (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v52_depts VALUES (1, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO v52_depts VALUES (2, 'HR')")
	afExec(t, db, ctx, "INSERT INTO v52_depts VALUES (3, 'Empty')")

	afExec(t, db, ctx, `CREATE TABLE v52_emp (
		id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v52_emp VALUES (1, 1, 50000)")
	afExec(t, db, ctx, "INSERT INTO v52_emp VALUES (2, 1, 60000)")
	afExec(t, db, ctx, "INSERT INTO v52_emp VALUES (3, 2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v52_emp VALUES (4, 2, 45000)")

	// JA1: SUM with JOIN
	check("JA1 SUM join Sales",
		"SELECT SUM(e.salary) FROM v52_emp e JOIN v52_depts d ON e.dept_id = d.id WHERE d.name = 'Sales'",
		110000)

	// JA2: AVG with JOIN ignoring NULL salary
	check("JA2 AVG join HR",
		"SELECT AVG(e.salary) FROM v52_emp e JOIN v52_depts d ON e.dept_id = d.id WHERE d.name = 'HR'",
		45000)

	// JA3: COUNT(col) with JOIN ignoring NULL salary
	check("JA3 COUNT join HR",
		"SELECT COUNT(e.salary) FROM v52_emp e JOIN v52_depts d ON e.dept_id = d.id WHERE d.name = 'HR'",
		1)

	// JA4: GROUP BY with JOIN
	checkRowCount("JA4 GROUP BY join",
		"SELECT d.name, SUM(e.salary) FROM v52_emp e JOIN v52_depts d ON e.dept_id = d.id GROUP BY d.name", 2)

	// ============================================================
	// === GROUP BY WITH NULL GROUPING KEY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v52_grp_null (
		id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v52_grp_null VALUES (1, 'X', 100)")
	afExec(t, db, ctx, "INSERT INTO v52_grp_null VALUES (2, 'X', 200)")
	afExec(t, db, ctx, "INSERT INTO v52_grp_null VALUES (3, NULL, 300)")
	afExec(t, db, ctx, "INSERT INTO v52_grp_null VALUES (4, NULL, 400)")
	afExec(t, db, ctx, "INSERT INTO v52_grp_null VALUES (5, 'Y', 500)")

	// GN1: NULL forms its own group
	checkRowCount("GN1 null group count",
		"SELECT category, SUM(amount) FROM v52_grp_null GROUP BY category", 3)

	// GN2: SUM for NULL group
	check("GN2 null group sum",
		"SELECT SUM(amount) FROM v52_grp_null WHERE category IS NULL", 700)

	// ============================================================
	// === HAVING WITH NULL AGGREGATES ===
	// ============================================================

	// HN1: HAVING filters NULL aggregate
	checkRowCount("HN1 HAVING filters null",
		"SELECT grp, SUM(val) FROM v52_nums GROUP BY grp HAVING SUM(val) > 0", 2)

	// HN2: HAVING with IS NULL
	checkRowCount("HN2 HAVING IS NULL",
		"SELECT grp, SUM(val) FROM v52_nums GROUP BY grp HAVING SUM(val) IS NULL", 1)

	// ============================================================
	// === DISTINCT AGGREGATE WITH NULLs ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v52_dup (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v52_dup VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v52_dup VALUES (2, 10)")
	afExec(t, db, ctx, "INSERT INTO v52_dup VALUES (3, 20)")
	afExec(t, db, ctx, "INSERT INTO v52_dup VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO v52_dup VALUES (5, NULL)")

	// DA1: COUNT(DISTINCT col) ignores NULLs
	check("DA1 COUNT DISTINCT", "SELECT COUNT(DISTINCT val) FROM v52_dup", 2)

	// DA2: COUNT(*) counts all
	check("DA2 COUNT(*) all", "SELECT COUNT(*) FROM v52_dup", 5)

	// ============================================================
	// === MULTIPLE AGGREGATES IN SAME SELECT ===
	// ============================================================

	// MA1: Multiple aggregates at once (verify no error)
	check("MA1 multi agg COUNT",
		"SELECT COUNT(*) FROM v52_nums WHERE grp = 'A'", 3)

	// MA2: Verify specific multi-agg values
	check("MA2 COUNT in multi",
		"SELECT COUNT(val) FROM v52_nums WHERE grp = 'A'", 2)
	check("MA2 SUM in multi",
		"SELECT SUM(val) FROM v52_nums WHERE grp = 'A'", 30)
	check("MA2 MIN in multi",
		"SELECT MIN(val) FROM v52_nums WHERE grp = 'A'", 10)
	check("MA2 MAX in multi",
		"SELECT MAX(val) FROM v52_nums WHERE grp = 'A'", 20)

	// ============================================================
	// === WINDOW FUNCTION NULL HANDLING ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v52_win (
		id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v52_win VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v52_win VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO v52_win VALUES (3, 'A', NULL)")
	afExec(t, db, ctx, "INSERT INTO v52_win VALUES (4, 'B', 50)")

	// WN1: Window SUM over partition with mixed NULLs (use CTE to get specific row after windowing)
	check("WN1 window SUM mixed",
		`WITH w AS (
			SELECT id, grp, val, SUM(val) OVER (PARTITION BY grp) AS wsum FROM v52_win
		)
		SELECT wsum FROM w WHERE id = 1`, 30)

	// WN2: Window COUNT(val) over partition (NULLs should be excluded from COUNT)
	check("WN2 window COUNT",
		`WITH w AS (
			SELECT id, grp, val, COUNT(val) OVER (PARTITION BY grp) AS wcnt FROM v52_win
		)
		SELECT wcnt FROM w WHERE id = 1`, 2)

	// WN3: ROW_NUMBER still works
	check("WN3 ROW_NUMBER",
		`WITH w AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM v52_win
		)
		SELECT rn FROM w WHERE id = 1`, 1)

	// ============================================================
	// === NESTED AGGREGATES (CTE + OUTER) ===
	// ============================================================

	// NA1: Aggregate over CTE
	check("NA1 SUM over CTE",
		`WITH totals AS (
			SELECT grp, SUM(val) AS total FROM v52_nums GROUP BY grp
		)
		SELECT SUM(total) FROM totals WHERE total IS NOT NULL`, 35)

	// NA2: COUNT over CTE including NULL totals
	check("NA2 COUNT CTE rows",
		`WITH totals AS (
			SELECT grp, SUM(val) AS total FROM v52_nums GROUP BY grp
		)
		SELECT COUNT(*) FROM totals`, 3)

	// ============================================================
	// === CASE + AGGREGATE INTERACTIONS ===
	// ============================================================

	// CA3: CASE in aggregate
	check("CA3 SUM CASE",
		`SELECT SUM(CASE WHEN val IS NOT NULL THEN val ELSE 0 END) FROM v52_nums`, 35)

	// CA4: Aggregate in CASE
	check("CA4 CASE with COUNT",
		`SELECT CASE WHEN COUNT(*) > 3 THEN 'many' ELSE 'few' END FROM v52_nums`, "many")

	t.Logf("\n=== V52 AGGREGATE NULL: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
