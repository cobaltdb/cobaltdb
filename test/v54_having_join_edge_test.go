package test

import (
	"fmt"
	"testing"
)

// TestV54HavingJoinEdge tests HAVING with aggregate comparisons, LEFT JOIN NULLs,
// multi-column ORDER BY, DISTINCT with aggregates, and nested GROUP BY.
func TestV54HavingJoinEdge(t *testing.T) {
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

	// ============================================================
	// === SETUP ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v54_emp (
		id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v54_emp VALUES (1, 'Alice', 'Eng', 90000)")
	afExec(t, db, ctx, "INSERT INTO v54_emp VALUES (2, 'Bob', 'Eng', 85000)")
	afExec(t, db, ctx, "INSERT INTO v54_emp VALUES (3, 'Carol', 'Eng', 95000)")
	afExec(t, db, ctx, "INSERT INTO v54_emp VALUES (4, 'Dave', 'Sales', 70000)")
	afExec(t, db, ctx, "INSERT INTO v54_emp VALUES (5, 'Eve', 'Sales', 75000)")
	afExec(t, db, ctx, "INSERT INTO v54_emp VALUES (6, 'Frank', 'HR', 60000)")

	afExec(t, db, ctx, `CREATE TABLE v54_projects (
		id INTEGER PRIMARY KEY, name TEXT, dept TEXT, budget INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v54_projects VALUES (1, 'Alpha', 'Eng', 500000)")
	afExec(t, db, ctx, "INSERT INTO v54_projects VALUES (2, 'Beta', 'Eng', 300000)")
	afExec(t, db, ctx, "INSERT INTO v54_projects VALUES (3, 'Gamma', 'Sales', 200000)")
	afExec(t, db, ctx, "INSERT INTO v54_projects VALUES (4, 'Delta', 'Marketing', 100000)")

	// ============================================================
	// === HAVING WITH AGGREGATE COMPARISONS ===
	// ============================================================

	// H1: HAVING COUNT > threshold
	checkRowCount("H1 HAVING COUNT",
		`SELECT dept, COUNT(*) as cnt FROM v54_emp
		 GROUP BY dept HAVING COUNT(*) > 1`, 2)

	// H2: HAVING SUM > threshold
	checkRowCount("H2 HAVING SUM",
		`SELECT dept, SUM(salary) FROM v54_emp
		 GROUP BY dept HAVING SUM(salary) > 100000`, 2)

	// H3: HAVING AVG > threshold
	check("H3 HAVING AVG dept",
		`SELECT dept FROM v54_emp
		 GROUP BY dept HAVING AVG(salary) > 80000`, "Eng")

	// H4: HAVING with multiple conditions
	checkRowCount("H4 HAVING multi",
		`SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal
		 FROM v54_emp
		 GROUP BY dept
		 HAVING COUNT(*) >= 2 AND AVG(salary) > 70000`, 2)

	// H5: HAVING with COUNT = exact value
	checkRowCount("H5 HAVING exact",
		`SELECT dept FROM v54_emp
		 GROUP BY dept HAVING COUNT(*) = 1`, 1)

	// ============================================================
	// === LEFT JOIN EDGE CASES ===
	// ============================================================

	// LJ1: LEFT JOIN with unmatched rows (Marketing dept has no employees)
	// Alpha(Eng)x3 + Beta(Eng)x3 + Gamma(Sales)x2 + Delta(Marketing)x1(NULL) = 9
	checkRowCount("LJ1 LEFT JOIN all",
		`SELECT p.name, e.name FROM v54_projects p
		 LEFT JOIN v54_emp e ON p.dept = e.dept`, 9)

	// LJ2: LEFT JOIN - unmatched row has NULL
	check("LJ2 NULL for unmatched",
		`SELECT COALESCE(e.name, 'NO_EMP') FROM v54_projects p
		 LEFT JOIN v54_emp e ON p.dept = e.dept
		 WHERE p.name = 'Delta'`, "NO_EMP")

	// LJ3: LEFT JOIN + COUNT - Marketing has no employees
	check("LJ3 LEFT JOIN COUNT",
		`SELECT COUNT(e.id) FROM v54_projects p
		 LEFT JOIN v54_emp e ON p.dept = e.dept
		 WHERE p.name = 'Delta'`, 0)

	// LJ4: LEFT JOIN + aggregate GROUP BY
	checkRowCount("LJ4 LEFT JOIN GROUP BY",
		`SELECT p.dept, COUNT(e.id) FROM v54_projects p
		 LEFT JOIN v54_emp e ON p.dept = e.dept
		 GROUP BY p.dept`, 3)

	// ============================================================
	// === MULTI-COLUMN ORDER BY ===
	// ============================================================

	// MO1: ORDER BY two columns (same direction)
	check("MO1 multi ORDER BY",
		`SELECT name FROM v54_emp ORDER BY dept ASC, salary DESC LIMIT 1`, "Carol")

	// MO2: ORDER BY mixed direction
	check("MO2 mixed ORDER BY",
		`SELECT name FROM v54_emp ORDER BY dept DESC, salary ASC LIMIT 1`, "Dave")

	// MO3: ORDER BY with column alias
	check("MO3 ORDER BY alias",
		`SELECT name, salary * 12 AS annual FROM v54_emp ORDER BY annual DESC LIMIT 1`, "Carol")

	// ============================================================
	// === DISTINCT WITH VARIOUS QUERIES ===
	// ============================================================

	// DI1: DISTINCT single column
	checkRowCount("DI1 DISTINCT dept",
		"SELECT DISTINCT dept FROM v54_emp", 3)

	// DI2: DISTINCT with ORDER BY
	check("DI2 DISTINCT ORDER BY",
		"SELECT DISTINCT dept FROM v54_emp ORDER BY dept ASC LIMIT 1", "Eng")

	// DI3: DISTINCT with JOIN
	checkRowCount("DI3 DISTINCT JOIN",
		`SELECT DISTINCT p.dept FROM v54_projects p
		 JOIN v54_emp e ON p.dept = e.dept`, 2)

	// ============================================================
	// === CTE WITH AGGREGATES ===
	// ============================================================

	// CA1: CTE with aggregate + outer filter
	check("CA1 CTE agg filter",
		`WITH dept_stats AS (
			SELECT dept, AVG(salary) AS avg_sal, COUNT(*) AS cnt
			FROM v54_emp GROUP BY dept
		)
		SELECT dept FROM dept_stats WHERE avg_sal > 80000`, "Eng")

	// CA2: CTE + outer ORDER BY
	check("CA2 CTE ORDER BY",
		`WITH dept_totals AS (
			SELECT dept, SUM(salary) AS total FROM v54_emp GROUP BY dept
		)
		SELECT dept FROM dept_totals ORDER BY total DESC LIMIT 1`, "Eng")

	// CA3: Multiple CTEs
	check("CA3 multi CTE",
		`WITH
		 eng AS (SELECT COUNT(*) AS cnt FROM v54_emp WHERE dept = 'Eng'),
		 sales AS (SELECT COUNT(*) AS cnt FROM v54_emp WHERE dept = 'Sales')
		SELECT eng.cnt + sales.cnt FROM eng, sales`, 5)

	// ============================================================
	// === JOIN + AGGREGATE + HAVING ===
	// ============================================================

	// JAH1: JOIN + GROUP BY + HAVING
	checkRowCount("JAH1 join group having",
		`SELECT e.dept, COUNT(DISTINCT p.id) AS proj_count
		 FROM v54_emp e
		 JOIN v54_projects p ON e.dept = p.dept
		 GROUP BY e.dept
		 HAVING COUNT(DISTINCT p.id) >= 2`, 1)

	// ============================================================
	// === SUBQUERY COMPARISONS ===
	// ============================================================

	// SC1: WHERE col > (subquery)
	checkRowCount("SC1 greater than subquery",
		`SELECT name FROM v54_emp
		 WHERE salary > (SELECT AVG(salary) FROM v54_emp)`, 3)

	// SC2: WHERE col = (subquery)
	check("SC2 equal subquery",
		`SELECT name FROM v54_emp
		 WHERE salary = (SELECT MAX(salary) FROM v54_emp)`, "Carol")

	// SC3: WHERE col < (subquery)
	checkRowCount("SC3 less than subquery",
		`SELECT name FROM v54_emp
		 WHERE salary < (SELECT AVG(salary) FROM v54_emp)`, 3)

	// ============================================================
	// === NESTED CTE WITH JOIN ===
	// ============================================================

	// NC1: CTE feeding into JOIN
	checkRowCount("NC1 CTE JOIN",
		`WITH high_paid AS (
			SELECT * FROM v54_emp WHERE salary > 80000
		)
		SELECT h.name, p.name FROM high_paid h
		JOIN v54_projects p ON h.dept = p.dept`, 6)

	// ============================================================
	// === AGGREGATE OVER JOINED DATA ===
	// ============================================================

	// AJ1: SUM with JOIN
	check("AJ1 SUM join budget",
		`SELECT SUM(p.budget) FROM v54_projects p
		 JOIN v54_emp e ON p.dept = e.dept
		 WHERE e.name = 'Alice'`, 800000)

	// AJ2: MAX with JOIN
	check("AJ2 MAX join salary",
		`SELECT MAX(e.salary) FROM v54_emp e
		 JOIN v54_projects p ON e.dept = p.dept
		 WHERE p.name = 'Alpha'`, 95000)

	// ============================================================
	// === GROUP BY WITH EXPRESSIONS ===
	// ============================================================

	// GE1: GROUP BY expression
	checkRowCount("GE1 GROUP BY CASE",
		`SELECT CASE WHEN salary >= 80000 THEN 'high' ELSE 'low' END AS tier, COUNT(*)
		 FROM v54_emp
		 GROUP BY CASE WHEN salary >= 80000 THEN 'high' ELSE 'low' END`, 2)

	// ============================================================
	// === EMPTY RESULT SET HANDLING ===
	// ============================================================

	// ER1: WHERE matches nothing
	checkRowCount("ER1 no match", "SELECT * FROM v54_emp WHERE dept = 'NonExistent'", 0)

	// ER2: Aggregate on empty result
	check("ER2 COUNT empty WHERE", "SELECT COUNT(*) FROM v54_emp WHERE dept = 'NonExistent'", 0)

	// ER3: JOIN with no matches
	checkRowCount("ER3 JOIN no match",
		`SELECT * FROM v54_emp e
		 JOIN v54_projects p ON e.dept = p.dept
		 WHERE p.dept = 'NonExistent'`, 0)

	// ============================================================
	// === MULTIPLE JOINS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v54_tasks (
		id INTEGER PRIMARY KEY, project_id INTEGER, assignee_id INTEGER, hours INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v54_tasks VALUES (1, 1, 1, 40)")
	afExec(t, db, ctx, "INSERT INTO v54_tasks VALUES (2, 1, 2, 30)")
	afExec(t, db, ctx, "INSERT INTO v54_tasks VALUES (3, 2, 3, 50)")
	afExec(t, db, ctx, "INSERT INTO v54_tasks VALUES (4, 3, 4, 20)")

	// MJ1: Three-table JOIN
	checkRowCount("MJ1 three-table join",
		`SELECT e.name, p.name, t.hours
		 FROM v54_tasks t
		 JOIN v54_emp e ON t.assignee_id = e.id
		 JOIN v54_projects p ON t.project_id = p.id`, 4)

	// MJ2: Three-table JOIN + aggregate
	check("MJ2 three-table agg",
		`SELECT SUM(t.hours) FROM v54_tasks t
		 JOIN v54_emp e ON t.assignee_id = e.id
		 JOIN v54_projects p ON t.project_id = p.id
		 WHERE e.dept = 'Eng'`, 120)

	// MJ3: Three-table JOIN + GROUP BY
	checkRowCount("MJ3 three-table group",
		`SELECT e.dept, SUM(t.hours) FROM v54_tasks t
		 JOIN v54_emp e ON t.assignee_id = e.id
		 JOIN v54_projects p ON t.project_id = p.id
		 GROUP BY e.dept`, 2)

	t.Logf("\n=== V54 HAVING/JOIN EDGE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
