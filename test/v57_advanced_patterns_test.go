package test

import (
	"fmt"
	"testing"
)

// TestV57AdvancedPatterns tests advanced SQL patterns including chained CTEs,
// window functions with PARTITION+ORDER, complex CASE expressions, and
// multi-level aggregation.
func TestV57AdvancedPatterns(t *testing.T) {
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

	afExec(t, db, ctx, `CREATE TABLE v57_emp (
		id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER, hire_year INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v57_emp VALUES (1, 'Alice', 'Eng', 90000, 2020)")
	afExec(t, db, ctx, "INSERT INTO v57_emp VALUES (2, 'Bob', 'Eng', 85000, 2019)")
	afExec(t, db, ctx, "INSERT INTO v57_emp VALUES (3, 'Carol', 'Eng', 95000, 2021)")
	afExec(t, db, ctx, "INSERT INTO v57_emp VALUES (4, 'Dave', 'Sales', 70000, 2020)")
	afExec(t, db, ctx, "INSERT INTO v57_emp VALUES (5, 'Eve', 'Sales', 75000, 2021)")
	afExec(t, db, ctx, "INSERT INTO v57_emp VALUES (6, 'Frank', 'HR', 60000, 2019)")
	afExec(t, db, ctx, "INSERT INTO v57_emp VALUES (7, 'Grace', 'HR', 65000, 2022)")
	afExec(t, db, ctx, "INSERT INTO v57_emp VALUES (8, 'Heidi', 'Eng', 100000, 2018)")

	// ============================================================
	// === CHAINED CTEs ===
	// ============================================================

	// CC1: Three-level chained CTE
	// dept_avg: Eng=92500, Sales=72500, HR=62500
	// above_avg (>70000): Eng, Sales
	// emp_in_above: 4(Eng) + 2(Sales) = 6
	check("CC1 three-level CTE",
		`WITH
		 dept_avg AS (
			SELECT dept, AVG(salary) AS avg_sal FROM v57_emp GROUP BY dept
		 ),
		 above_avg AS (
			SELECT dept FROM dept_avg WHERE avg_sal > 70000
		 ),
		 emp_in_above AS (
			SELECT COUNT(*) AS cnt FROM v57_emp WHERE dept IN (SELECT dept FROM above_avg)
		 )
		 SELECT cnt FROM emp_in_above`, 6)

	// CC2: Chained CTE with JOIN
	check("CC2 chained CTE join",
		`WITH
		 dept_stats AS (
			SELECT dept, COUNT(*) AS emp_count, AVG(salary) AS avg_sal
			FROM v57_emp GROUP BY dept
		 ),
		 best_dept AS (
			SELECT dept FROM dept_stats ORDER BY avg_sal DESC LIMIT 1
		 )
		 SELECT name FROM v57_emp WHERE dept = (SELECT dept FROM best_dept) ORDER BY salary DESC LIMIT 1`, "Heidi")

	// CC3: Chained CTE with window function
	check("CC3 chained CTE window",
		`WITH
		 dept_totals AS (
			SELECT dept, SUM(salary) AS total FROM v57_emp GROUP BY dept
		 ),
		 ranked AS (
			SELECT dept, total, RANK() OVER (ORDER BY total DESC) AS rnk
			FROM dept_totals
		 )
		 SELECT dept FROM ranked WHERE rnk = 1`, "Eng")

	// ============================================================
	// === WINDOW FUNCTIONS WITH PARTITION BY + ORDER BY ===
	// ============================================================

	// WP1: Running SUM within partitions
	check("WP1 running SUM partition",
		`WITH w AS (
			SELECT name, dept, salary,
				   SUM(salary) OVER (PARTITION BY dept ORDER BY salary ASC) AS running_total
			FROM v57_emp
		)
		SELECT running_total FROM w WHERE name = 'Alice'`, 175000)

	// WP2: RANK within department
	check("WP2 RANK partition",
		`WITH w AS (
			SELECT name, dept, salary,
				   RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank
			FROM v57_emp
		)
		SELECT dept_rank FROM w WHERE name = 'Alice'`, 3)

	// WP3: ROW_NUMBER within department
	check("WP3 ROW_NUMBER partition",
		`WITH w AS (
			SELECT name, dept, salary,
				   ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
			FROM v57_emp
		)
		SELECT name FROM w WHERE dept = 'Eng' AND rn = 1`, "Heidi")

	// WP4: LAG within department
	check("WP4 LAG partition",
		`WITH w AS (
			SELECT name, dept, salary,
				   LAG(salary) OVER (PARTITION BY dept ORDER BY salary ASC) AS prev_salary
			FROM v57_emp
		)
		SELECT prev_salary FROM w WHERE name = 'Alice'`, 85000)

	// ============================================================
	// === COMPLEX CASE EXPRESSIONS ===
	// ============================================================

	// CE1: CASE with aggregate
	check("CE1 CASE aggregate",
		`SELECT CASE
			WHEN AVG(salary) > 80000 THEN 'high'
			WHEN AVG(salary) > 60000 THEN 'medium'
			ELSE 'low'
		 END FROM v57_emp WHERE dept = 'Eng'`, "high")

	// CE2: Searched CASE in GROUP BY
	checkRowCount("CE2 CASE GROUP BY",
		`SELECT
			CASE WHEN salary >= 80000 THEN 'senior' ELSE 'junior' END AS level,
			COUNT(*)
		 FROM v57_emp
		 GROUP BY CASE WHEN salary >= 80000 THEN 'senior' ELSE 'junior' END`, 2)

	// CE3: CASE with IN
	check("CE3 CASE IN",
		`SELECT CASE
			WHEN dept IN ('Eng', 'Sales') THEN 'revenue'
			ELSE 'support'
		 END FROM v57_emp WHERE id = 1`, "revenue")

	// ============================================================
	// === MULTI-LEVEL AGGREGATION ===
	// ============================================================

	// MA1: Aggregate over CTE aggregate
	check("MA1 agg over agg",
		`WITH dept_avgs AS (
			SELECT dept, AVG(salary) AS avg_sal FROM v57_emp GROUP BY dept
		)
		SELECT MAX(avg_sal) FROM dept_avgs`, 92500)

	// MA2: Count of groups
	check("MA2 count groups",
		`WITH dept_counts AS (
			SELECT dept, COUNT(*) AS cnt FROM v57_emp GROUP BY dept
		)
		SELECT COUNT(*) FROM dept_counts WHERE cnt > 1`, 3)

	// ============================================================
	// === SUBQUERY PATTERNS ===
	// ============================================================

	// SQ1: Correlated subquery - employees earning above dept average
	checkRowCount("SQ1 correlated above avg",
		`SELECT name FROM v57_emp e
		 WHERE salary > (SELECT AVG(salary) FROM v57_emp WHERE dept = e.dept)`, 4)

	// SQ2: EXISTS with correlated subquery
	checkRowCount("SQ2 EXISTS correlated",
		`SELECT DISTINCT dept FROM v57_emp e
		 WHERE EXISTS (SELECT 1 FROM v57_emp WHERE dept = e.dept AND salary > 90000)`, 1)

	// SQ3: Subquery in CASE
	check("SQ3 subquery in CASE",
		`SELECT CASE
			WHEN (SELECT MAX(salary) FROM v57_emp) > 90000 THEN 'yes'
			ELSE 'no'
		 END FROM v57_emp WHERE id = 1`, "yes")

	// ============================================================
	// === MIXED JOIN + AGGREGATE + WINDOW ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v57_bonus (
		emp_id INTEGER, bonus INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v57_bonus VALUES (1, 5000)")
	afExec(t, db, ctx, "INSERT INTO v57_bonus VALUES (2, 3000)")
	afExec(t, db, ctx, "INSERT INTO v57_bonus VALUES (3, 7000)")
	afExec(t, db, ctx, "INSERT INTO v57_bonus VALUES (8, 10000)")

	// MX1: JOIN + aggregate
	check("MX1 JOIN aggregate",
		`SELECT SUM(b.bonus) FROM v57_bonus b
		 JOIN v57_emp e ON b.emp_id = e.id
		 WHERE e.dept = 'Eng'`, 25000)

	// MX2: CTE from JOIN + window
	check("MX2 CTE join window",
		`WITH emp_bonus AS (
			SELECT e.name, e.dept, e.salary + COALESCE(b.bonus, 0) AS total_comp
			FROM v57_emp e
			LEFT JOIN v57_bonus b ON e.id = b.emp_id
		),
		ranked AS (
			SELECT name, dept, total_comp,
				   ROW_NUMBER() OVER (ORDER BY total_comp DESC) AS rn
			FROM emp_bonus
		)
		SELECT name FROM ranked WHERE rn = 1`, "Heidi")

	// ============================================================
	// === DISTINCT + AGGREGATE ===
	// ============================================================

	// DA1: COUNT DISTINCT
	check("DA1 COUNT DISTINCT",
		"SELECT COUNT(DISTINCT dept) FROM v57_emp", 3)

	// DA2: COUNT DISTINCT with WHERE
	check("DA2 COUNT DISTINCT WHERE",
		"SELECT COUNT(DISTINCT dept) FROM v57_emp WHERE salary > 70000", 2)

	// ============================================================
	// === INSERT INTO SELECT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v57_senior (
		id INTEGER PRIMARY KEY, name TEXT, salary INTEGER)`)

	afExec(t, db, ctx, "INSERT INTO v57_senior SELECT id, name, salary FROM v57_emp WHERE salary >= 90000")
	check("IIS insert select",
		"SELECT COUNT(*) FROM v57_senior", 3)

	// ============================================================
	// === SINGLE COLUMN UNIQUE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v57_codes (
		id INTEGER PRIMARY KEY, code TEXT UNIQUE)`)
	afExec(t, db, ctx, "INSERT INTO v57_codes VALUES (1, 'ABC')")

	// UC1: Duplicate unique should error
	_, err := db.Exec(ctx, "INSERT INTO v57_codes VALUES (2, 'ABC')")
	total++
	if err != nil {
		pass++
	} else {
		t.Errorf("[FAIL] UC1: expected unique constraint error")
	}

	// UC2: Different value should work
	_, err2 := db.Exec(ctx, "INSERT INTO v57_codes VALUES (2, 'DEF')")
	total++
	if err2 == nil {
		pass++
	} else {
		t.Errorf("[FAIL] UC2: %v", err2)
	}

	t.Logf("\n=== V57 ADVANCED PATTERNS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
