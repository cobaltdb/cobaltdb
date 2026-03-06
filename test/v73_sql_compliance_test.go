package test

import (
	"fmt"
	"testing"
)

// TestV73SQLCompliance tests SQL compliance: complex CASE, expression in GROUP BY/HAVING,
// complex alias usage, ORDER BY mixed directions, IS NULL combinations,
// nested aggregates, and standard SQL patterns.
func TestV73SQLCompliance(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			if expected == nil {
				pass++
				return
			}
			t.Errorf("[FAIL] %s: no rows returned, expected %v", desc, expected)
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
	// === SETUP: Employee/Department Schema ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v73_dept (
		id INTEGER PRIMARY KEY, name TEXT, budget INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v73_dept VALUES (1, 'Engineering', 500000)")
	afExec(t, db, ctx, "INSERT INTO v73_dept VALUES (2, 'Sales', 300000)")
	afExec(t, db, ctx, "INSERT INTO v73_dept VALUES (3, 'HR', 200000)")
	afExec(t, db, ctx, "INSERT INTO v73_dept VALUES (4, 'Marketing', 250000)")

	afExec(t, db, ctx, `CREATE TABLE v73_emp (
		id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER,
		salary INTEGER, hire_year INTEGER, active INTEGER DEFAULT 1
	)`)
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (1, 'Alice', 1, 120000, 2020, 1)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (2, 'Bob', 1, 110000, 2019, 1)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (3, 'Carol', 2, 90000, 2021, 1)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (4, 'Dave', 2, 85000, 2020, 1)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (5, 'Eve', 3, 75000, 2018, 0)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (6, 'Frank', 1, 130000, 2017, 1)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (7, 'Grace', 3, 80000, 2022, 1)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (8, 'Hank', 4, 95000, 2021, 1)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (9, 'Ivy', 4, 88000, 2020, 0)")
	afExec(t, db, ctx, "INSERT INTO v73_emp VALUES (10, 'Jack', 2, 92000, 2019, 1)")

	// ============================================================
	// === CASE EXPRESSION EDGE CASES ===
	// ============================================================

	// CE1: Simple CASE
	check("CE1 simple CASE",
		"SELECT CASE dept_id WHEN 1 THEN 'eng' WHEN 2 THEN 'sales' ELSE 'other' END FROM v73_emp WHERE id = 1", "eng")

	// CE2: Searched CASE
	check("CE2 searched CASE",
		`SELECT CASE
			WHEN salary >= 120000 THEN 'senior'
			WHEN salary >= 90000 THEN 'mid'
			ELSE 'junior'
		 END FROM v73_emp WHERE id = 1`, "senior")

	// CE3: CASE with NULL
	check("CE3 CASE NULL",
		`SELECT CASE
			WHEN NULL THEN 'yes'
			ELSE 'no'
		 END`, "no")

	// CE4: CASE in aggregate
	check("CE4 CASE in SUM",
		`SELECT SUM(CASE WHEN active = 1 THEN salary ELSE 0 END) FROM v73_emp`, 802000)
	// Active: 120+110+90+85+130+80+95+92 = 802 (thousands)

	// CE5: CASE in GROUP BY result
	checkRowCount("CE5 CASE GROUP BY",
		`SELECT CASE WHEN salary >= 100000 THEN 'high' ELSE 'normal' END as level,
				COUNT(*) FROM v73_emp GROUP BY level`, 2)

	// CE6: Nested CASE
	check("CE6 nested CASE",
		`SELECT CASE
			WHEN dept_id = 1 THEN
				CASE WHEN salary > 115000 THEN 'senior eng' ELSE 'eng' END
			ELSE 'other'
		 END FROM v73_emp WHERE id = 1`, "senior eng")

	// ============================================================
	// === COMPLEX GROUP BY / HAVING ===
	// ============================================================

	// GH1: GROUP BY with multiple aggregates and HAVING
	checkRowCount("GH1 multi agg HAVING",
		`SELECT dept_id, COUNT(*) as cnt, AVG(salary) as avg_sal
		 FROM v73_emp
		 GROUP BY dept_id
		 HAVING COUNT(*) >= 2 AND AVG(salary) > 80000`, 3)
	// Eng: 3 emps, avg=120k ✓; Sales: 3 emps, avg=89k ✓; HR: 2 emps, avg=77.5k ✗; Mkt: 2 emps, avg=91.5k ✓

	// GH2: HAVING with OR
	checkRowCount("GH2 HAVING OR",
		`SELECT dept_id, SUM(salary) as total
		 FROM v73_emp
		 GROUP BY dept_id
		 HAVING SUM(salary) > 300000 OR COUNT(*) >= 3`, 2)
	// Eng: sum=360k>300k ✓; Sales: cnt=3 ✓; HR: sum=155k, cnt=2 ✗; Mkt: sum=183k, cnt=2 ✗

	// GH3: GROUP BY with WHERE filter
	check("GH3 GROUP WHERE",
		`SELECT COUNT(*) FROM v73_emp
		 WHERE active = 1
		 GROUP BY dept_id
		 ORDER BY COUNT(*) DESC LIMIT 1`, 3)
	// Active by dept: Eng=3, Sales=3, HR=1, Mkt=1 → max=3

	// ============================================================
	// === COMPLEX JOIN PATTERNS ===
	// ============================================================

	// JP1: JOIN with aggregate and alias
	check("JP1 JOIN agg alias",
		`SELECT d.name FROM v73_emp e
		 JOIN v73_dept d ON e.dept_id = d.id
		 GROUP BY d.name
		 ORDER BY SUM(e.salary) DESC LIMIT 1`, "Engineering")

	// JP2: LEFT JOIN with IS NULL
	checkRowCount("JP2 LEFT JOIN IS NULL",
		`SELECT d.name FROM v73_dept d
		 LEFT JOIN v73_emp e ON d.id = e.dept_id AND e.active = 0
		 WHERE e.id IS NULL`, 2)
	// Only HR and Marketing have inactive emps (Eve, Ivy). Dept without inactive: Eng, Sales = 2

	// JP3: Self-join with alias - count pairs where e1.salary > e2.salary in same dept
	check("JP3 self-join count",
		`SELECT COUNT(*) FROM v73_emp e1
		 JOIN v73_emp e2 ON e1.dept_id = e2.dept_id AND e1.salary > e2.salary`, 8)

	// JP4: JOIN with CASE
	check("JP4 JOIN CASE",
		`SELECT CASE WHEN d.budget > 300000 THEN 'high' ELSE 'normal' END
		 FROM v73_dept d WHERE d.id = 1`, "high")

	// ============================================================
	// === COMPLEX WHERE PATTERNS ===
	// ============================================================

	// WP1: IS NULL combined with AND
	checkRowCount("WP1 IS NOT NULL AND",
		"SELECT * FROM v73_emp WHERE active = 1 AND salary IS NOT NULL", 8)

	// WP2: Multiple OR with AND
	checkRowCount("WP2 complex OR AND",
		`SELECT * FROM v73_emp
		 WHERE (dept_id = 1 AND salary > 115000)
		    OR (dept_id = 2 AND salary > 89000)
		    OR (dept_id = 3 AND active = 1)`, 5)
	// Eng >115k: Alice(120k), Frank(130k) = 2
	// Sales >89k: Carol(90k), Jack(92k) = 2
	// HR active: Grace(id=7) = 1 → Total = 5

	// WP3: NOT with OR
	checkRowCount("WP3 NOT OR",
		"SELECT * FROM v73_emp WHERE NOT (dept_id = 1 OR dept_id = 2)", 4)
	// HR: Eve, Grace; Mkt: Hank, Ivy = 4

	// WP4: BETWEEN with AND
	checkRowCount("WP4 BETWEEN AND",
		"SELECT * FROM v73_emp WHERE salary BETWEEN 80000 AND 100000 AND active = 1", 5)
	// Active with salary 80k-100k: Carol(90k), Dave(85k), Grace(80k), Hank(95k), Jack(92k) = 5

	// ============================================================
	// === SUBQUERY PATTERNS ===
	// ============================================================

	// SQ1: Scalar subquery in SELECT
	check("SQ1 scalar subquery",
		`SELECT name FROM v73_emp
		 WHERE salary = (SELECT MAX(salary) FROM v73_emp)`, "Frank")

	// SQ2: IN subquery
	checkRowCount("SQ2 IN subquery",
		`SELECT * FROM v73_emp
		 WHERE dept_id IN (SELECT id FROM v73_dept WHERE budget > 250000)`, 6)
	// Depts with budget>250k: Eng(500k), Sales(300k) → 3+3=6

	// SQ3: EXISTS subquery
	checkRowCount("SQ3 EXISTS",
		`SELECT * FROM v73_dept d
		 WHERE EXISTS (SELECT 1 FROM v73_emp WHERE dept_id = d.id AND salary > 100000)`, 1)
	// Only Engineering has emp with salary > 100k

	// SQ4: NOT IN subquery
	checkRowCount("SQ4 NOT IN subquery",
		`SELECT * FROM v73_emp
		 WHERE dept_id NOT IN (SELECT id FROM v73_dept WHERE budget < 250000)`, 8)
	// budget < 250k: HR(200k) → NOT IN dept 3 → Eng(3)+Sales(3)+Mkt(2) = 8

	// SQ5: Correlated subquery with alias
	check("SQ5 correlated above avg",
		`SELECT COUNT(*) FROM v73_emp e1
		 WHERE salary > (SELECT AVG(salary) FROM v73_emp e2 WHERE e2.dept_id = e1.dept_id)`, 5)

	// ============================================================
	// === CTE PATTERNS ===
	// ============================================================

	// CT1: CTE with window function
	check("CT1 CTE window",
		`WITH ranked AS (
			SELECT name, salary,
				   ROW_NUMBER() OVER (ORDER BY salary DESC) as rn
			FROM v73_emp WHERE active = 1
		)
		SELECT name FROM ranked WHERE rn = 1`, "Frank")

	// CT2: CTE with JOIN
	check("CT2 CTE JOIN",
		`WITH dept_stats AS (
			SELECT dept_id, AVG(salary) as avg_sal, COUNT(*) as emp_count
			FROM v73_emp WHERE active = 1
			GROUP BY dept_id
		)
		SELECT d.name FROM dept_stats ds
		JOIN v73_dept d ON ds.dept_id = d.id
		ORDER BY ds.avg_sal DESC LIMIT 1`, "Engineering")

	// CT3: Multiple CTEs
	check("CT3 multi CTE",
		`WITH active_emps AS (
			SELECT * FROM v73_emp WHERE active = 1
		),
		dept_totals AS (
			SELECT dept_id, SUM(salary) as total FROM active_emps GROUP BY dept_id
		)
		SELECT MAX(total) FROM dept_totals`, 360000)
	// Eng active: 120+110+130=360k

	// CT4: CTE used in subquery
	check("CT4 CTE in subquery",
		`WITH avg_by_dept AS (
			SELECT dept_id, AVG(salary) as avg_sal FROM v73_emp GROUP BY dept_id
		)
		SELECT COUNT(*) FROM v73_emp
		WHERE salary > (SELECT AVG(avg_sal) FROM avg_by_dept)`, 4)
	// AVG of dept averages: (120000+89000+77500+91500)/4 ≈ 94500
	// Above 94500: Alice(120k), Bob(110k), Frank(130k), Hank(95k) = 4

	// ============================================================
	// === ORDER BY COMPLEX ===
	// ============================================================

	// OC1: ORDER BY multiple columns mixed directions
	check("OC1 ORDER mixed",
		"SELECT name FROM v73_emp ORDER BY dept_id ASC, salary DESC LIMIT 1", "Frank")
	// Dept 1 first, highest salary: Frank(130k)

	// OC2: ORDER BY alias
	check("OC2 ORDER BY alias",
		`SELECT name, salary * 12 as annual
		 FROM v73_emp ORDER BY annual DESC LIMIT 1`, "Frank")

	// OC3: ORDER BY aggregate (in GROUP BY context)
	check("OC3 ORDER BY agg",
		`SELECT dept_id FROM v73_emp
		 GROUP BY dept_id
		 ORDER BY SUM(salary) DESC LIMIT 1`, 1)

	// OC4: ORDER BY with LIMIT and OFFSET
	check("OC4 ORDER LIMIT OFFSET",
		"SELECT name FROM v73_emp ORDER BY salary DESC LIMIT 1 OFFSET 1", "Alice")
	// Sorted DESC: Frank(130k), Alice(120k), Bob(110k)... → offset 1 = Alice

	// ============================================================
	// === AGGREGATE EDGE CASES ===
	// ============================================================

	// AE1: COUNT(*) vs COUNT(col) with NULLs
	afExec(t, db, ctx, `CREATE TABLE v73_nullagg (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v73_nullagg VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v73_nullagg VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v73_nullagg VALUES (3, 20)")
	check("AE1 COUNT(*)", "SELECT COUNT(*) FROM v73_nullagg", 3)
	check("AE1 COUNT(val)", "SELECT COUNT(val) FROM v73_nullagg", 2)

	// AE2: SUM/AVG skip NULLs
	check("AE2 SUM skip NULL", "SELECT SUM(val) FROM v73_nullagg", 30)
	check("AE2 AVG skip NULL", "SELECT AVG(val) FROM v73_nullagg", 15)

	// AE3: MIN/MAX skip NULLs
	check("AE3 MIN skip NULL", "SELECT MIN(val) FROM v73_nullagg", 10)
	check("AE3 MAX skip NULL", "SELECT MAX(val) FROM v73_nullagg", 20)

	// AE4: COUNT DISTINCT
	afExec(t, db, ctx, `CREATE TABLE v73_cd (id INTEGER PRIMARY KEY, cat TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v73_cd VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v73_cd VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO v73_cd VALUES (3, 'a')")
	afExec(t, db, ctx, "INSERT INTO v73_cd VALUES (4, 'c')")
	afExec(t, db, ctx, "INSERT INTO v73_cd VALUES (5, 'b')")
	check("AE4 COUNT DISTINCT", "SELECT COUNT(DISTINCT cat) FROM v73_cd", 3)

	// ============================================================
	// === COMPLEX EXPRESSIONS ===
	// ============================================================

	// EX1: Arithmetic in WHERE
	checkRowCount("EX1 arith WHERE",
		"SELECT * FROM v73_emp WHERE salary / 1000 > 100", 3)
	// >100k: Alice(120k), Bob(110k), Frank(130k) = 3

	// EX2: Concatenation with number
	check("EX2 concat number",
		"SELECT name || ': $' || salary FROM v73_emp WHERE id = 1", "Alice: $120000")

	// EX3: COALESCE in expression
	check("EX3 COALESCE expr",
		"SELECT COALESCE(NULL, salary, 0) FROM v73_emp WHERE id = 1", 120000)

	// EX4: Complex nested expression
	check("EX4 nested expr",
		"SELECT ((salary - 50000) * 2 + 10000) / 1000 FROM v73_emp WHERE id = 1", 150)
	// ((120000-50000)*2+10000)/1000 = (140000+10000)/1000 = 150000/1000 = 150

	// ============================================================
	// === UNION PATTERNS ===
	// ============================================================

	// UP1: UNION removes duplicates
	check("UP1 UNION dedup",
		`SELECT COUNT(*) FROM (
			SELECT dept_id FROM v73_emp WHERE active = 1
			UNION
			SELECT dept_id FROM v73_emp WHERE active = 0
		) u`, 4)

	// UP2: UNION ALL keeps all
	check("UP2 UNION ALL",
		`SELECT COUNT(*) FROM (
			SELECT dept_id FROM v73_emp WHERE active = 1
			UNION ALL
			SELECT dept_id FROM v73_emp WHERE active = 0
		) u`, 10)

	// UP3: INTERSECT
	check("UP3 INTERSECT",
		`SELECT COUNT(*) FROM (
			SELECT dept_id FROM v73_emp WHERE active = 1
			INTERSECT
			SELECT dept_id FROM v73_emp WHERE active = 0
		) i`, 2)
	// Both active and inactive in: HR(3), Mkt(4) = 2

	// UP4: EXCEPT
	check("UP4 EXCEPT",
		`SELECT COUNT(*) FROM (
			SELECT dept_id FROM v73_emp WHERE active = 1
			EXCEPT
			SELECT dept_id FROM v73_emp WHERE active = 0
		) e`, 2)
	// Active-only depts: Eng(1), Sales(2) = 2

	// ============================================================
	// === WINDOW FUNCTION PATTERNS ===
	// ============================================================

	// WF1: ROW_NUMBER per department
	check("WF1 ROW_NUMBER partition",
		`WITH numbered AS (
			SELECT name, dept_id,
				   ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn
			FROM v73_emp WHERE active = 1
		)
		SELECT COUNT(*) FROM numbered WHERE rn = 1`, 4)
	// 4 departments with active employees

	// WF2: Running SUM - WHERE filters first, so only id=3 is in the window frame
	check("WF2 running SUM",
		`SELECT SUM(salary) OVER (ORDER BY id) FROM v73_emp WHERE id = 3`, 90000)
	// WHERE filters to single row (id=3, salary=90k), running SUM = 90k

	// WF3: RANK
	check("WF3 RANK top",
		`WITH ranked AS (
			SELECT name, RANK() OVER (ORDER BY salary DESC) as rk
			FROM v73_emp
		)
		SELECT name FROM ranked WHERE rk = 1`, "Frank")

	// ============================================================
	// === DERIVED TABLE PATTERNS ===
	// ============================================================

	// DT1: Derived table with aggregate
	check("DT1 derived agg",
		`SELECT MAX(cnt) FROM (
			SELECT dept_id, COUNT(*) as cnt FROM v73_emp GROUP BY dept_id
		) sub`, 3)

	// DT2: Derived table with JOIN
	check("DT2 derived JOIN",
		`SELECT d.name FROM (
			SELECT dept_id, SUM(salary) as total FROM v73_emp GROUP BY dept_id
		) sub
		JOIN v73_dept d ON sub.dept_id = d.id
		ORDER BY sub.total DESC LIMIT 1`, "Engineering")

	// ============================================================
	// === MIXED OPERATIONS ===
	// ============================================================

	// MO1: Conditional aggregation (pivot-like)
	check("MO1 conditional agg",
		`SELECT SUM(CASE WHEN dept_id = 1 THEN salary ELSE 0 END) FROM v73_emp`, 360000)

	// MO2: COALESCE in aggregate
	check("MO2 COALESCE agg",
		"SELECT SUM(COALESCE(val, 0)) FROM v73_nullagg", 30)

	// MO3: CASE in ORDER BY via CTE
	check("MO3 CASE ORDER",
		`WITH classified AS (
			SELECT name, CASE WHEN salary >= 100000 THEN 1 ELSE 2 END as priority
			FROM v73_emp
		)
		SELECT name FROM classified ORDER BY priority ASC, name ASC LIMIT 1`, "Alice")
	// Priority 1 (>=100k): Alice, Bob, Frank → Alice first alphabetically

	// MO4: Complex CTE + JOIN + Window
	check("MO4 CTE JOIN Window",
		`WITH dept_ranked AS (
			SELECT e.name, d.name as dept_name, e.salary,
				   ROW_NUMBER() OVER (PARTITION BY e.dept_id ORDER BY e.salary DESC) as rn
			FROM v73_emp e
			JOIN v73_dept d ON e.dept_id = d.id
			WHERE e.active = 1
		)
		SELECT name FROM dept_ranked WHERE rn = 1 ORDER BY salary DESC LIMIT 1`, "Frank")

	t.Logf("\n=== V73 SQL COMPLIANCE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
