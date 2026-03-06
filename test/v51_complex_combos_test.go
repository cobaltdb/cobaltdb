package test

import (
	"fmt"
	"testing"
)

// TestV51ComplexCombos exercises complex query combinations: self-JOINs,
// views with JOINs, chained CTEs, correlated subqueries, multi-level
// aggregation, and other patterns likely to uncover remaining bugs.
func TestV51ComplexCombos(t *testing.T) {
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

	// ============================================================
	// Setup: Organization database
	// ============================================================
	afExec(t, db, ctx, `CREATE TABLE v51_dept (
		id INTEGER PRIMARY KEY, name TEXT, budget INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v51_dept VALUES (1, 'Engineering', 1000000)")
	afExec(t, db, ctx, "INSERT INTO v51_dept VALUES (2, 'Marketing', 500000)")
	afExec(t, db, ctx, "INSERT INTO v51_dept VALUES (3, 'Finance', 300000)")

	afExec(t, db, ctx, `CREATE TABLE v51_emp (
		id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER,
		manager_id INTEGER, hire_year INTEGER,
		FOREIGN KEY (dept_id) REFERENCES v51_dept(id))`)
	afExec(t, db, ctx, "INSERT INTO v51_emp VALUES (1, 'Alice', 1, 150000, NULL, 2018)")
	afExec(t, db, ctx, "INSERT INTO v51_emp VALUES (2, 'Bob', 1, 120000, 1, 2019)")
	afExec(t, db, ctx, "INSERT INTO v51_emp VALUES (3, 'Carol', 1, 130000, 1, 2020)")
	afExec(t, db, ctx, "INSERT INTO v51_emp VALUES (4, 'Dave', 2, 100000, NULL, 2017)")
	afExec(t, db, ctx, "INSERT INTO v51_emp VALUES (5, 'Eve', 2, 95000, 4, 2021)")
	afExec(t, db, ctx, "INSERT INTO v51_emp VALUES (6, 'Frank', 3, 110000, NULL, 2016)")
	afExec(t, db, ctx, "INSERT INTO v51_emp VALUES (7, 'Grace', 3, 105000, 6, 2022)")
	afExec(t, db, ctx, "INSERT INTO v51_emp VALUES (8, 'Hank', 1, 140000, 1, 2019)")

	afExec(t, db, ctx, `CREATE TABLE v51_sales (
		id INTEGER PRIMARY KEY, emp_id INTEGER, amount INTEGER, quarter TEXT,
		FOREIGN KEY (emp_id) REFERENCES v51_emp(id))`)
	afExec(t, db, ctx, "INSERT INTO v51_sales VALUES (1, 4, 50000, 'Q1')")
	afExec(t, db, ctx, "INSERT INTO v51_sales VALUES (2, 4, 75000, 'Q2')")
	afExec(t, db, ctx, "INSERT INTO v51_sales VALUES (3, 5, 60000, 'Q1')")
	afExec(t, db, ctx, "INSERT INTO v51_sales VALUES (4, 5, 45000, 'Q2')")
	afExec(t, db, ctx, "INSERT INTO v51_sales VALUES (5, 4, 80000, 'Q3')")
	afExec(t, db, ctx, "INSERT INTO v51_sales VALUES (6, 5, 70000, 'Q3')")

	// ============================================================
	// === SELF-JOIN ===
	// ============================================================

	// SJ1: Find employees with their manager names
	check("SJ1 self-join manager",
		`SELECT e.name FROM v51_emp e
		 JOIN v51_emp m ON e.manager_id = m.id
		 WHERE m.name = 'Alice'
		 ORDER BY e.name LIMIT 1`,
		"Bob") // Alice manages Bob, Carol, Hank

	// SJ2: Count employees per manager
	check("SJ2 manager count",
		`SELECT m.name, COUNT(e.id) AS direct_reports
		 FROM v51_emp e
		 JOIN v51_emp m ON e.manager_id = m.id
		 GROUP BY m.name
		 ORDER BY direct_reports DESC LIMIT 1`,
		"Alice") // Alice manages 3 (Bob, Carol, Hank)

	// SJ3: Self-join find employees earning more than manager
	checkRowCount("SJ3 earn more than manager",
		`SELECT e.name FROM v51_emp e
		 JOIN v51_emp m ON e.manager_id = m.id
		 WHERE e.salary > m.salary`,
		0) // No one earns more than their manager

	// SJ4: Employees without a manager (top-level)
	check("SJ4 no manager",
		`SELECT COUNT(*) FROM v51_emp WHERE manager_id IS NULL`,
		3) // Alice, Dave, Frank

	// ============================================================
	// === VIEWS WITH JOINS ===
	// ============================================================

	// VJ1: Create view with JOIN
	checkNoError("VJ1 CREATE VIEW JOIN",
		`CREATE VIEW v51_emp_dept AS
		 SELECT e.id, e.name, e.salary, d.name AS dept_name
		 FROM v51_emp e JOIN v51_dept d ON e.dept_id = d.id`)

	check("VJ1 query view",
		"SELECT dept_name FROM v51_emp_dept WHERE name = 'Alice'",
		"Engineering")

	// VJ2: Query view with WHERE
	check("VJ2 view WHERE",
		"SELECT COUNT(*) FROM v51_emp_dept WHERE dept_name = 'Engineering'",
		4) // Alice, Bob, Carol, Hank

	// VJ3: View with aggregate query
	check("VJ3 view aggregate",
		`SELECT dept_name, SUM(salary) AS total
		 FROM v51_emp_dept
		 GROUP BY dept_name
		 ORDER BY total DESC LIMIT 1`,
		"Engineering") // 150000+120000+130000+140000=540000

	// ============================================================
	// === CTE WITH ORDER BY / LIMIT ===
	// ============================================================

	// CL1: CTE with ORDER BY and LIMIT
	check("CL1 CTE ORDER LIMIT",
		`WITH top_earners AS (
		   SELECT name, salary FROM v51_emp ORDER BY salary DESC LIMIT 3
		 )
		 SELECT name FROM top_earners ORDER BY name LIMIT 1`,
		"Alice") // Top 3: Alice(150), Hank(140), Carol(130)

	// CL2: CTE result used in aggregate
	check("CL2 CTE aggregate",
		`WITH dept_totals AS (
		   SELECT dept_id, SUM(salary) AS total_salary
		   FROM v51_emp GROUP BY dept_id
		 )
		 SELECT MAX(total_salary) FROM dept_totals`,
		540000) // Engineering: 540000

	// CL3: CTE with DISTINCT in aggregate
	check("CL3 CTE DISTINCT aggregate",
		`WITH unique_depts AS (
		   SELECT DISTINCT dept_id FROM v51_emp
		 )
		 SELECT COUNT(*) FROM unique_depts`,
		3)

	// ============================================================
	// === CORRELATED SUBQUERIES ===
	// ============================================================

	// CS1: Correlated subquery in WHERE
	check("CS1 correlated WHERE",
		`SELECT name FROM v51_emp e
		 WHERE salary > (SELECT AVG(salary) FROM v51_emp WHERE dept_id = e.dept_id)
		 ORDER BY salary DESC LIMIT 1`,
		"Alice") // Alice > Eng avg, others comparable

	// CS2: Correlated subquery - employees who earn max in their dept
	checkRowCount("CS2 max in dept",
		`SELECT name FROM v51_emp e
		 WHERE salary = (SELECT MAX(salary) FROM v51_emp WHERE dept_id = e.dept_id)`,
		3) // Alice(Eng), Dave(Mkt), Frank(Fin)

	// CS3: Correlated EXISTS
	check("CS3 correlated EXISTS",
		`SELECT COUNT(*) FROM v51_dept d
		 WHERE EXISTS (
		   SELECT 1 FROM v51_emp WHERE dept_id = d.id AND salary > 125000
		 )`,
		1) // Only Engineering has employees > 125000

	// CS4: NOT EXISTS - depts where no one earns under 100000
	check("CS4 NOT EXISTS",
		`SELECT d.name FROM v51_dept d
		 WHERE NOT EXISTS (
		   SELECT 1 FROM v51_emp WHERE dept_id = d.id AND salary < 100000
		 )
		 ORDER BY d.name LIMIT 1`,
		"Engineering") // Eng: min=120000, Finance: min=105000, Marketing: Eve=95000

	// ============================================================
	// === INSERT...SELECT WITH EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v51_bonuses (
		emp_id INTEGER, name TEXT, bonus INTEGER)`)

	// IE1: INSERT...SELECT with expression
	checkNoError("IE1 INSERT SELECT expr",
		`INSERT INTO v51_bonuses
		 SELECT id, name, salary / 10 FROM v51_emp WHERE dept_id = 1`)

	check("IE1 verify count", "SELECT COUNT(*) FROM v51_bonuses", 4)
	check("IE1 verify data",
		"SELECT bonus FROM v51_bonuses WHERE name = 'Alice'", 15000) // 150000/10

	// IE2: INSERT...SELECT with JOIN
	afExec(t, db, ctx, `CREATE TABLE v51_dept_summary (
		dept TEXT, emp_count INTEGER, total_salary INTEGER)`)

	checkNoError("IE2 INSERT SELECT JOIN",
		`INSERT INTO v51_dept_summary
		 SELECT d.name, COUNT(e.id), SUM(e.salary)
		 FROM v51_dept d JOIN v51_emp e ON d.id = e.dept_id
		 GROUP BY d.name`)

	check("IE2 verify", "SELECT total_salary FROM v51_dept_summary WHERE dept = 'Engineering'",
		540000)

	// ============================================================
	// === CHAINED CTEs WITH AGGREGATES ===
	// ============================================================

	// CH1: Three-level CTE chain
	check("CH1 three-level CTE",
		`WITH dept_stats AS (
		   SELECT dept_id, COUNT(*) AS emp_count, AVG(salary) AS avg_sal
		   FROM v51_emp GROUP BY dept_id
		 ),
		 dept_names AS (
		   SELECT d.name, ds.emp_count, ds.avg_sal
		   FROM v51_dept d JOIN dept_stats ds ON d.id = ds.dept_id
		 ),
		 top_dept AS (
		   SELECT name FROM dept_names ORDER BY avg_sal DESC LIMIT 1
		 )
		 SELECT name FROM top_dept`,
		"Engineering") // Eng avg: 135000

	// CH2: CTE with UNION and aggregate
	check("CH2 CTE UNION aggregate",
		`WITH all_amounts AS (
		   SELECT salary AS amount FROM v51_emp
		   UNION ALL
		   SELECT amount FROM v51_sales
		 )
		 SELECT SUM(amount) FROM all_amounts`,
		"1.33e+06") // Total salaries + total sales = 1330000 (formatted as scientific notation)

	// ============================================================
	// === MULTIPLE WHERE SUBQUERIES ===
	// ============================================================

	// MW1: Multiple IN subqueries
	check("MW1 multiple IN",
		`SELECT COUNT(*) FROM v51_emp
		 WHERE dept_id IN (SELECT id FROM v51_dept WHERE budget > 400000)
		   AND salary > 100000`,
		4) // Eng(budget 1M) + Mkt(budget 500K) = depts 1,2; salary>100K: Alice,Bob?,Carol,Hank,Dave?

	// MW2: Subquery AND expression
	check("MW2 subquery AND expr",
		`SELECT name FROM v51_emp
		 WHERE salary > (SELECT MIN(salary) FROM v51_emp) + 50000
		 ORDER BY name LIMIT 1`,
		"Alice") // min=95000, threshold=145000, Alice=150000

	// ============================================================
	// === AGGREGATE IN DIFFERENT POSITIONS ===
	// ============================================================

	// AP1: Aggregate in CASE
	check("AP1 aggregate in CASE",
		`SELECT CASE WHEN COUNT(*) > 5 THEN 'large' ELSE 'small' END
		 FROM v51_emp`,
		"large") // 8 employees

	// AP2: Multiple aggregates in SELECT
	check("AP2 multiple aggregates",
		`SELECT COUNT(*) FROM v51_emp
		 WHERE salary BETWEEN
		   (SELECT MIN(salary) FROM v51_emp) AND
		   (SELECT AVG(salary) FROM v51_emp)`,
		4) // min=95000, avg=118750, between: Eve(95K), Dave(100K), Grace(105K), Frank(110K)

	// AP3: Aggregate with DISTINCT
	check("AP3 COUNT DISTINCT",
		`SELECT COUNT(DISTINCT dept_id) FROM v51_emp`,
		3)

	// AP4: GROUP_CONCAT (or equivalent)
	check("AP4 GROUP BY with MIN/MAX",
		`SELECT MAX(name) FROM v51_emp WHERE dept_id = 1`,
		"Hank") // Alphabetically last in Eng: Hank

	// ============================================================
	// === DERIVED TABLE COMBINATIONS ===
	// ============================================================

	// DT1: Derived table with WHERE
	check("DT1 derived table WHERE",
		`SELECT COUNT(*) FROM (
		   SELECT * FROM v51_emp WHERE salary > 110000
		 ) AS high_earners`,
		4) // Alice(150), Bob(120), Carol(130), Hank(140)

	// DT2: Derived table in JOIN
	check("DT2 derived table JOIN",
		`SELECT d.name FROM v51_dept d
		 JOIN (SELECT dept_id, AVG(salary) AS avg_sal FROM v51_emp GROUP BY dept_id) AS ds
		   ON d.id = ds.dept_id
		 ORDER BY ds.avg_sal DESC LIMIT 1`,
		"Engineering")

	// DT3: Nested derived tables
	check("DT3 nested derived",
		`SELECT max_avg FROM (
		   SELECT MAX(avg_sal) AS max_avg FROM (
		     SELECT dept_id, AVG(salary) AS avg_sal FROM v51_emp GROUP BY dept_id
		   ) AS dept_avgs
		 ) AS result`,
		135000) // Eng avg: (150+120+130+140)/4=135000

	// ============================================================
	// === WINDOW FUNCTIONS IN DERIVED TABLES WITH JOINS ===
	// ============================================================

	// WD1: Window function in derived table, joined to real table
	check("WD1 window derived JOIN",
		`SELECT d.name FROM v51_dept d
		 JOIN (
		   SELECT dept_id, name,
		          ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rn
		   FROM v51_emp
		 ) AS ranked ON d.id = ranked.dept_id
		 WHERE ranked.rn = 1 AND d.name = 'Engineering'`,
		"Engineering")

	// WD2: Verify window function in derived table gives correct ranking
	check("WD2 window correct ranking",
		`SELECT name FROM (
		   SELECT name, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rn
		   FROM v51_emp
		 ) AS ranked WHERE rn = 1 ORDER BY name LIMIT 1`,
		"Alice") // Top per dept: Alice(Eng), Dave(Mkt), Frank(Fin)

	// ============================================================
	// === COMPLEX UPDATE/DELETE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v51_emp_copy (
		id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER,
		manager_id INTEGER, hire_year INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v51_emp_copy SELECT * FROM v51_emp")

	// CU1: UPDATE with correlated subquery in WHERE
	checkNoError("CU1 UPDATE correlated",
		`UPDATE v51_emp_copy SET salary = salary * 1.1
		 WHERE salary < (SELECT AVG(salary) FROM v51_emp)`)

	// Those with salary < 118750 (avg): Dave(100K), Eve(95K), Grace(105K), Frank(110K)
	// Use CAST to avoid float precision issues from * 1.1
	check("CU1 verify Dave",
		"SELECT CAST(salary AS INTEGER) FROM v51_emp_copy WHERE name = 'Dave'", 110000) // 100000*1.1

	check("CU1 verify Alice unchanged",
		"SELECT salary FROM v51_emp_copy WHERE name = 'Alice'", 150000)

	// CU2: DELETE with subquery
	checkNoError("CU2 DELETE subquery",
		`DELETE FROM v51_emp_copy
		 WHERE dept_id = (SELECT id FROM v51_dept WHERE name = 'Finance')`)

	check("CU2 verify",
		"SELECT COUNT(*) FROM v51_emp_copy", 6) // 8 - 2 (Finance)

	// ============================================================
	// === SAVEPOINT WITH COMPLEX QUERIES ===
	// ============================================================

	// SP1: Savepoint with CTE and aggregates
	checkNoError("SP1 BEGIN", "BEGIN")
	checkNoError("SP1 SAVEPOINT", "SAVEPOINT sp_test")

	checkNoError("SP1 complex update",
		`UPDATE v51_emp SET salary = salary + 10000 WHERE dept_id = 1`)

	check("SP1 verify update",
		"SELECT SUM(salary) FROM v51_emp WHERE dept_id = 1", 580000) // 540000 + 40000

	checkNoError("SP1 ROLLBACK TO", "ROLLBACK TO SAVEPOINT sp_test")

	check("SP1 verify rollback",
		"SELECT SUM(salary) FROM v51_emp WHERE dept_id = 1", 540000)

	checkNoError("SP1 COMMIT", "COMMIT")

	// ============================================================
	// === EDGE CASES ===
	// ============================================================

	// EC1: Empty table operations
	afExec(t, db, ctx, "CREATE TABLE v51_empty (id INTEGER PRIMARY KEY, val TEXT)")

	check("EC1 COUNT empty", "SELECT COUNT(*) FROM v51_empty", 0)
	check("EC1 SUM empty", "SELECT COALESCE(SUM(id), 0) FROM v51_empty", 0)

	// EC2: Single row table
	afExec(t, db, ctx, "CREATE TABLE v51_single (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v51_single VALUES (1, 42)")

	check("EC2 AVG single", "SELECT AVG(val) FROM v51_single", 42)
	check("EC2 MAX single", "SELECT MAX(val) FROM v51_single", 42)
	check("EC2 GROUP BY single",
		"SELECT COUNT(*) FROM v51_single GROUP BY val", 1)

	// EC3: Large IN list
	check("EC3 large IN",
		`SELECT COUNT(*) FROM v51_emp WHERE id IN (1,2,3,4,5,6,7,8,9,10)`,
		8)

	// EC4: Deeply nested expressions
	check("EC4 nested expressions",
		`SELECT ((salary + 1000) * 2 - 500) / 3 FROM v51_emp WHERE id = 1`,
		100500) // ((150000+1000)*2-500)/3 = (302000-500)/3 = 301500/3 = 100500

	t.Logf("\n=== V51 COMPLEX COMBOS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
