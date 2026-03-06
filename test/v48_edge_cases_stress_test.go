package test

import (
	"fmt"
	"testing"
)

// TestV48EdgeCasesStress exercises edge cases across complex SQL patterns:
// HAVING, CASE, CAST, BETWEEN, IN/EXISTS subqueries, UNION, INSERT INTO SELECT,
// NULL handling, COALESCE/NULLIF, window functions with non-SELECT columns, etc.
func TestV48EdgeCasesStress(t *testing.T) {
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

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got nil", desc)
			return
		}
		pass++
	}

	// ============================================================
	// Setup
	// ============================================================
	afExec(t, db, ctx, `CREATE TABLE v48_employees (
		id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER, manager_id INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v48_employees VALUES (1, 'Alice', 'Eng', 120000, NULL)")
	afExec(t, db, ctx, "INSERT INTO v48_employees VALUES (2, 'Bob', 'Eng', 100000, 1)")
	afExec(t, db, ctx, "INSERT INTO v48_employees VALUES (3, 'Carol', 'Eng', 110000, 1)")
	afExec(t, db, ctx, "INSERT INTO v48_employees VALUES (4, 'Dave', 'Sales', 90000, NULL)")
	afExec(t, db, ctx, "INSERT INTO v48_employees VALUES (5, 'Eve', 'Sales', 95000, 4)")
	afExec(t, db, ctx, "INSERT INTO v48_employees VALUES (6, 'Frank', 'HR', 85000, NULL)")
	afExec(t, db, ctx, "INSERT INTO v48_employees VALUES (7, 'Grace', 'HR', 80000, 6)")

	afExec(t, db, ctx, `CREATE TABLE v48_projects (
		id INTEGER PRIMARY KEY, name TEXT, dept TEXT, budget INTEGER, status TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v48_projects VALUES (1, 'Alpha', 'Eng', 500000, 'active')")
	afExec(t, db, ctx, "INSERT INTO v48_projects VALUES (2, 'Beta', 'Eng', 300000, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v48_projects VALUES (3, 'Gamma', 'Sales', 200000, 'active')")
	afExec(t, db, ctx, "INSERT INTO v48_projects VALUES (4, 'Delta', 'HR', 100000, 'cancelled')")
	afExec(t, db, ctx, "INSERT INTO v48_projects VALUES (5, 'Epsilon', 'Eng', 400000, 'active')")

	afExec(t, db, ctx, `CREATE TABLE v48_assignments (
		employee_id INTEGER, project_id INTEGER, hours INTEGER,
		FOREIGN KEY (employee_id) REFERENCES v48_employees(id),
		FOREIGN KEY (project_id) REFERENCES v48_projects(id))`)
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (1, 5, 60)")
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (2, 1, 150)")
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (2, 2, 200)")
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (3, 1, 80)")
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (3, 5, 120)")
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (4, 3, 100)")
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (5, 3, 90)")
	afExec(t, db, ctx, "INSERT INTO v48_assignments VALUES (6, 4, 50)")

	// Table for NULL tests
	afExec(t, db, ctx, `CREATE TABLE v48_nulls (
		id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)`)
	afExec(t, db, ctx, "INSERT INTO v48_nulls VALUES (1, 'hello', 10, 1.5)")
	afExec(t, db, ctx, "INSERT INTO v48_nulls VALUES (2, NULL, 20, NULL)")
	afExec(t, db, ctx, "INSERT INTO v48_nulls VALUES (3, 'world', NULL, 3.0)")
	afExec(t, db, ctx, "INSERT INTO v48_nulls VALUES (4, NULL, NULL, NULL)")

	// ============================================================
	// === HAVING CLAUSE TESTS ===
	// ============================================================

	// H1: HAVING with COUNT
	check("H1 HAVING COUNT",
		`SELECT dept FROM v48_employees GROUP BY dept HAVING COUNT(*) >= 3`,
		"Eng") // Eng has 3

	// H2: HAVING with SUM
	check("H2 HAVING SUM",
		`SELECT dept, SUM(salary) AS total FROM v48_employees
		 GROUP BY dept HAVING SUM(salary) > 200000 ORDER BY total DESC LIMIT 1`,
		"Eng") // Eng: 330000

	// H3: HAVING with multiple conditions
	checkRowCount("H3 HAVING multiple conditions",
		`SELECT dept FROM v48_employees
		 GROUP BY dept HAVING COUNT(*) >= 2 AND AVG(salary) > 85000`,
		2) // Eng(avg=110000) and Sales(avg=92500)

	// H4: HAVING without GROUP BY (aggregate on whole table)
	check("H4 HAVING no GROUP BY",
		`SELECT COUNT(*) FROM v48_employees HAVING COUNT(*) > 5`,
		7)

	// H5: HAVING with expression
	check("H5 HAVING expression",
		`SELECT dept FROM v48_employees
		 GROUP BY dept HAVING MAX(salary) - MIN(salary) > 15000 LIMIT 1`,
		"Eng") // 120000-100000=20000

	// ============================================================
	// === CASE WHEN TESTS ===
	// ============================================================

	// C1: Simple CASE
	check("C1 simple CASE",
		`SELECT CASE dept WHEN 'Eng' THEN 'Engineering'
		        WHEN 'Sales' THEN 'Sales Dept'
		        ELSE 'Other' END
		 FROM v48_employees WHERE id = 1`,
		"Engineering")

	// C2: Searched CASE
	check("C2 searched CASE",
		`SELECT CASE WHEN salary > 110000 THEN 'Senior'
		        WHEN salary > 90000 THEN 'Mid'
		        ELSE 'Junior' END
		 FROM v48_employees WHERE id = 2`,
		"Mid") // Bob: 100000

	// C3: CASE in aggregation
	check("C3 CASE in aggregate",
		`SELECT COUNT(CASE WHEN salary > 100000 THEN 1 END)
		 FROM v48_employees`,
		2) // Alice(120000), Carol(110000)

	// C4: CASE in GROUP BY
	check("C4 CASE in GROUP BY",
		`SELECT CASE WHEN salary >= 100000 THEN 'high' ELSE 'low' END AS tier,
		        COUNT(*) AS cnt
		 FROM v48_employees
		 GROUP BY CASE WHEN salary >= 100000 THEN 'high' ELSE 'low' END
		 ORDER BY cnt DESC LIMIT 1`,
		"low") // low: 4 (Dave, Eve, Frank, Grace)

	// C5: Nested CASE
	check("C5 nested CASE",
		`SELECT CASE WHEN dept = 'Eng'
		        THEN CASE WHEN salary > 110000 THEN 'Lead' ELSE 'Dev' END
		        ELSE 'Non-Eng' END
		 FROM v48_employees WHERE id = 3`,
		"Dev") // Carol: Eng, 110000

	// ============================================================
	// === CAST TESTS ===
	// ============================================================

	// CA1: CAST integer to text
	check("CA1 CAST int to text",
		`SELECT CAST(salary AS TEXT) FROM v48_employees WHERE id = 1`,
		"120000")

	// CA2: CAST text to integer
	check("CA2 CAST text to int",
		`SELECT CAST('42' AS INTEGER)`,
		42)

	// CA3: CAST in expression
	check("CA3 CAST in expression",
		`SELECT CAST(salary AS REAL) / 12 FROM v48_employees WHERE id = 6`,
		float64(85000)/12.0)

	// ============================================================
	// === BETWEEN TESTS ===
	// ============================================================

	// B1: BETWEEN basic
	checkRowCount("B1 BETWEEN basic",
		`SELECT * FROM v48_employees WHERE salary BETWEEN 90000 AND 110000`,
		4) // Bob(100000), Carol(110000), Dave(90000), Eve(95000)

	// B2: NOT BETWEEN
	checkRowCount("B2 NOT BETWEEN",
		`SELECT * FROM v48_employees WHERE salary NOT BETWEEN 90000 AND 110000`,
		3) // Alice(120000), Frank(85000), Grace(80000)

	// B3: BETWEEN with expression
	check("B3 BETWEEN with expression",
		`SELECT COUNT(*) FROM v48_projects WHERE budget BETWEEN 200000 AND 400000`,
		3) // Beta(300000), Gamma(200000), Epsilon(400000)

	// ============================================================
	// === IN / EXISTS SUBQUERY TESTS ===
	// ============================================================

	// IN1: IN with list
	checkRowCount("IN1 IN list",
		`SELECT * FROM v48_employees WHERE dept IN ('Eng', 'HR')`,
		5) // 3 Eng + 2 HR

	// IN2: IN with subquery
	check("IN2 IN subquery",
		`SELECT COUNT(*) FROM v48_employees
		 WHERE id IN (SELECT employee_id FROM v48_assignments WHERE hours > 100)`,
		2) // Bob(150,200), Carol(120) - Alice has exactly 100, not > 100

	// IN3: NOT IN with subquery
	check("IN3 NOT IN subquery",
		`SELECT COUNT(*) FROM v48_employees
		 WHERE id NOT IN (SELECT employee_id FROM v48_assignments)`,
		1) // Grace(7) has no assignments

	// EX1: EXISTS basic
	check("EX1 EXISTS basic",
		`SELECT COUNT(*) FROM v48_employees e
		 WHERE EXISTS (SELECT 1 FROM v48_assignments a WHERE a.employee_id = e.id)`,
		6) // All except Grace

	// EX2: NOT EXISTS
	check("EX2 NOT EXISTS",
		`SELECT name FROM v48_employees e
		 WHERE NOT EXISTS (SELECT 1 FROM v48_assignments a WHERE a.employee_id = e.id)`,
		"Grace")

	// EX3: EXISTS with complex subquery
	check("EX3 EXISTS complex",
		`SELECT COUNT(*) FROM v48_employees e
		 WHERE EXISTS (
		   SELECT 1 FROM v48_assignments a
		   JOIN v48_projects p ON a.project_id = p.id
		   WHERE a.employee_id = e.id AND p.status = 'active'
		 )`,
		5) // All assigned to active projects

	// ============================================================
	// === NULL HANDLING TESTS ===
	// ============================================================

	// N1: IS NULL
	check("N1 IS NULL count",
		`SELECT COUNT(*) FROM v48_nulls WHERE a IS NULL`,
		2) // ids 2 and 4

	// N2: IS NOT NULL
	check("N2 IS NOT NULL count",
		`SELECT COUNT(*) FROM v48_nulls WHERE b IS NOT NULL`,
		2) // ids 1 and 2

	// N3: COALESCE basic
	check("N3 COALESCE",
		`SELECT COALESCE(a, 'default') FROM v48_nulls WHERE id = 2`,
		"default")

	// N4: COALESCE chain
	check("N4 COALESCE chain",
		`SELECT COALESCE(a, CAST(b AS TEXT), 'none') FROM v48_nulls WHERE id = 4`,
		"none") // a=NULL, b=NULL

	// N5: NULLIF
	check("N5 NULLIF returns NULL",
		`SELECT COALESCE(NULLIF(10, 10), -1)`,
		-1) // NULLIF(10,10) = NULL, COALESCE(NULL, -1) = -1

	// N6: NULLIF returns first
	check("N6 NULLIF returns first",
		`SELECT NULLIF(10, 20)`,
		10) // 10 != 20, return 10

	// N7: IFNULL
	check("N7 IFNULL",
		`SELECT IFNULL(b, 0) FROM v48_nulls WHERE id = 3`,
		0) // b is NULL, return 0

	// N8: NULL in aggregate
	check("N8 NULL in SUM",
		`SELECT SUM(b) FROM v48_nulls`,
		30) // 10 + 20, NULLs ignored

	// N9: COUNT(*) vs COUNT(col) with NULLs
	check("N9 COUNT col vs star",
		`SELECT COUNT(b) FROM v48_nulls`,
		2) // Only non-NULL values: 10, 20

	// N10: NULL in BETWEEN
	check("N10 NULL in BETWEEN",
		`SELECT COUNT(*) FROM v48_nulls WHERE b BETWEEN 5 AND 25`,
		2) // 10 and 20

	// ============================================================
	// === UNION / SET OPERATIONS ===
	// ============================================================

	// U1: UNION removes duplicates (use CTE since derived UNION not supported yet)
	check("U1 UNION dedup",
		`WITH all_depts AS (
		   SELECT dept FROM v48_employees
		   UNION
		   SELECT dept FROM v48_projects
		 )
		 SELECT COUNT(*) FROM all_depts`,
		3) // Eng, Sales, HR

	// U2: UNION ALL keeps duplicates
	check("U2 UNION ALL",
		`WITH all_depts AS (
		   SELECT dept FROM v48_employees
		   UNION ALL
		   SELECT dept FROM v48_projects
		 )
		 SELECT COUNT(*) FROM all_depts`,
		12) // 7 employees + 5 projects

	// U3: UNION with different column names
	check("U3 UNION different cols",
		`WITH combined AS (
		   SELECT name FROM v48_employees WHERE salary > 110000
		   UNION
		   SELECT name FROM v48_projects WHERE budget > 400000
		 )
		 SELECT name FROM combined ORDER BY name LIMIT 1`,
		"Alice")

	// ============================================================
	// === INSERT INTO...SELECT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v48_high_earners (
		id INTEGER PRIMARY KEY, name TEXT, salary INTEGER)`)

	// IS1: INSERT INTO...SELECT
	checkNoError("IS1 INSERT SELECT",
		`INSERT INTO v48_high_earners SELECT id, name, salary FROM v48_employees WHERE salary > 100000`)

	check("IS1 verify count",
		`SELECT COUNT(*) FROM v48_high_earners`,
		2) // Alice(120000), Carol(110000)

	check("IS1 verify data",
		`SELECT name FROM v48_high_earners ORDER BY salary DESC LIMIT 1`,
		"Alice")

	// ============================================================
	// === WINDOW FUNCTIONS ADVANCED ===
	// ============================================================

	// W1: ROW_NUMBER with PARTITION BY on non-SELECT column
	check("W1 ROW_NUMBER PARTITION BY",
		`SELECT name FROM (
		   SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
		   FROM v48_employees
		 ) AS ranked WHERE rn = 1 ORDER BY name LIMIT 1`,
		"Alice") // Top earner per dept: Alice(Eng), Dave? No Eve? (Sales), Frank(HR)

	// W2: RANK with ties (use derived table so window runs on full dataset)
	afExec(t, db, ctx, `CREATE TABLE v48_scores (id INTEGER PRIMARY KEY, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v48_scores VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v48_scores VALUES (2, 90)")
	afExec(t, db, ctx, "INSERT INTO v48_scores VALUES (3, 100)")
	afExec(t, db, ctx, "INSERT INTO v48_scores VALUES (4, 80)")

	check("W2 RANK ties",
		`SELECT rk FROM (
		   SELECT id, RANK() OVER (ORDER BY score DESC) AS rk FROM v48_scores
		 ) AS ranked WHERE id = 2`,
		3) // Two 100s get rank 1,1 then 90 gets rank 3

	// W3: DENSE_RANK
	check("W3 DENSE_RANK",
		`SELECT drk FROM (
		   SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS drk FROM v48_scores
		 ) AS ranked WHERE id = 2`,
		2) // Two 100s get rank 1,1 then 90 gets dense_rank 2

	// W4: Window function SUM (use derived table so window runs on full dataset)
	check("W4 window SUM",
		`SELECT dept_total FROM (
		   SELECT id, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM v48_employees
		 ) AS windowed WHERE id = 1`,
		330000) // Eng total: 120000+100000+110000

	// W5: LAG function - first row has NULL LAG, check second row via derived table
	check("W5 LAG",
		`SELECT prev_name FROM (
		   SELECT name, LAG(name, 1) OVER (ORDER BY salary DESC) AS prev_name,
		          ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
		   FROM v48_employees
		 ) AS lagged WHERE rn = 2`,
		"Alice") // Second highest salary's LAG is Alice (highest)

	// ============================================================
	// === LIKE PATTERN MATCHING ===
	// ============================================================

	// L1: LIKE basic
	checkRowCount("L1 LIKE basic",
		`SELECT * FROM v48_employees WHERE name LIKE 'A%'`,
		1) // Alice

	// L2: LIKE wildcard (case-insensitive: Alice, Carol, Dave, Frank, Grace)
	checkRowCount("L2 LIKE wildcard",
		`SELECT * FROM v48_employees WHERE name LIKE '%a%'`,
		5)

	// L3: NOT LIKE
	checkRowCount("L3 NOT LIKE",
		`SELECT * FROM v48_employees WHERE name NOT LIKE 'A%'`,
		6)

	// L4: LIKE single char
	checkRowCount("L4 LIKE single char",
		`SELECT * FROM v48_employees WHERE name LIKE '___'`,
		2) // Bob, Eve (3 chars)

	// ============================================================
	// === SUBQUERY IN SELECT ===
	// ============================================================

	// SS1: Scalar subquery in SELECT
	check("SS1 scalar subquery",
		`SELECT name, (SELECT COUNT(*) FROM v48_assignments a WHERE a.employee_id = e.id) AS assignment_count
		 FROM v48_employees e WHERE id = 1`,
		"Alice")

	// SS2: Subquery in WHERE with aggregate
	check("SS2 subquery WHERE aggregate",
		`SELECT name FROM v48_employees
		 WHERE salary > (SELECT AVG(salary) FROM v48_employees)
		 ORDER BY salary DESC LIMIT 1`,
		"Alice") // avg=97143, Alice=120000

	// ============================================================
	// === COMPLEX COMBINATIONS ===
	// ============================================================

	// CC1: CTE with HAVING
	check("CC1 CTE HAVING",
		`WITH dept_stats AS (
		   SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal
		   FROM v48_employees GROUP BY dept HAVING COUNT(*) >= 2
		 )
		 SELECT dept FROM dept_stats ORDER BY avg_sal DESC LIMIT 1`,
		"Eng") // Eng avg=110000

	// CC2: Derived table with window + CASE
	check("CC2 derived window CASE",
		`SELECT category FROM (
		   SELECT name,
		          CASE WHEN salary >= 100000 THEN 'high' ELSE 'low' END AS category,
		          ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
		   FROM v48_employees
		 ) AS ranked WHERE rn = 1`,
		"high") // Alice: top salary, category=high

	// CC3: Multi-level aggregation
	check("CC3 multi-level aggregate",
		`SELECT MAX(total_hours) FROM (
		   SELECT employee_id, SUM(hours) AS total_hours
		   FROM v48_assignments
		   GROUP BY employee_id
		 ) AS emp_hours`,
		350) // Bob: 150+200=350

	// CC4: JOIN with CASE and aggregate
	check("CC4 JOIN CASE aggregate",
		`SELECT SUM(CASE WHEN p.status = 'active' THEN a.hours ELSE 0 END) AS active_hours
		 FROM v48_assignments a
		 JOIN v48_projects p ON a.project_id = p.id`,
		700) // Alpha(100+150+80)+Gamma(100+90)+Epsilon(60+120)=700

	// CC5: Correlated subquery with aggregate in HAVING
	checkRowCount("CC5 correlated HAVING",
		`SELECT dept FROM v48_employees e
		 GROUP BY dept
		 HAVING SUM(salary) > (SELECT AVG(salary) * 2 FROM v48_employees)`,
		1) // Only Eng(330000) > 2*97143=194286

	// CC6: Complex ORDER BY with expressions
	check("CC6 ORDER BY expression",
		`SELECT name FROM v48_employees
		 ORDER BY salary * (CASE WHEN dept = 'Eng' THEN 1.5 ELSE 1.0 END) DESC
		 LIMIT 1`,
		"Alice") // Alice: 120000*1.5=180000

	// CC7: COALESCE in JOIN condition (via expression)
	check("CC7 COALESCE in query",
		`SELECT COUNT(*) FROM v48_employees
		 WHERE COALESCE(manager_id, 0) = 0`,
		3) // Alice, Dave, Frank have NULL manager_id

	// CC8: Multiple CTEs with aggregates and JOIN
	check("CC8 multi-CTE aggregate JOIN",
		`WITH active_projects AS (
		   SELECT id, name FROM v48_projects WHERE status = 'active'
		 ),
		 project_hours AS (
		   SELECT project_id, SUM(hours) AS total_hours
		   FROM v48_assignments GROUP BY project_id
		 )
		 SELECT ap.name FROM active_projects ap
		 JOIN project_hours ph ON ap.id = ph.project_id
		 ORDER BY ph.total_hours DESC LIMIT 1`,
		"Alpha") // Alpha: 100+150+80=330

	// ============================================================
	// === ERROR HANDLING / CONSTRAINTS ===
	// ============================================================

	// E1: UNIQUE constraint violation
	afExec(t, db, ctx, `CREATE TABLE v48_unique_test (id INTEGER PRIMARY KEY, code TEXT UNIQUE)`)
	checkNoError("E1 first insert", "INSERT INTO v48_unique_test VALUES (1, 'ABC')")
	checkError("E1 duplicate unique", "INSERT INTO v48_unique_test VALUES (2, 'ABC')")

	// E2: PRIMARY KEY duplicate
	checkError("E2 PK duplicate", "INSERT INTO v48_employees VALUES (1, 'Duplicate', 'Eng', 0, NULL)")

	// E3: NOT NULL constraint
	afExec(t, db, ctx, `CREATE TABLE v48_notnull_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)`)
	checkNoError("E3 valid insert", "INSERT INTO v48_notnull_test VALUES (1, 'ok')")
	checkError("E3 NULL violation", "INSERT INTO v48_notnull_test VALUES (2, NULL)")

	// E4: CHECK constraint
	afExec(t, db, ctx, `CREATE TABLE v48_check_test (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))`)
	checkNoError("E4 valid check", "INSERT INTO v48_check_test VALUES (1, 25)")
	checkError("E4 check violation", "INSERT INTO v48_check_test VALUES (2, -1)")

	// E5: Foreign key validation
	checkError("E5 FK violation", "INSERT INTO v48_assignments VALUES (99, 1, 10)")

	// ============================================================
	// === TRANSACTIONS AND DATA INTEGRITY ===
	// ============================================================

	// T1: Transaction isolation - uncommitted data
	checkNoError("T1 BEGIN", "BEGIN")
	checkNoError("T1 INSERT", "INSERT INTO v48_employees VALUES (8, 'Hank', 'Eng', 130000, 1)")
	check("T1 visible in txn", "SELECT name FROM v48_employees WHERE id = 8", "Hank")
	checkNoError("T1 ROLLBACK", "ROLLBACK")
	check("T1 rolled back", "SELECT COUNT(*) FROM v48_employees", 7) // Back to 7

	// T2: Savepoint with complex operations
	checkNoError("T2 BEGIN", "BEGIN")
	checkNoError("T2 SAVEPOINT", "SAVEPOINT sp1")
	checkNoError("T2 INSERT", "INSERT INTO v48_employees VALUES (8, 'Hank', 'Eng', 130000, 1)")
	checkNoError("T2 SAVEPOINT2", "SAVEPOINT sp2")
	checkNoError("T2 UPDATE", "UPDATE v48_employees SET salary = 999999 WHERE id = 1")
	check("T2 updated visible", "SELECT salary FROM v48_employees WHERE id = 1", 999999)
	checkNoError("T2 ROLLBACK TO sp2", "ROLLBACK TO SAVEPOINT sp2")
	check("T2 salary restored", "SELECT salary FROM v48_employees WHERE id = 1", 120000)
	check("T2 insert kept", "SELECT name FROM v48_employees WHERE id = 8", "Hank")
	checkNoError("T2 ROLLBACK TO sp1", "ROLLBACK TO SAVEPOINT sp1")
	check("T2 everything rolled back", "SELECT COUNT(*) FROM v48_employees", 7)
	checkNoError("T2 COMMIT", "COMMIT")

	t.Logf("\n=== V48 EDGE CASES STRESS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
