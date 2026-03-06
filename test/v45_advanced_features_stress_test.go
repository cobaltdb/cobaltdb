package test

import (
	"fmt"
	"testing"
)

// TestV45AdvancedFeaturesStress stress-tests new features:
// derived tables, multi-CTE, comma-FROM, positional GROUP BY/ORDER BY,
// plus exercises many SQL edge cases that might reveal bugs.
func TestV45AdvancedFeaturesStress(t *testing.T) {
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
	afExec(t, db, ctx, "CREATE TABLE v45_dept (id INTEGER PRIMARY KEY, name TEXT, budget INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v45_dept VALUES (1, 'Engineering', 500000)")
	afExec(t, db, ctx, "INSERT INTO v45_dept VALUES (2, 'Marketing', 200000)")
	afExec(t, db, ctx, "INSERT INTO v45_dept VALUES (3, 'Sales', 300000)")

	afExec(t, db, ctx, "CREATE TABLE v45_emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER, manager_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v45_emp VALUES (1, 'Alice', 1, 120000, NULL)")
	afExec(t, db, ctx, "INSERT INTO v45_emp VALUES (2, 'Bob', 1, 95000, 1)")
	afExec(t, db, ctx, "INSERT INTO v45_emp VALUES (3, 'Charlie', 2, 80000, NULL)")
	afExec(t, db, ctx, "INSERT INTO v45_emp VALUES (4, 'Diana', 2, 75000, 3)")
	afExec(t, db, ctx, "INSERT INTO v45_emp VALUES (5, 'Eve', 3, 90000, NULL)")
	afExec(t, db, ctx, "INSERT INTO v45_emp VALUES (6, 'Frank', 3, 70000, 5)")
	afExec(t, db, ctx, "INSERT INTO v45_emp VALUES (7, 'Grace', 1, 110000, 1)")

	afExec(t, db, ctx, "CREATE TABLE v45_proj (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, status TEXT)")
	afExec(t, db, ctx, "INSERT INTO v45_proj VALUES (1, 'Alpha', 1, 'active')")
	afExec(t, db, ctx, "INSERT INTO v45_proj VALUES (2, 'Beta', 1, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v45_proj VALUES (3, 'Gamma', 2, 'active')")
	afExec(t, db, ctx, "INSERT INTO v45_proj VALUES (4, 'Delta', 3, 'active')")
	afExec(t, db, ctx, "INSERT INTO v45_proj VALUES (5, 'Epsilon', 3, 'paused')")

	// ============================================================
	// === DERIVED TABLE STRESS ===
	// ============================================================

	// DS1: Derived table with JOIN
	checkRowCount("DS1 derived table JOIN",
		`SELECT high_earners.name FROM
		   (SELECT * FROM v45_emp WHERE salary > 100000) AS high_earners
		   JOIN v45_dept d ON high_earners.dept_id = d.id`,
		2) // Alice(120000), Grace(110000)

	// DS2: Derived table with aggregate and JOIN
	check("DS2 derived table aggregate JOIN",
		`SELECT dept_avg.dept_name, dept_avg.avg_sal
		 FROM (SELECT v45_dept.name AS dept_name, AVG(v45_emp.salary) AS avg_sal
		       FROM v45_emp JOIN v45_dept ON v45_emp.dept_id = v45_dept.id
		       GROUP BY v45_dept.name) AS dept_avg
		 ORDER BY avg_sal DESC LIMIT 1`,
		"Engineering") // (120000+95000+110000)/3 = 108333

	// DS3: Derived table with correlated-like WHERE
	check("DS3 derived table complex WHERE",
		`SELECT COUNT(*) FROM
		   (SELECT * FROM v45_emp WHERE salary >
		     (SELECT AVG(salary) FROM v45_emp)) AS above_avg`,
		3) // Avg=91428, above: Alice(120000), Grace(110000), Bob(95000)

	// DS4: Self-join with derived tables
	checkRowCount("DS4 derived table self-join pattern",
		`SELECT e.name, m.name
		 FROM (SELECT * FROM v45_emp WHERE manager_id IS NOT NULL) AS e
		 JOIN (SELECT * FROM v45_emp) AS m ON e.manager_id = m.id`,
		4) // Bob->Alice, Diana->Charlie, Frank->Eve, Grace->Alice

	// DS5: Derived table with UNION
	check("DS5 derived table with UNION source",
		`WITH combined AS (
		   SELECT name, salary FROM v45_emp WHERE dept_id = 1
		   UNION ALL
		   SELECT name, salary FROM v45_emp WHERE dept_id = 2
		 )
		 SELECT COUNT(*) FROM combined`,
		5) // 3 eng + 2 mkt

	// DS6: Derived table with DISTINCT
	checkRowCount("DS6 derived table DISTINCT",
		`SELECT * FROM (SELECT DISTINCT dept_id FROM v45_emp) AS depts`, 3)

	// DS7: Derived table with CASE expression
	check("DS7 derived table CASE",
		`SELECT tier FROM (
		   SELECT CASE WHEN salary >= 100000 THEN 'senior'
		               WHEN salary >= 80000 THEN 'mid'
		               ELSE 'junior' END AS tier,
		          COUNT(*) AS cnt
		   FROM v45_emp GROUP BY 1
		 ) AS tiers ORDER BY cnt DESC LIMIT 1`,
		"mid") // mid: Bob(95000), Charlie(80000), Eve(90000) = 3

	// DS8: Multiple derived tables in FROM (comma-separated)
	checkRowCount("DS8 two derived tables cross join",
		`SELECT * FROM
		   (SELECT DISTINCT dept_id FROM v45_emp) AS depts,
		   (SELECT DISTINCT status FROM v45_proj) AS statuses`,
		9) // 3 depts * 3 statuses

	// DS9: Derived table with ORDER BY and LIMIT
	check("DS9 derived table ORDER BY LIMIT",
		`SELECT name FROM (SELECT name, salary FROM v45_emp ORDER BY salary DESC LIMIT 3) AS top3
		 ORDER BY name LIMIT 1`,
		"Alice") // Top 3: Alice(120000), Grace(110000), Bob(95000)

	// DS10: Deeply nested derived tables
	check("DS10 3-level nested derived table",
		`SELECT result FROM (
		   SELECT total AS result FROM (
		     SELECT SUM(salary) AS total FROM (
		       SELECT * FROM v45_emp WHERE dept_id = 1
		     ) AS eng
		   ) AS inner_t
		 ) AS outer_t`,
		325000) // 120000+95000+110000

	// ============================================================
	// === MULTI-CTE STRESS ===
	// ============================================================

	// MC1: Three CTEs
	check("MC1 three CTEs",
		`WITH
		   eng AS (SELECT * FROM v45_emp WHERE dept_id = 1),
		   mkt AS (SELECT * FROM v45_emp WHERE dept_id = 2),
		   sales AS (SELECT * FROM v45_emp WHERE dept_id = 3)
		 SELECT COUNT(*) FROM eng`,
		3)

	// MC2: CTE referencing another CTE (chained)
	check("MC2 CTE chain",
		`WITH
		   all_emps AS (SELECT * FROM v45_emp),
		   high_earners AS (SELECT * FROM all_emps WHERE salary > 90000)
		 SELECT COUNT(*) FROM high_earners`,
		3) // Alice(120000), Bob(95000), Grace(110000)

	// MC3: CTE with aggregate used in main query filter
	check("MC3 CTE aggregate filter",
		`WITH dept_stats AS (
		   SELECT dept_id, AVG(salary) AS avg_sal, COUNT(*) AS cnt
		   FROM v45_emp GROUP BY dept_id
		 )
		 SELECT cnt FROM dept_stats ORDER BY avg_sal DESC LIMIT 1`,
		3) // Engineering has highest avg

	// MC4: CTE with UNION ALL cross-joined
	checkRowCount("MC4 CTE UNION cross join",
		`WITH letters AS (SELECT 'A' AS letter UNION ALL SELECT 'B' UNION ALL SELECT 'C'),
		      numbers AS (SELECT 1 AS num UNION ALL SELECT 2)
		 SELECT * FROM letters, numbers`, 6) // 3*2

	// MC5: CTE used in subquery
	check("MC5 CTE in subquery",
		`WITH dept_budget AS (SELECT id, budget FROM v45_dept)
		 SELECT name FROM v45_dept
		 WHERE budget = (SELECT MAX(budget) FROM dept_budget)`,
		"Engineering") // 500000

	// ============================================================
	// === COMMA-SEPARATED FROM (implicit CROSS JOIN) ===
	// ============================================================

	// CS1: Two real tables cross join with WHERE
	check("CS1 comma FROM with WHERE",
		`SELECT v45_emp.name FROM v45_emp, v45_dept
		 WHERE v45_emp.dept_id = v45_dept.id AND v45_dept.name = 'Sales'
		 ORDER BY v45_emp.name LIMIT 1`,
		"Eve")

	// CS2: Three tables comma-separated
	// Eng(3 emps, 2 proj)=6, Mkt(2 emps, 1 proj)=2, Sales(2 emps, 2 proj)=4 = 12
	check("CS2 three comma FROM",
		`SELECT COUNT(*) FROM v45_emp, v45_dept, v45_proj
		 WHERE v45_emp.dept_id = v45_dept.id AND v45_proj.dept_id = v45_dept.id`,
		12)

	// CS3: Comma FROM with alias
	check("CS3 comma FROM alias",
		`SELECT e.name FROM v45_emp e, v45_dept d
		 WHERE e.dept_id = d.id AND d.name = 'Marketing'
		 ORDER BY e.salary DESC LIMIT 1`,
		"Charlie") // 80000

	// CS4: Comma FROM full cartesian
	check("CS4 full cartesian product count",
		`SELECT COUNT(*) FROM v45_dept, v45_proj`, 15) // 3*5

	// ============================================================
	// === GROUP BY POSITIONAL STRESS ===
	// ============================================================

	// GP1: GROUP BY 1 with JOIN
	checkRowCount("GP1 GROUP BY 1 JOIN",
		`SELECT v45_dept.name, COUNT(*)
		 FROM v45_emp JOIN v45_dept ON v45_emp.dept_id = v45_dept.id
		 GROUP BY 1`, 3)

	// GP2: GROUP BY 1 with HAVING
	check("GP2 GROUP BY 1 HAVING",
		`SELECT v45_dept.name, COUNT(*) AS cnt
		 FROM v45_emp JOIN v45_dept ON v45_emp.dept_id = v45_dept.id
		 GROUP BY 1 HAVING COUNT(*) >= 3`,
		"Engineering") // Only Engineering has 3

	// GP3: GROUP BY 1,2 positional
	// (1,senior):2, (1,other):1, (2,other):2, (3,other):2 = 4 groups
	checkRowCount("GP3 GROUP BY 1,2",
		`SELECT dept_id, CASE WHEN salary >= 100000 THEN 'senior' ELSE 'other' END, COUNT(*)
		 FROM v45_emp GROUP BY 1, 2`, 4)

	// GP4: GROUP BY positional with expression result column
	check("GP4 GROUP BY 1 expression column",
		`SELECT CASE WHEN salary >= 100000 THEN 'high' ELSE 'normal' END AS level, COUNT(*)
		 FROM v45_emp GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1`,
		"normal") // 5 normal vs 2 high

	// GP5: ORDER BY 2 in GROUP BY context
	check("GP5 ORDER BY 2 GROUP BY context",
		`SELECT dept_id, SUM(salary) AS total
		 FROM v45_emp GROUP BY 1 ORDER BY 2 DESC LIMIT 1`,
		1) // Eng: 325000

	// GP6: ORDER BY 1 ASC, 2 DESC
	check("GP6 multi-positional ORDER BY",
		`SELECT dept_id, salary FROM v45_emp ORDER BY 1 ASC, 2 DESC LIMIT 1`,
		1) // dept_id=1, highest salary

	// ============================================================
	// === COMBINED FEATURES ===
	// ============================================================

	// CM1: CTE + derived table + positional GROUP BY
	check("CM1 CTE + derived + positional",
		`WITH data AS (SELECT * FROM v45_emp)
		 SELECT dept_id, total FROM (
		   SELECT dept_id, SUM(salary) AS total FROM data GROUP BY 1
		 ) AS summary ORDER BY 2 DESC LIMIT 1`,
		1) // Engineering

	// CM2: Derived table in CTE definition
	check("CM2 derived table in CTE",
		`WITH high_sal AS (
		   SELECT * FROM (SELECT * FROM v45_emp WHERE salary > 80000) AS filtered
		 )
		 SELECT COUNT(*) FROM high_sal`,
		4) // Alice(120000), Bob(95000), Eve(90000), Grace(110000) - Charlie(80000) is NOT > 80000

	// CM3: Multi-CTE with derived table join
	check("CM3 multi-CTE derived join",
		`WITH eng AS (SELECT * FROM v45_emp WHERE dept_id = 1),
		      eng_proj AS (SELECT * FROM v45_proj WHERE dept_id = 1)
		 SELECT COUNT(*) FROM eng, eng_proj`, 6) // 3*2

	// CM4: GROUP BY positional with comma-FROM
	checkRowCount("CM4 GROUP BY 1 comma FROM",
		`SELECT v45_dept.name, COUNT(*)
		 FROM v45_emp, v45_dept
		 WHERE v45_emp.dept_id = v45_dept.id
		 GROUP BY 1`, 3)

	// CM5: Subquery in SELECT with derived table in FROM
	check("CM5 subquery + derived table",
		`SELECT name, (SELECT COUNT(*) FROM v45_proj WHERE v45_proj.dept_id = t.dept_id) AS proj_count
		 FROM (SELECT * FROM v45_emp WHERE salary > 100000) AS t
		 ORDER BY proj_count DESC LIMIT 1`,
		"Alice") // Alice: dept 1, 2 projects

	// ============================================================
	// === EDGE CASES & ERROR HANDLING ===
	// ============================================================

	// EE1: Empty derived table
	checkRowCount("EE1 empty derived table",
		`SELECT * FROM (SELECT * FROM v45_emp WHERE salary > 1000000) AS none`, 0)

	// EE2: Derived table with NULL values
	check("EE2 derived table NULL handling",
		`SELECT COUNT(*) FROM (SELECT manager_id FROM v45_emp WHERE manager_id IS NULL) AS no_mgr`,
		3) // Alice, Charlie, Eve

	// EE3: ORDER BY on derived table column
	check("EE3 ORDER BY derived column",
		`SELECT name FROM (SELECT name, salary * 12 AS annual FROM v45_emp) AS ann
		 ORDER BY annual DESC LIMIT 1`,
		"Alice") // 120000*12

	// EE4: GROUP BY on derived table result
	check("EE4 GROUP BY derived result",
		`SELECT dept, total_sal FROM (
		   SELECT dept_id AS dept, SUM(salary) AS total_sal
		   FROM v45_emp GROUP BY dept_id
		 ) AS dept_totals ORDER BY total_sal DESC LIMIT 1`,
		1) // Engineering: 325000

	// EE5: CTE name collision with table name should use CTE
	check("EE5 CTE shadows table",
		`WITH v45_dept AS (SELECT 'SHADOW' AS name)
		 SELECT name FROM v45_dept`,
		"SHADOW") // CTE should shadow the real table

	// EE6: INSERT column count validation
	checkError("EE6 INSERT wrong column count",
		`INSERT INTO v45_dept VALUES (10, 'Test')`) // Missing budget

	// EE7: UPDATE non-existent column
	checkError("EE7 UPDATE bad column",
		`UPDATE v45_dept SET nonexistent = 5 WHERE id = 1`)

	// EE8: CREATE INDEX on bad column
	checkError("EE8 CREATE INDEX bad column",
		`CREATE INDEX idx_bad ON v45_dept(no_such_column)`)

	// EE9: PRIMARY KEY violation on UPDATE
	checkError("EE9 PK violation UPDATE",
		`UPDATE v45_dept SET id = 2 WHERE id = 1`)

	// EE10: UNIQUE constraint violation
	afExec(t, db, ctx, "CREATE TABLE v45_uniq (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
	afExec(t, db, ctx, "INSERT INTO v45_uniq VALUES (1, 'test@test.com')")
	checkError("EE10 UNIQUE violation",
		`INSERT INTO v45_uniq VALUES (2, 'test@test.com')`)

	// EE11: Foreign key violation - use table with FK constraint
	afExec(t, db, ctx, "CREATE TABLE v45_fk_test (id INTEGER PRIMARY KEY, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES v45_dept(id))")
	checkError("EE11 FK violation",
		`INSERT INTO v45_fk_test VALUES (1, 999)`)

	// ============================================================
	// === TRANSACTION + DERIVED TABLE ===
	// ============================================================

	// TX1: Transaction with derived table
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO v45_dept VALUES (4, 'Research', 400000)")
	check("TX1 derived table sees uncommitted",
		`SELECT COUNT(*) FROM (SELECT * FROM v45_dept) AS all_depts`, 4)
	afExec(t, db, ctx, "ROLLBACK")
	check("TX1b derived table after rollback",
		`SELECT COUNT(*) FROM (SELECT * FROM v45_dept) AS all_depts`, 3)

	// TX2: Transaction with CTE
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "UPDATE v45_emp SET salary = salary + 10000 WHERE dept_id = 1")
	check("TX2 CTE sees uncommitted update",
		`WITH eng AS (SELECT * FROM v45_emp WHERE dept_id = 1)
		 SELECT SUM(salary) FROM eng`, 355000) // 325000 + 30000
	afExec(t, db, ctx, "ROLLBACK")
	check("TX2b CTE after rollback",
		`WITH eng AS (SELECT * FROM v45_emp WHERE dept_id = 1)
		 SELECT SUM(salary) FROM eng`, 325000)

	// ============================================================
	// === WINDOW FUNCTIONS WITH NEW FEATURES ===
	// ============================================================

	// WF1: Window function in derived table
	check("WF1 window function derived table",
		`SELECT name FROM (
		   SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
		   FROM v45_emp
		 ) AS ranked WHERE rn = 1`,
		"Alice")

	// WF2: Window function with positional ORDER BY
	check("WF2 window + positional ORDER BY",
		`SELECT name, salary FROM v45_emp ORDER BY 2 DESC LIMIT 1`,
		"Alice") // 120000

	// ============================================================
	// === COMPLEX REAL-WORLD QUERIES ===
	// ============================================================

	// RW1: Department report with derived tables and CTEs
	check("RW1 department report",
		`WITH dept_summary AS (
		   SELECT v45_dept.name AS dept_name,
		          COUNT(*) AS emp_count,
		          SUM(v45_emp.salary) AS total_salary
		   FROM v45_emp JOIN v45_dept ON v45_emp.dept_id = v45_dept.id
		   GROUP BY v45_dept.name
		 )
		 SELECT dept_name FROM dept_summary
		 WHERE total_salary = (SELECT MAX(total_salary) FROM dept_summary)`,
		"Engineering")

	// RW2: Top earner per department using derived tables
	check("RW2 top earner per dept",
		`SELECT name FROM (
		   SELECT name, salary, dept_id,
		          ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rn
		   FROM v45_emp
		 ) AS ranked WHERE rn = 1 AND dept_id = 2`,
		"Charlie") // dept 2 top: Charlie(80000)

	// RW3: Projects per department with comma-FROM
	// Engineering:2 (Alpha,Beta), Sales:2 (Delta,Epsilon), Marketing:1 (Gamma)
	check("RW3 projects per dept",
		`SELECT d.name, COUNT(DISTINCT p.id) AS proj_count
		 FROM v45_dept d, v45_proj p
		 WHERE d.id = p.dept_id
		 GROUP BY d.name
		 ORDER BY proj_count DESC, d.name ASC LIMIT 1`,
		"Engineering") // Tied at 2, Engineering first alphabetically

	// RW4: Employee hierarchy (self-join via derived tables)
	checkRowCount("RW4 employee hierarchy",
		`SELECT e.name AS employee, m.name AS manager
		 FROM (SELECT * FROM v45_emp WHERE manager_id IS NOT NULL) AS e
		 JOIN v45_emp m ON e.manager_id = m.id`, 4)

	// RW5: Budget utilization
	check("RW5 budget utilization",
		`WITH dept_costs AS (
		   SELECT dept_id, SUM(salary) AS total_cost FROM v45_emp GROUP BY dept_id
		 )
		 SELECT v45_dept.name FROM v45_dept
		 JOIN dept_costs ON v45_dept.id = dept_costs.dept_id
		 ORDER BY (dept_costs.total_cost * 100 / v45_dept.budget) DESC LIMIT 1`,
		"Engineering") // 325000/500000 = 65%

	t.Logf("\n=== V45 ADVANCED FEATURES STRESS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
