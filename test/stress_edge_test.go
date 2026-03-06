package test

import (
	"fmt"
	"testing"
)

func TestStressEdgeCases(t *testing.T) {
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

	checkRows := func(desc string, sql string, expectedCount int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expectedCount {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expectedCount)
			return
		}
		pass++
	}

	// Setup
	afExec(t, db, ctx, "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL, mgr_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (1, 'Alice', 'Eng', 120000, NULL)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (2, 'Bob', 'Eng', 100000, 1)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (3, 'Carol', 'Mkt', 90000, 1)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (4, 'Dave', 'Eng', 110000, 1)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (5, 'Eve', 'Mkt', 85000, 3)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (6, 'Frank', 'Sales', 95000, 1)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (7, 'Grace', 'Sales', 92000, 6)")

	afExec(t, db, ctx, "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT, budget REAL)")
	afExec(t, db, ctx, "INSERT INTO dept VALUES (1, 'Eng', 500000)")
	afExec(t, db, ctx, "INSERT INTO dept VALUES (2, 'Mkt', 200000)")
	afExec(t, db, ctx, "INSERT INTO dept VALUES (3, 'Sales', 150000)")
	afExec(t, db, ctx, "INSERT INTO dept VALUES (4, 'HR', 100000)")

	// 1. Self-join with aggregates
	check("Self-join count mgrs",
		"SELECT COUNT(DISTINCT e.mgr_id) FROM emp e INNER JOIN emp m ON e.mgr_id = m.id",
		3)

	// 2. Subquery in HAVING
	checkRows("Subquery in HAVING",
		"SELECT dept FROM emp GROUP BY dept HAVING AVG(salary) > (SELECT AVG(salary) FROM emp)",
		1)

	// 3. Multiple aggregates
	rows := afQuery(t, db, ctx, "SELECT dept, COUNT(*), AVG(salary), MIN(salary), MAX(salary), SUM(salary) FROM emp GROUP BY dept")
	t.Logf("Multi-agg: %v", rows)
	if len(rows) == 3 {
		pass++
		total++
	} else {
		t.Errorf("Multi-agg: expected 3 groups, got %d", len(rows))
		total++
	}

	// 4. INTERSECT with WHERE
	checkRows("INTERSECT WHERE",
		"SELECT name FROM emp WHERE salary > 100000 INTERSECT SELECT name FROM emp WHERE dept = 'Eng'",
		2) // Alice(120k, Eng), Dave(110k, Eng)

	// 5. EXCEPT with ORDER BY
	rows = afQuery(t, db, ctx, "SELECT name FROM emp WHERE dept = 'Eng' EXCEPT SELECT name FROM emp WHERE salary > 110000 ORDER BY name")
	t.Logf("EXCEPT ORDER: %v", rows)
	if len(rows) == 2 && fmt.Sprintf("%v", rows[0][0]) == "Bob" {
		pass++
		total++
	} else {
		t.Errorf("EXCEPT ORDER: expected [Bob, Dave], got %v", rows)
		total++
	}

	// 6. Window function with alias
	rows = afQuery(t, db, ctx, "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as ranking FROM emp")
	if len(rows) == 7 {
		pass++
		total++
	} else {
		t.Errorf("Window alias: expected 7 rows, got %d", len(rows))
		total++
	}

	// 7. LAG with default value
	rows = afQuery(t, db, ctx, "SELECT name, salary, LAG(salary, 1, 0) OVER (ORDER BY salary) as prev FROM emp")
	t.Logf("LAG default: %v", rows)
	total++
	// First row should have prev=0 (default)
	found := false
	for _, row := range rows {
		if fmt.Sprintf("%v", row[2]) == "0" {
			found = true
			break
		}
	}
	if found {
		pass++
	} else {
		t.Errorf("LAG default: expected 0 as default, not found")
	}

	// 8. FULL OUTER JOIN with aggregates
	check("FULL JOIN count",
		"SELECT COUNT(*) FROM emp e FULL OUTER JOIN dept d ON e.dept = d.name",
		8) // 7 emp matched + 1 unmatched dept (HR)

	// 9. CROSS JOIN with WHERE
	checkRows("CROSS JOIN WHERE",
		"SELECT e.name, d.name FROM emp e CROSS JOIN dept d WHERE d.name = e.dept",
		7)

	// 10. Nested CASE WHEN
	check("Nested CASE",
		"SELECT CASE WHEN salary > 100000 THEN CASE WHEN dept = 'Eng' THEN 'senior-eng' ELSE 'senior' END ELSE 'normal' END FROM emp WHERE id = 1",
		"senior-eng")

	// 11. COUNT(*) with GROUP BY HAVING
	check("COUNT GROUP HAVING",
		"SELECT COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) = 3",
		3)

	// 12. SUM with CASE
	check("SUM CASE",
		"SELECT SUM(CASE WHEN dept = 'Eng' THEN salary ELSE 0 END) FROM emp",
		330000)

	// 13. Multiple joins
	afExec(t, db, ctx, "CREATE TABLE proj (id INTEGER PRIMARY KEY, name TEXT, lead_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO proj VALUES (1, 'Alpha', 1)")
	afExec(t, db, ctx, "INSERT INTO proj VALUES (2, 'Beta', 2)")

	rows = afQuery(t, db, ctx, "SELECT e.name, d.name, p.name FROM emp e INNER JOIN dept d ON e.dept = d.name INNER JOIN proj p ON p.lead_id = e.id")
	t.Logf("3-way JOIN: %v", rows)
	checkRows("3-way JOIN", "SELECT e.name, d.name, p.name FROM emp e INNER JOIN dept d ON e.dept = d.name INNER JOIN proj p ON p.lead_id = e.id", 2)

	// 14. UPDATE with JOIN-like subquery
	afExec(t, db, ctx, "UPDATE emp SET salary = salary * 1.1 WHERE id IN (SELECT e.id FROM emp e WHERE e.dept = 'Eng')")
	// Note: salary * 1.1 produces floats, so just check count of updated rows
	checkRows("UPDATE subquery", "SELECT name FROM emp WHERE salary > 100000", 3) // Alice, Bob, Dave got 10% raise

	// 15. DELETE with NOT IN
	afExec(t, db, ctx, "CREATE TABLE temp_ids (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO temp_ids VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO temp_ids VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO temp_ids VALUES (3)")
	afExec(t, db, ctx, "INSERT INTO temp_ids VALUES (4)")
	afExec(t, db, ctx, "INSERT INTO temp_ids VALUES (5)")
	afExec(t, db, ctx, "DELETE FROM temp_ids WHERE id NOT IN (SELECT id FROM temp_ids WHERE id <= 3)")
	checkRows("DELETE NOT IN", "SELECT * FROM temp_ids", 3)

	// 16. INSERT SELECT with expression
	afExec(t, db, ctx, "CREATE TABLE emp_bonus (id INTEGER PRIMARY KEY, name TEXT, bonus REAL)")
	afExec(t, db, ctx, "INSERT INTO emp_bonus SELECT id, name, salary * 0.1 FROM emp WHERE dept = 'Eng'")
	checkRows("INSERT SELECT expr", "SELECT * FROM emp_bonus", 3)

	// 17. Multiple ORDER BY columns
	rows = afQuery(t, db, ctx, "SELECT dept, name FROM emp ORDER BY dept ASC, salary DESC")
	t.Logf("Multi ORDER BY: %v", rows)
	total++
	if len(rows) == 7 && fmt.Sprintf("%v", rows[0][0]) == "Eng" {
		pass++
	} else {
		t.Errorf("Multi ORDER BY: unexpected result %v", rows)
	}

	// 18. Window with aggregate: SUM OVER PARTITION
	rows = afQuery(t, db, ctx, "SELECT name, dept, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM emp")
	t.Logf("SUM OVER PARTITION: %v", rows)
	total++
	if len(rows) == 7 {
		pass++
	} else {
		t.Errorf("SUM OVER PARTITION: expected 7 rows, got %d", len(rows))
	}

	// 19. Empty result aggregate
	check("Empty COUNT", "SELECT COUNT(*) FROM emp WHERE salary > 999999", 0)

	// 20. NULL handling in aggregates
	check("COUNT non-null", "SELECT COUNT(mgr_id) FROM emp", 6) // Alice has NULL mgr_id

	// 21. Nested subquery in WHERE
	check("Nested subquery",
		"SELECT name FROM emp WHERE salary = (SELECT MAX(salary) FROM emp WHERE dept = (SELECT dept FROM emp WHERE id = 2))",
		"Alice") // Alice has max salary in Eng (after update: 132000)

	// 22. DISTINCT with ORDER BY
	rows = afQuery(t, db, ctx, "SELECT DISTINCT dept FROM emp ORDER BY dept")
	t.Logf("DISTINCT ORDER: %v", rows)
	checkRows("DISTINCT ORDER", "SELECT DISTINCT dept FROM emp ORDER BY dept", 3)

	// 23. BETWEEN with expressions
	checkRows("BETWEEN expr",
		"SELECT name FROM emp WHERE salary BETWEEN 90000 AND 120000",
		4)

	// 24. LIKE patterns
	checkRows("LIKE middle", "SELECT name FROM emp WHERE name LIKE '%a%'", 5) // Alice, Carol, Dave, Frank, Grace
	check("LIKE end", "SELECT name FROM emp WHERE name LIKE '%ce'", "Alice")

	// 25. Complex WHERE with AND/OR/NOT
	checkRows("Complex WHERE",
		"SELECT name FROM emp WHERE (dept = 'Eng' OR dept = 'Sales') AND salary > 95000 AND NOT name = 'Frank'",
		3) // Alice(132k), Bob(110k), Dave(121k) after updates

	t.Logf("\n=== STRESS EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed!")
	}
}
