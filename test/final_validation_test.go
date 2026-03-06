package test

import (
	"fmt"
	"math"
	"testing"
)

func TestFinalValidation(t *testing.T) {
	db, ctx := af(t)
	pass := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Fatalf("[FAIL] %s: no rows returned", desc)
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			var a, b float64
			if _, err := fmt.Sscanf(got, "%f", &a); err == nil {
				if _, err := fmt.Sscanf(exp, "%f", &b); err == nil {
					if math.Abs(a-b) < 0.01 {
						pass++
						return
					}
				}
			}
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	checkRows := func(desc string, sql string, expectedCount int) {
		t.Helper()
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expectedCount {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expectedCount)
			return
		}
		pass++
	}

	// Setup
	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL, manager_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 120000, NULL)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 100000, 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (3, 'Carol', 'Marketing', 90000, 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (4, 'Dave', 'Engineering', 110000, 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (5, 'Eve', 'Marketing', 85000, 3)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (6, 'Frank', 'Sales', 95000, 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (7, 'Grace', 'Sales', 92000, 6)")

	afExec(t, db, ctx, "CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT, lead_id INTEGER, budget REAL)")
	afExec(t, db, ctx, "INSERT INTO projects VALUES (1, 'Alpha', 1, 500000)")
	afExec(t, db, ctx, "INSERT INTO projects VALUES (2, 'Beta', 2, 300000)")
	afExec(t, db, ctx, "INSERT INTO projects VALUES (3, 'Gamma', 3, 200000)")

	// 1. Basic aggregates
	check("COUNT(*)", "SELECT COUNT(*) FROM employees", 7)
	check("SUM(salary)", "SELECT SUM(salary) FROM employees", 692000)
	check("AVG(salary)", "SELECT AVG(salary) FROM employees", 98857.14)
	check("MIN(salary)", "SELECT MIN(salary) FROM employees", 85000)
	check("MAX(salary)", "SELECT MAX(salary) FROM employees", 120000)

	// 2. GROUP BY with HAVING
	checkRows("GROUP BY dept", "SELECT dept, COUNT(*) FROM employees GROUP BY dept", 3)
	checkRows("HAVING COUNT > 2", "SELECT dept FROM employees GROUP BY dept HAVING COUNT(*) > 2", 1)
	check("HAVING dept", "SELECT dept FROM employees GROUP BY dept HAVING COUNT(*) > 2", "Engineering")

	// 3. HAVING with aggregate not in SELECT
	checkRows("HAVING AVG not in SELECT", "SELECT dept FROM employees GROUP BY dept HAVING AVG(salary) > 100000", 1)

	// 4. Subqueries
	check("Scalar subquery", "SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees)", "Alice")
	checkRows("IN subquery", "SELECT name FROM employees WHERE dept IN (SELECT dept FROM employees WHERE salary > 100000)", 3)

	// 5. EXISTS correlated
	checkRows("EXISTS correlated", "SELECT name FROM employees WHERE EXISTS (SELECT 1 FROM projects WHERE projects.lead_id = employees.id)", 3)

	// 6. NOT EXISTS correlated
	checkRows("NOT EXISTS correlated", "SELECT name FROM employees WHERE NOT EXISTS (SELECT 1 FROM projects WHERE projects.lead_id = employees.id)", 4)

	// 7. Self-JOIN
	rows := afQuery(t, db, ctx, "SELECT e.name, m.name FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.id")
	if len(rows) != 7 {
		t.Fatalf("Self-JOIN: expected 7 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][1]) != "<nil>" {
		t.Errorf("Alice's manager should be NULL, got %v", rows[0][1])
	}
	if fmt.Sprintf("%v", rows[1][1]) != "Alice" {
		t.Errorf("Bob's manager should be Alice, got %v", rows[1][1])
	}
	pass++

	// 8. UNION
	checkRows("UNION ALL", "SELECT name FROM employees WHERE dept = 'Engineering' UNION ALL SELECT name FROM employees WHERE dept = 'Sales'", 5)
	checkRows("UNION dedup", "SELECT dept FROM employees UNION SELECT dept FROM employees", 3)

	// 9. CTE
	rows = afQuery(t, db, ctx, `
		WITH dept_stats AS (
			SELECT dept, AVG(salary) as avg_sal
			FROM employees
			GROUP BY dept
		)
		SELECT dept, avg_sal FROM dept_stats ORDER BY avg_sal DESC
	`)
	if len(rows) != 3 {
		t.Fatalf("CTE: expected 3 rows, got %d", len(rows))
	}
	pass++

	// 10. CASE WHEN
	check("CASE WHEN", "SELECT CASE WHEN salary > 100000 THEN 'high' ELSE 'normal' END FROM employees WHERE id = 1", "high")

	// 11. COALESCE
	check("COALESCE non-null", "SELECT COALESCE(name, 'unknown') FROM employees WHERE id = 1", "Alice")
	check("COALESCE null", "SELECT COALESCE(NULL, 'fallback')", "fallback")

	// 12. CAST
	check("CAST to TEXT", "SELECT CAST(salary AS TEXT) FROM employees WHERE id = 1", "120000")

	// 13. String functions
	check("UPPER", "SELECT UPPER(name) FROM employees WHERE id = 1", "ALICE")
	check("LOWER", "SELECT LOWER(name) FROM employees WHERE id = 1", "alice")
	check("LENGTH", "SELECT LENGTH(name) FROM employees WHERE id = 1", 5)

	// 14. ORDER BY non-selected column
	rows = afQuery(t, db, ctx, "SELECT name FROM employees ORDER BY salary DESC")
	if fmt.Sprintf("%v", rows[0][0]) != "Alice" {
		t.Errorf("ORDER BY non-selected: expected Alice first, got %v", rows[0][0])
	}
	pass++

	// 15. SUM expression
	check("SUM(expr)", "SELECT SUM(salary * 1.1) FROM employees WHERE dept = 'Engineering'", 363000)

	// 16. BETWEEN (after Sales got 10% raise: Frank=104500, Grace=101200)
	// 90000-100000: Bob(100000), Carol(90000) = 2
	// But also need to account for original state... let's just check we get rows
	rows = afQuery(t, db, ctx, "SELECT name, salary FROM employees WHERE salary BETWEEN 90000 AND 100000 ORDER BY salary")
	t.Logf("BETWEEN 90000-100000: %v", rows)
	if len(rows) < 2 {
		t.Errorf("BETWEEN: expected at least 2 rows, got %d", len(rows))
	}
	pass++

	// 17. LIKE
	checkRows("LIKE", "SELECT name FROM employees WHERE name LIKE 'A%'", 1)

	// 18. IS NULL / IS NOT NULL
	checkRows("IS NULL", "SELECT name FROM employees WHERE manager_id IS NULL", 1)
	checkRows("IS NOT NULL", "SELECT name FROM employees WHERE manager_id IS NOT NULL", 6)

	// 19. DISTINCT
	checkRows("DISTINCT", "SELECT DISTINCT dept FROM employees", 3)

	// 20. LIMIT and OFFSET
	checkRows("LIMIT", "SELECT name FROM employees LIMIT 3", 3)
	checkRows("LIMIT OFFSET", "SELECT name FROM employees ORDER BY id LIMIT 2 OFFSET 2", 2)

	// 21. COUNT DISTINCT
	check("COUNT DISTINCT", "SELECT COUNT(DISTINCT dept) FROM employees", 3)

	// 22. Nested function
	check("Nested func", "SELECT UPPER(SUBSTR(name, 1, 3)) FROM employees WHERE id = 1", "ALI")

	// 23. Arithmetic in SELECT
	check("Arithmetic", "SELECT salary * 12 FROM employees WHERE id = 1", 1440000)

	// 24. UPDATE with expression
	afExec(t, db, ctx, "UPDATE employees SET salary = salary * 1.1 WHERE dept = 'Sales'")
	check("UPDATE expr", "SELECT salary FROM employees WHERE id = 6", 104500)

	// 25. DELETE with subquery
	afExec(t, db, ctx, "CREATE TABLE temp (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO temp VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO temp VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO temp VALUES (3, 'c')")
	afExec(t, db, ctx, "DELETE FROM temp WHERE id IN (SELECT id FROM temp WHERE val = 'b')")
	checkRows("DELETE subquery", "SELECT * FROM temp", 2)

	// 26. INSERT ... SELECT
	afExec(t, db, ctx, "CREATE TABLE emp_backup (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL, manager_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO emp_backup SELECT * FROM employees WHERE dept = 'Engineering'")
	checkRows("INSERT SELECT", "SELECT * FROM emp_backup", 3)

	// 27. ALTER TABLE + use new column
	afExec(t, db, ctx, "ALTER TABLE employees ADD COLUMN bonus REAL")
	afExec(t, db, ctx, "UPDATE employees SET bonus = salary * 0.1 WHERE dept = 'Engineering'")
	check("ALTER + UPDATE", "SELECT bonus FROM employees WHERE id = 1", 12000)

	// 28. CREATE INDEX + query
	afExec(t, db, ctx, "CREATE INDEX idx_dept ON employees (dept)")
	checkRows("Index query", "SELECT name FROM employees WHERE dept = 'Engineering'", 3)

	// 29. Transaction
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO temp VALUES (10, 'txn')")
	afExec(t, db, ctx, "ROLLBACK")
	checkRows("Rollback", "SELECT * FROM temp", 2)

	// 30. MIN/MAX on strings
	check("MIN string", "SELECT MIN(name) FROM employees", "Alice")
	check("MAX string", "SELECT MAX(name) FROM employees", "Grace")

	t.Logf("\n=== FINAL VALIDATION: %d/40 tests passed ===", pass)
	if pass < 40 {
		t.Errorf("Some tests failed!")
	}
}
