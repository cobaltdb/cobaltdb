package test

import (
	"fmt"
	"strings"
	"testing"
)

func TestFinalV3(t *testing.T) {
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

	checkExec := func(desc string, sql string) {
		t.Helper()
		total++
		afExec(t, db, ctx, sql)
		pass++
	}

	// === 1. SETUP ===
	checkExec("CREATE employees", "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL, mgr_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 130000, NULL)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 110000, 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (3, 'Carol', 'Marketing', 95000, 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (4, 'Dave', 'Engineering', 120000, 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (5, 'Eve', 'Marketing', 88000, 3)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (6, 'Frank', 'Sales', 100000, 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (7, 'Grace', 'Sales', 95000, 6)")

	checkExec("CREATE departments", "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget REAL, location TEXT)")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (1, 'Engineering', 600000, 'Building A')")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (2, 'Marketing', 250000, 'Building B')")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (3, 'Sales', 200000, 'Building C')")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (4, 'HR', 150000, 'Building A')")

	// === 2. BASIC QUERIES ===
	check("COUNT all", "SELECT COUNT(*) FROM employees", 7)
	check("SUM", "SELECT SUM(salary) FROM employees", 738000)
	check("AVG", "SELECT AVG(salary) FROM employees WHERE dept = 'Engineering'", 120000)
	check("MIN", "SELECT MIN(salary) FROM employees", 88000)
	check("MAX", "SELECT MAX(salary) FROM employees", 130000)

	// === 3. GROUP BY ===
	checkRows("GROUP BY", "SELECT dept, COUNT(*) FROM employees GROUP BY dept", 3)
	check("GROUP BY HAVING", "SELECT dept FROM employees GROUP BY dept HAVING COUNT(*) = 3", "Engineering")
	check("GROUP BY alias", "SELECT dept AS department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) >= 2", "Engineering")

	// === 4. JOINS ===
	checkRows("INNER JOIN", "SELECT e.name, d.location FROM employees e INNER JOIN departments d ON e.dept = d.name", 7)
	checkRows("LEFT JOIN", "SELECT e.name, d.budget FROM employees e LEFT JOIN departments d ON e.dept = d.name", 7)
	check("RIGHT JOIN count", "SELECT COUNT(*) FROM employees e RIGHT JOIN departments d ON e.dept = d.name", 8)
	check("FULL OUTER JOIN count", "SELECT COUNT(*) FROM employees e FULL OUTER JOIN departments d ON e.dept = d.name", 8)
	checkRows("CROSS JOIN", "SELECT e.name, d.name FROM employees e CROSS JOIN departments d WHERE d.name = 'HR'", 7)

	// === 5. JOIN + AGGREGATE ===
	rows := afQuery(t, db, ctx, "SELECT d.name, SUM(e.salary) FROM employees e INNER JOIN departments d ON e.dept = d.name GROUP BY d.name")
	total++
	if len(rows) == 3 {
		pass++
	} else {
		t.Errorf("[FAIL] JOIN SUM GROUP BY: expected 3 groups, got %d", len(rows))
	}
	t.Logf("JOIN SUM: %v", rows)

	// HAVING with JOIN
	checkRows("JOIN HAVING", "SELECT d.name, COUNT(e.id) FROM employees e INNER JOIN departments d ON e.dept = d.name GROUP BY d.name HAVING COUNT(e.id) >= 3", 1)

	// === 6. SUBQUERIES ===
	check("Scalar subquery", "SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees)", "Alice")
	check("Subquery in WHERE", "SELECT COUNT(*) FROM employees WHERE dept = (SELECT dept FROM employees WHERE id = 2)", 3)
	checkRows("IN subquery", "SELECT name FROM employees WHERE dept IN (SELECT name FROM departments WHERE budget > 200000)", 5)

	// Nested subquery
	check("Nested subquery", "SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees WHERE dept = (SELECT dept FROM employees WHERE id = 4))", "Alice")

	// Correlated subquery in SELECT
	rows = afQuery(t, db, ctx, "SELECT name, (SELECT COUNT(*) FROM employees e2 WHERE e2.dept = employees.dept) as dept_count FROM employees WHERE id = 1")
	total++
	if len(rows) > 0 && fmt.Sprintf("%v", rows[0][1]) == "3" {
		pass++
	} else {
		t.Errorf("[FAIL] Correlated subquery: %v", rows)
	}

	// === 7. SET OPERATIONS ===
	checkRows("UNION", "SELECT name FROM employees WHERE dept = 'Engineering' UNION SELECT name FROM employees WHERE salary > 100000", 3)
	checkRows("UNION ALL", "SELECT dept FROM employees WHERE salary > 100000 UNION ALL SELECT dept FROM employees WHERE dept = 'Engineering'", 6)
	checkRows("INTERSECT", "SELECT name FROM employees WHERE salary > 100000 INTERSECT SELECT name FROM employees WHERE dept = 'Engineering'", 3)
	checkRows("EXCEPT", "SELECT name FROM employees WHERE dept = 'Engineering' EXCEPT SELECT name FROM employees WHERE salary > 120000", 2)

	// === 8. WINDOW FUNCTIONS ===
	rows = afQuery(t, db, ctx, "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM employees")
	total++
	if len(rows) == 7 {
		pass++
	} else {
		t.Errorf("[FAIL] ROW_NUMBER: expected 7 rows, got %d", len(rows))
	}

	rows = afQuery(t, db, ctx, "SELECT name, dept, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk FROM employees")
	total++
	if len(rows) == 7 {
		pass++
	} else {
		t.Errorf("[FAIL] RANK PARTITION: expected 7, got %d", len(rows))
	}

	// SUM OVER PARTITION
	rows = afQuery(t, db, ctx, "SELECT name, dept, SUM(salary) OVER (PARTITION BY dept) FROM employees")
	total++
	if len(rows) == 7 {
		pass++
	} else {
		t.Errorf("[FAIL] SUM OVER PARTITION: expected 7, got %d", len(rows))
	}

	// === 9. INSERT OR REPLACE / IGNORE ===
	afExec(t, db, ctx, "CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)")
	afExec(t, db, ctx, "INSERT INTO config VALUES ('theme', 'light')")
	afExec(t, db, ctx, "INSERT OR REPLACE INTO config VALUES ('theme', 'dark')")
	check("INSERT OR REPLACE", "SELECT value FROM config WHERE key = 'theme'", "dark")

	afExec(t, db, ctx, "INSERT OR IGNORE INTO config VALUES ('theme', 'blue')")
	check("INSERT OR IGNORE", "SELECT value FROM config WHERE key = 'theme'", "dark")

	// === 10. SQL FUNCTIONS ===
	check("UPPER", "SELECT UPPER('hello')", "HELLO")
	check("LOWER", "SELECT LOWER('HELLO')", "hello")
	check("LENGTH", "SELECT LENGTH('hello')", 5)
	check("TRIM", "SELECT TRIM('  hi  ')", "hi")
	check("LTRIM", "SELECT LTRIM('  hi  ')", "hi  ")
	check("RTRIM", "SELECT RTRIM('  hi  ')", "  hi")
	check("SUBSTR", "SELECT SUBSTR('hello', 2, 3)", "ell")
	check("CONCAT", "SELECT CONCAT('a', 'b', 'c')", "abc")
	check("CONCAT_WS", "SELECT CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c")
	check("REVERSE", "SELECT REVERSE('hello')", "olleh")
	check("REPEAT", "SELECT REPEAT('ab', 3)", "ababab")
	check("LEFT func", "SELECT LEFT('hello', 3)", "hel")
	check("RIGHT func", "SELECT RIGHT('hello', 3)", "llo")
	check("LPAD", "SELECT LPAD('hi', 5, '0')", "000hi")
	check("RPAD", "SELECT RPAD('hi', 5, '0')", "hi000")
	check("REPLACE func", "SELECT REPLACE('hello', 'l', 'r')", "herro")
	check("INSTR", "SELECT INSTR('hello', 'lo')", 4)
	check("ABS", "SELECT ABS(-42)", 42)
	check("ROUND", "SELECT ROUND(3.14159, 2)", 3.14)
	check("FLOOR", "SELECT FLOOR(3.7)", 3)
	check("CEIL", "SELECT CEIL(3.2)", 4)
	check("COALESCE", "SELECT COALESCE(NULL, NULL, 'found')", "found")
	check("NULLIF equal", "SELECT NULLIF(5, 5)", "<nil>")
	check("NULLIF diff", "SELECT NULLIF(5, 3)", 5)
	check("IIF true", "SELECT IIF(1, 'yes', 'no')", "yes")
	check("IIF false", "SELECT IIF(0, 'yes', 'no')", "no")
	check("TYPEOF", "SELECT TYPEOF(42)", "integer")
	check("HEX", "SELECT HEX(255)", "FF")
	check("UNICODE", "SELECT UNICODE('A')", 65)

	// === 11. CASE WHEN ===
	check("CASE simple", "SELECT CASE WHEN salary > 100000 THEN 'high' ELSE 'normal' END FROM employees WHERE id = 1", "high")
	check("Nested CASE", "SELECT CASE WHEN salary > 100000 THEN CASE WHEN dept = 'Engineering' THEN 'senior-eng' ELSE 'senior' END ELSE 'junior' END FROM employees WHERE id = 1", "senior-eng")
	check("SUM CASE", "SELECT SUM(CASE WHEN dept = 'Engineering' THEN salary ELSE 0 END) FROM employees", 360000)
	checkRows("GROUP BY CASE", "SELECT CASE WHEN salary > 100000 THEN 'high' ELSE 'normal' END, COUNT(*) FROM employees GROUP BY CASE WHEN salary > 100000 THEN 'high' ELSE 'normal' END", 2)

	// === 12. COMPLEX QUERIES ===
	rows = afQuery(t, db, ctx, "SELECT DISTINCT dept FROM employees ORDER BY dept")
	total++
	if len(rows) == 3 && fmt.Sprintf("%v", rows[0][0]) == "Engineering" {
		pass++
	} else {
		t.Errorf("[FAIL] DISTINCT ORDER BY: %v", rows)
	}

	checkRows("BETWEEN", "SELECT name FROM employees WHERE salary BETWEEN 90000 AND 110000", 4) // Carol, Bob, Grace, Frank
	checkRows("LIKE prefix", "SELECT name FROM employees WHERE name LIKE 'A%'", 1)
	checkRows("LIKE suffix", "SELECT name FROM employees WHERE name LIKE '%e'", 4) // Alice, Eve, Grace, Dave

	// GROUP_CONCAT
	rows = afQuery(t, db, ctx, "SELECT dept, GROUP_CONCAT(name) FROM employees GROUP BY dept")
	t.Logf("GROUP_CONCAT: %v", rows)
	total++
	if len(rows) == 3 {
		pass++
		for _, row := range rows {
			if fmt.Sprintf("%v", row[0]) == "Engineering" {
				val := fmt.Sprintf("%v", row[1])
				if strings.Contains(val, "Alice") && strings.Contains(val, "Bob") && strings.Contains(val, "Dave") {
					total++
					pass++
				}
			}
		}
	} else {
		t.Errorf("[FAIL] GROUP_CONCAT: expected 3, got %d", len(rows))
	}

	// UPDATE with subquery
	afExec(t, db, ctx, "CREATE TABLE bonuses (id INTEGER PRIMARY KEY, emp_id INTEGER, amount REAL)")
	afExec(t, db, ctx, "INSERT INTO bonuses SELECT id, id, salary * 0.1 FROM employees WHERE dept = 'Engineering'")
	checkRows("INSERT SELECT", "SELECT * FROM bonuses", 3)

	// NOT IN
	checkRows("NOT IN", "SELECT name FROM employees WHERE dept NOT IN ('Engineering', 'Sales')", 2)

	// NULL handling
	check("COUNT non-null", "SELECT COUNT(mgr_id) FROM employees", 6)
	check("Empty aggregate", "SELECT COUNT(*) FROM employees WHERE salary > 999999", 0)

	// Self-join
	check("Self-join", "SELECT COUNT(DISTINCT e.mgr_id) FROM employees e INNER JOIN employees m ON e.mgr_id = m.id", 3)

	// Multiple ORDER BY
	rows = afQuery(t, db, ctx, "SELECT dept, name FROM employees ORDER BY dept ASC, salary DESC LIMIT 3")
	total++
	if len(rows) == 3 && fmt.Sprintf("%v", rows[0][0]) == "Engineering" {
		pass++
	} else {
		t.Errorf("[FAIL] Multi ORDER BY: %v", rows)
	}

	// CAST
	check("CAST int", "SELECT CAST(3.14 AS INTEGER)", 3)
	check("CAST text", "SELECT CAST(42 AS TEXT)", "42")

	// LIMIT OFFSET
	checkRows("LIMIT", "SELECT name FROM employees ORDER BY salary DESC LIMIT 3", 3)
	checkRows("LIMIT OFFSET", "SELECT name FROM employees ORDER BY salary DESC LIMIT 3 OFFSET 2", 3)

	// UPDATE with subquery in SET
	afExec(t, db, ctx, "CREATE TABLE settings (id INTEGER PRIMARY KEY, key TEXT, value REAL)")
	afExec(t, db, ctx, "INSERT INTO settings VALUES (1, 'avg_salary', 0)")
	afExec(t, db, ctx, "UPDATE settings SET value = (SELECT AVG(salary) FROM employees) WHERE key = 'avg_salary'")
	rows = afQuery(t, db, ctx, "SELECT value FROM settings WHERE key = 'avg_salary'")
	total++
	if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) != "0" {
		pass++
	} else {
		t.Errorf("[FAIL] UPDATE subquery: %v", rows)
	}

	t.Logf("\n=== FINAL V3 VALIDATION: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d passed", pass, total)
	}
}
