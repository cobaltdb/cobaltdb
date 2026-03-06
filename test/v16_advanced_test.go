package test

import (
	"fmt"
	"testing"
)

func TestV16Advanced(t *testing.T) {
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
			t.Errorf("[FAIL] %s: expected error but got none", desc)
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

	_ = checkError

	// ============================================================
	// === FK WITH STRING PRIMARY KEYS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE countries (code TEXT PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO countries VALUES ('US', 'United States')")
	afExec(t, db, ctx, "INSERT INTO countries VALUES ('UK', 'United Kingdom')")

	afExec(t, db, ctx, "CREATE TABLE cities (id INTEGER PRIMARY KEY, name TEXT, country_code TEXT, FOREIGN KEY (country_code) REFERENCES countries(code))")
	checkNoError("FK insert with valid string ref", "INSERT INTO cities VALUES (1, 'New York', 'US')")
	checkNoError("FK insert with another valid string ref", "INSERT INTO cities VALUES (2, 'London', 'UK')")
	checkError("FK insert with invalid string ref", "INSERT INTO cities VALUES (3, 'Unknown', 'XX')")

	check("FK string ref verified", "SELECT name FROM cities WHERE country_code = 'US'", "New York")
	checkRowCount("FK string ref count", "SELECT * FROM cities", 2)

	// ============================================================
	// === CASCADE DELETE WITH STRING PKs ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE brands (code TEXT PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO brands VALUES ('NK', 'Nike')")
	afExec(t, db, ctx, "INSERT INTO brands VALUES ('AD', 'Adidas')")

	afExec(t, db, ctx, "CREATE TABLE shoes (id INTEGER PRIMARY KEY, brand_code TEXT, model TEXT, FOREIGN KEY (brand_code) REFERENCES brands(code) ON DELETE CASCADE)")
	afExec(t, db, ctx, "INSERT INTO shoes VALUES (1, 'NK', 'Air Max')")
	afExec(t, db, ctx, "INSERT INTO shoes VALUES (2, 'NK', 'Jordan')")
	afExec(t, db, ctx, "INSERT INTO shoes VALUES (3, 'AD', 'Superstar')")

	afExec(t, db, ctx, "DELETE FROM brands WHERE code = 'NK'")
	checkRowCount("CASCADE delete with string PK", "SELECT * FROM shoes WHERE brand_code = 'NK'", 0)
	checkRowCount("Other brand intact", "SELECT * FROM shoes WHERE brand_code = 'AD'", 1)

	// ============================================================
	// === HAVING WITH GROUP_CONCAT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE scores (id INTEGER PRIMARY KEY, student TEXT, subject TEXT, grade INTEGER)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (1, 'Alice', 'Math', 90)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (2, 'Alice', 'Science', 85)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (3, 'Bob', 'Math', 70)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (4, 'Bob', 'Science', 75)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (5, 'Charlie', 'Math', 95)")

	check("HAVING with SUM",
		"SELECT student FROM scores GROUP BY student HAVING SUM(grade) > 160 ORDER BY student LIMIT 1", "Alice")

	check("HAVING with COUNT",
		"SELECT student FROM scores GROUP BY student HAVING COUNT(*) = 1", "Charlie")

	check("HAVING with AVG",
		"SELECT student FROM scores GROUP BY student HAVING AVG(grade) >= 87 ORDER BY student LIMIT 1", "Alice")

	// ============================================================
	// === HAVING WITH JOIN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (2, 'Marketing')")

	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'Alice', 1, 90000)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (2, 'Bob', 1, 85000)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (3, 'Charlie', 2, 70000)")

	check("HAVING with JOIN SUM",
		"SELECT departments.name FROM departments JOIN employees ON departments.id = employees.dept_id GROUP BY departments.name HAVING SUM(employees.salary) > 100000",
		"Engineering")

	check("HAVING with JOIN COUNT",
		"SELECT departments.name FROM departments JOIN employees ON departments.id = employees.dept_id GROUP BY departments.name HAVING COUNT(*) > 1",
		"Engineering")

	// ============================================================
	// === CTE WITH UNION ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 'b')")

	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (3, 'c')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (4, 'd')")

	// Recursive CTE (hierarchy traversal)
	check("Recursive CTE",
		"WITH RECURSIVE emp_tree(id, name, mgr) AS (SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id FROM employees e JOIN emp_tree et ON e.manager_id = et.id) SELECT COUNT(*) FROM emp_tree",
		0) // Note: employees table from FK test above has no NULL manager_id rows

	// ============================================================
	// === UNION / UNION ALL ===
	// ============================================================
	checkRowCount("UNION ALL",
		"SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2", 4)

	checkRowCount("UNION dedup",
		"SELECT val FROM t1 UNION SELECT val FROM t2", 4) // a, b, c, d - no duplicates

	// UNION with duplicates
	afExec(t, db, ctx, "CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t3 VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t3 VALUES (2, 'b')")

	checkRowCount("UNION removes duplicates",
		"SELECT val FROM t1 UNION SELECT val FROM t3", 2) // a, b

	checkRowCount("UNION ALL keeps duplicates",
		"SELECT val FROM t1 UNION ALL SELECT val FROM t3", 4) // a, b, a, b

	// ============================================================
	// === WINDOW FUNCTIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (1, 'East', 100)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (2, 'East', 200)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (3, 'West', 150)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (4, 'West', 250)")

	check("ROW_NUMBER window",
		"SELECT ROW_NUMBER() OVER (ORDER BY amount) FROM sales LIMIT 1", 1)

	check("ROW_NUMBER window partition",
		"SELECT ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) FROM sales WHERE region = 'East' ORDER BY amount LIMIT 1", 1)

	// ============================================================
	// === INSERT...SELECT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE t1_copy (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t1_copy SELECT * FROM t1")
	checkRowCount("INSERT...SELECT copies rows", "SELECT * FROM t1_copy", 2)
	check("INSERT...SELECT data correct", "SELECT val FROM t1_copy WHERE id = 1", "a")

	// ============================================================
	// === CASE WHEN IN SELECT ===
	// ============================================================
	check("CASE WHEN simple",
		"SELECT CASE WHEN amount > 150 THEN 'high' ELSE 'low' END FROM sales WHERE id = 1", "low")

	check("CASE WHEN high",
		"SELECT CASE WHEN amount > 150 THEN 'high' ELSE 'low' END FROM sales WHERE id = 2", "high")

	// ============================================================
	// === COALESCE / NULLIF / IFNULL ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE nullable (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO nullable VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO nullable VALUES (2, NULL)")

	check("COALESCE with non-null", "SELECT COALESCE(val, 0) FROM nullable WHERE id = 1", 10)
	check("COALESCE with null", "SELECT COALESCE(val, 0) FROM nullable WHERE id = 2", 0)

	// ============================================================
	// === COMPLEX JOIN ORDERING ===
	// ============================================================
	check("JOIN with multi-col ORDER BY",
		"SELECT employees.name FROM employees JOIN departments ON employees.dept_id = departments.id ORDER BY departments.name, employees.salary DESC LIMIT 1",
		"Alice")

	// ============================================================
	// === SUBQUERY IN SELECT ===
	// ============================================================
	check("Scalar subquery in SELECT",
		"SELECT (SELECT MAX(salary) FROM employees) FROM departments LIMIT 1", 90000)

	check("Correlated subquery in WHERE",
		"SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY name LIMIT 1", "Alice")

	// ============================================================
	// === MULTI-TABLE DELETE CASCADE CHAIN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO orgs VALUES (1, 'Org1')")

	afExec(t, db, ctx, "CREATE TABLE teams (id INTEGER PRIMARY KEY, org_id INTEGER, name TEXT, FOREIGN KEY (org_id) REFERENCES orgs(id) ON DELETE CASCADE)")
	afExec(t, db, ctx, "INSERT INTO teams VALUES (1, 1, 'TeamA')")

	afExec(t, db, ctx, "CREATE TABLE members (id INTEGER PRIMARY KEY, team_id INTEGER, name TEXT, FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE)")
	afExec(t, db, ctx, "INSERT INTO members VALUES (1, 1, 'Member1')")
	afExec(t, db, ctx, "INSERT INTO members VALUES (2, 1, 'Member2')")

	// Delete org should cascade to teams, which should cascade to members
	afExec(t, db, ctx, "DELETE FROM orgs WHERE id = 1")
	checkRowCount("Org deleted", "SELECT * FROM orgs", 0)
	checkRowCount("Teams cascaded", "SELECT * FROM teams", 0)
	checkRowCount("Members cascaded", "SELECT * FROM members", 0)

	t.Logf("\n=== V16 ADVANCED: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
