package test

import (
	"fmt"
	"testing"
)

func TestV25JSONAndComplexQueries(t *testing.T) {
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

	_ = checkNoError

	// ============================================================
	// === JSON OPERATIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE json_data (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO json_data VALUES (1, '{"name":"Alice","age":30,"city":"NYC"}')`)
	afExec(t, db, ctx, `INSERT INTO json_data VALUES (2, '{"name":"Bob","age":25,"city":"LA"}')`)
	afExec(t, db, ctx, `INSERT INTO json_data VALUES (3, '{"name":"Charlie","age":35,"city":"NYC"}')`)

	check("JSON_EXTRACT string",
		`SELECT JSON_EXTRACT(data, '$.name') FROM json_data WHERE id = 1`, "Alice")
	check("JSON_EXTRACT number",
		`SELECT JSON_EXTRACT(data, '$.age') FROM json_data WHERE id = 2`, 25)
	check("JSON_EXTRACT city",
		`SELECT JSON_EXTRACT(data, '$.city') FROM json_data WHERE id = 3`, "NYC")

	// JSON with nested objects
	afExec(t, db, ctx, `INSERT INTO json_data VALUES (4, '{"user":{"name":"Dave","role":"admin"},"active":true}')`)
	check("JSON_EXTRACT nested",
		`SELECT JSON_EXTRACT(data, '$.user.name') FROM json_data WHERE id = 4`, "Dave")

	// JSON with arrays
	afExec(t, db, ctx, `INSERT INTO json_data VALUES (5, '{"tags":["go","sql","db"],"count":3}')`)
	check("JSON_EXTRACT from array",
		`SELECT JSON_EXTRACT(data, '$.count') FROM json_data WHERE id = 5`, 3)

	// ============================================================
	// === UNION WITH ORDER BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE u1 (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE u2 (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO u1 VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO u1 VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO u2 VALUES (1, 'C', 30)")
	afExec(t, db, ctx, "INSERT INTO u2 VALUES (2, 'D', 40)")

	checkRowCount("UNION ALL",
		"SELECT name, val FROM u1 UNION ALL SELECT name, val FROM u2", 4)

	checkRowCount("UNION (dedup)",
		"SELECT val FROM u1 UNION SELECT val FROM u2", 4) // All different values

	// Add duplicates
	afExec(t, db, ctx, "INSERT INTO u2 VALUES (3, 'A', 10)")
	checkRowCount("UNION dedup with overlap",
		"SELECT name, val FROM u1 UNION SELECT name, val FROM u2", 4) // A,10 deduped: A10, B20, C30, D40

	checkRowCount("UNION ALL with overlap",
		"SELECT name, val FROM u1 UNION ALL SELECT name, val FROM u2", 5)

	// ============================================================
	// === COMPLEX JOINS WITH AGGREGATES AND ORDER BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE cj_depts (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE cj_emps (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)")

	afExec(t, db, ctx, "INSERT INTO cj_depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO cj_depts VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO cj_depts VALUES (3, 'HR')")

	afExec(t, db, ctx, "INSERT INTO cj_emps VALUES (1, 'Alice', 1, 120000)")
	afExec(t, db, ctx, "INSERT INTO cj_emps VALUES (2, 'Bob', 1, 110000)")
	afExec(t, db, ctx, "INSERT INTO cj_emps VALUES (3, 'Charlie', 2, 90000)")
	afExec(t, db, ctx, "INSERT INTO cj_emps VALUES (4, 'Diana', 2, 95000)")
	afExec(t, db, ctx, "INSERT INTO cj_emps VALUES (5, 'Eve', 3, 80000)")

	check("JOIN + GROUP BY + ORDER BY aggregate",
		"SELECT cj_depts.name, SUM(cj_emps.salary) FROM cj_depts JOIN cj_emps ON cj_depts.id = cj_emps.dept_id GROUP BY cj_depts.name ORDER BY SUM(cj_emps.salary) DESC LIMIT 1",
		"Engineering") // 120000+110000=230000

	check("JOIN + GROUP BY + HAVING + ORDER BY",
		"SELECT cj_depts.name FROM cj_depts JOIN cj_emps ON cj_depts.id = cj_emps.dept_id GROUP BY cj_depts.name HAVING COUNT(*) >= 2 ORDER BY cj_depts.name LIMIT 1",
		"Engineering")

	check("JOIN + AVG salary",
		"SELECT cj_depts.name FROM cj_depts JOIN cj_emps ON cj_depts.id = cj_emps.dept_id GROUP BY cj_depts.name ORDER BY AVG(cj_emps.salary) DESC LIMIT 1",
		"Engineering") // avg 115000

	// ============================================================
	// === CORRELATED SUBQUERY WITH AGGREGATE ===
	// ============================================================
	check("Correlated subquery with SUM",
		"SELECT name FROM cj_depts WHERE (SELECT SUM(salary) FROM cj_emps WHERE cj_emps.dept_id = cj_depts.id) > 150000",
		"Engineering") // Only Engineering has sum > 150000

	checkRowCount("Correlated subquery multi result",
		"SELECT name FROM cj_depts WHERE (SELECT AVG(salary) FROM cj_emps WHERE cj_emps.dept_id = cj_depts.id) > 85000",
		2) // Engineering (115000) and Sales (92500)

	// ============================================================
	// === WINDOW FUNCTIONS ===
	// ============================================================
	check("ROW_NUMBER",
		"SELECT cj_emps.name FROM cj_emps ORDER BY salary DESC LIMIT 1", "Alice")

	check("ROW_NUMBER with partition",
		"SELECT ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) FROM cj_emps WHERE dept_id = 1 ORDER BY salary DESC LIMIT 1", 1)

	// ============================================================
	// === MULTIPLE LEVEL NESTING ===
	// ============================================================
	check("Subquery in subquery",
		"SELECT name FROM cj_emps WHERE salary > (SELECT AVG(salary) FROM cj_emps WHERE dept_id = (SELECT id FROM cj_depts WHERE name = 'Sales')) ORDER BY salary DESC LIMIT 1",
		"Alice") // Sales avg = 92500, Alice=120000 > 92500

	// ============================================================
	// === COMPLEX CASE IN SELECT ===
	// ============================================================
	check("CASE with aggregate",
		"SELECT CASE WHEN SUM(salary) > 200000 THEN 'high' ELSE 'low' END FROM cj_emps WHERE dept_id = 1",
		"high") // 230000 > 200000

	check("CASE with column comparison",
		"SELECT CASE WHEN salary > 100000 THEN 'senior' WHEN salary > 80000 THEN 'mid' ELSE 'junior' END FROM cj_emps WHERE id = 3",
		"mid") // Charlie: 90000

	// ============================================================
	// === INSERT...SELECT WITH JOIN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE emp_summary (id INTEGER PRIMARY KEY AUTO_INCREMENT, dept_name TEXT, emp_name TEXT)")
	checkNoError("INSERT...SELECT with JOIN",
		"INSERT INTO emp_summary (dept_name, emp_name) SELECT cj_depts.name, cj_emps.name FROM cj_depts JOIN cj_emps ON cj_depts.id = cj_emps.dept_id")
	checkRowCount("INSERT...SELECT result", "SELECT * FROM emp_summary", 5)
	check("INSERT...SELECT data correct",
		"SELECT dept_name FROM emp_summary WHERE emp_name = 'Alice'", "Engineering")

	// ============================================================
	// === CTE WITH JOIN ===
	// ============================================================
	check("CTE from JOIN",
		"WITH dept_totals AS (SELECT cj_depts.name AS dept, SUM(cj_emps.salary) AS total FROM cj_depts JOIN cj_emps ON cj_depts.id = cj_emps.dept_id GROUP BY cj_depts.name) SELECT dept FROM dept_totals ORDER BY total DESC LIMIT 1",
		"Engineering")

	// ============================================================
	// === MULTIPLE CONDITIONS IN JOIN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE mc_a (id INTEGER PRIMARY KEY, type TEXT, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE mc_b (id INTEGER PRIMARY KEY, type TEXT, ref_val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO mc_a VALUES (1, 'X', 10)")
	afExec(t, db, ctx, "INSERT INTO mc_a VALUES (2, 'Y', 20)")
	afExec(t, db, ctx, "INSERT INTO mc_b VALUES (1, 'X', 10)")
	afExec(t, db, ctx, "INSERT INTO mc_b VALUES (2, 'X', 20)")
	afExec(t, db, ctx, "INSERT INTO mc_b VALUES (3, 'Y', 20)")

	checkRowCount("JOIN with AND condition",
		"SELECT * FROM mc_a JOIN mc_b ON mc_a.type = mc_b.type AND mc_a.val = mc_b.ref_val", 2) // (1,X,10)+(1,X,10) and (2,Y,20)+(3,Y,20)

	// ============================================================
	// === GROUP BY MULTIPLE COLUMNS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE multi_gb (id INTEGER PRIMARY KEY, cat TEXT, subcat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO multi_gb VALUES (1, 'A', 'x', 10)")
	afExec(t, db, ctx, "INSERT INTO multi_gb VALUES (2, 'A', 'x', 20)")
	afExec(t, db, ctx, "INSERT INTO multi_gb VALUES (3, 'A', 'y', 30)")
	afExec(t, db, ctx, "INSERT INTO multi_gb VALUES (4, 'B', 'x', 40)")

	checkRowCount("GROUP BY two columns",
		"SELECT cat, subcat, SUM(val) FROM multi_gb GROUP BY cat, subcat", 3) // (A,x), (A,y), (B,x)

	check("GROUP BY two columns with ORDER BY",
		"SELECT cat, subcat FROM multi_gb GROUP BY cat, subcat ORDER BY SUM(val) DESC LIMIT 1",
		"B") // B,x has SUM=40

	// ============================================================
	// === SELF-JOIN WITH AGGREGATE ===
	// ============================================================
	check("Self-join count",
		"SELECT COUNT(*) FROM cj_emps e1 JOIN cj_emps e2 ON e1.dept_id = e2.dept_id AND e1.id < e2.id",
		2) // Eng: (1,2), Sales: (3,4) = 2 pairs

	t.Logf("\n=== V25 JSON & COMPLEX: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
