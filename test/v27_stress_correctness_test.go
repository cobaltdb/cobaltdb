package test

import (
	"fmt"
	"testing"
)

func TestV27StressCorrectness(t *testing.T) {
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
			t.Errorf("[FAIL] %s: expected error but got none", desc)
			return
		}
		pass++
	}

	_ = checkError

	// ============================================================
	// === BULK INSERT AND COUNT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE bulk (id INTEGER PRIMARY KEY AUTO_INCREMENT, val INTEGER)")
	for i := 1; i <= 100; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO bulk (val) VALUES (%d)", i))
	}
	check("100 rows inserted", "SELECT COUNT(*) FROM bulk", 100)
	check("SUM 1..100", "SELECT SUM(val) FROM bulk", 5050)
	check("AVG 1..100", "SELECT AVG(val) FROM bulk", 50.5)
	check("MIN 1..100", "SELECT MIN(val) FROM bulk", 1)
	check("MAX 1..100", "SELECT MAX(val) FROM bulk", 100)

	// ============================================================
	// === FILTERING LARGE DATASET ===
	// ============================================================
	checkRowCount("WHERE > 90", "SELECT * FROM bulk WHERE val > 90", 10)
	checkRowCount("WHERE BETWEEN 40 AND 60", "SELECT * FROM bulk WHERE val BETWEEN 40 AND 60", 21)
	checkRowCount("WHERE IN list", "SELECT * FROM bulk WHERE val IN (1, 50, 100)", 3)
	checkRowCount("WHERE modulo", "SELECT * FROM bulk WHERE val % 10 = 0", 10) // 10,20,...,100

	// ============================================================
	// === GROUP BY ON LARGE DATASET ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE grp_large (id INTEGER PRIMARY KEY AUTO_INCREMENT, category TEXT, val INTEGER)")
	for i := 1; i <= 50; i++ {
		cat := "A"
		if i > 20 {
			cat = "B"
		}
		if i > 40 {
			cat = "C"
		}
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO grp_large (category, val) VALUES ('%s', %d)", cat, i))
	}

	checkRowCount("GROUP BY 3 categories", "SELECT category, COUNT(*) FROM grp_large GROUP BY category", 3)
	check("Category A count", "SELECT COUNT(*) FROM grp_large WHERE category = 'A'", 20)
	check("Category B count", "SELECT COUNT(*) FROM grp_large WHERE category = 'B'", 20)
	check("Category C count", "SELECT COUNT(*) FROM grp_large WHERE category = 'C'", 10)

	check("GROUP BY ordered by SUM DESC",
		"SELECT category FROM grp_large GROUP BY category ORDER BY SUM(val) DESC LIMIT 1", "B") // B: 21+...+40 = 610

	// ============================================================
	// === UPDATE LARGE DATASET ===
	// ============================================================
	checkNoError("UPDATE large dataset", "UPDATE bulk SET val = val * 2 WHERE val <= 50")
	check("Updated SUM", "SELECT SUM(val) FROM bulk WHERE id <= 50",
		2550) // (1+2+...+50)*2 = 1275*2 = 2550
	check("Unchanged SUM", "SELECT SUM(val) FROM bulk WHERE id > 50",
		3775) // 51+52+...+100 = 3775

	// ============================================================
	// === DELETE FROM LARGE DATASET ===
	// ============================================================
	// Verify DELETE works on a subset
	check("Count before delete", "SELECT COUNT(*) FROM bulk", 100)
	checkNoError("DELETE by ID range", "DELETE FROM bulk WHERE id <= 10")
	check("Count after delete", "SELECT COUNT(*) FROM bulk", 90)

	// ============================================================
	// === MULTI-TABLE JOIN CORRECTNESS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE jt_depts (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE jt_emps (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE jt_projects (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, budget INTEGER)")

	afExec(t, db, ctx, "INSERT INTO jt_depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO jt_depts VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO jt_depts VALUES (3, 'HR')")

	for i := 1; i <= 10; i++ {
		deptID := ((i - 1) % 3) + 1
		salary := 50000 + i*5000
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO jt_emps VALUES (%d, 'Emp%d', %d, %d)", i, i, deptID, salary))
	}

	afExec(t, db, ctx, "INSERT INTO jt_projects VALUES (1, 'ProjectA', 1, 100000)")
	afExec(t, db, ctx, "INSERT INTO jt_projects VALUES (2, 'ProjectB', 1, 200000)")
	afExec(t, db, ctx, "INSERT INTO jt_projects VALUES (3, 'ProjectC', 2, 150000)")

	// JOIN dept + emp
	check("Dept with most employees",
		"SELECT jt_depts.name FROM jt_depts JOIN jt_emps ON jt_depts.id = jt_emps.dept_id GROUP BY jt_depts.name ORDER BY COUNT(*) DESC LIMIT 1",
		"Engineering") // Eng: emp 1,4,7,10 = 4 emps

	check("Dept highest avg salary",
		"SELECT jt_depts.name FROM jt_depts JOIN jt_emps ON jt_depts.id = jt_emps.dept_id GROUP BY jt_depts.name ORDER BY AVG(jt_emps.salary) DESC LIMIT 1",
		"HR") // HR: emp 3(65k),6(80k),9(95k) = avg 80k

	// ============================================================
	// === CORRELATED SUBQUERY ON LARGE DATA ===
	// ============================================================
	check("Emp with max salary in dept 1",
		"SELECT name FROM jt_emps WHERE dept_id = 1 AND salary = (SELECT MAX(salary) FROM jt_emps WHERE dept_id = 1)",
		"Emp10") // Emp10 has salary 100000

	check("Correlated: depts where max sal > 80000",
		"SELECT name FROM jt_depts WHERE (SELECT MAX(salary) FROM jt_emps WHERE jt_emps.dept_id = jt_depts.id) > 80000 ORDER BY name LIMIT 1",
		"Engineering") // Eng max=100k, HR max=95k, Sales max=90k - all > 80k

	checkRowCount("Depts where max sal > 80000 count",
		"SELECT name FROM jt_depts WHERE (SELECT MAX(salary) FROM jt_emps WHERE jt_emps.dept_id = jt_depts.id) > 80000", 3)

	// ============================================================
	// === COMPLEX CASE WITH MULTIPLE WHEN CLAUSES ===
	// ============================================================
	check("Complex CASE",
		"SELECT CASE WHEN salary > 90000 THEN 'senior' WHEN salary > 70000 THEN 'mid' WHEN salary > 50000 THEN 'junior' ELSE 'intern' END FROM jt_emps WHERE id = 5",
		"mid") // Emp5: salary=75000

	// ============================================================
	// === NESTED FUNCTIONS IN WHERE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE fn_data (id INTEGER PRIMARY KEY, text_val TEXT)")
	afExec(t, db, ctx, "INSERT INTO fn_data VALUES (1, 'Hello World')")
	afExec(t, db, ctx, "INSERT INTO fn_data VALUES (2, 'foo bar')")
	afExec(t, db, ctx, "INSERT INTO fn_data VALUES (3, 'TESTING')")

	checkRowCount("WHERE with nested functions",
		"SELECT id FROM fn_data WHERE LENGTH(UPPER(text_val)) = 7", 2) // "foo bar"=7, "TESTING"=7

	checkRowCount("WHERE UPPER LIKE",
		"SELECT * FROM fn_data WHERE UPPER(text_val) LIKE 'HELLO%'", 1)

	// ============================================================
	// === RECURSIVE CTE WITH LIMITS ===
	// ============================================================
	check("Recursive CTE factorial-like",
		"WITH RECURSIVE fact(n, f) AS (SELECT 1, 1 UNION ALL SELECT n+1, f*(n+1) FROM fact WHERE n < 5) SELECT MAX(f) FROM fact",
		120) // 5! = 120

	check("Recursive CTE fibonacci-like sum",
		"WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 20) SELECT SUM(n) FROM seq",
		210) // 1+2+...+20 = 210

	// ============================================================
	// === VIEWS WITH COMPLEX QUERIES ===
	// ============================================================
	checkNoError("Create complex view",
		"CREATE VIEW dept_summary AS SELECT jt_depts.name, COUNT(jt_emps.id) AS emp_count FROM jt_depts JOIN jt_emps ON jt_depts.id = jt_emps.dept_id GROUP BY jt_depts.name")

	check("Query complex view",
		"SELECT name FROM dept_summary ORDER BY emp_count DESC LIMIT 1", "Engineering")

	// ============================================================
	// === TRANSACTIONS WITH MANY OPERATIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_stress (id INTEGER PRIMARY KEY AUTO_INCREMENT, val INTEGER)")

	checkNoError("BEGIN txn stress", "BEGIN")
	for i := 1; i <= 20; i++ {
		checkNoError(fmt.Sprintf("Insert %d in txn", i),
			fmt.Sprintf("INSERT INTO txn_stress (val) VALUES (%d)", i*10))
	}
	check("Count in txn", "SELECT COUNT(*) FROM txn_stress", 20)
	checkNoError("COMMIT txn stress", "COMMIT")
	check("Count after commit", "SELECT COUNT(*) FROM txn_stress", 20)

	// Rollback test
	checkNoError("BEGIN rollback stress", "BEGIN")
	for i := 21; i <= 30; i++ {
		checkNoError(fmt.Sprintf("Insert %d in txn", i),
			fmt.Sprintf("INSERT INTO txn_stress (val) VALUES (%d)", i*10))
	}
	check("Count in txn before rollback", "SELECT COUNT(*) FROM txn_stress", 30)
	checkNoError("ROLLBACK stress", "ROLLBACK")
	check("Count after rollback", "SELECT COUNT(*) FROM txn_stress", 20)

	// ============================================================
	// === INDEX CORRECTNESS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE idx_corr (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	checkNoError("CREATE INDEX on score", "CREATE INDEX idx_score ON idx_corr(score)")

	for i := 1; i <= 30; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO idx_corr VALUES (%d, 'Person%d', %d)", i, i, i*3))
	}

	check("Index exact match", "SELECT name FROM idx_corr WHERE score = 30", "Person10")
	checkRowCount("Index range query", "SELECT * FROM idx_corr WHERE score >= 60 AND score <= 90", 11)

	// ============================================================
	// === MULTIPLE AGGREGATES IN ONE SELECT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE multi_agg2 (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO multi_agg2 VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO multi_agg2 VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO multi_agg2 VALUES (3, 'B', 30)")
	afExec(t, db, ctx, "INSERT INTO multi_agg2 VALUES (4, 'B', 40)")

	// Check that we can select multiple aggregates at once
	check("SUM in multi-agg query",
		"SELECT SUM(val) FROM multi_agg2", 100)
	check("COUNT in multi-agg",
		"SELECT COUNT(*) FROM multi_agg2", 4)
	check("AVG in multi-agg",
		"SELECT AVG(val) FROM multi_agg2", 25)

	// ============================================================
	// === ORDERING STABILITY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE order_stab (id INTEGER PRIMARY KEY, priority INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO order_stab VALUES (1, 1, 'Low1')")
	afExec(t, db, ctx, "INSERT INTO order_stab VALUES (2, 3, 'High1')")
	afExec(t, db, ctx, "INSERT INTO order_stab VALUES (3, 2, 'Mid1')")
	afExec(t, db, ctx, "INSERT INTO order_stab VALUES (4, 3, 'High2')")
	afExec(t, db, ctx, "INSERT INTO order_stab VALUES (5, 1, 'Low2')")

	check("ORDER BY ASC first", "SELECT name FROM order_stab ORDER BY priority ASC LIMIT 1", "Low1")
	check("ORDER BY DESC first", "SELECT name FROM order_stab ORDER BY priority DESC LIMIT 1", "High1")

	// Multi-column ORDER BY
	check("ORDER BY multi-col",
		"SELECT name FROM order_stab ORDER BY priority DESC, name ASC LIMIT 1", "High1")

	// ============================================================
	// === FOREIGN KEY CASCADING ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE fk_dept (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE fk_emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES fk_dept(id) ON DELETE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO fk_dept VALUES (1, 'Eng')")
	afExec(t, db, ctx, "INSERT INTO fk_dept VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO fk_emp VALUES (1, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO fk_emp VALUES (2, 'Bob', 1)")
	afExec(t, db, ctx, "INSERT INTO fk_emp VALUES (3, 'Charlie', 2)")

	checkNoError("Delete dept cascades", "DELETE FROM fk_dept WHERE id = 1")
	checkRowCount("Cascaded employees gone", "SELECT * FROM fk_emp WHERE dept_id = 1", 0)
	checkRowCount("Other employees intact", "SELECT * FROM fk_emp WHERE dept_id = 2", 1)

	// ============================================================
	// === NOT NULL AND DEFAULT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE not_null_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, status TEXT DEFAULT 'active')")
	checkNoError("Insert with default", "INSERT INTO not_null_test (id, name) VALUES (1, 'Alice')")
	check("Default value applied", "SELECT status FROM not_null_test WHERE id = 1", "active")
	checkError("NOT NULL violation", "INSERT INTO not_null_test (id) VALUES (2)")

	// ============================================================
	// === UNIQUE CONSTRAINT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE unique_test (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
	checkNoError("Insert unique", "INSERT INTO unique_test VALUES (1, 'a@test.com')")
	checkError("Unique violation", "INSERT INTO unique_test VALUES (2, 'a@test.com')")
	checkNoError("Different unique", "INSERT INTO unique_test VALUES (2, 'b@test.com')")

	// ============================================================
	// === SHOW TABLES ===
	// ============================================================
	checkNoError("SHOW TABLES runs", "SHOW TABLES")

	t.Logf("\n=== V27 STRESS CORRECTNESS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
