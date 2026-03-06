package test

import (
	"fmt"
	"testing"
)

// TestV77BugHunt targets complex edge cases designed to reveal bugs in the SQL engine.
// Focuses on multi-table operations, complex expressions, NULL edge cases,
// DDL+transaction interactions, and tricky SQL patterns.
func TestV77BugHunt(t *testing.T) {
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

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
		}
	}

	checkNull := func(desc string, sql string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			pass++
			return
		}
		if rows[0][0] == nil {
			pass++
			return
		}
		t.Errorf("[FAIL] %s: expected NULL, got %v", desc, rows[0][0])
	}

	// ============================================================
	// === COMPLEX 3-TABLE JOIN WITH AGGREGATION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_depts (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v77_depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v77_depts VALUES (2, 'Marketing')")
	afExec(t, db, ctx, "INSERT INTO v77_depts VALUES (3, 'Sales')")

	afExec(t, db, ctx, `CREATE TABLE v77_emps (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_emps VALUES (1, 'Alice', 1, 90000)")
	afExec(t, db, ctx, "INSERT INTO v77_emps VALUES (2, 'Bob', 1, 85000)")
	afExec(t, db, ctx, "INSERT INTO v77_emps VALUES (3, 'Carol', 2, 70000)")
	afExec(t, db, ctx, "INSERT INTO v77_emps VALUES (4, 'Dave', 2, 75000)")
	afExec(t, db, ctx, "INSERT INTO v77_emps VALUES (5, 'Eve', 3, 65000)")

	afExec(t, db, ctx, `CREATE TABLE v77_projects (id INTEGER PRIMARY KEY, emp_id INTEGER, name TEXT, budget INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_projects VALUES (1, 1, 'Alpha', 50000)")
	afExec(t, db, ctx, "INSERT INTO v77_projects VALUES (2, 1, 'Beta', 30000)")
	afExec(t, db, ctx, "INSERT INTO v77_projects VALUES (3, 2, 'Gamma', 40000)")
	afExec(t, db, ctx, "INSERT INTO v77_projects VALUES (4, 3, 'Delta', 20000)")
	afExec(t, db, ctx, "INSERT INTO v77_projects VALUES (5, 5, 'Epsilon', 10000)")

	// 3-table JOIN: dept name + employee count + total project budget
	check("3JOIN: eng project count",
		`SELECT COUNT(p.id) FROM v77_depts d
		 JOIN v77_emps e ON d.id = e.dept_id
		 JOIN v77_projects p ON e.id = p.emp_id
		 WHERE d.name = 'Engineering'`, 3) // Alpha, Beta, Gamma

	check("3JOIN: eng total budget",
		`SELECT SUM(p.budget) FROM v77_depts d
		 JOIN v77_emps e ON d.id = e.dept_id
		 JOIN v77_projects p ON e.id = p.emp_id
		 WHERE d.name = 'Engineering'`, 120000)

	// 3-table LEFT JOIN: dept name + project count (including depts with no projects)
	check("3JOIN LEFT: marketing budget",
		`SELECT COALESCE(SUM(p.budget), 0) FROM v77_depts d
		 JOIN v77_emps e ON d.id = e.dept_id
		 LEFT JOIN v77_projects p ON e.id = p.emp_id
		 WHERE d.name = 'Marketing'`, 20000) // only Carol has Delta

	// ============================================================
	// === CORRELATED SUBQUERY IN SELECT LIST ===
	// ============================================================

	check("CORR-SEL: project count per dept",
		`SELECT d.name, (
			SELECT COUNT(*) FROM v77_emps e
			JOIN v77_projects p ON e.id = p.emp_id
			WHERE e.dept_id = d.id
		) as proj_count
		FROM v77_depts d WHERE d.name = 'Engineering'`, "Engineering")

	// ============================================================
	// === UPDATE WITH COMPLEX WHERE + SUBQUERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_upd (id INTEGER PRIMARY KEY, val INTEGER, status TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v77_upd VALUES (1, 100, 'active')")
	afExec(t, db, ctx, "INSERT INTO v77_upd VALUES (2, 200, 'active')")
	afExec(t, db, ctx, "INSERT INTO v77_upd VALUES (3, 300, 'active')")
	afExec(t, db, ctx, "INSERT INTO v77_upd VALUES (4, 400, 'active')")

	// UPDATE with subquery in WHERE
	checkNoError("UPD-SUB: update above avg",
		`UPDATE v77_upd SET status = 'high'
		 WHERE val > (SELECT AVG(val) FROM v77_upd)`)

	checkRowCount("UPD-SUB: verify high",
		"SELECT * FROM v77_upd WHERE status = 'high'", 2) // 300, 400

	// UPDATE with CASE expression
	checkNoError("UPD-CASE: tier update",
		`UPDATE v77_upd SET status = CASE
			WHEN val >= 400 THEN 'premium'
			WHEN val >= 200 THEN 'standard'
			ELSE 'basic'
		 END`)

	check("UPD-CASE: verify premium",
		"SELECT status FROM v77_upd WHERE id = 4", "premium")
	check("UPD-CASE: verify basic",
		"SELECT status FROM v77_upd WHERE id = 1", "basic")

	// ============================================================
	// === DELETE WITH SUBQUERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_del (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_del VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v77_del VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v77_del VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO v77_del VALUES (4, 40)")
	afExec(t, db, ctx, "INSERT INTO v77_del VALUES (5, 50)")

	// Delete rows below average
	checkNoError("DEL-SUB: below avg",
		"DELETE FROM v77_del WHERE val < (SELECT AVG(val) FROM v77_del)")
	check("DEL-SUB: remaining count", "SELECT COUNT(*) FROM v77_del", 3) // 30,40,50

	// ============================================================
	// === WINDOW FUNCTIONS WITH NULL VALUES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_wnull (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_wnull VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v77_wnull VALUES (2, 'A', NULL)")
	afExec(t, db, ctx, "INSERT INTO v77_wnull VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO v77_wnull VALUES (4, 'B', NULL)")
	afExec(t, db, ctx, "INSERT INTO v77_wnull VALUES (5, 'B', 50)")

	// ROW_NUMBER ignores NULLs in the data (counts all rows)
	check("WIN-NULL: ROW_NUMBER with NULLs",
		`WITH rn AS (
			SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) as rnum FROM v77_wnull
		) SELECT rnum FROM rn WHERE id = 3`, 3) // 3rd row in group A

	// SUM with NULLs (should skip NULLs)
	check("WIN-NULL: SUM partitioned",
		`WITH ws AS (
			SELECT id, grp, SUM(val) OVER (PARTITION BY grp) as total FROM v77_wnull
		) SELECT total FROM ws WHERE id = 1`, 40) // 10+30=40 (NULL skipped)

	// COUNT(*) vs COUNT(val) with NULLs
	check("WIN-NULL: COUNT(*) partition",
		`WITH wc AS (
			SELECT id, grp, COUNT(*) OVER (PARTITION BY grp) as cnt FROM v77_wnull
		) SELECT cnt FROM wc WHERE id = 1`, 3) // all 3 rows in A

	// ============================================================
	// === TRANSACTION + CREATE TABLE + DROP TABLE ===
	// ============================================================

	checkNoError("TXN-DDL: BEGIN", "BEGIN")
	checkNoError("TXN-DDL: CREATE", "CREATE TABLE v77_txn_tbl (id INTEGER PRIMARY KEY, val TEXT)")
	checkNoError("TXN-DDL: INSERT", "INSERT INTO v77_txn_tbl VALUES (1, 'test')")
	check("TXN-DDL: in-txn query", "SELECT val FROM v77_txn_tbl WHERE id = 1", "test")
	checkNoError("TXN-DDL: ROLLBACK", "ROLLBACK")
	// Table should not exist after rollback
	checkError("TXN-DDL: table gone", "SELECT * FROM v77_txn_tbl")

	// ============================================================
	// === TRANSACTION + CREATE INDEX ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_idx_tbl (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_idx_tbl VALUES (1, 'Alice', 90)")
	afExec(t, db, ctx, "INSERT INTO v77_idx_tbl VALUES (2, 'Bob', 80)")

	checkNoError("TXN-IDX: BEGIN", "BEGIN")
	checkNoError("TXN-IDX: CREATE INDEX", "CREATE INDEX idx_v77_score ON v77_idx_tbl(score)")
	// Index should work during txn
	check("TXN-IDX: query", "SELECT name FROM v77_idx_tbl WHERE score = 90", "Alice")
	checkNoError("TXN-IDX: COMMIT", "COMMIT")
	// Index should still work after commit
	check("TXN-IDX: after commit", "SELECT name FROM v77_idx_tbl WHERE score = 80", "Bob")

	// ============================================================
	// === DROP TABLE IN TRANSACTION + ROLLBACK ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_drop_txn (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_drop_txn VALUES (1, 42)")

	checkNoError("TXN-DROP: BEGIN", "BEGIN")
	checkNoError("TXN-DROP: DROP TABLE", "DROP TABLE v77_drop_txn")
	checkError("TXN-DROP: table gone in txn", "SELECT * FROM v77_drop_txn")
	checkNoError("TXN-DROP: ROLLBACK", "ROLLBACK")
	// Table should be restored after rollback
	check("TXN-DROP: table restored", "SELECT val FROM v77_drop_txn WHERE id = 1", 42)

	// ============================================================
	// === SAVEPOINT + CREATE TABLE + ROLLBACK TO ===
	// ============================================================

	checkNoError("SP-DDL: BEGIN", "BEGIN")
	checkNoError("SP-DDL: SAVEPOINT", "SAVEPOINT sp_ddl")
	checkNoError("SP-DDL: CREATE", "CREATE TABLE v77_sp_tbl (id INTEGER PRIMARY KEY)")
	checkNoError("SP-DDL: INSERT", "INSERT INTO v77_sp_tbl VALUES (1)")
	checkNoError("SP-DDL: ROLLBACK TO", "ROLLBACK TO sp_ddl")
	checkError("SP-DDL: table gone", "SELECT * FROM v77_sp_tbl")
	checkNoError("SP-DDL: COMMIT", "COMMIT")

	// ============================================================
	// === COMPLEX CASE IN ORDER BY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_case_ord (id INTEGER PRIMARY KEY, priority TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_case_ord VALUES (1, 'low', 10)")
	afExec(t, db, ctx, "INSERT INTO v77_case_ord VALUES (2, 'high', 20)")
	afExec(t, db, ctx, "INSERT INTO v77_case_ord VALUES (3, 'medium', 30)")
	afExec(t, db, ctx, "INSERT INTO v77_case_ord VALUES (4, 'high', 40)")
	afExec(t, db, ctx, "INSERT INTO v77_case_ord VALUES (5, 'low', 50)")

	// ORDER BY CASE to custom-sort priority
	check("CASE-ORD: custom sort first",
		`SELECT id FROM v77_case_ord ORDER BY
			CASE priority
				WHEN 'high' THEN 1
				WHEN 'medium' THEN 2
				WHEN 'low' THEN 3
			END, val DESC
		LIMIT 1`, 4) // high priority, highest val = 40

	// ============================================================
	// === EXPRESSIONS IN INSERT VALUES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_expr_ins (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)`)
	checkNoError("EXPR-INS: computed values",
		"INSERT INTO v77_expr_ins VALUES (1, 10 + 20, 'sum')")
	check("EXPR-INS: verify sum", "SELECT val FROM v77_expr_ins WHERE id = 1", 30)

	checkNoError("EXPR-INS: CASE",
		"INSERT INTO v77_expr_ins VALUES (2, CASE WHEN 1=1 THEN 42 ELSE 0 END, 'case')")
	check("EXPR-INS: verify case", "SELECT val FROM v77_expr_ins WHERE id = 2", 42)

	// ============================================================
	// === LIKE EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_like (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v77_like VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO v77_like VALUES (2, 'HELLO')")
	afExec(t, db, ctx, "INSERT INTO v77_like VALUES (3, 'Hello World')")
	afExec(t, db, ctx, "INSERT INTO v77_like VALUES (4, '%special')")
	afExec(t, db, ctx, "INSERT INTO v77_like VALUES (5, 'under_score')")
	afExec(t, db, ctx, "INSERT INTO v77_like VALUES (6, '')")

	// Case-insensitive LIKE (CobaltDB behavior)
	checkRowCount("LIKE: case insensitive",
		"SELECT * FROM v77_like WHERE val LIKE 'hello'", 2) // 'hello' and 'HELLO'

	// Wildcard patterns
	checkRowCount("LIKE: starts with",
		"SELECT * FROM v77_like WHERE val LIKE 'hello%'", 3) // hello, HELLO, Hello World

	checkRowCount("LIKE: ends with",
		"SELECT * FROM v77_like WHERE val LIKE '%world'", 1) // Hello World

	checkRowCount("LIKE: contains",
		"SELECT * FROM v77_like WHERE val LIKE '%llo%'", 3) // hello, HELLO, Hello World

	// NOT LIKE
	checkRowCount("NOT LIKE: basic",
		"SELECT * FROM v77_like WHERE val NOT LIKE '%hello%'", 3) // %special, under_score, ''

	// ============================================================
	// === UNIQUE CONSTRAINT BEHAVIOR ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_uq (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)`)
	checkNoError("UQ: insert 1", "INSERT INTO v77_uq VALUES (1, 'alice@test.com', 'Alice')")
	checkNoError("UQ: insert 2", "INSERT INTO v77_uq VALUES (2, 'bob@test.com', 'Bob')")
	checkError("UQ: dup email", "INSERT INTO v77_uq VALUES (3, 'alice@test.com', 'Not Alice')")
	check("UQ: count", "SELECT COUNT(*) FROM v77_uq", 2)
	// UNIQUE with NULL: multiple NULLs should be allowed (SQL standard)
	checkNoError("UQ: null 1", "INSERT INTO v77_uq VALUES (3, NULL, 'NoEmail1')")
	checkNoError("UQ: null 2", "INSERT INTO v77_uq VALUES (4, NULL, 'NoEmail2')")
	check("UQ: count after nulls", "SELECT COUNT(*) FROM v77_uq", 4)

	// ============================================================
	// === SELF-JOIN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_tree (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v77_tree VALUES (1, NULL, 'root')")
	afExec(t, db, ctx, "INSERT INTO v77_tree VALUES (2, 1, 'child1')")
	afExec(t, db, ctx, "INSERT INTO v77_tree VALUES (3, 1, 'child2')")
	afExec(t, db, ctx, "INSERT INTO v77_tree VALUES (4, 2, 'grandchild1')")

	// Self-join to get parent name
	check("SELF-JOIN: parent name",
		`SELECT p.name FROM v77_tree c JOIN v77_tree p ON c.parent_id = p.id
		 WHERE c.name = 'child1'`, "root")

	// Count children
	check("SELF-JOIN: child count",
		`SELECT COUNT(*) FROM v77_tree c JOIN v77_tree p ON c.parent_id = p.id
		 WHERE p.name = 'root'`, 2)

	// LEFT JOIN to find leaves (nodes with no children)
	checkRowCount("SELF-JOIN: leaves",
		`SELECT p.name FROM v77_tree p
		 LEFT JOIN v77_tree c ON p.id = c.parent_id
		 WHERE c.id IS NULL`, 2) // child2, grandchild1

	// ============================================================
	// === COMPLEX HAVING WITH MULTIPLE CONDITIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_hav (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER, status TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v77_hav VALUES (1, 'A', 10, 'active')")
	afExec(t, db, ctx, "INSERT INTO v77_hav VALUES (2, 'A', 20, 'active')")
	afExec(t, db, ctx, "INSERT INTO v77_hav VALUES (3, 'A', 5, 'inactive')")
	afExec(t, db, ctx, "INSERT INTO v77_hav VALUES (4, 'B', 30, 'active')")
	afExec(t, db, ctx, "INSERT INTO v77_hav VALUES (5, 'B', 40, 'active')")
	afExec(t, db, ctx, "INSERT INTO v77_hav VALUES (6, 'C', 1, 'inactive')")

	// HAVING with compound conditions
	checkRowCount("HAV-COMPLEX: count + sum",
		"SELECT cat FROM v77_hav GROUP BY cat HAVING COUNT(*) >= 2 AND SUM(val) > 30", 2) // A=35, B=70

	// HAVING with OR
	checkRowCount("HAV-OR: min or max",
		"SELECT cat FROM v77_hav GROUP BY cat HAVING MIN(val) < 5 OR MAX(val) > 35", 2) // B(max=40), C(min=1)

	// ============================================================
	// === COMPLEX CTE WITH MULTIPLE AGGREGATIONS ===
	// ============================================================

	check("CTE-AGG: multi-level",
		`WITH dept_totals AS (
			SELECT dept_id, SUM(salary) as total_sal, COUNT(*) as emp_count
			FROM v77_emps GROUP BY dept_id
		),
		dept_with_name AS (
			SELECT d.name, dt.total_sal, dt.emp_count
			FROM dept_totals dt JOIN v77_depts d ON dt.dept_id = d.id
		)
		SELECT name FROM dept_with_name ORDER BY total_sal DESC LIMIT 1`, "Engineering")

	// ============================================================
	// === SUBQUERY IN INSERT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_ins_sub (id INTEGER PRIMARY KEY, max_val INTEGER)`)
	// Subquery in INSERT VALUES (was a bug: EvalExpression didn't handle SubqueryExpr)
	checkNoError("INS-SUB: insert with subquery",
		"INSERT INTO v77_ins_sub VALUES (1, (SELECT MAX(val) FROM v77_del))")
	check("INS-SUB: verify subquery value", "SELECT max_val FROM v77_ins_sub WHERE id = 1", 50)

	// Also test INSERT INTO SELECT
	checkNoError("INS-SUB: insert select",
		"INSERT INTO v77_ins_sub SELECT 2, MIN(val) FROM v77_del")
	check("INS-SUB: verify select", "SELECT max_val FROM v77_ins_sub WHERE id = 2", 30)

	// ============================================================
	// === BETWEEN WITH EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_btw (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_btw VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v77_btw VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v77_btw VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO v77_btw VALUES (4, 40)")
	afExec(t, db, ctx, "INSERT INTO v77_btw VALUES (5, 50)")

	checkRowCount("BETWEEN: basic",
		"SELECT * FROM v77_btw WHERE val BETWEEN 20 AND 40", 3)

	checkRowCount("BETWEEN: NOT",
		"SELECT * FROM v77_btw WHERE val NOT BETWEEN 20 AND 40", 2)

	// BETWEEN with expressions
	checkRowCount("BETWEEN: with expr",
		"SELECT * FROM v77_btw WHERE val BETWEEN 10 + 5 AND 50 - 10", 3) // 20,30,40

	// ============================================================
	// === IN WITH SUBQUERY ===
	// ============================================================

	checkRowCount("IN-SUB: basic",
		`SELECT * FROM v77_emps WHERE dept_id IN (
			SELECT id FROM v77_depts WHERE name IN ('Engineering', 'Sales')
		)`, 3) // Alice, Bob, Eve

	// NOT IN with subquery
	checkRowCount("NOT-IN-SUB: basic",
		`SELECT * FROM v77_emps WHERE dept_id NOT IN (
			SELECT id FROM v77_depts WHERE name = 'Engineering'
		)`, 3) // Carol, Dave, Eve

	// ============================================================
	// === MULTIPLE AGGREGATES IN ONE QUERY ===
	// ============================================================

	check("MULTI-AGG: all aggregates",
		`SELECT COUNT(*) FROM v77_emps`, 5)
	check("MULTI-AGG: sum",
		"SELECT SUM(salary) FROM v77_emps", 385000)
	check("MULTI-AGG: avg",
		"SELECT AVG(salary) FROM v77_emps", 77000)
	check("MULTI-AGG: min",
		"SELECT MIN(salary) FROM v77_emps", 65000)
	check("MULTI-AGG: max",
		"SELECT MAX(salary) FROM v77_emps", 90000)

	// ============================================================
	// === UPDATE MULTIPLE COLUMNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_multi_upd (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v77_multi_upd VALUES (1, 10, 20, 'old')")

	checkNoError("MULTI-UPD: update 3 cols",
		"UPDATE v77_multi_upd SET a = 100, b = 200, c = 'new' WHERE id = 1")
	check("MULTI-UPD: verify a", "SELECT a FROM v77_multi_upd WHERE id = 1", 100)
	check("MULTI-UPD: verify b", "SELECT b FROM v77_multi_upd WHERE id = 1", 200)
	check("MULTI-UPD: verify c", "SELECT c FROM v77_multi_upd WHERE id = 1", "new")

	// ============================================================
	// === NULL COMPARISONS IN JOIN ON CLAUSE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_j1 (id INTEGER PRIMARY KEY, ref INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_j1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v77_j1 VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v77_j1 VALUES (3, 20)")

	afExec(t, db, ctx, `CREATE TABLE v77_j2 (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_j2 VALUES (10, 100)")
	afExec(t, db, ctx, "INSERT INTO v77_j2 VALUES (20, 200)")
	afExec(t, db, ctx, "INSERT INTO v77_j2 VALUES (30, 300)")

	// INNER JOIN: NULLs should not match
	checkRowCount("JOIN-NULL: inner excludes null",
		"SELECT * FROM v77_j1 a JOIN v77_j2 b ON a.ref = b.id", 2) // id=1→10, id=3→20

	// LEFT JOIN: NULL ref should still appear with NULL on right
	checkRowCount("JOIN-NULL: left includes null",
		"SELECT * FROM v77_j1 a LEFT JOIN v77_j2 b ON a.ref = b.id", 3)

	// LEFT JOIN: check NULL side
	checkNull("JOIN-NULL: null right side",
		"SELECT b.val FROM v77_j1 a LEFT JOIN v77_j2 b ON a.ref = b.id WHERE a.id = 2")

	// ============================================================
	// === COMPLEX NESTED SUBQUERIES ===
	// ============================================================

	check("NESTED-SUB: 3 levels",
		`SELECT * FROM (
			SELECT MAX(total) as max_total FROM (
				SELECT dept_id, SUM(salary) as total FROM v77_emps GROUP BY dept_id
			) sub1
		) sub2`, 175000) // Engineering: 90000+85000

	// ============================================================
	// === ORDER BY with NULL values ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_ord_null (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_ord_null VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO v77_ord_null VALUES (2, 10)")
	afExec(t, db, ctx, "INSERT INTO v77_ord_null VALUES (3, 20)")
	afExec(t, db, ctx, "INSERT INTO v77_ord_null VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO v77_ord_null VALUES (5, 5)")

	// ORDER BY ASC: NULLs sort last in CobaltDB
	check("ORD-NULL: ASC third val",
		"SELECT val FROM v77_ord_null ORDER BY val ASC LIMIT 1 OFFSET 2", 20)

	// ORDER BY DESC: NULLs first
	checkNull("ORD-NULL: DESC first is null",
		"SELECT val FROM v77_ord_null ORDER BY val DESC LIMIT 1")

	// ============================================================
	// === COMPLEX UNION WITH CTE ===
	// ============================================================

	// high_sal: Alice(90k), Bob(85k). high_budget: Alice(Alpha 50k), Bob(Gamma 40k)
	check("UNION-CTE: combined",
		`WITH high_sal AS (
			SELECT name, salary FROM v77_emps WHERE salary >= 80000
		),
		high_budget AS (
			SELECT e.name, p.budget as salary FROM v77_emps e
			JOIN v77_projects p ON e.id = p.emp_id
			WHERE p.budget >= 40000
		)
		SELECT COUNT(*) FROM (
			SELECT name FROM high_sal
			UNION
			SELECT name FROM high_budget
		) combined`, 2) // Alice, Bob (deduped)

	// ============================================================
	// === AUTOINCREMENT / ROWID behavior ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_auto (id INTEGER PRIMARY KEY, val TEXT)`)
	checkNoError("AUTO: insert without id", "INSERT INTO v77_auto (val) VALUES ('first')")
	checkNoError("AUTO: insert without id 2", "INSERT INTO v77_auto (val) VALUES ('second')")
	check("AUTO: count", "SELECT COUNT(*) FROM v77_auto", 2)

	// ============================================================
	// === EMPTY TABLE OPERATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_empty (id INTEGER PRIMARY KEY, val INTEGER)`)

	check("EMPTY: COUNT", "SELECT COUNT(*) FROM v77_empty", 0)
	checkNull("EMPTY: SUM", "SELECT SUM(val) FROM v77_empty")
	checkNull("EMPTY: AVG", "SELECT AVG(val) FROM v77_empty")
	checkNull("EMPTY: MIN", "SELECT MIN(val) FROM v77_empty")
	checkNull("EMPTY: MAX", "SELECT MAX(val) FROM v77_empty")
	checkRowCount("EMPTY: SELECT *", "SELECT * FROM v77_empty", 0)

	// INSERT INTO empty, then verify
	checkNoError("EMPTY: insert", "INSERT INTO v77_empty VALUES (1, 42)")
	check("EMPTY: verify", "SELECT val FROM v77_empty WHERE id = 1", 42)

	// DELETE all and verify aggregates again
	checkNoError("EMPTY: delete all", "DELETE FROM v77_empty")
	check("EMPTY: COUNT after delete", "SELECT COUNT(*) FROM v77_empty", 0)
	checkNull("EMPTY: SUM after delete", "SELECT SUM(val) FROM v77_empty")

	// ============================================================
	// === COMPLEX EXPRESSIONS IN WHERE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v77_expr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v77_expr VALUES (1, 10, 20)")
	afExec(t, db, ctx, "INSERT INTO v77_expr VALUES (2, 30, 40)")
	afExec(t, db, ctx, "INSERT INTO v77_expr VALUES (3, 50, 10)")

	// WHERE with arithmetic: a+b → id=1→30, id=2→70, id=3→60
	checkRowCount("EXPR-WHERE: arithmetic",
		"SELECT * FROM v77_expr WHERE a + b > 60", 1) // only id=2(70)

	checkRowCount("EXPR-WHERE: arithmetic gte",
		"SELECT * FROM v77_expr WHERE a + b >= 60", 2) // id=2(70), id=3(60)

	// WHERE with multiplication
	checkRowCount("EXPR-WHERE: multiply",
		"SELECT * FROM v77_expr WHERE a * b >= 500", 2) // id=2(1200), id=3(500)

	// ============================================================
	// === TOTAL COUNT ===
	// ============================================================

	t.Logf("\n=== V77 BUG HUNT: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
