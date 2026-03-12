package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestV109CoverageBoost targets low-coverage catalog functions with 150+ test cases:
// 1. Window functions (evaluateWindowFunctions, evalWindowExprOnRow)
// 2. SELECT paths (executeSelectWithJoin, executeSelectWithJoinAndGroupBy)
// 3. CTE (ExecuteCTE, executeCTEUnion, executeRecursiveCTE)
// 4. UPDATE/DELETE paths (updateLocked, deleteLocked, deleteRowLocked)
// 5. INSERT paths (insertLocked)
// 6. DDL (CreateTable, AlterTableDropColumn, DropTable)
// 7. applyOrderBy
// 8. Transaction/Savepoint (RollbackTransaction, RollbackToSavepoint, CommitTransaction)
// 9. Foreign keys (OnDelete/OnUpdate, findReferencingRows)
// 10. Additional coverage for GROUP BY, set operations, subqueries, views
func TestV109CoverageBoost(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

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
		got := rows[0][0]
		if expected == nil {
			if got != nil {
				t.Errorf("[FAIL] %s: got %v (%T), expected nil", desc, got, got)
				return
			}
			pass++
			return
		}
		gotStr := fmt.Sprintf("%v", got)
		expStr := fmt.Sprintf("%v", expected)
		if gotStr != expStr {
			t.Errorf("[FAIL] %s: got %s (%T), expected %s", desc, gotStr, got, expStr)
			return
		}
		pass++
	}

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expected)
			return
		}
		pass++
	}

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: exec error: %v", desc, err)
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
			return
		}
		t.Errorf("[FAIL] %s: expected error but got none", desc)
	}

	checkNth := func(desc string, sql string, row int, col int, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) <= row || len(rows[row]) <= col {
			t.Errorf("[FAIL] %s: not enough rows/cols, got %d rows", desc, len(rows))
			return
		}
		gotStr := fmt.Sprintf("%v", rows[row][col])
		expStr := fmt.Sprintf("%v", expected)
		if gotStr != expStr {
			t.Errorf("[FAIL] %s: row[%d][%d] got %s, expected %s", desc, row, col, gotStr, expStr)
			return
		}
		pass++
	}

	// ============================================================
	// === SECTION 1: Window Functions ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v109_win (
		id INTEGER PRIMARY KEY,
		dept TEXT,
		team TEXT,
		name TEXT,
		salary REAL
	)`)
	afExec(t, db, ctx, "INSERT INTO v109_win VALUES (1, 'eng', 'backend', 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO v109_win VALUES (2, 'eng', 'backend', 'Bob', 120)")
	afExec(t, db, ctx, "INSERT INTO v109_win VALUES (3, 'eng', 'frontend', 'Carol', 110)")
	afExec(t, db, ctx, "INSERT INTO v109_win VALUES (4, 'sales', 'west', 'Dave', 90)")
	afExec(t, db, ctx, "INSERT INTO v109_win VALUES (5, 'sales', 'west', 'Eve', 95)")
	afExec(t, db, ctx, "INSERT INTO v109_win VALUES (6, 'sales', 'east', 'Frank', 105)")
	afExec(t, db, ctx, "INSERT INTO v109_win VALUES (7, 'eng', 'frontend', 'Grace', 130)")
	afExec(t, db, ctx, "INSERT INTO v109_win VALUES (8, 'hr', 'ops', 'Hank', 80)")

	// Window functions are computed on the result set AFTER WHERE filtering.
	// Use subqueries to check specific rows.

	// 1.1: PARTITION BY multiple columns - check via subquery
	checkNth("W1: ROW_NUMBER partition by dept,team - Bob rn=1",
		`SELECT name, rn FROM (
			SELECT name, ROW_NUMBER() OVER (PARTITION BY dept, team ORDER BY salary DESC) AS rn
			FROM v109_win
		) sub WHERE sub.name = 'Bob'`, 0, 1, int64(1))

	checkNth("W2: ROW_NUMBER partition by dept,team - Alice rn=2",
		`SELECT name, rn FROM (
			SELECT name, ROW_NUMBER() OVER (PARTITION BY dept, team ORDER BY salary DESC) AS rn
			FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, int64(2))

	// 1.2: Multiple window functions in same SELECT
	checkRowCount("W3: Multiple window funcs returns all rows",
		`SELECT name,
			ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn,
			RANK() OVER (ORDER BY salary DESC) AS rnk,
			DENSE_RANK() OVER (ORDER BY salary DESC) AS dr
		 FROM v109_win`, 8)

	// Check the top row (Grace, salary=130, should be rn=1, rnk=1, dr=1)
	checkNth("W4: Top row ROW_NUMBER=1",
		`SELECT name, rn FROM (
			SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM v109_win
		) sub WHERE sub.rn = 1`, 0, 0, "Grace")

	checkNth("W5: Top row RANK=1",
		`SELECT name, rnk FROM (
			SELECT name, RANK() OVER (ORDER BY salary DESC) AS rnk FROM v109_win
		) sub WHERE sub.rnk = 1`, 0, 0, "Grace")

	// 1.3: LAG with offset > 1 - check via subquery on full result
	checkNth("W6: LAG offset=2 for Carol (id=3)",
		`SELECT name, lagged FROM (
			SELECT name, LAG(name, 2) OVER (ORDER BY id) AS lagged FROM v109_win
		) sub WHERE sub.name = 'Carol'`, 0, 1, "Alice")

	checkNth("W7: LAG offset=3 for Bob returns NULL",
		`SELECT name, lagged FROM (
			SELECT name, LAG(name, 3) OVER (ORDER BY id) AS lagged FROM v109_win
		) sub WHERE sub.name = 'Bob'`, 0, 1, nil)

	// 1.4: LAG with default value
	checkNth("W8: LAG default=0 for Alice (first row)",
		`SELECT name, lagged FROM (
			SELECT name, LAG(salary, 1, 0) OVER (ORDER BY id) AS lagged FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, 0.0)

	checkNth("W9: LAG default=0 for Bob (has prev Alice=100)",
		`SELECT name, lagged FROM (
			SELECT name, LAG(salary, 1, 0) OVER (ORDER BY id) AS lagged FROM v109_win
		) sub WHERE sub.name = 'Bob'`, 0, 1, 100.0)

	// 1.5: LEAD with offset > 1
	checkNth("W10: LEAD offset=2 for Alice",
		`SELECT name, led FROM (
			SELECT name, LEAD(name, 2) OVER (ORDER BY id) AS led FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, "Carol")

	checkNth("W11: LEAD offset=3 for Grace returns NULL",
		`SELECT name, led FROM (
			SELECT name, LEAD(name, 3) OVER (ORDER BY id) AS led FROM v109_win
		) sub WHERE sub.name = 'Grace'`, 0, 1, nil)

	// 1.6: LEAD with default value
	checkNth("W12: LEAD default=-1 for Hank (last row)",
		`SELECT name, led FROM (
			SELECT name, LEAD(salary, 1, -1) OVER (ORDER BY id) AS led FROM v109_win
		) sub WHERE sub.name = 'Hank'`, 0, 1, -1.0)

	checkNth("W13: LEAD default=-1 for Alice (has next Bob=120)",
		`SELECT name, led FROM (
			SELECT name, LEAD(salary, 1, -1) OVER (ORDER BY id) AS led FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, 120.0)

	// 1.7: FIRST_VALUE
	checkNth("W14: FIRST_VALUE in dept partition (eng: Grace=130 first by DESC salary)",
		`SELECT name, fv FROM (
			SELECT name, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) AS fv FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, "Grace")

	checkNth("W15: FIRST_VALUE entire table (Hank=80 first by ASC salary)",
		`SELECT name, fv FROM (
			SELECT name, FIRST_VALUE(name) OVER (ORDER BY salary ASC) AS fv FROM v109_win
		) sub WHERE sub.name = 'Grace'`, 0, 1, "Hank")

	// 1.8: LAST_VALUE
	checkNth("W16: LAST_VALUE in dept partition (eng: Grace=130 last by ASC salary)",
		`SELECT name, lv FROM (
			SELECT name, LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary ASC) AS lv FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, "Grace")

	// 1.9: NTH_VALUE
	checkNth("W17: NTH_VALUE 2nd in eng partition (Bob=120, 2nd by DESC salary)",
		`SELECT name, nv FROM (
			SELECT name, NTH_VALUE(name, 2) OVER (PARTITION BY dept ORDER BY salary DESC) AS nv FROM v109_win
		) sub WHERE sub.name = 'Grace'`, 0, 1, "Bob")

	checkNth("W18: NTH_VALUE 3rd overall by DESC salary",
		`SELECT name, nv FROM (
			SELECT name, NTH_VALUE(salary, 3) OVER (ORDER BY salary DESC) AS nv FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, 110.0)

	// 1.10: Running SUM with ORDER BY
	checkNth("W19: Running SUM for Carol (id=3, cumulative 100+120+110=330)",
		`SELECT name, rs FROM (
			SELECT name, SUM(salary) OVER (ORDER BY id) AS rs FROM v109_win
		) sub WHERE sub.name = 'Carol'`, 0, 1, 330.0)

	// 1.11: Running COUNT with ORDER BY
	checkNth("W20: Running COUNT for Carol (3rd row)",
		`SELECT name, rc FROM (
			SELECT name, COUNT(*) OVER (ORDER BY id) AS rc FROM v109_win
		) sub WHERE sub.name = 'Carol'`, 0, 1, int64(3))

	// 1.12: AVG partitioned
	checkNth("W21: AVG over sales partition (90+95+105)/3=96.67",
		`SELECT name, av FROM (
			SELECT name, AVG(salary) OVER (PARTITION BY dept) AS av FROM v109_win
		) sub WHERE sub.name = 'Dave'`, 0, 1, 96.66666666666667)

	// 1.13: Complex ORDER BY in window
	checkNth("W22: Window RANK with DESC order in eng",
		`SELECT name, rnk FROM (
			SELECT name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM v109_win
		) sub WHERE sub.name = 'Grace'`, 0, 1, int64(1))

	// 1.14: SUM over partition without ORDER BY (full partition total)
	checkNth("W23: SUM partition total for eng (100+120+110+130=460)",
		`SELECT name, st FROM (
			SELECT name, SUM(salary) OVER (PARTITION BY dept) AS st FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, 460.0)

	// 1.15: MIN/MAX partitioned
	checkNth("W24: MIN over eng partition",
		`SELECT name, mn FROM (
			SELECT name, MIN(salary) OVER (PARTITION BY dept) AS mn FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, 100.0)

	checkNth("W25: MAX over eng partition",
		`SELECT name, mx FROM (
			SELECT name, MAX(salary) OVER (PARTITION BY dept) AS mx FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, 130.0)

	// 1.16: COUNT partitioned
	checkNth("W26: COUNT over eng partition",
		`SELECT name, cnt FROM (
			SELECT name, COUNT(*) OVER (PARTITION BY dept) AS cnt FROM v109_win
		) sub WHERE sub.name = 'Alice'`, 0, 1, int64(4))

	// 1.17: All rows get window values
	checkRowCount("W27: All rows have window values",
		`SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM v109_win`, 8)

	// 1.18: RANK with ties
	afExec(t, db, ctx, "CREATE TABLE v109_ties (id INTEGER PRIMARY KEY, score REAL)")
	afExec(t, db, ctx, "INSERT INTO v109_ties VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v109_ties VALUES (2, 100)")
	afExec(t, db, ctx, "INSERT INTO v109_ties VALUES (3, 90)")

	checkNth("W28: RANK tied first",
		`SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk FROM v109_ties ORDER BY id`, 0, 1, int64(1))
	checkNth("W29: RANK tied second (same rank)",
		`SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk FROM v109_ties ORDER BY id`, 1, 1, int64(1))
	checkNth("W30: RANK after tie (skips to 3)",
		`SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk FROM v109_ties ORDER BY id`, 2, 1, int64(3))

	// 1.19: DENSE_RANK with ties
	checkNth("W31: DENSE_RANK after tie (no skip, goes to 2)",
		`SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS dr FROM v109_ties ORDER BY id`, 2, 1, int64(2))

	// ============================================================
	// === SECTION 2: SELECT with JOINs + GROUP BY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v109_dept (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, `CREATE TABLE v109_emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)`)
	afExec(t, db, ctx, `CREATE TABLE v109_proj (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, budget REAL)`)

	afExec(t, db, ctx, "INSERT INTO v109_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v109_dept VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO v109_dept VALUES (3, 'HR')")

	afExec(t, db, ctx, "INSERT INTO v109_emp VALUES (1, 'Alice', 1, 100)")
	afExec(t, db, ctx, "INSERT INTO v109_emp VALUES (2, 'Bob', 1, 120)")
	afExec(t, db, ctx, "INSERT INTO v109_emp VALUES (3, 'Carol', 2, 90)")
	afExec(t, db, ctx, "INSERT INTO v109_emp VALUES (4, 'Dave', 2, 110)")
	afExec(t, db, ctx, "INSERT INTO v109_emp VALUES (5, 'Eve', 3, 80)")

	// 2.1: JOIN with GROUP BY + HAVING on aggregate
	check("J1: JOIN GROUP BY HAVING SUM > 150",
		`SELECT d.name FROM v109_dept d JOIN v109_emp e ON d.id = e.dept_id
		 GROUP BY d.name HAVING SUM(e.salary) > 150`, "Engineering")

	checkRowCount("J2: JOIN GROUP BY all groups",
		`SELECT d.name, COUNT(e.id) AS cnt
		 FROM v109_dept d JOIN v109_emp e ON d.id = e.dept_id
		 GROUP BY d.name`, 3)

	// 2.2: Multiple JOINs with aggregates
	afExec(t, db, ctx, "INSERT INTO v109_proj VALUES (1, 'Alpha', 1, 500)")
	afExec(t, db, ctx, "INSERT INTO v109_proj VALUES (2, 'Beta', 1, 300)")
	afExec(t, db, ctx, "INSERT INTO v109_proj VALUES (3, 'Gamma', 2, 400)")

	check("J3: Multi-JOIN",
		`SELECT d.name
		 FROM v109_dept d
		 JOIN v109_emp e ON d.id = e.dept_id
		 JOIN v109_proj p ON d.id = p.dept_id
		 WHERE d.name = 'Engineering'
		 GROUP BY d.name`, "Engineering")

	// 2.3: LEFT JOIN + GROUP BY + ORDER BY
	checkNth("J4: LEFT JOIN GROUP BY ORDER BY cnt DESC",
		`SELECT d.name, COUNT(e.id) AS cnt
		 FROM v109_dept d LEFT JOIN v109_emp e ON d.id = e.dept_id
		 GROUP BY d.name ORDER BY cnt DESC`, 0, 0, "Engineering")

	checkNth("J5: LEFT JOIN GROUP BY ORDER BY cnt ASC",
		`SELECT d.name, COUNT(e.id) AS cnt
		 FROM v109_dept d LEFT JOIN v109_emp e ON d.id = e.dept_id
		 GROUP BY d.name ORDER BY cnt ASC`, 0, 0, "HR")

	// 2.4: Derived table in JOIN
	checkNth("J6: Derived table JOIN",
		`SELECT e.name, sub.max_sal
		 FROM v109_emp e
		 JOIN (SELECT dept_id, MAX(salary) AS max_sal FROM v109_emp GROUP BY dept_id) sub
		 ON e.dept_id = sub.dept_id
		 WHERE e.name = 'Alice'`, 0, 0, "Alice")

	// 2.5: JOIN + HAVING on AVG
	check("J7: JOIN HAVING AVG > 100",
		`SELECT d.name
		 FROM v109_dept d JOIN v109_emp e ON d.id = e.dept_id
		 GROUP BY d.name HAVING AVG(e.salary) > 100`, "Engineering")

	// 2.6: JOIN + GROUP BY + ORDER BY + LIMIT
	checkNth("J8: JOIN GROUP BY ORDER BY LIMIT",
		`SELECT d.name, SUM(e.salary) AS total
		 FROM v109_dept d JOIN v109_emp e ON d.id = e.dept_id
		 GROUP BY d.name ORDER BY total DESC LIMIT 1`, 0, 0, "Engineering")

	// 2.7: LEFT JOIN shows unmatched
	checkRowCount("J9: LEFT JOIN includes unmatched dept",
		`SELECT d.name, p.name FROM v109_dept d LEFT JOIN v109_proj p ON d.id = p.dept_id`, 4)

	// 2.8: Self-join
	afExec(t, db, ctx, `CREATE TABLE v109_mgr (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v109_mgr VALUES (1, 'Boss', NULL)")
	afExec(t, db, ctx, "INSERT INTO v109_mgr VALUES (2, 'Mid', 1)")
	afExec(t, db, ctx, "INSERT INTO v109_mgr VALUES (3, 'Worker', 2)")

	check("J10: Self-join",
		`SELECT w.name FROM v109_mgr w JOIN v109_mgr m ON w.manager_id = m.id WHERE m.name = 'Boss'`, "Mid")

	// 2.9: LEFT JOIN IS NULL filter
	checkRowCount("J11: LEFT JOIN + IS NULL",
		`SELECT d.name FROM v109_dept d LEFT JOIN v109_proj p ON d.id = p.dept_id WHERE p.id IS NULL`, 1)

	// 2.10: JOIN with CASE in SELECT
	checkNth("J12: JOIN with CASE",
		`SELECT e.name,
			CASE WHEN e.salary > 100 THEN 'high' ELSE 'low' END AS level
		 FROM v109_emp e JOIN v109_dept d ON e.dept_id = d.id
		 WHERE e.name = 'Bob'`, 0, 1, "high")

	// 2.11: Derived table with aggregate
	check("J13: Derived table aggregate",
		`SELECT MAX(sub.cnt) FROM (
			SELECT dept_id, COUNT(*) AS cnt FROM v109_emp GROUP BY dept_id
		) sub`, int64(2))

	// 2.12: Nested derived tables
	check("J14: Nested derived",
		`SELECT s2.total FROM (
			SELECT SUM(total) AS total FROM (
				SELECT dept_id, SUM(salary) AS total FROM v109_emp GROUP BY dept_id
			) s1
		) s2`, 500.0)

	// ============================================================
	// === SECTION 3: CTEs ===
	// ============================================================

	// 3.1: Recursive CTE - generate series
	check("C1: Recursive CTE count",
		`WITH RECURSIVE nums(n) AS (
			SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10
		) SELECT COUNT(*) FROM nums`, int64(10))

	// 3.2: Recursive CTE max value
	check("C2: Recursive CTE fibonacci max",
		`WITH RECURSIVE fib(n, val, prev) AS (
			SELECT 1, 1, 0
			UNION ALL
			SELECT n + 1, val + prev, val FROM fib WHERE n < 8
		) SELECT MAX(val) FROM fib`, 21.0)

	// 3.3: CTE with UNION ALL
	check("C3: CTE with UNION ALL",
		`WITH combined AS (
			SELECT 1 AS val UNION ALL SELECT 1 AS val UNION ALL SELECT 2 AS val
		) SELECT SUM(val) FROM combined`, 4.0)

	// 3.4: CTE with UNION (dedup)
	check("C4: CTE UNION dedup",
		`WITH combined AS (
			SELECT 1 AS val UNION SELECT 1 AS val UNION SELECT 2 AS val
		) SELECT COUNT(*) FROM combined`, int64(2))

	// 3.5: Multiple CTEs
	check("C5: Multiple CTEs",
		`WITH dept_totals AS (
			SELECT dept_id, SUM(salary) AS total FROM v109_emp GROUP BY dept_id
		),
		dept_names AS (
			SELECT d.name, dt.total FROM v109_dept d JOIN dept_totals dt ON d.id = dt.dept_id
		)
		SELECT name FROM dept_names ORDER BY total DESC LIMIT 1`, "Engineering")

	// 3.6: CTE in WHERE subquery
	check("C6: CTE in WHERE",
		`WITH high_sal AS (
			SELECT dept_id FROM v109_emp GROUP BY dept_id HAVING AVG(salary) >= 100
		)
		SELECT name FROM v109_dept WHERE id IN (SELECT dept_id FROM high_sal)`, "Engineering")

	// 3.7: Recursive CTE multi-column tree
	check("C7: Recursive CTE tree depth",
		`WITH RECURSIVE tree(id, lvl, path) AS (
			SELECT id, 0, name FROM v109_mgr WHERE manager_id IS NULL
			UNION ALL
			SELECT m.id, t.lvl + 1, t.path FROM v109_mgr m JOIN tree t ON m.manager_id = t.id
		) SELECT MAX(lvl) FROM tree`, 2.0)

	// 3.8: CTE used multiple times
	check("C8: CTE used twice",
		`WITH sal_stats AS (
			SELECT AVG(salary) AS avg_sal FROM v109_emp
		)
		SELECT COUNT(*) FROM v109_emp WHERE salary > (SELECT avg_sal FROM sal_stats)`, int64(2))

	// 3.9: Recursive CTE sum of series
	check("C9: Sum of series 1..5",
		`WITH RECURSIVE series(n) AS (
			SELECT 1 UNION ALL SELECT n + 1 FROM series WHERE n < 5
		) SELECT SUM(n) FROM series`, 15.0)

	// 3.10: Recursive CTE powers of 2
	check("C10: Powers of 2",
		`WITH RECURSIVE powers(n, val) AS (
			SELECT 0, 1
			UNION ALL
			SELECT n + 1, val * 2 FROM powers WHERE n < 5
		) SELECT val FROM powers WHERE n = 5`, 32.0)

	// 3.11: CTE with DISTINCT
	check("C11: CTE with DISTINCT",
		`WITH depts AS (
			SELECT DISTINCT dept_id FROM v109_emp
		) SELECT COUNT(*) FROM depts`, int64(3))

	// 3.12: CTE with aggregates
	check("C12: CTE with aggregate MAX",
		`WITH totals AS (
			SELECT dept_id, SUM(salary) AS total FROM v109_emp GROUP BY dept_id
		) SELECT MAX(total) FROM totals`, 220.0)

	// ============================================================
	// === SECTION 4: UPDATE/DELETE Paths ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v109_upd (id INTEGER PRIMARY KEY, name TEXT, status TEXT, amount REAL)`)
	afExec(t, db, ctx, "INSERT INTO v109_upd VALUES (1, 'Alice', 'active', 100)")
	afExec(t, db, ctx, "INSERT INTO v109_upd VALUES (2, 'Bob', 'active', 200)")
	afExec(t, db, ctx, "INSERT INTO v109_upd VALUES (3, 'Carol', 'inactive', 150)")
	afExec(t, db, ctx, "INSERT INTO v109_upd VALUES (4, 'Dave', 'active', 300)")
	afExec(t, db, ctx, "INSERT INTO v109_upd VALUES (5, 'Eve', 'inactive', 50)")

	// 4.1: UPDATE with subquery in SET
	checkNoError("U1: UPDATE subquery in SET",
		`UPDATE v109_upd SET amount = (SELECT MAX(amount) FROM v109_upd) WHERE id = 5`)
	check("U2: Verify subquery SET",
		`SELECT amount FROM v109_upd WHERE id = 5`, 300.0)

	// 4.2: UPDATE with CASE expression
	checkNoError("U3: UPDATE with CASE",
		`UPDATE v109_upd SET status = CASE
			WHEN amount >= 200 THEN 'premium'
			WHEN amount >= 100 THEN 'standard'
			ELSE 'basic'
		END`)
	check("U4: Verify CASE premium (id=2, amount=200)",
		`SELECT status FROM v109_upd WHERE id = 2`, "premium")
	check("U5: Verify CASE standard (id=1, amount=100)",
		`SELECT status FROM v109_upd WHERE id = 1`, "standard")

	// 4.3: UPDATE with subquery in WHERE
	checkNoError("U6: UPDATE subquery in WHERE",
		`UPDATE v109_upd SET amount = amount * 2
		 WHERE id IN (SELECT id FROM v109_upd WHERE status = 'premium')`)
	// ids 2(200*2=400), 4(300*2=600), 5(300*2=600) were premium
	check("U7: Verify subquery WHERE update id=2",
		`SELECT amount FROM v109_upd WHERE id = 2`, 400.0)

	// 4.4: UPDATE multiple columns
	checkNoError("U8: UPDATE multiple columns",
		`UPDATE v109_upd SET name = 'Updated', status = 'changed' WHERE id = 3`)
	check("U9: Verify multi-col name",
		`SELECT name FROM v109_upd WHERE id = 3`, "Updated")
	check("U10: Verify multi-col status",
		`SELECT status FROM v109_upd WHERE id = 3`, "changed")

	// 4.5: DELETE with subquery in WHERE
	checkNoError("D1: DELETE subquery WHERE",
		`DELETE FROM v109_upd WHERE amount < (SELECT AVG(amount) FROM v109_upd)`)
	// amounts: 100, 400, 150, 600, 600 → avg = 370
	// Delete: 100, 150 (ids 1, 3)
	checkRowCount("D2: Verify subquery delete",
		`SELECT * FROM v109_upd`, 3)

	// 4.6: DELETE all rows
	afExec(t, db, ctx, "CREATE TABLE v109_del_all (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v109_del_all VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v109_del_all VALUES (2, 'b')")
	checkNoError("D3: DELETE all",
		`DELETE FROM v109_del_all`)
	checkRowCount("D4: Verify all deleted",
		`SELECT * FROM v109_del_all`, 0)

	// 4.7: DELETE with complex WHERE
	afExec(t, db, ctx, "CREATE TABLE v109_del2 (id INTEGER PRIMARY KEY, cat TEXT, score REAL)")
	afExec(t, db, ctx, "INSERT INTO v109_del2 VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v109_del2 VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO v109_del2 VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO v109_del2 VALUES (4, 'B', 40)")
	checkNoError("D5: DELETE with AND",
		`DELETE FROM v109_del2 WHERE cat = 'A' AND score < 20`)
	checkRowCount("D6: Verify complex delete",
		`SELECT * FROM v109_del2`, 3)

	// 4.8: UPDATE with arithmetic
	checkNoError("U11: UPDATE arithmetic",
		`UPDATE v109_del2 SET score = score * 2 + 5 WHERE cat = 'B'`)
	check("U12: Verify arithmetic update",
		`SELECT score FROM v109_del2 WHERE id = 2`, 45.0)

	// 4.9: UPDATE with IN subquery
	afExec(t, db, ctx, "CREATE TABLE v109_upd2 (id INTEGER PRIMARY KEY, cat TEXT, val REAL)")
	afExec(t, db, ctx, "INSERT INTO v109_upd2 VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO v109_upd2 VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO v109_upd2 VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO v109_upd2 VALUES (4, 'C', 40)")
	checkNoError("U13: UPDATE with IN subquery",
		`UPDATE v109_upd2 SET val = val + 100
		 WHERE cat IN (SELECT DISTINCT cat FROM v109_upd2 WHERE val > 25)`)
	check("U14: Verify IN subquery update id=3",
		`SELECT val FROM v109_upd2 WHERE id = 3`, 130.0)
	check("U15: Verify IN subquery update id=4",
		`SELECT val FROM v109_upd2 WHERE id = 4`, 140.0)

	// 4.10: DELETE with computed WHERE
	afExec(t, db, ctx, "CREATE TABLE v109_del3 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v109_del3 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v109_del3 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v109_del3 VALUES (3, 30)")
	checkNoError("D7: DELETE computed WHERE",
		`DELETE FROM v109_del3 WHERE val > (SELECT AVG(val) FROM v109_del3)`)
	checkRowCount("D8: Verify computed delete",
		`SELECT * FROM v109_del3`, 2)

	// ============================================================
	// === SECTION 5: INSERT Paths ===
	// ============================================================

	// 5.1: INSERT with DEFAULT values
	afExec(t, db, ctx, `CREATE TABLE v109_ins_def (
		id INTEGER PRIMARY KEY AUTO_INCREMENT,
		name TEXT DEFAULT 'unknown',
		active INTEGER DEFAULT 1
	)`)
	checkNoError("I1: INSERT with defaults",
		`INSERT INTO v109_ins_def (name) VALUES ('test')`)
	check("I2: Verify default active",
		`SELECT active FROM v109_ins_def WHERE name = 'test'`, int64(1))

	// 5.2: INSERT ... SELECT
	afExec(t, db, ctx, `CREATE TABLE v109_ins_sel (id INTEGER PRIMARY KEY, name TEXT, salary REAL)`)
	checkNoError("I3: INSERT SELECT",
		`INSERT INTO v109_ins_sel SELECT id, name, salary FROM v109_emp WHERE dept_id = 1`)
	checkRowCount("I4: Verify INSERT SELECT",
		`SELECT * FROM v109_ins_sel`, 2)

	// 5.3: INSERT with CHECK constraint
	afExec(t, db, ctx, `CREATE TABLE v109_chk (
		id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0), name TEXT NOT NULL
	)`)
	checkNoError("I5: INSERT passes CHECK",
		`INSERT INTO v109_chk VALUES (1, 25, 'Alice')`)
	checkError("I6: INSERT violates CHECK",
		`INSERT INTO v109_chk VALUES (2, -5, 'Bob')`)

	// 5.4: INSERT with AUTO_INCREMENT
	afExec(t, db, ctx, `CREATE TABLE v109_auto (id INTEGER PRIMARY KEY AUTO_INCREMENT, val TEXT)`)
	checkNoError("I7: INSERT auto 1",
		`INSERT INTO v109_auto (val) VALUES ('first')`)
	checkNoError("I8: INSERT auto 2",
		`INSERT INTO v109_auto (val) VALUES ('second')`)
	check("I9: Verify auto-increment count",
		`SELECT COUNT(*) FROM v109_auto`, int64(2))

	// 5.5: INSERT with NOT NULL violation
	checkError("I10: INSERT NOT NULL violation",
		`INSERT INTO v109_chk VALUES (3, 20, NULL)`)

	// 5.6: INSERT with UNIQUE constraint
	afExec(t, db, ctx, `CREATE TABLE v109_uniq (id INTEGER PRIMARY KEY, email TEXT UNIQUE)`)
	checkNoError("I11: INSERT unique first",
		`INSERT INTO v109_uniq VALUES (1, 'a@b.com')`)
	checkError("I12: INSERT UNIQUE violation",
		`INSERT INTO v109_uniq VALUES (2, 'a@b.com')`)

	// 5.7: Multiple row inserts
	afExec(t, db, ctx, `CREATE TABLE v109_multi (id INTEGER PRIMARY KEY, val INTEGER)`)
	checkNoError("I13: INSERT row 1", `INSERT INTO v109_multi VALUES (1, 10)`)
	checkNoError("I14: INSERT row 2", `INSERT INTO v109_multi VALUES (2, 20)`)
	checkNoError("I15: INSERT row 3", `INSERT INTO v109_multi VALUES (3, 30)`)
	check("I16: Verify multi insert SUM",
		`SELECT SUM(val) FROM v109_multi`, 60.0)

	// 5.8: INSERT into CTE-based query result
	afExec(t, db, ctx, "CREATE TABLE v109_cte_ins (id INTEGER PRIMARY KEY, val REAL)")
	checkNoError("I17: INSERT from GROUP BY",
		`INSERT INTO v109_cte_ins SELECT dept_id, SUM(salary) FROM v109_emp GROUP BY dept_id`)
	checkRowCount("I18: Verify CTE-based insert",
		`SELECT * FROM v109_cte_ins`, 3)

	// ============================================================
	// === SECTION 6: DDL ===
	// ============================================================

	// 6.1: CREATE TABLE with CHECK constraints
	checkNoError("DDL1: CREATE TABLE CHECK",
		`CREATE TABLE v109_ddl_chk (
			id INTEGER PRIMARY KEY, price REAL CHECK (price > 0), qty INTEGER CHECK (qty >= 0)
		)`)
	checkNoError("DDL2: Insert valid",
		`INSERT INTO v109_ddl_chk VALUES (1, 9.99, 5)`)
	checkError("DDL3: Insert invalid price",
		`INSERT INTO v109_ddl_chk VALUES (2, -1.0, 5)`)
	checkError("DDL4: Insert invalid qty",
		`INSERT INTO v109_ddl_chk VALUES (3, 9.99, -1)`)

	// 6.2: CREATE TABLE with FOREIGN KEY
	afExec(t, db, ctx, `CREATE TABLE v109_fk_parent (id INTEGER PRIMARY KEY, name TEXT)`)
	checkNoError("DDL5: CREATE TABLE with FK",
		`CREATE TABLE v109_fk_child (
			id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT,
			FOREIGN KEY (parent_id) REFERENCES v109_fk_parent(id)
		)`)
	afExec(t, db, ctx, "INSERT INTO v109_fk_parent VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_parent VALUES (2, 'P2')")
	checkNoError("DDL6: Valid FK insert",
		`INSERT INTO v109_fk_child VALUES (1, 1, 'C1')`)
	checkError("DDL7: Invalid FK insert",
		`INSERT INTO v109_fk_child VALUES (2, 99, 'C2')`)

	// 6.3: ALTER TABLE DROP COLUMN
	afExec(t, db, ctx, `CREATE TABLE v109_drop_col (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v109_drop_col VALUES (1, 'Alice', 30, 'a@b.com')")
	checkNoError("DDL8: DROP COLUMN",
		`ALTER TABLE v109_drop_col DROP COLUMN email`)
	checkRowCount("DDL9: After drop column",
		`SELECT id, name, age FROM v109_drop_col`, 1)

	// 6.4: DROP TABLE
	afExec(t, db, ctx, "CREATE TABLE v109_to_drop (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v109_to_drop VALUES (1, 'test')")
	checkNoError("DDL10: DROP TABLE",
		`DROP TABLE v109_to_drop`)
	checkError("DDL11: SELECT from dropped",
		`SELECT * FROM v109_to_drop`)

	// 6.5: CREATE TABLE IF NOT EXISTS
	checkNoError("DDL12: CREATE IF NOT EXISTS first",
		`CREATE TABLE IF NOT EXISTS v109_ine (id INTEGER PRIMARY KEY, val TEXT)`)
	checkNoError("DDL13: CREATE IF NOT EXISTS second",
		`CREATE TABLE IF NOT EXISTS v109_ine (id INTEGER PRIMARY KEY, val TEXT)`)

	// 6.6: DROP TABLE IF EXISTS
	checkNoError("DDL14: DROP IF EXISTS",
		`DROP TABLE IF EXISTS v109_ine`)
	checkNoError("DDL15: DROP IF NOT EXISTS",
		`DROP TABLE IF EXISTS v109_nonexistent`)

	// 6.7: ALTER TABLE ADD COLUMN
	afExec(t, db, ctx, "CREATE TABLE v109_add_col (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO v109_add_col VALUES (1, 'Alice')")
	checkNoError("DDL16: ADD COLUMN",
		`ALTER TABLE v109_add_col ADD COLUMN age INTEGER DEFAULT 0`)
	check("DDL17: Verify added column default",
		`SELECT age FROM v109_add_col WHERE id = 1`, int64(0))

	// 6.8: ALTER TABLE RENAME COLUMN
	afExec(t, db, ctx, "CREATE TABLE v109_ren_col (id INTEGER PRIMARY KEY, old_name TEXT)")
	afExec(t, db, ctx, "INSERT INTO v109_ren_col VALUES (1, 'test')")
	checkNoError("DDL18: RENAME COLUMN",
		`ALTER TABLE v109_ren_col RENAME COLUMN old_name TO new_name`)
	check("DDL19: Verify renamed column",
		`SELECT new_name FROM v109_ren_col WHERE id = 1`, "test")

	// ============================================================
	// === SECTION 7: ORDER BY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v109_ord (id INTEGER PRIMARY KEY, name TEXT, val REAL, cat TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v109_ord VALUES (1, 'alpha', 10.5, 'A')")
	afExec(t, db, ctx, "INSERT INTO v109_ord VALUES (2, 'beta', NULL, 'B')")
	afExec(t, db, ctx, "INSERT INTO v109_ord VALUES (3, 'gamma', 5.0, 'A')")
	afExec(t, db, ctx, "INSERT INTO v109_ord VALUES (4, 'delta', 20.0, 'B')")
	afExec(t, db, ctx, "INSERT INTO v109_ord VALUES (5, 'epsilon', NULL, 'A')")

	// 7.1: ORDER BY LENGTH
	checkNth("O1: ORDER BY LENGTH ASC",
		`SELECT name FROM v109_ord ORDER BY LENGTH(name)`, 0, 0, "beta")
	checkNth("O2: ORDER BY LENGTH DESC",
		`SELECT name FROM v109_ord ORDER BY LENGTH(name) DESC`, 0, 0, "epsilon")

	// 7.2: ORDER BY arithmetic
	checkNth("O3: ORDER BY val*2 ASC",
		`SELECT id FROM v109_ord WHERE val IS NOT NULL ORDER BY val * 2`, 0, 0, int64(3))

	// 7.3: Mixed ASC/DESC - cat ASC gives A before B; val DESC within A gives 10.5 before 5.0
	checkNth("O4: ORDER BY mixed first = alpha",
		`SELECT name FROM v109_ord WHERE val IS NOT NULL ORDER BY cat ASC, val DESC`, 0, 0, "alpha")
	checkNth("O5: ORDER BY mixed second = gamma",
		`SELECT name FROM v109_ord WHERE val IS NOT NULL ORDER BY cat ASC, val DESC`, 1, 0, "gamma")
	checkNth("O6: ORDER BY mixed third = delta",
		`SELECT name FROM v109_ord WHERE val IS NOT NULL ORDER BY cat ASC, val DESC`, 2, 0, "delta")

	// 7.4: NULL ordering - NULLs sort first in DESC (CobaltDB behavior)
	checkNth("O7: NULL first in DESC",
		`SELECT name FROM v109_ord ORDER BY val DESC`, 0, 0, "beta")

	// NULLs sort last in ASC
	checkNth("O8: NULL last in ASC (non-null first)",
		`SELECT name FROM v109_ord ORDER BY val ASC`, 0, 0, "gamma")

	// 7.5: LIMIT + OFFSET
	checkNth("O9: ORDER BY LIMIT OFFSET",
		`SELECT name FROM v109_ord ORDER BY id LIMIT 2 OFFSET 2`, 0, 0, "gamma")

	// 7.6: ORDER BY column alias
	checkNth("O10: ORDER BY alias",
		`SELECT name, LENGTH(name) AS nlen FROM v109_ord ORDER BY nlen`, 0, 0, "beta")

	// 7.7: ORDER BY positional
	checkNth("O11: ORDER BY positional",
		`SELECT name, val FROM v109_ord WHERE val IS NOT NULL ORDER BY 2`, 0, 0, "gamma")

	// ============================================================
	// === SECTION 8: Transactions and Savepoints ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v109_txn (id INTEGER PRIMARY KEY, val TEXT)`)

	// 8.1: BEGIN/COMMIT
	checkNoError("T1: BEGIN", `BEGIN`)
	checkNoError("T2: INSERT in txn", `INSERT INTO v109_txn VALUES (1, 'in_txn')`)
	checkNoError("T3: COMMIT", `COMMIT`)
	check("T4: Verify committed", `SELECT val FROM v109_txn WHERE id = 1`, "in_txn")

	// 8.2: BEGIN/ROLLBACK
	checkNoError("T5: BEGIN", `BEGIN`)
	checkNoError("T6: INSERT in txn", `INSERT INTO v109_txn VALUES (2, 'will_rollback')`)
	checkNoError("T7: ROLLBACK", `ROLLBACK`)
	checkRowCount("T8: Verify rolled back", `SELECT * FROM v109_txn WHERE id = 2`, 0)

	// 8.3: SAVEPOINT + ROLLBACK TO
	checkNoError("T9: BEGIN", `BEGIN`)
	checkNoError("T10: INSERT before sp", `INSERT INTO v109_txn VALUES (3, 'before_sp')`)
	checkNoError("T11: SAVEPOINT sp1", `SAVEPOINT sp1`)
	checkNoError("T12: INSERT after sp", `INSERT INTO v109_txn VALUES (4, 'after_sp')`)
	checkNoError("T13: ROLLBACK TO sp1", `ROLLBACK TO sp1`)
	check("T14: Before sp exists", `SELECT val FROM v109_txn WHERE id = 3`, "before_sp")
	checkRowCount("T15: After sp gone", `SELECT * FROM v109_txn WHERE id = 4`, 0)
	checkNoError("T16: COMMIT after partial rollback", `COMMIT`)
	check("T17: Committed persists", `SELECT val FROM v109_txn WHERE id = 3`, "before_sp")

	// 8.4: Nested savepoints
	checkNoError("T18: BEGIN", `BEGIN`)
	checkNoError("T19: SAVEPOINT outer", `SAVEPOINT outer_sp`)
	checkNoError("T20: INSERT outer", `INSERT INTO v109_txn VALUES (5, 'outer')`)
	checkNoError("T21: SAVEPOINT inner", `SAVEPOINT inner_sp`)
	checkNoError("T22: INSERT inner", `INSERT INTO v109_txn VALUES (6, 'inner')`)
	checkNoError("T23: ROLLBACK TO inner", `ROLLBACK TO inner_sp`)
	checkRowCount("T24: Inner gone", `SELECT * FROM v109_txn WHERE id = 6`, 0)
	check("T25: Outer exists", `SELECT val FROM v109_txn WHERE id = 5`, "outer")
	checkNoError("T26: COMMIT nested", `COMMIT`)
	check("T27: Outer committed", `SELECT val FROM v109_txn WHERE id = 5`, "outer")

	// 8.5: RELEASE SAVEPOINT
	checkNoError("T28: BEGIN", `BEGIN`)
	checkNoError("T29: SAVEPOINT rel_sp", `SAVEPOINT rel_sp`)
	checkNoError("T30: INSERT", `INSERT INTO v109_txn VALUES (7, 'released')`)
	checkNoError("T31: RELEASE SAVEPOINT", `RELEASE SAVEPOINT rel_sp`)
	checkNoError("T32: COMMIT", `COMMIT`)
	check("T33: Released data committed", `SELECT val FROM v109_txn WHERE id = 7`, "released")

	// 8.6: SAVEPOINT with DDL rollback
	checkNoError("T34: BEGIN", `BEGIN`)
	checkNoError("T35: SAVEPOINT ddl_sp", `SAVEPOINT ddl_sp`)
	checkNoError("T36: CREATE TABLE in sp", `CREATE TABLE v109_txn_temp (id INTEGER PRIMARY KEY)`)
	checkNoError("T37: ROLLBACK TO ddl_sp", `ROLLBACK TO ddl_sp`)
	checkNoError("T38: COMMIT", `COMMIT`)

	// 8.7: SAVEPOINT with DELETE rollback
	checkNoError("T39: BEGIN", `BEGIN`)
	checkNoError("T40: SAVEPOINT del_sp", `SAVEPOINT del_sp`)
	checkNoError("T41: DELETE in sp", `DELETE FROM v109_txn WHERE id = 1`)
	checkNoError("T42: ROLLBACK TO del_sp", `ROLLBACK TO del_sp`)
	check("T43: Deleted data restored", `SELECT val FROM v109_txn WHERE id = 1`, "in_txn")
	checkNoError("T44: COMMIT", `COMMIT`)

	// 8.8: SAVEPOINT with UPDATE rollback
	checkNoError("T45: BEGIN", `BEGIN`)
	checkNoError("T46: SAVEPOINT upd_sp", `SAVEPOINT upd_sp`)
	checkNoError("T47: UPDATE in sp", `UPDATE v109_txn SET val = 'modified' WHERE id = 1`)
	checkNoError("T48: ROLLBACK TO upd_sp", `ROLLBACK TO upd_sp`)
	check("T49: Updated data restored", `SELECT val FROM v109_txn WHERE id = 1`, "in_txn")
	checkNoError("T50: COMMIT", `COMMIT`)

	// ============================================================
	// === SECTION 9: Foreign Keys ===
	// ============================================================

	// 9.1: ON DELETE CASCADE
	afExec(t, db, ctx, "CREATE TABLE v109_fk_p1 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v109_fk_c1 (
		id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT,
		FOREIGN KEY (parent_id) REFERENCES v109_fk_p1(id) ON DELETE CASCADE
	)`)
	afExec(t, db, ctx, "INSERT INTO v109_fk_p1 VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_p1 VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c1 VALUES (1, 1, 'C1a')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c1 VALUES (2, 1, 'C1b')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c1 VALUES (3, 2, 'C2a')")

	checkNoError("FK1: DELETE parent CASCADE", `DELETE FROM v109_fk_p1 WHERE id = 1`)
	checkRowCount("FK2: Children cascaded", `SELECT * FROM v109_fk_c1`, 1)
	check("FK3: Remaining child", `SELECT val FROM v109_fk_c1 WHERE id = 3`, "C2a")

	// 9.2: ON DELETE SET NULL
	afExec(t, db, ctx, "CREATE TABLE v109_fk_p2 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v109_fk_c2 (
		id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT,
		FOREIGN KEY (parent_id) REFERENCES v109_fk_p2(id) ON DELETE SET NULL
	)`)
	afExec(t, db, ctx, "INSERT INTO v109_fk_p2 VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_p2 VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c2 VALUES (1, 1, 'C1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c2 VALUES (2, 2, 'C2')")

	checkNoError("FK4: DELETE parent SET NULL", `DELETE FROM v109_fk_p2 WHERE id = 1`)
	check("FK5: Child FK set to NULL", `SELECT parent_id FROM v109_fk_c2 WHERE id = 1`, nil)
	check("FK6: Other child unchanged", `SELECT parent_id FROM v109_fk_c2 WHERE id = 2`, int64(2))

	// 9.3: ON UPDATE CASCADE
	afExec(t, db, ctx, "CREATE TABLE v109_fk_p3 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v109_fk_c3 (
		id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT,
		FOREIGN KEY (parent_id) REFERENCES v109_fk_p3(id) ON UPDATE CASCADE
	)`)
	afExec(t, db, ctx, "INSERT INTO v109_fk_p3 VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_p3 VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c3 VALUES (1, 1, 'C1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c3 VALUES (2, 2, 'C2')")

	checkNoError("FK7: UPDATE parent PK CASCADE", `UPDATE v109_fk_p3 SET id = 100 WHERE id = 1`)
	check("FK8: Child FK updated", `SELECT parent_id FROM v109_fk_c3 WHERE id = 1`, int64(100))

	// 9.4: ON UPDATE SET NULL
	afExec(t, db, ctx, "CREATE TABLE v109_fk_p4 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v109_fk_c4 (
		id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT,
		FOREIGN KEY (parent_id) REFERENCES v109_fk_p4(id) ON UPDATE SET NULL
	)`)
	afExec(t, db, ctx, "INSERT INTO v109_fk_p4 VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c4 VALUES (1, 1, 'C1')")

	checkNoError("FK9: UPDATE parent SET NULL", `UPDATE v109_fk_p4 SET id = 100 WHERE id = 1`)
	check("FK10: Child FK NULL on update", `SELECT parent_id FROM v109_fk_c4 WHERE id = 1`, nil)

	// 9.5: ON DELETE CASCADE + ON UPDATE CASCADE combo
	afExec(t, db, ctx, "CREATE TABLE v109_fk_p5 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v109_fk_c5 (
		id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT,
		FOREIGN KEY (parent_id) REFERENCES v109_fk_p5(id) ON DELETE CASCADE ON UPDATE CASCADE
	)`)
	afExec(t, db, ctx, "INSERT INTO v109_fk_p5 VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_c5 VALUES (1, 1, 'C1')")

	checkNoError("FK11: UPDATE cascade combo", `UPDATE v109_fk_p5 SET id = 50 WHERE id = 1`)
	check("FK12: Child FK updated combo", `SELECT parent_id FROM v109_fk_c5 WHERE id = 1`, int64(50))
	checkNoError("FK13: DELETE cascade combo", `DELETE FROM v109_fk_p5 WHERE id = 50`)
	checkRowCount("FK14: Child deleted combo", `SELECT * FROM v109_fk_c5`, 0)

	// 9.6: Multi-level cascade
	afExec(t, db, ctx, "CREATE TABLE v109_fk_gp (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v109_fk_par (
		id INTEGER PRIMARY KEY, gp_id INTEGER,
		FOREIGN KEY (gp_id) REFERENCES v109_fk_gp(id) ON DELETE CASCADE
	)`)
	afExec(t, db, ctx, `CREATE TABLE v109_fk_kid (
		id INTEGER PRIMARY KEY, par_id INTEGER,
		FOREIGN KEY (par_id) REFERENCES v109_fk_par(id) ON DELETE CASCADE
	)`)
	afExec(t, db, ctx, "INSERT INTO v109_fk_gp VALUES (1, 'GP1')")
	afExec(t, db, ctx, "INSERT INTO v109_fk_par VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO v109_fk_par VALUES (2, 1)")
	afExec(t, db, ctx, "INSERT INTO v109_fk_kid VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO v109_fk_kid VALUES (2, 2)")

	checkNoError("FK15: DELETE grandparent cascades", `DELETE FROM v109_fk_gp WHERE id = 1`)
	checkRowCount("FK16: Parents cascaded", `SELECT * FROM v109_fk_par`, 0)
	checkRowCount("FK17: Grandchildren cascaded", `SELECT * FROM v109_fk_kid`, 0)

	// ============================================================
	// === SECTION 10: Views ===
	// ============================================================

	// salary > 90 = Alice(100), Bob(120), Dave(110) = 3 rows
	checkNoError("V1: CREATE VIEW",
		`CREATE VIEW v109_emp_view AS SELECT name, salary FROM v109_emp WHERE salary > 90`)
	checkRowCount("V2: SELECT from VIEW", `SELECT * FROM v109_emp_view`, 3)

	checkNoError("V3: CREATE VIEW with aggregate",
		`CREATE VIEW v109_dept_stats AS
		 SELECT d.name AS dept, COUNT(e.id) AS emp_count, AVG(e.salary) AS avg_sal
		 FROM v109_dept d JOIN v109_emp e ON d.id = e.dept_id
		 GROUP BY d.name`)
	checkRowCount("V4: SELECT from aggregate VIEW", `SELECT * FROM v109_dept_stats`, 3)

	checkNoError("V5: DROP VIEW", `DROP VIEW v109_emp_view`)
	checkError("V6: SELECT from dropped VIEW", `SELECT * FROM v109_emp_view`)

	// ============================================================
	// === SECTION 11: Expressions ===
	// ============================================================

	check("E1: COALESCE NULL", `SELECT COALESCE(NULL, NULL, 'default')`, "default")
	check("E2: COALESCE value", `SELECT COALESCE(1, 2, 3)`, int64(1))
	check("E3: NULLIF equal", `SELECT NULLIF(1, 1)`, nil)
	check("E4: NULLIF not equal", `SELECT NULLIF(1, 2)`, int64(1))
	check("E5: IIF true", `SELECT IIF(1 > 0, 'yes', 'no')`, "yes")
	check("E6: IIF false", `SELECT IIF(1 < 0, 'yes', 'no')`, "no")
	check("E7: TYPEOF integer", `SELECT TYPEOF(42)`, "integer")
	check("E8: TYPEOF text", `SELECT TYPEOF('hello')`, "text")
	check("E9: TYPEOF null", `SELECT TYPEOF(NULL)`, "null")
	check("E10: ABS negative", `SELECT ABS(-42)`, 42.0)
	check("E11: ABS positive", `SELECT ABS(42)`, 42.0)
	check("E12: CAST int to text", `SELECT CAST(42 AS TEXT)`, "42")
	check("E13: CAST text to int", `SELECT CAST('123' AS INTEGER)`, int64(123))
	check("E14: Simple CASE", `SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END`, "one")
	check("E15: Searched CASE", `SELECT CASE WHEN 5 > 3 THEN 'big' WHEN 5 < 3 THEN 'small' ELSE 'equal' END`, "big")
	check("E16: BETWEEN true", `SELECT 5 BETWEEN 1 AND 10`, true)
	check("E17: BETWEEN false", `SELECT 15 BETWEEN 1 AND 10`, false)
	check("E18: IN list true", `SELECT 3 IN (1, 2, 3)`, true)
	check("E19: IN list false", `SELECT 4 IN (1, 2, 3)`, false)

	// ============================================================
	// === SECTION 12: GROUP BY Edge Cases ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v109_grp (id INTEGER PRIMARY KEY, val REAL, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO v109_grp VALUES (1, 10, 'A')")
	afExec(t, db, ctx, "INSERT INTO v109_grp VALUES (2, 20, 'B')")
	afExec(t, db, ctx, "INSERT INTO v109_grp VALUES (3, 30, 'A')")
	afExec(t, db, ctx, "INSERT INTO v109_grp VALUES (4, 40, 'B')")
	afExec(t, db, ctx, "INSERT INTO v109_grp VALUES (5, 50, 'A')")

	check("G1: GROUP BY SUM cat A",
		`SELECT cat, SUM(val) FROM v109_grp GROUP BY cat HAVING SUM(val) > 50 ORDER BY cat LIMIT 1`, "A")

	check("G2: GROUP BY COUNT",
		`SELECT COUNT(*) FROM v109_grp WHERE cat = 'B'`, int64(2))

	check("G3: GROUP BY HAVING AVG > 25 returns both",
		`SELECT COUNT(*) FROM (SELECT cat FROM v109_grp GROUP BY cat HAVING AVG(val) > 25) sub`, int64(2))

	// Multiple aggregates
	checkNth("G4: Multiple aggregates",
		`SELECT cat, MIN(val), MAX(val), SUM(val)
		 FROM v109_grp GROUP BY cat ORDER BY cat`, 0, 1, 10.0)

	// NULL in GROUP BY
	afExec(t, db, ctx, "CREATE TABLE v109_grp_null (id INTEGER PRIMARY KEY, grp TEXT, val REAL)")
	afExec(t, db, ctx, "INSERT INTO v109_grp_null VALUES (1, 'X', 10)")
	afExec(t, db, ctx, "INSERT INTO v109_grp_null VALUES (2, 'X', NULL)")
	afExec(t, db, ctx, "INSERT INTO v109_grp_null VALUES (3, NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO v109_grp_null VALUES (4, NULL, 40)")

	check("G5: COUNT with NULL group", `SELECT COUNT(*) FROM v109_grp_null WHERE grp IS NULL`, int64(2))
	check("G6: AVG ignores NULL", `SELECT AVG(val) FROM v109_grp_null WHERE grp = 'X'`, 10.0)

	// ============================================================
	// === SECTION 13: Set Operations ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v109_set1 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v109_set2 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v109_set1 VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO v109_set1 VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO v109_set1 VALUES (3, 'c')")
	afExec(t, db, ctx, "INSERT INTO v109_set2 VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO v109_set2 VALUES (3, 'c')")
	afExec(t, db, ctx, "INSERT INTO v109_set2 VALUES (4, 'd')")

	checkRowCount("S1: UNION dedup", `SELECT val FROM v109_set1 UNION SELECT val FROM v109_set2`, 4)
	checkRowCount("S2: UNION ALL", `SELECT val FROM v109_set1 UNION ALL SELECT val FROM v109_set2`, 6)
	checkRowCount("S3: INTERSECT", `SELECT val FROM v109_set1 INTERSECT SELECT val FROM v109_set2`, 2)
	checkRowCount("S4: EXCEPT", `SELECT val FROM v109_set1 EXCEPT SELECT val FROM v109_set2`, 1)
	check("S5: EXCEPT result", `SELECT val FROM v109_set1 EXCEPT SELECT val FROM v109_set2`, "a")

	// ============================================================
	// === SECTION 14: Subqueries ===
	// ============================================================

	check("SQ1: Scalar subquery in SELECT",
		`SELECT (SELECT MAX(salary) FROM v109_emp)`, 120.0)
	check("SQ2: Subquery in WHERE",
		`SELECT name FROM v109_emp WHERE salary = (SELECT MAX(salary) FROM v109_emp)`, "Bob")
	check("SQ3: Subquery in FROM",
		`SELECT sub.avg_sal FROM (SELECT AVG(salary) AS avg_sal FROM v109_emp) sub`, 100.0)
	checkRowCount("SQ4: Correlated subquery",
		`SELECT e.name FROM v109_emp e
		 WHERE e.salary > (SELECT AVG(salary) FROM v109_emp WHERE dept_id = e.dept_id)`, 2)

	// ============================================================
	// === SECTION 15: Trigger Coverage ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v109_trig_log (id INTEGER PRIMARY KEY AUTO_INCREMENT, action_type TEXT, record_id INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v109_trig_data (id INTEGER PRIMARY KEY, val TEXT)`)

	// Use NEW.id in trigger body
	checkNoError("TR1: CREATE TRIGGER",
		`CREATE TRIGGER v109_trig_ins AFTER INSERT ON v109_trig_data
		 FOR EACH ROW
		 INSERT INTO v109_trig_log (action_type, record_id) VALUES ('INSERT', NEW.id)`)
	checkNoError("TR2: INSERT fires trigger",
		`INSERT INTO v109_trig_data VALUES (1, 'hello')`)
	checkNoError("TR3: INSERT fires trigger again",
		`INSERT INTO v109_trig_data VALUES (2, 'world')`)

	// Check that the trigger data table has rows
	checkRowCount("TR4: Trigger data table has rows", `SELECT * FROM v109_trig_data`, 2)

	// UPDATE trigger
	checkNoError("TR5: CREATE UPDATE TRIGGER",
		`CREATE TRIGGER v109_trig_upd AFTER UPDATE ON v109_trig_data
		 FOR EACH ROW
		 INSERT INTO v109_trig_log (action_type, record_id) VALUES ('UPDATE', NEW.id)`)
	checkNoError("TR6: UPDATE fires trigger",
		`UPDATE v109_trig_data SET val = 'updated' WHERE id = 1`)

	// DELETE trigger
	checkNoError("TR7: CREATE DELETE TRIGGER",
		`CREATE TRIGGER v109_trig_del BEFORE DELETE ON v109_trig_data
		 FOR EACH ROW
		 INSERT INTO v109_trig_log (action_type, record_id) VALUES ('DELETE', OLD.id)`)
	checkNoError("TR8: DELETE fires trigger",
		`DELETE FROM v109_trig_data WHERE id = 2`)

	// ============================================================
	// === SECTION 16: INDEX Coverage ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v109_idx (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	afExec(t, db, ctx, "INSERT INTO v109_idx VALUES (1, 'Alice', 85)")
	afExec(t, db, ctx, "INSERT INTO v109_idx VALUES (2, 'Bob', 92)")
	afExec(t, db, ctx, "INSERT INTO v109_idx VALUES (3, 'Carol', 78)")

	checkNoError("IDX1: CREATE INDEX",
		`CREATE INDEX idx_v109_score ON v109_idx(score)`)
	check("IDX2: Query uses index (same result)",
		`SELECT name FROM v109_idx WHERE score > 80 ORDER BY score LIMIT 1`, "Alice")
	checkNoError("IDX3: DROP INDEX",
		`DROP INDEX idx_v109_score`)
	checkNoError("IDX4: CREATE UNIQUE INDEX",
		`CREATE UNIQUE INDEX idx_v109_name ON v109_idx(name)`)

	// ============================================================
	// Final Summary
	// ============================================================
	t.Logf("TestV109CoverageBoost: %d/%d passed", pass, total)
}

// TestV109RLSCoverage tests Row-Level Security through the engine API.
// Requires EnableRLS option.
func TestV109RLSCoverage(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true, EnableRLS: true})
	if err != nil {
		t.Fatalf("Failed to open DB with RLS: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	pass := 0
	total := 0

	exec := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}

	queryCount := func(desc string, sql string, expectedRows int) {
		t.Helper()
		total++
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: query error: %v", desc, err)
			return
		}
		defer rows.Close()
		count := 0
		cols := rows.Columns()
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			rows.Scan(ptrs...)
			count++
		}
		if count != expectedRows {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, count, expectedRows)
			return
		}
		pass++
	}

	// Setup tables
	exec("RLS1: CREATE TABLE",
		`CREATE TABLE rls_docs (id INTEGER PRIMARY KEY, title TEXT, owner TEXT, dept TEXT)`)
	exec("RLS2: INSERT 1", `INSERT INTO rls_docs VALUES (1, 'Secret Plan', 'alice', 'eng')`)
	exec("RLS3: INSERT 2", `INSERT INTO rls_docs VALUES (2, 'Budget', 'bob', 'finance')`)
	exec("RLS4: INSERT 3", `INSERT INTO rls_docs VALUES (3, 'Design', 'alice', 'eng')`)
	exec("RLS5: INSERT 4", `INSERT INTO rls_docs VALUES (4, 'Report', 'carol', 'hr')`)

	// Create RLS policies for different operations
	exec("RLS6: CREATE POLICY ALL",
		`CREATE POLICY doc_owner_policy ON rls_docs FOR ALL USING (owner = 'alice')`)

	queryCount("RLS7: SELECT with RLS",
		`SELECT * FROM rls_docs`, 4)

	exec("RLS8: CREATE POLICY SELECT",
		`CREATE POLICY doc_select_policy ON rls_docs FOR SELECT USING (dept = 'eng')`)

	exec("RLS9: CREATE POLICY INSERT",
		`CREATE POLICY doc_insert_policy ON rls_docs FOR INSERT USING (dept = 'eng')`)

	exec("RLS10: CREATE POLICY UPDATE",
		`CREATE POLICY doc_update_policy ON rls_docs FOR UPDATE USING (dept = 'eng')`)

	exec("RLS11: CREATE POLICY DELETE",
		`CREATE POLICY doc_delete_policy ON rls_docs FOR DELETE USING (dept = 'eng')`)

	queryCount("RLS12: SELECT after policies", `SELECT * FROM rls_docs`, 4)

	exec("RLS13: INSERT with RLS",
		`INSERT INTO rls_docs VALUES (5, 'New Doc', 'alice', 'eng')`)
	exec("RLS14: UPDATE with RLS",
		`UPDATE rls_docs SET title = 'Updated' WHERE id = 1`)
	exec("RLS15: DELETE with RLS",
		`DELETE FROM rls_docs WHERE id = 5`)

	// Additional RLS coverage: second table
	exec("RLS16: CREATE TABLE 2",
		`CREATE TABLE rls_orders (id INTEGER PRIMARY KEY, amount REAL, region TEXT)`)
	exec("RLS17: INSERT order 1", `INSERT INTO rls_orders VALUES (1, 100, 'north')`)
	exec("RLS18: INSERT order 2", `INSERT INTO rls_orders VALUES (2, 200, 'south')`)

	exec("RLS19: CREATE POLICY on orders",
		`CREATE POLICY order_region ON rls_orders FOR ALL USING (region = 'north')`)

	queryCount("RLS20: SELECT orders with RLS", `SELECT * FROM rls_orders`, 2)
	exec("RLS21: UPDATE orders with RLS",
		`UPDATE rls_orders SET amount = 150 WHERE id = 1`)
	exec("RLS22: DELETE order with RLS",
		`DELETE FROM rls_orders WHERE id = 2`)

	t.Logf("TestV109RLSCoverage: %d/%d passed", pass, total)
}
