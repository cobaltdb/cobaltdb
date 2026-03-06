package test

import (
	"fmt"
	"testing"
)

func TestV82ComplexBugHunt(t *testing.T) {
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
	// === SECTION 1: COMPLEX UPDATE PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_emp (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER, dept TEXT, bonus INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v82_emp VALUES (1, 'Alice', 80000, 'Engineering', 0)")
	afExec(t, db, ctx, "INSERT INTO v82_emp VALUES (2, 'Bob', 90000, 'Engineering', 0)")
	afExec(t, db, ctx, "INSERT INTO v82_emp VALUES (3, 'Charlie', 70000, 'Marketing', 0)")
	afExec(t, db, ctx, "INSERT INTO v82_emp VALUES (4, 'Diana', 60000, 'Sales', 0)")
	afExec(t, db, ctx, "INSERT INTO v82_emp VALUES (5, 'Eve', 95000, 'Engineering', 0)")

	// UPDATE with CASE based on department
	checkNoError("UPDATE CASE dept",
		`UPDATE v82_emp SET bonus = CASE dept
			WHEN 'Engineering' THEN salary / 10
			WHEN 'Marketing' THEN salary / 20
			ELSE salary / 50
		END`)

	check("Bonus Engineering", "SELECT bonus FROM v82_emp WHERE id = 1", float64(8000))
	check("Bonus Marketing", "SELECT bonus FROM v82_emp WHERE id = 3", float64(3500))
	check("Bonus Sales", "SELECT bonus FROM v82_emp WHERE id = 4", float64(1200))

	// UPDATE with correlated subquery in WHERE
	checkNoError("UPDATE with correlated WHERE",
		`UPDATE v82_emp SET salary = salary + 5000
		 WHERE salary > (SELECT AVG(salary) FROM v82_emp e2 WHERE e2.dept = v82_emp.dept)`)

	// Alice(80000) < avg(88333), Bob(90000) > avg, Eve(95000) > avg -> Bob and Eve get +5000
	check("Bob salary after update", "SELECT salary FROM v82_emp WHERE id = 2", float64(95000))
	check("Eve salary after update", "SELECT salary FROM v82_emp WHERE id = 5", float64(100000))
	check("Alice salary unchanged", "SELECT salary FROM v82_emp WHERE id = 1", float64(80000))

	// UPDATE multiple columns at once
	checkNoError("UPDATE multi-column",
		"UPDATE v82_emp SET name = UPPER(name), bonus = bonus + 500 WHERE dept = 'Engineering'")
	check("Multi-col name", "SELECT name FROM v82_emp WHERE id = 1", "ALICE")
	check("Multi-col bonus", "SELECT bonus FROM v82_emp WHERE id = 1", float64(8500))

	// ============================================================
	// === SECTION 2: COMPLEX DELETE PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_log (id INTEGER PRIMARY KEY, event TEXT, ts INTEGER)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v82_log VALUES (%d, 'event_%d', %d)", i, i, i*100))
	}

	// DELETE with subquery in WHERE
	checkNoError("DELETE with subquery",
		"DELETE FROM v82_log WHERE ts < (SELECT AVG(ts) FROM v82_log)")
	// AVG(ts) = (100+200+...+2000)/20 = 21000/20 = 1050
	// Delete where ts < 1050: ts=100,200,...,1000 (10 rows)
	checkRowCount("After subquery delete", "SELECT * FROM v82_log", 10)

	// DELETE with IN subquery
	afExec(t, db, ctx, "CREATE TABLE v82_blacklist (ts INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v82_blacklist VALUES (1100)")
	afExec(t, db, ctx, "INSERT INTO v82_blacklist VALUES (1200)")

	checkNoError("DELETE IN subquery",
		"DELETE FROM v82_log WHERE ts IN (SELECT ts FROM v82_blacklist)")
	checkRowCount("After IN delete", "SELECT * FROM v82_log", 8)

	// DELETE with complex WHERE
	checkNoError("DELETE complex WHERE",
		"DELETE FROM v82_log WHERE ts > 1500 AND event LIKE '%_1%'")
	// ts > 1500: 1600,1700,1800,1900,2000
	// event LIKE '%_1%': event_16, event_17, event_18, event_19 match (contain '1')
	// So delete: ts=1600(event_16),1700(event_17),1800(event_18),1900(event_19) = 4 rows
	checkRowCount("After complex delete", "SELECT * FROM v82_log", 4)

	// ============================================================
	// === SECTION 3: COMPLEX JOIN WITH ALL CLAUSES ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_products (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER, price REAL)")
	afExec(t, db, ctx, "CREATE TABLE v82_categories (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v82_order_items (id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER)")

	afExec(t, db, ctx, "INSERT INTO v82_categories VALUES (1, 'Electronics')")
	afExec(t, db, ctx, "INSERT INTO v82_categories VALUES (2, 'Books')")
	afExec(t, db, ctx, "INSERT INTO v82_categories VALUES (3, 'Clothing')")

	afExec(t, db, ctx, "INSERT INTO v82_products VALUES (1, 'Laptop', 1, 999.99)")
	afExec(t, db, ctx, "INSERT INTO v82_products VALUES (2, 'Phone', 1, 599.99)")
	afExec(t, db, ctx, "INSERT INTO v82_products VALUES (3, 'Novel', 2, 14.99)")
	afExec(t, db, ctx, "INSERT INTO v82_products VALUES (4, 'Textbook', 2, 49.99)")
	afExec(t, db, ctx, "INSERT INTO v82_products VALUES (5, 'T-Shirt', 3, 19.99)")

	afExec(t, db, ctx, "INSERT INTO v82_order_items VALUES (1, 1, 2)")  // 2 Laptops
	afExec(t, db, ctx, "INSERT INTO v82_order_items VALUES (2, 2, 5)")  // 5 Phones
	afExec(t, db, ctx, "INSERT INTO v82_order_items VALUES (3, 3, 10)") // 10 Novels
	afExec(t, db, ctx, "INSERT INTO v82_order_items VALUES (4, 4, 3)")  // 3 Textbooks
	afExec(t, db, ctx, "INSERT INTO v82_order_items VALUES (5, 1, 1)")  // 1 more Laptop

	// 3-way JOIN + GROUP BY + HAVING + ORDER BY + LIMIT
	check("Complex 3-way join",
		`SELECT v82_categories.name
		 FROM v82_categories
		 JOIN v82_products ON v82_products.category_id = v82_categories.id
		 JOIN v82_order_items ON v82_order_items.product_id = v82_products.id
		 GROUP BY v82_categories.name
		 HAVING SUM(v82_order_items.qty * v82_products.price) > 1000
		 ORDER BY SUM(v82_order_items.qty * v82_products.price) DESC
		 LIMIT 1`, "Electronics")

	// Electronics: 2*999.99 + 5*599.99 + 1*999.99 = 1999.98 + 2999.95 + 999.99 = 5999.92
	// Books: 10*14.99 + 3*49.99 = 149.90 + 149.97 = 299.87
	checkRowCount("HAVING filter count",
		`SELECT v82_categories.name
		 FROM v82_categories
		 JOIN v82_products ON v82_products.category_id = v82_categories.id
		 JOIN v82_order_items ON v82_order_items.product_id = v82_products.id
		 GROUP BY v82_categories.name
		 HAVING SUM(v82_order_items.qty) > 5`, 2) // Electronics(8), Books(13)

	// LEFT JOIN with aggregate
	check("LEFT JOIN aggregate",
		`SELECT v82_categories.name, COALESCE(SUM(v82_order_items.qty), 0) AS total_qty
		 FROM v82_categories
		 LEFT JOIN v82_products ON v82_products.category_id = v82_categories.id
		 LEFT JOIN v82_order_items ON v82_order_items.product_id = v82_products.id
		 GROUP BY v82_categories.name
		 ORDER BY total_qty ASC
		 LIMIT 1`, "Clothing")

	// ============================================================
	// === SECTION 4: RECURSIVE CTE EDGE CASES ===
	// ============================================================

	// Deep recursion (100 levels)
	check("Recursive CTE 100 levels",
		`WITH RECURSIVE seq(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 100
		)
		SELECT COUNT(*) FROM seq`, float64(100))

	// Recursive CTE with computation
	check("Recursive CTE factorial",
		`WITH RECURSIVE fact(n, val) AS (
			SELECT 1, 1
			UNION ALL
			SELECT n + 1, val * (n + 1) FROM fact WHERE n < 10
		)
		SELECT val FROM fact WHERE n = 10`, int64(3628800))

	// Recursive CTE for graph traversal
	afExec(t, db, ctx, "CREATE TABLE v82_graph (src INTEGER, dst INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v82_graph VALUES (1, 2)")
	afExec(t, db, ctx, "INSERT INTO v82_graph VALUES (2, 3)")
	afExec(t, db, ctx, "INSERT INTO v82_graph VALUES (3, 4)")
	afExec(t, db, ctx, "INSERT INTO v82_graph VALUES (4, 5)")
	afExec(t, db, ctx, "INSERT INTO v82_graph VALUES (2, 6)")

	check("Recursive CTE reachable count",
		`WITH RECURSIVE reachable(node) AS (
			SELECT 1
			UNION
			SELECT g.dst FROM v82_graph g JOIN reachable r ON g.src = r.node
		)
		SELECT COUNT(*) FROM reachable`, float64(6)) // 1,2,3,4,5,6

	// ============================================================
	// === SECTION 5: TRANSACTION EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_txn (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v82_txn VALUES (1, 100)")

	// Transaction with INSERT + UPDATE + DELETE + ROLLBACK
	checkNoError("BEGIN", "BEGIN")
	checkNoError("INSERT in txn", "INSERT INTO v82_txn VALUES (2, 200)")
	checkNoError("UPDATE in txn", "UPDATE v82_txn SET val = 999 WHERE id = 1")
	checkNoError("DELETE in txn", "DELETE FROM v82_txn WHERE id = 1")
	check("During txn", "SELECT COUNT(*) FROM v82_txn", float64(1)) // only id=2
	checkNoError("ROLLBACK", "ROLLBACK")

	// Everything should be back to original
	checkRowCount("After rollback", "SELECT * FROM v82_txn", 1)
	check("Original value", "SELECT val FROM v82_txn WHERE id = 1", float64(100))

	// Nested savepoints with interleaved operations
	checkNoError("BEGIN 2", "BEGIN")
	checkNoError("INSERT 2", "INSERT INTO v82_txn VALUES (10, 1000)")
	checkNoError("SP1", "SAVEPOINT sp1")
	checkNoError("UPDATE sp1", "UPDATE v82_txn SET val = val + 1 WHERE id = 10")
	checkNoError("SP2", "SAVEPOINT sp2")
	checkNoError("INSERT sp2", "INSERT INTO v82_txn VALUES (20, 2000)")
	checkNoError("UPDATE sp2", "UPDATE v82_txn SET val = val + 1 WHERE id = 10")

	// Check state during nested savepoints
	check("Val at sp2", "SELECT val FROM v82_txn WHERE id = 10", float64(1002))
	checkRowCount("Rows at sp2", "SELECT * FROM v82_txn", 3) // 1, 10, 20

	// Rollback to sp2 - undo sp2's INSERT and UPDATE
	checkNoError("ROLLBACK TO sp2", "ROLLBACK TO sp2")
	check("Val after sp2 rollback", "SELECT val FROM v82_txn WHERE id = 10", float64(1001))
	checkRowCount("Rows after sp2 rollback", "SELECT * FROM v82_txn", 2) // 1, 10

	// Continue and commit
	checkNoError("COMMIT 2", "COMMIT")
	check("Final val", "SELECT val FROM v82_txn WHERE id = 10", float64(1001))

	// ============================================================
	// === SECTION 6: INDEX + TRANSACTION INTERACTIONS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_idx (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v82_idx VALUES (%d, 'user_%d', %d)", i, i, i*5))
	}

	// Create index and query
	checkNoError("CREATE INDEX", "CREATE INDEX v82_idx_score ON v82_idx(score)")
	check("Index query", "SELECT name FROM v82_idx WHERE score = 50", "user_10")

	// Drop and recreate in transaction
	checkNoError("BEGIN idx", "BEGIN")
	checkNoError("DROP INDEX txn", "DROP INDEX v82_idx_score")
	checkNoError("CREATE INDEX txn", "CREATE INDEX v82_idx_name ON v82_idx(name)")
	checkNoError("COMMIT idx", "COMMIT")

	// Verify new index works
	check("New index query", "SELECT score FROM v82_idx WHERE name = 'user_5'", float64(25))

	// ============================================================
	// === SECTION 7: MULTIPLE TRIGGERS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_account (id INTEGER PRIMARY KEY, balance INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v82_audit (id INTEGER PRIMARY KEY, account_id INTEGER, action TEXT, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE v82_audit_counter (cnt INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v82_audit_counter VALUES (0)")

	// Trigger 1: Log deposits
	afExec(t, db, ctx, `CREATE TRIGGER v82_deposit_log
		AFTER INSERT ON v82_account
		FOR EACH ROW
		BEGIN
			UPDATE v82_audit_counter SET cnt = cnt + 1;
			INSERT INTO v82_audit VALUES (
				(SELECT cnt FROM v82_audit_counter),
				NEW.id, 'DEPOSIT', NEW.balance
			);
		END`)

	// Trigger 2: Log updates
	afExec(t, db, ctx, `CREATE TRIGGER v82_update_log
		AFTER UPDATE ON v82_account
		FOR EACH ROW
		BEGIN
			UPDATE v82_audit_counter SET cnt = cnt + 1;
			INSERT INTO v82_audit VALUES (
				(SELECT cnt FROM v82_audit_counter),
				NEW.id, 'UPDATE', NEW.balance - OLD.balance
			);
		END`)

	afExec(t, db, ctx, "INSERT INTO v82_account VALUES (1, 1000)")
	afExec(t, db, ctx, "INSERT INTO v82_account VALUES (2, 2000)")
	afExec(t, db, ctx, "UPDATE v82_account SET balance = balance + 500 WHERE id = 1")

	check("Audit deposit 1", "SELECT action FROM v82_audit WHERE id = 1", "DEPOSIT")
	check("Audit deposit 2", "SELECT action FROM v82_audit WHERE id = 2", "DEPOSIT")
	check("Audit update", "SELECT action FROM v82_audit WHERE id = 3", "UPDATE")
	check("Audit update amount", "SELECT amount FROM v82_audit WHERE id = 3", float64(500))

	// ============================================================
	// === SECTION 8: VIEW WITH COMPLEX QUERIES ===
	// ============================================================

	// View with JOIN
	checkNoError("CREATE VIEW with JOIN",
		`CREATE VIEW v82_product_details AS
		 SELECT v82_products.name AS product, v82_categories.name AS category, v82_products.price
		 FROM v82_products
		 JOIN v82_categories ON v82_categories.id = v82_products.category_id`)

	checkRowCount("View with JOIN rows", "SELECT * FROM v82_product_details", 5)
	check("View filter",
		"SELECT product FROM v82_product_details WHERE category = 'Electronics' AND price > 600 LIMIT 1", "Laptop")

	// View with ORDER BY
	check("View ORDER BY",
		"SELECT product FROM v82_product_details ORDER BY price DESC LIMIT 1", "Laptop")

	// ============================================================
	// === SECTION 9: DATA INTEGRITY UNDER STRESS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_stress (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT)")

	// Bulk insert
	for i := 1; i <= 100; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO v82_stress VALUES (%d, %d, 'tag_%d')", i, i*10, i%5))
	}
	check("Stress count", "SELECT COUNT(*) FROM v82_stress", float64(100))

	// Bulk update
	checkNoError("Stress update", "UPDATE v82_stress SET val = val + 1 WHERE val > 500")
	check("Stress update count",
		"SELECT COUNT(*) FROM v82_stress WHERE val > 500", float64(50))

	// Bulk delete
	checkNoError("Stress delete", "DELETE FROM v82_stress WHERE tag = 'tag_0'")
	check("Stress after delete", "SELECT COUNT(*) FROM v82_stress", float64(80))

	// Complex aggregate on remaining data
	check("Stress SUM",
		"SELECT SUM(val) FROM v82_stress WHERE val > 100", float64(39640))

	// VACUUM after stress
	checkNoError("VACUUM after stress", "VACUUM")
	check("Count after vacuum", "SELECT COUNT(*) FROM v82_stress", float64(80))

	// ANALYZE after stress
	checkNoError("ANALYZE after stress", "ANALYZE v82_stress")

	// ============================================================
	// === SECTION 10: COMPLEX EXPRESSION IN DIFFERENT POSITIONS ===
	// ============================================================

	// Expression in INSERT
	afExec(t, db, ctx, "CREATE TABLE v82_expr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO v82_expr VALUES (1, 10 + 20, 100 / 4, UPPER('hello'))")
	check("Expr INSERT a", "SELECT a FROM v82_expr WHERE id = 1", float64(30))
	check("Expr INSERT b", "SELECT b FROM v82_expr WHERE id = 1", float64(25))
	check("Expr INSERT c", "SELECT c FROM v82_expr WHERE id = 1", "HELLO")

	// Expression in UPDATE SET
	checkNoError("Expr UPDATE",
		"UPDATE v82_expr SET a = a * 2 + b, c = LOWER(c) WHERE id = 1")
	check("Expr UPDATE a", "SELECT a FROM v82_expr WHERE id = 1", float64(85))
	check("Expr UPDATE c", "SELECT c FROM v82_expr WHERE id = 1", "hello")

	// CASE in SELECT
	afExec(t, db, ctx, "CREATE TABLE v82_scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v82_scores VALUES (1, 'Alice', 95)")
	afExec(t, db, ctx, "INSERT INTO v82_scores VALUES (2, 'Bob', 72)")
	afExec(t, db, ctx, "INSERT INTO v82_scores VALUES (3, 'Charlie', 88)")
	afExec(t, db, ctx, "INSERT INTO v82_scores VALUES (4, 'Diana', 45)")

	check("CASE in SELECT",
		`SELECT name FROM v82_scores WHERE
			CASE WHEN score >= 90 THEN 'A'
				 WHEN score >= 80 THEN 'B'
				 WHEN score >= 70 THEN 'C'
				 ELSE 'F'
			END = 'A'`, "Alice")

	// IIF in ORDER BY
	check("IIF in ORDER BY",
		`SELECT name FROM v82_scores
		 ORDER BY IIF(score >= 80, 0, 1), score DESC
		 LIMIT 1`, "Alice") // high scorers first, then by score desc

	// ============================================================
	// === SECTION 11: UNION / INTERSECT / EXCEPT COMBINATIONS ===
	// ============================================================

	// UNION ALL with ORDER BY
	check("UNION ALL ORDER BY",
		`SELECT name FROM v82_scores WHERE score >= 80
		 UNION ALL
		 SELECT name FROM v82_scores WHERE score >= 90
		 ORDER BY name
		 LIMIT 1`, "Alice")

	// EXCEPT
	checkRowCount("EXCEPT",
		`SELECT name FROM v82_scores WHERE score >= 70
		 EXCEPT
		 SELECT name FROM v82_scores WHERE score >= 80`, 1) // Bob only

	// INTERSECT
	checkRowCount("INTERSECT",
		`SELECT name FROM v82_scores WHERE score >= 70
		 INTERSECT
		 SELECT name FROM v82_scores WHERE score >= 80`, 2) // Alice, Charlie

	// ============================================================
	// === SECTION 12: SELF-JOIN EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_hier (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT, level INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v82_hier VALUES (1, NULL, 'CEO', 0)")
	afExec(t, db, ctx, "INSERT INTO v82_hier VALUES (2, 1, 'CTO', 1)")
	afExec(t, db, ctx, "INSERT INTO v82_hier VALUES (3, 1, 'CFO', 1)")
	afExec(t, db, ctx, "INSERT INTO v82_hier VALUES (4, 2, 'VP Engineering', 2)")
	afExec(t, db, ctx, "INSERT INTO v82_hier VALUES (5, 2, 'VP Product', 2)")
	afExec(t, db, ctx, "INSERT INTO v82_hier VALUES (6, 4, 'Senior Dev', 3)")

	// Self-join to find manager name
	check("Self-join manager",
		`SELECT parent.name FROM v82_hier child
		 JOIN v82_hier parent ON parent.id = child.parent_id
		 WHERE child.name = 'Senior Dev'`, "VP Engineering")

	// Self-join to count direct reports
	check("Self-join direct reports",
		`SELECT COUNT(*) FROM v82_hier child
		 JOIN v82_hier parent ON parent.id = child.parent_id
		 WHERE parent.name = 'CTO'`, float64(2))

	// Find leaf nodes (no children)
	checkRowCount("Leaf nodes",
		`SELECT h1.name FROM v82_hier h1
		 LEFT JOIN v82_hier h2 ON h2.parent_id = h1.id
		 WHERE h2.id IS NULL`, 3) // CFO, VP Product, Senior Dev

	// Recursive CTE for full hierarchy
	check("Full hierarchy depth",
		`WITH RECURSIVE tree(id, name, depth) AS (
			SELECT id, name, 0 FROM v82_hier WHERE parent_id IS NULL
			UNION ALL
			SELECT h.id, h.name, t.depth + 1
			FROM v82_hier h JOIN tree t ON h.parent_id = t.id
		)
		SELECT MAX(depth) FROM tree`, float64(3))

	// ============================================================
	// === SECTION 13: NULL EDGE CASES IN COMPLEX QUERIES ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v82_nulls (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v82_nulls VALUES (1, NULL, NULL)")
	afExec(t, db, ctx, "INSERT INTO v82_nulls VALUES (2, NULL, 10)")
	afExec(t, db, ctx, "INSERT INTO v82_nulls VALUES (3, 10, NULL)")
	afExec(t, db, ctx, "INSERT INTO v82_nulls VALUES (4, 10, 10)")
	afExec(t, db, ctx, "INSERT INTO v82_nulls VALUES (5, 10, 20)")

	// NULL in arithmetic
	check("NULL + NULL", "SELECT a + b FROM v82_nulls WHERE id = 1", nil)
	check("NULL + int", "SELECT a + b FROM v82_nulls WHERE id = 2", nil)
	check("int + NULL", "SELECT a + b FROM v82_nulls WHERE id = 3", nil)
	check("int + int", "SELECT a + b FROM v82_nulls WHERE id = 4", float64(20))

	// NULL in comparison
	checkRowCount("a = b with NULLs",
		"SELECT * FROM v82_nulls WHERE a = b", 1) // only id=4

	// NULL in GROUP BY
	check("GROUP BY NULL",
		"SELECT COUNT(*) FROM v82_nulls WHERE a IS NULL GROUP BY a", float64(2))

	// COALESCE chain
	check("COALESCE chain",
		"SELECT COALESCE(a, b, 0) FROM v82_nulls WHERE id = 1", float64(0))
	check("COALESCE chain 2",
		"SELECT COALESCE(a, b, 0) FROM v82_nulls WHERE id = 2", float64(10))

	// ============================================================
	// === SECTION 14: SUBQUERY IN VARIOUS POSITIONS ===
	// ============================================================

	// Subquery as computed column in INSERT
	afExec(t, db, ctx, "CREATE TABLE v82_summary (id INTEGER PRIMARY KEY, total_emps INTEGER, avg_salary INTEGER)")
	checkNoError("INSERT with subqueries",
		`INSERT INTO v82_summary VALUES (
			1,
			(SELECT COUNT(*) FROM v82_emp),
			(SELECT AVG(salary) FROM v82_emp)
		)`)
	check("Subquery INSERT total",
		"SELECT total_emps FROM v82_summary WHERE id = 1", float64(5))

	// Subquery in CASE
	check("Subquery in CASE",
		`SELECT CASE
			WHEN (SELECT COUNT(*) FROM v82_emp WHERE dept = 'Engineering') > 2 THEN 'large'
			ELSE 'small'
		END`, "large")

	// Double nested subquery
	// Note: Engineering names were UPPERed earlier, so Eve is now EVE
	check("Double nested subquery",
		`SELECT name FROM v82_emp
		 WHERE salary = (
			SELECT MAX(salary) FROM v82_emp
			WHERE dept = (SELECT dept FROM v82_emp WHERE name = 'ALICE')
		 )`, "EVE")

	// ============================================================
	// === SECTION 15: COMPLEX REAL-WORLD SCENARIOS ===
	// ============================================================

	// E-commerce: Find top-selling category
	check("Top selling category",
		`WITH sales AS (
			SELECT v82_categories.name AS category,
				   SUM(v82_order_items.qty) AS total_qty
			FROM v82_categories
			JOIN v82_products ON v82_products.category_id = v82_categories.id
			JOIN v82_order_items ON v82_order_items.product_id = v82_products.id
			GROUP BY v82_categories.name
		)
		SELECT category FROM sales ORDER BY total_qty DESC LIMIT 1`, "Books")

	// Find products never ordered
	checkRowCount("Products never ordered",
		`SELECT v82_products.name FROM v82_products
		 WHERE NOT EXISTS (
			SELECT 1 FROM v82_order_items WHERE product_id = v82_products.id
		 )`, 1) // T-Shirt

	// Employee report: name, dept, salary rank within dept
	// Note: Engineering names were UPPERed earlier, so Eve is now EVE
	check("Salary rank in dept",
		`SELECT rnk FROM (
			SELECT name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
			FROM v82_emp
		) sub WHERE name = 'EVE'`, float64(1))

	// Revenue per category with running total
	checkRowCount("Revenue per category",
		`SELECT v82_categories.name, SUM(v82_order_items.qty * v82_products.price) AS revenue
		 FROM v82_categories
		 JOIN v82_products ON v82_products.category_id = v82_categories.id
		 JOIN v82_order_items ON v82_order_items.product_id = v82_products.id
		 GROUP BY v82_categories.name
		 ORDER BY revenue DESC`, 2) // Electronics, Books (Clothing has no orders)

	t.Logf("v82 Score: %d/%d tests passed", pass, total)
}
