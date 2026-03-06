package test

import (
	"fmt"
	"testing"
)

// TestV68AdvancedSQL tests advanced SQL features: complex JOINs, multi-table operations,
// window function combinations, advanced CTE patterns, recursive-like CTEs,
// complex GROUP BY/HAVING, multi-level subqueries, and real-world query patterns.
func TestV68AdvancedSQL(t *testing.T) {
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

	// ============================================================
	// === E-COMMERCE SCHEMA ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v68_customers (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT,
		tier TEXT DEFAULT 'basic',
		created_at TEXT
	)`)
	afExec(t, db, ctx, `CREATE TABLE v68_products (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		category TEXT,
		price INTEGER,
		stock INTEGER DEFAULT 0
	)`)
	afExec(t, db, ctx, `CREATE TABLE v68_orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		product_id INTEGER,
		quantity INTEGER,
		total INTEGER,
		status TEXT DEFAULT 'pending',
		order_date TEXT
	)`)

	// Insert customers
	afExec(t, db, ctx, "INSERT INTO v68_customers VALUES (1, 'Alice', 'alice@test.com', 'gold', '2024-01-01')")
	afExec(t, db, ctx, "INSERT INTO v68_customers VALUES (2, 'Bob', 'bob@test.com', 'silver', '2024-02-01')")
	afExec(t, db, ctx, "INSERT INTO v68_customers VALUES (3, 'Carol', 'carol@test.com', 'gold', '2024-03-01')")
	afExec(t, db, ctx, "INSERT INTO v68_customers VALUES (4, 'Dave', NULL, 'basic', '2024-04-01')")
	afExec(t, db, ctx, "INSERT INTO v68_customers VALUES (5, 'Eve', 'eve@test.com', 'basic', '2024-05-01')")

	// Insert products
	afExec(t, db, ctx, "INSERT INTO v68_products VALUES (1, 'Widget', 'electronics', 100, 50)")
	afExec(t, db, ctx, "INSERT INTO v68_products VALUES (2, 'Gadget', 'electronics', 200, 30)")
	afExec(t, db, ctx, "INSERT INTO v68_products VALUES (3, 'Book', 'media', 25, 100)")
	afExec(t, db, ctx, "INSERT INTO v68_products VALUES (4, 'Album', 'media', 15, 80)")
	afExec(t, db, ctx, "INSERT INTO v68_products VALUES (5, 'Shirt', 'clothing', 35, 60)")

	// Insert orders
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (1, 1, 1, 3, 300, 'completed', '2024-01-15')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (2, 1, 2, 1, 200, 'completed', '2024-02-10')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (3, 2, 3, 5, 125, 'completed', '2024-01-20')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (4, 2, 1, 2, 200, 'pending', '2024-03-01')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (5, 3, 4, 3, 45, 'completed', '2024-02-15')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (6, 3, 5, 2, 70, 'shipped', '2024-03-10')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (7, 4, 2, 1, 200, 'completed', '2024-04-01')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (8, 1, 3, 10, 250, 'completed', '2024-04-15')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (9, 5, 1, 1, 100, 'cancelled', '2024-05-01')")
	afExec(t, db, ctx, "INSERT INTO v68_orders VALUES (10, 3, 2, 2, 400, 'completed', '2024-05-15')")

	// ============================================================
	// === MULTI-TABLE JOIN QUERIES ===
	// ============================================================

	// MJ1: 3-table JOIN with aggregate
	check("MJ1 3-table JOIN revenue",
		`SELECT SUM(o.total) FROM v68_orders o
		 JOIN v68_customers c ON o.customer_id = c.id
		 JOIN v68_products p ON o.product_id = p.id
		 WHERE c.tier = 'gold' AND o.status = 'completed'`, 1195)
	// Alice completed: 300+200+250=750; Carol completed: 45+400=445 → 1195

	// MJ2: JOIN with GROUP BY and HAVING
	checkRowCount("MJ2 JOIN GROUP HAVING",
		`SELECT c.name, COUNT(*) as order_count
		 FROM v68_orders o
		 JOIN v68_customers c ON o.customer_id = c.id
		 GROUP BY c.name
		 HAVING COUNT(*) >= 2`, 3)
	// Alice:3, Bob:2, Carol:3 → 3 customers

	// MJ3: JOIN with category aggregation
	check("MJ3 category revenue",
		`SELECT SUM(o.total) FROM v68_orders o
		 JOIN v68_products p ON o.product_id = p.id
		 WHERE p.category = 'electronics'`, 1400)
	// Widget(id=1): order 1(300), order 4(200), order 9(100) = 600
	// Gadget(id=2): order 2(200), order 7(200), order 10(400) = 800
	// Total = 1400

	// MJ4: LEFT JOIN to find customers without orders
	checkRowCount("MJ4 LEFT JOIN no orders",
		`SELECT c.name FROM v68_customers c
		 LEFT JOIN v68_orders o ON c.id = o.customer_id
		 WHERE o.id IS NULL`, 0)
	// All customers have orders

	// ============================================================
	// === COMPLEX CTE ANALYTICS ===
	// ============================================================

	// CA1: Customer lifetime value
	check("CA1 customer LTV",
		`WITH customer_revenue AS (
			SELECT customer_id, SUM(total) as revenue
			FROM v68_orders
			WHERE status = 'completed'
			GROUP BY customer_id
		)
		SELECT MAX(revenue) FROM customer_revenue`, 750)
	// Alice: 300+200+250=750, Bob: 125, Carol: 45+400=445, Dave: 200 → MAX=750

	// CA2: Product popularity
	check("CA2 most ordered product",
		`WITH product_orders AS (
			SELECT product_id, SUM(quantity) as total_qty
			FROM v68_orders
			GROUP BY product_id
		)
		SELECT MAX(total_qty) FROM product_orders`, 15)
	// Widget: 3+2+1=6, Gadget: 1+1+2=4, Book: 5+10=15, Album: 3, Shirt: 2 → MAX=15

	// CA3: Category analysis with CTE
	check("CA3 top category",
		`WITH cat_rev AS (
			SELECT p.category, SUM(o.total) as revenue
			FROM v68_orders o
			JOIN v68_products p ON o.product_id = p.id
			WHERE o.status = 'completed'
			GROUP BY p.category
		)
		SELECT category FROM cat_rev ORDER BY revenue DESC LIMIT 1`, "electronics")

	// CA4: Multi-level CTE
	check("CA4 multi-level CTE",
		`WITH order_totals AS (
			SELECT customer_id, SUM(total) as total_spent
			FROM v68_orders
			WHERE status = 'completed'
			GROUP BY customer_id
		),
		ranked AS (
			SELECT customer_id, total_spent,
				   ROW_NUMBER() OVER (ORDER BY total_spent DESC) as rn
			FROM order_totals
		)
		SELECT customer_id FROM ranked WHERE rn = 1`, 1)
	// Alice has highest spend

	// ============================================================
	// === WINDOW FUNCTION COMBINATIONS ===
	// ============================================================

	// WC1: Running total
	check("WC1 running total first",
		`SELECT SUM(total) OVER (ORDER BY id) FROM v68_orders WHERE id = 1`, 300)

	// WC2: ROW_NUMBER for pagination
	checkRowCount("WC2 ROW_NUMBER pagination",
		`SELECT id, ROW_NUMBER() OVER (ORDER BY total DESC) as rn
		 FROM v68_orders`, 10)

	// WC3: RANK by customer spending - use CTE to rank then filter
	check("WC3 RANK",
		`WITH spending AS (
			SELECT customer_id, SUM(total) as total_spent
			FROM v68_orders GROUP BY customer_id
		),
		ranked AS (
			SELECT customer_id, RANK() OVER (ORDER BY total_spent DESC) as rk
			FROM spending
		)
		SELECT rk FROM ranked WHERE customer_id = 1`, 1)
	// Alice: 750, Carol: 515, Bob: 325, Dave: 200, Eve: 100

	// ============================================================
	// === COMPLEX WHERE CLAUSES ===
	// ============================================================

	// CW1: OR with AND combinations
	checkRowCount("CW1 complex WHERE",
		`SELECT * FROM v68_orders
		 WHERE (status = 'completed' AND total > 200)
		    OR (status = 'pending' AND total >= 200)`, 4)
	// completed>200: order 1(300), 8(250), 10(400) = 3
	// pending>=200: order 4(200) = 1
	// Total = 4

	// CW2: NOT with AND/OR
	checkRowCount("CW2 NOT WHERE",
		`SELECT * FROM v68_orders
		 WHERE NOT (status = 'cancelled' OR status = 'pending')`, 8)
	// Not cancelled(9) or pending(4) = 10-2 = 8

	// CW3: IN with subquery and AND
	checkRowCount("CW3 IN subquery AND",
		`SELECT * FROM v68_orders
		 WHERE customer_id IN (SELECT id FROM v68_customers WHERE tier = 'gold')
		   AND status = 'completed'`, 5)
	// Gold customers: Alice(1), Carol(3)
	// Alice completed: 1,2,8 = 3; Carol completed: 5,10 = 2 → 5

	// ============================================================
	// === CONDITIONAL AGGREGATION ===
	// ============================================================

	// CD1: CASE in SUM
	check("CD1 conditional SUM",
		`SELECT SUM(CASE WHEN status = 'completed' THEN total ELSE 0 END)
		 FROM v68_orders`, 1520)
	// 300+200+125+45+200+250+400 = 1520

	// CD2: CASE in COUNT
	check("CD2 conditional COUNT",
		`SELECT SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)
		 FROM v68_orders`, 7)

	// CD3: Multiple CASE aggregates
	check("CD3 completed revenue",
		`SELECT SUM(CASE WHEN status = 'completed' THEN total ELSE 0 END)
		 FROM v68_orders`, 1520)

	// ============================================================
	// === CONSTRAINT ENFORCEMENT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v68_strict (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		age INTEGER CHECK(age >= 0 AND age <= 150),
		email TEXT UNIQUE
	)`)

	// CE1: Valid insert
	checkNoError("CE1 valid insert",
		"INSERT INTO v68_strict VALUES (1, 'Test', 25, 'test@test.com')")

	// CE2: NOT NULL violation
	checkError("CE2 NOT NULL",
		"INSERT INTO v68_strict VALUES (2, NULL, 30, 'test2@test.com')")

	// CE3: CHECK violation
	checkError("CE3 CHECK negative age",
		"INSERT INTO v68_strict VALUES (3, 'Bad', -1, 'bad@test.com')")

	// CE4: CHECK upper bound
	checkError("CE4 CHECK age > 150",
		"INSERT INTO v68_strict VALUES (4, 'Old', 151, 'old@test.com')")

	// CE5: UNIQUE violation
	checkError("CE5 UNIQUE email",
		"INSERT INTO v68_strict VALUES (5, 'Dup', 30, 'test@test.com')")

	// CE6: Primary key violation
	checkError("CE6 PK violation",
		"INSERT INTO v68_strict VALUES (1, 'Dup', 30, 'dup@test.com')")

	// ============================================================
	// === DERIVED TABLE PATTERNS ===
	// ============================================================

	// DT1: Derived table with aggregate
	check("DT1 derived agg",
		`SELECT MAX(total_spent) FROM (
			SELECT customer_id, SUM(total) as total_spent
			FROM v68_orders
			GROUP BY customer_id
		) sub`, 750)

	// DT2: Derived table with filter
	checkRowCount("DT2 derived filter",
		`SELECT * FROM (
			SELECT customer_id, SUM(total) as total_spent
			FROM v68_orders
			GROUP BY customer_id
		) sub WHERE total_spent > 300`, 3)
	// Alice: 750, Bob: 325, Carol: 515 → 3 (all > 300)

	// DT3: Nested derived tables
	check("DT3 nested derived",
		`SELECT COUNT(*) FROM (
			SELECT customer_id FROM (
				SELECT customer_id, COUNT(*) as cnt
				FROM v68_orders
				GROUP BY customer_id
			) inner_sub WHERE cnt >= 2
		) outer_sub`, 3)
	// Alice:3, Bob:2, Carol:3 → 3 with cnt >= 2

	// ============================================================
	// === COMPLEX UPDATE PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v68_inventory (
		id INTEGER PRIMARY KEY, name TEXT, stock INTEGER, reorder_point INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v68_inventory VALUES (1, 'A', 10, 5)")
	afExec(t, db, ctx, "INSERT INTO v68_inventory VALUES (2, 'B', 3, 5)")
	afExec(t, db, ctx, "INSERT INTO v68_inventory VALUES (3, 'C', 20, 10)")
	afExec(t, db, ctx, "INSERT INTO v68_inventory VALUES (4, 'D', 1, 5)")

	// CU1: UPDATE with CASE
	checkNoError("CU1 UPDATE CASE",
		`UPDATE v68_inventory SET stock = CASE
			WHEN stock < reorder_point THEN stock + 10
			ELSE stock
		 END`)
	check("CU1 verify B", "SELECT stock FROM v68_inventory WHERE id = 2", 13)
	check("CU1 verify D", "SELECT stock FROM v68_inventory WHERE id = 4", 11)
	check("CU1 verify A unchanged", "SELECT stock FROM v68_inventory WHERE id = 1", 10)

	// ============================================================
	// === COMPLEX DELETE PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v68_logs (
		id INTEGER PRIMARY KEY, level TEXT, message TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v68_logs VALUES (1, 'INFO', 'start')")
	afExec(t, db, ctx, "INSERT INTO v68_logs VALUES (2, 'ERROR', 'crash')")
	afExec(t, db, ctx, "INSERT INTO v68_logs VALUES (3, 'DEBUG', 'trace')")
	afExec(t, db, ctx, "INSERT INTO v68_logs VALUES (4, 'INFO', 'done')")
	afExec(t, db, ctx, "INSERT INTO v68_logs VALUES (5, 'DEBUG', 'verbose')")

	// CD4: DELETE with complex WHERE
	checkNoError("CD4 DELETE complex",
		"DELETE FROM v68_logs WHERE level IN ('DEBUG', 'INFO') AND id < 4")
	check("CD4 count", "SELECT COUNT(*) FROM v68_logs", 3)
	// Deleted: id=1(INFO,<4), id=3(DEBUG,<4) → remaining: 2,4,5

	// ============================================================
	// === STRING FUNCTIONS ===
	// ============================================================

	// SF1: UPPER
	check("SF1 UPPER", "SELECT UPPER('hello')", "HELLO")

	// SF2: LOWER
	check("SF2 LOWER", "SELECT LOWER('HELLO')", "hello")

	// SF3: LENGTH
	check("SF3 LENGTH", "SELECT LENGTH('hello')", 5)

	// SF4: SUBSTR
	check("SF4 SUBSTR", "SELECT SUBSTR('hello world', 7, 5)", "world")

	// SF5: TRIM
	check("SF5 TRIM", "SELECT TRIM('  hello  ')", "hello")

	// SF6: REPLACE
	check("SF6 REPLACE", "SELECT REPLACE('hello world', 'world', 'earth')", "hello earth")

	// SF7: CONCAT via ||
	check("SF7 CONCAT", "SELECT 'hello' || ' ' || 'world'", "hello world")

	// SF8: INSTR
	check("SF8 INSTR", "SELECT INSTR('hello world', 'world')", 7)

	// ============================================================
	// === NUMERIC FUNCTIONS ===
	// ============================================================

	// NF1: ABS
	check("NF1 ABS", "SELECT ABS(-42)", 42)

	// NF2: MAX with single-arg aggregate (standard SQL)
	check("NF2 MAX agg", "SELECT MAX(price) FROM v68_products", 200)

	// NF3: MIN with single-arg aggregate (standard SQL)
	check("NF3 MIN agg", "SELECT MIN(price) FROM v68_products", 15)

	// ============================================================
	// === TRANSACTION ISOLATION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v68_txn (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v68_txn VALUES (1, 100)")

	// TI1: Commit works
	checkNoError("TI1 BEGIN", "BEGIN")
	checkNoError("TI1 UPDATE", "UPDATE v68_txn SET val = 200 WHERE id = 1")
	checkNoError("TI1 COMMIT", "COMMIT")
	check("TI1 verify", "SELECT val FROM v68_txn WHERE id = 1", 200)

	// TI2: Rollback undoes changes
	checkNoError("TI2 BEGIN", "BEGIN")
	checkNoError("TI2 UPDATE", "UPDATE v68_txn SET val = 999 WHERE id = 1")
	checkNoError("TI2 ROLLBACK", "ROLLBACK")
	check("TI2 verify unchanged", "SELECT val FROM v68_txn WHERE id = 1", 200)

	// TI3: SAVEPOINT
	checkNoError("TI3 BEGIN", "BEGIN")
	checkNoError("TI3 UPDATE 1", "UPDATE v68_txn SET val = 300 WHERE id = 1")
	checkNoError("TI3 SAVEPOINT", "SAVEPOINT sp1")
	checkNoError("TI3 UPDATE 2", "UPDATE v68_txn SET val = 400 WHERE id = 1")
	checkNoError("TI3 ROLLBACK TO", "ROLLBACK TO sp1")
	checkNoError("TI3 COMMIT", "COMMIT")
	check("TI3 verify savepoint", "SELECT val FROM v68_txn WHERE id = 1", 300)

	// ============================================================
	// === INDEX USAGE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v68_indexed (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v68_indexed VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v68_indexed VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO v68_indexed VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO v68_indexed VALUES (4, 'c', 40)")
	afExec(t, db, ctx, "INSERT INTO v68_indexed VALUES (5, 'b', 50)")

	// IX1: Create index
	checkNoError("IX1 CREATE INDEX",
		"CREATE INDEX idx_v68_cat ON v68_indexed (category)")

	// IX2: Query with index
	check("IX2 indexed query",
		"SELECT SUM(val) FROM v68_indexed WHERE category = 'a'", 40)

	// IX3: Index doesn't change results
	checkRowCount("IX3 full scan",
		"SELECT * FROM v68_indexed", 5)

	// ============================================================
	// === VIEWS ===
	// ============================================================

	// VW1: Create view
	checkNoError("VW1 CREATE VIEW",
		`CREATE VIEW v68_customer_orders AS
		 SELECT c.name, COUNT(*) as order_count, SUM(o.total) as total_spent
		 FROM v68_customers c
		 JOIN v68_orders o ON c.id = o.customer_id
		 GROUP BY c.name`)

	// VW2: Query view
	check("VW2 query view",
		"SELECT total_spent FROM v68_customer_orders WHERE name = 'Alice'", 750)

	// VW3: View count
	check("VW3 view count",
		"SELECT COUNT(*) FROM v68_customer_orders", 5)

	// ============================================================
	// === DROP TABLE IF EXISTS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v68_temp (id INTEGER PRIMARY KEY)`)
	checkNoError("DT4 DROP TABLE", "DROP TABLE v68_temp")
	checkError("DT5 query dropped", "SELECT * FROM v68_temp")
	checkNoError("DT6 DROP IF EXISTS", "DROP TABLE IF EXISTS v68_temp")

	// ============================================================
	// === COMPLEX ORDERING ===
	// ============================================================

	// CO1: Multi-column ORDER BY
	check("CO1 multi-col ORDER",
		`SELECT name FROM v68_customers ORDER BY tier ASC, name ASC LIMIT 1`, "Dave")
	// basic: Dave, Eve; gold: Alice, Carol; silver: Bob → Dave first

	// CO2: ORDER BY DESC
	check("CO2 ORDER DESC",
		"SELECT name FROM v68_customers ORDER BY name DESC LIMIT 1", "Eve")

	// CO3: ORDER BY with expression
	check("CO3 ORDER BY expr",
		"SELECT name FROM v68_products ORDER BY price * stock DESC LIMIT 1", "Gadget")
	// Widget:5000, Gadget:6000, Book:2500, Album:1200, Shirt:2100 → Gadget

	// ============================================================
	// === EDGE CASES ===
	// ============================================================

	// EC1: SELECT with no FROM
	check("EC1 no FROM", "SELECT 1 + 1", 2)

	// EC2: Empty string vs NULL
	afExec(t, db, ctx, `CREATE TABLE v68_str (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v68_str VALUES (1, '')")
	afExec(t, db, ctx, "INSERT INTO v68_str VALUES (2, NULL)")
	check("EC2 empty string length", "SELECT LENGTH(val) FROM v68_str WHERE id = 1", 0)

	// EC3: NULL in arithmetic
	check("EC3 NULL + 1", "SELECT NULL + 1", nil)

	// EC4: Boolean expressions (CobaltDB returns bool type)
	check("EC4 true AND false", "SELECT 1 AND 0", false)
	check("EC4 true OR false", "SELECT 1 OR 0", true)

	t.Logf("\n=== V68 ADVANCED SQL: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
