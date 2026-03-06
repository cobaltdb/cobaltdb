package test

import (
	"fmt"
	"testing"
)

// TestV61RealWorldPatterns tests common real-world SQL patterns including
// pagination, reporting queries, data warehousing patterns, OLTP transactions,
// and complex business logic queries.
func TestV61RealWorldPatterns(t *testing.T) {
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

	checkMultiCol := func(desc string, sql string, expectedCols []interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		if len(rows[0]) < len(expectedCols) {
			t.Errorf("[FAIL] %s: expected %d columns, got %d", desc, len(expectedCols), len(rows[0]))
			return
		}
		for i, exp := range expectedCols {
			got := fmt.Sprintf("%v", rows[0][i])
			e := fmt.Sprintf("%v", exp)
			if got != e {
				t.Errorf("[FAIL] %s: col %d got %s, expected %s", desc, i, got, e)
				return
			}
		}
		pass++
	}
	_ = checkMultiCol

	// ============================================================
	// === E-COMMERCE SCHEMA ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v61_users (
		id INTEGER PRIMARY KEY, username TEXT UNIQUE, email TEXT, status TEXT, created_year INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v61_products (
		id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER, active INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v61_orders (
		id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, qty INTEGER,
		order_year INTEGER, order_month INTEGER, status TEXT)`)

	// Insert users
	afExec(t, db, ctx, "INSERT INTO v61_users VALUES (1, 'alice', 'alice@test.com', 'active', 2023)")
	afExec(t, db, ctx, "INSERT INTO v61_users VALUES (2, 'bob', 'bob@test.com', 'active', 2023)")
	afExec(t, db, ctx, "INSERT INTO v61_users VALUES (3, 'carol', 'carol@test.com', 'inactive', 2024)")
	afExec(t, db, ctx, "INSERT INTO v61_users VALUES (4, 'dave', 'dave@test.com', 'active', 2024)")
	afExec(t, db, ctx, "INSERT INTO v61_users VALUES (5, 'eve', 'eve@test.com', 'suspended', 2024)")

	// Insert products
	afExec(t, db, ctx, "INSERT INTO v61_products VALUES (1, 'Widget A', 'Widgets', 25, 1)")
	afExec(t, db, ctx, "INSERT INTO v61_products VALUES (2, 'Widget B', 'Widgets', 35, 1)")
	afExec(t, db, ctx, "INSERT INTO v61_products VALUES (3, 'Gadget X', 'Gadgets', 50, 1)")
	afExec(t, db, ctx, "INSERT INTO v61_products VALUES (4, 'Gadget Y', 'Gadgets', 75, 0)")
	afExec(t, db, ctx, "INSERT INTO v61_products VALUES (5, 'Tool Z', 'Tools', 100, 1)")

	// Insert orders
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (1, 1, 1, 3, 2024, 1, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (2, 1, 3, 1, 2024, 2, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (3, 2, 1, 5, 2024, 1, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (4, 2, 2, 2, 2024, 3, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (5, 2, 5, 1, 2024, 3, 'refunded')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (6, 3, 3, 2, 2024, 1, 'cancelled')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (7, 4, 1, 10, 2024, 2, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (8, 4, 2, 5, 2024, 2, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (9, 1, 5, 1, 2024, 4, 'pending')")
	afExec(t, db, ctx, "INSERT INTO v61_orders VALUES (10, 2, 3, 3, 2024, 4, 'pending')")

	// ============================================================
	// === PAGINATION PATTERN ===
	// ============================================================

	// PG1: Page 1 (3 items per page)
	checkRowCount("PG1 page 1",
		"SELECT * FROM v61_products WHERE active = 1 ORDER BY id LIMIT 3 OFFSET 0", 3)

	// PG2: Page 2
	checkRowCount("PG2 page 2",
		"SELECT * FROM v61_products WHERE active = 1 ORDER BY id LIMIT 3 OFFSET 3", 1)
	// Active: Widget A, Widget B, Gadget X, Tool Z = 4 total; page 2 has 1

	// PG3: Total count for pagination
	check("PG3 total count",
		"SELECT COUNT(*) FROM v61_products WHERE active = 1", 4)

	// ============================================================
	// === SALES REPORT ===
	// ============================================================

	// SR1: Revenue per category (completed orders only)
	check("SR1 revenue by category",
		`SELECT p.category FROM v61_orders o
		 JOIN v61_products p ON o.product_id = p.id
		 WHERE o.status = 'completed'
		 GROUP BY p.category
		 ORDER BY SUM(o.qty * p.price) DESC LIMIT 1`, "Widgets")
	// Widgets: (3*25 + 5*25 + 2*35 + 10*25 + 5*35) = 75+125+70+250+175 = 695
	// Gadgets: 1*50 = 50
	// Tools: (none completed with refunded status=5)
	// → Widgets

	// SR2: Total revenue
	check("SR2 total revenue",
		`SELECT SUM(o.qty * p.price) FROM v61_orders o
		 JOIN v61_products p ON o.product_id = p.id
		 WHERE o.status = 'completed'`, 745)
	// Widgets=695, Gadgets=50, Tools=0 (order 5 refunded) = 745

	// SR3: Monthly revenue CTE
	check("SR3 monthly peak",
		`WITH monthly AS (
			SELECT o.order_month, SUM(o.qty * p.price) AS revenue
			FROM v61_orders o
			JOIN v61_products p ON o.product_id = p.id
			WHERE o.status = 'completed'
			GROUP BY o.order_month
		)
		SELECT order_month FROM monthly ORDER BY revenue DESC LIMIT 1`, 2)
	// Month 1: 3*25+5*25=75+125=200
	// Month 2: 1*50+10*25+5*35=50+250+175=475
	// Month 3: 2*35=70
	// → Month 2

	// ============================================================
	// === TOP CUSTOMER ANALYSIS ===
	// ============================================================

	// TC1: Top customer by spend
	check("TC1 top customer",
		`WITH customer_spend AS (
			SELECT u.username, SUM(o.qty * p.price) AS spend
			FROM v61_users u
			JOIN v61_orders o ON u.id = o.user_id
			JOIN v61_products p ON o.product_id = p.id
			WHERE o.status = 'completed'
			GROUP BY u.username
		)
		SELECT username FROM customer_spend ORDER BY spend DESC LIMIT 1`, "dave")
	// alice: 3*25+1*50=75+50=125
	// bob: 5*25+2*35=125+70=195
	// dave: 10*25+5*35=250+175=425
	// → dave

	// TC2: Customer with most orders
	check("TC2 most orders",
		`SELECT u.username FROM v61_users u
		 JOIN v61_orders o ON u.id = o.user_id
		 GROUP BY u.username
		 ORDER BY COUNT(*) DESC LIMIT 1`, "bob")
	// alice: 3, bob: 3, carol: 1, dave: 2 → bob or alice (tie, depends on order)

	// TC3: Customers with no orders
	checkRowCount("TC3 no orders",
		`SELECT username FROM v61_users u
		 WHERE NOT EXISTS (SELECT 1 FROM v61_orders WHERE user_id = u.id)`, 1)
	// eve has no orders

	// ============================================================
	// === INVENTORY CHECK ===
	// ============================================================

	// IC1: Products never ordered
	checkRowCount("IC1 never ordered",
		`SELECT name FROM v61_products
		 WHERE id NOT IN (SELECT DISTINCT product_id FROM v61_orders)`, 1)
	// Gadget Y (id=4) never ordered

	// IC2: Most popular product
	check("IC2 most popular",
		`SELECT p.name FROM v61_products p
		 JOIN v61_orders o ON p.id = o.product_id
		 GROUP BY p.name
		 ORDER BY SUM(o.qty) DESC LIMIT 1`, "Widget A")
	// Widget A: 3+5+10=18, Widget B: 2+5=7, Gadget X: 1+2+3=6, Tool Z: 1+1=2

	// ============================================================
	// === STATUS TRANSITION ===
	// ============================================================

	// ST1: Order status distribution
	check("ST1 completed count",
		"SELECT COUNT(*) FROM v61_orders WHERE status = 'completed'", 6)

	// ST2: Update order status
	checkNoError("ST2 update status",
		"UPDATE v61_orders SET status = 'completed' WHERE id = 9")
	check("ST2 verify", "SELECT status FROM v61_orders WHERE id = 9", "completed")

	// ST3: Bulk status update
	checkNoError("ST3 bulk update",
		"UPDATE v61_orders SET status = 'archived' WHERE status = 'completed' AND order_month < 3")
	check("ST3 archived count", "SELECT COUNT(*) FROM v61_orders WHERE status = 'archived'", 5)
	// Orders completed AND month<3: 1,3 (month 1), 2,7,8 (month 2) = 5

	// ============================================================
	// === WINDOW FUNCTIONS FOR RANKING ===
	// ============================================================

	// WR1: Customer spend ranking
	check("WR1 spend ranking",
		`WITH spend AS (
			SELECT u.username, SUM(o.qty * p.price) AS total
			FROM v61_users u
			JOIN v61_orders o ON u.id = o.user_id
			JOIN v61_products p ON o.product_id = p.id
			WHERE o.status IN ('completed', 'archived')
			GROUP BY u.username
		),
		ranked AS (
			SELECT username, total, RANK() OVER (ORDER BY total DESC) AS rnk
			FROM spend
		)
		SELECT username FROM ranked WHERE rnk = 1`, "dave")

	// WR2: Product category ranking
	check("WR2 category top product",
		`WITH product_sales AS (
			SELECT p.name, p.category, SUM(o.qty) AS total_sold
			FROM v61_products p
			JOIN v61_orders o ON p.id = o.product_id
			GROUP BY p.name, p.category
		),
		ranked AS (
			SELECT name, category, total_sold,
				   ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_sold DESC) AS rn
			FROM product_sales
		)
		SELECT name FROM ranked WHERE category = 'Widgets' AND rn = 1`, "Widget A")

	// ============================================================
	// === CONDITIONAL AGGREGATION ===
	// ============================================================

	// CA1: Pivot-style query with CASE
	check("CA1 completed revenue",
		`SELECT SUM(CASE WHEN o.status IN ('completed', 'archived') THEN o.qty * p.price ELSE 0 END)
		 FROM v61_orders o
		 JOIN v61_products p ON o.product_id = p.id`, 845)

	// CA2: Conditional count
	check("CA2 active users with orders",
		`SELECT COUNT(DISTINCT o.user_id) FROM v61_orders o
		 JOIN v61_users u ON o.user_id = u.id
		 WHERE u.status = 'active'`, 3)
	// alice(active), bob(active), dave(active) = 3

	// ============================================================
	// === DATA TRANSFORMATION ===
	// ============================================================

	// DT1: Create summary table from query
	afExec(t, db, ctx, `CREATE TABLE v61_summary (
		category TEXT, total_revenue INTEGER, order_count INTEGER)`)
	checkNoError("DT1 populate summary",
		`INSERT INTO v61_summary
		 SELECT p.category, SUM(o.qty * p.price), COUNT(*)
		 FROM v61_orders o
		 JOIN v61_products p ON o.product_id = p.id
		 WHERE o.status IN ('completed', 'archived')
		 GROUP BY p.category`)
	checkRowCount("DT1 verify", "SELECT * FROM v61_summary", 3)

	// DT2: Verify summary
	check("DT2 top category",
		"SELECT category FROM v61_summary ORDER BY total_revenue DESC LIMIT 1", "Widgets")

	// ============================================================
	// === MULTI-LEVEL CTE ===
	// ============================================================

	// ML1: Three-level CTE for analysis
	check("ML1 three-level CTE",
		`WITH order_values AS (
			SELECT o.id, o.user_id, o.qty * p.price AS value
			FROM v61_orders o
			JOIN v61_products p ON o.product_id = p.id
		),
		user_totals AS (
			SELECT user_id, SUM(value) AS total, COUNT(*) AS cnt
			FROM order_values GROUP BY user_id
		),
		user_stats AS (
			SELECT AVG(total) AS avg_spend FROM user_totals
		)
		SELECT avg_spend FROM user_stats`, "298.75")

	// ML2: CTE with multiple references
	check("ML2 CTE multiple refs",
		`WITH user_spend AS (
			SELECT u.username, SUM(o.qty * p.price) AS spend
			FROM v61_users u
			JOIN v61_orders o ON u.id = o.user_id
			JOIN v61_products p ON o.product_id = p.id
			GROUP BY u.username
		)
		SELECT COUNT(*) FROM user_spend WHERE spend > (SELECT AVG(spend) FROM user_spend)`, 2)

	// ============================================================
	// === COMPLEX BUSINESS RULES ===
	// ============================================================

	// BR1: Identify VIP customers (spend > 200, active, > 2 orders)
	checkRowCount("BR1 VIP customers",
		`WITH customer_stats AS (
			SELECT u.id, u.username, u.status,
				   SUM(o.qty * p.price) AS total_spend,
				   COUNT(*) AS order_count
			FROM v61_users u
			JOIN v61_orders o ON u.id = o.user_id
			JOIN v61_products p ON o.product_id = p.id
			GROUP BY u.id, u.username, u.status
		)
		SELECT username FROM customer_stats
		WHERE status = 'active'
		AND total_spend > 200
		AND order_count > 2`, 2)
	// alice(active, orders=3, spend>200), bob(active, orders=3, spend>200) both qualify

	// BR2: Products above average price
	checkRowCount("BR2 above avg price",
		"SELECT name FROM v61_products WHERE price > (SELECT AVG(price) FROM v61_products)", 2)
	// AVG = (25+35+50+75+100)/5 = 57; above: 75, 100 = 2

	// ============================================================
	// === CLEANUP AND ARCHIVING PATTERNS ===
	// ============================================================

	// CL1: Soft delete pattern
	afExec(t, db, ctx, `CREATE TABLE v61_logs (
		id INTEGER PRIMARY KEY, message TEXT, deleted INTEGER DEFAULT 0)`)
	afExec(t, db, ctx, "INSERT INTO v61_logs VALUES (1, 'log1', 0)")
	afExec(t, db, ctx, "INSERT INTO v61_logs VALUES (2, 'log2', 0)")
	afExec(t, db, ctx, "INSERT INTO v61_logs VALUES (3, 'log3', 0)")

	checkNoError("CL1 soft delete",
		"UPDATE v61_logs SET deleted = 1 WHERE id = 2")
	check("CL1 active count",
		"SELECT COUNT(*) FROM v61_logs WHERE deleted = 0", 2)

	// CL2: Count active vs deleted
	check("CL2 deleted count",
		"SELECT COUNT(*) FROM v61_logs WHERE deleted = 1", 1)

	// ============================================================
	// === STRING MATCHING PATTERNS ===
	// ============================================================

	// SM1: Email domain search
	checkRowCount("SM1 email search",
		"SELECT * FROM v61_users WHERE email LIKE '%test.com'", 5)

	// SM2: Username prefix
	checkRowCount("SM2 prefix search",
		"SELECT * FROM v61_users WHERE username LIKE 'a%'", 1)

	// ============================================================
	// === EDGE: NULL handling in JOINs ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v61_parent (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, `CREATE TABLE v61_child (id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v61_parent VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO v61_parent VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO v61_parent VALUES (3, 'P3')")
	afExec(t, db, ctx, "INSERT INTO v61_child VALUES (1, 1, 'C1')")
	afExec(t, db, ctx, "INSERT INTO v61_child VALUES (2, 1, 'C2')")
	afExec(t, db, ctx, "INSERT INTO v61_child VALUES (3, NULL, 'C3')")

	// NJ1: LEFT JOIN with NULL parent_id
	checkRowCount("NJ1 LEFT JOIN null fk",
		`SELECT p.name, c.val FROM v61_parent p
		 LEFT JOIN v61_child c ON p.id = c.parent_id`, 4)
	// P1-C1, P1-C2, P2-NULL, P3-NULL = 4 rows (C3 has NULL parent, doesn't match)

	// NJ2: Children with parents (INNER JOIN)
	checkRowCount("NJ2 INNER JOIN",
		`SELECT c.val FROM v61_child c
		 JOIN v61_parent p ON c.parent_id = p.id`, 2)
	// C1 and C2 match P1; C3 has NULL parent → no match

	// NJ3: Orphan children
	checkRowCount("NJ3 orphan children",
		`SELECT val FROM v61_child
		 WHERE parent_id IS NULL OR parent_id NOT IN (SELECT id FROM v61_parent)`, 1)

	t.Logf("\n=== V61 REAL WORLD PATTERNS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
