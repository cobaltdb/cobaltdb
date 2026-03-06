package test

import (
	"fmt"
	"testing"
)

// TestV47ComprehensiveRegression exercises complex combinations of all features
// to uncover any remaining bugs through realistic real-world query patterns.
func TestV47ComprehensiveRegression(t *testing.T) {
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
			t.Errorf("[FAIL] %s: expected error but got nil", desc)
			return
		}
		pass++
	}

	// ============================================================
	// Setup: E-commerce database
	// ============================================================
	afExec(t, db, ctx, `CREATE TABLE v47_categories (
		id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v47_categories VALUES (1, 'Electronics', NULL)")
	afExec(t, db, ctx, "INSERT INTO v47_categories VALUES (2, 'Phones', 1)")
	afExec(t, db, ctx, "INSERT INTO v47_categories VALUES (3, 'Laptops', 1)")
	afExec(t, db, ctx, "INSERT INTO v47_categories VALUES (4, 'Clothing', NULL)")
	afExec(t, db, ctx, "INSERT INTO v47_categories VALUES (5, 'Shirts', 4)")

	afExec(t, db, ctx, `CREATE TABLE v47_products (
		id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER, price INTEGER, stock INTEGER,
		FOREIGN KEY (category_id) REFERENCES v47_categories(id))`)
	afExec(t, db, ctx, "INSERT INTO v47_products VALUES (1, 'iPhone', 2, 999, 50)")
	afExec(t, db, ctx, "INSERT INTO v47_products VALUES (2, 'Galaxy', 2, 799, 30)")
	afExec(t, db, ctx, "INSERT INTO v47_products VALUES (3, 'MacBook', 3, 1999, 20)")
	afExec(t, db, ctx, "INSERT INTO v47_products VALUES (4, 'ThinkPad', 3, 1499, 15)")
	afExec(t, db, ctx, "INSERT INTO v47_products VALUES (5, 'T-Shirt', 5, 29, 200)")
	afExec(t, db, ctx, "INSERT INTO v47_products VALUES (6, 'Polo', 5, 49, 150)")

	afExec(t, db, ctx, `CREATE TABLE v47_customers (
		id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE, tier TEXT DEFAULT 'standard')`)
	afExec(t, db, ctx, "INSERT INTO v47_customers VALUES (1, 'Alice', 'alice@test.com', 'premium')")
	afExec(t, db, ctx, "INSERT INTO v47_customers VALUES (2, 'Bob', 'bob@test.com', 'standard')")
	afExec(t, db, ctx, "INSERT INTO v47_customers VALUES (3, 'Carol', 'carol@test.com', 'premium')")
	afExec(t, db, ctx, "INSERT INTO v47_customers VALUES (4, 'Dave', 'dave@test.com', 'standard')")

	afExec(t, db, ctx, `CREATE TABLE v47_orders (
		id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER,
		quantity INTEGER, total INTEGER, status TEXT,
		FOREIGN KEY (customer_id) REFERENCES v47_customers(id),
		FOREIGN KEY (product_id) REFERENCES v47_products(id))`)
	afExec(t, db, ctx, "INSERT INTO v47_orders VALUES (1, 1, 1, 1, 999, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v47_orders VALUES (2, 1, 3, 1, 1999, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v47_orders VALUES (3, 2, 5, 3, 87, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v47_orders VALUES (4, 2, 2, 1, 799, 'pending')")
	afExec(t, db, ctx, "INSERT INTO v47_orders VALUES (5, 3, 1, 2, 1998, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v47_orders VALUES (6, 3, 6, 5, 245, 'completed')")
	afExec(t, db, ctx, "INSERT INTO v47_orders VALUES (7, 4, 4, 1, 1499, 'cancelled')")
	afExec(t, db, ctx, "INSERT INTO v47_orders VALUES (8, 1, 5, 2, 58, 'completed')")

	// ============================================================
	// === DERIVED TABLES WITH JOINS ===
	// ============================================================

	// DJ1: Derived table in main FROM with JOIN
	check("DJ1 derived table JOIN real table",
		`SELECT c.name FROM
		   (SELECT * FROM v47_orders WHERE status = 'completed') AS completed_orders
		   JOIN v47_customers c ON completed_orders.customer_id = c.id
		 GROUP BY c.name ORDER BY SUM(completed_orders.total) DESC LIMIT 1`,
		"Alice") // Alice: 999+1999+58=3056

	// DJ2: Two derived tables joined
	check("DJ2 two derived tables joined",
		`SELECT high.name FROM
		   (SELECT * FROM v47_products WHERE price > 500) AS high
		   JOIN (SELECT DISTINCT product_id FROM v47_orders WHERE status = 'completed') AS sold
		   ON high.id = sold.product_id
		 ORDER BY high.price DESC LIMIT 1`,
		"MacBook")

	// DJ3: Derived table with aggregate joined with regular table
	check("DJ3 derived aggregate JOIN",
		`SELECT cs.name, cs.order_count FROM
		   (SELECT customer_id, COUNT(*) AS order_count FROM v47_orders GROUP BY customer_id) AS co
		   JOIN v47_customers cs ON co.customer_id = cs.id
		 ORDER BY co.order_count DESC LIMIT 1`,
		"Alice") // 3 orders

	// ============================================================
	// === CTE WITH WINDOW FUNCTIONS ===
	// ============================================================

	// CW1: CTE with window function
	check("CW1 CTE with ROW_NUMBER",
		`WITH ranked AS (
		   SELECT name, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn
		   FROM v47_products
		 )
		 SELECT name FROM ranked WHERE rn = 1`,
		"MacBook")

	// CW2: CTE with window function and outer filter
	check("CW2 CTE window + filter",
		`WITH ranked AS (
		   SELECT name, price, category_id,
		          ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY price DESC) AS rn
		   FROM v47_products
		 )
		 SELECT name FROM ranked WHERE rn = 1 AND category_id = 2`,
		"iPhone") // Most expensive phone

	// CW3: CTE with RANK
	check("CW3 CTE RANK",
		`WITH customer_spending AS (
		   SELECT customer_id, SUM(total) AS total_spent
		   FROM v47_orders WHERE status = 'completed'
		   GROUP BY customer_id
		 )
		 SELECT total_spent FROM customer_spending ORDER BY total_spent DESC LIMIT 1`,
		3056) // Alice: 999+1999+58

	// ============================================================
	// === SAVEPOINT WITH DERIVED TABLES ===
	// ============================================================

	// SD1: Savepoint with derived table query
	checkNoError("SD1 BEGIN", "BEGIN")
	checkNoError("SD1 SAVEPOINT", "SAVEPOINT sd1")
	checkNoError("SD1 UPDATE stock", "UPDATE v47_products SET stock = stock - 5 WHERE id = 1")
	check("SD1 derived table sees update",
		`SELECT stock FROM (SELECT * FROM v47_products WHERE id = 1) AS t`, 45)
	checkNoError("SD1 ROLLBACK TO", "ROLLBACK TO SAVEPOINT sd1")
	check("SD1 stock restored",
		`SELECT stock FROM (SELECT * FROM v47_products WHERE id = 1) AS t`, 50)
	checkNoError("SD1 COMMIT", "COMMIT")

	// SD2: Savepoint with CTE query
	checkNoError("SD2 BEGIN", "BEGIN")
	checkNoError("SD2 SAVEPOINT", "SAVEPOINT sd2")
	checkNoError("SD2 INSERT order", "INSERT INTO v47_orders VALUES (9, 4, 1, 1, 999, 'completed')")
	check("SD2 CTE sees new order",
		`WITH recent AS (SELECT * FROM v47_orders WHERE id = 9)
		 SELECT COUNT(*) FROM recent`, 1)
	checkNoError("SD2 ROLLBACK TO", "ROLLBACK TO SAVEPOINT sd2")
	check("SD2 order removed",
		`WITH recent AS (SELECT * FROM v47_orders WHERE id = 9)
		 SELECT COUNT(*) FROM recent`, 0)
	checkNoError("SD2 COMMIT", "COMMIT")

	// ============================================================
	// === COMPLEX MULTI-TABLE QUERIES ===
	// ============================================================

	// MQ1: Three-table join with comma-FROM
	check("MQ1 three table comma join",
		`SELECT v47_customers.name
		 FROM v47_customers, v47_orders, v47_products
		 WHERE v47_customers.id = v47_orders.customer_id
		   AND v47_orders.product_id = v47_products.id
		   AND v47_products.price > 1000
		 ORDER BY v47_products.price DESC LIMIT 1`,
		"Alice") // MacBook at 1999

	// MQ2: Self-join through derived table (category hierarchy)
	checkRowCount("MQ2 category children",
		`SELECT child.name FROM v47_categories child
		 JOIN v47_categories parent ON child.parent_id = parent.id
		 WHERE parent.name = 'Electronics'`, 2) // Phones, Laptops

	// MQ3: Correlated subquery with derived table
	check("MQ3 correlated subquery",
		`SELECT name FROM v47_products p
		 WHERE price > (SELECT AVG(price) FROM v47_products WHERE category_id = p.category_id)
		 ORDER BY price DESC LIMIT 1`,
		"MacBook") // 1999 > avg(1999,1499)=1749

	// MQ4: EXISTS with CTE
	checkRowCount("MQ4 EXISTS with CTE",
		`WITH premium AS (SELECT id FROM v47_customers WHERE tier = 'premium')
		 SELECT * FROM v47_orders WHERE EXISTS (
		   SELECT 1 FROM premium WHERE premium.id = v47_orders.customer_id
		 )`, 5) // Alice(3) + Carol(2) = 5

	// ============================================================
	// === GROUP BY POSITIONAL WITH COMPLEX EXPRESSIONS ===
	// ============================================================

	// GP1: GROUP BY 1 with CASE expression
	check("GP1 GROUP BY 1 CASE",
		`SELECT CASE WHEN price > 500 THEN 'expensive' ELSE 'affordable' END AS tier,
		        COUNT(*), AVG(price)
		 FROM v47_products GROUP BY 1
		 ORDER BY AVG(price) DESC LIMIT 1`,
		"expensive") // iPhone(999), Galaxy(799), MacBook(1999), ThinkPad(1499)

	// GP2: ORDER BY 2 with GROUP BY 1
	check("GP2 ORDER BY 2 aggregate",
		`SELECT category_id, SUM(price) AS total
		 FROM v47_products GROUP BY 1 ORDER BY 2 DESC LIMIT 1`,
		3) // Laptops: 1999+1499=3498

	// GP3: GROUP BY 1 in CTE
	check("GP3 GROUP BY 1 in CTE",
		`WITH cat_totals AS (
		   SELECT category_id, SUM(stock) AS total_stock
		   FROM v47_products GROUP BY 1
		 )
		 SELECT total_stock FROM cat_totals ORDER BY total_stock DESC LIMIT 1`,
		350) // Shirts: 200+150=350

	// ============================================================
	// === EDGE CASES WITH NEW FEATURES ===
	// ============================================================

	// EC1: Empty derived table in JOIN
	checkRowCount("EC1 empty derived table JOIN",
		`SELECT * FROM v47_products p
		 JOIN (SELECT * FROM v47_orders WHERE status = 'nonexistent') AS empty
		 ON p.id = empty.product_id`, 0)

	// EC2: Derived table with LIMIT used in outer query
	check("EC2 derived table LIMIT in subquery",
		`SELECT COUNT(*) FROM (SELECT * FROM v47_products ORDER BY price DESC LIMIT 3) AS top3`,
		3)

	// EC3: CTE with comma-FROM (cross join between CTEs)
	check("EC3 CTE cross join",
		`WITH tiers AS (SELECT DISTINCT tier FROM v47_customers),
		      statuses AS (SELECT DISTINCT status FROM v47_orders)
		 SELECT COUNT(*) FROM tiers, statuses`,
		6) // 2 tiers * 3 statuses

	// EC4: Nested derived tables with aggregate
	check("EC4 nested derived aggregate",
		`SELECT max_price FROM (
		   SELECT MAX(price) AS max_price FROM (
		     SELECT price FROM v47_products WHERE category_id = 2
		   ) AS phones
		 ) AS result`,
		999) // iPhone

	// EC5: Derived table with ORDER BY and window function
	check("EC5 derived table window function",
		`SELECT name FROM (
		   SELECT name, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn
		   FROM v47_products
		 ) AS ranked WHERE rn = 2`,
		"ThinkPad") // Second most expensive

	// EC6: Multiple CTEs where later CTE uses earlier one
	check("EC6 CTE chain dependency",
		`WITH electronics AS (SELECT * FROM v47_products WHERE category_id IN (2, 3)),
		      expensive_electronics AS (SELECT * FROM electronics WHERE price > 900)
		 SELECT COUNT(*) FROM expensive_electronics`,
		3) // iPhone(999), MacBook(1999), ThinkPad(1499)

	// EC7: Derived table with DISTINCT and COUNT
	check("EC7 derived DISTINCT COUNT",
		`SELECT COUNT(*) FROM (SELECT DISTINCT customer_id FROM v47_orders) AS unique_customers`,
		4)

	// EC8: ORDER BY positional with alias
	check("EC8 ORDER BY positional alias",
		`SELECT name, price * stock AS inventory_value
		 FROM v47_products ORDER BY 2 DESC LIMIT 1`,
		"iPhone") // 999*50=49950 > 1999*20=39980

	// ============================================================
	// === TRANSACTION + FEATURE COMBOS ===
	// ============================================================

	// TF1: Savepoint with CTE and GROUP BY positional
	checkNoError("TF1 BEGIN", "BEGIN")
	checkNoError("TF1 SAVEPOINT", "SAVEPOINT tf1")
	checkNoError("TF1 UPDATE prices", "UPDATE v47_products SET price = price * 2 WHERE category_id = 2")
	check("TF1 CTE with GROUP BY 1 sees update",
		`WITH phone_prices AS (SELECT name, price FROM v47_products WHERE category_id = 2)
		 SELECT SUM(price) FROM phone_prices`,
		3596) // (999+799)*2
	checkNoError("TF1 ROLLBACK TO", "ROLLBACK TO SAVEPOINT tf1")
	check("TF1 prices restored",
		`SELECT SUM(price) FROM v47_products WHERE category_id = 2`, 1798) // 999+799
	checkNoError("TF1 COMMIT", "COMMIT")

	// TF2: Nested savepoints with derived tables
	checkNoError("TF2 BEGIN", "BEGIN")
	checkNoError("TF2 update1", "UPDATE v47_products SET stock = 0 WHERE id = 1")
	checkNoError("TF2 SAVEPOINT", "SAVEPOINT tf2")
	checkNoError("TF2 update2", "UPDATE v47_products SET stock = 0 WHERE id = 2")
	check("TF2 derived table sees both updates",
		`SELECT COUNT(*) FROM (SELECT * FROM v47_products WHERE stock = 0) AS out_of_stock`, 2)
	checkNoError("TF2 ROLLBACK TO", "ROLLBACK TO SAVEPOINT tf2")
	check("TF2 only first update kept",
		`SELECT COUNT(*) FROM (SELECT * FROM v47_products WHERE stock = 0) AS out_of_stock`, 1)
	checkNoError("TF2 ROLLBACK", "ROLLBACK") // Full rollback
	check("TF2 all restored",
		`SELECT stock FROM v47_products WHERE id = 1`, 50)

	// ============================================================
	// === CONSTRAINT VALIDATION ===
	// ============================================================

	// CV1: UNIQUE constraint
	checkError("CV1 UNIQUE violation",
		"INSERT INTO v47_customers VALUES (5, 'Eve', 'alice@test.com', 'standard')")

	// CV2: FK constraint
	checkError("CV2 FK violation",
		"INSERT INTO v47_orders VALUES (99, 999, 1, 1, 100, 'test')")

	// CV3: PK constraint on UPDATE
	checkError("CV3 PK violation UPDATE",
		"UPDATE v47_products SET id = 2 WHERE id = 1")

	// CV4: Column validation on UPDATE
	checkError("CV4 bad column UPDATE",
		"UPDATE v47_products SET nonexistent = 5 WHERE id = 1")

	// CV5: VALUES count validation
	checkError("CV5 wrong VALUES count",
		"INSERT INTO v47_categories VALUES (10, 'Test')")

	// CV6: INDEX on non-existent column
	checkError("CV6 bad column INDEX",
		"CREATE INDEX idx_bad ON v47_products(nonexistent)")

	// ============================================================
	// === REAL-WORLD ANALYTICS QUERIES ===
	// ============================================================

	// RW1: Revenue by category using derived table
	check("RW1 revenue by category",
		`SELECT cat_name FROM (
		   SELECT c.name AS cat_name, SUM(o.total) AS revenue
		   FROM v47_categories c
		   JOIN v47_products p ON p.category_id = c.id
		   JOIN v47_orders o ON o.product_id = p.id
		   WHERE o.status = 'completed'
		   GROUP BY c.name
		 ) AS cat_rev ORDER BY revenue DESC LIMIT 1`,
		"Phones") // iPhone: 999+1998=2997, vs Laptops:1999

	// RW2: Customer lifetime value with CTE
	check("RW2 customer LTV",
		`WITH clv AS (
		   SELECT customer_id, SUM(total) AS lifetime_value
		   FROM v47_orders WHERE status = 'completed'
		   GROUP BY customer_id
		 )
		 SELECT v47_customers.name FROM v47_customers
		 JOIN clv ON v47_customers.id = clv.customer_id
		 ORDER BY clv.lifetime_value DESC LIMIT 1`,
		"Alice") // 999+1999+58=3056

	// RW3: Product popularity ranking
	check("RW3 product popularity",
		`SELECT name FROM (
		   SELECT p.name, SUM(o.quantity) AS units_sold
		   FROM v47_products p
		   JOIN v47_orders o ON p.id = o.product_id
		   WHERE o.status = 'completed'
		   GROUP BY p.name
		 ) AS sales ORDER BY units_sold DESC LIMIT 1`,
		"T-Shirt") // T-Shirt(3+2=5) ties Polo(5), T-Shirt comes first

	// RW4: Category with no completed orders
	checkRowCount("RW4 categories without orders",
		`SELECT c.name FROM v47_categories c
		 WHERE NOT EXISTS (
		   SELECT 1 FROM v47_products p
		   JOIN v47_orders o ON p.id = o.product_id
		   WHERE p.category_id = c.id AND o.status = 'completed'
		 )`, 2) // Electronics(parent), Clothing(parent)

	// RW5: Average order value by tier
	check("RW5 avg order by tier",
		`SELECT tier FROM (
		   SELECT c.tier, AVG(o.total) AS avg_order
		   FROM v47_customers c
		   JOIN v47_orders o ON c.id = o.customer_id
		   WHERE o.status = 'completed'
		   GROUP BY c.tier
		 ) AS tier_stats ORDER BY avg_order DESC LIMIT 1`,
		"premium") // Premium: (999+1999+58+1998+245)/5=1059.8 vs Standard: 87/1=87

	t.Logf("\n=== V47 COMPREHENSIVE REGRESSION: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
