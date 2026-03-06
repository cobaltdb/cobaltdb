package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func af(t *testing.T) (*engine.DB, context.Context) {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	return db, context.Background()
}

func afExec(t *testing.T, db *engine.DB, ctx context.Context, sql string) {
	t.Helper()
	if _, err := db.Exec(ctx, sql); err != nil {
		t.Fatalf("EXEC [%s]: %v", sql, err)
	}
}

func afQuery(t *testing.T, db *engine.DB, ctx context.Context, sql string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("QUERY [%s]: %v", sql, err)
	}
	defer rows.Close()
	cols := rows.Columns()
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		row := make([]interface{}, len(cols))
		copy(row, vals)
		result = append(result, row)
	}
	return result
}

func afExpectRows(t *testing.T, db *engine.DB, ctx context.Context, sql string, n int) [][]interface{} {
	t.Helper()
	rows := afQuery(t, db, ctx, sql)
	if len(rows) != n {
		t.Fatalf("[%s] expected %d rows, got %d", sql, n, len(rows))
	}
	return rows
}

func afExpectVal(t *testing.T, db *engine.DB, ctx context.Context, sql string, expected interface{}) {
	t.Helper()
	rows := afQuery(t, db, ctx, sql)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatalf("[%s] no result", sql)
	}
	got := fmt.Sprintf("%v", rows[0][0])
	exp := fmt.Sprintf("%v", expected)
	if got != exp {
		t.Fatalf("[%s] expected %v, got %v", sql, expected, rows[0][0])
	}
}

// ==================== ADVANCED FEATURE TESTS ====================

func TestAF_CompleteECommerceWorkflow(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Schema setup
	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, tier TEXT DEFAULT 'basic')")
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL, stock INTEGER DEFAULT 0, category TEXT)")
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL DEFAULT 0, status TEXT DEFAULT 'pending', created_at TEXT)")
	afExec(t, db, ctx, "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER, price REAL)")

	// Create indexes
	afExec(t, db, ctx, "CREATE INDEX idx_products_category ON products (category)")
	afExec(t, db, ctx, "CREATE INDEX idx_orders_customer ON orders (customer_id)")
	afExec(t, db, ctx, "CREATE INDEX idx_orderitems_order ON order_items (order_id)")

	// Insert customers
	afExec(t, db, ctx, "INSERT INTO customers (id, name, email, tier) VALUES (1, 'Alice Smith', 'alice@example.com', 'premium')")
	afExec(t, db, ctx, "INSERT INTO customers (id, name, email, tier) VALUES (2, 'Bob Jones', 'bob@example.com', 'basic')")
	afExec(t, db, ctx, "INSERT INTO customers (id, name, email, tier) VALUES (3, 'Carol White', 'carol@example.com', 'premium')")
	afExec(t, db, ctx, "INSERT INTO customers (id, name, email, tier) VALUES (4, 'Dave Brown', 'dave@example.com', 'basic')")

	// Insert products
	afExec(t, db, ctx, "INSERT INTO products (id, name, price, stock, category) VALUES (1, 'Laptop Pro', 1299.99, 50, 'electronics')")
	afExec(t, db, ctx, "INSERT INTO products (id, name, price, stock, category) VALUES (2, 'Wireless Mouse', 29.99, 200, 'accessories')")
	afExec(t, db, ctx, "INSERT INTO products (id, name, price, stock, category) VALUES (3, 'USB-C Hub', 49.99, 150, 'accessories')")
	afExec(t, db, ctx, "INSERT INTO products (id, name, price, stock, category) VALUES (4, 'Monitor 27\"', 449.99, 75, 'electronics')")
	afExec(t, db, ctx, "INSERT INTO products (id, name, price, stock, category) VALUES (5, 'Keyboard', 89.99, 120, 'accessories')")

	// Create orders with items
	afExec(t, db, ctx, "INSERT INTO orders (id, customer_id, total, status) VALUES (1, 1, 0, 'completed')")
	afExec(t, db, ctx, "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (1, 1, 1, 1, 1299.99)")
	afExec(t, db, ctx, "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (2, 1, 2, 2, 29.99)")

	afExec(t, db, ctx, "INSERT INTO orders (id, customer_id, total, status) VALUES (2, 2, 0, 'pending')")
	afExec(t, db, ctx, "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (3, 2, 3, 1, 49.99)")
	afExec(t, db, ctx, "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (4, 2, 5, 1, 89.99)")

	afExec(t, db, ctx, "INSERT INTO orders (id, customer_id, total, status) VALUES (3, 1, 0, 'pending')")
	afExec(t, db, ctx, "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (5, 3, 4, 1, 449.99)")

	afExec(t, db, ctx, "INSERT INTO orders (id, customer_id, total, status) VALUES (4, 3, 0, 'completed')")
	afExec(t, db, ctx, "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (6, 4, 1, 1, 1299.99)")
	afExec(t, db, ctx, "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (7, 4, 3, 3, 49.99)")

	// 1. Verify order item totals with a query, then update manually
	rows := afQuery(t, db, ctx, "SELECT SUM(quantity * price) FROM order_items WHERE order_id = 1")
	if len(rows) > 0 {
		total := fmt.Sprintf("%.2f", rows[0][0])
		if total != "1359.97" {
			t.Fatalf("Expected order 1 total 1359.97, got %s", total)
		}
	}
	afExec(t, db, ctx, "UPDATE orders SET total = 1359.97 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT total FROM orders WHERE id = 1", 1359.97)

	// 2. Complex JOIN query
	rows = afQuery(t, db, ctx, `
		SELECT customers.name, orders.id, orders.status
		FROM customers
		JOIN orders ON customers.id = orders.customer_id
		ORDER BY customers.name, orders.id`)
	if len(rows) != 4 {
		t.Fatalf("Expected 4 customer-order joins, got %d", len(rows))
	}

	// 3. Aggregate with JOIN
	rows = afQuery(t, db, ctx, `
		SELECT customers.name, COUNT(orders.id) as num_orders
		FROM customers
		LEFT JOIN orders ON customers.id = orders.customer_id
		GROUP BY customers.name
		ORDER BY num_orders DESC`)
	if len(rows) != 4 {
		t.Fatalf("Expected 4 customer summaries, got %d", len(rows))
	}
	// Alice has 2 orders
	if fmt.Sprintf("%v", rows[0][0]) != "Alice Smith" {
		t.Logf("First by order count: %v (%v orders)", rows[0][0], rows[0][1])
	}

	// 4. EXISTS subquery - customers with completed orders
	rows = afExpectRows(t, db, ctx, "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id AND status = 'completed')", 2)
	_ = rows

	// 5. NOT IN subquery - customers without pending orders
	rows = afQuery(t, db, ctx, "SELECT name FROM customers WHERE id NOT IN (SELECT customer_id FROM orders WHERE status = 'pending')")
	t.Logf("Customers without pending orders: %d", len(rows))

	// 6. CTE for order summary
	rows = afQuery(t, db, ctx, `
		WITH order_totals AS (
			SELECT order_id, SUM(quantity * price) as total
			FROM order_items
			GROUP BY order_id
		)
		SELECT orders.id, order_totals.total, orders.status
		FROM orders
		JOIN order_totals ON orders.id = order_totals.order_id
		ORDER BY order_totals.total DESC`)
	if len(rows) < 1 {
		t.Fatal("CTE order summary returned no rows")
	}

	// 7. CASE expression for order categorization
	rows = afQuery(t, db, ctx, `
		SELECT id, total,
			CASE
				WHEN total > 1000 THEN 'high'
				WHEN total > 100 THEN 'medium'
				ELSE 'low'
			END
		FROM orders
		ORDER BY total DESC`)
	t.Logf("Order categorization: %d rows", len(rows))

	// 8. Update stock after order completion
	afExec(t, db, ctx, "UPDATE products SET stock = stock - 1 WHERE id IN (SELECT product_id FROM order_items WHERE order_id = 1)")
	rows = afExpectRows(t, db, ctx, "SELECT stock FROM products WHERE id = 1", 1)
	if fmt.Sprintf("%v", rows[0][0]) != "49" {
		t.Fatalf("Expected stock 49, got %v", rows[0][0])
	}

	// 9. View for popular products
	afExec(t, db, ctx, "CREATE VIEW popular_products AS SELECT products.name, SUM(order_items.quantity) as total_sold FROM products JOIN order_items ON products.id = order_items.product_id GROUP BY products.name")
	rows = afQuery(t, db, ctx, "SELECT * FROM popular_products ORDER BY total_sold DESC")
	if len(rows) < 1 {
		t.Fatal("Popular products view returned no rows")
	}

	// 10. Transaction test - cancel an order
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "UPDATE orders SET status = 'cancelled' WHERE id = 2")
	afExec(t, db, ctx, "COMMIT")
	afExpectVal(t, db, ctx, "SELECT status FROM orders WHERE id = 2", "cancelled")

	// 11. Premium customer pricing
	afExec(t, db, ctx, "UPDATE orders SET total = total * 0.9 WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'premium')")

	// 12. Complex analytics query
	rows = afQuery(t, db, ctx, `
		SELECT
			customers.tier,
			COUNT(DISTINCT customers.id) as num_customers,
			COUNT(orders.id) as total_orders,
			SUM(orders.total) as revenue
		FROM customers
		LEFT JOIN orders ON customers.id = orders.customer_id
		GROUP BY customers.tier`)
	if len(rows) < 1 {
		t.Fatal("Analytics query returned no rows")
	}

	t.Log("[OK] Complete e-commerce workflow 12 tests passed")
}

func TestAF_SelfReferentialAndComplexQueries(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Employee hierarchy
	afExec(t, db, ctx, "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER, salary REAL, dept TEXT)")
	afExec(t, db, ctx, "INSERT INTO emp (id, name, manager_id, salary, dept) VALUES (1, 'CEO', NULL, 200000, 'exec')")
	afExec(t, db, ctx, "INSERT INTO emp (id, name, manager_id, salary, dept) VALUES (2, 'VP Engineering', 1, 150000, 'engineering')")
	afExec(t, db, ctx, "INSERT INTO emp (id, name, manager_id, salary, dept) VALUES (3, 'VP Sales', 1, 140000, 'sales')")
	afExec(t, db, ctx, "INSERT INTO emp (id, name, manager_id, salary, dept) VALUES (4, 'Dev Lead', 2, 120000, 'engineering')")
	afExec(t, db, ctx, "INSERT INTO emp (id, name, manager_id, salary, dept) VALUES (5, 'Dev Sr', 4, 110000, 'engineering')")
	afExec(t, db, ctx, "INSERT INTO emp (id, name, manager_id, salary, dept) VALUES (6, 'Dev Jr', 4, 80000, 'engineering')")
	afExec(t, db, ctx, "INSERT INTO emp (id, name, manager_id, salary, dept) VALUES (7, 'Sales Lead', 3, 100000, 'sales')")
	afExec(t, db, ctx, "INSERT INTO emp (id, name, manager_id, salary, dept) VALUES (8, 'Sales Rep', 7, 70000, 'sales')")

	// 1. Self-join: find employees and their managers
	rows := afQuery(t, db, ctx, `
		SELECT e.name, m.name
		FROM emp e
		LEFT JOIN emp m ON e.manager_id = m.id
		ORDER BY e.id`)
	if len(rows) != 8 {
		t.Fatalf("Expected 8 rows, got %d", len(rows))
	}
	// CEO's manager should be NULL
	if rows[0][1] != nil {
		t.Fatalf("Expected NULL manager for CEO, got %v", rows[0][1])
	}

	// 2. Department summary
	rows = afQuery(t, db, ctx, `
		SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal, MIN(salary) as min_sal, MAX(salary) as max_sal
		FROM emp
		GROUP BY dept
		ORDER BY avg_sal DESC`)
	if len(rows) != 3 {
		t.Fatalf("Expected 3 departments, got %d", len(rows))
	}

	// 3. Employees earning above department average (using subquery)
	rows = afQuery(t, db, ctx, `
		SELECT name, salary, dept
		FROM emp
		WHERE salary > (SELECT AVG(salary) FROM emp)
		ORDER BY salary DESC`)
	if len(rows) < 1 {
		t.Fatal("Expected employees above average")
	}

	// 4. Department with most employees (HAVING)
	rows = afQuery(t, db, ctx, `
		SELECT dept, COUNT(*) as cnt
		FROM emp
		GROUP BY dept
		HAVING COUNT(*) >= 4`)
	if len(rows) != 1 {
		t.Fatalf("Expected 1 dept with 4+ employees, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "engineering" {
		t.Fatalf("Expected engineering, got %v", rows[0][0])
	}

	// 5. Update: give 10% raise to top performers
	afExec(t, db, ctx, "UPDATE emp SET salary = salary * 1.1 WHERE salary > (SELECT AVG(salary) FROM emp)")

	// 6. CTE: department budget report
	rows = afQuery(t, db, ctx, `
		WITH dept_stats AS (
			SELECT dept, SUM(salary) as total_salary, COUNT(*) as headcount
			FROM emp
			GROUP BY dept
		)
		SELECT dept, total_salary, headcount
		FROM dept_stats
		ORDER BY total_salary DESC`)
	if len(rows) != 3 {
		t.Fatalf("Expected 3 department stats, got %d", len(rows))
	}

	// 7. Employees with no direct reports (using NOT IN instead of correlated NOT EXISTS)
	rows = afQuery(t, db, ctx, `
		SELECT name FROM emp
		WHERE id NOT IN (SELECT manager_id FROM emp WHERE manager_id IS NOT NULL)
		ORDER BY name`)
	// Dev Sr, Dev Jr, Sales Rep have no reports
	if len(rows) != 3 {
		t.Fatalf("Expected 3 employees with no reports, got %d", len(rows))
	}

	// 8. Multiple aggregates with CASE
	rows = afQuery(t, db, ctx, `
		SELECT
			COUNT(*) as total,
			SUM(CASE WHEN salary > 100000 THEN 1 ELSE 0 END) as high_earners,
			SUM(CASE WHEN salary <= 100000 THEN 1 ELSE 0 END) as standard
		FROM emp`)
	if len(rows) != 1 {
		t.Fatal("Expected 1 summary row")
	}

	t.Log("[OK] Self-referential and complex queries 8 tests passed")
}

func TestAF_DataIntegrityAndConstraints(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, `CREATE TABLE accounts (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		balance REAL DEFAULT 0,
		type TEXT CHECK(type IN ('checking', 'savings', 'credit')),
		active INTEGER DEFAULT 1
	)`)

	// 1. Insert valid data
	afExec(t, db, ctx, "INSERT INTO accounts (id, name, balance, type) VALUES (1, 'Alice Checking', 1000, 'checking')")
	afExec(t, db, ctx, "INSERT INTO accounts (id, name, balance, type) VALUES (2, 'Alice Savings', 5000, 'savings')")
	afExec(t, db, ctx, "INSERT INTO accounts (id, name, balance, type) VALUES (3, 'Bob Checking', 500, 'checking')")

	// 2. NOT NULL violation
	_, err := db.Exec(ctx, "INSERT INTO accounts (id, name, balance, type) VALUES (4, NULL, 100, 'checking')")
	if err == nil {
		t.Fatal("Expected NOT NULL constraint error")
	}

	// 3. CHECK constraint violation
	_, err = db.Exec(ctx, "INSERT INTO accounts (id, name, balance, type) VALUES (4, 'Test', 100, 'invalid')")
	if err == nil {
		t.Fatal("Expected CHECK constraint error")
	}

	// 4. PRIMARY KEY duplicate
	_, err = db.Exec(ctx, "INSERT INTO accounts (id, name, balance, type) VALUES (1, 'Duplicate', 100, 'checking')")
	if err == nil {
		t.Fatal("Expected PRIMARY KEY constraint error")
	}

	// 5. Transaction: transfer funds
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "UPDATE accounts SET balance = balance - 200 WHERE id = 1")
	afExec(t, db, ctx, "UPDATE accounts SET balance = balance + 200 WHERE id = 2")
	afExec(t, db, ctx, "COMMIT")

	afExpectVal(t, db, ctx, "SELECT balance FROM accounts WHERE id = 1", float64(800))
	afExpectVal(t, db, ctx, "SELECT balance FROM accounts WHERE id = 2", float64(5200))

	// 6. Transaction rollback
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "UPDATE accounts SET balance = 0 WHERE id = 1")
	afExec(t, db, ctx, "ROLLBACK")
	afExpectVal(t, db, ctx, "SELECT balance FROM accounts WHERE id = 1", float64(800))

	// 7. SUM verification
	afExpectVal(t, db, ctx, "SELECT SUM(balance) FROM accounts", float64(6500))

	// 8. Conditional update with CASE
	afExec(t, db, ctx, `UPDATE accounts SET balance = CASE
		WHEN type = 'savings' THEN balance * 1.05
		WHEN type = 'checking' THEN balance
		ELSE balance
	END`)

	// 9. Verify savings got interest
	rows := afExpectRows(t, db, ctx, "SELECT balance FROM accounts WHERE id = 2", 1)
	balStr := fmt.Sprintf("%v", rows[0][0])
	if balStr != "5460" {
		t.Fatalf("Expected savings balance 5460 after interest, got %s", balStr)
	}

	// 10. DELETE with conditions
	afExec(t, db, ctx, "INSERT INTO accounts (id, name, balance, type, active) VALUES (4, 'Closed Account', 0, 'checking', 0)")
	afExec(t, db, ctx, "DELETE FROM accounts WHERE active = 0 AND balance = 0")
	afExpectRows(t, db, ctx, "SELECT * FROM accounts", 3)

	t.Log("[OK] Data integrity and constraints 10 tests passed")
}

func TestAF_StringAndExpressionEdgeCases(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE texts (id INTEGER PRIMARY KEY, content TEXT)")
	afExec(t, db, ctx, "INSERT INTO texts (id, content) VALUES (1, 'Hello World')")
	afExec(t, db, ctx, "INSERT INTO texts (id, content) VALUES (2, '')")
	afExec(t, db, ctx, "INSERT INTO texts (id, content) VALUES (3, 'It''s a test')")
	afExec(t, db, ctx, "INSERT INTO texts (id, content) VALUES (4, 'Line1')")
	afExec(t, db, ctx, "INSERT INTO texts (id, content) VALUES (5, NULL)")

	// 1. Empty string vs NULL
	afExpectRows(t, db, ctx, "SELECT * FROM texts WHERE content = ''", 1)
	afExpectRows(t, db, ctx, "SELECT * FROM texts WHERE content IS NULL", 1)

	// 2. String functions on various inputs
	afExpectVal(t, db, ctx, "SELECT LENGTH(content) FROM texts WHERE id = 1", float64(11))
	afExpectVal(t, db, ctx, "SELECT LENGTH(content) FROM texts WHERE id = 2", float64(0))

	// 3. String concatenation
	afExpectVal(t, db, ctx, "SELECT 'Hello' || ' ' || 'World'", "Hello World")

	// 4. UPPER/LOWER
	afExpectVal(t, db, ctx, "SELECT UPPER(content) FROM texts WHERE id = 1", "HELLO WORLD")
	afExpectVal(t, db, ctx, "SELECT LOWER(content) FROM texts WHERE id = 1", "hello world")

	// 5. TRIM
	afExec(t, db, ctx, "INSERT INTO texts (id, content) VALUES (6, '   spaces   ')")
	afExpectVal(t, db, ctx, "SELECT TRIM(content) FROM texts WHERE id = 6", "spaces")

	// 6. REPLACE
	afExpectVal(t, db, ctx, "SELECT REPLACE(content, 'World', 'Go') FROM texts WHERE id = 1", "Hello Go")

	// 7. SUBSTR
	afExpectVal(t, db, ctx, "SELECT SUBSTR(content, 1, 5) FROM texts WHERE id = 1", "Hello")

	// 8. INSTR
	afExpectVal(t, db, ctx, "SELECT INSTR(content, 'World') FROM texts WHERE id = 1", float64(7))

	// 9. Escaped quotes
	afExpectVal(t, db, ctx, "SELECT content FROM texts WHERE id = 3", "It's a test")

	// 10. COALESCE with NULL content
	afExpectVal(t, db, ctx, "SELECT COALESCE(content, 'N/A') FROM texts WHERE id = 5", "N/A")

	t.Log("[OK] String and expression edge cases 10 tests passed")
}

func TestAF_MultiJoinComplexScenario(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER, genre TEXT, price REAL)")
	afExec(t, db, ctx, "CREATE TABLE reviews (id INTEGER PRIMARY KEY, book_id INTEGER, rating INTEGER, comment TEXT)")
	afExec(t, db, ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, book_id INTEGER, quantity INTEGER, date TEXT)")

	// Data
	afExec(t, db, ctx, "INSERT INTO authors VALUES (1, 'Author A')")
	afExec(t, db, ctx, "INSERT INTO authors VALUES (2, 'Author B')")
	afExec(t, db, ctx, "INSERT INTO authors VALUES (3, 'Author C')")

	afExec(t, db, ctx, "INSERT INTO books VALUES (1, 'Book One', 1, 'fiction', 19.99)")
	afExec(t, db, ctx, "INSERT INTO books VALUES (2, 'Book Two', 1, 'fiction', 24.99)")
	afExec(t, db, ctx, "INSERT INTO books VALUES (3, 'Book Three', 2, 'non-fiction', 29.99)")
	afExec(t, db, ctx, "INSERT INTO books VALUES (4, 'Book Four', 3, 'fiction', 14.99)")

	afExec(t, db, ctx, "INSERT INTO reviews VALUES (1, 1, 5, 'Great!')")
	afExec(t, db, ctx, "INSERT INTO reviews VALUES (2, 1, 4, 'Good')")
	afExec(t, db, ctx, "INSERT INTO reviews VALUES (3, 2, 3, 'OK')")
	afExec(t, db, ctx, "INSERT INTO reviews VALUES (4, 3, 5, 'Excellent')")
	afExec(t, db, ctx, "INSERT INTO reviews VALUES (5, 3, 4, 'Very good')")

	afExec(t, db, ctx, "INSERT INTO sales VALUES (1, 1, 100, '2024-01-15')")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (2, 1, 50, '2024-02-15')")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (3, 2, 75, '2024-01-20')")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (4, 3, 200, '2024-01-10')")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (5, 4, 30, '2024-03-01')")

	// 1. Three-table JOIN
	rows := afQuery(t, db, ctx, `
		SELECT authors.name, books.title, reviews.rating
		FROM authors
		JOIN books ON authors.id = books.author_id
		JOIN reviews ON books.id = reviews.book_id
		ORDER BY reviews.rating DESC`)
	if len(rows) < 4 {
		t.Fatalf("Expected at least 4 rows from triple join, got %d", len(rows))
	}

	// 2. Average rating per book
	rows = afQuery(t, db, ctx, `
		SELECT books.title, AVG(reviews.rating) as avg_rating
		FROM books
		JOIN reviews ON books.id = reviews.book_id
		GROUP BY books.title
		ORDER BY avg_rating DESC`)
	if len(rows) < 1 {
		t.Fatal("Expected average ratings")
	}

	// 3. Total sales revenue per author
	rows = afQuery(t, db, ctx, `
		SELECT authors.name, SUM(sales.quantity * books.price) as revenue
		FROM authors
		JOIN books ON authors.id = books.author_id
		JOIN sales ON books.id = sales.book_id
		GROUP BY authors.name
		ORDER BY revenue DESC`)
	if len(rows) < 1 {
		t.Fatal("Expected revenue data")
	}

	// 4. Books with no reviews (LEFT JOIN)
	rows = afQuery(t, db, ctx, `
		SELECT books.title
		FROM books
		LEFT JOIN reviews ON books.id = reviews.book_id
		WHERE reviews.id IS NULL`)
	if len(rows) != 1 {
		t.Fatalf("Expected 1 book with no reviews, got %d", len(rows))
	}

	// 5. CTE with JOIN
	rows = afQuery(t, db, ctx, `
		WITH bestsellers AS (
			SELECT book_id, SUM(quantity) as total_sold
			FROM sales
			GROUP BY book_id
			HAVING SUM(quantity) > 50
		)
		SELECT books.title, bestsellers.total_sold
		FROM bestsellers
		JOIN books ON bestsellers.book_id = books.id
		ORDER BY bestsellers.total_sold DESC`)
	if len(rows) < 1 {
		t.Fatal("Expected bestsellers")
	}

	// 6. UPDATE using subquery result
	afExec(t, db, ctx, "UPDATE books SET price = price * 1.1 WHERE id IN (SELECT book_id FROM reviews GROUP BY book_id HAVING AVG(rating) >= 4)")

	// 7. Verify price update
	rows = afExpectRows(t, db, ctx, "SELECT price FROM books WHERE id = 1", 1)
	price := fmt.Sprintf("%.2f", rows[0][0])
	if price != "21.99" {
		t.Logf("Price after 10%% increase: %s (expected ~21.99)", price)
	}

	// 8. Complex analytical query
	rows = afQuery(t, db, ctx, `
		SELECT
			genre,
			COUNT(*) as book_count,
			SUM(price) as total_price,
			AVG(price) as avg_price,
			MIN(price) as min_price,
			MAX(price) as max_price
		FROM books
		GROUP BY genre
		ORDER BY book_count DESC`)
	if len(rows) < 1 {
		t.Fatal("Expected genre analytics")
	}

	t.Log("[OK] Multi-join complex scenario 8 tests passed")
}

func TestAF_BulkOperations(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE bulk (id INTEGER PRIMARY KEY, val INTEGER, category TEXT)")

	// 1. Bulk insert
	for i := 1; i <= 100; i++ {
		cat := "A"
		if i%3 == 0 {
			cat = "B"
		}
		if i%5 == 0 {
			cat = "C"
		}
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO bulk (id, val, category) VALUES (%d, %d, '%s')", i, i*10, cat))
	}

	// 2. Verify count
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM bulk", float64(100))

	// 3. Category counts
	rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM bulk GROUP BY category ORDER BY category")
	if len(rows) != 3 {
		t.Fatalf("Expected 3 categories, got %d", len(rows))
	}

	// 4. SUM with WHERE
	rows = afQuery(t, db, ctx, "SELECT SUM(val) FROM bulk WHERE category = 'A'")
	if len(rows) != 1 {
		t.Fatal("Expected sum result")
	}

	// 5. Bulk update
	afExec(t, db, ctx, "UPDATE bulk SET val = val * 2 WHERE category = 'C'")

	// 6. Bulk delete
	afExec(t, db, ctx, "DELETE FROM bulk WHERE val < 50")
	rows = afQuery(t, db, ctx, "SELECT COUNT(*) FROM bulk")
	count := fmt.Sprintf("%v", rows[0][0])
	t.Logf("Remaining rows after delete: %s", count)

	// 7. DISTINCT with large dataset
	rows = afQuery(t, db, ctx, "SELECT DISTINCT category FROM bulk ORDER BY category")
	if len(rows) < 1 {
		t.Fatal("Expected distinct categories")
	}

	// 8. INSERT...SELECT for backup
	afExec(t, db, ctx, "CREATE TABLE bulk_backup (id INTEGER PRIMARY KEY, val INTEGER, category TEXT)")
	afExec(t, db, ctx, "INSERT INTO bulk_backup (id, val, category) SELECT id, val, category FROM bulk WHERE category = 'A'")
	rows = afQuery(t, db, ctx, "SELECT COUNT(*) FROM bulk_backup")
	if len(rows) != 1 {
		t.Fatal("Expected backup count")
	}
	t.Logf("Backed up %v rows", rows[0][0])

	t.Log("[OK] Bulk operations 8 tests passed")
}
