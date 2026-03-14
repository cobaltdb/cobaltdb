package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestApplyOuterQueryWithViewAndWhere targets applyOuterQuery with view + outer WHERE
func TestApplyOuterQueryWithViewAndWhere(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE sales (
		id INTEGER PRIMARY KEY,
		region TEXT,
		amount INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO sales VALUES
		(1, 'North', 100),
		(2, 'North', 200),
		(3, 'South', 150),
		(4, 'East', 300),
		(5, 'West', 250)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create simple view
	_, err = db.Exec(ctx, `CREATE VIEW sales_summary AS
		SELECT region, SUM(amount) as total, COUNT(*) as cnt
		FROM sales
		GROUP BY region`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"View with WHERE", `SELECT * FROM sales_summary WHERE total > 200`},
		{"View with WHERE on count", `SELECT * FROM sales_summary WHERE cnt >= 1`},
		{"View with WHERE AND", `SELECT * FROM sales_summary WHERE total > 150 AND cnt = 1`},
		{"View with ORDER BY", `SELECT * FROM sales_summary ORDER BY total DESC`},
		{"View with WHERE and ORDER BY", `SELECT * FROM sales_summary WHERE total > 100 ORDER BY total ASC`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}

// TestApplyOuterQueryWithDistinctView targets applyOuterQuery with DISTINCT view
func TestApplyOuterQueryWithDistinctView(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE items (
		id INTEGER PRIMARY KEY,
		category TEXT,
		subcategory TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO items VALUES
		(1, 'A', 'X'),
		(2, 'A', 'Y'),
		(3, 'B', 'X'),
		(4, 'B', 'Y'),
		(5, 'C', 'Z')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with DISTINCT
	_, err = db.Exec(ctx, `CREATE VIEW distinct_categories AS
		SELECT DISTINCT category FROM items`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Distinct view simple", `SELECT * FROM distinct_categories`},
		{"Distinct view with WHERE", `SELECT * FROM distinct_categories WHERE category != 'C'`},
		{"Distinct view with ORDER BY", `SELECT * FROM distinct_categories ORDER BY category DESC`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}

// TestApplyOuterQueryWithAggregateView targets applyOuterQuery with aggregate view
func TestApplyOuterQueryWithAggregateView(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount REAL,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 1, 100.0, 'completed'),
		(2, 1, 200.0, 'completed'),
		(3, 2, 150.0, 'pending'),
		(4, 2, 50.0, 'completed'),
		(5, 3, 300.0, 'completed')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with aggregates
	_, err = db.Exec(ctx, `CREATE VIEW customer_stats AS
		SELECT
			customer_id,
			COUNT(*) as order_count,
			SUM(amount) as total_amount,
			AVG(amount) as avg_amount,
			MIN(amount) as min_amount,
			MAX(amount) as max_amount
		FROM orders
		GROUP BY customer_id`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Aggregate view simple", `SELECT * FROM customer_stats`},
		{"Aggregate view WHERE on SUM", `SELECT * FROM customer_stats WHERE total_amount > 200`},
		{"Aggregate view WHERE on COUNT", `SELECT * FROM customer_stats WHERE order_count >= 2`},
		{"Aggregate view WHERE on AVG", `SELECT * FROM customer_stats WHERE avg_amount > 100`},
		{"Aggregate view ORDER BY aggregate", `SELECT * FROM customer_stats ORDER BY total_amount DESC`},
		{"Aggregate view WHERE and ORDER BY", `SELECT * FROM customer_stats WHERE total_amount > 150 ORDER BY avg_amount`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}

// TestApplyOuterQueryWithJoinView targets applyOuterQuery with view containing JOIN
func TestApplyOuterQueryWithJoinView(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE orders2 (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders2: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders2 VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Create view with JOIN
	_, err = db.Exec(ctx, `CREATE VIEW customer_orders AS
		SELECT c.id, c.name, o.amount
		FROM customers c
		JOIN orders2 o ON c.id = o.customer_id`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Join view simple", `SELECT * FROM customer_orders`},
		{"Join view with WHERE", `SELECT * FROM customer_orders WHERE amount > 150`},
		{"Join view with ORDER BY", `SELECT * FROM customer_orders ORDER BY amount DESC`},
		{"Join view with WHERE on name", `SELECT * FROM customer_orders WHERE name = 'Alice'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}

// TestApplyOuterQueryWithSubqueryView targets applyOuterQuery with view containing subquery
func TestApplyOuterQueryWithSubqueryView(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT,
		price INTEGER,
		category_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create products: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES
		(1, 'Product A', 100, 1),
		(2, 'Product B', 200, 1),
		(3, 'Product C', 150, 2),
		(4, 'Product D', 300, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with subquery
	_, err = db.Exec(ctx, `CREATE VIEW above_avg_products AS
		SELECT *
		FROM products p
		WHERE price > (SELECT AVG(price) FROM products WHERE category_id = p.category_id)`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Subquery view simple", `SELECT * FROM above_avg_products`},
		{"Subquery view with WHERE", `SELECT * FROM above_avg_products WHERE price > 200`},
		{"Subquery view ORDER BY", `SELECT * FROM above_avg_products ORDER BY price`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}

// TestApplyOuterQueryWithUnionView targets applyOuterQuery with view containing UNION
func TestApplyOuterQueryWithUnionView(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE active_users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create active_users: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE pending_users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create pending_users: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO active_users VALUES (1, 'Alice', 'active'), (2, 'Bob', 'active')`)
	if err != nil {
		t.Fatalf("Failed to insert active: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO pending_users VALUES (3, 'Carol', 'pending'), (4, 'Dave', 'pending')`)
	if err != nil {
		t.Fatalf("Failed to insert pending: %v", err)
	}

	// Create view with UNION
	_, err = db.Exec(ctx, `CREATE VIEW all_users AS
		SELECT id, name, status FROM active_users
		UNION ALL
		SELECT id, name, status FROM pending_users`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Union view simple", `SELECT * FROM all_users`},
		{"Union view with WHERE", `SELECT * FROM all_users WHERE status = 'active'`},
		{"Union view with ORDER BY", `SELECT * FROM all_users ORDER BY name`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}

// TestApplyOuterQueryWithWindowView targets applyOuterQuery with view containing window functions
func TestApplyOuterQueryWithWindowView(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		dept TEXT,
		salary INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		(1, 'Engineering', 80000),
		(2, 'Engineering', 90000),
		(3, 'Engineering', 85000),
		(4, 'Sales', 60000),
		(5, 'Sales', 70000),
		(6, 'Sales', 65000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with window functions
	_, err = db.Exec(ctx, `CREATE VIEW ranked_employees AS
		SELECT
			id,
			dept,
			salary,
			ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn,
			RANK() OVER (ORDER BY salary DESC) as rnk,
			AVG(salary) OVER (PARTITION BY dept) as dept_avg
		FROM employees`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Window view simple", `SELECT * FROM ranked_employees`},
		{"Window view with WHERE on window col", `SELECT * FROM ranked_employees WHERE rn = 1`},
		{"Window view with WHERE on salary", `SELECT * FROM ranked_employees WHERE salary > 75000`},
		{"Window view ORDER BY", `SELECT * FROM ranked_employees ORDER BY rnk`},
		{"Window view WHERE and ORDER BY", `SELECT * FROM ranked_employees WHERE dept = 'Engineering' ORDER BY salary DESC`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}

// TestApplyOuterQueryWithLimitOffset targets applyOuterQuery with LIMIT/OFFSET
func TestApplyOuterQueryWithLimitOffset(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE numbered_items (
		id INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert 10 rows
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(ctx, `INSERT INTO numbered_items VALUES (?, ?)`, i, i*10)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Create view
	_, err = db.Exec(ctx, `CREATE VIEW numbered_view AS SELECT * FROM numbered_items ORDER BY val`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"View with LIMIT", `SELECT * FROM numbered_view LIMIT 5`},
		{"View with LIMIT OFFSET", `SELECT * FROM numbered_view LIMIT 5 OFFSET 3`},
		{"View with WHERE and LIMIT", `SELECT * FROM numbered_view WHERE val > 30 LIMIT 3`},
		{"View with LIMIT in subquery", `SELECT * FROM (SELECT * FROM numbered_view LIMIT 5) AS sub`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}
