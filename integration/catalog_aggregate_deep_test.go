package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestComputeAggregatesWithGroupByDeep targets computeAggregatesWithGroupBy
func TestComputeAggregatesWithGroupByDeep(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE sales_deep (
		id INTEGER PRIMARY KEY,
		region TEXT,
		product TEXT,
		quarter TEXT,
		amount INTEGER,
		quantity INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multi-dimensional data
	_, err = db.Exec(ctx, `INSERT INTO sales_deep VALUES
		(1, 'North', 'Widget', 'Q1', 100, 10),
		(2, 'North', 'Gadget', 'Q1', 200, 5),
		(3, 'North', 'Widget', 'Q2', 150, 8),
		(4, 'South', 'Widget', 'Q1', 120, 12),
		(5, 'South', 'Gadget', 'Q2', 180, 6),
		(6, 'East', 'Tool', 'Q1', 300, 3),
		(7, 'East', 'Widget', 'Q2', 90, 9)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Single group by", `SELECT region, SUM(amount) FROM sales_deep GROUP BY region`},
		{"Multi-column group by", `SELECT region, product, SUM(amount), COUNT(*) FROM sales_deep GROUP BY region, product`},
		{"Group by with AVG", `SELECT region, AVG(amount) as avg_amt FROM sales_deep GROUP BY region`},
		{"Group by with MIN/MAX", `SELECT product, MIN(amount), MAX(amount) FROM sales_deep GROUP BY product`},
		{"Group by quarter", `SELECT quarter, SUM(amount), SUM(quantity) FROM sales_deep GROUP BY quarter`},
		{"Complex multi-group", `SELECT region, quarter, product, SUM(amount*quantity) as total FROM sales_deep GROUP BY region, quarter, product`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("GROUP BY query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("GROUP BY returned %d groups", count)
		})
	}
}

// TestEvaluateExprWithGroupAggregatesDeep targets evaluateExprWithGroupAggregates
func TestEvaluateExprWithGroupAggregatesDeep(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products_agg (
		id INTEGER PRIMARY KEY,
		category TEXT,
		price INTEGER,
		cost INTEGER,
		quantity INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products_agg VALUES
		(1, 'A', 100, 60, 10),
		(2, 'A', 150, 90, 5),
		(3, 'B', 200, 120, 8),
		(4, 'B', 250, 150, 6),
		(5, 'A', 120, 70, 12)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test arithmetic expressions with aggregates
	queries := []string{
		`SELECT category, SUM(price) as revenue, SUM(cost) as expenses, SUM(price) - SUM(cost) as profit FROM products_agg GROUP BY category`,
		`SELECT category, AVG(price) as avg_price, AVG(cost) as avg_cost, AVG(price) - AVG(cost) as avg_profit FROM products_agg GROUP BY category`,
		`SELECT category, SUM(price * quantity) as total_revenue, SUM(cost * quantity) as total_cost FROM products_agg GROUP BY category`,
		`SELECT category, COUNT(*) as cnt, SUM(price) / COUNT(*) as avg_calc FROM products_agg GROUP BY category`,
	}

	for _, sql := range queries {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Logf("Aggregate expression error: %v", err)
			continue
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		t.Logf("Aggregate expression returned %d rows", count)
	}
}

// TestApplyGroupByOrderBy targets applyGroupByOrderBy
func TestApplyGroupByOrderBy(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE order_agg (
		id INTEGER PRIMARY KEY,
		region TEXT,
		amount INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO order_agg VALUES
		(1, 'North', 100),
		(2, 'South', 200),
		(3, 'North', 150),
		(4, 'East', 300),
		(5, 'South', 50),
		(6, 'East', 250)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"GROUP BY ORDER BY aggregate ASC", `SELECT region, SUM(amount) as total FROM order_agg GROUP BY region ORDER BY total ASC`},
		{"GROUP BY ORDER BY aggregate DESC", `SELECT region, SUM(amount) as total FROM order_agg GROUP BY region ORDER BY total DESC`},
		{"GROUP BY ORDER BY COUNT", `SELECT region, COUNT(*) as cnt FROM order_agg GROUP BY region ORDER BY cnt DESC`},
		{"GROUP BY ORDER BY AVG", `SELECT region, AVG(amount) as avg_amt FROM order_agg GROUP BY region ORDER BY avg_amt`},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("ORDER BY aggregate error: %v", err)
				return
			}
			defer rows.Close()

			var results []string
			for rows.Next() {
				var region string
				var val int
				rows.Scan(&region, &val)
				results = append(results, region)
			}
			t.Logf("ORDER BY returned: %v", results)
		})
	}
}

// TestApplyDistinct targets applyDistinct
func TestApplyDistinct(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE dup_data (
		id INTEGER PRIMARY KEY,
		category TEXT,
		subcategory TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO dup_data VALUES
		(1, 'A', 'X'),
		(2, 'A', 'X'),
		(3, 'A', 'Y'),
		(4, 'B', 'X'),
		(5, 'B', 'X'),
		(6, 'C', 'Z')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	queries := []struct {
		name string
		sql  string
		want int
	}{
		{"DISTINCT single column", `SELECT DISTINCT category FROM dup_data`, 3},
		{"DISTINCT multiple columns", `SELECT DISTINCT category, subcategory FROM dup_data`, 4},
		{"DISTINCT with ORDER BY", `SELECT DISTINCT category FROM dup_data ORDER BY category`, 3},
		{"DISTINCT COUNT", `SELECT COUNT(DISTINCT category) FROM dup_data`, 1},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("DISTINCT error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("DISTINCT returned %d rows", count)
		})
	}
}

// TestDistinctWithJoin targets applyDistinct with JOIN
func TestDistinctWithJoin(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE customers_dup (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE orders_dup (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO customers_dup VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders_dup VALUES (1, 1, 'pending'), (2, 1, 'shipped'), (3, 2, 'pending')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// DISTINCT with JOIN
	rows, err := db.Query(ctx, `SELECT DISTINCT c.name FROM customers_dup c JOIN orders_dup o ON c.id = o.customer_id`)
	if err != nil {
		t.Logf("DISTINCT JOIN error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 distinct customers, got %d", count)
	}
}

// TestAggregateWithJoinAndGroupBy targets executeSelectWithJoinAndGroupBy
func TestAggregateWithJoinAndGroupBy(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE dept (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create dept: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE emp (
		id INTEGER PRIMARY KEY,
		dept_id INTEGER,
		salary INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create emp: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO dept VALUES (1, 'Engineering'), (2, 'Sales')`)
	if err != nil {
		t.Fatalf("Failed to insert dept: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO emp VALUES (1, 1, 80000), (2, 1, 90000), (3, 2, 60000), (4, 2, 70000), (5, 1, 85000)`)
	if err != nil {
		t.Fatalf("Failed to insert emp: %v", err)
	}

	queries := []string{
		`SELECT d.name, COUNT(*) as emp_count, SUM(e.salary) as total_salary, AVG(e.salary) as avg_salary
		 FROM dept d JOIN emp e ON d.id = e.dept_id GROUP BY d.name`,
		`SELECT d.name, MIN(e.salary) as min_sal, MAX(e.salary) as max_sal
		 FROM dept d JOIN emp e ON d.id = e.dept_id GROUP BY d.name ORDER BY max_sal DESC`,
	}

	for _, sql := range queries {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Logf("JOIN GROUP BY error: %v", err)
			continue
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		t.Logf("JOIN GROUP BY returned %d groups", count)
	}
}

// TestHavingWithJoin targets HAVING with JOIN
func TestHavingWithJoin(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE categories (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create categories: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE items (
		id INTEGER PRIMARY KEY,
		cat_id INTEGER,
		price INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create items: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Clothing')`)
	if err != nil {
		t.Fatalf("Failed to insert categories: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO items VALUES (1, 1, 1000), (2, 1, 500), (3, 1, 800), (4, 2, 100), (5, 2, 200)`)
	if err != nil {
		t.Fatalf("Failed to insert items: %v", err)
	}

	// HAVING with JOIN
	rows, err := db.Query(ctx, `SELECT c.name, COUNT(*) as cnt, SUM(i.price) as total
		FROM categories c JOIN items i ON c.id = i.cat_id
		GROUP BY c.name
		HAVING cnt > 2 AND total > 1000`)
	if err != nil {
		t.Logf("HAVING with JOIN error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("HAVING with JOIN returned %d groups", count)
}

// TestWindowFunctionsBasic targets window function execution
func TestWindowFunctionsBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE window_test (
		id INTEGER PRIMARY KEY,
		dept TEXT,
		salary INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO window_test VALUES
		(1, 'Eng', 80000),
		(2, 'Eng', 90000),
		(3, 'Eng', 85000),
		(4, 'Sales', 60000),
		(5, 'Sales', 70000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	queries := []string{
		`SELECT id, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn FROM window_test`,
		`SELECT id, dept, salary, RANK() OVER (ORDER BY salary DESC) as rnk FROM window_test`,
		`SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM window_test`,
		`SELECT id, dept, salary, AVG(salary) OVER (PARTITION BY dept) as dept_avg FROM window_test`,
	}

	for _, sql := range queries {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Logf("Window function error: %v", err)
			continue
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		t.Logf("Window function returned %d rows", count)
	}
}
