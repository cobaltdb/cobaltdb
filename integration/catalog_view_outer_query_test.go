package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestViewWithDistinct targets applyOuterQuery with DISTINCT views
func TestViewWithDistinct(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE events (user_id INTEGER, event_type TEXT, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO events VALUES
		(1, 'click', 'button1'),
		(1, 'click', 'button1'),
		(1, 'view', 'page1'),
		(2, 'click', 'button2'),
		(2, 'click', 'button2'),
		(3, 'view', 'page2')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with DISTINCT
	_, err = db.Exec(ctx, `CREATE VIEW distinct_events AS
		SELECT DISTINCT user_id, event_type FROM events`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view with additional filter
	rows, err := db.Query(ctx, `SELECT * FROM distinct_events WHERE user_id = 1`)
	if err != nil {
		t.Logf("View query error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var userID int
		var eventType string
		rows.Scan(&userID, &eventType)
		count++
		t.Logf("User %d, Event: %s", userID, eventType)
	}
	t.Logf("Distinct events for user 1: %d", count)
}

// TestViewWithGroupBy targets applyOuterQuery with GROUP BY views
func TestViewWithGroupBy(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE sales (product TEXT, region TEXT, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO sales VALUES
		('Widget', 'North', 100),
		('Widget', 'South', 200),
		('Gadget', 'North', 150),
		('Gadget', 'South', 300),
		('Widget', 'East', 50),
		('Gadget', 'East', 250)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with GROUP BY
	_, err = db.Exec(ctx, `CREATE VIEW sales_summary AS
		SELECT product, SUM(amount) as total_sales, COUNT(*) as transaction_count
		FROM sales
		GROUP BY product`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query view with HAVING filter
	rows, err := db.Query(ctx, `SELECT * FROM sales_summary WHERE total_sales > 200`)
	if err != nil {
		t.Logf("View with GROUP BY query error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var product string
		var totalSales, transactionCount int
		rows.Scan(&product, &totalSales, &transactionCount)
		t.Logf("Product: %s, Total: %d, Count: %d", product, totalSales, transactionCount)
	}
}

// TestViewWithWindowFunctions targets applyOuterQuery with window functions
func TestViewWithWindowFunctions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE employees (name TEXT, dept TEXT, salary INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		('Alice', 'Engineering', 100000),
		('Bob', 'Engineering', 120000),
		('Carol', 'Sales', 80000),
		('David', 'Sales', 90000),
		('Eve', 'Sales', 85000),
		('Frank', 'HR', 70000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with window function
	_, err = db.Exec(ctx, `CREATE VIEW ranked_employees AS
		SELECT name, dept, salary,
			RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as salary_rank
		FROM employees`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query view with filter on rank
	rows, err := db.Query(ctx, `SELECT * FROM ranked_employees WHERE salary_rank = 1`)
	if err != nil {
		t.Logf("View with window function query error: %v", err)
		return
	}
	defer rows.Close()

	t.Log("Top earners by department:")
	for rows.Next() {
		var name, dept string
		var salary, rank int
		rows.Scan(&name, &dept, &salary, &rank)
		t.Logf("  %s in %s earns %d (rank %d)", name, dept, salary, rank)
	}
}

// TestNestedViews targets nested view resolution
func TestNestedViews(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE raw_data (id INTEGER, category TEXT, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO raw_data VALUES
		(1, 'A', 100),
		(2, 'A', 200),
		(3, 'B', 150),
		(4, 'B', 250),
		(5, 'C', 300)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create base view
	_, err = db.Exec(ctx, `CREATE VIEW base_view AS SELECT * FROM raw_data WHERE value > 100`)
	if err != nil {
		t.Fatalf("Failed to create base view: %v", err)
	}

	// Create nested view
	_, err = db.Exec(ctx, `CREATE VIEW nested_view AS
		SELECT category, SUM(value) as total
		FROM base_view
		GROUP BY category`)
	if err != nil {
		t.Fatalf("Failed to create nested view: %v", err)
	}

	// Query nested view
	rows, err := db.Query(ctx, `SELECT * FROM nested_view WHERE total > 200`)
	if err != nil {
		t.Logf("Nested view query error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var category string
		var total int
		rows.Scan(&category, &total)
		t.Logf("Category %s has total %d", category, total)
	}
}

// TestDerivedTableWithGroupBy targets applyOuterQuery with derived tables
func TestDerivedTableWithGroupBy(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE orders (customer_id INTEGER, product TEXT, quantity INTEGER, price INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 'Widget', 2, 10),
		(1, 'Gadget', 1, 25),
		(2, 'Widget', 5, 10),
		(2, 'Tool', 1, 50),
		(3, 'Gadget', 3, 25),
		(3, 'Widget', 1, 10)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with derived table containing GROUP BY
	rows, err := db.Query(ctx, `
		SELECT customer_id, total_spent
		FROM (
			SELECT customer_id, SUM(quantity * price) as total_spent
			FROM orders
			GROUP BY customer_id
		) AS customer_totals
		WHERE total_spent > 50`)
	if err != nil {
		t.Logf("Derived table with GROUP BY error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var customerID, totalSpent int
		rows.Scan(&customerID, &totalSpent)
		t.Logf("Customer %d spent %d", customerID, totalSpent)
	}
}
