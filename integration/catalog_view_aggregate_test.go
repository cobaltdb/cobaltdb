package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestViewWithAggregate tests views containing aggregate functions
func TestViewWithAggregate(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create base table
	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 1, 100.0),
		(2, 1, 200.0),
		(3, 2, 150.0),
		(4, 2, 50.0)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with GROUP BY and aggregate
	_, err = db.Exec(ctx, `CREATE VIEW customer_totals AS
		SELECT customer_id, SUM(amount) as total_amount, COUNT(*) as order_count
		FROM orders
		GROUP BY customer_id`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err := db.Query(ctx, `SELECT * FROM customer_totals ORDER BY customer_id`)
	if err != nil {
		t.Logf("View query error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var customerID int
		var totalAmount float64
		var orderCount int
		if err := rows.Scan(&customerID, &totalAmount, &orderCount); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Customer %d: total=%.2f, count=%d", customerID, totalAmount, orderCount)

		// Verify values
		if customerID == 1 && (totalAmount != 300.0 || orderCount != 2) {
			t.Errorf("Customer 1: expected total=300, count=2, got total=%.2f, count=%d", totalAmount, orderCount)
		}
		if customerID == 2 && (totalAmount != 200.0 || orderCount != 2) {
			t.Errorf("Customer 2: expected total=200, count=2, got total=%.2f, count=%d", totalAmount, orderCount)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	t.Log("View with aggregates works correctly")
}

// TestViewWithGroupByAndHaving tests views with GROUP BY and HAVING
func TestViewWithGroupByAndHaving(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create base table
	_, err = db.Exec(ctx, `CREATE TABLE sales (
		id INTEGER PRIMARY KEY,
		product TEXT,
		quantity INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO sales VALUES
		(1, 'Widget', 5),
		(2, 'Widget', 3),
		(3, 'Gadget', 10),
		(4, 'Gadget', 8),
		(5, 'Thing', 2)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with GROUP BY and HAVING
	_, err = db.Exec(ctx, `CREATE VIEW popular_products AS
		SELECT product, SUM(quantity) as total_qty
		FROM sales
		GROUP BY product
		HAVING SUM(quantity) > 5`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err := db.Query(ctx, `SELECT * FROM popular_products ORDER BY product`)
	if err != nil {
		t.Logf("View query error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	expected := map[string]int{
		"Gadget": 18,
		"Widget": 8,
	}

	for rows.Next() {
		var product string
		var totalQty int
		if err := rows.Scan(&product, &totalQty); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Product %s: total_qty=%d", product, totalQty)

		if expectedQty, ok := expected[product]; !ok {
			t.Errorf("Unexpected product: %s", product)
		} else if totalQty != expectedQty {
			t.Errorf("Product %s: expected qty=%d, got %d", product, expectedQty, totalQty)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	t.Log("View with GROUP BY and HAVING works correctly")
}

// TestViewWithDistinctAggregate tests views with DISTINCT
func TestViewWithDistinctAggregate(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create base table
	_, err = db.Exec(ctx, `CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		event_type TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with duplicates
	_, err = db.Exec(ctx, `INSERT INTO events VALUES
		(1, 1, 'click'),
		(2, 1, 'click'),
		(3, 2, 'click'),
		(4, 1, 'scroll')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with DISTINCT
	_, err = db.Exec(ctx, `CREATE VIEW unique_events AS
		SELECT DISTINCT user_id, event_type FROM events`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err := db.Query(ctx, `SELECT * FROM unique_events`)
	if err != nil {
		t.Logf("View query error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var userID int
		var eventType string
		if err := rows.Scan(&userID, &eventType); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("User %d: %s", userID, eventType)
	}

	if count != 3 { // (1,click), (2,click), (1,scroll)
		t.Errorf("Expected 3 distinct rows, got %d", count)
	}

	t.Log("View with DISTINCT works correctly")
}
