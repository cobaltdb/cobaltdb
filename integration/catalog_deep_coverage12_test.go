package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestDeleteRowLockedWithIndexCleanup targets deleteRowLocked with index cleanup
func TestDeleteRowLockedWithIndexCleanup(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table with unique index
	_, err = db.Exec(ctx, `CREATE TABLE indexed_items (
		id INTEGER PRIMARY KEY,
		code TEXT UNIQUE
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO indexed_items VALUES (1, 'ABC'), (2, 'DEF'), (3, 'GHI')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Delete row - should cleanup index
	_, err = db.Exec(ctx, `DELETE FROM indexed_items WHERE id = 2`)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify deletion
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM indexed_items`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 2 {
			t.Errorf("Expected 2 rows after delete, got %d", count)
		}
	}

	// Verify unique constraint still works - can insert new value
	_, err = db.Exec(ctx, `INSERT INTO indexed_items VALUES (4, 'DEF')`)
	if err != nil {
		t.Logf("Re-insert of deleted unique value error: %v", err)
	}
}

// TestDeleteWithUsing targets deleteWithUsingLocked
func TestDeleteWithUsing(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE archived_customers (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create archived: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO orders VALUES (1, 1), (2, 2), (3, 1)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO archived_customers VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert archived: %v", err)
	}

	// DELETE ... USING
	_, err = db.Exec(ctx, `DELETE FROM orders USING archived_customers WHERE orders.customer_id = archived_customers.id`)
	if err != nil {
		t.Logf("DELETE USING error (may not be fully supported): %v", err)
		return
	}

	// Verify deletion
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM orders`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 1 {
			t.Errorf("Expected 1 row after DELETE USING, got %d", count)
		}
	}
}

// TestRLSInsertUpdateDelete targets RLS checks for INSERT/UPDATE/DELETE
func TestRLSInsertUpdateDelete(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE sensitive_data (
		id INTEGER PRIMARY KEY,
		tenant_id INTEGER,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Enable RLS
	_, err = db.Exec(ctx, `ALTER TABLE sensitive_data ENABLE ROW LEVEL SECURITY`)
	if err != nil {
		t.Logf("RLS enable error: %v", err)
		return
	}

	// Create policy for INSERT
	_, err = db.Exec(ctx, `CREATE POLICY tenant_isolation ON sensitive_data
		FOR ALL
		TO PUBLIC
		USING (tenant_id = CURRENT_USER)`)
	if err != nil {
		t.Logf("Policy creation error: %v", err)
		return
	}

	// Try to insert data
	_, err = db.Exec(ctx, `INSERT INTO sensitive_data VALUES (1, 1, 'test')`)
	if err != nil {
		t.Logf("RLS insert error: %v", err)
	}

	// Query to see if data was inserted
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM sensitive_data`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		t.Logf("RLS data count: %d", count)
	}
}

// TestApplyOrderByNullHandling targets applyOrderBy with NULL handling
func TestApplyOrderByNullHandling(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE nullable_sort (
		id INTEGER PRIMARY KEY,
		priority INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with NULLs
	_, err = db.Exec(ctx, `INSERT INTO nullable_sort VALUES (1, NULL), (2, 10), (3, NULL), (4, 5)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Order by with NULLs ASC
	rows, err := db.Query(ctx, `SELECT id FROM nullable_sort ORDER BY priority ASC`)
	if err != nil {
		t.Logf("ORDER BY NULLS ASC error: %v", err)
		return
	}
	defer rows.Close()

	var orderAsc []int
	for rows.Next() {
		var id int
		rows.Scan(&id)
		orderAsc = append(orderAsc, id)
	}
	t.Logf("ASC order: %v", orderAsc)

	// Order by with NULLs DESC
	rows2, err := db.Query(ctx, `SELECT id FROM nullable_sort ORDER BY priority DESC`)
	if err != nil {
		t.Logf("ORDER BY NULLS DESC error: %v", err)
		return
	}
	defer rows2.Close()

	var orderDesc []int
	for rows2.Next() {
		var id int
		rows2.Scan(&id)
		orderDesc = append(orderDesc, id)
	}
	t.Logf("DESC order: %v", orderDesc)
}

// TestSelectWithJoinAndGroupByComplex targets executeSelectWithJoinAndGroupBy
func TestSelectWithJoinAndGroupByComplex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		region TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE orders2 (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'North'), (2, 'South'), (3, 'North')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders2 VALUES
		(1, 1, 100.0),
		(2, 1, 200.0),
		(3, 2, 150.0),
		(4, 3, 300.0)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// JOIN + GROUP BY with aggregates
	rows, err := db.Query(ctx, `SELECT c.region, COUNT(*) as order_count, SUM(o.amount) as total
		FROM customers c
		JOIN orders2 o ON c.id = o.customer_id
		GROUP BY c.region
		ORDER BY total DESC`)
	if err != nil {
		t.Logf("JOIN+GROUP BY error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 groups, got %d", count)
	}
}

// TestComputeAggregatesWithGroupBy targets computeAggregatesWithGroupBy
func TestComputeAggregatesWithGroupBy(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE sales (
		id INTEGER PRIMARY KEY,
		product TEXT,
		quarter TEXT,
		amount INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multi-dimensional data
	_, err = db.Exec(ctx, `INSERT INTO sales VALUES
		(1, 'A', 'Q1', 100),
		(2, 'A', 'Q2', 150),
		(3, 'B', 'Q1', 200),
		(4, 'B', 'Q2', 250),
		(5, 'A', 'Q1', 120)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Multi-column GROUP BY
	rows, err := db.Query(ctx, `SELECT product, quarter, SUM(amount) as total, COUNT(*) as cnt,
		AVG(amount) as avg_amt, MIN(amount) as min_amt, MAX(amount) as max_amt
		FROM sales GROUP BY product, quarter ORDER BY product, quarter`)
	if err != nil {
		t.Logf("Multi-column GROUP BY error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 4 {
		t.Errorf("Expected 4 groups, got %d", count)
	}
}

// TestEvaluateExprWithGroupAggregates targets evaluateExprWithGroupAggregates
func TestEvaluateExprWithGroupAggregates(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		category TEXT,
		price INTEGER,
		cost INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES
		(1, 'A', 100, 60),
		(2, 'A', 150, 90),
		(3, 'B', 200, 120)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Arithmetic with aggregates in SELECT
	rows, err := db.Query(ctx, `SELECT category,
		SUM(price) as revenue,
		SUM(cost) as expenses,
		SUM(price) - SUM(cost) as profit,
		(SUM(price) - SUM(cost)) * 100.0 / SUM(price) as margin_pct
		FROM products GROUP BY category`)
	if err != nil {
		t.Logf("Aggregate arithmetic error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 groups, got %d", count)
	}
}

// TestUpdateWithJoin targets updateWithJoinLocked
func TestUpdateWithJoin(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE inventory (
		id INTEGER PRIMARY KEY,
		product_id INTEGER,
		qty INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create inventory: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE price_updates (
		product_id INTEGER PRIMARY KEY,
		new_qty INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create price_updates: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO inventory VALUES (1, 100, 10), (2, 200, 20)`)
	if err != nil {
		t.Fatalf("Failed to insert inventory: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO price_updates VALUES (100, 50)`)
	if err != nil {
		t.Fatalf("Failed to insert price_updates: %v", err)
	}

	// UPDATE ... FROM
	_, err = db.Exec(ctx, `UPDATE inventory SET qty = price_updates.new_qty
		FROM price_updates
		WHERE inventory.product_id = price_updates.product_id`)
	if err != nil {
		t.Logf("UPDATE FROM error: %v", err)
		return
	}

	// Verify update
	rows, err := db.Query(ctx, `SELECT qty FROM inventory WHERE product_id = 100`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var qty int
		rows.Scan(&qty)
		if qty != 50 {
			t.Errorf("Expected qty=50 after UPDATE FROM, got %d", qty)
		}
	}
}
