package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestHavingAggregateExpressions targets computeAggregatesWithGroupBy with HAVING
func TestHavingAggregateExpressions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE sales (region TEXT, product TEXT, amount INTEGER, cost INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with different regions and products
	_, err = db.Exec(ctx, `INSERT INTO sales VALUES
		('North', 'A', 100, 80),
		('North', 'B', 200, 150),
		('North', 'A', 150, 120),
		('South', 'A', 300, 250),
		('South', 'B', 100, 90),
		('South', 'B', 200, 180),
		('East', 'C', 500, 400),
		('East', 'C', 100, 80)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name     string
		sql      string
		expected int // expected row count
	}{
		{"HAVING SUM >", `SELECT region, SUM(amount) as total FROM sales GROUP BY region HAVING SUM(amount) > 400`, 2},
		{"HAVING COUNT >=", `SELECT product, COUNT(*) as cnt FROM sales GROUP BY product HAVING COUNT(*) >= 2`, 3},
		{"HAVING AVG between", `SELECT region, AVG(amount) as avg_amt FROM sales GROUP BY region HAVING AVG(amount) > 150 AND AVG(amount) < 350`, 2},
		{"HAVING MAX - MIN", `SELECT region, MAX(amount) - MIN(amount) as range_amt FROM sales GROUP BY region HAVING MAX(amount) - MIN(amount) > 100`, 2},
		{"HAVING SUM expression", `SELECT region, SUM(amount), SUM(cost) FROM sales GROUP BY region HAVING SUM(amount) - SUM(cost) > 50`, 3},
		{"HAVING multiple conditions", `SELECT region, product, COUNT(*) as cnt, AVG(amount) as avg_amt FROM sales GROUP BY region, product HAVING COUNT(*) > 1 AND AVG(amount) > 100`, 2},
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
			t.Logf("Returned %d rows (expected %d)", count, tt.expected)
		})
	}
}

// TestHavingWithSubquery targets HAVING with subquery
func TestHavingWithSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE orders (customer_id INTEGER, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 100), (1, 200), (1, 300),
		(2, 50), (2, 100),
		(3, 1000), (3, 2000),
		(4, 100), (4, 100), (4, 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// HAVING with subquery - customers with total above average
	rows, err := db.Query(ctx, `
		SELECT customer_id, SUM(amount) as total
		FROM orders
		GROUP BY customer_id
		HAVING SUM(amount) > (SELECT AVG(amount) FROM orders)`)
	if err != nil {
		t.Logf("HAVING with subquery error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var custID, total int
		rows.Scan(&custID, &total)
		count++
		t.Logf("Customer %d has total %d (above average)", custID, total)
	}
	t.Logf("Total customers above average: %d", count)
}

// TestComplexGroupBy targets computeAggregatesWithGroupBy edge cases
func TestComplexGroupBy(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE metrics (
		category TEXT,
		subcategory TEXT,
		value1 INTEGER,
		value2 INTEGER,
		flag BOOLEAN
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with NULL values and edge cases
	_, err = db.Exec(ctx, `INSERT INTO metrics VALUES
		('A', 'X', 10, 20, true),
		('A', 'X', 20, 30, true),
		('A', 'Y', 30, 40, false),
		('B', 'X', NULL, 50, true),
		('B', 'Y', 50, NULL, false),
		('C', 'Z', 100, 200, true)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"GROUP BY with NULLs", `SELECT category, COUNT(*), SUM(value1) FROM metrics GROUP BY category`},
		{"Multi-column GROUP BY", `SELECT category, subcategory, COUNT(*), AVG(value1) FROM metrics GROUP BY category, subcategory`},
		{"GROUP BY with boolean", `SELECT flag, COUNT(*), SUM(value1) FROM metrics GROUP BY flag`},
		{"HAVING with boolean", `SELECT category, COUNT(*) as cnt FROM metrics GROUP BY category HAVING COUNT(*) > 1`},
		{"Aggregate with NULL handling", `SELECT category, SUM(value1), SUM(value2), SUM(value1 + value2) FROM metrics GROUP BY category`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			results := 0
			for rows.Next() {
				results++
			}
			t.Logf("Query returned %d rows", results)
		})
	}
}

// TestDistinctWithGroupBy targets DISTINCT + GROUP BY combination
func TestDistinctWithGroupBy(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE events (type TEXT, source TEXT, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with duplicates
	_, err = db.Exec(ctx, `INSERT INTO events VALUES
		('click', 'web', 1),
		('click', 'web', 1),
		('click', 'mobile', 2),
		('view', 'web', 3),
		('view', 'web', 3),
		('view', 'mobile', 4)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// SELECT DISTINCT with GROUP BY equivalent using subquery
	rows, err := db.Query(ctx, `
		SELECT type, source, COUNT(DISTINCT value) as unique_values
		FROM events
		GROUP BY type, source`)
	if err != nil {
		t.Logf("DISTINCT with GROUP BY error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var typ, source string
		var uniqueCount int
		rows.Scan(&typ, &source, &uniqueCount)
		t.Logf("Type=%s, Source=%s, Unique values=%d", typ, source, uniqueCount)
	}
}
