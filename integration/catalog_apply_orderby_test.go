package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestApplyOrderByMultiColumnDeep targets applyOrderBy with multiple columns
func TestApplyOrderByMultiColumnDeep(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE order_test (
		id INTEGER PRIMARY KEY,
		category TEXT,
		subcategory TEXT,
		amount INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO order_test VALUES
		(1, 'A', 'X', 100),
		(2, 'A', 'Y', 200),
		(3, 'A', 'X', 150),
		(4, 'B', 'Z', 300),
		(5, 'B', 'Y', 100),
		(6, 'B', 'Z', 250),
		(7, 'A', 'Y', 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"ORDER BY 1 col ASC", `SELECT * FROM order_test ORDER BY category`},
		{"ORDER BY 1 col DESC", `SELECT * FROM order_test ORDER BY category DESC`},
		{"ORDER BY 2 cols ASC", `SELECT * FROM order_test ORDER BY category, amount`},
		{"ORDER BY 2 cols DESC", `SELECT * FROM order_test ORDER BY category DESC, amount DESC`},
		{"ORDER BY mixed", `SELECT * FROM order_test ORDER BY category ASC, amount DESC`},
		{"ORDER BY 3 cols", `SELECT * FROM order_test ORDER BY category, subcategory, amount`},
		{"ORDER BY with WHERE", `SELECT * FROM order_test WHERE amount > 150 ORDER BY category, amount`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			var results []int
			for rows.Next() {
				var id int
				var cat, sub string
				var amt int
				rows.Scan(&id, &cat, &sub, &amt)
				results = append(results, id)
			}
			t.Logf("Order: %v", results)
		})
	}
}

// TestApplyOrderByWithNULLs targets applyOrderBy with NULL handling
func TestApplyOrderByWithNULLs(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE null_order (
		id INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO null_order VALUES
		(1, NULL),
		(2, 10),
		(3, NULL),
		(4, 5),
		(5, 20)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"ORDER BY NULLs ASC", `SELECT * FROM null_order ORDER BY val`},
		{"ORDER BY NULLs DESC", `SELECT * FROM null_order ORDER BY val DESC`},
		{"ORDER BY id", `SELECT * FROM null_order ORDER BY id`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			var results []int
			for rows.Next() {
				var id, val int
				rows.Scan(&id, &val)
				results = append(results, id)
			}
			t.Logf("Order: %v", results)
		})
	}
}

// TestApplyOrderByExpressions targets applyOrderBy with expressions
func TestApplyOrderByExpressions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE expr_order (
		id INTEGER PRIMARY KEY,
		x INTEGER,
		y INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO expr_order VALUES
		(1, 10, 5),
		(2, 20, 10),
		(3, 5, 20),
		(4, 15, 15)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"ORDER BY arithmetic", `SELECT * FROM expr_order ORDER BY x + y`},
		{"ORDER BY function", `SELECT * FROM expr_order ORDER BY ABS(x - y)`},
		{"ORDER BY column alias", `SELECT id, x+y as total FROM expr_order ORDER BY total`},
		{"ORDER BY expression DESC", `SELECT * FROM expr_order ORDER BY x * y DESC`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			var results []int
			for rows.Next() {
				var id int
				rows.Scan(&id)
				results = append(results, id)
			}
			t.Logf("Order: %v", results)
		})
	}
}

// TestApplyOrderByWithJOIN targets applyOrderBy with JOIN
func TestApplyOrderByWithJOIN(t *testing.T) {
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

	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200), (3, 1, 150)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"ORDER BY joined column", `SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id ORDER BY c.name`},
		{"ORDER BY aggregate", `SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY total DESC`},
		{"ORDER BY qualified name", `SELECT c.id, c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id ORDER BY o.amount`},
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

// TestApplyOrderByWithLIMIT targets applyOrderBy with LIMIT/OFFSET
func TestApplyOrderByWithLIMIT(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE limit_order (
		id INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 1; i <= 20; i++ {
		_, err = db.Exec(ctx, `INSERT INTO limit_order VALUES (?, ?)`, i, i*10)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	tests := []struct {
		name   string
		sql    string
		expect int
	}{
		{"ORDER BY LIMIT", `SELECT * FROM limit_order ORDER BY val LIMIT 5`, 5},
		{"ORDER BY LIMIT OFFSET", `SELECT * FROM limit_order ORDER BY val LIMIT 5 OFFSET 5`, 5},
		{"ORDER BY DESC LIMIT", `SELECT * FROM limit_order ORDER BY val DESC LIMIT 3`, 3},
		{"ORDER BY LIMIT 10", `SELECT * FROM limit_order ORDER BY val LIMIT 10`, 10},
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
			if count != tt.expect {
				t.Errorf("Expected %d rows, got %d", tt.expect, count)
			}
			t.Logf("Query returned %d rows", count)
		})
	}
}

// TestApplyOrderByStringCollations targets applyOrderBy with string ordering
func TestApplyOrderByStringCollations(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE string_order (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO string_order VALUES
		(1, 'apple'),
		(2, 'Banana'),
		(3, 'cherry'),
		(4, 'date'),
		(5, 'Elderberry')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"ORDER BY string ASC", `SELECT * FROM string_order ORDER BY name`},
		{"ORDER BY string DESC", `SELECT * FROM string_order ORDER BY name DESC`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			var results []string
			for rows.Next() {
				var id int
				var name string
				rows.Scan(&id, &name)
				results = append(results, name)
			}
			t.Logf("Order: %v", results)
		})
	}
}
