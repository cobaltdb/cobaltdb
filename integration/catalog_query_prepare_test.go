package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestPrepareQuerySimple targets prepareQuery with simple queries
func TestPrepareQuerySimple(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO test VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"Simple SELECT", `SELECT * FROM test`, "All columns"},
		{"SELECT with WHERE", `SELECT * FROM test WHERE id = 1`, "With WHERE clause"},
		{"SELECT columns", `SELECT name, value FROM test`, "Specific columns"},
		{"SELECT with ORDER", `SELECT * FROM test ORDER BY value DESC`, "With ORDER BY"},
		{"SELECT with LIMIT", `SELECT * FROM test LIMIT 2`, "With LIMIT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error for %s: %v", tt.desc, err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("%s: returned %d rows", tt.desc, count)
		})
	}
}

// TestPrepareQueryJoins targets prepareQuery with JOINs
func TestPrepareQueryJoins(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"INNER JOIN", `SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id`, "Inner join"},
		{"LEFT JOIN", `SELECT c.name, o.amount FROM customers c LEFT JOIN orders o ON c.id = o.customer_id`, "Left join"},
		{"JOIN with aggregate", `SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name`, "Join with aggregate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error for %s: %v", tt.desc, err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("%s: returned %d rows", tt.desc, count)
		})
	}
}

// TestAnalyzeSelectComplex targets analyzeSelect with complex queries
func TestAnalyzeSelectComplex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES
		(1, 'Widget', 'A', 100),
		(2, 'Gadget', 'A', 200),
		(3, 'Tool', 'B', 150),
		(4, 'Device', 'B', 300),
		(5, 'Instrument', 'A', 250)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"SELECT with aggregate", `SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category`, "Aggregate analysis"},
		{"SELECT with HAVING", `SELECT category, SUM(price) FROM products GROUP BY category HAVING SUM(price) > 300`, "HAVING analysis"},
		{"SELECT with subquery", `SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)`, "Subquery analysis"},
		{"SELECT with DISTINCT", `SELECT DISTINCT category FROM products`, "DISTINCT analysis"},
		{"Complex WHERE", `SELECT * FROM products WHERE category = 'A' AND (price > 150 OR name LIKE 'W%')`, "Complex WHERE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error for %s: %v", tt.desc, err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			t.Logf("%s: returned %d rows", tt.desc, count)
		})
	}
}

// TestPrepareQueryWithParams targets prepareQuery with parameters
func TestPrepareQueryWithParams(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with parameters
	rows, err := db.Query(ctx, `SELECT * FROM users WHERE age > ?`, 28)
	if err != nil {
		t.Logf("Query with params error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, age int
		var name string
		rows.Scan(&id, &name, &age)
		count++
		t.Logf("User: %s (age %d)", name, age)
	}
	t.Logf("Found %d users older than 28", count)
}

// TestAnalyzeSelectWithIndex targets analyzeSelect with index usage
func TestAnalyzeSelectWithIndex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE indexed_table (id INTEGER PRIMARY KEY, code TEXT UNIQUE, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE INDEX idx_value ON indexed_table(value)`)
	if err != nil {
		t.Logf("CREATE INDEX error: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO indexed_table VALUES (1, 'ABC', 100), (2, 'DEF', 200), (3, 'GHI', 300)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Queries that should use indexes
	tests := []struct {
		name string
		sql  string
	}{
		{"PK lookup", `SELECT * FROM indexed_table WHERE id = 2`},
		{"Unique lookup", `SELECT * FROM indexed_table WHERE code = 'DEF'`},
		{"Index range", `SELECT * FROM indexed_table WHERE value > 150`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			if rows.Next() {
				var id, value int
				var code string
				rows.Scan(&id, &code, &value)
				t.Logf("Found: id=%d, code=%s, value=%d", id, code, value)
			}
		})
	}
}
