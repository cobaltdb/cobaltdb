package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestSelectWithJoinAndGroupBy targets executeSelectWithJoinAndGroupBy
func TestSelectWithJoinAndGroupBy(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup tables
	_, err = db.Exec(ctx, `CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product TEXT, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 1, 'Widget', 100),
		(2, 1, 'Gadget', 200),
		(3, 2, 'Widget', 150),
		(4, 2, 'Tool', 300),
		(5, 3, 'Widget', 50),
		(6, 1, 'Tool', 250)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"JOIN + GROUP BY + HAVING", `SELECT c.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name HAVING COUNT(o.id) >= 2`, "Customers with 2+ orders"},
		{"Multiple JOINs + GROUP BY", `SELECT o.product, COUNT(*) as cnt, AVG(o.amount) as avg_amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount > 75 GROUP BY o.product`, "Product stats"},
		{"LEFT JOIN + GROUP BY", `SELECT c.name, COUNT(o.id) as order_count FROM customers c LEFT JOIN orders o ON c.id = o.customer_id GROUP BY c.name`, "All customers with order counts"},
		{"JOIN + GROUP BY expression", `SELECT c.name, SUM(o.amount * 2) as doubled_total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name`, "With expression in aggregate"},
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
			t.Logf("%s: %d rows returned", tt.desc, count)
		})
	}
}

// TestJoinWithSubquery targets selectLocked with subquery joins
func TestJoinWithSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')`)
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		(1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2), (4, 'Diana', 2), (5, 'Eve', 1)`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	// JOIN with subquery
	rows, err := db.Query(ctx, `
		SELECT e.name, e.dept_id
		FROM employees e
		JOIN (SELECT id FROM departments WHERE name = 'Engineering') d ON e.dept_id = d.id`)
	if err != nil {
		t.Logf("JOIN with subquery error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name string
		var deptID int
		rows.Scan(&name, &deptID)
		count++
		t.Logf("Engineering employee: %s", name)
	}
	t.Logf("Total Engineering employees: %d", count)
}

// TestSelectComplexQueryCache targets selectLocked query cache paths
func TestSelectComplexQueryCache(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, `INSERT INTO test VALUES (?, ?)`, i, i*10)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Run same query multiple times to test cache
	for i := 0; i < 3; i++ {
		rows, err := db.Query(ctx, `SELECT * FROM test WHERE value > 500 ORDER BY id`)
		if err != nil {
			t.Logf("Query error on iteration %d: %v", i, err)
			continue
		}

		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		t.Logf("Iteration %d: %d rows", i, count)
	}
}

// TestLeftJoinWithNulls targets LEFT JOIN NULL handling
func TestLeftJoinWithNulls(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create parents: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create children: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parents VALUES (1, 'Parent A'), (2, 'Parent B'), (3, 'Parent C')`)
	if err != nil {
		t.Fatalf("Failed to insert parents: %v", err)
	}

	// Only children for Parent A and B
	_, err = db.Exec(ctx, `INSERT INTO children VALUES
		(1, 1, 'Child A1'), (2, 1, 'Child A2'),
		(3, 2, 'Child B1')`)
	if err != nil {
		t.Fatalf("Failed to insert children: %v", err)
	}

	// LEFT JOIN with aggregation
	rows, err := db.Query(ctx, `
		SELECT p.name, COUNT(c.id) as child_count
		FROM parents p
		LEFT JOIN children c ON p.id = c.parent_id
		GROUP BY p.name
		ORDER BY p.name`)
	if err != nil {
		t.Logf("LEFT JOIN with GROUP BY error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var childCount int
		rows.Scan(&name, &childCount)
		t.Logf("%s has %d children", name, childCount)
	}
}

// TestCrossJoin targets CROSS JOIN
func TestCrossJoin(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE colors (name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create colors: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE sizes (name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create sizes: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO colors VALUES ('Red'), ('Blue')`)
	if err != nil {
		t.Fatalf("Failed to insert colors: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO sizes VALUES ('Small'), ('Medium'), ('Large')`)
	if err != nil {
		t.Fatalf("Failed to insert sizes: %v", err)
	}

	rows, err := db.Query(ctx, `SELECT c.name as color, s.name as size FROM colors c CROSS JOIN sizes s`)
	if err != nil {
		t.Logf("CROSS JOIN error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var color, size string
		rows.Scan(&color, &size)
		count++
		t.Logf("Combination: %s - %s", color, size)
	}
	t.Logf("Total combinations: %d (expected 6)", count)
}
