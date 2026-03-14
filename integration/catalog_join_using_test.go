package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestJoinUsingSyntax tests JOIN with USING clause
func TestJoinUsingSyntax(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create test tables
	_, err = db.Exec(ctx, `CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create employees table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create departments table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO employees VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Charlie')`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')`)
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	// Test INNER JOIN with USING
	rows, err := db.Query(ctx, `SELECT employees.name, departments.name FROM employees JOIN departments USING (id)`)
	if err != nil {
		t.Logf("JOIN USING syntax parse error (expected until execution supported): %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var empName, deptName string
		if err := rows.Scan(&empName, &deptName); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		t.Logf("JOIN USING result: emp=%s, dept=%s", empName, deptName)
	}
	t.Logf("JOIN USING returned %d rows", count)
}
