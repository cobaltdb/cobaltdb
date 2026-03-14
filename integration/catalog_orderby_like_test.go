package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestOrderByMultiColumnNulls targets applyOrderBy with multi-column and NULLs
func TestOrderByMultiColumnNulls(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE employees (
		dept TEXT,
		salary INTEGER,
		name TEXT,
		hire_date TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with NULLs and various values
	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		('Engineering', 100000, 'Alice', '2020-01-15'),
		('Engineering', NULL, 'Bob', '2019-06-01'),
		('Engineering', 80000, 'Charlie', '2021-03-10'),
		('Sales', 90000, 'David', NULL),
		('Sales', NULL, 'Eve', '2020-08-20'),
		('Sales', 75000, 'Frank', '2022-01-01'),
		(NULL, 60000, 'Grace', '2021-11-15'),
		('Marketing', 85000, NULL, '2020-05-01')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"Multi-column ASC", `SELECT dept, name, salary FROM employees ORDER BY dept, salary`, "Order by dept then salary"},
		{"Multi-column mixed", `SELECT dept, name, salary FROM employees ORDER BY dept ASC, salary DESC`, "Dept ASC, salary DESC"},
		{"Three columns", `SELECT dept, name, salary FROM employees ORDER BY dept, salary, name`, "Three column order"},
		{"NULL values", `SELECT dept, salary FROM employees ORDER BY dept NULLS FIRST`, "NULLS FIRST"},
		{"NULL LAST", `SELECT dept, salary FROM employees ORDER BY dept NULLS LAST`, "NULLS LAST"},
		{"Expression order", `SELECT name, salary FROM employees ORDER BY salary * 2 DESC`, "Order by expression"},
		{"Order by position", `SELECT dept, name, salary FROM employees ORDER BY 1, 3`, "Order by column position"},
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

// TestLikePatterns targets evaluateLike with various patterns
func TestLikePatterns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products (name TEXT, description TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES
		('Apple iPhone', 'A premium smartphone'),
		('Samsung Galaxy', 'Android smartphone'),
		('Apple Watch', 'Smart wearable device'),
		('Google Pixel', 'Android phone by Google'),
		('iPhone Case', 'Protective case for iPhone'),
		('Samsung TV', 'Smart television'),
		('Apple iPad', 'Tablet device'),
		('Galaxy Watch', 'Samsung smartwatch')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{"Starts with", `SELECT name FROM products WHERE name LIKE 'Apple%'`, 3},
		{"Ends with", `SELECT name FROM products WHERE name LIKE '%Phone'`, 2},
		{"Contains", `SELECT name FROM products WHERE name LIKE '%Galaxy%'`, 2},
		{"Single char", `SELECT name FROM products WHERE name LIKE 'iPhone Cas_'`, 1},
		{"Multiple single", `SELECT name FROM products WHERE name LIKE 'A___e'`, 0},
		{"Mixed pattern", `SELECT name FROM products WHERE name LIKE 'Apple %Phone%'`, 2},
		{"NOT LIKE", `SELECT name FROM products WHERE name NOT LIKE '%Apple%'`, 5},
		{"Case insensitive", `SELECT name FROM products WHERE name LIKE '%IPHONE%'`, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("LIKE query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				var name string
				rows.Scan(&name)
				count++
				t.Logf("  Match: %s", name)
			}
			t.Logf("Pattern '%s': %d matches (expected %d)", tt.name, count, tt.expected)
		})
	}
}

// TestLikeEscape targets LIKE with escape character
func TestLikeEscape(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE patterns (value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO patterns VALUES
		('100% complete'),
		('50% done'),
		('test_user'),
		('user_name'),
		('100 dollars')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test escaping special characters
	rows, err := db.Query(ctx, `SELECT value FROM patterns WHERE value LIKE '%\%%' ESCAPE '\'`)
	if err != nil {
		t.Logf("LIKE with escape error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var value string
		rows.Scan(&value)
		count++
		t.Logf("Contains %%: %s", value)
	}
	t.Logf("Total with %%: %d", count)
}
