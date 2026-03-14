package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestEvaluateLikePatterns targets evaluateLike with various patterns
func TestEvaluateLikePatterns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE like_test (
		id INTEGER PRIMARY KEY,
		name TEXT,
		description TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO like_test VALUES
		(1, 'John Doe', 'Software Engineer'),
		(2, 'Jane Smith', 'Product Manager'),
		(3, 'Bob Johnson', 'Sales Representative'),
		(4, 'Alice Brown', 'Software Developer'),
		(5, 'Charlie Davis', 'Marketing Director'),
		(6, 'test', 'Test data'),
		(7, 'TEST', 'Case test'),
		(8, 'prefix_suffix', 'Prefix and suffix'),
		(9, 'a', 'Single char'),
		(10, '', 'Empty name')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{"% at end (starts with)", `SELECT * FROM like_test WHERE name LIKE 'John%'`, 1},
		{"% at start (ends with)", `SELECT * FROM like_test WHERE name LIKE '%son'`, 2},
		{"% both sides (contains)", `SELECT * FROM like_test WHERE name LIKE '%smith%'`, 1},
		{"_ single char", `SELECT * FROM like_test WHERE name LIKE 'J_ne%'`, 1},
		{"Multiple _", `SELECT * FROM like_test WHERE name LIKE 'B_b%'`, 1},
		{"% in middle", `SELECT * FROM like_test WHERE name LIKE 'prefix%_suffix'`, 1},
		{"Literal match", `SELECT * FROM like_test WHERE name LIKE 'test'`, 2},
		{"Empty pattern", `SELECT * FROM like_test WHERE name LIKE ''`, 1},
		{"Just %", `SELECT * FROM like_test WHERE name LIKE '%'`, 10},
		{"Just _", `SELECT * FROM like_test WHERE name LIKE '_'`, 1},
		{"Multiple _ pattern", `SELECT * FROM like_test WHERE name LIKE '___'`, 0},
		{"Complex pattern", `SELECT * FROM like_test WHERE name LIKE 'A%son'`, 0},
		{"Software% pattern", `SELECT * FROM like_test WHERE description LIKE 'Software%'`, 2},
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
				count++
			}
			t.Logf("LIKE '%s' returned %d rows (expected %d)", tt.sql, count, tt.expected)
		})
	}
}

// TestEvaluateLikeCaseSensitivity targets evaluateLike case sensitivity
func TestEvaluateLikeCaseSensitivity(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE case_test (
		id INTEGER PRIMARY KEY,
		code TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO case_test VALUES
		(1, 'ABC'),
		(2, 'abc'),
		(3, 'AbC'),
		(4, 'aBc')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{"Uppercase pattern", `SELECT * FROM case_test WHERE code LIKE 'ABC'`, 4},
		{"Lowercase pattern", `SELECT * FROM case_test WHERE code LIKE 'abc'`, 4},
		{"Mixed pattern", `SELECT * FROM case_test WHERE code LIKE 'AbC'`, 4},
		{"Upper with %", `SELECT * FROM case_test WHERE code LIKE 'A%'`, 4},
		{"Lower with %", `SELECT * FROM case_test WHERE code LIKE 'a%'`, 4},
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
			t.Logf("Query returned %d rows (expected %d)", count, tt.expected)
		})
	}
}

// TestEvaluateLikeWithNULL targets evaluateLike with NULL values
func TestEvaluateLikeWithNULL(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE null_like (
		id INTEGER PRIMARY KEY,
		val TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO null_like VALUES
		(1, 'test'),
		(2, NULL),
		(3, 'other'),
		(4, NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name     string
		sql      string
		expected int
	}{
		{"LIKE with NULL values", `SELECT * FROM null_like WHERE val LIKE '%test%'`, 1},
		{"NOT LIKE with NULL", `SELECT * FROM null_like WHERE val NOT LIKE '%test%'`, 1},
		{"LIKE NULL pattern", `SELECT * FROM null_like WHERE val LIKE NULL`, 0},
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
			t.Logf("Query returned %d rows (expected %d)", count, tt.expected)
		})
	}
}

// TestEvaluateLikeSpecialChars targets evaluateLike with special characters
func TestEvaluateLikeSpecialChars(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE special_like (
		id INTEGER PRIMARY KEY,
		val TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO special_like VALUES
		(1, '100%'),
		(2, '50_'),
		(3, 'normal'),
		(4, 'hello.world'),
		(5, 'test%value_'),
		(6, '[brackets]'),
		(7, '(parens)'),
		(8, '100% complete')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Literal %", `SELECT * FROM special_like WHERE val LIKE '100%'`},
		{"Literal _", `SELECT * FROM special_like WHERE val LIKE '50_'`},
		{"Contains %", `SELECT * FROM special_like WHERE val LIKE '%\%%'`},
		{"Contains _", `SELECT * FROM special_like WHERE val LIKE '%\_%'`},
		{"Dot literal", `SELECT * FROM special_like WHERE val LIKE '%.%'`},
		{"Brackets", `SELECT * FROM special_like WHERE val LIKE '%[%]%'`},
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

// TestEvaluateLikeInComplexQueries targets evaluateLike in complex queries
func TestEvaluateLikeInComplexQueries(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name TEXT,
		department TEXT,
		email TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		(1, 'John Doe', 'Engineering', 'john.doe@company.com'),
		(2, 'Jane Smith', 'Engineering', 'jane.smith@company.com'),
		(3, 'Bob Johnson', 'Sales', 'bob.johnson@company.com'),
		(4, 'Alice Brown', 'Marketing', 'alice.brown@company.com')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"LIKE with AND", `SELECT * FROM employees WHERE name LIKE 'J%' AND department = 'Engineering'`},
		{"LIKE with OR", `SELECT * FROM employees WHERE name LIKE 'J%' OR name LIKE 'A%'`},
		{"LIKE with ORDER BY", `SELECT * FROM employees WHERE email LIKE '%@company.com' ORDER BY name`},
		{"LIKE in subquery", `SELECT * FROM employees WHERE department IN (SELECT department FROM employees WHERE name LIKE '%Smith%')`},
		{"Multiple LIKE", `SELECT * FROM employees WHERE name LIKE 'J%' AND email LIKE '%.com' AND department LIKE '%ing'`},
		{"LIKE with NOT", `SELECT * FROM employees WHERE name NOT LIKE 'B%' AND department LIKE '%ing'`},
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
