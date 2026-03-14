package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestEvaluateWhereComplexComparisons targets evaluateWhere with complex comparisons
func TestEvaluateWhereComplexComparisons(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE comparison_test (
		id INTEGER PRIMARY KEY,
		int_val INTEGER,
		real_val REAL,
		text_val TEXT,
		bool_val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO comparison_test VALUES
		(1, 10, 10.5, 'hello', 1),
		(2, 20, 20.0, 'world', 0),
		(3, NULL, 15.5, NULL, 1),
		(4, 30, NULL, 'test', NULL),
		(5, 10, 10.5, 'hello', 1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"= with integers", `SELECT * FROM comparison_test WHERE int_val = 10`},
		{"<> (not equal)", `SELECT * FROM comparison_test WHERE int_val <> 20`},
		{"!= (not equal)", `SELECT * FROM comparison_test WHERE int_val != 20`},
		{">", `SELECT * FROM comparison_test WHERE int_val > 15`},
		{"<", `SELECT * FROM comparison_test WHERE int_val < 25`},
		{">=", `SELECT * FROM comparison_test WHERE int_val >= 20`},
		{"<=", `SELECT * FROM comparison_test WHERE int_val <= 10`},
		{"= with real", `SELECT * FROM comparison_test WHERE real_val = 10.5`},
		{"> with real", `SELECT * FROM comparison_test WHERE real_val > 15.0`},
		{"= with text", `SELECT * FROM comparison_test WHERE text_val = 'hello'`},
		{"!= with text", `SELECT * FROM comparison_test WHERE text_val != 'world'`},
		{"NULL comparison", `SELECT * FROM comparison_test WHERE int_val = NULL`},
		{"Mixed types int=real", `SELECT * FROM comparison_test WHERE int_val = real_val`},
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

// TestEvaluateWhereArithmetic targets evaluateWhere with arithmetic expressions
func TestEvaluateWhereArithmetic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE arithmetic_test (
		id INTEGER PRIMARY KEY,
		x INTEGER,
		y INTEGER,
		z REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO arithmetic_test VALUES
		(1, 10, 5, 2.5),
		(2, 20, 4, 5.0),
		(3, 15, 3, 3.0),
		(4, 0, 10, 0.0)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"+", `SELECT * FROM arithmetic_test WHERE x + y = 15`},
		{"-", `SELECT * FROM arithmetic_test WHERE x - y = 16`},
		{"*", `SELECT * FROM arithmetic_test WHERE x * y = 60`},
		{"/", `SELECT * FROM arithmetic_test WHERE x / y = 5`},
		{"%", `SELECT * FROM arithmetic_test WHERE x % y = 0`},
		{"Complex arithmetic", `SELECT * FROM arithmetic_test WHERE (x + y) * 2 = 30`},
		{"Arithmetic with NULL", `SELECT * FROM arithmetic_test WHERE x + NULL = 10`},
		{"Division by zero check", `SELECT * FROM arithmetic_test WHERE y <> 0 AND x / y > 3`},
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

// TestEvaluateWhereStringOperations targets evaluateWhere with string operations
func TestEvaluateWhereStringOperations(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE string_test (
		id INTEGER PRIMARY KEY,
		name TEXT,
		description TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO string_test VALUES
		(1, 'John Doe', 'Software Engineer'),
		(2, 'Jane Smith', 'Product Manager'),
		(3, 'Bob Johnson', 'Sales Rep'),
		(4, 'Alice Brown', 'Software Developer')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"LIKE pattern %", `SELECT * FROM string_test WHERE name LIKE 'John%'`},
		{"LIKE pattern _", `SELECT * FROM string_test WHERE name LIKE 'J_ne%'`},
		{"LIKE pattern %%", `SELECT * FROM string_test WHERE description LIKE '%Software%'`},
		{"NOT LIKE", `SELECT * FROM string_test WHERE name NOT LIKE 'Bob%'`},
		{"String concatenation", `SELECT * FROM string_test WHERE name || ' - ' || description = 'John Doe - Software Engineer'`},
		{"LOWER function", `SELECT * FROM string_test WHERE LOWER(name) = 'john doe'`},
		{"UPPER function", `SELECT * FROM string_test WHERE UPPER(name) = 'JANE SMITH'`},
		{"LENGTH function", `SELECT * FROM string_test WHERE LENGTH(name) > 10`},
		{"SUBSTR function", `SELECT * FROM string_test WHERE SUBSTR(name, 1, 4) = 'John'`},
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

// TestEvaluateWhereFunctions targets evaluateWhere with SQL functions
func TestEvaluateWhereFunctions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE func_test (
		id INTEGER PRIMARY KEY,
		val INTEGER,
		amount REAL,
		created TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO func_test VALUES
		(1, -10, 10.5, '2024-01-15'),
		(2, 20, -5.5, '2024-02-20'),
		(3, -30, 15.0, '2024-03-10'),
		(4, 40, 20.5, NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"ABS", `SELECT * FROM func_test WHERE ABS(val) > 15`},
		{"ROUND", `SELECT * FROM func_test WHERE ROUND(amount) = 11`},
		{"COALESCE", `SELECT * FROM func_test WHERE COALESCE(created, '1900-01-01') > '2024-01-01'`},
		{"NULLIF", `SELECT * FROM func_test WHERE NULLIF(val, -10) IS NOT NULL`},
		{"CAST to TEXT", `SELECT * FROM func_test WHERE CAST(val AS TEXT) = '20'`},
		{"CAST to REAL", `SELECT * FROM func_test WHERE CAST(val AS REAL) > 15.5`},
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

// TestEvaluateWhereExists targets evaluateWhere with EXISTS subqueries
func TestEvaluateWhereExists(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE departments (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		dept_id INTEGER,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Empty Dept')`)
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Carol')`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"EXISTS", `SELECT * FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)`},
		{"NOT EXISTS", `SELECT * FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)`},
		{"EXISTS with condition", `SELECT * FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.name LIKE 'A%')`},
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

// TestEvaluateWhereAllAny targets evaluateWhere with ALL/ANY subqueries
func TestEvaluateWhereAllAny(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		price INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create products: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE price_thresholds (
		min_price INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create thresholds: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES (1, 100), (2, 200), (3, 300), (4, 400)`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO price_thresholds VALUES (150), (250)`)
	if err != nil {
		t.Fatalf("Failed to insert thresholds: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"> ALL", `SELECT * FROM products WHERE price > ALL (SELECT min_price FROM price_thresholds)`},
		{"> ANY", `SELECT * FROM products WHERE price > ANY (SELECT min_price FROM price_thresholds)`},
		{"< ALL", `SELECT * FROM products WHERE price < ALL (SELECT min_price FROM price_thresholds)`},
		{"= ANY", `SELECT * FROM products WHERE price = ANY (SELECT min_price FROM price_thresholds)`},
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

// TestEvaluateWhereScalarSubquery targets evaluateWhere with scalar subqueries
func TestEvaluateWhereScalarSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE sales (
		id INTEGER PRIMARY KEY,
		amount INTEGER,
		region TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create sales: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO sales VALUES (1, 100, 'North'), (2, 200, 'North'), (3, 150, 'South'), (4, 300, 'South')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"> scalar subquery", `SELECT * FROM sales WHERE amount > (SELECT AVG(amount) FROM sales)`},
		{"= scalar subquery", `SELECT * FROM sales WHERE amount = (SELECT MAX(amount) FROM sales WHERE region = 'North')`},
		{"Scalar in expression", `SELECT * FROM sales WHERE amount * 2 > (SELECT SUM(amount) FROM sales) / 4`},
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

// TestEvaluateWhereDateTime targets evaluateWhere with datetime functions
func TestEvaluateWhereDateTime(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		event_date TEXT,
		event_time TEXT,
		datetime_val TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO events VALUES
		(1, '2024-01-15', '10:30:00', '2024-01-15 10:30:00'),
		(2, '2024-02-20', '14:45:00', '2024-02-20 14:45:00'),
		(3, '2024-03-10', '09:00:00', '2024-03-10 09:00:00'),
		(4, NULL, NULL, NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"DATE function", `SELECT * FROM events WHERE DATE(datetime_val) = '2024-01-15'`},
		{"TIME function", `SELECT * FROM events WHERE TIME(datetime_val) > '10:00:00'`},
		{"DATETIME function", `SELECT * FROM events WHERE DATETIME(event_date || ' ' || event_time) > '2024-01-01 00:00:00'`},
		{"STRFTIME", `SELECT * FROM events WHERE STRFTIME('%Y', event_date) = '2024'`},
		{"Date comparison", `SELECT * FROM events WHERE event_date > '2024-01-31'`},
		{"JULIANDAY", `SELECT * FROM events WHERE JULIANDAY(event_date) > JULIANDAY('2024-01-01')`},
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

// TestEvaluateWhereQualifiedNames targets evaluateWhere with qualified column names
func TestEvaluateWhereQualifiedNames(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE table_a (
		id INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table_a: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE table_b (
		id INTEGER PRIMARY KEY,
		val INTEGER,
		a_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table_b: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO table_a VALUES (1, 100), (2, 200)`)
	if err != nil {
		t.Fatalf("Failed to insert table_a: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO table_b VALUES (1, 50, 1), (2, 150, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert table_b: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Qualified name in JOIN", `SELECT * FROM table_a a JOIN table_b b ON a.id = b.a_id WHERE a.val > b.val`},
		{"Qualified name with alias", `SELECT * FROM table_a a JOIN table_b b ON a.id = b.a_id WHERE a.val = 100`},
		{"Both qualified", `SELECT * FROM table_a a JOIN table_b b ON a.id = b.a_id WHERE table_a.val < table_b.val`},
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
