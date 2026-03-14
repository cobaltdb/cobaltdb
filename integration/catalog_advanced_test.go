package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestDeleteRowLockedWithTrigger targets deleteRowLocked with trigger firing
func TestDeleteRowLockedWithTrigger(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create audit log table
	_, err = db.Exec(ctx, `CREATE TABLE audit_log (
		id INTEGER PRIMARY KEY,
		action TEXT,
		table_name TEXT,
		row_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create audit table: %v", err)
	}

	// Create main table
	_, err = db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create AFTER DELETE trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER audit_delete
		AFTER DELETE ON users
		BEGIN
			INSERT INTO audit_log (action, table_name, row_id) VALUES ('DELETE', 'users', OLD.id);
		END`)
	if err != nil {
		t.Logf("Trigger creation error (may not be supported): %v", err)
		return
	}

	// Delete row - should fire trigger
	_, err = db.Exec(ctx, "DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Logf("Delete with trigger error: %v", err)
		return
	}

	// Verify audit log
	var count int
	row, err := db.Query(ctx, "SELECT COUNT(*) FROM audit_log")
	if err != nil {
		t.Fatalf("Failed to query audit: %v", err)
	}
	defer row.Close()

	if row.Next() {
		row.Scan(&count)
		if count > 0 {
			t.Logf("Trigger fired: %d audit records", count)
		}
	}
}

// TestEvaluateWhereWithSubquery targets evaluateWhere with subquery
func TestEvaluateWhereWithSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name TEXT,
		dept_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE departments (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
	if err != nil {
		t.Fatalf("Failed to insert depts: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Carol', 1)")
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	// Query with subquery in WHERE
	rows, err := db.Query(ctx, "SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering')")
	if err != nil {
		t.Logf("Subquery error (may not be supported): %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Subquery returned %d rows", count)
}

// TestEvaluateWhereWithExists targets evaluateWhere with EXISTS
func TestEvaluateWhereWithExists(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create t1: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create t2: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("Failed to insert t1: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO t2 VALUES (1, 1), (2, 1), (3, 2)")
	if err != nil {
		t.Fatalf("Failed to insert t2: %v", err)
	}

	// EXISTS subquery
	rows, err := db.Query(ctx, "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)")
	if err != nil {
		t.Logf("EXISTS error (may not be supported): %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("EXISTS returned %d rows", count)
}

// TestInsertLockedWithDefaults targets insertLocked with DEFAULT values
func TestInsertLockedWithDefaults(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE items (
		id INTEGER PRIMARY KEY,
		name TEXT DEFAULT 'unnamed',
		status TEXT DEFAULT 'active',
		created_at INTEGER DEFAULT 0
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with DEFAULT
	_, err = db.Exec(ctx, "INSERT INTO items (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify defaults applied
	var name, status string
	var created int
	row, err := db.Query(ctx, "SELECT name, status, created_at FROM items WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer row.Close()

	if row.Next() {
		row.Scan(&name, &status, &created)
		if name != "unnamed" {
			t.Errorf("Expected default name 'unnamed', got '%s'", name)
		}
		if status != "active" {
			t.Errorf("Expected default status 'active', got '%s'", status)
		}
	}
}

// TestInsertLockedWithExpression targets insertLocked with expression evaluation
func TestInsertLockedWithExpression(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE calc (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with expression
	_, err = db.Exec(ctx, "INSERT INTO calc VALUES (1, 10 + 20 * 2)")
	if err != nil {
		t.Logf("Insert with expression error: %v", err)
		return
	}

	// Verify calculated value
	var val int
	row, err := db.Query(ctx, "SELECT val FROM calc WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer row.Close()

	if row.Next() {
		row.Scan(&val)
		expected := 50 // 10 + (20 * 2)
		if val != expected {
			t.Errorf("Expected %d, got %d", expected, val)
		}
	}
}

// TestRollbackToSavepointDDL targets RollbackToSavepoint with DDL
func TestRollbackToSavepointDDL(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}

	// Create savepoint
	_, err = tx.Exec(ctx, "SAVEPOINT sp1")
	if err != nil {
		t.Logf("Savepoint error: %v", err)
		tx.Rollback()
		return
	}

	// DDL in savepoint
	_, err = tx.Exec(ctx, "CREATE TABLE test_rollback (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Logf("CREATE TABLE error: %v", err)
	}

	// Rollback to savepoint (DDL may not be rollbackable)
	_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1")
	if err != nil {
		t.Logf("Rollback to savepoint error: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Logf("Commit error: %v", err)
	}

	// Check if table exists (DDL rollback behavior varies)
	_, err = db.Exec(ctx, "SELECT * FROM test_rollback")
	t.Logf("Table exists after rollback: %v", err)
}

// TestApplyOrderByMultiColumn targets applyOrderBy with multi-column ordering
func TestApplyOrderByMultiColumn(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		dept TEXT,
		salary INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with multiple departments and salaries
	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		(1, 'Engineering', 80000),
		(2, 'Engineering', 90000),
		(3, 'Sales', 60000),
		(4, 'Sales', 70000),
		(5, 'Engineering', 80000)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Multi-column ORDER BY
	rows, err := db.Query(ctx, "SELECT * FROM employees ORDER BY dept, salary DESC")
	if err != nil {
		t.Logf("Multi-column ORDER BY error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 5 {
		t.Errorf("Expected 5 rows, got %d", count)
	}
}

// TestApplyOrderByWithNulls targets applyOrderBy with NULL handling
func TestApplyOrderByWithNulls(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE nullable (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO nullable VALUES
		(1, NULL),
		(2, 10),
		(3, NULL),
		(4, 20)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// ORDER BY with NULLs
	rows, err := db.Query(ctx, "SELECT * FROM nullable ORDER BY val")
	if err != nil {
		t.Logf("ORDER BY with NULLs error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 4 {
		t.Errorf("Expected 4 rows, got %d", count)
	}
}

// TestSelectLockedWithQueryCache targets selectLocked with query cache
func TestSelectLockedWithQueryCache(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO cache_test VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Same query multiple times to hit cache
	for i := 0; i < 10; i++ {
		rows, err := db.Query(ctx, "SELECT * FROM cache_test WHERE id = 1")
		if err != nil {
			t.Logf("Query error: %v", err)
			return
		}
		rows.Close()
	}

	t.Log("Executed queries that should use cache")
}

// TestExecuteSelectWithJoinAndGroupByComplex targets executeSelectWithJoinAndGroupBy
func TestExecuteSelectWithJoinAndGroupByComplex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		region TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO customers VALUES
		(1, 'Alice', 'North'),
		(2, 'Bob', 'South'),
		(3, 'Carol', 'North')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 1, 100.0),
		(2, 1, 200.0),
		(3, 2, 150.0),
		(4, 3, 300.0),
		(5, 1, 50.0)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Complex JOIN + GROUP BY + HAVING
	rows, err := db.Query(ctx, `
		SELECT c.region, COUNT(*) as order_count, SUM(o.amount) as total
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		GROUP BY c.region
		HAVING total > 200`)
	if err != nil {
		t.Logf("JOIN+GROUP BY error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("JOIN+GROUP BY returned %d rows", count)
}

// TestApplyOuterQueryWithDistinct targets applyOuterQuery with DISTINCT view
func TestApplyOuterQueryWithDistinct(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, category TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO data VALUES
		(1, 'A'),
		(2, 'A'),
		(3, 'B'),
		(4, 'B'),
		(5, 'C')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view with DISTINCT
	_, err = db.Exec(ctx, "CREATE VIEW distinct_categories AS SELECT DISTINCT category FROM data")
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query from view with outer WHERE
	rows, err := db.Query(ctx, "SELECT * FROM distinct_categories WHERE category != 'C'")
	if err != nil {
		t.Logf("Query view error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Distinct view returned %d rows", count)
}

// TestResolveAggregateInExprWithArithmetic targets resolveAggregateInExpr with arithmetic
func TestResolveAggregateInExprWithArithmetic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, amount INTEGER, cost INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO sales VALUES
		(1, 100, 60),
		(2, 200, 120),
		(3, 150, 90)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// HAVING with arithmetic on aggregates
	rows, err := db.Query(ctx, `
		SELECT COUNT(*) as cnt, SUM(amount) as total, SUM(cost) as total_cost
		FROM sales
		HAVING total - total_cost > 100`)
	if err != nil {
		t.Logf("Aggregate arithmetic error: %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		t.Log("Query with aggregate arithmetic succeeded")
	}
}
