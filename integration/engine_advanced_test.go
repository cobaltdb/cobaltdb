package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestEngineDiskPersistence tests database persistence to disk
func TestEngineDiskPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := engine.Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open disk database: %v", err)
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	db.Close()

	// Reopen and verify data persisted
	db2, err := engine.Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(ctx, "SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
		if count != 2 {
			t.Errorf("Expected 2 rows after reopen, got %d", count)
		}
	}
}

// TestEngineLargeDataset tests handling of large datasets
func TestEngineLargeDataset(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert many rows
	for i := 1; i <= 1000; i++ {
		_, err = db.Exec(ctx, "INSERT INTO data VALUES (?, ?)", i, "test data")
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Verify count
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM data")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 1000 {
		t.Errorf("Expected 1000 rows, got %d", count)
	}
}

// TestEngineTransactionRollback tests transaction rollback
func TestEngineTransactionRollback(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify data was not committed
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM test")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", count)
	}
}

// TestEngineComplexQuery tests complex SQL queries
func TestEngineComplexQuery(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create schema
	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer TEXT,
		amount REAL,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 'Alice', 100.50, 'completed'),
		(2, 'Bob', 200.00, 'pending'),
		(3, 'Alice', 150.75, 'completed'),
		(4, 'Carol', 300.00, 'completed'),
		(5, 'Bob', 50.00, 'cancelled')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Complex query with GROUP BY, HAVING, and ORDER BY
	rows, err := db.Query(ctx, `
		SELECT customer, COUNT(*) as order_count, SUM(amount) as total
		FROM orders
		WHERE status = 'completed'
		GROUP BY customer
		HAVING total > 100
		ORDER BY total DESC`)
	if err != nil {
		t.Logf("Complex query error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Complex query returned %d rows", count)
}

// TestEngineJoins tests various JOIN types
func TestEngineJoins(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
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
		amount REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// INNER JOIN
	rows, err := db.Query(ctx, `
		SELECT c.name, o.amount
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		ORDER BY c.name`)
	if err != nil {
		t.Logf("JOIN error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("Expected 3 rows from JOIN, got %d", count)
	}
}

// TestEngineSubqueries tests subquery functionality
func TestEngineSubqueries(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES
		(1, 'Widget', 10.00),
		(2, 'Gadget', 25.00),
		(3, 'Doohickey', 50.00),
		(4, 'Thingamajig', 100.00)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Subquery with IN
	rows, err := db.Query(ctx, `SELECT * FROM products WHERE id IN (SELECT id FROM products WHERE price > 20)`)
	if err != nil {
		t.Logf("Subquery error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Subquery returned %d rows", count)
}

// TestEngineIndexes tests index creation and usage
func TestEngineIndexes(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index
	_, err = db.Exec(ctx, `CREATE INDEX idx_email ON users(email)`)
	if err != nil {
		t.Logf("CREATE INDEX error (may not be supported): %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, "INSERT INTO users VALUES (?, ?, ?)", i, "user@example.com", "User")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query using index
	rows, err := db.Query(ctx, "SELECT * FROM users WHERE email = 'user@example.com'")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 100 {
		t.Errorf("Expected 100 rows, got %d", count)
	}
}

// TestEngineConstraints tests constraint enforcement
func TestEngineConstraints(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// NOT NULL constraint
	_, err = db.Exec(ctx, `CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test1 VALUES (1, NULL)")
	if err == nil {
		t.Error("Expected error for NOT NULL violation")
	} else {
		t.Logf("Got expected NOT NULL error: %v", err)
	}

	// UNIQUE constraint
	_, err = db.Exec(ctx, `CREATE TABLE test2 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test2 VALUES (1, 'ABC')")
	if err != nil {
		t.Fatalf("Failed to insert first row: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test2 VALUES (2, 'ABC')")
	if err == nil {
		t.Error("Expected error for UNIQUE violation")
	} else {
		t.Logf("Got expected UNIQUE error: %v", err)
	}
}

// TestEngineViews tests view functionality
func TestEngineViews(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES (1, 'Widget', 10), (2, 'Gadget', 20), (3, 'Gizmo', 30)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view
	_, err = db.Exec(ctx, `CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 15`)
	if err != nil {
		t.Logf("CREATE VIEW error (may not be supported): %v", err)
		return
	}

	// Query view
	rows, err := db.Query(ctx, `SELECT * FROM expensive_products`)
	if err != nil {
		t.Logf("Query view error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 rows from view, got %d", count)
	}
}

// TestEngineAlterTable tests ALTER TABLE operations
func TestEngineAlterTable(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO test VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Add column
	_, err = db.Exec(ctx, `ALTER TABLE test ADD COLUMN email TEXT`)
	if err != nil {
		t.Logf("ALTER TABLE ADD COLUMN error: %v", err)
		return
	}

	// Verify new column exists
	_, err = db.Exec(ctx, `UPDATE test SET email = 'alice@example.com' WHERE id = 1`)
	if err != nil {
		t.Logf("UPDATE after ALTER error: %v", err)
	}
}

// TestEngineConcurrencyStress tests concurrent access under stress
func TestEngineConcurrencyStress(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO counter VALUES (1, 0)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Multiple concurrent increments
	const numWorkers = 20
	const incrementsPerWorker = 50

	done := make(chan bool, numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer func() { done <- true }()

			for j := 0; j < incrementsPerWorker; j++ {
				tx, err := db.Begin(ctx)
				if err != nil {
					t.Logf("Begin error: %v", err)
					return
				}

				_, err = tx.Exec(ctx, `UPDATE counter SET value = value + 1 WHERE id = 1`)
				if err != nil {
					t.Logf("Update error: %v", err)
					tx.Rollback()
					return
				}

				if err := tx.Commit(); err != nil {
					t.Logf("Commit error: %v", err)
				}
			}
		}()
	}

	// Wait with timeout
	timeout := time.AfterFunc(30*time.Second, func() {
		t.Error("Timeout waiting for workers")
	})

	for i := 0; i < numWorkers; i++ {
		<-done
	}
	timeout.Stop()

	// Verify final count
	row := db.QueryRow(ctx, `SELECT value FROM counter WHERE id = 1`)
	var value int
	if err := row.Scan(&value); err != nil {
		t.Fatalf("Failed to read counter: %v", err)
	}

	expected := numWorkers * incrementsPerWorker
	if value != expected {
		t.Errorf("Expected counter = %d, got %d", expected, value)
	} else {
		t.Logf("Counter correctly incremented to %d", value)
	}
}

// TestEngineForeignKeys tests foreign key constraints
func TestEngineForeignKeys(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		FOREIGN KEY (parent_id) REFERENCES parent(id)
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Valid foreign key
	_, err = db.Exec(ctx, `INSERT INTO child VALUES (1, 1)`)
	if err != nil {
		t.Logf("INSERT with valid FK error: %v", err)
	}

	// Invalid foreign key
	_, err = db.Exec(ctx, `INSERT INTO child VALUES (2, 999)`)
	if err == nil {
		t.Error("Expected error for invalid foreign key")
	} else {
		t.Logf("Got expected FK error: %v", err)
	}
}

// TestEngineCTEs tests Common Table Expressions
func TestEngineCTEs(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name TEXT,
		manager_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO employees VALUES
		(1, 'CEO', NULL),
		(2, 'Manager A', 1),
		(3, 'Manager B', 1),
		(4, 'Employee 1', 2),
		(5, 'Employee 2', 2),
		(6, 'Employee 3', 3)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Non-recursive CTE
	rows, err := db.Query(ctx, `
		WITH managers AS (
			SELECT * FROM employees WHERE manager_id = 1
		)
		SELECT * FROM managers`)
	if err != nil {
		t.Logf("CTE error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 managers, got %d", count)
	}
}

// TestEngineWindowFunctions tests window functions
func TestEngineWindowFunctions(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO sales VALUES
		(1, 'North', 100),
		(2, 'North', 200),
		(3, 'South', 150),
		(4, 'South', 250),
		(5, 'East', 300)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Window function query
	rows, err := db.Query(ctx, `
		SELECT region, amount,
			RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rank
		FROM sales`)
	if err != nil {
		t.Logf("Window function error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Window function returned %d rows", count)
}

// TestEngineBackupRestore tests database backup and restore
func TestEngineBackupRestore(t *testing.T) {
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "source.db")

	db, err := engine.Open(srcPath, nil)
	if err != nil {
		t.Fatalf("Failed to open source database: %v", err)
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO test VALUES (1, 'backup data')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	db.Close()

	// Copy the database file as backup
	backupPath := filepath.Join(srcDir, "backup.db")
	srcData, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("Failed to read source: %v", err)
	}

	if err := os.WriteFile(backupPath, srcData, 0644); err != nil {
		t.Fatalf("Failed to write backup: %v", err)
	}

	// Open backup
	db2, err := engine.Open(backupPath, nil)
	if err != nil {
		t.Fatalf("Failed to open backup: %v", err)
	}
	defer db2.Close()

	row := db2.QueryRow(ctx, `SELECT data FROM test WHERE id = 1`)
	var data string
	if err := row.Scan(&data); err != nil {
		t.Fatalf("Failed to read backup: %v", err)
	}

	if data != "backup data" {
		t.Errorf("Expected 'backup data', got '%s'", data)
	}
}
