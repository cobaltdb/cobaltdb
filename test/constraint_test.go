package test

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// Helper to create an in-memory test database
func newConstraintTestDB(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	return db
}

func TestPrimaryKeyDuplicateRejected(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO t1 (id, name) VALUES (1, 'Alice')")

	// Should fail - duplicate PK
	_, err := db.Exec(ctx, "INSERT INTO t1 (id, name) VALUES (1, 'Bob')")
	if err == nil {
		t.Fatal("Expected error for duplicate primary key, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate") && !strings.Contains(err.Error(), "UNIQUE") {
		t.Fatalf("Expected duplicate/UNIQUE error, got: %v", err)
	}

	// Verify original row still exists
	rows, _ := db.Query(ctx, "SELECT name FROM t1 WHERE id = 1")
	defer rows.Close()
	rows.Next()
	var name string
	rows.Scan(&name)
	if name != "Alice" {
		t.Fatalf("Expected Alice, got %s (original row was overwritten!)", name)
	}
}

func TestNotNullConstraint(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)")

	// Should succeed - name is provided
	_, err := db.Exec(ctx, "INSERT INTO t1 (id, name, email) VALUES (1, 'Alice', 'alice@test.com')")
	if err != nil {
		t.Fatalf("Valid insert failed: %v", err)
	}

	// Should fail - name is NULL
	_, err = db.Exec(ctx, "INSERT INTO t1 (id, name, email) VALUES (2, NULL, 'bob@test.com')")
	if err == nil {
		t.Fatal("Expected NOT NULL error, got nil")
	}
	if !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("Expected NOT NULL error, got: %v", err)
	}

	// Should fail - name omitted (becomes NULL)
	_, err = db.Exec(ctx, "INSERT INTO t1 (id, email) VALUES (3, 'carol@test.com')")
	if err == nil {
		t.Fatal("Expected NOT NULL error for omitted column, got nil")
	}
}

func TestUniqueConstraint(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")

	db.Exec(ctx, "INSERT INTO t1 (id, email, name) VALUES (1, 'alice@test.com', 'Alice')")

	// Should fail - duplicate email
	_, err := db.Exec(ctx, "INSERT INTO t1 (id, email, name) VALUES (2, 'alice@test.com', 'Bob')")
	if err == nil {
		t.Fatal("Expected UNIQUE constraint error, got nil")
	}
	if !strings.Contains(err.Error(), "UNIQUE") {
		t.Fatalf("Expected UNIQUE error, got: %v", err)
	}

	// Should succeed - different email
	_, err = db.Exec(ctx, "INSERT INTO t1 (id, email, name) VALUES (2, 'bob@test.com', 'Bob')")
	if err != nil {
		t.Fatalf("Valid unique insert failed: %v", err)
	}

	// NULL values should be allowed (SQL standard: NULLs are not considered duplicates)
	_, err = db.Exec(ctx, "INSERT INTO t1 (id, email, name) VALUES (3, NULL, 'Carol')")
	if err != nil {
		t.Fatalf("NULL in UNIQUE column should be allowed: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO t1 (id, email, name) VALUES (4, NULL, 'Dave')")
	if err != nil {
		t.Fatalf("Multiple NULLs in UNIQUE column should be allowed: %v", err)
	}
}

func TestDefaultValues(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'Unknown', active BOOLEAN DEFAULT TRUE)")

	// Insert without name - should get default
	db.Exec(ctx, "INSERT INTO t1 (id) VALUES (1)")

	rows, _ := db.Query(ctx, "SELECT name FROM t1 WHERE id = 1")
	defer rows.Close()
	if rows.Next() {
		var name string
		rows.Scan(&name)
		if name != "Unknown" {
			t.Fatalf("Expected default 'Unknown', got '%s'", name)
		}
	} else {
		t.Fatal("Expected row with id=1")
	}
}

func TestCheckConstraint(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0), name TEXT)")

	// Should succeed - valid age
	_, err := db.Exec(ctx, "INSERT INTO t1 (id, age, name) VALUES (1, 25, 'Alice')")
	if err != nil {
		t.Fatalf("Valid insert failed: %v", err)
	}

	// Should fail - negative age
	_, err = db.Exec(ctx, "INSERT INTO t1 (id, age, name) VALUES (2, -5, 'Bob')")
	if err == nil {
		t.Fatal("Expected CHECK constraint error, got nil")
	}
	if !strings.Contains(err.Error(), "CHECK") {
		t.Fatalf("Expected CHECK constraint error, got: %v", err)
	}

	// Zero should be valid (edge case)
	_, err = db.Exec(ctx, "INSERT INTO t1 (id, age, name) VALUES (3, 0, 'Carol')")
	if err != nil {
		t.Fatalf("Insert with age=0 should succeed: %v", err)
	}
}

func TestAutoIncrementConstraints(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")

	db.Exec(ctx, "INSERT INTO t1 (name) VALUES ('Alice')")
	db.Exec(ctx, "INSERT INTO t1 (name) VALUES ('Bob')")
	db.Exec(ctx, "INSERT INTO t1 (name) VALUES ('Carol')")

	// Verify IDs are monotonically increasing
	rows, _ := db.Query(ctx, "SELECT id FROM t1 ORDER BY id")
	defer rows.Close()

	prevID := 0
	count := 0
	for rows.Next() {
		var id int
		rows.Scan(&id)
		if id <= prevID {
			t.Fatalf("IDs not increasing: prev=%d, curr=%d", prevID, id)
		}
		prevID = id
		count++
	}
	if count != 3 {
		t.Fatalf("Expected 3 rows, got %d", count)
	}
}

func TestMultipleConstraintsCombined(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT UNIQUE NOT NULL, price REAL CHECK (price > 0), name TEXT NOT NULL DEFAULT 'Unnamed')")

	// Valid insert
	_, err := db.Exec(ctx, "INSERT INTO products (id, sku, price) VALUES (1, 'SKU001', 9.99)")
	if err != nil {
		t.Fatalf("Valid insert failed: %v", err)
	}

	// Verify default name was applied
	rows, _ := db.Query(ctx, "SELECT name FROM products WHERE id = 1")
	rows.Next()
	var name string
	rows.Scan(&name)
	rows.Close()
	if name != "Unnamed" {
		t.Fatalf("Expected default name 'Unnamed', got '%s'", name)
	}

	// Duplicate PK
	_, err = db.Exec(ctx, "INSERT INTO products (id, sku, price) VALUES (1, 'SKU002', 19.99)")
	if err == nil {
		t.Fatal("Expected PK duplicate error")
	}

	// Duplicate SKU
	_, err = db.Exec(ctx, "INSERT INTO products (id, sku, price) VALUES (2, 'SKU001', 19.99)")
	if err == nil {
		t.Fatal("Expected UNIQUE constraint error for SKU")
	}

	// NULL SKU (violates NOT NULL)
	_, err = db.Exec(ctx, "INSERT INTO products (id, sku, price) VALUES (3, NULL, 19.99)")
	if err == nil {
		t.Fatal("Expected NOT NULL error for SKU")
	}

	// Negative price (violates CHECK)
	_, err = db.Exec(ctx, "INSERT INTO products (id, sku, price) VALUES (4, 'SKU004', -5.00)")
	if err == nil {
		t.Fatal("Expected CHECK constraint error for negative price")
	}

	// Valid insert should still work
	_, err = db.Exec(ctx, "INSERT INTO products (id, sku, price, name) VALUES (5, 'SKU005', 29.99, 'Widget')")
	if err != nil {
		t.Fatalf("Valid insert after constraint errors failed: %v", err)
	}

	// Verify only 2 rows exist (1 and 5)
	rows, _ = db.Query(ctx, "SELECT COUNT(*) FROM products")
	rows.Next()
	var count int
	rows.Scan(&count)
	rows.Close()
	if count != 2 {
		t.Fatalf("Expected 2 rows, got %d", count)
	}
}

func TestShowTables(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")

	rows, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) == 0 {
		t.Fatal("Expected columns from SHOW TABLES")
	}

	count := 0
	for rows.Next() {
		var tableName string
		rows.Scan(&tableName)
		t.Logf("Table: %s", tableName)
		count++
	}
	if count < 2 {
		t.Fatalf("Expected at least 2 tables, got %d", count)
	}
}

func TestShowCreateTable(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)")

	rows, err := db.Query(ctx, "SHOW CREATE TABLE users")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected result from SHOW CREATE TABLE")
	}
	var tableName, createSQL string
	rows.Scan(&tableName, &createSQL)

	if tableName != "users" {
		t.Fatalf("Expected table name 'users', got '%s'", tableName)
	}
	if !strings.Contains(createSQL, "CREATE TABLE users") {
		t.Fatalf("Expected CREATE TABLE statement, got: %s", createSQL)
	}
	if !strings.Contains(createSQL, "NOT NULL") {
		t.Fatalf("Expected NOT NULL in schema, got: %s", createSQL)
	}
}

func TestShowDatabases(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	rows, err := db.Query(ctx, "SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected at least one database")
	}
	var dbName string
	rows.Scan(&dbName)
	if dbName != "cobaltdb" {
		t.Fatalf("Expected 'cobaltdb', got '%s'", dbName)
	}
}

func TestDescribeTable(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT UNIQUE)")

	rows, err := db.Query(ctx, "DESCRIBE users")
	if err != nil {
		t.Fatalf("DESCRIBE failed: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) < 4 {
		t.Fatalf("Expected at least 4 columns (Field, Type, Null, Key, ...), got %d: %v", len(cols), cols)
	}

	count := 0
	for rows.Next() {
		var field, typ, nullable, key, defVal, extra string
		rows.Scan(&field, &typ, &nullable, &key, &defVal, &extra)
		t.Logf("Field=%s Type=%s Null=%s Key=%s Default=%s Extra=%s", field, typ, nullable, key, defVal, extra)
		count++
	}
	if count != 3 {
		t.Fatalf("Expected 3 columns, got %d", count)
	}
}

func TestSetCommand(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	// SET commands should be accepted without error (MySQL compatibility)
	_, err := db.Exec(ctx, "SET NAMES utf8")
	if err != nil {
		t.Fatalf("SET NAMES failed: %v", err)
	}

	_, err = db.Exec(ctx, "SET character_set_client = utf8mb4")
	if err != nil {
		t.Fatalf("SET variable failed: %v", err)
	}
}

func TestUseCommand(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	// USE commands should be accepted without error (MySQL compatibility)
	_, err := db.Exec(ctx, "USE cobaltdb")
	if err != nil {
		t.Fatalf("USE failed: %v", err)
	}
}

func TestShowColumnsFrom(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)")

	rows, err := db.Query(ctx, "SHOW COLUMNS FROM products")
	if err != nil {
		t.Fatalf("SHOW COLUMNS FROM failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var field, typ, nullable, key, defVal, extra string
		rows.Scan(&field, &typ, &nullable, &key, &defVal, &extra)
		t.Logf("Column: %s %s", field, typ)
		count++
	}
	if count != 3 {
		t.Fatalf("Expected 3 columns, got %d", count)
	}
}

func TestUpdateConstraints(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL)")
	db.Exec(ctx, "INSERT INTO t1 (id, email, name) VALUES (1, 'alice@test.com', 'Alice')")
	db.Exec(ctx, "INSERT INTO t1 (id, email, name) VALUES (2, 'bob@test.com', 'Bob')")

	// Valid update
	_, err := db.Exec(ctx, "UPDATE t1 SET name = 'Alice Smith' WHERE id = 1")
	if err != nil {
		t.Fatalf("Valid update failed: %v", err)
	}

	// Verify update
	rows, _ := db.Query(ctx, "SELECT name FROM t1 WHERE id = 1")
	rows.Next()
	var name string
	rows.Scan(&name)
	rows.Close()
	if name != "Alice Smith" {
		t.Fatalf("Expected 'Alice Smith', got '%s'", name)
	}
}

func TestTransactionRollbackConstraints(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO t1 (id, name) VALUES (1, 'Alice')")

	// Begin transaction
	db.Exec(ctx, "BEGIN")
	db.Exec(ctx, "INSERT INTO t1 (id, name) VALUES (2, 'Bob')")
	db.Exec(ctx, "ROLLBACK")

	// Verify Bob was rolled back
	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM t1")
	rows.Next()
	var count int
	rows.Scan(&count)
	rows.Close()
	if count != 1 {
		t.Fatalf("Expected 1 row after rollback, got %d", count)
	}
}

func TestLargeInsertWithConstraints(t *testing.T) {
	db := newConstraintTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT UNIQUE)")

	// Insert 100 unique values
	for i := 1; i <= 100; i++ {
		_, err := db.Exec(ctx, "INSERT INTO t1 (value) VALUES (?)", strings.Repeat("x", i))
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify count
	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM t1")
	rows.Next()
	var count int
	rows.Scan(&count)
	rows.Close()
	if count != 100 {
		t.Fatalf("Expected 100 rows, got %d", count)
	}
}
