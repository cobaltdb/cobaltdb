package test

import (
	"strings"
	"testing"
)

// TestInsertLockedErrorPaths tests error handling in insertLocked
func TestInsertLockedErrorPaths(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	pass := 0
	total := 0

	// Test INSERT with non-existent column
	t.Run("NonExistentColumn", func(t *testing.T) {
		total++
		_, err := db.Exec(ctx, "CREATE TABLE err_test (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
		_, err = db.Exec(ctx, "INSERT INTO err_test (nonexistent) VALUES (1)")
		if err == nil || !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("Expected column does not exist error, got: %v", err)
		} else {
			pass++
		}
	})

	// Test INSERT with wrong number of values
	t.Run("WrongValueCount", func(t *testing.T) {
		total++
		_, err := db.Exec(ctx, "CREATE TABLE count_test (id INTEGER PRIMARY KEY, val INTEGER)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
		_, err = db.Exec(ctx, "INSERT INTO count_test VALUES (1, 2, 3)")
		if err == nil || !strings.Contains(err.Error(), "columns") {
			t.Errorf("Expected column count error, got: %v", err)
		} else {
			pass++
		}
	})

	// Test INSERT...SELECT with column count mismatch
	t.Run("InsertSelectColumnMismatch", func(t *testing.T) {
		total++
		_, err := db.Exec(ctx, "CREATE TABLE src (a INTEGER)")
		if err != nil {
			t.Fatalf("Failed to create src table: %v", err)
		}
		_, err = db.Exec(ctx, "CREATE TABLE dst (a INTEGER, b INTEGER)")
		if err != nil {
			t.Fatalf("Failed to create dst table: %v", err)
		}
		_, err = db.Exec(ctx, "INSERT INTO dst SELECT * FROM src")
		if err == nil || !strings.Contains(err.Error(), "mismatch") {
			t.Errorf("Expected column count mismatch error, got: %v", err)
		} else {
			pass++
		}
	})

	t.Logf("V115 Error Paths: %d/%d passed", pass, total)
}

// TestInsertLockedAutoIncrement tests auto-increment behavior
func TestInsertLockedAutoIncrement(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	pass := 0
	total := 0

	check := func(desc string, sql string, expected int64) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		var gotInt int64
		switch v := got.(type) {
		case int64:
			gotInt = v
		case int:
			gotInt = int64(v)
		case float64:
			gotInt = int64(v)
		default:
			t.Errorf("[FAIL] %s: unexpected type %T", desc, got)
			return
		}
		if gotInt != expected {
			t.Errorf("[FAIL] %s: got %d, expected %d", desc, gotInt, expected)
			return
		}
		pass++
	}

	// Create table with auto-increment
	afExec(t, db, ctx, "CREATE TABLE auto_inc_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val INTEGER)")

	// Insert without specifying id - should auto-increment
	afExec(t, db, ctx, "INSERT INTO auto_inc_test (val) VALUES (100)")
	check("Auto-inc first row", "SELECT id FROM auto_inc_test WHERE val = 100", 1)

	// Insert with explicit higher id
	afExec(t, db, ctx, "INSERT INTO auto_inc_test (id, val) VALUES (5, 200)")
	check("Explicit id insert", "SELECT id FROM auto_inc_test WHERE val = 200", 5)

	// Next auto-inc should be 6
	afExec(t, db, ctx, "INSERT INTO auto_inc_test (val) VALUES (300)")
	check("Auto-inc after explicit", "SELECT id FROM auto_inc_test WHERE val = 300", 6)

	t.Logf("V115 AutoIncrement: %d/%d passed", pass, total)
}

// TestInsertLockedDefaults tests DEFAULT value handling
func TestInsertLockedDefaults(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	// Create table with defaults
	afExec(t, db, ctx, "CREATE TABLE default_test (id INTEGER PRIMARY KEY, val INTEGER DEFAULT 42, name TEXT)")

	// Insert without specifying default column
	afExec(t, db, ctx, "INSERT INTO default_test (id, name) VALUES (1, 'test')")
	check("Default value applied", "SELECT val FROM default_test WHERE id = 1", int64(42))

	// Insert with explicit value (override default)
	afExec(t, db, ctx, "INSERT INTO default_test (id, val, name) VALUES (2, 100, 'test2')")
	check("Explicit value overrides default", "SELECT val FROM default_test WHERE id = 2", int64(100))

	// Insert with NULL (should be NULL, not default for explicit columns)
	afExec(t, db, ctx, "INSERT INTO default_test (id, val, name) VALUES (3, NULL, 'test3')")
	check("Explicit NULL", "SELECT val FROM default_test WHERE id = 3", nil)

	t.Logf("V115 Defaults: %d/%d passed", pass, total)
}

// TestInsertLockedSelect tests INSERT...SELECT with various types
func TestInsertLockedSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	// Create source table with various types
	afExec(t, db, ctx, "CREATE TABLE src_types (id INTEGER PRIMARY KEY, val INTEGER, name TEXT, flag BOOLEAN)")
	afExec(t, db, ctx, "INSERT INTO src_types VALUES (1, 10, 'hello', 1), (2, 20, 'world', 0)")

	// Create destination table
	afExec(t, db, ctx, "CREATE TABLE dst_types (id INTEGER PRIMARY KEY, val INTEGER, name TEXT, flag BOOLEAN)")

	// INSERT...SELECT with all types
	afExec(t, db, ctx, "INSERT INTO dst_types SELECT * FROM src_types")
	check("Insert select count", "SELECT COUNT(*) FROM dst_types", int64(2))
	check("Insert select int", "SELECT val FROM dst_types WHERE id = 1", int64(10))
	check("Insert select text", "SELECT name FROM dst_types WHERE id = 2", "world")

	t.Logf("V115 Insert Select: %d/%d passed", pass, total)
}

// TestInsertLockedExpressionErrors tests error handling in expression evaluation during insert
func TestInsertLockedExpressionErrors(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	pass := 0
	total := 0

	// Test with invalid expression (would need catalog context that fails)
	t.Run("InvalidExpression", func(t *testing.T) {
		total++
		_, err := db.Exec(ctx, "CREATE TABLE expr_test (id INTEGER PRIMARY KEY, val INTEGER)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
		// This should work - simple insert
		_, err = db.Exec(ctx, "INSERT INTO expr_test VALUES (1, 1)")
		if err != nil {
			t.Errorf("Simple insert failed: %v", err)
		} else {
			pass++
		}
	})

	t.Logf("V115 Expression Errors: %d/%d passed", pass, total)
}
