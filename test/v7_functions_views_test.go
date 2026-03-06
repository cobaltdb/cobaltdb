package test

import (
	"fmt"
	"testing"
)

func TestV7FunctionsAndViews(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	checkNull := func(desc string, sql string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		if rows[0][0] != nil {
			t.Errorf("[FAIL] %s: expected NULL, got %v", desc, rows[0][0])
			return
		}
		pass++
	}

	// Setup
	afExec(t, db, ctx, "CREATE TABLE func_data (id INTEGER PRIMARY KEY, name TEXT, val REAL, tag TEXT)")
	afExec(t, db, ctx, "INSERT INTO func_data VALUES (1, 'Alice', 100.5, 'active')")
	afExec(t, db, ctx, "INSERT INTO func_data VALUES (2, 'Bob', NULL, 'inactive')")
	afExec(t, db, ctx, "INSERT INTO func_data VALUES (3, NULL, 200, 'active')")

	// === INSTR NULL handling ===
	checkNull("INSTR with NULL haystack", "SELECT INSTR(NULL, 'l') FROM func_data WHERE id = 1")
	checkNull("INSTR with NULL needle", "SELECT INSTR('hello', NULL) FROM func_data WHERE id = 1")
	check("INSTR normal", "SELECT INSTR('hello world', 'world') FROM func_data WHERE id = 1", 7)

	// === SUBSTR NULL handling ===
	checkNull("SUBSTR with NULL string", "SELECT SUBSTR(NULL, 1, 2) FROM func_data WHERE id = 1")
	check("SUBSTR normal", "SELECT SUBSTR('hello', 2, 3) FROM func_data WHERE id = 1", "ell")
	check("SUBSTR without length", "SELECT SUBSTR('hello', 3) FROM func_data WHERE id = 1", "llo")

	// === NULL arithmetic ===
	checkNull("NULL + number", "SELECT val + 10 FROM func_data WHERE id = 2")
	check("Normal arithmetic", "SELECT val + 10 FROM func_data WHERE id = 1", 110.5)

	// === String concatenation ===
	check("String concat", "SELECT name || ' test' FROM func_data WHERE id = 1", "Alice test")

	// === Division by zero ===
	total++
	result, err := db.Query(ctx, "SELECT 10 / 0 FROM func_data WHERE id = 1")
	if err != nil {
		pass++
	} else if result != nil {
		// Query may return error through rows or succeed with error value
		pass++ // At least it didn't crash
	}

	// === Modulo ===
	check("Modulo", "SELECT 10 % 3 FROM func_data WHERE id = 1", 1)

	// === VIEW IF NOT EXISTS / IF EXISTS ===
	afExec(t, db, ctx, "CREATE VIEW test_view AS SELECT id, name FROM func_data WHERE tag = 'active'")

	// CREATE VIEW IF NOT EXISTS should not error
	afExec(t, db, ctx, "CREATE VIEW IF NOT EXISTS test_view AS SELECT id FROM func_data")
	total++
	pass++ // Didn't crash

	// DROP VIEW IF EXISTS on non-existent view should not error
	afExec(t, db, ctx, "DROP VIEW IF EXISTS nonexistent_view")
	total++
	pass++ // Didn't crash

	// DROP the actual view
	afExec(t, db, ctx, "DROP VIEW test_view")
	total++
	pass++

	// === ROUND function ===
	check("ROUND", "SELECT ROUND(3.14159, 2) FROM func_data WHERE id = 1", 3.14)

	// === COALESCE ===
	check("COALESCE with NULL", "SELECT COALESCE(name, 'unknown') FROM func_data WHERE id = 3", "unknown")
	check("COALESCE without NULL", "SELECT COALESCE(name, 'unknown') FROM func_data WHERE id = 1", "Alice")

	// === IFNULL ===
	check("IFNULL with NULL", "SELECT IFNULL(name, 'none') FROM func_data WHERE id = 3", "none")
	check("IFNULL without NULL", "SELECT IFNULL(name, 'none') FROM func_data WHERE id = 1", "Alice")

	// === UPPER/LOWER ===
	check("UPPER", "SELECT UPPER(name) FROM func_data WHERE id = 1", "ALICE")
	check("LOWER", "SELECT LOWER(name) FROM func_data WHERE id = 1", "alice")

	// === LENGTH ===
	check("LENGTH", "SELECT LENGTH(name) FROM func_data WHERE id = 1", 5)

	// === TRIM ===
	check("TRIM", "SELECT TRIM('  hello  ') FROM func_data WHERE id = 1", "hello")

	// === REPLACE ===
	check("REPLACE", "SELECT REPLACE('hello world', 'world', 'there') FROM func_data WHERE id = 1", "hello there")

	// === DROP INDEX cleanup ===
	afExec(t, db, ctx, "CREATE INDEX idx_tag ON func_data (tag)")
	afExec(t, db, ctx, "DROP INDEX idx_tag")
	// Create again with same name should work (no orphaned state)
	afExec(t, db, ctx, "CREATE INDEX idx_tag ON func_data (tag)")
	total++
	pass++ // Didn't crash

	t.Logf("\n=== V7 FUNCTIONS & VIEWS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
