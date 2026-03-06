package test

import (
	"fmt"
	"testing"
)

func TestV10TypeHandling(t *testing.T) {
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

	checkType := func(desc string, sql string, expectedType string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		if got != expectedType {
			t.Errorf("[FAIL] %s: got type %s, expected %s", desc, got, expectedType)
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
	afExec(t, db, ctx, "CREATE TABLE types_test (id INTEGER PRIMARY KEY, int_val INTEGER, real_val REAL, text_val TEXT)")
	afExec(t, db, ctx, "INSERT INTO types_test VALUES (1, 42, 3.14, 'hello')")
	afExec(t, db, ctx, "INSERT INTO types_test VALUES (2, 100, 2.5, 'world')")
	afExec(t, db, ctx, "INSERT INTO types_test VALUES (3, -7, 0.0, 'test')")

	// === INTEGER TYPE PRESERVATION ===
	check("Integer column value", "SELECT int_val FROM types_test WHERE id = 1", 42)
	check("Integer column negative", "SELECT int_val FROM types_test WHERE id = 3", -7)
	checkType("TYPEOF integer column", "SELECT TYPEOF(int_val) FROM types_test WHERE id = 1", "integer")

	// === REAL TYPE ===
	check("Real column value", "SELECT real_val FROM types_test WHERE id = 1", 3.14)
	checkType("TYPEOF real column", "SELECT TYPEOF(real_val) FROM types_test WHERE id = 1", "real")

	// === TEXT TYPE ===
	check("Text column value", "SELECT text_val FROM types_test WHERE id = 1", "hello")
	checkType("TYPEOF text column", "SELECT TYPEOF(text_val) FROM types_test WHERE id = 1", "text")

	// === TYPEOF NULL ===
	checkType("TYPEOF NULL", "SELECT TYPEOF(NULL) FROM types_test WHERE id = 1", "null")

	// === CAST TO INTEGER ===
	check("CAST float to int", "SELECT CAST(3.7 AS INTEGER) FROM types_test WHERE id = 1", 3)
	check("CAST string to int", "SELECT CAST('123' AS INTEGER) FROM types_test WHERE id = 1", 123)
	check("CAST text col to int", "SELECT CAST('456' AS INTEGER) FROM types_test WHERE id = 1", 456)

	// === CAST TO REAL ===
	check("CAST int to real", "SELECT CAST(42 AS REAL) FROM types_test WHERE id = 1", 42)
	check("CAST string to real", "SELECT CAST('3.14' AS REAL) FROM types_test WHERE id = 1", 3.14)

	// === CAST TO TEXT ===
	check("CAST int to text", "SELECT CAST(42 AS TEXT) FROM types_test WHERE id = 1", "42")

	// === CAST TO BOOLEAN ===
	check("CAST 1 to bool", "SELECT CAST(1 AS BOOLEAN) FROM types_test WHERE id = 1", true)
	check("CAST 0 to bool", "SELECT CAST(0 AS BOOLEAN) FROM types_test WHERE id = 1", false)
	check("CAST 'true' to bool", "SELECT CAST('true' AS BOOLEAN) FROM types_test WHERE id = 1", true)

	// === CAST NULL ===
	checkNull("CAST NULL to int", "SELECT CAST(NULL AS INTEGER) FROM types_test WHERE id = 1")

	// === ARITHMETIC INTEGER PRESERVATION ===
	check("Int + Int", "SELECT 10 + 20 FROM types_test WHERE id = 1", 30)
	check("Int - Int", "SELECT 50 - 20 FROM types_test WHERE id = 1", 30)
	check("Int * Int", "SELECT 6 * 7 FROM types_test WHERE id = 1", 42)
	check("Int / Int = float", "SELECT 10 / 3 FROM types_test WHERE id = 1", 3.3333333333333335)
	check("Int % Int", "SELECT 10 % 3 FROM types_test WHERE id = 1", 1)

	// === ARITHMETIC WITH COLUMN VALUES ===
	check("Column + literal", "SELECT int_val + 8 FROM types_test WHERE id = 1", 50)
	check("Column * column", "SELECT int_val * int_val FROM types_test WHERE id = 1", 1764)
	check("Column - column", "SELECT int_val - int_val FROM types_test WHERE id = 1", 0)

	// === DEFAULT VALUES (persistence test) ===
	afExec(t, db, ctx, "CREATE TABLE defaults_persist (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'pending', count INTEGER DEFAULT 0)")
	afExec(t, db, ctx, "INSERT INTO defaults_persist (id) VALUES (1)")
	check("DEFAULT text persisted", "SELECT status FROM defaults_persist WHERE id = 1", "pending")
	check("DEFAULT int persisted", "SELECT count FROM defaults_persist WHERE id = 1", 0)

	// === DEFAULT with expressions ===
	afExec(t, db, ctx, "CREATE TABLE defaults_expr (id INTEGER PRIMARY KEY, val INTEGER DEFAULT 42, neg INTEGER DEFAULT -1)")
	afExec(t, db, ctx, "INSERT INTO defaults_expr (id) VALUES (1)")
	check("DEFAULT expr int", "SELECT val FROM defaults_expr WHERE id = 1", 42)
	check("DEFAULT expr neg", "SELECT neg FROM defaults_expr WHERE id = 1", -1)

	// === AGGREGATE TYPE HANDLING ===
	check("COUNT returns int", "SELECT COUNT(*) FROM types_test", 3)
	check("SUM of integers", "SELECT SUM(int_val) FROM types_test", 135)
	check("MIN of integers", "SELECT MIN(int_val) FROM types_test", -7)
	check("MAX of integers", "SELECT MAX(int_val) FROM types_test", 100)
	check("AVG of integers", "SELECT AVG(int_val) FROM types_test", 45)

	// === UPDATE preserves types ===
	afExec(t, db, ctx, "CREATE TABLE update_types (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO update_types VALUES (1, 10, 20)")
	afExec(t, db, ctx, "UPDATE update_types SET a = a + b WHERE id = 1")
	check("UPDATE arithmetic preserves int", "SELECT a FROM update_types WHERE id = 1", 30)

	// === Negative number literal ===
	check("Negative literal", "SELECT -5 FROM types_test WHERE id = 1", -5)
	check("Negative multiply", "SELECT -5 * 3 FROM types_test WHERE id = 1", -15)

	// === Mixed int/float arithmetic ===
	check("Int + Float", "SELECT 10 + 0.5 FROM types_test WHERE id = 1", 10.5)
	check("Int * Float", "SELECT 3 * 1.5 FROM types_test WHERE id = 1", 4.5)

	// === CHECK constraint with DEFAULT ===
	afExec(t, db, ctx, "CREATE TABLE check_default (id INTEGER PRIMARY KEY, age INTEGER DEFAULT 18 CHECK (age >= 0))")
	afExec(t, db, ctx, "INSERT INTO check_default (id) VALUES (1)")
	check("DEFAULT passes CHECK", "SELECT age FROM check_default WHERE id = 1", 18)

	t.Logf("\n=== V10 TYPE HANDLING: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
