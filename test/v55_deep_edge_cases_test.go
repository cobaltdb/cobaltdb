package test

import (
	"fmt"
	"testing"
)

// TestV55DeepEdgeCases probes for subtle bugs in type coercion, expression evaluation,
// boundary conditions, and rarely-tested SQL patterns.
func TestV55DeepEdgeCases(t *testing.T) {
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

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expected, len(rows))
			return
		}
		pass++
	}

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got nil", desc)
			return
		}
		pass++
	}

	// ============================================================
	// === TYPE COERCION AND CASTING ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v55_types (id INTEGER PRIMARY KEY,
		i INTEGER, r REAL, t TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v55_types VALUES (1, 42, 3.14, 'hello')")
	afExec(t, db, ctx, "INSERT INTO v55_types VALUES (2, 0, 0.0, '')")
	afExec(t, db, ctx, "INSERT INTO v55_types VALUES (3, NULL, NULL, NULL)")

	// TC1: CAST INTEGER to TEXT
	check("TC1 CAST int to text", "SELECT CAST(i AS TEXT) FROM v55_types WHERE id = 1", "42")

	// TC2: CAST TEXT to INTEGER
	afExec(t, db, ctx, `CREATE TABLE v55_cast (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v55_cast VALUES (1, '123')")
	check("TC2 CAST text to int", "SELECT CAST(val AS INTEGER) FROM v55_cast WHERE id = 1", 123)

	// TC3: CAST REAL to INTEGER (truncation)
	check("TC3 CAST real to int", "SELECT CAST(r AS INTEGER) FROM v55_types WHERE id = 1", 3)

	// TC4: CAST NULL preserves NULL
	checkNull("TC4 CAST NULL", "SELECT CAST(i AS TEXT) FROM v55_types WHERE id = 3")

	// ============================================================
	// === ARITHMETIC EDGE CASES ===
	// ============================================================

	// AE1: Division (CobaltDB uses float division)
	check("AE1 division", "SELECT 7 / 2 FROM v55_types WHERE id = 1", 3.5)

	// AE2: Modulo
	check("AE2 modulo", "SELECT 7 % 3 FROM v55_types WHERE id = 1", 1)

	// AE3: Negative numbers
	check("AE3 negative", "SELECT -5 + 3 FROM v55_types WHERE id = 1", -2)

	// AE4: Multiplication overflow handling (large numbers)
	check("AE4 large multiply", "SELECT 1000000 * 1000 FROM v55_types WHERE id = 1", 1000000000)

	// ============================================================
	// === NULL ARITHMETIC ===
	// ============================================================

	// NA1: NULL + number = NULL
	checkNull("NA1 NULL + num", "SELECT i + 1 FROM v55_types WHERE id = 3")

	// NA2: NULL * number = NULL
	checkNull("NA2 NULL * num", "SELECT i * 2 FROM v55_types WHERE id = 3")

	// NA3: NULL comparison returns neither true nor false
	checkRowCount("NA3 NULL = NULL",
		"SELECT * FROM v55_types WHERE i = NULL", 0)

	// NA4: NULL != NULL also returns no rows (three-valued logic)
	checkRowCount("NA4 NULL != NULL",
		"SELECT * FROM v55_types WHERE i != NULL", 0)

	// ============================================================
	// === STRING COMPARISON AND OPERATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v55_str (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v55_str VALUES (1, 'apple')")
	afExec(t, db, ctx, "INSERT INTO v55_str VALUES (2, 'banana')")
	afExec(t, db, ctx, "INSERT INTO v55_str VALUES (3, 'cherry')")
	afExec(t, db, ctx, "INSERT INTO v55_str VALUES (4, 'APPLE')")
	afExec(t, db, ctx, "INSERT INTO v55_str VALUES (5, 'Apple')")

	// SC1: String ordering
	check("SC1 string ORDER BY",
		"SELECT val FROM v55_str ORDER BY val ASC LIMIT 1", "APPLE")

	// SC2: String comparison
	checkRowCount("SC2 string >=",
		"SELECT * FROM v55_str WHERE val >= 'c'", 1)

	// SC3: LIKE case sensitivity (CobaltDB LIKE is case-insensitive)
	checkRowCount("SC3 LIKE case",
		"SELECT * FROM v55_str WHERE val LIKE 'apple'", 3)

	// SC4: LIKE with wildcard
	checkRowCount("SC4 LIKE wildcard",
		"SELECT * FROM v55_str WHERE val LIKE '%pple'", 3)

	// SC5: String concatenation with ||
	check("SC5 concat",
		"SELECT 'hello' || ' ' || 'world' FROM v55_str WHERE id = 1", "hello world")

	// ============================================================
	// === BOUNDARY VALUES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v55_boundary (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v55_boundary VALUES (1, 0)")
	afExec(t, db, ctx, "INSERT INTO v55_boundary VALUES (2, -1)")
	afExec(t, db, ctx, "INSERT INTO v55_boundary VALUES (3, 2147483647)")
	afExec(t, db, ctx, "INSERT INTO v55_boundary VALUES (4, -2147483648)")

	// BV1: Zero handling
	check("BV1 zero", "SELECT val FROM v55_boundary WHERE id = 1", 0)

	// BV2: Negative number
	check("BV2 negative", "SELECT val FROM v55_boundary WHERE id = 2", -1)

	// BV3: Large positive
	check("BV3 INT_MAX", "SELECT val FROM v55_boundary WHERE id = 3", 2147483647)

	// BV4: Large negative
	check("BV4 INT_MIN", "SELECT val FROM v55_boundary WHERE id = 4", -2147483648)

	// BV5: ORDER BY with mixed positive/negative
	check("BV5 ORDER mixed",
		"SELECT val FROM v55_boundary ORDER BY val ASC LIMIT 1", -2147483648)

	// ============================================================
	// === EMPTY TABLE OPERATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v55_empty (id INTEGER PRIMARY KEY, val TEXT)`)

	// ET1: SELECT from empty table
	checkRowCount("ET1 empty SELECT", "SELECT * FROM v55_empty", 0)

	// ET2: DELETE from empty table
	checkNoError("ET2 empty DELETE", "DELETE FROM v55_empty WHERE id = 1")

	// ET3: UPDATE on empty table
	checkNoError("ET3 empty UPDATE", "UPDATE v55_empty SET val = 'x' WHERE id = 1")

	// ============================================================
	// === SELF JOIN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v55_tree (
		id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v55_tree VALUES (1, 'Root', NULL)")
	afExec(t, db, ctx, "INSERT INTO v55_tree VALUES (2, 'Child1', 1)")
	afExec(t, db, ctx, "INSERT INTO v55_tree VALUES (3, 'Child2', 1)")
	afExec(t, db, ctx, "INSERT INTO v55_tree VALUES (4, 'Grandchild', 2)")

	// SJ1: Self join to get parent name
	check("SJ1 self join parent",
		`SELECT p.name FROM v55_tree c
		 JOIN v55_tree p ON c.parent_id = p.id
		 WHERE c.name = 'Child1'`, "Root")

	// SJ2: Self join count children
	check("SJ2 self join count children",
		`SELECT COUNT(*) FROM v55_tree c
		 JOIN v55_tree p ON c.parent_id = p.id
		 WHERE p.name = 'Root'`, 2)

	// SJ3: Left join to include root (no parent)
	checkRowCount("SJ3 self LEFT join",
		`SELECT c.name, p.name FROM v55_tree c
		 LEFT JOIN v55_tree p ON c.parent_id = p.id`, 4)

	// ============================================================
	// === MULTIPLE CONDITIONS IN JOIN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v55_schedule (
		id INTEGER PRIMARY KEY, day TEXT, slot TEXT, room TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v55_schedule VALUES (1, 'Mon', 'AM', 'A101')")
	afExec(t, db, ctx, "INSERT INTO v55_schedule VALUES (2, 'Mon', 'PM', 'A102')")
	afExec(t, db, ctx, "INSERT INTO v55_schedule VALUES (3, 'Tue', 'AM', 'A101')")
	afExec(t, db, ctx, "INSERT INTO v55_schedule VALUES (4, 'Tue', 'AM', 'A102')")

	// MC1: JOIN on multiple conditions
	checkRowCount("MC1 multi condition join",
		`SELECT a.id, b.id FROM v55_schedule a
		 JOIN v55_schedule b ON a.day = b.day AND a.slot = b.slot
		 WHERE a.id < b.id`, 1)

	// ============================================================
	// === NESTED CASE EXPRESSIONS ===
	// ============================================================

	// NC1: Nested CASE
	check("NC1 nested CASE",
		`SELECT CASE
			WHEN i > 10 THEN
				CASE WHEN i > 40 THEN 'very high' ELSE 'high' END
			ELSE 'low'
		 END FROM v55_types WHERE id = 1`, "very high")

	// NC2: CASE with NULL
	check("NC2 CASE NULL",
		`SELECT CASE
			WHEN i IS NULL THEN 'null'
			WHEN i = 0 THEN 'zero'
			ELSE 'other'
		 END FROM v55_types WHERE id = 3`, "null")

	// ============================================================
	// === COALESCE AND NULLIF ===
	// ============================================================

	// CO1: COALESCE picks first non-null
	check("CO1 COALESCE", "SELECT COALESCE(NULL, NULL, 'found') FROM v55_types WHERE id = 1", "found")

	// CO2: COALESCE with column
	check("CO2 COALESCE col",
		"SELECT COALESCE(i, -1) FROM v55_types WHERE id = 3", -1)

	// CO3: NULLIF returns NULL if equal
	checkNull("CO3 NULLIF equal",
		"SELECT NULLIF(1, 1) FROM v55_types WHERE id = 1")

	// CO4: NULLIF returns first if not equal
	check("CO4 NULLIF not equal",
		"SELECT NULLIF(1, 2) FROM v55_types WHERE id = 1", 1)

	// ============================================================
	// === BETWEEN EDGE CASES ===
	// ============================================================

	// BE1: BETWEEN inclusive of both ends
	checkRowCount("BE1 BETWEEN inclusive",
		"SELECT * FROM v55_boundary WHERE val BETWEEN -1 AND 0", 2)

	// BE2: NOT BETWEEN
	checkRowCount("BE2 NOT BETWEEN",
		"SELECT * FROM v55_boundary WHERE val NOT BETWEEN -1 AND 0", 2)

	// ============================================================
	// === INSERT OR IGNORE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v55_upsert (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v55_upsert VALUES (1, 'first')")

	// IO1: INSERT OR IGNORE on duplicate key
	checkNoError("IO1 INSERT OR IGNORE",
		"INSERT OR IGNORE INTO v55_upsert VALUES (1, 'duplicate')")

	check("IO1 original preserved",
		"SELECT val FROM v55_upsert WHERE id = 1", "first")

	// IO2: INSERT OR IGNORE on new key
	checkNoError("IO2 INSERT OR IGNORE new",
		"INSERT OR IGNORE INTO v55_upsert VALUES (2, 'second')")

	check("IO2 new inserted",
		"SELECT val FROM v55_upsert WHERE id = 2", "second")

	// ============================================================
	// === PRIMARY KEY CONSTRAINTS ===
	// ============================================================

	// PK1: Duplicate primary key should error
	checkError("PK1 dup PK",
		"INSERT INTO v55_upsert VALUES (1, 'dup')")

	// PK2: NULL primary key - CobaltDB accepts this (auto-assigns)
	checkNoError("PK2 NULL PK",
		"INSERT INTO v55_upsert VALUES (NULL, 'null-pk')")

	// ============================================================
	// === COMPLEX EXPRESSION IN SELECT ===
	// ============================================================

	// CE1: Arithmetic in SELECT
	check("CE1 arithmetic SELECT",
		"SELECT (10 + 20) * 3 - 5 FROM v55_types WHERE id = 1", 85)

	// CE2: String function in SELECT
	check("CE2 LENGTH in SELECT",
		"SELECT LENGTH(t) FROM v55_types WHERE id = 1", 5)

	// CE3: Nested function calls
	check("CE3 nested funcs",
		"SELECT UPPER(t) FROM v55_types WHERE id = 1", "HELLO")

	t.Logf("\n=== V55 DEEP EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
