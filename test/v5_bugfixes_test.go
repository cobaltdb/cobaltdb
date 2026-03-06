package test

import (
	"fmt"
	"testing"
)

func TestV5UpdatePrimaryKey(t *testing.T) {
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

	checkRows := func(desc string, sql string, expectedCount int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expectedCount {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expectedCount)
			return
		}
		pass++
	}

	checkFail := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: should have failed", desc)
		}
	}

	// === UPDATE PRIMARY KEY ===
	afExec(t, db, ctx, "CREATE TABLE pk_test (id INTEGER PRIMARY KEY, name TEXT, val REAL)")
	afExec(t, db, ctx, "INSERT INTO pk_test VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO pk_test VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO pk_test VALUES (3, 'Carol', 300)")

	// Update primary key
	afExec(t, db, ctx, "UPDATE pk_test SET id = 10 WHERE id = 1")
	check("PK update: find by new key", "SELECT name FROM pk_test WHERE id = 10", "Alice")
	checkRows("PK update: old key gone", "SELECT * FROM pk_test WHERE id = 1", 0)
	checkRows("PK update: total rows unchanged", "SELECT * FROM pk_test", 3)

	// Update PK and other columns simultaneously
	afExec(t, db, ctx, "UPDATE pk_test SET id = 20, val = 999 WHERE id = 2")
	check("PK+val update: new key", "SELECT val FROM pk_test WHERE id = 20", 999)
	checkRows("PK+val update: old key gone", "SELECT * FROM pk_test WHERE id = 2", 0)

	// Update PK with text primary key
	afExec(t, db, ctx, "CREATE TABLE pk_text (code TEXT PRIMARY KEY, label TEXT)")
	afExec(t, db, ctx, "INSERT INTO pk_text VALUES ('A1', 'first')")
	afExec(t, db, ctx, "UPDATE pk_text SET code = 'B2' WHERE code = 'A1'")
	check("Text PK update", "SELECT label FROM pk_text WHERE code = 'B2'", "first")
	checkRows("Text PK old key gone", "SELECT * FROM pk_text WHERE code = 'A1'", 0)

	// === INSERT COLUMN VALIDATION ===
	checkFail("Invalid column name in INSERT",
		"INSERT INTO pk_test (id, nonexistent) VALUES (99, 'test')")

	// === CASE NULL behavior ===
	afExec(t, db, ctx, "CREATE TABLE null_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO null_test VALUES (1, NULL)")
	check("CASE NULL WHEN NULL",
		"SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END FROM null_test WHERE id = 1",
		"no match") // SQL standard: NULL = NULL is UNKNOWN

	check("CASE val WHEN NULL with non-null",
		"SELECT CASE 'hello' WHEN NULL THEN 'match' WHEN 'hello' THEN 'found' ELSE 'miss' END FROM null_test WHERE id = 1",
		"found")

	// === VARCHAR(255) type parameters ===
	afExec(t, db, ctx, "CREATE TABLE type_test (id INTEGER PRIMARY KEY, name VARCHAR(255), price REAL)")
	afExec(t, db, ctx, "INSERT INTO type_test VALUES (1, 'test', 9.99)")
	check("VARCHAR(255) table works", "SELECT name FROM type_test WHERE id = 1", "test")

	// === ALTER TABLE DROP COLUMN ===
	afExec(t, db, ctx, "CREATE TABLE alter_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)")
	afExec(t, db, ctx, "INSERT INTO alter_test VALUES (1, 'Alice', 30, 'alice@test.com')")
	afExec(t, db, ctx, "INSERT INTO alter_test VALUES (2, 'Bob', 25, 'bob@test.com')")

	afExec(t, db, ctx, "ALTER TABLE alter_test DROP COLUMN age")
	check("After DROP COLUMN: name still works", "SELECT name FROM alter_test WHERE id = 1", "Alice")
	check("After DROP COLUMN: email still works", "SELECT email FROM alter_test WHERE id = 1", "alice@test.com")
	checkFail("After DROP COLUMN: dropped col fails", "SELECT age FROM alter_test WHERE id = 1")

	// Cannot drop PRIMARY KEY
	checkFail("Cannot drop PK column", "ALTER TABLE alter_test DROP COLUMN id")

	// === ALTER TABLE RENAME TO ===
	afExec(t, db, ctx, "CREATE TABLE old_name (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO old_name VALUES (1, 'hello')")
	afExec(t, db, ctx, "ALTER TABLE old_name RENAME TO new_name")
	check("Renamed table works", "SELECT val FROM new_name WHERE id = 1", "hello")
	checkFail("Old table name fails", "SELECT * FROM old_name")

	// === ALTER TABLE RENAME COLUMN ===
	afExec(t, db, ctx, "CREATE TABLE rename_col_test (id INTEGER PRIMARY KEY, old_col TEXT)")
	afExec(t, db, ctx, "INSERT INTO rename_col_test VALUES (1, 'data')")
	afExec(t, db, ctx, "ALTER TABLE rename_col_test RENAME COLUMN old_col TO new_col")
	check("Renamed column works", "SELECT new_col FROM rename_col_test WHERE id = 1", "data")

	// === Multi-expression ORDER BY ===
	afExec(t, db, ctx, "CREATE TABLE order_test (id INTEGER PRIMARY KEY, price REAL, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO order_test VALUES (1, 10, 3)")  // price*qty=30
	afExec(t, db, ctx, "INSERT INTO order_test VALUES (2, 20, 2)")  // price*qty=40
	afExec(t, db, ctx, "INSERT INTO order_test VALUES (3, 5, 10)")  // price*qty=50

	rows := afQuery(t, db, ctx, "SELECT id FROM order_test ORDER BY price * qty DESC")
	total++
	if len(rows) == 3 && fmt.Sprintf("%v", rows[0][0]) == "3" {
		pass++ // 3 has price*qty=50, highest
	} else {
		t.Errorf("[FAIL] ORDER BY expression: expected id=3 first, got %v", rows)
	}

	t.Logf("\n=== V5 BUGFIXES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
