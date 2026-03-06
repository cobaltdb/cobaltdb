package test

import (
	"fmt"
	"testing"
)

func TestV11FunctionsAndTransactions(t *testing.T) {
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

	// Setup
	afExec(t, db, ctx, "CREATE TABLE func_data (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO func_data VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO func_data VALUES (2, 'Bob', NULL)")
	afExec(t, db, ctx, "INSERT INTO func_data VALUES (3, 'Charlie', 200)")

	// === SUBSTR edge cases ===
	check("SUBSTR basic", "SELECT SUBSTR('hello', 2, 3) FROM func_data WHERE id = 1", "ell")
	check("SUBSTR no length", "SELECT SUBSTR('hello', 2) FROM func_data WHERE id = 1", "ello")
	check("SUBSTR negative length returns empty", "SELECT SUBSTR('hello', 2, -1) FROM func_data WHERE id = 1", "")
	check("SUBSTR start beyond string", "SELECT SUBSTR('hello', 10) FROM func_data WHERE id = 1", "")
	check("SUBSTR length > remaining", "SELECT SUBSTR('hello', 4, 100) FROM func_data WHERE id = 1", "lo")
	checkNull("SUBSTR with NULL string", "SELECT SUBSTR(NULL, 1, 3) FROM func_data WHERE id = 1")

	// === REPLACE with different types ===
	check("REPLACE basic", "SELECT REPLACE('hello world', 'world', 'earth') FROM func_data WHERE id = 1", "hello earth")
	check("REPLACE no match", "SELECT REPLACE('hello', 'xyz', 'abc') FROM func_data WHERE id = 1", "hello")
	checkNull("REPLACE with NULL", "SELECT REPLACE(NULL, 'a', 'b') FROM func_data WHERE id = 1")

	// === COALESCE ===
	check("COALESCE first non-null", "SELECT COALESCE(NULL, NULL, 'found') FROM func_data WHERE id = 1", "found")
	check("COALESCE first arg", "SELECT COALESCE('first', 'second') FROM func_data WHERE id = 1", "first")
	checkNull("COALESCE all null", "SELECT COALESCE(NULL, NULL) FROM func_data WHERE id = 1")
	check("COALESCE with column", "SELECT COALESCE(val, 0) FROM func_data WHERE id = 2", 0)

	// === NULLIF ===
	checkNull("NULLIF equal", "SELECT NULLIF(1, 1) FROM func_data WHERE id = 1")
	check("NULLIF not equal", "SELECT NULLIF(1, 2) FROM func_data WHERE id = 1", 1)

	// === UPPER/LOWER with NULL ===
	checkNull("UPPER NULL", "SELECT UPPER(NULL) FROM func_data WHERE id = 1")
	checkNull("LOWER NULL", "SELECT LOWER(NULL) FROM func_data WHERE id = 1")
	check("UPPER basic", "SELECT UPPER('hello') FROM func_data WHERE id = 1", "HELLO")
	check("LOWER basic", "SELECT LOWER('HELLO') FROM func_data WHERE id = 1", "hello")

	// === LENGTH with NULL ===
	checkNull("LENGTH NULL", "SELECT LENGTH(NULL) FROM func_data WHERE id = 1")
	check("LENGTH basic", "SELECT LENGTH('hello') FROM func_data WHERE id = 1", 5)

	// === ABS ===
	check("ABS negative", "SELECT ABS(-42) FROM func_data WHERE id = 1", 42)
	check("ABS positive", "SELECT ABS(42) FROM func_data WHERE id = 1", 42)

	// === INSERT...SELECT ===
	afExec(t, db, ctx, "CREATE TABLE func_copy (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO func_copy SELECT * FROM func_data")
	checkRowCount("INSERT...SELECT copies rows", "SELECT * FROM func_copy", 3)
	check("INSERT...SELECT preserves data", "SELECT name FROM func_copy WHERE id = 1", "Alice")
	check("INSERT...SELECT preserves int", "SELECT val FROM func_copy WHERE id = 1", 100)

	// === CREATE INDEX inside transaction with ROLLBACK ===
	afExec(t, db, ctx, "CREATE TABLE idx_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO idx_test VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO idx_test VALUES (2, 'b')")

	afExec(t, db, ctx, "BEGIN")
	checkNoError("CREATE INDEX in txn", "CREATE INDEX idx_val ON idx_test (val)")
	// Index exists during transaction
	checkRowCount("Query after index creation", "SELECT * FROM idx_test WHERE val = 'a'", 1)
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, index should be gone - but data should still be queryable
	checkRowCount("Data after rollback", "SELECT * FROM idx_test", 2)

	// === DROP INDEX inside transaction with ROLLBACK ===
	afExec(t, db, ctx, "CREATE INDEX idx_val2 ON idx_test (val)")
	afExec(t, db, ctx, "BEGIN")
	checkNoError("DROP INDEX in txn", "DROP INDEX idx_val2")
	afExec(t, db, ctx, "ROLLBACK")
	// After rollback, index should be restored
	checkRowCount("Data after drop rollback", "SELECT * FROM idx_test", 2)

	// === Transaction with multiple operations ===
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO func_data VALUES (4, 'Dave', 300)")
	afExec(t, db, ctx, "UPDATE func_data SET val = 999 WHERE id = 1")
	checkRowCount("Rows during txn", "SELECT * FROM func_data", 4)
	check("Updated val during txn", "SELECT val FROM func_data WHERE id = 1", 999)
	afExec(t, db, ctx, "ROLLBACK")
	checkRowCount("Rows after rollback", "SELECT * FROM func_data", 3)
	check("Val restored after rollback", "SELECT val FROM func_data WHERE id = 1", 100)

	// === CASE WHEN edge cases ===
	check("CASE simple", "SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END FROM func_data WHERE id = 1", "yes")
	check("CASE else", "SELECT CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END FROM func_data WHERE id = 1", "no")
	check("CASE with column", "SELECT CASE WHEN val > 150 THEN 'high' ELSE 'low' END FROM func_data WHERE id = 1", "low")
	check("CASE with NULL column", "SELECT CASE WHEN val IS NULL THEN 'null' ELSE 'not null' END FROM func_data WHERE id = 2", "null")

	// === IIF function ===
	check("IIF true", "SELECT IIF(1 = 1, 'yes', 'no') FROM func_data WHERE id = 1", "yes")
	check("IIF false", "SELECT IIF(1 = 2, 'yes', 'no') FROM func_data WHERE id = 1", "no")

	t.Logf("\n=== V11 FUNCTIONS/TXN: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
