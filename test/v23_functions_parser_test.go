package test

import (
	"fmt"
	"testing"
)

func TestV23FunctionsAndParser(t *testing.T) {
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

	_ = checkNoError

	// ============================================================
	// === STRING FUNCTIONS ===
	// ============================================================
	check("UPPER basic", "SELECT UPPER('hello world')", "HELLO WORLD")
	check("LOWER basic", "SELECT LOWER('HELLO WORLD')", "hello world")
	check("LENGTH basic", "SELECT LENGTH('hello')", 5)
	check("LENGTH empty", "SELECT LENGTH('')", 0)
	check("SUBSTR from start", "SELECT SUBSTR('abcdef', 1, 3)", "abc")
	check("SUBSTR middle", "SELECT SUBSTR('abcdef', 3, 2)", "cd")
	check("SUBSTR to end", "SELECT SUBSTR('abcdef', 4)", "def")
	check("REPLACE basic", "SELECT REPLACE('hello world', 'world', 'go')", "hello go")
	check("REPLACE no match", "SELECT REPLACE('hello', 'xyz', 'abc')", "hello")
	check("TRIM spaces", "SELECT TRIM('  hello  ')", "hello")
	check("INSTR found", "SELECT INSTR('hello world', 'world')", 7)
	check("INSTR not found", "SELECT INSTR('hello', 'xyz')", 0)

	// ============================================================
	// === NESTED FUNCTION CALLS ===
	// ============================================================
	check("Nested UPPER(SUBSTR)", "SELECT UPPER(SUBSTR('hello world', 1, 5))", "HELLO")
	check("Nested LENGTH(UPPER)", "SELECT LENGTH(UPPER('hello'))", 5)
	check("Nested REPLACE(LOWER)", "SELECT REPLACE(LOWER('Hello World'), 'world', 'go')", "hello go")

	// ============================================================
	// === MATH FUNCTIONS ===
	// ============================================================
	check("ABS positive", "SELECT ABS(42)", 42)
	check("ABS negative", "SELECT ABS(-42)", 42)
	check("ABS zero", "SELECT ABS(0)", 0)
	check("ROUND to 0 places", "SELECT ROUND(3.567, 0)", 4)
	check("ROUND to 1 place", "SELECT ROUND(3.567, 1)", 3.6)
	check("ROUND to 2 places", "SELECT ROUND(3.567, 2)", 3.57)

	// ============================================================
	// === COALESCE WITH MULTIPLE ARGS ===
	// ============================================================
	check("COALESCE 3 args all null first",
		"SELECT COALESCE(NULL, NULL, 42)", 42)
	check("COALESCE first non-null",
		"SELECT COALESCE(1, 2, 3)", 1)

	// ============================================================
	// === EXPRESSION ARITHMETIC ===
	// ============================================================
	check("Complex arithmetic", "SELECT (10 + 20) * 3 - 5", 85)
	check("Negative arithmetic", "SELECT -10 + 5", -5)
	check("Modulo", "SELECT 17 % 5", 2)
	check("String concat", "SELECT 'hello' || ' ' || 'world'", "hello world")

	// ============================================================
	// === CASE WITH NULL ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE case_null (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO case_null VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO case_null VALUES (2, 10)")

	check("CASE with NULL IS NULL",
		"SELECT CASE WHEN val IS NULL THEN 'null' ELSE 'not null' END FROM case_null WHERE id = 1", "null")
	check("CASE with non-NULL",
		"SELECT CASE WHEN val IS NULL THEN 'null' ELSE 'not null' END FROM case_null WHERE id = 2", "not null")

	// ============================================================
	// === BETWEEN WITH STRINGS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE str_between (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO str_between VALUES (1, 'apple')")
	afExec(t, db, ctx, "INSERT INTO str_between VALUES (2, 'banana')")
	afExec(t, db, ctx, "INSERT INTO str_between VALUES (3, 'cherry')")
	afExec(t, db, ctx, "INSERT INTO str_between VALUES (4, 'date')")

	checkRowCount("BETWEEN strings", "SELECT * FROM str_between WHERE name BETWEEN 'b' AND 'd'", 2) // banana, cherry

	// ============================================================
	// === IN WITH MIXED TYPES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE in_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO in_test VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO in_test VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO in_test VALUES (3, 30)")

	checkRowCount("IN with integers", "SELECT * FROM in_test WHERE val IN (10, 30)", 2)
	checkRowCount("NOT IN", "SELECT * FROM in_test WHERE val NOT IN (10, 30)", 1)

	// ============================================================
	// === LIKE PATTERNS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE like_patterns (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO like_patterns VALUES (1, 'abc')")
	afExec(t, db, ctx, "INSERT INTO like_patterns VALUES (2, 'ABC')")
	afExec(t, db, ctx, "INSERT INTO like_patterns VALUES (3, 'a%c')")
	afExec(t, db, ctx, "INSERT INTO like_patterns VALUES (4, 'xyz')")

	checkRowCount("LIKE percent", "SELECT * FROM like_patterns WHERE val LIKE 'a%'", 3) // abc, ABC (case-insensitive), a%c
	checkRowCount("LIKE underscore", "SELECT * FROM like_patterns WHERE val LIKE '_bc'", 2) // abc, ABC (case-insensitive)

	// ============================================================
	// === COLUMN ALIASES IN EXPRESSIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE alias_test (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)")
	afExec(t, db, ctx, "INSERT INTO alias_test VALUES (1, 'John', 'Doe')")
	afExec(t, db, ctx, "INSERT INTO alias_test VALUES (2, 'Jane', 'Smith')")

	check("Alias in SELECT", "SELECT first_name AS fname FROM alias_test WHERE id = 1", "John")

	// ============================================================
	// === EXPRESSIONS IN INSERT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE expr_insert (id INTEGER PRIMARY KEY, val INTEGER)")
	checkNoError("INSERT with expression", "INSERT INTO expr_insert VALUES (1, 10 + 20)")
	check("Expression in INSERT", "SELECT val FROM expr_insert WHERE id = 1", 30)

	// ============================================================
	// === EXPRESSIONS IN UPDATE ===
	// ============================================================
	checkNoError("UPDATE with expression", "UPDATE expr_insert SET val = val * 2 + 5 WHERE id = 1")
	check("Expression in UPDATE", "SELECT val FROM expr_insert WHERE id = 1", 65) // 30*2+5

	// ============================================================
	// === AGGREGATE FUNCTIONS ON EXPRESSIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE agg_expr (id INTEGER PRIMARY KEY, price INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO agg_expr VALUES (1, 10, 5)")
	afExec(t, db, ctx, "INSERT INTO agg_expr VALUES (2, 20, 3)")
	afExec(t, db, ctx, "INSERT INTO agg_expr VALUES (3, 15, 4)")

	check("SUM of expression", "SELECT SUM(price * qty) FROM agg_expr", 170) // 50 + 60 + 60

	// ============================================================
	// === WHERE WITH FUNCTION ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE func_where (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO func_where VALUES (1, 'Hello')")
	afExec(t, db, ctx, "INSERT INTO func_where VALUES (2, 'World')")

	checkRowCount("WHERE with UPPER", "SELECT * FROM func_where WHERE UPPER(name) = 'HELLO'", 1)
	checkRowCount("WHERE with LENGTH", "SELECT * FROM func_where WHERE LENGTH(name) = 5", 2)

	// ============================================================
	// === COMPLEX MULTI-JOIN QUERY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE mj_users (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE mj_orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE mj_items (id INTEGER PRIMARY KEY, order_id INTEGER, product TEXT)")

	afExec(t, db, ctx, "INSERT INTO mj_users VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO mj_users VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO mj_orders VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO mj_orders VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO mj_orders VALUES (3, 2, 150)")
	afExec(t, db, ctx, "INSERT INTO mj_items VALUES (1, 1, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO mj_items VALUES (2, 1, 'Gadget')")
	afExec(t, db, ctx, "INSERT INTO mj_items VALUES (3, 2, 'Phone')")
	afExec(t, db, ctx, "INSERT INTO mj_items VALUES (4, 3, 'Book')")

	check("3-table JOIN query",
		"SELECT mj_users.name FROM mj_users JOIN mj_orders ON mj_users.id = mj_orders.user_id JOIN mj_items ON mj_orders.id = mj_items.order_id WHERE mj_items.product = 'Phone'",
		"Alice")

	check("3-table JOIN with COUNT",
		"SELECT mj_users.name, COUNT(mj_items.id) FROM mj_users JOIN mj_orders ON mj_users.id = mj_orders.user_id JOIN mj_items ON mj_orders.id = mj_items.order_id GROUP BY mj_users.name ORDER BY COUNT(mj_items.id) DESC LIMIT 1",
		"Alice") // Alice: 3 items, Bob: 1 item

	// ============================================================
	// === HAVING WITH COMPLEX EXPRESSION ===
	// ============================================================
	check("HAVING with arithmetic",
		"SELECT mj_users.name FROM mj_users JOIN mj_orders ON mj_users.id = mj_orders.user_id GROUP BY mj_users.name HAVING SUM(mj_orders.total) > 200",
		"Alice") // Alice: 100+200=300 > 200

	// ============================================================
	// === FUNCTION IN ORDER BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE fn_order (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO fn_order VALUES (1, 'Charlie')")
	afExec(t, db, ctx, "INSERT INTO fn_order VALUES (2, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO fn_order VALUES (3, 'Bob')")

	check("ORDER BY LOWER",
		"SELECT name FROM fn_order ORDER BY LOWER(name) LIMIT 1", "Alice")

	// ============================================================
	// === DISTINCT WITH ORDER BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE dist_order (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dist_order VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO dist_order VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO dist_order VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO dist_order VALUES (4, 'C', 40)")

	checkRowCount("DISTINCT", "SELECT DISTINCT cat FROM dist_order", 3)

	// ============================================================
	// === COMPARISON OPERATORS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE cmp_ops (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cmp_ops VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO cmp_ops VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO cmp_ops VALUES (3, 30)")

	checkRowCount("Greater than", "SELECT * FROM cmp_ops WHERE val > 15", 2)
	checkRowCount("Greater or equal", "SELECT * FROM cmp_ops WHERE val >= 20", 2)
	checkRowCount("Less than", "SELECT * FROM cmp_ops WHERE val < 20", 1)
	checkRowCount("Less or equal", "SELECT * FROM cmp_ops WHERE val <= 20", 2)
	checkRowCount("Not equal !=", "SELECT * FROM cmp_ops WHERE val != 20", 2)
	checkRowCount("Equal", "SELECT * FROM cmp_ops WHERE val = 20", 1)

	t.Logf("\n=== V23 FUNCTIONS & PARSER: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
