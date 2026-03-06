package test

import (
	"fmt"
	"strings"
	"testing"
)

// TestV59TypeArithmetic tests type system, CAST, arithmetic edge cases,
// string operations, NULL propagation, comparison operators, and expression evaluation.
func TestV59TypeArithmetic(t *testing.T) {
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

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
		}
	}

	checkContains := func(desc string, sql string, substring string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		if !strings.Contains(got, substring) {
			t.Errorf("[FAIL] %s: got %s, expected to contain %s", desc, got, substring)
			return
		}
		pass++
	}
	_ = checkError
	_ = checkContains

	// Setup a helper table
	afExec(t, db, ctx, `CREATE TABLE v59_t (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v59_t VALUES (1, 42)")

	// ============================================================
	// === BASIC ARITHMETIC ===
	// ============================================================

	// AR1: Addition
	check("AR1 addition", "SELECT 2 + 3 FROM v59_t WHERE id = 1", 5)

	// AR2: Subtraction
	check("AR2 subtraction", "SELECT 10 - 4 FROM v59_t WHERE id = 1", 6)

	// AR3: Multiplication
	check("AR3 multiplication", "SELECT 6 * 7 FROM v59_t WHERE id = 1", 42)

	// AR4: Division (CobaltDB returns float)
	check("AR4 division", "SELECT 10 / 3 FROM v59_t WHERE id = 1", "3.3333333333333335")

	// AR5: Modulo (if supported)
	// check("AR5 modulo", "SELECT 10 % 3 FROM v59_t WHERE id = 1", 1)

	// AR6: Negative numbers
	check("AR6 negative", "SELECT -5 + 3 FROM v59_t WHERE id = 1", -2)

	// AR7: Order of operations
	check("AR7 order ops", "SELECT 2 + 3 * 4 FROM v59_t WHERE id = 1", 14)

	// AR8: Parentheses override precedence
	check("AR8 parens", "SELECT (2 + 3) * 4 FROM v59_t WHERE id = 1", 20)

	// ============================================================
	// === CAST OPERATIONS ===
	// ============================================================

	// CA1: CAST integer to text
	check("CA1 int to text", "SELECT CAST(42 AS TEXT) FROM v59_t WHERE id = 1", "42")

	// CA2: CAST text to integer
	check("CA2 text to int", "SELECT CAST('123' AS INTEGER) + 1 FROM v59_t WHERE id = 1", 124)

	// CA3: CAST in expression
	check("CA3 CAST expr", "SELECT CAST(val AS TEXT) FROM v59_t WHERE id = 1", "42")

	// ============================================================
	// === NULL ARITHMETIC PROPAGATION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v59_nulls (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v59_nulls VALUES (1, NULL, 'hello')")
	afExec(t, db, ctx, "INSERT INTO v59_nulls VALUES (2, 10, NULL)")
	afExec(t, db, ctx, "INSERT INTO v59_nulls VALUES (3, 20, 'world')")

	// NA1: NULL + number = NULL
	checkNull("NA1 NULL + number", "SELECT a + 5 FROM v59_nulls WHERE id = 1")

	// NA2: NULL * number = NULL
	checkNull("NA2 NULL * number", "SELECT a * 5 FROM v59_nulls WHERE id = 1")

	// NA3: NULL comparison returns no match
	checkRowCount("NA3 NULL = NULL",
		"SELECT * FROM v59_nulls WHERE a = NULL", 0)

	// NA4: IS NULL
	checkRowCount("NA4 IS NULL",
		"SELECT * FROM v59_nulls WHERE a IS NULL", 1)

	// NA5: IS NOT NULL
	checkRowCount("NA5 IS NOT NULL",
		"SELECT * FROM v59_nulls WHERE a IS NOT NULL", 2)

	// NA6: NULL in concatenation
	// NULL || 'text' behavior varies by DB — check behavior
	// In SQLite it returns NULL, in PostgreSQL it returns NULL

	// ============================================================
	// === STRING OPERATIONS ===
	// ============================================================

	// ST1: Concatenation
	check("ST1 concat", "SELECT 'hello' || ' ' || 'world' FROM v59_t WHERE id = 1", "hello world")

	// ST2: LENGTH
	check("ST2 LENGTH", "SELECT LENGTH('hello') FROM v59_t WHERE id = 1", 5)

	// ST3: UPPER
	check("ST3 UPPER", "SELECT UPPER('hello') FROM v59_t WHERE id = 1", "HELLO")

	// ST4: LOWER
	check("ST4 LOWER", "SELECT LOWER('HELLO') FROM v59_t WHERE id = 1", "hello")

	// ST5: SUBSTR
	check("ST5 SUBSTR", "SELECT SUBSTR('hello', 2, 3) FROM v59_t WHERE id = 1", "ell")

	// ST6: TRIM
	check("ST6 TRIM", "SELECT TRIM('  hello  ') FROM v59_t WHERE id = 1", "hello")

	// ST7: REPLACE
	check("ST7 REPLACE", "SELECT REPLACE('hello world', 'world', 'there') FROM v59_t WHERE id = 1", "hello there")

	// ============================================================
	// === COMPARISON OPERATORS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v59_cmp (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v59_cmp VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v59_cmp VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v59_cmp VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO v59_cmp VALUES (4, 20)")

	// CM1: Greater than
	checkRowCount("CM1 >", "SELECT * FROM v59_cmp WHERE val > 20", 1)

	// CM2: Greater than or equal
	checkRowCount("CM2 >=", "SELECT * FROM v59_cmp WHERE val >= 20", 3)

	// CM3: Less than
	checkRowCount("CM3 <", "SELECT * FROM v59_cmp WHERE val < 20", 1)

	// CM4: Less than or equal
	checkRowCount("CM4 <=", "SELECT * FROM v59_cmp WHERE val <= 20", 3)

	// CM5: Not equal
	checkRowCount("CM5 !=", "SELECT * FROM v59_cmp WHERE val != 20", 2)

	// CM6: LIKE
	afExec(t, db, ctx, `CREATE TABLE v59_like (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v59_like VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v59_like VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v59_like VALUES (3, 'Anna')")
	afExec(t, db, ctx, "INSERT INTO v59_like VALUES (4, 'albert')")

	checkRowCount("CM6 LIKE prefix", "SELECT * FROM v59_like WHERE name LIKE 'A%'", 3)
	// Alice, Anna, albert (LIKE is case-insensitive in CobaltDB)

	// CM7: LIKE suffix
	checkRowCount("CM7 LIKE suffix", "SELECT * FROM v59_like WHERE name LIKE '%a'", 1)
	// Anna

	// CM8: LIKE contains
	checkRowCount("CM8 LIKE contains", "SELECT * FROM v59_like WHERE name LIKE '%ob%'", 1)

	// CM9: LIKE underscore
	checkRowCount("CM9 LIKE underscore", "SELECT * FROM v59_like WHERE name LIKE 'B_b'", 1)

	// ============================================================
	// === COMPLEX EXPRESSIONS ===
	// ============================================================

	// CE1: Nested CASE
	check("CE1 nested CASE",
		`SELECT CASE
			WHEN val > 25 THEN 'high'
			WHEN val > 15 THEN CASE
				WHEN val = 20 THEN 'medium-exact'
				ELSE 'medium-other'
			END
			ELSE 'low'
		 END FROM v59_cmp WHERE id = 2`, "medium-exact")

	// CE2: CASE with NULL
	check("CE2 CASE NULL",
		`SELECT CASE
			WHEN a IS NULL THEN 'null'
			ELSE 'not null'
		 END FROM v59_nulls WHERE id = 1`, "null")

	// CE3: Multiple WHEN in CASE
	check("CE3 multiple WHEN",
		`SELECT CASE val
			WHEN 10 THEN 'ten'
			WHEN 20 THEN 'twenty'
			WHEN 30 THEN 'thirty'
			ELSE 'other'
		 END FROM v59_cmp WHERE id = 3`, "thirty")

	// ============================================================
	// === AGGREGATE EDGE CASES ===
	// ============================================================

	// AE1: MIN/MAX of strings
	check("AE1 MIN string", "SELECT MIN(name) FROM v59_like", "Alice")
	check("AE2 MAX string", "SELECT MAX(name) FROM v59_like", "albert")

	// AE3: COUNT(*) vs COUNT(column)
	check("AE3 COUNT star", "SELECT COUNT(*) FROM v59_nulls", 3)
	check("AE4 COUNT col", "SELECT COUNT(a) FROM v59_nulls", 2)
	// COUNT(a) excludes NULL values

	// AE5: GROUP BY with single row groups
	checkRowCount("AE5 single row groups",
		"SELECT val, COUNT(*) FROM v59_cmp GROUP BY val", 3)
	// 10:1, 20:2, 30:1 = 3 groups

	// AE6: Aggregate with no matching rows
	check("AE6 COUNT no match",
		"SELECT COUNT(*) FROM v59_cmp WHERE val > 100", 0)

	checkNull("AE7 SUM no match",
		"SELECT SUM(val) FROM v59_cmp WHERE val > 100")

	// ============================================================
	// === SUBQUERY EDGE CASES ===
	// ============================================================

	// SE1: Subquery returning single value
	check("SE1 scalar subquery",
		"SELECT val FROM v59_cmp WHERE val = (SELECT MAX(val) FROM v59_cmp)", 30)

	// SE2: IN with empty subquery
	checkRowCount("SE2 IN empty subquery",
		"SELECT * FROM v59_cmp WHERE val IN (SELECT val FROM v59_cmp WHERE val > 100)", 0)

	// SE3: NOT IN with empty subquery (all rows should match)
	checkRowCount("SE3 NOT IN empty",
		"SELECT * FROM v59_cmp WHERE val NOT IN (SELECT val FROM v59_cmp WHERE val > 100)", 4)

	// SE4: Correlated subquery
	check("SE4 correlated",
		`SELECT name FROM v59_like
		 WHERE LENGTH(name) = (SELECT MAX(LENGTH(name)) FROM v59_like)`, "albert")
	// Alice=5, Bob=3, Anna=4, albert=6 → albert

	// ============================================================
	// === ORDER BY EXPRESSION ===
	// ============================================================

	// OE1: ORDER BY computed expression
	check("OE1 ORDER BY expr",
		"SELECT name FROM v59_like ORDER BY LENGTH(name) DESC LIMIT 1", "albert")

	// OE2: ORDER BY with CASE
	check("OE2 ORDER BY CASE",
		`SELECT name FROM v59_like
		 ORDER BY CASE WHEN name LIKE 'A%' THEN 0 ELSE 1 END, name ASC
		 LIMIT 1`, "Alice")

	// ============================================================
	// === INSERT EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v59_insert_test (
		id INTEGER PRIMARY KEY, name TEXT NOT NULL, val INTEGER DEFAULT 0)`)

	// IE1: Insert with explicit DEFAULT (if supported) or default value
	afExec(t, db, ctx, "INSERT INTO v59_insert_test (id, name) VALUES (1, 'test1')")
	check("IE1 default value",
		"SELECT val FROM v59_insert_test WHERE id = 1", 0)

	// IE2: Insert with NULL into NOT NULL column should fail
	checkError("IE2 NOT NULL constraint",
		"INSERT INTO v59_insert_test VALUES (2, NULL, 10)")

	// IE3: Duplicate primary key should fail
	checkError("IE3 duplicate PK",
		"INSERT INTO v59_insert_test VALUES (1, 'dup', 99)")

	// ============================================================
	// === MIXED COMPLEX QUERIES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v59_mix (
		id INTEGER PRIMARY KEY, cat TEXT, sub_cat TEXT, amount INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v59_mix VALUES (1, 'A', 'x', 100)")
	afExec(t, db, ctx, "INSERT INTO v59_mix VALUES (2, 'A', 'y', 200)")
	afExec(t, db, ctx, "INSERT INTO v59_mix VALUES (3, 'A', 'x', 150)")
	afExec(t, db, ctx, "INSERT INTO v59_mix VALUES (4, 'B', 'x', 300)")
	afExec(t, db, ctx, "INSERT INTO v59_mix VALUES (5, 'B', 'y', 250)")
	afExec(t, db, ctx, "INSERT INTO v59_mix VALUES (6, 'C', 'x', 50)")

	// MX1: CTE with multi-column GROUP BY
	checkRowCount("MX1 CTE multi group",
		`WITH grouped AS (
			SELECT cat, sub_cat, SUM(amount) AS total
			FROM v59_mix GROUP BY cat, sub_cat
		)
		SELECT * FROM grouped`, 5)
	// A-x:250, A-y:200, B-x:300, B-y:250, C-x:50 = 5

	// MX2: CTE aggregate over grouped data
	check("MX2 CTE agg over group",
		`WITH cat_totals AS (
			SELECT cat, SUM(amount) AS total FROM v59_mix GROUP BY cat
		)
		SELECT MAX(total) FROM cat_totals`, 550)
	// A:450, B:550, C:50 → max=550

	// MX3: Window function with GROUP BY
	check("MX3 window over group",
		`WITH cat_totals AS (
			SELECT cat, SUM(amount) AS total FROM v59_mix GROUP BY cat
		),
		ranked AS (
			SELECT cat, total, RANK() OVER (ORDER BY total DESC) AS rnk
			FROM cat_totals
		)
		SELECT cat FROM ranked WHERE rnk = 1`, "B")

	// MX4: Multiple conditions with subquery
	checkRowCount("MX4 multi condition subquery",
		`SELECT * FROM v59_mix
		 WHERE cat IN (SELECT cat FROM v59_mix GROUP BY cat HAVING SUM(amount) > 100)
		 AND sub_cat = 'x'`, 3)
	// Cats with SUM>100: A(450), B(550) → rows with sub_cat='x': A-x(id=1,3), B-x(id=4) = 3

	// MX5: Nested aggregates through CTE
	check("MX5 nested agg CTE",
		`WITH sub_totals AS (
			SELECT cat, sub_cat, SUM(amount) AS total
			FROM v59_mix GROUP BY cat, sub_cat
		),
		cat_avg AS (
			SELECT cat, AVG(total) AS avg_total FROM sub_totals GROUP BY cat
		)
		SELECT cat FROM cat_avg ORDER BY avg_total DESC LIMIT 1`, "B")
	// A: avg(250,200)=225; B: avg(300,250)=275; C: avg(50)=50 → B

	t.Logf("\n=== V59 TYPE & ARITHMETIC: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
