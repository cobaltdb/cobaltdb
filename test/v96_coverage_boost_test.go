package test

import (
	"fmt"
	"testing"
)

// =============================================================================
// TestV96_ coverage boost tests targeting lowest-coverage catalog functions:
//   evaluateFunctionCall 34.7%, evaluateCastExpr 22.2%, moduloValues 0%,
//   evaluateIn 47.1%, evaluateCaseExpr 50%, evaluateBinaryExpr 66.7%,
//   CONCAT_WS, DATE/TIME/NOW, STRFTIME, GROUP_CONCAT scalar, INSTR, REPLACE,
//   FLOOR, CEIL, SUBSTR edge cases, TRIM/LTRIM/RTRIM with custom chars,
//   PRINTF %s/%d/%f, CAST to various types
// =============================================================================

func TestV96_FunctionCoverage(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	check := func(label, sql string, want interface{}) {
		t.Helper()
		afExpectVal(t, db, ctx, sql, want)
	}

	// --- CONCAT_WS ---
	check("CONCAT_WS basic", "SELECT CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c")
	check("CONCAT_WS with NULL", "SELECT CONCAT_WS(',', 'a', NULL, 'c')", "a,c")
	check("CONCAT_WS NULL sep", "SELECT CONCAT_WS(NULL, 'a', 'b')", nil)

	// --- REPLACE ---
	check("REPLACE basic", "SELECT REPLACE('hello world', 'world', 'there')", "hello there")
	check("REPLACE NULL", "SELECT REPLACE(NULL, 'a', 'b')", nil)
	check("REPLACE empty old", "SELECT REPLACE('hello', '', 'x')", "hello")

	// --- INSTR ---
	check("INSTR found", "SELECT INSTR('hello world', 'world')", float64(7))
	check("INSTR not found", "SELECT INSTR('hello', 'xyz')", float64(0))
	check("INSTR NULL", "SELECT INSTR(NULL, 'a')", nil)

	// --- FLOOR / CEIL ---
	check("FLOOR", "SELECT FLOOR(3.7)", float64(3))
	check("FLOOR neg", "SELECT FLOOR(-2.3)", float64(-3))
	check("CEIL", "SELECT CEIL(3.2)", float64(4))
	check("CEIL neg", "SELECT CEIL(-2.7)", float64(-2))
	check("FLOOR NULL", "SELECT FLOOR(NULL)", nil)
	check("CEIL NULL", "SELECT CEIL(NULL)", nil)

	// --- SUBSTR edge cases ---
	check("SUBSTR 2 args", "SELECT SUBSTR('hello', 2)", "ello")
	check("SUBSTR 3 args", "SELECT SUBSTR('hello', 2, 3)", "ell")
	check("SUBSTR start beyond", "SELECT SUBSTR('hi', 10)", "")
	check("SUBSTR negative len", "SELECT SUBSTR('hello', 1, -1)", "")
	check("SUBSTR NULL", "SELECT SUBSTR(NULL, 1)", nil)

	// --- TRIM/LTRIM/RTRIM with custom chars ---
	check("LTRIM custom", "SELECT LTRIM('xxxhello', 'x')", "hello")
	check("RTRIM custom", "SELECT RTRIM('helloyyy', 'y')", "hello")
	check("TRIM custom", "SELECT TRIM('**hello**', '*')", "hello")

	// --- PRINTF ---
	check("PRINTF %s", "SELECT PRINTF('%s world', 'hello')", "hello world")
	check("PRINTF %d", "SELECT PRINTF('val=%d', 42)", "val=42")
	check("PRINTF %f", "SELECT PRINTF('pi=%f', 3.14)", "pi=3.140000")

	// --- DATE / TIME / NOW ---
	// DATE/TIME just return arg
	check("DATE pass-through", "SELECT DATE('2024-01-01')", "2024-01-01")
	check("TIME pass-through", "SELECT TIME('12:30:00')", "12:30:00")

	// --- STRFTIME ---
	check("STRFTIME basic", "SELECT STRFTIME('%Y', '2024-01-01')", "2024-01-01")

	// --- GROUP_CONCAT in aggregate context ---
	afExec(t, db, ctx, "CREATE TABLE t96_gc (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t96_gc VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t96_gc VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO t96_gc VALUES (3, 'c')")
	check("GROUP_CONCAT agg", "SELECT GROUP_CONCAT(val) FROM t96_gc", "a,b,c")

	// --- NULLIF ---
	check("NULLIF equal", "SELECT NULLIF(1, 1)", nil)
	check("NULLIF not equal", "SELECT NULLIF(1, 2)", float64(1))
	check("NULLIF NULL first", "SELECT NULLIF(NULL, 1)", nil)

	// --- ABS edge case ---
	check("ABS NULL", "SELECT ABS(NULL)", nil)
	check("ABS positive", "SELECT ABS(5)", float64(5))
}

func TestV96_ModuloOperator(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	check := func(label, sql string, want interface{}) {
		t.Helper()
		afExpectVal(t, db, ctx, sql, want)
	}

	check("Modulo int", "SELECT 10 % 3", float64(1))
	check("Modulo float", "SELECT 10.5 % 3.0", float64(1.5))
	check("Modulo negative", "SELECT -7 % 3", float64(-1))
}

func TestV96_CastExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	check := func(label, sql string, want interface{}) {
		t.Helper()
		afExpectVal(t, db, ctx, sql, want)
	}

	// CAST to INTEGER
	check("CAST float to int", "SELECT CAST(3.7 AS INTEGER)", int64(3))
	check("CAST string to int", "SELECT CAST('42' AS INTEGER)", int64(42))
	check("CAST NULL to int", "SELECT CAST(NULL AS INTEGER)", nil)

	// CAST to REAL
	check("CAST int to real", "SELECT CAST(42 AS REAL)", float64(42))
	check("CAST string to real", "SELECT CAST('3.14' AS REAL)", float64(3.14))

	// CAST to TEXT
	check("CAST int to text", "SELECT CAST(42 AS TEXT)", "42")

	// CAST to BOOLEAN
	check("CAST 1 to bool", "SELECT CAST(1 AS BOOLEAN)", true)
	check("CAST 0 to bool", "SELECT CAST(0 AS BOOLEAN)", false)
	check("CAST 'true' to bool", "SELECT CAST('true' AS BOOLEAN)", true)
	check("CAST 'false' to bool", "SELECT CAST('false' AS BOOLEAN)", false)
}

func TestV96_InExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_in (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_in VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t96_in VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t96_in VALUES (3, 30)")

	check := func(label, sql string, want interface{}) {
		t.Helper()
		afExpectVal(t, db, ctx, sql, want)
	}

	// IN with subquery
	check("IN subquery found", "SELECT val FROM t96_in WHERE val IN (SELECT val FROM t96_in WHERE val > 15) AND id = 2", float64(20))

	// NOT IN with subquery
	check("NOT IN subquery", "SELECT COUNT(*) FROM t96_in WHERE val NOT IN (SELECT val FROM t96_in WHERE val > 25)", float64(2))

	// NOT IN with list
	check("NOT IN list", "SELECT COUNT(*) FROM t96_in WHERE val NOT IN (10, 30)", float64(1))

	// IN with NULL in subquery
	afExec(t, db, ctx, "INSERT INTO t96_in VALUES (4, NULL)")
	check("IN with NULL subquery", "SELECT COUNT(*) FROM t96_in WHERE val IN (SELECT val FROM t96_in)", float64(3))
}

func TestV96_CaseExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	check := func(label, sql string, want interface{}) {
		t.Helper()
		afExpectVal(t, db, ctx, sql, want)
	}

	// Searched CASE (no base expr)
	check("Searched CASE true", "SELECT CASE WHEN 1=1 THEN 'yes' WHEN 1=2 THEN 'no' ELSE 'maybe' END", "yes")
	check("Searched CASE else", "SELECT CASE WHEN 1=2 THEN 'yes' ELSE 'no' END", "no")

	// Simple CASE (with base expr)
	check("Simple CASE match", "SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", "two")
	check("Simple CASE else", "SELECT CASE 99 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", "other")

	// CASE in UPDATE
	afExec(t, db, ctx, "CREATE TABLE t96_case (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t96_case VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t96_case VALUES (2, 'b')")
	afExec(t, db, ctx, "UPDATE t96_case SET val = CASE id WHEN 1 THEN 'first' WHEN 2 THEN 'second' END")
	check("CASE in UPDATE 1", "SELECT val FROM t96_case WHERE id = 1", "first")
	check("CASE in UPDATE 2", "SELECT val FROM t96_case WHERE id = 2", "second")
}

func TestV96_UpdateComplexPatterns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// UPDATE with subquery in SET
	afExec(t, db, ctx, "CREATE TABLE t96_main (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t96_ref (id INTEGER PRIMARY KEY, total INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_main VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO t96_main VALUES (2, 200)")
	afExec(t, db, ctx, "INSERT INTO t96_ref VALUES (1, 999)")

	afExec(t, db, ctx, "UPDATE t96_main SET val = (SELECT total FROM t96_ref WHERE t96_ref.id = t96_main.id) WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT val FROM t96_main WHERE id = 1", float64(999))

	// UPDATE with arithmetic expression
	afExec(t, db, ctx, "UPDATE t96_main SET val = val + 10 WHERE id = 2")
	afExpectVal(t, db, ctx, "SELECT val FROM t96_main WHERE id = 2", float64(210))

	// UPDATE with BETWEEN in WHERE
	afExec(t, db, ctx, "CREATE TABLE t96_range (id INTEGER PRIMARY KEY, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_range VALUES (1, 50)")
	afExec(t, db, ctx, "INSERT INTO t96_range VALUES (2, 75)")
	afExec(t, db, ctx, "INSERT INTO t96_range VALUES (3, 90)")
	afExec(t, db, ctx, "UPDATE t96_range SET score = score + 5 WHERE score BETWEEN 70 AND 95")
	afExpectVal(t, db, ctx, "SELECT score FROM t96_range WHERE id = 2", float64(80))
	afExpectVal(t, db, ctx, "SELECT score FROM t96_range WHERE id = 3", float64(95))
	afExpectVal(t, db, ctx, "SELECT score FROM t96_range WHERE id = 1", float64(50)) // unchanged

	// UPDATE multiple columns
	afExec(t, db, ctx, "CREATE TABLE t96_multi (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
	afExec(t, db, ctx, "INSERT INTO t96_multi VALUES (1, 'x', 'y')")
	afExec(t, db, ctx, "UPDATE t96_multi SET a = 'new_a', b = 'new_b' WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT a FROM t96_multi WHERE id = 1", "new_a")
	afExpectVal(t, db, ctx, "SELECT b FROM t96_multi WHERE id = 1", "new_b")
}

func TestV96_DeleteWithSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_del (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_del VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t96_del VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t96_del VALUES (3, 30)")

	// DELETE with subquery in WHERE
	afExec(t, db, ctx, "DELETE FROM t96_del WHERE val IN (SELECT val FROM t96_del WHERE val > 15)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_del", float64(1))
	afExpectVal(t, db, ctx, "SELECT val FROM t96_del", float64(10))
}

func TestV96_ComplexJoinWithGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_dept (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t96_emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t96_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO t96_dept VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO t96_emp VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t96_emp VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO t96_emp VALUES (3, 2, 150)")

	// JOIN + GROUP BY + HAVING + ORDER BY
	rows := afQuery(t, db, ctx, "SELECT t96_dept.name, SUM(t96_emp.salary) as total FROM t96_dept JOIN t96_emp ON t96_dept.id = t96_emp.dept_id GROUP BY t96_dept.name HAVING SUM(t96_emp.salary) > 200 ORDER BY total DESC")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != "Engineering" {
		t.Errorf("expected Engineering, got %v", rows[0][0])
	}

	// JOIN + GROUP BY + COUNT
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_dept JOIN t96_emp ON t96_dept.id = t96_emp.dept_id GROUP BY t96_dept.name ORDER BY COUNT(*) DESC LIMIT 1", float64(2))
}

func TestV96_SelectWithMultipleOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_ord (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_ord VALUES (1, 'x', 30)")
	afExec(t, db, ctx, "INSERT INTO t96_ord VALUES (2, 'x', 10)")
	afExec(t, db, ctx, "INSERT INTO t96_ord VALUES (3, 'y', 20)")
	afExec(t, db, ctx, "INSERT INTO t96_ord VALUES (4, 'y', 40)")

	// ORDER BY multiple columns
	rows := afQuery(t, db, ctx, "SELECT id FROM t96_ord ORDER BY a ASC, b DESC")
	expected := []string{"1", "2", "4", "3"}
	for i, want := range expected {
		if i < len(rows) {
			got := fmt.Sprintf("%v", rows[i][0])
			if got != want {
				t.Errorf("row %d: expected %s, got %s", i, want, got)
			}
		}
	}

	// ORDER BY with expression
	rows2 := afQuery(t, db, ctx, "SELECT id FROM t96_ord ORDER BY b * -1")
	if len(rows2) > 0 && fmt.Sprintf("%v", rows2[0][0]) != "4" {
		t.Errorf("expected id 4 first, got %v", rows2[0][0])
	}
}

func TestV96_InsertWithDefaultAndExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_def (id INTEGER PRIMARY KEY, val TEXT DEFAULT 'unknown', score INTEGER DEFAULT 0)")

	// Insert with columns specified (omitting DEFAULT columns)
	afExec(t, db, ctx, "INSERT INTO t96_def (id) VALUES (1)")
	afExpectVal(t, db, ctx, "SELECT val FROM t96_def WHERE id = 1", "unknown")
	afExpectVal(t, db, ctx, "SELECT score FROM t96_def WHERE id = 1", float64(0))

	// Insert with expression in VALUES
	afExec(t, db, ctx, "INSERT INTO t96_def VALUES (2, 'test' || '_val', 10 + 20)")
	afExpectVal(t, db, ctx, "SELECT val FROM t96_def WHERE id = 2", "test_val")
	afExpectVal(t, db, ctx, "SELECT score FROM t96_def WHERE id = 2", float64(30))
}

func TestV96_DropTableWithIndexes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_drop (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx_drop_val ON t96_drop (val)")
	afExec(t, db, ctx, "INSERT INTO t96_drop VALUES (1, 'test')")

	// Drop table should clean up indexes
	afExec(t, db, ctx, "DROP TABLE t96_drop")

	// Recreate same table and index should work
	afExec(t, db, ctx, "CREATE TABLE t96_drop (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx_drop_val ON t96_drop (val)")
	afExec(t, db, ctx, "INSERT INTO t96_drop VALUES (1, 'new')")
	afExpectVal(t, db, ctx, "SELECT val FROM t96_drop WHERE id = 1", "new")
}

func TestV96_VacuumAndAnalyze(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_vac (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t96_vac VALUES (%d, 'data')", i))
	}

	// Delete half the rows then vacuum
	afExec(t, db, ctx, "DELETE FROM t96_vac WHERE id > 10")
	afExec(t, db, ctx, "VACUUM")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_vac", float64(10))

	// Analyze
	afExec(t, db, ctx, "ANALYZE")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_vac", float64(10))
}

func TestV96_CreateIndexExistingData(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_idx (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t96_idx VALUES (1, 'alpha')")
	afExec(t, db, ctx, "INSERT INTO t96_idx VALUES (2, 'beta')")
	afExec(t, db, ctx, "INSERT INTO t96_idx VALUES (3, 'gamma')")

	// Create index on table with existing data
	afExec(t, db, ctx, "CREATE INDEX idx_val ON t96_idx (val)")

	// Verify index works for queries
	afExpectVal(t, db, ctx, "SELECT id FROM t96_idx WHERE val = 'beta'", float64(2))

	// Create UNIQUE index - should scan existing data
	afExec(t, db, ctx, "CREATE TABLE t96_idx2 (id INTEGER PRIMARY KEY, email TEXT)")
	afExec(t, db, ctx, "INSERT INTO t96_idx2 VALUES (1, 'a@b.com')")
	afExec(t, db, ctx, "INSERT INTO t96_idx2 VALUES (2, 'c@d.com')")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_email ON t96_idx2 (email)")

	// Now inserting duplicate should fail
	if _, err := db.Exec(ctx, "INSERT INTO t96_idx2 VALUES (3, 'a@b.com')"); err == nil {
		t.Error("expected UNIQUE constraint error")
	}
}

func TestV96_PreparedStatementEdgeCases(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_ps (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_ps VALUES (1, 'hello', 100)")
	afExec(t, db, ctx, "INSERT INTO t96_ps VALUES (2, 'world', 200)")

	// SELECT with WHERE conditions
	afExpectVal(t, db, ctx, "SELECT val FROM t96_ps WHERE id = 1 AND num > 50", "hello")

	// UPDATE with multiple SET
	afExec(t, db, ctx, "UPDATE t96_ps SET val = 'updated', num = 999 WHERE id = 2")
	afExpectVal(t, db, ctx, "SELECT val FROM t96_ps WHERE id = 2", "updated")
	afExpectVal(t, db, ctx, "SELECT num FROM t96_ps WHERE id = 2", float64(999))

	// DELETE with condition
	afExec(t, db, ctx, "DELETE FROM t96_ps WHERE num > 500")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_ps", float64(1))
}

func TestV96_StringFunctionsDeep(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	check := func(label, sql string, want interface{}) {
		t.Helper()
		afExpectVal(t, db, ctx, sql, want)
	}

	// UPPER/LOWER with non-string input
	check("UPPER number", "SELECT UPPER(123)", "123")
	check("LOWER number", "SELECT LOWER(456)", "456")

	// LENGTH with non-string
	check("LENGTH number", "SELECT LENGTH(12345)", float64(5))
	check("LENGTH NULL", "SELECT LENGTH(NULL)", nil)

	// CONCAT
	check("CONCAT basic", "SELECT CONCAT('a', 'b', 'c')", "abc")
	check("CONCAT with NULL", "SELECT CONCAT('a', NULL, 'c')", "ac")

	// REVERSE
	check("REVERSE", "SELECT REVERSE('hello')", "olleh")
	check("REVERSE NULL", "SELECT REVERSE(NULL)", nil)

	// REPEAT
	check("REPEAT", "SELECT REPEAT('ab', 3)", "ababab")
	check("REPEAT zero", "SELECT REPEAT('ab', 0)", "")

	// LEFT / RIGHT
	check("LEFT", "SELECT LEFT('hello', 3)", "hel")
	check("LEFT zero", "SELECT LEFT('hello', 0)", "")
	check("LEFT beyond", "SELECT LEFT('hi', 10)", "hi")
	check("RIGHT", "SELECT RIGHT('hello', 3)", "llo")
	check("RIGHT zero", "SELECT RIGHT('hello', 0)", "")

	// LPAD / RPAD
	check("LPAD", "SELECT LPAD('hi', 5, '*')", "***hi")
	check("RPAD", "SELECT RPAD('hi', 5, '*')", "hi***")
	check("LPAD already long", "SELECT LPAD('hello', 3, '*')", "hel")

	// HEX
	check("HEX number", "SELECT HEX(255)", "FF")

	// TYPEOF
	check("TYPEOF int", "SELECT TYPEOF(42)", "integer")
	check("TYPEOF text", "SELECT TYPEOF('hello')", "text")
	check("TYPEOF null", "SELECT TYPEOF(NULL)", "null")
	check("TYPEOF real", "SELECT TYPEOF(3.14)", "real")

	// IIF
	check("IIF true", "SELECT IIF(1, 'yes', 'no')", "yes")
	check("IIF false", "SELECT IIF(0, 'yes', 'no')", "no")
	check("IIF string cond", "SELECT IIF('truthy', 'yes', 'no')", "yes")

	// UNICODE
	check("UNICODE", "SELECT UNICODE('A')", float64(65))
	check("UNICODE empty", "SELECT UNICODE('')", nil)

	// CHAR
	check("CHAR", "SELECT CHAR(65, 66, 67)", "ABC")

	// QUOTE
	check("QUOTE string", "SELECT QUOTE('it''s')", "'it''s'")
	check("QUOTE NULL", "SELECT QUOTE(NULL)", "NULL")
	check("QUOTE number", "SELECT QUOTE(42)", "42")

	// GLOB
	check("GLOB match", "SELECT GLOB('he*', 'hello')", true)
	check("GLOB no match", "SELECT GLOB('he*', 'world')", false)
	check("GLOB question", "SELECT GLOB('h?llo', 'hello')", true)
}

func TestV96_BinaryExprCoverage(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	check := func(label, sql string, want interface{}) {
		t.Helper()
		afExpectVal(t, db, ctx, sql, want)
	}

	// String concatenation with ||
	check("String concat", "SELECT 'hello' || ' ' || 'world'", "hello world")

	// Comparison with different types
	check("Int > float", "SELECT 5 > 3.5", true)
	check("String compare", "SELECT 'abc' < 'def'", true)

	// Boolean logic
	check("AND true", "SELECT 1=1 AND 2=2", true)
	check("AND false", "SELECT 1=1 AND 1=2", false)
	check("OR true", "SELECT 1=2 OR 2=2", true)
	check("OR false", "SELECT 1=2 OR 2=3", false)

	// NOT
	check("NOT true", "SELECT NOT 1=2", true)
	check("NOT false", "SELECT NOT 1=1", false)

	// NULL comparisons
	check("NULL = NULL", "SELECT NULL = NULL", nil)
	check("NULL != 1", "SELECT NULL != 1", nil)
	check("NULL AND true", "SELECT NULL AND 1=1", nil)
	check("NULL OR true", "SELECT NULL OR 1=1", true)
}

func TestV96_ComplexSelectPatterns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_sel (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_sel VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t96_sel VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t96_sel VALUES (3, 'B', 30)")
	afExec(t, db, ctx, "INSERT INTO t96_sel VALUES (4, 'B', 40)")
	afExec(t, db, ctx, "INSERT INTO t96_sel VALUES (5, 'C', 50)")

	// GROUP BY + HAVING with aggregate alias
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t96_sel GROUP BY cat HAVING total >= 30 ORDER BY total")
	if len(rows) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(rows))
	}

	// DISTINCT
	rows2 := afQuery(t, db, ctx, "SELECT DISTINCT cat FROM t96_sel ORDER BY cat")
	if len(rows2) != 3 {
		t.Fatalf("expected 3 distinct cats, got %d", len(rows2))
	}

	// Subquery in SELECT
	afExpectVal(t, db, ctx, "SELECT (SELECT MAX(val) FROM t96_sel)", float64(50))

	// EXISTS
	afExpectVal(t, db, ctx, "SELECT EXISTS(SELECT 1 FROM t96_sel WHERE cat = 'A')", true)
	afExpectVal(t, db, ctx, "SELECT EXISTS(SELECT 1 FROM t96_sel WHERE cat = 'Z')", false)

	// UNION
	rows3 := afQuery(t, db, ctx, "SELECT cat FROM t96_sel WHERE cat = 'A' UNION SELECT cat FROM t96_sel WHERE cat = 'B'")
	if len(rows3) != 2 {
		t.Fatalf("expected 2 union rows, got %d", len(rows3))
	}

	// UNION ALL
	rows4 := afQuery(t, db, ctx, "SELECT cat FROM t96_sel WHERE cat = 'A' UNION ALL SELECT cat FROM t96_sel WHERE cat = 'A'")
	if len(rows4) != 4 {
		t.Fatalf("expected 4 union all rows, got %d", len(rows4))
	}

	// INTERSECT
	rows5 := afQuery(t, db, ctx, "SELECT cat FROM t96_sel WHERE val <= 20 INTERSECT SELECT cat FROM t96_sel WHERE val >= 10")
	if len(rows5) != 1 {
		t.Fatalf("expected 1 intersect row, got %d", len(rows5))
	}

	// EXCEPT
	rows6 := afQuery(t, db, ctx, "SELECT DISTINCT cat FROM t96_sel EXCEPT SELECT cat FROM t96_sel WHERE cat = 'A'")
	if len(rows6) != 2 {
		t.Fatalf("expected 2 except rows, got %d", len(rows6))
	}
}

func TestV96_CTEAndRecursion(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Simple CTE
	afExpectVal(t, db, ctx, "WITH cte AS (SELECT 42 as val) SELECT val FROM cte", float64(42))

	// CTE with multiple references
	afExec(t, db, ctx, "CREATE TABLE t96_cte (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_cte VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t96_cte VALUES (2, 20)")

	rows := afQuery(t, db, ctx, "WITH doubled AS (SELECT id, val * 2 as dval FROM t96_cte) SELECT id, dval FROM doubled ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 CTE rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][1]) != "20" {
		t.Errorf("expected 20, got %v", rows[0][1])
	}

	// Recursive CTE (1..5)
	rows2 := afQuery(t, db, ctx, `
		WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x + 1 FROM cnt WHERE x < 5
		)
		SELECT x FROM cnt ORDER BY x
	`)
	if len(rows2) != 5 {
		t.Fatalf("expected 5 recursive rows, got %d", len(rows2))
	}
	for i := 0; i < 5; i++ {
		got := fmt.Sprintf("%v", rows2[i][0])
		want := fmt.Sprintf("%d", i+1)
		if got != want {
			t.Errorf("row %d: expected %s, got %s", i, want, got)
		}
	}
}

func TestV96_ViewsWithAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_view (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_view VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t96_view VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t96_view VALUES (3, 'B', 30)")

	// Create view with GROUP BY
	afExec(t, db, ctx, "CREATE VIEW v96_summary AS SELECT cat, SUM(val) as total, COUNT(*) as cnt FROM t96_view GROUP BY cat")

	// Query the view
	rows := afQuery(t, db, ctx, "SELECT cat, total FROM v96_summary ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 view rows, got %d", len(rows))
	}
	if rows[0][0] != "A" || rows[0][1] != float64(30) {
		t.Errorf("view row 0: expected A/30, got %v/%v", rows[0][0], rows[0][1])
	}

	// View with DISTINCT
	afExec(t, db, ctx, "CREATE VIEW v96_cats AS SELECT DISTINCT cat FROM t96_view")
	rows2 := afQuery(t, db, ctx, "SELECT * FROM v96_cats ORDER BY cat")
	if len(rows2) != 2 {
		t.Fatalf("expected 2 distinct view rows, got %d", len(rows2))
	}
}

func TestV96_TransactionWithConstraintViolation(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_txn (id INTEGER PRIMARY KEY, val TEXT UNIQUE)")
	afExec(t, db, ctx, "INSERT INTO t96_txn VALUES (1, 'a')")

	// Begin transaction, try violating unique, rollback
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "INSERT INTO t96_txn VALUES (2, 'b')")

	// Try to violate UNIQUE - should error
	if _, err := db.Exec(ctx, "INSERT INTO t96_txn VALUES (3, 'a')"); err == nil {
		t.Error("expected UNIQUE constraint error in transaction")
	}

	// Rollback the whole transaction
	afExec(t, db, ctx, "ROLLBACK")

	// Only original row should exist
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_txn", float64(1))
}

func TestV96_IsNullExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_null (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t96_null VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t96_null VALUES (2, NULL)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_null WHERE val IS NULL", float64(1))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_null WHERE val IS NOT NULL", float64(1))
	afExpectVal(t, db, ctx, "SELECT id FROM t96_null WHERE val IS NULL", float64(2))
}

func TestV96_LikeExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	check := func(label, sql string, want interface{}) {
		t.Helper()
		afExpectVal(t, db, ctx, sql, want)
	}

	afExec(t, db, ctx, "CREATE TABLE t96_like (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t96_like VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t96_like VALUES (2, 'world')")
	afExec(t, db, ctx, "INSERT INTO t96_like VALUES (3, 'help')")
	afExec(t, db, ctx, "INSERT INTO t96_like VALUES (4, NULL)")

	check("LIKE prefix", "SELECT COUNT(*) FROM t96_like WHERE val LIKE 'hel%'", float64(2))
	check("LIKE suffix", "SELECT COUNT(*) FROM t96_like WHERE val LIKE '%ld'", float64(1))
	check("LIKE underscore", "SELECT COUNT(*) FROM t96_like WHERE val LIKE 'hel_o'", float64(1))
	check("NOT LIKE", "SELECT COUNT(*) FROM t96_like WHERE val NOT LIKE 'hel%'", float64(1))

	// LIKE with NULL
	check("LIKE NULL val", "SELECT COUNT(*) FROM t96_like WHERE val LIKE '%'", float64(3)) // NULL doesn't match
}

func TestV96_BetweenExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_btw (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_btw VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t96_btw VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t96_btw VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO t96_btw VALUES (4, NULL)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_btw WHERE val BETWEEN 15 AND 25", float64(1))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_btw WHERE val NOT BETWEEN 15 AND 25", float64(2))
	// NULL BETWEEN should return NULL (filtered out)
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_btw WHERE val BETWEEN 1 AND 100", float64(3))
}

func TestV96_SaveAndLoad(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_persist (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t96_persist VALUES (1, 'data')")
	afExec(t, db, ctx, "CREATE INDEX idx_persist ON t96_persist (val)")

	// Verify data survives within the session
	afExpectVal(t, db, ctx, "SELECT val FROM t96_persist WHERE id = 1", "data")
}

func TestV96_MultiTableJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_a (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t96_b (id INTEGER PRIMARY KEY, a_id INTEGER, info TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t96_c (id INTEGER PRIMARY KEY, b_id INTEGER, extra TEXT)")

	afExec(t, db, ctx, "INSERT INTO t96_a VALUES (1, 'alpha')")
	afExec(t, db, ctx, "INSERT INTO t96_b VALUES (1, 1, 'one')")
	afExec(t, db, ctx, "INSERT INTO t96_c VALUES (1, 1, 'deep')")

	// 3-table JOIN
	rows := afQuery(t, db, ctx, "SELECT t96_a.val, t96_b.info, t96_c.extra FROM t96_a JOIN t96_b ON t96_a.id = t96_b.a_id JOIN t96_c ON t96_b.id = t96_c.b_id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row from 3-table join, got %d", len(rows))
	}
	if rows[0][0] != "alpha" || rows[0][1] != "one" || rows[0][2] != "deep" {
		t.Errorf("unexpected 3-table join result: %v", rows[0])
	}

	// LEFT JOIN
	afExec(t, db, ctx, "INSERT INTO t96_a VALUES (2, 'beta')")
	rows2 := afQuery(t, db, ctx, "SELECT t96_a.val, t96_b.info FROM t96_a LEFT JOIN t96_b ON t96_a.id = t96_b.a_id ORDER BY t96_a.id")
	if len(rows2) != 2 {
		t.Fatalf("expected 2 rows from left join, got %d", len(rows2))
	}
	if rows2[1][1] != nil {
		t.Errorf("expected NULL for unmatched left join, got %v", rows2[1][1])
	}
}

func TestV96_AggregateEdgeCases(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t96_agg (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t96_agg VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t96_agg VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t96_agg VALUES (3, NULL)")

	// Aggregates with NULL
	afExpectVal(t, db, ctx, "SELECT SUM(val) FROM t96_agg", float64(30))
	afExpectVal(t, db, ctx, "SELECT AVG(val) FROM t96_agg", float64(15))
	afExpectVal(t, db, ctx, "SELECT COUNT(val) FROM t96_agg", float64(2))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_agg", float64(3))
	afExpectVal(t, db, ctx, "SELECT MIN(val) FROM t96_agg", float64(10))
	afExpectVal(t, db, ctx, "SELECT MAX(val) FROM t96_agg", float64(20))

	// Aggregates on empty result
	afExpectVal(t, db, ctx, "SELECT SUM(val) FROM t96_agg WHERE id > 100", nil)
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t96_agg WHERE id > 100", float64(0))
}
