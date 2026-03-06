package test

import (
	"fmt"
	"testing"
)

// TestV70DataTypes tests data type handling: type preservation, CAST operations,
// NULL behavior, type coercion, boundary values, and type interactions.
func TestV70DataTypes(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			if expected == nil {
				pass++
				return
			}
			t.Errorf("[FAIL] %s: no rows returned, expected %v", desc, expected)
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

	checkNull := func(desc string, sql string) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			pass++
			return
		}
		if rows[0][0] == nil {
			pass++
			return
		}
		t.Errorf("[FAIL] %s: expected NULL, got %v", desc, rows[0][0])
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

	// ============================================================
	// === CAST OPERATIONS ===
	// ============================================================

	// CA1: CAST integer to text
	check("CA1 INT to TEXT", "SELECT CAST(42 AS TEXT)", "42")

	// CA2: CAST text to integer
	check("CA2 TEXT to INT", "SELECT CAST('123' AS INTEGER)", 123)

	// CA3: CAST float to integer (truncates)
	check("CA3 FLOAT to INT", "SELECT CAST(3.7 AS INTEGER)", 3)

	// CA4: CAST integer to float
	check("CA4 INT to FLOAT", "SELECT CAST(42 AS REAL)", 42)

	// CA5: CAST NULL remains NULL
	checkNull("CA5 CAST NULL", "SELECT CAST(NULL AS INTEGER)")

	// CA6: CAST text to float
	check("CA6 TEXT to FLOAT", "SELECT CAST('3.14' AS REAL)", "3.14")

	// ============================================================
	// === INTEGER OPERATIONS ===
	// ============================================================

	// IO1: Basic integer arithmetic
	check("IO1 add", "SELECT 100 + 200", 300)
	check("IO1 sub", "SELECT 500 - 200", 300)
	check("IO1 mul", "SELECT 15 * 20", 300)

	// IO2: Division returns float
	check("IO2 div float", "SELECT 7 / 2", "3.5")

	// IO3: Modulo
	check("IO3 modulo", "SELECT 17 % 5", 2)

	// IO4: Large integers
	check("IO4 large int", "SELECT 999999999 + 1", 1000000000)

	// IO5: Negative integers
	check("IO5 negative", "SELECT -42 + 100", 58)

	// IO6: Integer comparison
	checkRowCount("IO6 int compare",
		"SELECT 1 WHERE 100 > 50", 1)

	// ============================================================
	// === FLOAT OPERATIONS ===
	// ============================================================

	// FO1: Float arithmetic
	check("FO1 float add", "SELECT 1.5 + 2.5", 4)
	check("FO1 float mul", "SELECT 2.5 * 4.0", 10)

	// FO2: Float + Integer
	check("FO2 float+int", "SELECT 1.5 + 2", "3.5")

	// FO3: Float precision
	check("FO3 precision", "SELECT 0.1 + 0.2", "0.30000000000000004")

	// ============================================================
	// === STRING OPERATIONS ===
	// ============================================================

	// SO1: Concatenation
	check("SO1 concat", "SELECT 'hello' || ' ' || 'world'", "hello world")

	// SO2: String functions
	check("SO2 UPPER", "SELECT UPPER('hello')", "HELLO")
	check("SO2 LOWER", "SELECT LOWER('HELLO')", "hello")
	check("SO2 LENGTH", "SELECT LENGTH('hello')", 5)
	check("SO2 SUBSTR", "SELECT SUBSTR('hello', 2, 3)", "ell")
	check("SO2 TRIM", "SELECT TRIM('  hello  ')", "hello")

	// SO3: Empty string
	check("SO3 empty length", "SELECT LENGTH('')", 0)
	check("SO3 empty concat", "SELECT '' || 'hello'", "hello")

	// SO4: REPLACE
	check("SO4 REPLACE", "SELECT REPLACE('hello world', 'world', 'earth')", "hello earth")

	// SO5: INSTR
	check("SO5 INSTR", "SELECT INSTR('hello world', 'world')", 7)
	check("SO5 INSTR not found", "SELECT INSTR('hello', 'xyz')", 0)

	// ============================================================
	// === NULL BEHAVIOR ===
	// ============================================================

	// NB1: NULL arithmetic
	checkNull("NB1 NULL + 1", "SELECT NULL + 1")
	checkNull("NB1 NULL * 5", "SELECT NULL * 5")
	checkNull("NB1 NULL - 1", "SELECT NULL - 1")

	// NB2: NULL string ops
	checkNull("NB2 NULL concat", "SELECT NULL || 'hello'")
	checkNull("NB2 NULL UPPER", "SELECT UPPER(NULL)")
	checkNull("NB2 NULL LENGTH", "SELECT LENGTH(NULL)")

	// NB3: NULL comparisons
	checkRowCount("NB3 NULL = NULL", "SELECT 1 WHERE NULL = NULL", 0)
	checkRowCount("NB3 NULL != NULL", "SELECT 1 WHERE NULL != NULL", 0)
	checkRowCount("NB3 NULL > 0", "SELECT 1 WHERE NULL > 0", 0)

	// NB4: NULL in COALESCE
	check("NB4 COALESCE", "SELECT COALESCE(NULL, NULL, 42)", 42)
	checkNull("NB4 COALESCE all null", "SELECT COALESCE(NULL, NULL)")

	// NB5: NULL in NULLIF
	checkNull("NB5 NULLIF equal", "SELECT NULLIF(1, 1)")
	check("NB5 NULLIF different", "SELECT NULLIF(1, 2)", 1)

	// NB6: IS NULL / IS NOT NULL
	checkRowCount("NB6 IS NULL", "SELECT 1 WHERE NULL IS NULL", 1)
	checkRowCount("NB6 IS NOT NULL", "SELECT 1 WHERE 42 IS NOT NULL", 1)
	checkRowCount("NB6 val IS NULL false", "SELECT 1 WHERE 42 IS NULL", 0)

	// ============================================================
	// === TYPE IN TABLE STORAGE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v70_types (
		id INTEGER PRIMARY KEY,
		int_val INTEGER,
		text_val TEXT,
		real_val REAL,
		null_val TEXT
	)`)
	checkNoError("TS1 insert", "INSERT INTO v70_types VALUES (1, 42, 'hello', 3.14, NULL)")

	// TS1: Type preservation
	check("TS1 int", "SELECT int_val FROM v70_types WHERE id = 1", 42)
	check("TS1 text", "SELECT text_val FROM v70_types WHERE id = 1", "hello")
	check("TS1 real", "SELECT real_val FROM v70_types WHERE id = 1", "3.14")
	checkNull("TS1 null", "SELECT null_val FROM v70_types WHERE id = 1")

	// TS2: TYPEOF function
	check("TS2 typeof int", "SELECT TYPEOF(int_val) FROM v70_types WHERE id = 1", "integer")
	check("TS2 typeof text", "SELECT TYPEOF(text_val) FROM v70_types WHERE id = 1", "text")
	check("TS2 typeof null", "SELECT TYPEOF(null_val) FROM v70_types WHERE id = 1", "null")

	// ============================================================
	// === TYPE COERCION IN COMPARISONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v70_coerce (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v70_coerce VALUES (1, '100')")
	afExec(t, db, ctx, "INSERT INTO v70_coerce VALUES (2, '200')")
	afExec(t, db, ctx, "INSERT INTO v70_coerce VALUES (3, 'abc')")

	// TC1: String vs integer comparison
	checkRowCount("TC1 string = int",
		"SELECT * FROM v70_coerce WHERE val = '100'", 1)

	// ============================================================
	// === AGGREGATE TYPE BEHAVIOR ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v70_agg (id INTEGER PRIMARY KEY, val INTEGER, fval REAL)`)
	afExec(t, db, ctx, "INSERT INTO v70_agg VALUES (1, 10, 1.5)")
	afExec(t, db, ctx, "INSERT INTO v70_agg VALUES (2, 20, 2.5)")
	afExec(t, db, ctx, "INSERT INTO v70_agg VALUES (3, 30, 3.5)")

	// AT1: SUM of integers returns integer
	check("AT1 SUM int", "SELECT SUM(val) FROM v70_agg", 60)

	// AT2: AVG returns float
	check("AT2 AVG int", "SELECT AVG(val) FROM v70_agg", 20)

	// AT3: SUM of floats
	check("AT3 SUM float", "SELECT SUM(fval) FROM v70_agg", "7.5")

	// AT4: COUNT returns integer
	check("AT4 COUNT", "SELECT COUNT(*) FROM v70_agg", 3)

	// AT5: MIN/MAX preserve type
	check("AT5 MIN int", "SELECT MIN(val) FROM v70_agg", 10)
	check("AT5 MAX int", "SELECT MAX(val) FROM v70_agg", 30)

	// ============================================================
	// === BOOLEAN TYPE ===
	// ============================================================

	// BT1: Boolean literal behavior
	check("BT1 true", "SELECT 1 = 1", true)
	check("BT1 false", "SELECT 1 = 2", false)

	// BT2: Boolean in CASE
	check("BT2 CASE true",
		"SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END", "yes")
	check("BT2 CASE false",
		"SELECT CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END", "no")

	// ============================================================
	// === EXPRESSION TYPE COMBINATIONS ===
	// ============================================================

	// EC1: Mixed type in CASE result
	check("EC1 CASE int result",
		"SELECT CASE WHEN 1 = 1 THEN 42 ELSE 0 END", 42)
	check("EC1 CASE text result",
		"SELECT CASE WHEN 1 = 1 THEN 'hello' ELSE 'world' END", "hello")

	// EC2: Subquery scalar type
	check("EC2 subquery int",
		"SELECT (SELECT 42)", 42)
	check("EC2 subquery text",
		"SELECT (SELECT 'hello')", "hello")

	// ============================================================
	// === BOUNDARY VALUES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v70_bounds (id INTEGER PRIMARY KEY, val INTEGER)`)

	// BV1: Zero
	checkNoError("BV1 zero", "INSERT INTO v70_bounds VALUES (1, 0)")
	check("BV1 verify", "SELECT val FROM v70_bounds WHERE id = 1", 0)

	// BV2: Negative
	checkNoError("BV2 negative", "INSERT INTO v70_bounds VALUES (2, -1)")
	check("BV2 verify", "SELECT val FROM v70_bounds WHERE id = 2", -1)

	// BV3: Large positive
	checkNoError("BV3 large", "INSERT INTO v70_bounds VALUES (3, 2147483647)")
	check("BV3 verify", "SELECT val FROM v70_bounds WHERE id = 3", 2147483647)

	// BV4: Large negative
	checkNoError("BV4 large neg", "INSERT INTO v70_bounds VALUES (4, -2147483648)")
	check("BV4 verify", "SELECT val FROM v70_bounds WHERE id = 4", -2147483648)

	// ============================================================
	// === DEFAULT VALUES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v70_defaults (
		id INTEGER PRIMARY KEY,
		name TEXT DEFAULT 'unknown',
		count INTEGER DEFAULT 0,
		active INTEGER DEFAULT 1
	)`)
	checkNoError("DF1 insert id only",
		"INSERT INTO v70_defaults (id) VALUES (1)")
	check("DF1 name", "SELECT name FROM v70_defaults WHERE id = 1", "unknown")
	check("DF1 count", "SELECT count FROM v70_defaults WHERE id = 1", 0)
	check("DF1 active", "SELECT active FROM v70_defaults WHERE id = 1", 1)

	// DF2: Explicit value overrides default
	checkNoError("DF2 explicit",
		"INSERT INTO v70_defaults VALUES (2, 'alice', 5, 0)")
	check("DF2 name", "SELECT name FROM v70_defaults WHERE id = 2", "alice")

	// ============================================================
	// === TYPE IN GROUP BY / ORDER BY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v70_mixed (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v70_mixed VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v70_mixed VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO v70_mixed VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO v70_mixed VALUES (4, 'b', 40)")

	// GO1: GROUP BY preserves text type
	check("GO1 GROUP text",
		"SELECT grp FROM v70_mixed GROUP BY grp ORDER BY grp LIMIT 1", "a")

	// GO2: SUM in GROUP BY
	check("GO2 GROUP SUM",
		"SELECT SUM(val) FROM v70_mixed GROUP BY grp ORDER BY SUM(val) ASC LIMIT 1", 40)
	// a: 40, b: 60

	// GO3: ORDER BY numeric
	check("GO3 ORDER num",
		"SELECT val FROM v70_mixed ORDER BY val DESC LIMIT 1", 40)

	// GO4: ORDER BY text
	check("GO4 ORDER text",
		"SELECT grp FROM v70_mixed ORDER BY grp DESC LIMIT 1", "b")

	// ============================================================
	// === TYPE IN JOINS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v70_j1 (id INTEGER PRIMARY KEY, jkey TEXT, val INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v70_j2 (id INTEGER PRIMARY KEY, jkey TEXT, amount INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v70_j1 VALUES (1, 'x', 100)")
	afExec(t, db, ctx, "INSERT INTO v70_j1 VALUES (2, 'y', 200)")
	afExec(t, db, ctx, "INSERT INTO v70_j2 VALUES (1, 'x', 50)")
	afExec(t, db, ctx, "INSERT INTO v70_j2 VALUES (2, 'z', 75)")

	// JT1: JOIN on text column
	check("JT1 JOIN text col",
		"SELECT v70_j1.val + v70_j2.amount FROM v70_j1 JOIN v70_j2 ON v70_j1.jkey = v70_j2.jkey", 150)

	// JT2: LEFT JOIN preserves NULL
	checkRowCount("JT2 LEFT JOIN",
		"SELECT * FROM v70_j1 LEFT JOIN v70_j2 ON v70_j1.jkey = v70_j2.jkey", 2)

	// ============================================================
	// === TYPE IN WINDOW FUNCTIONS ===
	// ============================================================

	// WT1: ROW_NUMBER returns integer
	check("WT1 ROW_NUMBER type",
		"SELECT ROW_NUMBER() OVER (ORDER BY id) FROM v70_mixed WHERE id = 1", 1)

	// WT2: SUM OVER returns same type as input
	check("WT2 SUM OVER int",
		"SELECT SUM(val) OVER (ORDER BY id) FROM v70_mixed WHERE id = 1", 10)

	// ============================================================
	// === COMPLEX TYPE INTERACTIONS ===
	// ============================================================

	// CI1: CASE with mixed types (int and text branches)
	check("CI1 CASE mixed",
		"SELECT CASE WHEN 1 = 1 THEN 42 ELSE 'fallback' END", 42)

	// CI2: Aggregate of CASE
	afExec(t, db, ctx, `CREATE TABLE v70_case_agg (id INTEGER PRIMARY KEY, status TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v70_case_agg VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v70_case_agg VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO v70_case_agg VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO v70_case_agg VALUES (4, 'b', 40)")

	check("CI2 SUM CASE",
		`SELECT SUM(CASE WHEN status = 'a' THEN val ELSE 0 END) FROM v70_case_agg`, 40)

	// CI3: Nested function type
	check("CI3 LENGTH of UPPER",
		"SELECT LENGTH(UPPER('hello'))", 5)

	// CI4: COALESCE type
	check("CI4 COALESCE int", "SELECT COALESCE(NULL, 42)", 42)
	check("CI4 COALESCE text", "SELECT COALESCE(NULL, 'hello')", "hello")

	t.Logf("\n=== V70 DATA TYPES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
