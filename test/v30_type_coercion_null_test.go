package test

import (
	"fmt"
	"testing"
)

func TestV30TypeCoercionAndNulls(t *testing.T) {
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

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
			return
		}
		pass++
	}
	_ = checkError

	// ============================================================
	// === NULL ARITHMETIC ===
	// ============================================================
	check("NULL + number", "SELECT NULL + 5", "<nil>")
	check("NULL * number", "SELECT NULL * 5", "<nil>")
	check("NULL concatenation", "SELECT NULL || 'hello'", "<nil>")

	// ============================================================
	// === TYPE COERCION IN COMPARISONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE coerce (id INTEGER PRIMARY KEY, int_val INTEGER, text_val TEXT)")
	afExec(t, db, ctx, "INSERT INTO coerce VALUES (1, 42, '42')")
	afExec(t, db, ctx, "INSERT INTO coerce VALUES (2, 100, '100')")

	// String to integer comparison
	checkRowCount("String-int comparison", "SELECT * FROM coerce WHERE text_val = '42'", 1)
	checkRowCount("Int column comparison", "SELECT * FROM coerce WHERE int_val = 42", 1)

	// ============================================================
	// === NULL IN AGGREGATES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE null_agg (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO null_agg VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO null_agg VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO null_agg VALUES (3, 20)")
	afExec(t, db, ctx, "INSERT INTO null_agg VALUES (4, NULL)")

	check("COUNT(*) includes NULLs", "SELECT COUNT(*) FROM null_agg", 4)
	check("COUNT(col) excludes NULLs", "SELECT COUNT(val) FROM null_agg", 2)
	check("SUM ignores NULLs", "SELECT SUM(val) FROM null_agg", 30)
	check("AVG ignores NULLs", "SELECT AVG(val) FROM null_agg", 15)
	check("MIN ignores NULLs", "SELECT MIN(val) FROM null_agg", 10)
	check("MAX ignores NULLs", "SELECT MAX(val) FROM null_agg", 20)

	// ============================================================
	// === NULL IN GROUP BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE null_gb (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO null_gb VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO null_gb VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO null_gb VALUES (3, NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO null_gb VALUES (4, NULL, 40)")
	afExec(t, db, ctx, "INSERT INTO null_gb VALUES (5, 'B', 50)")

	// NULLs should form their own group
	checkRowCount("GROUP BY with NULLs", "SELECT grp, SUM(val) FROM null_gb GROUP BY grp", 3) // A, NULL, B

	// ============================================================
	// === NULL IN ORDER BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE null_ob (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO null_ob VALUES (1, 30)")
	afExec(t, db, ctx, "INSERT INTO null_ob VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO null_ob VALUES (3, 10)")
	afExec(t, db, ctx, "INSERT INTO null_ob VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO null_ob VALUES (5, 20)")

	// All rows should be returned
	checkRowCount("ORDER BY with NULLs", "SELECT * FROM null_ob ORDER BY val", 5)

	// Non-null values should be ordered correctly
	check("ORDER BY ASC first non-null",
		"SELECT val FROM null_ob WHERE val IS NOT NULL ORDER BY val ASC LIMIT 1", 10)
	check("ORDER BY DESC first non-null",
		"SELECT val FROM null_ob WHERE val IS NOT NULL ORDER BY val DESC LIMIT 1", 30)

	// ============================================================
	// === NULL IN JOINS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE null_j1 (id INTEGER PRIMARY KEY, key_col INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE null_j2 (id INTEGER PRIMARY KEY, key_col INTEGER, val TEXT)")

	afExec(t, db, ctx, "INSERT INTO null_j1 VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO null_j1 VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO null_j1 VALUES (3, 2)")

	afExec(t, db, ctx, "INSERT INTO null_j2 VALUES (1, 1, 'match1')")
	afExec(t, db, ctx, "INSERT INTO null_j2 VALUES (2, NULL, 'nullkey')")
	afExec(t, db, ctx, "INSERT INTO null_j2 VALUES (3, 2, 'match2')")

	// NULL != NULL in JOIN, so null keys shouldn't match
	checkRowCount("JOIN with NULL keys",
		"SELECT * FROM null_j1 JOIN null_j2 ON null_j1.key_col = null_j2.key_col", 2)

	// LEFT JOIN preserves NULL key rows
	checkRowCount("LEFT JOIN with NULL keys",
		"SELECT * FROM null_j1 LEFT JOIN null_j2 ON null_j1.key_col = null_j2.key_col", 3)

	// ============================================================
	// === EMPTY STRING vs NULL ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE empty_null (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO empty_null VALUES (1, '')")
	afExec(t, db, ctx, "INSERT INTO empty_null VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO empty_null VALUES (3, 'hello')")

	checkRowCount("Empty string IS NOT NULL", "SELECT * FROM empty_null WHERE val IS NOT NULL", 2) // '' and 'hello'
	checkRowCount("NULL IS NULL", "SELECT * FROM empty_null WHERE val IS NULL", 1)
	checkRowCount("Empty string = ''", "SELECT * FROM empty_null WHERE val = ''", 1)

	// ============================================================
	// === BOOLEAN EXPRESSIONS ===
	// ============================================================
	check("TRUE literal", "SELECT 1 = 1", true)
	check("FALSE literal", "SELECT 1 = 2", false)
	check("AND logic TT", "SELECT 1 = 1 AND 2 = 2", true)
	check("AND logic TF", "SELECT 1 = 1 AND 1 = 2", false)
	check("OR logic TF", "SELECT 1 = 1 OR 1 = 2", true)
	check("OR logic FF", "SELECT 1 = 2 OR 1 = 3", false)
	check("NOT logic", "SELECT NOT (1 = 2)", true)

	// ============================================================
	// === COMPLEX BOOLEAN IN WHERE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE bool_where (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO bool_where VALUES (1, 10, 20, 'X')")
	afExec(t, db, ctx, "INSERT INTO bool_where VALUES (2, 30, 40, 'Y')")
	afExec(t, db, ctx, "INSERT INTO bool_where VALUES (3, 50, 60, 'X')")
	afExec(t, db, ctx, "INSERT INTO bool_where VALUES (4, 70, 80, 'Z')")

	checkRowCount("NOT in WHERE",
		"SELECT * FROM bool_where WHERE NOT (c = 'X')", 2) // Y, Z

	checkRowCount("Complex OR AND NOT",
		"SELECT * FROM bool_where WHERE (a > 20 AND c = 'X') OR NOT (b < 70)", 2) // id=3 (50,X), id=4 (80>=70)

	// ============================================================
	// === INSERT DEFAULT VALUES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE def_vals (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT DEFAULT 'unknown', active INTEGER DEFAULT 1)")
	checkNoError("Insert with all defaults",
		"INSERT INTO def_vals (name) VALUES ('Alice')")
	check("Default active value", "SELECT active FROM def_vals WHERE name = 'Alice'", 1)

	checkNoError("Insert overriding default",
		"INSERT INTO def_vals (name, active) VALUES ('Bob', 0)")
	check("Overridden default", "SELECT active FROM def_vals WHERE name = 'Bob'", 0)

	// ============================================================
	// === MULTIPLE WHERE CONDITIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE multi_where (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)")
	afExec(t, db, ctx, "INSERT INTO multi_where VALUES (1, 10, 'apple', 100)")
	afExec(t, db, ctx, "INSERT INTO multi_where VALUES (2, 20, 'banana', 200)")
	afExec(t, db, ctx, "INSERT INTO multi_where VALUES (3, 30, 'cherry', 300)")
	afExec(t, db, ctx, "INSERT INTO multi_where VALUES (4, 40, 'date', 400)")

	checkRowCount("Multiple AND conditions",
		"SELECT * FROM multi_where WHERE a >= 20 AND b LIKE 'b%' AND c < 500", 1) // banana

	checkRowCount("BETWEEN and LIKE combined",
		"SELECT * FROM multi_where WHERE a BETWEEN 10 AND 30 AND b LIKE '%e%'", 2) // apple, cherry

	// ============================================================
	// === UPDATE WITH NULL ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE upd_null (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO upd_null VALUES (1, 42)")

	checkNoError("UPDATE to NULL", "UPDATE upd_null SET val = NULL WHERE id = 1")
	checkRowCount("Updated to NULL", "SELECT * FROM upd_null WHERE val IS NULL", 1)

	checkNoError("UPDATE from NULL", "UPDATE upd_null SET val = 99 WHERE id = 1")
	check("Updated from NULL", "SELECT val FROM upd_null WHERE id = 1", 99)

	// ============================================================
	// === DELETE WITH COMPLEX WHERE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE del_complex (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO del_complex VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO del_complex VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO del_complex VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO del_complex VALUES (4, 'C', 40)")
	afExec(t, db, ctx, "INSERT INTO del_complex VALUES (5, 'B', 50)")

	checkNoError("DELETE with complex WHERE",
		"DELETE FROM del_complex WHERE cat = 'A' OR val > 40")
	checkRowCount("After complex DELETE", "SELECT * FROM del_complex", 2) // id=2 (B,20), id=4 (C,40)

	// ============================================================
	// === SELECT WITH EXPRESSIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE sel_expr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO sel_expr VALUES (1, 10, 20)")
	afExec(t, db, ctx, "INSERT INTO sel_expr VALUES (2, 30, 40)")

	check("SELECT with addition", "SELECT a + b FROM sel_expr WHERE id = 1", 30)
	check("SELECT with multiplication", "SELECT a * b FROM sel_expr WHERE id = 2", 1200)
	check("SELECT with subtraction", "SELECT b - a FROM sel_expr WHERE id = 1", 10)

	// ============================================================
	// === RECURSIVE CTE EDGE CASES ===
	// ============================================================
	check("Recursive CTE single iteration",
		"WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 1) SELECT COUNT(*) FROM seq", 1)

	check("Recursive CTE powers of 2",
		"WITH RECURSIVE pow(n, val) AS (SELECT 0, 1 UNION ALL SELECT n+1, val*2 FROM pow WHERE n < 10) SELECT MAX(val) FROM pow", 1024)

	// ============================================================
	// === NESTED SUBQUERY IN WHERE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE nsq_outer (id INTEGER PRIMARY KEY, dept TEXT)")
	afExec(t, db, ctx, "CREATE TABLE nsq_inner (id INTEGER PRIMARY KEY, dept TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO nsq_outer VALUES (1, 'Eng')")
	afExec(t, db, ctx, "INSERT INTO nsq_outer VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO nsq_inner VALUES (1, 'Eng', 100)")
	afExec(t, db, ctx, "INSERT INTO nsq_inner VALUES (2, 'Eng', 200)")
	afExec(t, db, ctx, "INSERT INTO nsq_inner VALUES (3, 'Sales', 50)")

	check("Nested subquery SUM",
		"SELECT dept FROM nsq_outer WHERE (SELECT SUM(val) FROM nsq_inner WHERE nsq_inner.dept = nsq_outer.dept) > 100",
		"Eng") // Eng: 300 > 100, Sales: 50 < 100

	checkRowCount("Nested subquery with COUNT",
		"SELECT * FROM nsq_outer WHERE (SELECT COUNT(*) FROM nsq_inner WHERE nsq_inner.dept = nsq_outer.dept) >= 1", 2)

	// ============================================================
	// === MIXED AGGREGATES AND NON-AGGREGATES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE mix_agg (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO mix_agg VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO mix_agg VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO mix_agg VALUES (3, 'B', 30)")

	// A: 10+20=30, B: 30, same SUM - use ASC to get deterministic result
	check("GROUP BY with mixed cols",
		"SELECT grp, SUM(val) FROM mix_agg GROUP BY grp ORDER BY SUM(val) ASC, grp ASC LIMIT 1", "A")

	// GROUP BY with all aggregate functions
	check("Multiple aggs in GROUP BY",
		"SELECT COUNT(*) FROM mix_agg GROUP BY grp ORDER BY COUNT(*) DESC LIMIT 1", 2) // Group A

	// ============================================================
	// === CASE IN WHERE CLAUSE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE case_where (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO case_where VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO case_where VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO case_where VALUES (3, 30)")

	checkRowCount("CASE in WHERE",
		"SELECT * FROM case_where WHERE CASE WHEN val > 15 THEN 1 ELSE 0 END = 1", 2)

	// ============================================================
	// === INSERT WITH SELECT AND AGGREGATE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE ins_agg_src (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE ins_agg_tgt (id INTEGER PRIMARY KEY AUTO_INCREMENT, grp TEXT, total INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ins_agg_src VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO ins_agg_src VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO ins_agg_src VALUES (3, 'B', 30)")

	checkNoError("INSERT...SELECT with GROUP BY",
		"INSERT INTO ins_agg_tgt (grp, total) SELECT grp, SUM(val) FROM ins_agg_src GROUP BY grp")
	checkRowCount("Inserted group rows", "SELECT * FROM ins_agg_tgt", 2)
	check("Group A total", "SELECT total FROM ins_agg_tgt WHERE grp = 'A'", 30)
	check("Group B total", "SELECT total FROM ins_agg_tgt WHERE grp = 'B'", 30)

	t.Logf("\n=== V30 TYPE COERCION & NULLS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
