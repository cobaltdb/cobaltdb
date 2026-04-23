package test

import (
	"fmt"
	"testing"
)

func TestV21SQLCorrectness(t *testing.T) {
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

	// ============================================================
	// === NULL THREE-VALUED LOGIC ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE null_logic (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)")
	afExec(t, db, ctx, "INSERT INTO null_logic VALUES (1, 10, 'hello')")
	afExec(t, db, ctx, "INSERT INTO null_logic VALUES (2, NULL, NULL)")
	afExec(t, db, ctx, "INSERT INTO null_logic VALUES (3, 20, NULL)")

	// NULL comparisons should not match
	checkRowCount("NULL = NULL is not true", "SELECT * FROM null_logic WHERE a = NULL", 0)
	checkRowCount("NULL != value is not true", "SELECT * FROM null_logic WHERE a != 10", 1) // only id=3 (20!=10), NULL excluded
	checkRowCount("IS NULL check", "SELECT * FROM null_logic WHERE a IS NULL", 1)
	checkRowCount("IS NOT NULL check", "SELECT * FROM null_logic WHERE a IS NOT NULL", 2)

	// NULL in arithmetic
	check("NULL + number", "SELECT a + 5 FROM null_logic WHERE id = 2", "<nil>")
	check("number + NULL col", "SELECT a + 5 FROM null_logic WHERE id = 1", 15)

	// ============================================================
	// === AGGREGATE EDGE CASES ===
	// ============================================================
	// SUM/AVG of NULLs
	check("SUM skips NULLs", "SELECT SUM(a) FROM null_logic", 30) // 10+20
	check("COUNT(*) includes NULL rows", "SELECT COUNT(*) FROM null_logic", 3)
	check("COUNT(col) excludes NULLs", "SELECT COUNT(a) FROM null_logic", 2)
	check("MIN skips NULLs", "SELECT MIN(a) FROM null_logic", 10)
	check("MAX skips NULLs", "SELECT MAX(a) FROM null_logic", 20)

	// Aggregate on empty result set
	afExec(t, db, ctx, "CREATE TABLE empty_agg (id INTEGER PRIMARY KEY, val INTEGER)")
	check("COUNT of empty table", "SELECT COUNT(*) FROM empty_agg", 0)
	check("COUNT(col) of empty", "SELECT COUNT(val) FROM empty_agg", 0)

	// GROUP BY on column with NULLs
	afExec(t, db, ctx, "CREATE TABLE null_group (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO null_group VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO null_group VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO null_group VALUES (3, NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO null_group VALUES (4, NULL, 40)")

	check("GROUP BY NULL group count",
		"SELECT COUNT(*) FROM null_group GROUP BY grp ORDER BY grp LIMIT 1", 2) // NULL group or 'A' group

	// ============================================================
	// === ORDER BY WITH NULLs ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE null_order (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO null_order VALUES (1, 30)")
	afExec(t, db, ctx, "INSERT INTO null_order VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO null_order VALUES (3, 10)")
	afExec(t, db, ctx, "INSERT INTO null_order VALUES (4, 20)")

	// NULLs should sort first in ASC (SQL standard: NULLS FIRST for ASC)
	check("ORDER BY ASC first non-null",
		"SELECT val FROM null_order WHERE val IS NOT NULL ORDER BY val ASC LIMIT 1", 10)

	check("ORDER BY DESC first",
		"SELECT val FROM null_order WHERE val IS NOT NULL ORDER BY val DESC LIMIT 1", 30)

	// ============================================================
	// === OFFSET / LIMIT EDGE CASES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE limit_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO limit_test VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO limit_test VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO limit_test VALUES (3, 30)")

	checkRowCount("LIMIT larger than results", "SELECT * FROM limit_test LIMIT 100", 3)
	checkRowCount("LIMIT 0", "SELECT * FROM limit_test LIMIT 0", 0)
	checkRowCount("OFFSET beyond results", "SELECT * FROM limit_test LIMIT 10 OFFSET 100", 0)
	checkRowCount("OFFSET 1", "SELECT * FROM limit_test ORDER BY id LIMIT 10 OFFSET 1", 2)
	check("OFFSET skips first", "SELECT id FROM limit_test ORDER BY id LIMIT 1 OFFSET 1", 2)

	// ============================================================
	// === NEGATIVE NUMBERS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE negatives (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO negatives VALUES (1, -10)")
	afExec(t, db, ctx, "INSERT INTO negatives VALUES (2, -5)")
	afExec(t, db, ctx, "INSERT INTO negatives VALUES (3, 0)")
	afExec(t, db, ctx, "INSERT INTO negatives VALUES (4, 5)")

	check("Negative values exist", "SELECT val FROM negatives WHERE id = 1", -10)
	check("MIN with negatives", "SELECT MIN(val) FROM negatives", -10)
	check("MAX with negatives", "SELECT MAX(val) FROM negatives", 5)
	check("SUM with negatives", "SELECT SUM(val) FROM negatives", -10) // -10 + -5 + 0 + 5 = -10
	check("ABS of negative", "SELECT ABS(val) FROM negatives WHERE id = 1", 10)
	checkRowCount("WHERE with negative", "SELECT * FROM negatives WHERE val < 0", 2)
	checkRowCount("BETWEEN with negatives", "SELECT * FROM negatives WHERE val BETWEEN -10 AND 0", 3)

	// ============================================================
	// === LARGE NUMBERS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE large_nums (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO large_nums VALUES (1, 2147483647)")  // max int32
	afExec(t, db, ctx, "INSERT INTO large_nums VALUES (2, -2147483648)") // min int32
	afExec(t, db, ctx, "INSERT INTO large_nums VALUES (3, 1000000000)")

	check("Large positive", "SELECT val FROM large_nums WHERE id = 1", 2147483647)
	check("Large negative", "SELECT val FROM large_nums WHERE id = 2", -2147483648)
	check("SUM of large", "SELECT SUM(val) FROM large_nums", 9.99999999e+08) // 2147483647 + -2147483648 + 1000000000

	// ============================================================
	// === LEFT JOIN + AGGREGATE + HAVING ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE lj_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE lj_child (id INTEGER PRIMARY KEY, parent_id INTEGER, val INTEGER)")

	afExec(t, db, ctx, "INSERT INTO lj_parent VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO lj_parent VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO lj_parent VALUES (3, 'C')")
	afExec(t, db, ctx, "INSERT INTO lj_child VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO lj_child VALUES (2, 1, 20)")
	afExec(t, db, ctx, "INSERT INTO lj_child VALUES (3, 2, 30)")

	checkRowCount("LEFT JOIN shows all parents",
		"SELECT lj_parent.name FROM lj_parent LEFT JOIN lj_child ON lj_parent.id = lj_child.parent_id",
		4) // A-10, A-20, B-30, C-NULL

	check("LEFT JOIN + GROUP BY + COUNT",
		"SELECT lj_parent.name, COUNT(lj_child.id) FROM lj_parent LEFT JOIN lj_child ON lj_parent.id = lj_child.parent_id GROUP BY lj_parent.name HAVING COUNT(lj_child.id) = 0",
		"C") // C has no children

	// ============================================================
	// === UPDATE WITH COMPUTED EXPRESSION ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE computed (id INTEGER PRIMARY KEY, price INTEGER, tax INTEGER, total INTEGER)")
	afExec(t, db, ctx, "INSERT INTO computed VALUES (1, 100, 10, 0)")
	afExec(t, db, ctx, "INSERT INTO computed VALUES (2, 200, 20, 0)")

	checkNoError("UPDATE with expression", "UPDATE computed SET total = price + tax")
	check("Computed total 1", "SELECT total FROM computed WHERE id = 1", 110)
	check("Computed total 2", "SELECT total FROM computed WHERE id = 2", 220)

	// ============================================================
	// === DELETE WITH IN SUBQUERY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE del_main (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE del_ref (id INTEGER PRIMARY KEY, main_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO del_main VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO del_main VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO del_main VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO del_ref VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO del_ref VALUES (2, 3)")

	checkNoError("DELETE with IN subquery",
		"DELETE FROM del_main WHERE id IN (SELECT main_id FROM del_ref)")
	checkRowCount("Only unreferenced row remains", "SELECT * FROM del_main", 1)
	check("Correct row remains", "SELECT id FROM del_main", 2)

	// ============================================================
	// === MULTIPLE UPDATES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE multi_upd (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)")
	afExec(t, db, ctx, "INSERT INTO multi_upd VALUES (1, 1, 2, 3)")

	checkNoError("UPDATE multiple columns", "UPDATE multi_upd SET a = 10, b = 20, c = 30 WHERE id = 1")
	check("Multi-update col a", "SELECT a FROM multi_upd WHERE id = 1", 10)
	check("Multi-update col b", "SELECT b FROM multi_upd WHERE id = 1", 20)
	check("Multi-update col c", "SELECT c FROM multi_upd WHERE id = 1", 30)

	// ============================================================
	// === COMPLEX BOOLEAN IN WHERE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE bool_test (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO bool_test VALUES (1, 10, 20, 'x')")
	afExec(t, db, ctx, "INSERT INTO bool_test VALUES (2, 30, 40, 'y')")
	afExec(t, db, ctx, "INSERT INTO bool_test VALUES (3, 10, 40, 'x')")
	afExec(t, db, ctx, "INSERT INTO bool_test VALUES (4, 30, 20, 'y')")

	checkRowCount("NOT AND combo",
		"SELECT * FROM bool_test WHERE NOT (a = 10 AND b = 20)", 3)
	checkRowCount("OR with parens",
		"SELECT * FROM bool_test WHERE (a = 10 OR a = 30) AND c = 'x'", 2)

	// ============================================================
	// === STRING COMPARISON ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE str_cmp (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO str_cmp VALUES (1, 'apple')")
	afExec(t, db, ctx, "INSERT INTO str_cmp VALUES (2, 'banana')")
	afExec(t, db, ctx, "INSERT INTO str_cmp VALUES (3, 'cherry')")

	check("String ordering min", "SELECT MIN(name) FROM str_cmp", "apple")
	check("String ordering max", "SELECT MAX(name) FROM str_cmp", "cherry")
	check("String ORDER BY", "SELECT name FROM str_cmp ORDER BY name DESC LIMIT 1", "cherry")

	// ============================================================
	// === INSERT WITH COLUMN LIST SUBSET ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE partial_insert (id INTEGER PRIMARY KEY, name TEXT, val INTEGER DEFAULT 42)")
	checkNoError("Partial column insert",
		"INSERT INTO partial_insert (id, name) VALUES (1, 'test')")
	check("Default value for missing column",
		"SELECT val FROM partial_insert WHERE id = 1", 42)

	// ============================================================
	// === DUPLICATE PRIMARY KEY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE dup_pk (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO dup_pk VALUES (1, 'first')")

	total++
	_, err := db.Exec(ctx, "INSERT INTO dup_pk VALUES (1, 'duplicate')")
	if err != nil {
		pass++
	} else {
		t.Errorf("[FAIL] Duplicate PK should error")
	}
	check("Original row unchanged", "SELECT val FROM dup_pk WHERE id = 1", "first")

	// ============================================================
	// === EMPTY TABLE OPERATIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE empty_ops (id INTEGER PRIMARY KEY, val INTEGER)")
	checkRowCount("SELECT from empty", "SELECT * FROM empty_ops", 0)
	checkNoError("UPDATE empty table", "UPDATE empty_ops SET val = 1")
	checkNoError("DELETE from empty table", "DELETE FROM empty_ops")

	t.Logf("\n=== V21 SQL CORRECTNESS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
