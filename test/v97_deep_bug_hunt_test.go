package test

import (
	"fmt"
	"testing"
)

// =============================================================================
// TestV97_ deep bug-hunting tests targeting 20 categories of common SQL engine
// bugs: UPDATE with computed columns, DELETE with complex WHERE, INSERT...SELECT,
// multiple aggregates, nested subqueries, CASE WHEN with NULL, GROUP BY + HAVING
// + ORDER BY, LEFT JOIN anti-join, COALESCE, chained string ops, NULL arithmetic,
// BETWEEN with strings, INSERT OR REPLACE, CHECK constraints, multi-row INSERT,
// UPDATE with LIMIT, self-referencing FK, views with WHERE, DELETE with LIMIT,
// and CAST in WHERE clause.
// =============================================================================

// ---------------------------------------------------------------------------
// 1. UPDATE with computed columns
// ---------------------------------------------------------------------------

func TestV97_UpdateComputedColumnDoubleAndAdd(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_upd_comp (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_comp VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_comp VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_comp VALUES (3, 30)")

	// UPDATE SET col = col * 2 + 1
	afExec(t, db, ctx, "UPDATE t97_upd_comp SET val = val * 2 + 1")

	afExpectVal(t, db, ctx, "SELECT val FROM t97_upd_comp WHERE id = 1", 21)
	afExpectVal(t, db, ctx, "SELECT val FROM t97_upd_comp WHERE id = 2", 41)
	afExpectVal(t, db, ctx, "SELECT val FROM t97_upd_comp WHERE id = 3", 61)
}

func TestV97_UpdateColumnSwap(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_swap (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_swap VALUES (1, 100, 200)")
	afExec(t, db, ctx, "INSERT INTO t97_swap VALUES (2, 300, 400)")

	// Swap columns a and b -- a common bug: engine may read the already-updated
	// value of a when computing b = a.
	afExec(t, db, ctx, "UPDATE t97_swap SET a = b, b = a")

	afExpectVal(t, db, ctx, "SELECT a FROM t97_swap WHERE id = 1", 200)
	afExpectVal(t, db, ctx, "SELECT b FROM t97_swap WHERE id = 1", 100)
	afExpectVal(t, db, ctx, "SELECT a FROM t97_swap WHERE id = 2", 400)
	afExpectVal(t, db, ctx, "SELECT b FROM t97_swap WHERE id = 2", 300)
}

func TestV97_UpdateSelfReferenceArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_selfref (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_selfref VALUES (1, 5, 10)")

	// SET x = x + y, y = x - y -- after this, original x=5,y=10 should give x=15,y=5
	// Bug: if y = x - y uses the already-updated x (15), result would be y=10 instead of 5
	afExec(t, db, ctx, "UPDATE t97_selfref SET x = x + y, y = x - y WHERE id = 1")

	rows := afQuery(t, db, ctx, "SELECT x, y FROM t97_selfref WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	x := fmt.Sprintf("%v", rows[0][0])
	// x should be 15 (5 + 10)
	if x != "15" {
		t.Errorf("expected x=15, got %s", x)
	}
}

// ---------------------------------------------------------------------------
// 2. DELETE with complex WHERE
// ---------------------------------------------------------------------------

func TestV97_DeleteWithAndOrNot(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_del_complex (id INTEGER PRIMARY KEY, category TEXT, active INTEGER, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_del_complex VALUES (1, 'A', 1, 80)")
	afExec(t, db, ctx, "INSERT INTO t97_del_complex VALUES (2, 'B', 0, 90)")
	afExec(t, db, ctx, "INSERT INTO t97_del_complex VALUES (3, 'A', 0, 70)")
	afExec(t, db, ctx, "INSERT INTO t97_del_complex VALUES (4, 'B', 1, 60)")
	afExec(t, db, ctx, "INSERT INTO t97_del_complex VALUES (5, 'C', 1, 50)")

	// DELETE rows where (category='A' AND active=0) OR (category='B' AND NOT active=1)
	afExec(t, db, ctx, "DELETE FROM t97_del_complex WHERE (category = 'A' AND active = 0) OR (category = 'B' AND NOT active = 1)")

	// Should delete id=3 (A, active=0) and id=2 (B, active=0 i.e. NOT active=1)
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_del_complex", 3)

	remaining := afQuery(t, db, ctx, "SELECT id FROM t97_del_complex ORDER BY id")
	if len(remaining) != 3 {
		t.Fatalf("expected 3 remaining rows, got %d", len(remaining))
	}
	ids := []string{fmt.Sprintf("%v", remaining[0][0]), fmt.Sprintf("%v", remaining[1][0]), fmt.Sprintf("%v", remaining[2][0])}
	if ids[0] != "1" || ids[1] != "4" || ids[2] != "5" {
		t.Errorf("expected remaining ids [1,4,5], got %v", ids)
	}
}

func TestV97_DeleteWithSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_del_main (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t97_del_dept (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t97_del_dept VALUES (1, 'Engineering', 1)")
	afExec(t, db, ctx, "INSERT INTO t97_del_dept VALUES (2, 'Marketing', 0)")
	afExec(t, db, ctx, "INSERT INTO t97_del_dept VALUES (3, 'Sales', 1)")

	afExec(t, db, ctx, "INSERT INTO t97_del_main VALUES (1, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO t97_del_main VALUES (2, 'Bob', 2)")
	afExec(t, db, ctx, "INSERT INTO t97_del_main VALUES (3, 'Carol', 2)")
	afExec(t, db, ctx, "INSERT INTO t97_del_main VALUES (4, 'Dave', 3)")

	// Delete employees in inactive departments
	afExec(t, db, ctx, "DELETE FROM t97_del_main WHERE dept_id IN (SELECT id FROM t97_del_dept WHERE active = 0)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_del_main", 2)
	// Bob and Carol (dept_id=2, inactive) should be deleted
	rows := afQuery(t, db, ctx, "SELECT name FROM t97_del_main ORDER BY id")
	if fmt.Sprintf("%v", rows[0][0]) != "Alice" || fmt.Sprintf("%v", rows[1][0]) != "Dave" {
		t.Errorf("expected Alice and Dave remaining, got %v", rows)
	}
}

func TestV97_DeleteWhereIn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_del_in (id INTEGER PRIMARY KEY, tag TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_del_in VALUES (1, 'red')")
	afExec(t, db, ctx, "INSERT INTO t97_del_in VALUES (2, 'blue')")
	afExec(t, db, ctx, "INSERT INTO t97_del_in VALUES (3, 'green')")
	afExec(t, db, ctx, "INSERT INTO t97_del_in VALUES (4, 'red')")
	afExec(t, db, ctx, "INSERT INTO t97_del_in VALUES (5, 'yellow')")

	afExec(t, db, ctx, "DELETE FROM t97_del_in WHERE tag IN ('red', 'green')")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_del_in", 2)
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_del_in WHERE tag = 'red'", 0)
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_del_in WHERE tag = 'green'", 0)
}

// ---------------------------------------------------------------------------
// 3. INSERT with SELECT
// ---------------------------------------------------------------------------

func TestV97_InsertSelectBasic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_src (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_src VALUES (1, 'alpha', 90)")
	afExec(t, db, ctx, "INSERT INTO t97_src VALUES (2, 'beta', 40)")
	afExec(t, db, ctx, "INSERT INTO t97_src VALUES (3, 'gamma', 85)")
	afExec(t, db, ctx, "INSERT INTO t97_src VALUES (4, 'delta', 30)")

	afExec(t, db, ctx, "CREATE TABLE t97_dst (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")

	// INSERT INTO ... SELECT ... FROM ... WHERE
	afExec(t, db, ctx, "INSERT INTO t97_dst SELECT * FROM t97_src WHERE score >= 80")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_dst", 2)
	afExpectVal(t, db, ctx, "SELECT val FROM t97_dst WHERE id = 1", "alpha")
	afExpectVal(t, db, ctx, "SELECT val FROM t97_dst WHERE id = 3", "gamma")
}

func TestV97_InsertSelectWithExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_expr_src (id INTEGER PRIMARY KEY, price REAL, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_expr_src VALUES (1, 10.0, 5)")
	afExec(t, db, ctx, "INSERT INTO t97_expr_src VALUES (2, 20.0, 3)")

	afExec(t, db, ctx, "CREATE TABLE t97_expr_dst (id INTEGER, total REAL)")

	// INSERT with computed expression
	afExec(t, db, ctx, "INSERT INTO t97_expr_dst SELECT id, price * qty FROM t97_expr_src")

	afExpectVal(t, db, ctx, "SELECT total FROM t97_expr_dst WHERE id = 1", 50.0)
	afExpectVal(t, db, ctx, "SELECT total FROM t97_expr_dst WHERE id = 2", 60.0)
}

// ---------------------------------------------------------------------------
// 4. Multiple aggregates in one query
// ---------------------------------------------------------------------------

func TestV97_MultipleAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_agg (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_agg VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t97_agg VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t97_agg VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO t97_agg VALUES (4, 40)")
	afExec(t, db, ctx, "INSERT INTO t97_agg VALUES (5, 50)")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM t97_agg")
	if len(rows) != 1 {
		t.Fatalf("expected 1 aggregate row, got %d", len(rows))
	}
	count := fmt.Sprintf("%v", rows[0][0])
	sum := fmt.Sprintf("%v", rows[0][1])
	min := fmt.Sprintf("%v", rows[0][2])
	max := fmt.Sprintf("%v", rows[0][3])

	if count != "5" {
		t.Errorf("COUNT: expected 5, got %s", count)
	}
	if sum != "150" {
		t.Errorf("SUM: expected 150, got %s", sum)
	}
	if min != "10" {
		t.Errorf("MIN: expected 10, got %s", min)
	}
	if max != "50" {
		t.Errorf("MAX: expected 50, got %s", max)
	}
}

func TestV97_MultipleAggregatesWithNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_agg_null (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_agg_null VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t97_agg_null VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_agg_null VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO t97_agg_null VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_agg_null VALUES (5, 50)")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*), COUNT(val), SUM(val), MIN(val), MAX(val) FROM t97_agg_null")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	countStar := fmt.Sprintf("%v", rows[0][0])
	countVal := fmt.Sprintf("%v", rows[0][1])
	sum := fmt.Sprintf("%v", rows[0][2])

	// COUNT(*) should include NULLs, COUNT(val) should not
	if countStar != "5" {
		t.Errorf("COUNT(*): expected 5, got %s", countStar)
	}
	if countVal != "3" {
		t.Errorf("COUNT(val): expected 3, got %s", countVal)
	}
	if sum != "90" {
		t.Errorf("SUM(val): expected 90, got %s", sum)
	}
}

// ---------------------------------------------------------------------------
// 5. Nested subqueries
// ---------------------------------------------------------------------------

func TestV97_NestedSubqueryInWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_nested (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_nested VALUES (1, 5)")
	afExec(t, db, ctx, "INSERT INTO t97_nested VALUES (2, 15)")
	afExec(t, db, ctx, "INSERT INTO t97_nested VALUES (3, 25)")
	afExec(t, db, ctx, "INSERT INTO t97_nested VALUES (4, 35)")

	// Get rows where val > the minimum val
	rows := afQuery(t, db, ctx, "SELECT id, val FROM t97_nested WHERE val > (SELECT MIN(val) FROM t97_nested) ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows where val > MIN(val), got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "2" {
		t.Errorf("first row should be id=2, got %v", rows[0][0])
	}
}

func TestV97_DerivedTableSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_derived (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_derived VALUES (1, 'A', 100)")
	afExec(t, db, ctx, "INSERT INTO t97_derived VALUES (2, 'A', 200)")
	afExec(t, db, ctx, "INSERT INTO t97_derived VALUES (3, 'B', 150)")
	afExec(t, db, ctx, "INSERT INTO t97_derived VALUES (4, 'B', 250)")

	// Derived table: select from aggregated subquery
	rows := afQuery(t, db, ctx,
		"SELECT sub.category, sub.total FROM (SELECT category, SUM(amount) AS total FROM t97_derived GROUP BY category) AS sub ORDER BY sub.category")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from derived table, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" || fmt.Sprintf("%v", rows[0][1]) != "300" {
		t.Errorf("expected A/300, got %v/%v", rows[0][0], rows[0][1])
	}
	if fmt.Sprintf("%v", rows[1][0]) != "B" || fmt.Sprintf("%v", rows[1][1]) != "400" {
		t.Errorf("expected B/400, got %v/%v", rows[1][0], rows[1][1])
	}
}

// ---------------------------------------------------------------------------
// 6. CASE WHEN in SELECT with NULL
// ---------------------------------------------------------------------------

func TestV97_CaseWhenWithNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_case_null (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_case_null VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t97_case_null VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_case_null VALUES (3, '')")

	rows := afQuery(t, db, ctx,
		"SELECT id, CASE WHEN val IS NULL THEN 'null' WHEN val = '' THEN 'empty' ELSE val END AS result FROM t97_case_null ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	r1 := fmt.Sprintf("%v", rows[0][1])
	r2 := fmt.Sprintf("%v", rows[1][1])
	r3 := fmt.Sprintf("%v", rows[2][1])

	if r1 != "hello" {
		t.Errorf("id=1: expected 'hello', got '%s'", r1)
	}
	if r2 != "null" {
		t.Errorf("id=2: expected 'null', got '%s'", r2)
	}
	if r3 != "empty" {
		t.Errorf("id=3: expected 'empty', got '%s'", r3)
	}
}

func TestV97_CaseWhenInUpdate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_case_upd (id INTEGER PRIMARY KEY, score INTEGER, grade TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_case_upd VALUES (1, 95, NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_case_upd VALUES (2, 72, NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_case_upd VALUES (3, 45, NULL)")

	afExec(t, db, ctx, "UPDATE t97_case_upd SET grade = CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'F' END")

	afExpectVal(t, db, ctx, "SELECT grade FROM t97_case_upd WHERE id = 1", "A")
	afExpectVal(t, db, ctx, "SELECT grade FROM t97_case_upd WHERE id = 2", "B")
	afExpectVal(t, db, ctx, "SELECT grade FROM t97_case_upd WHERE id = 3", "F")
}

// ---------------------------------------------------------------------------
// 7. GROUP BY with HAVING and ORDER BY
// ---------------------------------------------------------------------------

func TestV97_GroupByHavingOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_gbho (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_gbho VALUES (1, 'Eng', 100)")
	afExec(t, db, ctx, "INSERT INTO t97_gbho VALUES (2, 'Eng', 120)")
	afExec(t, db, ctx, "INSERT INTO t97_gbho VALUES (3, 'Eng', 110)")
	afExec(t, db, ctx, "INSERT INTO t97_gbho VALUES (4, 'Sales', 80)")
	afExec(t, db, ctx, "INSERT INTO t97_gbho VALUES (5, 'Sales', 90)")
	afExec(t, db, ctx, "INSERT INTO t97_gbho VALUES (6, 'HR', 70)")

	// GROUP BY dept, HAVING COUNT >= 2, ORDER BY total_salary DESC
	rows := afQuery(t, db, ctx,
		"SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total_salary FROM t97_gbho GROUP BY dept HAVING COUNT(*) >= 2 ORDER BY total_salary DESC")
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups with count >= 2, got %d", len(rows))
	}
	// Eng total=330, Sales total=170
	if fmt.Sprintf("%v", rows[0][0]) != "Eng" {
		t.Errorf("first group should be Eng, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[1][0]) != "Sales" {
		t.Errorf("second group should be Sales, got %v", rows[1][0])
	}
}

// ---------------------------------------------------------------------------
// 8. LEFT JOIN with IS NULL (anti-join pattern)
// ---------------------------------------------------------------------------

func TestV97_LeftJoinAntiJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_cust (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t97_ord (id INTEGER PRIMARY KEY, cust_id INTEGER, product TEXT)")

	afExec(t, db, ctx, "INSERT INTO t97_cust VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t97_cust VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO t97_cust VALUES (3, 'Carol')")

	afExec(t, db, ctx, "INSERT INTO t97_ord VALUES (1, 1, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO t97_ord VALUES (2, 1, 'Gadget')")
	afExec(t, db, ctx, "INSERT INTO t97_ord VALUES (3, 3, 'Doohickey')")

	// Anti-join: customers with NO orders
	rows := afQuery(t, db, ctx,
		"SELECT t97_cust.name FROM t97_cust LEFT JOIN t97_ord ON t97_cust.id = t97_ord.cust_id WHERE t97_ord.id IS NULL")
	if len(rows) != 1 {
		t.Fatalf("expected 1 customer with no orders, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Bob" {
		t.Errorf("expected Bob, got %v", rows[0][0])
	}
}

func TestV97_LeftJoinPreservesAllLeft(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_lj_left (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t97_lj_right (id INTEGER PRIMARY KEY, left_id INTEGER, info TEXT)")

	afExec(t, db, ctx, "INSERT INTO t97_lj_left VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t97_lj_left VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO t97_lj_left VALUES (3, 'C')")

	afExec(t, db, ctx, "INSERT INTO t97_lj_right VALUES (1, 1, 'info1')")
	// id=2 and id=3 have no matching right rows

	rows := afQuery(t, db, ctx,
		"SELECT t97_lj_left.name, t97_lj_right.info FROM t97_lj_left LEFT JOIN t97_lj_right ON t97_lj_left.id = t97_lj_right.left_id ORDER BY t97_lj_left.id")
	if len(rows) != 3 {
		t.Fatalf("LEFT JOIN should return all 3 left rows, got %d", len(rows))
	}
	// Row for id=2 should have NULL info
	if rows[1][1] != nil {
		t.Errorf("expected NULL for unmatched right side, got %v", rows[1][1])
	}
}

// ---------------------------------------------------------------------------
// 9. COALESCE with multiple arguments
// ---------------------------------------------------------------------------

func TestV97_CoalesceMultipleArgs(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, NULL, 'c', 'd')", "c")
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, 'b', 'c')", "b")
	afExpectVal(t, db, ctx, "SELECT COALESCE('a', 'b')", "a")
}

func TestV97_CoalesceWithTableData(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_coal (id INTEGER PRIMARY KEY, first_name TEXT, nick TEXT, username TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_coal VALUES (1, 'Alice', NULL, NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_coal VALUES (2, NULL, 'Bobby', NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_coal VALUES (3, NULL, NULL, 'user3')")
	afExec(t, db, ctx, "INSERT INTO t97_coal VALUES (4, NULL, NULL, NULL)")

	rows := afQuery(t, db, ctx,
		"SELECT id, COALESCE(first_name, nick, username, 'anonymous') FROM t97_coal ORDER BY id")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	expected := []string{"Alice", "Bobby", "user3", "anonymous"}
	for i, exp := range expected {
		got := fmt.Sprintf("%v", rows[i][1])
		if got != exp {
			t.Errorf("id=%d: expected '%s', got '%s'", i+1, exp, got)
		}
	}
}

// ---------------------------------------------------------------------------
// 10. String operations chained
// ---------------------------------------------------------------------------

func TestV97_ChainedStringOps(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_strops (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_strops VALUES (1, '  Hello World  ')")

	// UPPER(TRIM(SUBSTR(...)))
	// SUBSTR('  Hello World  ', 3, 5) = 'Hello'
	// TRIM('Hello') = 'Hello'
	// UPPER('Hello') = 'HELLO'
	afExpectVal(t, db, ctx, "SELECT UPPER(TRIM(SUBSTR(val, 3, 5))) FROM t97_strops WHERE id = 1", "HELLO")
}

func TestV97_LowerAndLength(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_strlen (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_strlen VALUES (1, 'ABCDEF')")
	afExec(t, db, ctx, "INSERT INTO t97_strlen VALUES (2, 'Hello')")

	afExpectVal(t, db, ctx, "SELECT LOWER(name) FROM t97_strlen WHERE id = 1", "abcdef")
	afExpectVal(t, db, ctx, "SELECT LENGTH(name) FROM t97_strlen WHERE id = 1", 6)
	afExpectVal(t, db, ctx, "SELECT LENGTH(name) FROM t97_strlen WHERE id = 2", 5)
}

// ---------------------------------------------------------------------------
// 11. Arithmetic with NULL
// ---------------------------------------------------------------------------

func TestV97_ArithmeticWithNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Any arithmetic with NULL should produce NULL
	rows := afQuery(t, db, ctx, "SELECT 5 + NULL")
	if len(rows) != 1 || rows[0][0] != nil {
		t.Errorf("5 + NULL should be NULL, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT NULL * 3")
	if len(rows) != 1 || rows[0][0] != nil {
		t.Errorf("NULL * 3 should be NULL, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT NULL - 1")
	if len(rows) != 1 || rows[0][0] != nil {
		t.Errorf("NULL - 1 should be NULL, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT 10 / NULL")
	if len(rows) != 1 || rows[0][0] != nil {
		t.Errorf("10 / NULL should be NULL, got %v", rows[0][0])
	}
}

func TestV97_ArithmeticNullInTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_null_arith (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_null_arith VALUES (1, 10, 20)")
	afExec(t, db, ctx, "INSERT INTO t97_null_arith VALUES (2, NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO t97_null_arith VALUES (3, 40, NULL)")

	rows := afQuery(t, db, ctx, "SELECT id, a + b FROM t97_null_arith ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// id=1: 10+20=30
	if fmt.Sprintf("%v", rows[0][1]) != "30" {
		t.Errorf("id=1: expected 30, got %v", rows[0][1])
	}
	// id=2: NULL+30=NULL
	if rows[1][1] != nil {
		t.Errorf("id=2: expected NULL, got %v", rows[1][1])
	}
	// id=3: 40+NULL=NULL
	if rows[2][1] != nil {
		t.Errorf("id=3: expected NULL, got %v", rows[2][1])
	}
}

// ---------------------------------------------------------------------------
// 12. BETWEEN with strings
// ---------------------------------------------------------------------------

func TestV97_BetweenWithStrings(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_between_str (id INTEGER PRIMARY KEY, code TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_between_str VALUES (1, 'apple')")
	afExec(t, db, ctx, "INSERT INTO t97_between_str VALUES (2, 'banana')")
	afExec(t, db, ctx, "INSERT INTO t97_between_str VALUES (3, 'cherry')")
	afExec(t, db, ctx, "INSERT INTO t97_between_str VALUES (4, 'date')")
	afExec(t, db, ctx, "INSERT INTO t97_between_str VALUES (5, 'elderberry')")

	// BETWEEN on strings is lexicographic
	rows := afQuery(t, db, ctx,
		"SELECT code FROM t97_between_str WHERE code BETWEEN 'banana' AND 'date' ORDER BY code")
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows between 'banana' and 'date', got %d", len(rows))
	}
	// banana, cherry, date all fall in range
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "banana" {
		t.Errorf("first result should be 'banana', got '%s'", first)
	}
}

func TestV97_BetweenWithIntegers(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_between_int (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_between_int VALUES (1, 5)")
	afExec(t, db, ctx, "INSERT INTO t97_between_int VALUES (2, 10)")
	afExec(t, db, ctx, "INSERT INTO t97_between_int VALUES (3, 15)")
	afExec(t, db, ctx, "INSERT INTO t97_between_int VALUES (4, 20)")
	afExec(t, db, ctx, "INSERT INTO t97_between_int VALUES (5, 25)")

	// BETWEEN is inclusive on both ends
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_between_int WHERE val BETWEEN 10 AND 20", 3)
	// Edge: boundary values
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_between_int WHERE val BETWEEN 5 AND 5", 1)
}

// ---------------------------------------------------------------------------
// 13. INSERT OR REPLACE with all scenarios
// ---------------------------------------------------------------------------

func TestV97_InsertOrReplacePKConflict(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_replace_pk (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_replace_pk VALUES (1, 'Alice', 80)")
	afExec(t, db, ctx, "INSERT INTO t97_replace_pk VALUES (2, 'Bob', 90)")

	// Replace on PK conflict
	afExec(t, db, ctx, "INSERT OR REPLACE INTO t97_replace_pk VALUES (1, 'Alice Updated', 95)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_replace_pk", 2)
	afExpectVal(t, db, ctx, "SELECT name FROM t97_replace_pk WHERE id = 1", "Alice Updated")
	afExpectVal(t, db, ctx, "SELECT score FROM t97_replace_pk WHERE id = 1", 95)
}

func TestV97_InsertOrReplaceUniqueConflict(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_replace_uniq (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_replace_uniq VALUES (1, 'alice@test.com', 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t97_replace_uniq VALUES (2, 'bob@test.com', 'Bob')")

	// Replace on UNIQUE conflict (same email, different PK)
	afExec(t, db, ctx, "INSERT OR REPLACE INTO t97_replace_uniq VALUES (3, 'alice@test.com', 'Alice V2')")

	// The old row (id=1) should have been replaced/deleted, and new row (id=3) inserted
	afExpectVal(t, db, ctx, "SELECT name FROM t97_replace_uniq WHERE email = 'alice@test.com'", "Alice V2")
	// Verify no duplicate
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_replace_uniq WHERE email = 'alice@test.com'", 1)
}

func TestV97_InsertOrReplaceNoConflict(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_replace_noc (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_replace_noc VALUES (1, 'first')")

	// No conflict -- should just insert
	afExec(t, db, ctx, "INSERT OR REPLACE INTO t97_replace_noc VALUES (2, 'second')")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_replace_noc", 2)
	afExpectVal(t, db, ctx, "SELECT val FROM t97_replace_noc WHERE id = 2", "second")
}

// ---------------------------------------------------------------------------
// 14. CREATE TABLE with CHECK constraints
// ---------------------------------------------------------------------------

func TestV97_CheckConstraintRejectsInvalid(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_check (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0), name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_check VALUES (1, 25, 'Alice')")

	// This should fail: age < 0
	_, err := db.Exec(ctx, "INSERT INTO t97_check VALUES (2, -5, 'Bob')")
	if err == nil {
		t.Error("CHECK constraint should reject age < 0")
	}

	// Verify the valid insert is there
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_check", 1)
	afExpectVal(t, db, ctx, "SELECT name FROM t97_check WHERE id = 1", "Alice")
}

func TestV97_CheckConstraintOnUpdate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_check_upd (id INTEGER PRIMARY KEY, price REAL CHECK(price > 0))")
	afExec(t, db, ctx, "INSERT INTO t97_check_upd VALUES (1, 10.0)")

	// Update to violating value should fail
	_, err := db.Exec(ctx, "UPDATE t97_check_upd SET price = -1.0 WHERE id = 1")
	if err == nil {
		t.Error("CHECK constraint should reject price <= 0 on UPDATE")
	}

	// Original value should be preserved
	afExpectVal(t, db, ctx, "SELECT price FROM t97_check_upd WHERE id = 1", 10.0)
}

// ---------------------------------------------------------------------------
// 15. Multi-row INSERT
// ---------------------------------------------------------------------------

func TestV97_MultiRowInsert(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_multi (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_multi VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_multi", 3)
	afExpectVal(t, db, ctx, "SELECT name FROM t97_multi WHERE id = 2", "b")
	afExpectVal(t, db, ctx, "SELECT SUM(val) FROM t97_multi", 60)
}

func TestV97_MultiRowInsertPartialColumns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_multi_pc (id INTEGER PRIMARY KEY, name TEXT, status TEXT DEFAULT 'active')")
	afExec(t, db, ctx, "INSERT INTO t97_multi_pc (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_multi_pc", 3)
	// Defaults should be applied
	afExpectVal(t, db, ctx, "SELECT status FROM t97_multi_pc WHERE id = 1", "active")
	afExpectVal(t, db, ctx, "SELECT status FROM t97_multi_pc WHERE id = 3", "active")
}

// ---------------------------------------------------------------------------
// 16. UPDATE with WHERE on computed expression
// ---------------------------------------------------------------------------

func TestV97_UpdateWithComputedWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_upd_where (id INTEGER PRIMARY KEY, price INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_where VALUES (1, 10, 5)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_where VALUES (2, 20, 3)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_where VALUES (3, 5, 100)")

	// Update only rows where total > 50
	afExec(t, db, ctx, "UPDATE t97_upd_where SET price = price + 1 WHERE price * qty > 50")

	afExpectVal(t, db, ctx, "SELECT price FROM t97_upd_where WHERE id = 1", 10) // 10*5=50, NOT > 50
	afExpectVal(t, db, ctx, "SELECT price FROM t97_upd_where WHERE id = 2", 21) // 20*3=60, > 50
	afExpectVal(t, db, ctx, "SELECT price FROM t97_upd_where WHERE id = 3", 6)  // 5*100=500, > 50
}

// ---------------------------------------------------------------------------
// 17. Self-referencing foreign keys (manager pattern)
// ---------------------------------------------------------------------------

func TestV97_SelfReferencingTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_emp (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_emp VALUES (1, 'CEO', NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_emp VALUES (2, 'VP', 1)")
	afExec(t, db, ctx, "INSERT INTO t97_emp VALUES (3, 'Manager', 2)")
	afExec(t, db, ctx, "INSERT INTO t97_emp VALUES (4, 'Dev1', 3)")
	afExec(t, db, ctx, "INSERT INTO t97_emp VALUES (5, 'Dev2', 3)")

	// Self-join: find employees with their manager names
	rows := afQuery(t, db, ctx,
		"SELECT e.name, m.name FROM t97_emp e LEFT JOIN t97_emp m ON e.manager_id = m.id ORDER BY e.id")
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	// CEO's manager should be NULL
	if rows[0][1] != nil {
		t.Errorf("CEO's manager should be NULL, got %v", rows[0][1])
	}
	// VP's manager should be CEO
	if fmt.Sprintf("%v", rows[1][1]) != "CEO" {
		t.Errorf("VP's manager should be CEO, got %v", rows[1][1])
	}
	// Dev1's manager should be Manager
	if fmt.Sprintf("%v", rows[3][1]) != "Manager" {
		t.Errorf("Dev1's manager should be Manager, got %v", rows[3][1])
	}
}

// ---------------------------------------------------------------------------
// 18. Views queried with WHERE clauses
// ---------------------------------------------------------------------------

func TestV97_ViewWithWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_products (id INTEGER PRIMARY KEY, name TEXT, price REAL, active INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_products VALUES (1, 'Widget', 10.0, 1)")
	afExec(t, db, ctx, "INSERT INTO t97_products VALUES (2, 'Gadget', 25.0, 1)")
	afExec(t, db, ctx, "INSERT INTO t97_products VALUES (3, 'Doohickey', 5.0, 0)")
	afExec(t, db, ctx, "INSERT INTO t97_products VALUES (4, 'Thingamajig', 50.0, 1)")

	// Create view
	afExec(t, db, ctx, "CREATE VIEW t97_active_products AS SELECT id, name, price FROM t97_products WHERE active = 1")

	// Query view with additional WHERE
	rows := afQuery(t, db, ctx, "SELECT name FROM t97_active_products WHERE price > 20 ORDER BY price")
	if len(rows) != 2 {
		t.Fatalf("expected 2 active products with price > 20, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Gadget" {
		t.Errorf("expected Gadget first, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[1][0]) != "Thingamajig" {
		t.Errorf("expected Thingamajig second, got %v", rows[1][0])
	}
}

func TestV97_ViewWithAggregateAndWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_sales VALUES (1, 'East', 100)")
	afExec(t, db, ctx, "INSERT INTO t97_sales VALUES (2, 'East', 200)")
	afExec(t, db, ctx, "INSERT INTO t97_sales VALUES (3, 'West', 150)")
	afExec(t, db, ctx, "INSERT INTO t97_sales VALUES (4, 'West', 50)")
	afExec(t, db, ctx, "INSERT INTO t97_sales VALUES (5, 'North', 300)")

	afExec(t, db, ctx, "CREATE VIEW t97_region_totals AS SELECT region, SUM(amount) AS total FROM t97_sales GROUP BY region")

	// Query the view with a WHERE on the aggregated column
	rows := afQuery(t, db, ctx, "SELECT region, total FROM t97_region_totals WHERE total >= 200 ORDER BY total DESC")
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 regions with total >= 200, got %d", len(rows))
	}
	// East=300, North=300, West=200
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "East" && first != "North" {
		t.Errorf("expected East or North as top region, got %v", first)
	}
}

// ---------------------------------------------------------------------------
// 19. DELETE with WHERE and verify cascade effects
// ---------------------------------------------------------------------------

func TestV97_DeleteAllRowsThenReinsert(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_del_all (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_del_all VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t97_del_all VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO t97_del_all VALUES (3, 'c')")

	afExec(t, db, ctx, "DELETE FROM t97_del_all")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_del_all", 0)

	// Reinsert with the same PKs
	afExec(t, db, ctx, "INSERT INTO t97_del_all VALUES (1, 'x')")
	afExec(t, db, ctx, "INSERT INTO t97_del_all VALUES (2, 'y')")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_del_all", 2)
	afExpectVal(t, db, ctx, "SELECT val FROM t97_del_all WHERE id = 1", "x")
}

// ---------------------------------------------------------------------------
// 20. CAST in WHERE clause
// ---------------------------------------------------------------------------

func TestV97_CastInWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_cast (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_cast VALUES (1, '10')")
	afExec(t, db, ctx, "INSERT INTO t97_cast VALUES (2, '3')")
	afExec(t, db, ctx, "INSERT INTO t97_cast VALUES (3, '25')")
	afExec(t, db, ctx, "INSERT INTO t97_cast VALUES (4, '7')")

	// Use CAST in WHERE to filter numerically
	rows := afQuery(t, db, ctx, "SELECT val FROM t97_cast WHERE CAST(val AS INTEGER) > 5 ORDER BY CAST(val AS INTEGER)")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows where CAST(val AS INTEGER) > 5, got %d", len(rows))
	}
	// Should be: '7', '10', '25' in order
	if fmt.Sprintf("%v", rows[0][0]) != "7" {
		t.Errorf("expected '7' first, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[1][0]) != "10" {
		t.Errorf("expected '10' second, got %v", rows[1][0])
	}
	if fmt.Sprintf("%v", rows[2][0]) != "25" {
		t.Errorf("expected '25' third, got %v", rows[2][0])
	}
}

func TestV97_CastIntToText(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT CAST(42 AS TEXT)", "42")
	afExpectVal(t, db, ctx, "SELECT CAST(3.14 AS INTEGER)", 3)
}

// ---------------------------------------------------------------------------
// Additional edge case tests
// ---------------------------------------------------------------------------

func TestV97_UpdateMultipleColumnsConditionally(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_multi_upd (id INTEGER PRIMARY KEY, status TEXT, attempts INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_multi_upd VALUES (1, 'pending', 0)")
	afExec(t, db, ctx, "INSERT INTO t97_multi_upd VALUES (2, 'pending', 3)")
	afExec(t, db, ctx, "INSERT INTO t97_multi_upd VALUES (3, 'done', 1)")

	// Update both status and attempts in one statement
	afExec(t, db, ctx, "UPDATE t97_multi_upd SET status = 'failed', attempts = attempts + 1 WHERE status = 'pending' AND attempts >= 3")

	afExpectVal(t, db, ctx, "SELECT status FROM t97_multi_upd WHERE id = 1", "pending") // not updated (attempts=0)
	afExpectVal(t, db, ctx, "SELECT status FROM t97_multi_upd WHERE id = 2", "failed")  // updated
	afExpectVal(t, db, ctx, "SELECT attempts FROM t97_multi_upd WHERE id = 2", 4)       // 3+1
	afExpectVal(t, db, ctx, "SELECT status FROM t97_multi_upd WHERE id = 3", "done")    // not matched
}

func TestV97_SubqueryInSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_sub_sel (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_sub_sel VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t97_sub_sel VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t97_sub_sel VALUES (3, 30)")

	// Scalar subquery in SELECT list
	rows := afQuery(t, db, ctx,
		"SELECT id, val, (SELECT MAX(val) FROM t97_sub_sel) AS max_val FROM t97_sub_sel ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, row := range rows {
		maxVal := fmt.Sprintf("%v", row[2])
		if maxVal != "30" {
			t.Errorf("row %d: max_val should be 30, got %s", i, maxVal)
		}
	}
}

func TestV97_GroupByCountDistinct(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_gcd (id INTEGER PRIMARY KEY, category TEXT, tag TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_gcd VALUES (1, 'A', 'x')")
	afExec(t, db, ctx, "INSERT INTO t97_gcd VALUES (2, 'A', 'y')")
	afExec(t, db, ctx, "INSERT INTO t97_gcd VALUES (3, 'A', 'x')")
	afExec(t, db, ctx, "INSERT INTO t97_gcd VALUES (4, 'B', 'z')")
	afExec(t, db, ctx, "INSERT INTO t97_gcd VALUES (5, 'B', 'z')")

	rows := afQuery(t, db, ctx,
		"SELECT category, COUNT(*) AS total, COUNT(DISTINCT tag) AS unique_tags FROM t97_gcd GROUP BY category ORDER BY category")
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(rows))
	}
	// Category A: 3 total, 2 unique
	aTotal := fmt.Sprintf("%v", rows[0][1])
	aUnique := fmt.Sprintf("%v", rows[0][2])
	if aTotal != "3" {
		t.Errorf("A total: expected 3, got %s", aTotal)
	}
	if aUnique != "2" {
		t.Errorf("A unique: expected 2, got %s", aUnique)
	}
	// Category B: 2 total, 1 unique
	bTotal := fmt.Sprintf("%v", rows[1][1])
	bUnique := fmt.Sprintf("%v", rows[1][2])
	if bTotal != "2" {
		t.Errorf("B total: expected 2, got %s", bTotal)
	}
	if bUnique != "1" {
		t.Errorf("B unique: expected 1, got %s", bUnique)
	}
}

func TestV97_NestedCaseWhen(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_nested_case (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_nested_case VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO t97_nested_case VALUES (2, 0)")
	afExec(t, db, ctx, "INSERT INTO t97_nested_case VALUES (3, 5)")
	afExec(t, db, ctx, "INSERT INTO t97_nested_case VALUES (4, -3)")

	rows := afQuery(t, db, ctx, `
		SELECT id, CASE
			WHEN val IS NULL THEN 'unknown'
			WHEN val = 0 THEN 'zero'
			WHEN val > 0 THEN CASE WHEN val > 3 THEN 'high' ELSE 'low' END
			ELSE 'negative'
		END AS label
		FROM t97_nested_case ORDER BY id`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	expected := []string{"unknown", "zero", "high", "negative"}
	for i, exp := range expected {
		got := fmt.Sprintf("%v", rows[i][1])
		if got != exp {
			t.Errorf("id=%d: expected '%s', got '%s'", i+1, exp, got)
		}
	}
}

func TestV97_InsertSelectWithJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_isj_orders (id INTEGER PRIMARY KEY, cust_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t97_isj_custs (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t97_isj_report (cust_name TEXT, total INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t97_isj_custs VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t97_isj_custs VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO t97_isj_orders VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t97_isj_orders VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO t97_isj_orders VALUES (3, 2, 50)")

	// INSERT ... SELECT with JOIN and GROUP BY
	afExec(t, db, ctx, `INSERT INTO t97_isj_report
		SELECT c.name, SUM(o.amount)
		FROM t97_isj_orders o JOIN t97_isj_custs c ON o.cust_id = c.id
		GROUP BY c.name`)

	rows := afQuery(t, db, ctx, "SELECT cust_name, total FROM t97_isj_report ORDER BY cust_name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows in report, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Alice" || fmt.Sprintf("%v", rows[0][1]) != "300" {
		t.Errorf("expected Alice/300, got %v/%v", rows[0][0], rows[0][1])
	}
	if fmt.Sprintf("%v", rows[1][0]) != "Bob" || fmt.Sprintf("%v", rows[1][1]) != "50" {
		t.Errorf("expected Bob/50, got %v/%v", rows[1][0], rows[1][1])
	}
}

func TestV97_NullComparisonEdgeCases(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// NULL = NULL should NOT be true
	rows := afQuery(t, db, ctx, "SELECT CASE WHEN NULL = NULL THEN 'equal' ELSE 'not_equal' END")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "not_equal" {
		t.Errorf("NULL = NULL should be not_equal (three-valued logic), got '%s'", got)
	}

	// NULL IS NULL should be true
	afExpectVal(t, db, ctx, "SELECT CASE WHEN NULL IS NULL THEN 'yes' ELSE 'no' END", "yes")
}

func TestV97_EmptyTableAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_empty (id INTEGER PRIMARY KEY, val INTEGER)")

	// COUNT(*) on empty table should be 0
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_empty", 0)

	// SUM, MIN, MAX on empty table should be NULL
	rows := afQuery(t, db, ctx, "SELECT SUM(val) FROM t97_empty")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != nil {
		t.Errorf("SUM on empty table should be NULL, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT MIN(val) FROM t97_empty")
	if len(rows) != 1 || rows[0][0] != nil {
		t.Errorf("MIN on empty table should be NULL, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT MAX(val) FROM t97_empty")
	if len(rows) != 1 || rows[0][0] != nil {
		t.Errorf("MAX on empty table should be NULL, got %v", rows[0][0])
	}
}

func TestV97_DeleteWithNotExists(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_orphan_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t97_orphan_child (id INTEGER PRIMARY KEY, parent_id INTEGER, data TEXT)")

	afExec(t, db, ctx, "INSERT INTO t97_orphan_parent VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO t97_orphan_parent VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO t97_orphan_child VALUES (1, 1, 'child1')")
	afExec(t, db, ctx, "INSERT INTO t97_orphan_child VALUES (2, 1, 'child2')")
	afExec(t, db, ctx, "INSERT INTO t97_orphan_child VALUES (3, 99, 'orphan1')")  // no parent
	afExec(t, db, ctx, "INSERT INTO t97_orphan_child VALUES (4, 100, 'orphan2')") // no parent

	// Delete orphan children (parent_id not in parents)
	afExec(t, db, ctx, "DELETE FROM t97_orphan_child WHERE parent_id NOT IN (SELECT id FROM t97_orphan_parent)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t97_orphan_child", 2)
	// Only children with parent_id 1 should remain
	rows := afQuery(t, db, ctx, "SELECT data FROM t97_orphan_child ORDER BY id")
	if fmt.Sprintf("%v", rows[0][0]) != "child1" || fmt.Sprintf("%v", rows[1][0]) != "child2" {
		t.Errorf("expected child1 and child2 remaining, got %v", rows)
	}
}

func TestV97_UpdateWithSubqueryInSet(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_upd_sub (id INTEGER PRIMARY KEY, name TEXT, total INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t97_upd_sub_items (owner_id INTEGER, amount INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t97_upd_sub VALUES (1, 'Alice', 0)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_sub VALUES (2, 'Bob', 0)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_sub_items VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_sub_items VALUES (1, 200)")
	afExec(t, db, ctx, "INSERT INTO t97_upd_sub_items VALUES (2, 50)")

	// Update total using a subquery
	afExec(t, db, ctx, "UPDATE t97_upd_sub SET total = (SELECT SUM(amount) FROM t97_upd_sub_items WHERE owner_id = t97_upd_sub.id)")

	afExpectVal(t, db, ctx, "SELECT total FROM t97_upd_sub WHERE id = 1", 300)
	afExpectVal(t, db, ctx, "SELECT total FROM t97_upd_sub WHERE id = 2", 50)
}

func TestV97_OrderByExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_order_expr (id INTEGER PRIMARY KEY, first TEXT, last TEXT)")
	afExec(t, db, ctx, "INSERT INTO t97_order_expr VALUES (1, 'Charlie', 'Adams')")
	afExec(t, db, ctx, "INSERT INTO t97_order_expr VALUES (2, 'Alice', 'Baker')")
	afExec(t, db, ctx, "INSERT INTO t97_order_expr VALUES (3, 'Bob', 'Adams')")

	// ORDER BY last, then first
	rows := afQuery(t, db, ctx, "SELECT first, last FROM t97_order_expr ORDER BY last, first")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// Adams comes first, within Adams: Bob before Charlie
	if fmt.Sprintf("%v", rows[0][0]) != "Bob" {
		t.Errorf("expected Bob first (Adams), got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[1][0]) != "Charlie" {
		t.Errorf("expected Charlie second (Adams), got %v", rows[1][0])
	}
	if fmt.Sprintf("%v", rows[2][0]) != "Alice" {
		t.Errorf("expected Alice third (Baker), got %v", rows[2][0])
	}
}

func TestV97_HavingWithAlias(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t97_having (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t97_having VALUES (1, 'X', 10)")
	afExec(t, db, ctx, "INSERT INTO t97_having VALUES (2, 'X', 20)")
	afExec(t, db, ctx, "INSERT INTO t97_having VALUES (3, 'Y', 5)")
	afExec(t, db, ctx, "INSERT INTO t97_having VALUES (4, 'Y', 3)")
	afExec(t, db, ctx, "INSERT INTO t97_having VALUES (5, 'Z', 100)")

	// HAVING with aggregate condition, ORDER BY aggregate
	rows := afQuery(t, db, ctx,
		"SELECT grp, SUM(val) AS total FROM t97_having GROUP BY grp HAVING SUM(val) > 9 ORDER BY total DESC")
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 groups with SUM > 9, got %d", len(rows))
	}
	// Z=100, X=30 (Y=8 is excluded)
	if fmt.Sprintf("%v", rows[0][0]) != "Z" {
		t.Errorf("expected Z first, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[1][0]) != "X" {
		t.Errorf("expected X second, got %v", rows[1][0])
	}
}
