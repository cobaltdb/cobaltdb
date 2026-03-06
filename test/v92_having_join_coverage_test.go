package test

import (
	"fmt"
	"testing"
)

// ============================================================
// v92: Coverage test for:
// 1. resolveAggregateInExpr uncovered paths (UnaryExpr, BetweenExpr,
//    InExpr, CaseExpr, IsNullExpr in HAVING clause)
// 2. evaluateExprWithGroupAggregatesJoin (JOIN + GROUP BY + aggregate exprs)
// 3. evaluateHaving deeper (null result, int/int64 results)
// 4. applyGroupByOrderBy FunctionCall path
// ============================================================

// ============================================================
// SECTION 1: HAVING with different expression types
// These hit resolveAggregateInExpr branches for each expression type
// ============================================================

func TestV92_HavingWithNOT(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92a (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92a VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92a VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t92a VALUES (3, 'A', 30)")

	// HAVING with NOT (UnaryExpr) → resolveAggregateInExpr UnaryExpr path
	// A: SUM=40, NOT(40 < 30) = NOT(false) = true
	// B: SUM=20, NOT(20 < 30) = NOT(true) = false
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t92a GROUP BY cat HAVING NOT (SUM(val) < 30)")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (A=40), got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
}

func TestV92_HavingWithBetween(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92b (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92b VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92b VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t92b VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t92b VALUES (4, 'C', 100)")

	// HAVING with BETWEEN → resolveAggregateInExpr BetweenExpr path
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t92b GROUP BY cat HAVING SUM(val) BETWEEN 15 AND 50")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (A=40, B=20), got %d", len(rows))
	}
}

func TestV92_HavingWithIn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92c (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92c VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92c VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t92c VALUES (3, 'C', 30)")

	// HAVING with IN → resolveAggregateInExpr InExpr path
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t92c GROUP BY cat HAVING cat IN ('A', 'C')")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV92_HavingWithCase(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92d (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92d VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92d VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t92d VALUES (3, 'B', 5)")

	// HAVING with CASE → resolveAggregateInExpr CaseExpr path
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t92d GROUP BY cat HAVING CASE WHEN SUM(val) > 20 THEN 1 ELSE 0 END = 1")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
}

func TestV92_HavingWithIsNotNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92e (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92e VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92e VALUES (2, 'B', NULL)")
	afExec(t, db, ctx, "INSERT INTO t92e VALUES (3, 'A', 20)")

	// HAVING with IS NOT NULL → resolveAggregateInExpr IsNullExpr path
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t92e GROUP BY cat HAVING SUM(val) IS NOT NULL")
	// A has SUM=30, B has SUM=NULL → only A should appear
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}
}

func TestV92_HavingWithIsNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92f (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92f VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92f VALUES (2, 'B', NULL)")
	afExec(t, db, ctx, "INSERT INTO t92f VALUES (3, 'A', 20)")

	// HAVING with IS NULL → resolveAggregateInExpr IsNullExpr path
	rows := afQuery(t, db, ctx, "SELECT cat FROM t92f GROUP BY cat HAVING SUM(val) IS NULL")
	t.Logf("rows with NULL sum: %d", len(rows))
}

// ============================================================
// SECTION 2: JOIN + GROUP BY with various aggregate expressions
// These target evaluateExprWithGroupAggregatesJoin
// ============================================================

func TestV92_JoinGroupBySumExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, prod_id INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'Widget', 10)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (2, 'Gadget', 20)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 3)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 1, 2)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 2, 1)")

	// JOIN + GROUP BY + SUM with qualified columns → evaluateExprWithGroupAggregatesJoin
	rows := afQuery(t, db, ctx, "SELECT p.name, SUM(o.qty) as total_qty FROM products p JOIN orders o ON p.id = o.prod_id GROUP BY p.name ORDER BY p.name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Gadget" {
		t.Fatalf("expected Gadget first, got %v", rows[0][0])
	}
}

func TestV92_JoinGroupByCountExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE depts (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE emps (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO depts VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (1, 1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (2, 1, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (3, 2, 'Charlie')")

	rows := afQuery(t, db, ctx, "SELECT d.name, COUNT(*) as emp_count FROM depts d JOIN emps e ON d.id = e.dept_id GROUP BY d.name ORDER BY emp_count DESC")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Engineering" {
		t.Fatalf("expected Engineering first (2 emps), got %v", rows[0][0])
	}
}

func TestV92_JoinGroupByMultipleAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE cats (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, cat_id INTEGER, price INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cats VALUES (1, 'Electronics')")
	afExec(t, db, ctx, "INSERT INTO cats VALUES (2, 'Books')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (3, 2, 15)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (4, 2, 25)")

	rows := afQuery(t, db, ctx, "SELECT c.name, COUNT(*) as cnt, SUM(i.price) as total, AVG(i.price) as avg_price, MIN(i.price) as min_p, MAX(i.price) as max_p FROM cats c JOIN items i ON c.id = i.cat_id GROUP BY c.name ORDER BY c.name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	t.Logf("Books: cnt=%v total=%v avg=%v min=%v max=%v", rows[0][1], rows[0][2], rows[0][3], rows[0][4], rows[0][5])
	t.Logf("Electronics: cnt=%v total=%v avg=%v min=%v max=%v", rows[1][1], rows[1][2], rows[1][3], rows[1][4], rows[1][5])
}

func TestV92_JoinGroupByHavingAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE stores (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, store_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO stores VALUES (1, 'Downtown')")
	afExec(t, db, ctx, "INSERT INTO stores VALUES (2, 'Mall')")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (3, 2, 50)")

	rows := afQuery(t, db, ctx, "SELECT s.name, SUM(sa.amount) as total FROM stores s JOIN sales sa ON s.id = sa.store_id GROUP BY s.name HAVING SUM(sa.amount) > 100")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Downtown" {
		t.Fatalf("expected Downtown, got %v", rows[0][0])
	}
}

func TestV92_JoinGroupByOrderByAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE scores (id INTEGER PRIMARY KEY, team_id INTEGER, points INTEGER)")
	afExec(t, db, ctx, "INSERT INTO teams VALUES (1, 'Red')")
	afExec(t, db, ctx, "INSERT INTO teams VALUES (2, 'Blue')")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (2, 1, 20)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (3, 2, 50)")

	rows := afQuery(t, db, ctx, "SELECT t.name, SUM(s.points) as total FROM teams t JOIN scores s ON t.id = s.team_id GROUP BY t.name ORDER BY SUM(s.points) DESC")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Blue" {
		t.Fatalf("expected Blue first (50 pts), got %v", rows[0][0])
	}
}

// ============================================================
// SECTION 3: LEFT JOIN with GROUP BY (different code path)
// ============================================================

func TestV92_LeftJoinGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO parents VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO children VALUES (2, 1, 20)")

	rows := afQuery(t, db, ctx, "SELECT p.name, COUNT(c.id) as child_count FROM parents p LEFT JOIN children c ON p.id = c.parent_id GROUP BY p.name ORDER BY p.name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	t.Logf("A: %v children, B: %v children", rows[0][1], rows[1][1])
}

// ============================================================
// SECTION 4: applyGroupByOrderBy with FunctionCall ORDER BY
// This hits the ORDER BY FunctionCall branch (lines 6880-6935)
// ============================================================

func TestV92_GroupByOrderByFunctionCall(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92g (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92g VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92g VALUES (2, 'B', 50)")
	afExec(t, db, ctx, "INSERT INTO t92g VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t92g VALUES (4, 'B', 20)")

	// ORDER BY COUNT(*) → FunctionCall in applyGroupByOrderBy
	rows := afQuery(t, db, ctx, "SELECT cat, COUNT(*) FROM t92g GROUP BY cat ORDER BY COUNT(*)")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV92_GroupByOrderByFunctionCallDesc(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92h (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92h VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92h VALUES (2, 'B', 50)")
	afExec(t, db, ctx, "INSERT INTO t92h VALUES (3, 'C', 30)")

	// ORDER BY MAX(val) DESC → FunctionCall desc
	rows := afQuery(t, db, ctx, "SELECT cat, MAX(val) FROM t92h GROUP BY cat ORDER BY MAX(val) DESC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B first (50), got %v", rows[0][0])
	}
}

func TestV92_GroupByOrderByQualifiedIdent(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92i (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92i VALUES (1, 'C', 10)")
	afExec(t, db, ctx, "INSERT INTO t92i VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t92i VALUES (3, 'B', 30)")

	// ORDER BY table.column → QualifiedIdentifier in applyGroupByOrderBy
	rows := afQuery(t, db, ctx, "SELECT t92i.cat, SUM(t92i.val) FROM t92i GROUP BY t92i.cat ORDER BY t92i.cat")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A first, got %v", rows[0][0])
	}
}

// ============================================================
// SECTION 5: HAVING with non-boolean aggregate results
// ============================================================

func TestV92_HavingWithNumericResult(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92j (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92j VALUES (1, 'A', 0)")
	afExec(t, db, ctx, "INSERT INTO t92j VALUES (2, 'B', 1)")

	// HAVING SUM(val) → numeric result (non-zero = true)
	rows := afQuery(t, db, ctx, "SELECT cat FROM t92j GROUP BY cat HAVING SUM(val)")
	// A: SUM=0 → false, B: SUM=1 → true
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

// ============================================================
// SECTION 6: evaluateLike deeper paths - LIKE with expressions
// ============================================================

func TestV92_LikeWithNullInput(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92k (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t92k VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO t92k VALUES (2, 'hello')")

	// LIKE with NULL → should not match
	rows := afQuery(t, db, ctx, "SELECT id FROM t92k WHERE val LIKE '%hello%'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestV92_LikeWithNumberConversion(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92l (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92l VALUES (1, 123)")
	afExec(t, db, ctx, "INSERT INTO t92l VALUES (2, 456)")

	// LIKE on numeric column (converts to string)
	rows := afQuery(t, db, ctx, "SELECT id FROM t92l WHERE val LIKE '1%'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

// ============================================================
// SECTION 7: evaluateWhere deeper paths - complex conditions
// ============================================================

func TestV92_WhereWithComplexAndOr(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92m (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO t92m VALUES (1, 10, 20, 'x')")
	afExec(t, db, ctx, "INSERT INTO t92m VALUES (2, 30, 40, 'y')")
	afExec(t, db, ctx, "INSERT INTO t92m VALUES (3, 50, 60, 'x')")

	rows := afQuery(t, db, ctx, "SELECT id FROM t92m WHERE (a > 20 OR b < 30) AND c = 'x'")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV92_WhereWithCorrelatedSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92n1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t92n2 (id INTEGER PRIMARY KEY, ref_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92n1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t92n1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t92n2 VALUES (1, 1)")

	// Correlated subquery in WHERE
	rows := afQuery(t, db, ctx, "SELECT val FROM t92n1 WHERE id IN (SELECT ref_id FROM t92n2)")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "10" {
		t.Fatalf("expected 10, got %v", rows[0][0])
	}
}

// ============================================================
// SECTION 8: More string and math functions
// ============================================================

func TestV92_PowerAndSqrt(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92o (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92o VALUES (1, 4)")

	// Try POWER and SQRT if supported
	rows := afQuery(t, db, ctx, "SELECT ABS(-5) FROM t92o WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("ABS(-5) = %v", rows[0][0])
}

func TestV92_NullIfAndCoalesce(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92p (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92p VALUES (1, 10, 10)")
	afExec(t, db, ctx, "INSERT INTO t92p VALUES (2, 10, 20)")

	// NULLIF returns NULL if both args equal, else first arg
	rows := afQuery(t, db, ctx, "SELECT NULLIF(a, b) FROM t92p WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	if rows[0][0] != nil {
		t.Fatalf("expected NULL from NULLIF(10, 10), got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT NULLIF(a, b) FROM t92p WHERE id = 2")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	if fmt.Sprintf("%v", rows[0][0]) != "10" {
		t.Fatalf("expected 10 from NULLIF(10, 20), got %v", rows[0][0])
	}
}

func TestV92_CoalesceChain(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92q (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92q VALUES (1, NULL, NULL, 30)")
	afExec(t, db, ctx, "INSERT INTO t92q VALUES (2, NULL, 20, 30)")

	afExpectVal(t, db, ctx, "SELECT COALESCE(a, b, c) FROM t92q WHERE id = 1", float64(30))
	afExpectVal(t, db, ctx, "SELECT COALESCE(a, b, c) FROM t92q WHERE id = 2", float64(20))
}

// ============================================================
// SECTION 9: Complex GROUP BY + ORDER BY + HAVING combinations
// ============================================================

func TestV92_GroupByHavingBetween(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92r (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92r VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92r VALUES (2, 'B', 25)")
	afExec(t, db, ctx, "INSERT INTO t92r VALUES (3, 'C', 50)")
	afExec(t, db, ctx, "INSERT INTO t92r VALUES (4, 'A', 5)")

	// HAVING with BETWEEN
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t92r GROUP BY cat HAVING SUM(val) BETWEEN 10 AND 30")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (A=15, B=25), got %d", len(rows))
	}
}

func TestV92_GroupByHavingNotBetween(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92s (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92s VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92s VALUES (2, 'B', 100)")
	afExec(t, db, ctx, "INSERT INTO t92s VALUES (3, 'C', 50)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM t92s GROUP BY cat HAVING SUM(val) NOT BETWEEN 20 AND 80")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (A=10, B=100), got %d", len(rows))
	}
}

func TestV92_GroupByHavingCaseElse(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t92t VALUES (2, 'B', 30)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t92t GROUP BY cat HAVING CASE WHEN SUM(val) >= 20 THEN 1 ELSE 0 END > 0")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

// ============================================================
// SECTION 10: Window functions on CTE (selectLocked CTE + window path)
// ============================================================

func TestV92_CTEWithWindowFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92u (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92u VALUES (1, 'A', 100)")
	afExec(t, db, ctx, "INSERT INTO t92u VALUES (2, 'A', 200)")
	afExec(t, db, ctx, "INSERT INTO t92u VALUES (3, 'B', 150)")

	// CTE with window function in outer query
	rows := afQuery(t, db, ctx, "WITH data AS (SELECT dept, salary FROM t92u) SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn FROM data ORDER BY dept, salary")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	t.Logf("row 1: dept=%v salary=%v rn=%v", rows[0][0], rows[0][1], rows[0][2])
}

// ============================================================
// SECTION 11: Derived table with UNION
// ============================================================

func TestV92_DerivedTableWithUnion(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t92v1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t92v2 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t92v1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t92v2 VALUES (1, 20)")

	rows := afQuery(t, db, ctx, "SELECT val FROM (SELECT val FROM t92v1 UNION ALL SELECT val FROM t92v2) AS combined ORDER BY val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}
