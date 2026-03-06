package test

import (
	"fmt"
	"strings"
	"testing"
)

// ============================================================
// v90: High-impact coverage test targeting uncovered functions
//
// Targets:
// 1. evaluateFunctionCall: CONCAT_WS, REVERSE, REPEAT, LEFT, RIGHT,
//    LPAD, RPAD, IIF, QUOTE, ZEROBLOB, TYPEOF variants, RANDOM, CAST string/bool
// 2. applyOuterQuery: aggregates on CTE/view results with GROUP BY,
//    HAVING, ORDER BY, LIMIT/OFFSET
// 3. computeViewAggregate: aggregate functions on view results
// 4. applyGroupByOrderBy: ORDER BY with aggregates on grouped results,
//    positional ORDER BY after GROUP BY
// 5. evaluateExprWithGroupAggregatesJoin: JOIN with GROUP BY and
//    aggregate expressions in SELECT
// ============================================================

// ============================================================
// SECTION 1: String functions (evaluateFunctionCall uncovered paths)
// ============================================================

func TestV90_ConcatWS(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello', 'beautiful', 'world')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'foo', NULL, 'bar')")

	// CONCAT_WS with separator
	afExpectVal(t, db, ctx, "SELECT CONCAT_WS(' ', a, b, c) FROM t WHERE id = 1", "hello beautiful world")
	// CONCAT_WS skips NULLs
	afExpectVal(t, db, ctx, "SELECT CONCAT_WS('-', a, b, c) FROM t WHERE id = 2", "foo-bar")
}

func TestV90_ConcatWSComma(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT, y TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'alpha', 'beta')")
	afExpectVal(t, db, ctx, "SELECT CONCAT_WS(', ', x, y) FROM t WHERE id = 1", "alpha, beta")
}

func TestV90_Reverse(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'abcdef')")
	afExpectVal(t, db, ctx, "SELECT REVERSE(val) FROM t WHERE id = 1", "olleh")
	afExpectVal(t, db, ctx, "SELECT REVERSE(val) FROM t WHERE id = 2", "fedcba")
}

func TestV90_ReverseNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, NULL)")
	rows := afQuery(t, db, ctx, "SELECT REVERSE(val) FROM t WHERE id = 1")
	if len(rows) != 1 || rows[0][0] != nil {
		t.Fatalf("expected NULL, got %v", rows[0][0])
	}
}

func TestV90_Repeat(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'ab')")
	afExpectVal(t, db, ctx, "SELECT REPEAT(val, 3) FROM t WHERE id = 1", "ababab")
	// Repeat 0 times
	afExpectVal(t, db, ctx, "SELECT REPEAT(val, 0) FROM t WHERE id = 1", "")
}

func TestV90_Left(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello world')")
	afExpectVal(t, db, ctx, "SELECT LEFT(val, 5) FROM t WHERE id = 1", "hello")
	// LEFT with n > length returns full string
	afExpectVal(t, db, ctx, "SELECT LEFT(val, 100) FROM t WHERE id = 1", "hello world")
	// LEFT with n = 0
	afExpectVal(t, db, ctx, "SELECT LEFT(val, 0) FROM t WHERE id = 1", "")
}

func TestV90_Right(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello world')")
	afExpectVal(t, db, ctx, "SELECT RIGHT(val, 5) FROM t WHERE id = 1", "world")
	// RIGHT with n > length returns full string
	afExpectVal(t, db, ctx, "SELECT RIGHT(val, 100) FROM t WHERE id = 1", "hello world")
	// RIGHT with n = 0
	afExpectVal(t, db, ctx, "SELECT RIGHT(val, 0) FROM t WHERE id = 1", "")
}

func TestV90_LPAD(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hi')")
	afExpectVal(t, db, ctx, "SELECT LPAD(val, 5, '*') FROM t WHERE id = 1", "***hi")
	// LPAD when string is already long enough, truncate
	afExpectVal(t, db, ctx, "SELECT LPAD(val, 1, '*') FROM t WHERE id = 1", "h")
}

func TestV90_RPAD(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hi')")
	afExpectVal(t, db, ctx, "SELECT RPAD(val, 5, '*') FROM t WHERE id = 1", "hi***")
	// RPAD when string is already long enough, truncate
	afExpectVal(t, db, ctx, "SELECT RPAD(val, 1, '*') FROM t WHERE id = 1", "h")
}

// ============================================================
// SECTION 2: Type and conversion functions
// ============================================================

func TestV90_TypeofNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, NULL)")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(val) FROM t WHERE id = 1", "null")
}

func TestV90_TypeofText(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(val) FROM t WHERE id = 1", "text")
}

func TestV90_IIF(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 0)")
	// IIF with truthy condition
	afExpectVal(t, db, ctx, "SELECT IIF(val > 5, 'big', 'small') FROM t WHERE id = 1", "big")
	// IIF with falsy condition
	afExpectVal(t, db, ctx, "SELECT IIF(val > 5, 'big', 'small') FROM t WHERE id = 2", "small")
}

func TestV90_Quote(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'it''s a test')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, NULL)")
	// QUOTE escapes single quotes
	rows := afQuery(t, db, ctx, "SELECT QUOTE(val) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	s := fmt.Sprintf("%v", rows[0][0])
	if !strings.Contains(s, "'") {
		t.Fatalf("expected quoted string, got %v", s)
	}
	// QUOTE of NULL
	afExpectVal(t, db, ctx, "SELECT QUOTE(val) FROM t WHERE id = 2", "NULL")
}

func TestV90_QuoteNumber(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 42)")
	rows := afQuery(t, db, ctx, "SELECT QUOTE(val) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("QUOTE(42) = %v", rows[0][0])
}

func TestV90_Zeroblob(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")
	rows := afQuery(t, db, ctx, "SELECT ZEROBLOB(4) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	s, ok := rows[0][0].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rows[0][0])
	}
	if len(s) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(s))
	}
}

func TestV90_Random(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")
	rows := afQuery(t, db, ctx, "SELECT RANDOM() FROM t WHERE id = 1")
	if len(rows) != 1 || rows[0][0] == nil {
		t.Fatal("expected non-nil result from RANDOM()")
	}
	t.Logf("RANDOM() = %v", rows[0][0])
}

func TestV90_CastStringToInt(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, '42')")
	rows := afQuery(t, db, ctx, "SELECT CAST(val AS INTEGER) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("CAST('42' AS INTEGER) = %v (type %T)", rows[0][0], rows[0][0])
}

func TestV90_CastStringToReal(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, '3.14')")
	rows := afQuery(t, db, ctx, "SELECT CAST(val AS REAL) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("CAST('3.14' AS REAL) = %v (type %T)", rows[0][0], rows[0][0])
}

func TestV90_CastToText(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 42)")
	afExpectVal(t, db, ctx, "SELECT CAST(val AS TEXT) FROM t WHERE id = 1", "42")
}

func TestV90_CastToBool(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 0)")
	rows := afQuery(t, db, ctx, "SELECT CAST(val AS BOOLEAN) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("CAST(1 AS BOOLEAN) = %v (type %T)", rows[0][0], rows[0][0])
}

// ============================================================
// SECTION 3: CTE/View with aggregates (applyOuterQuery paths)
// ============================================================

func TestV90_CTEWithAggregateCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'A', 30)")

	// CTE with aggregate in outer query (applyOuterQuery aggregate path)
	afExpectVal(t, db, ctx, "WITH data AS (SELECT cat, val FROM t) SELECT COUNT(*) FROM data", float64(3))
}

func TestV90_CTEWithAggregateSum(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	afExpectVal(t, db, ctx, "WITH data AS (SELECT val FROM t) SELECT SUM(val) FROM data", float64(60))
}

func TestV90_CTEWithGroupByAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'B', 40)")

	// CTE with GROUP BY in outer query
	rows := afQuery(t, db, ctx, "WITH data AS (SELECT cat, val FROM t) SELECT cat, SUM(val) as total FROM data GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// A: 10+30=40, B: 20+40=60
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[0][1]) != "40" {
		t.Fatalf("expected 40, got %v", rows[0][1])
	}
}

func TestV90_CTEWithGroupByHaving(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'B', 40)")

	// CTE with GROUP BY + HAVING in outer query
	rows := afQuery(t, db, ctx, "WITH data AS (SELECT cat, val FROM t) SELECT cat, SUM(val) as total FROM data GROUP BY cat HAVING SUM(val) > 50")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B, got %v", rows[0][0])
	}
}

func TestV90_CTEWithGroupByOrderByLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'C', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'A', 5)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (5, 'B', 25)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (6, 'C', 35)")

	// CTE with GROUP BY + ORDER BY + LIMIT
	rows := afQuery(t, db, ctx, "WITH data AS (SELECT cat, val FROM t) SELECT cat, SUM(val) as total FROM data GROUP BY cat ORDER BY total DESC LIMIT 2")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// C: 65, B: 45, A: 15 → top 2: C, B
	if fmt.Sprintf("%v", rows[0][0]) != "C" {
		t.Fatalf("expected C first, got %v", rows[0][0])
	}
}

func TestV90_CTEWithGroupByOffset(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'C', 30)")

	// CTE with GROUP BY + LIMIT + OFFSET
	rows := afQuery(t, db, ctx, "WITH data AS (SELECT cat, val FROM t) SELECT cat, SUM(val) as total FROM data GROUP BY cat ORDER BY cat LIMIT 1 OFFSET 1")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B, got %v", rows[0][0])
	}
}

func TestV90_CTEWithWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	// CTE with WHERE in outer query
	afExpectVal(t, db, ctx, "WITH data AS (SELECT val FROM t) SELECT COUNT(*) FROM data WHERE val > 15", float64(2))
}

func TestV90_ViewWithAggregateCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'A', 30)")

	afExec(t, db, ctx, "CREATE VIEW v AS SELECT cat, val FROM t")

	// View with aggregate in outer query
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v", float64(3))
}

func TestV90_ViewWithGroupByAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'A', 30)")

	afExec(t, db, ctx, "CREATE VIEW v AS SELECT cat, val FROM t")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM v GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV90_CTEWithAvg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	// CTE with AVG aggregate
	afExpectVal(t, db, ctx, "WITH data AS (SELECT val FROM t) SELECT AVG(val) FROM data", float64(20))
}

func TestV90_CTEWithMinMax(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	afExpectVal(t, db, ctx, "WITH data AS (SELECT val FROM t) SELECT MIN(val) FROM data", float64(10))
	afExpectVal(t, db, ctx, "WITH data AS (SELECT val FROM t) SELECT MAX(val) FROM data", float64(30))
}

func TestV90_CTEWithGroupConcat(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 'alice')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 'bob')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 'charlie')")

	rows := afQuery(t, db, ctx, "WITH data AS (SELECT cat, name FROM t) SELECT cat, GROUP_CONCAT(name) FROM data GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	t.Logf("A: %v, B: %v", rows[0][1], rows[1][1])
}

// ============================================================
// SECTION 4: applyGroupByOrderBy paths
// ============================================================

func TestV90_GroupByWithPositionalOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'C', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 30)")

	// GROUP BY + ORDER BY positional (ORDER BY 1 = first column)
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM t GROUP BY cat ORDER BY 1")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A first, got %v", rows[0][0])
	}
}

func TestV90_GroupByWithPositionalOrderByDesc(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'C', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 30)")

	// GROUP BY + ORDER BY positional DESC
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM t GROUP BY cat ORDER BY 2 DESC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// B:30, A:20, C:10
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B first (highest sum), got %v", rows[0][0])
	}
}

func TestV90_GroupByOrderByAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 50)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'B', 20)")

	// ORDER BY SUM(val) on grouped results
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t GROUP BY cat ORDER BY SUM(val)")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// A: 40, B: 70
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A first (lowest sum), got %v", rows[0][0])
	}
}

func TestV90_GroupByOrderByAggregateDesc(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 50)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'B', 20)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM t GROUP BY cat ORDER BY SUM(val) DESC")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B first (highest sum), got %v", rows[0][0])
	}
}

// ============================================================
// SECTION 5: JOIN with GROUP BY and aggregate expressions
// ============================================================

func TestV90_JoinGroupByAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, cust_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 2, 150)")

	rows := afQuery(t, db, ctx, "SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.cust_id GROUP BY c.name ORDER BY c.name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Alice" {
		t.Fatalf("expected Alice, got %v", rows[0][0])
	}
}

func TestV90_JoinGroupByHaving(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, cust_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 2, 50)")

	rows := afQuery(t, db, ctx, "SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.cust_id GROUP BY c.name HAVING SUM(o.amount) > 100")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Alice" {
		t.Fatalf("expected Alice, got %v", rows[0][0])
	}
}

func TestV90_JoinGroupByCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, cust_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 2, 50)")

	rows := afQuery(t, db, ctx, "SELECT c.name, COUNT(*) as order_count FROM customers c JOIN orders o ON c.id = o.cust_id GROUP BY c.name ORDER BY order_count DESC")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Alice" {
		t.Fatalf("expected Alice first (2 orders), got %v", rows[0][0])
	}
}

func TestV90_JoinGroupByAvgMinMax(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, cust_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 2, 50)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (4, 2, 150)")

	rows := afQuery(t, db, ctx, "SELECT c.name, AVG(o.amount) as avg_amt, MIN(o.amount) as min_amt, MAX(o.amount) as max_amt FROM customers c JOIN orders o ON c.id = o.cust_id GROUP BY c.name ORDER BY c.name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	t.Logf("Alice: avg=%v min=%v max=%v", rows[0][1], rows[0][2], rows[0][3])
	t.Logf("Bob: avg=%v min=%v max=%v", rows[1][1], rows[1][2], rows[1][3])
}

// ============================================================
// SECTION 6: View aggregate functions (computeViewAggregate)
// ============================================================

func TestV90_ViewAggregateAvg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE VIEW v AS SELECT val FROM t")
	afExpectVal(t, db, ctx, "SELECT AVG(val) FROM v", float64(20))
}

func TestV90_ViewAggregateMinMax(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 50)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE VIEW v AS SELECT val FROM t")
	afExpectVal(t, db, ctx, "SELECT MIN(val) FROM v", float64(10))
	afExpectVal(t, db, ctx, "SELECT MAX(val) FROM v", float64(50))
}

func TestV90_ViewAggregateSum(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")

	afExec(t, db, ctx, "CREATE VIEW v AS SELECT val FROM t")
	afExpectVal(t, db, ctx, "SELECT SUM(val) FROM v", float64(30))
}

func TestV90_ViewWithGroupByHaving(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 5)")

	afExec(t, db, ctx, "CREATE VIEW v AS SELECT cat, val FROM t")
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM v GROUP BY cat HAVING SUM(val) > 20 ORDER BY cat")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
}

func TestV90_ViewWithOrderByLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'C', 30)")

	afExec(t, db, ctx, "CREATE VIEW v AS SELECT cat, val FROM t")
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as total FROM v GROUP BY cat ORDER BY total DESC LIMIT 2")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// SECTION 7: More evaluateHaving paths
// ============================================================

func TestV90_HavingWithMultipleConditions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 50)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'C', 5)")

	// HAVING with AND
	rows := afQuery(t, db, ctx, "SELECT cat, COUNT(*) as cnt, SUM(val) as total FROM t GROUP BY cat HAVING COUNT(*) >= 2 AND SUM(val) > 20")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	// A: count=2, sum=30 (matches); B: count=1, sum=50 (no - count < 2); C: count=1, sum=5 (no)
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
}

func TestV90_HavingWithAvg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 5)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'B', 6)")

	rows := afQuery(t, db, ctx, "SELECT cat, AVG(val) FROM t GROUP BY cat HAVING AVG(val) > 10")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
}

// ============================================================
// SECTION 8: evaluateWhere deeper paths
// ============================================================

func TestV90_WhereWithSubqueryExists(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 1)")

	rows := afQuery(t, db, ctx, "SELECT val FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "10" {
		t.Fatalf("expected 10, got %v", rows[0][0])
	}
}

func TestV90_WhereWithNotExists(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 1)")

	rows := afQuery(t, db, ctx, "SELECT val FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "20" {
		t.Fatalf("expected 20, got %v", rows[0][0])
	}
}

// ============================================================
// SECTION 9: evaluateLike deeper paths
// ============================================================

func TestV90_LikeWithUnderscore(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'abc')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'aXc')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'abcd')")

	// _ matches exactly one character
	rows := afQuery(t, db, ctx, "SELECT val FROM t WHERE val LIKE 'a_c' ORDER BY val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (abc, aXc), got %d", len(rows))
	}
}

func TestV90_LikeWithMultiplePercent(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello world foo')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'hello foo')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'world')")

	rows := afQuery(t, db, ctx, "SELECT val FROM t WHERE val LIKE '%world%'")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// SECTION 10: JSON functions deeper (evaluateJSONFunction uncovered)
// ============================================================

func TestV90_JSONArrayAndObject(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'test')")

	rows := afQuery(t, db, ctx, "SELECT JSON_ARRAY(1, 2, 3) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("JSON_ARRAY(1,2,3) = %v", rows[0][0])

	rows = afQuery(t, db, ctx, "SELECT JSON_OBJECT('name', 'alice', 'age', 30) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("JSON_OBJECT = %v", rows[0][0])
}

func TestV90_JSONKeys(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '{"a":1,"b":2,"c":3}')`)

	rows := afQuery(t, db, ctx, "SELECT JSON_KEYS(data) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("JSON_KEYS = %v", rows[0][0])
}

func TestV90_JSONArrayLength(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '[1,2,3,4,5]')`)

	afExpectVal(t, db, ctx, "SELECT JSON_ARRAY_LENGTH(data) FROM t WHERE id = 1", float64(5))
}

func TestV90_JSONMerge(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	rows := afQuery(t, db, ctx, `SELECT JSON_MERGE('{"a":1}', '{"b":2}') FROM t WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	s := fmt.Sprintf("%v", rows[0][0])
	if !strings.Contains(s, "a") || !strings.Contains(s, "b") {
		t.Fatalf("expected merged JSON, got %v", s)
	}
}

func TestV90_JSONPretty(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	rows := afQuery(t, db, ctx, `SELECT JSON_PRETTY('{"a":1}') FROM t WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	s := fmt.Sprintf("%v", rows[0][0])
	if !strings.Contains(s, "\n") {
		t.Logf("JSON_PRETTY result: %v", s)
	}
}

func TestV90_JSONQuoteUnquote(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	rows := afQuery(t, db, ctx, `SELECT JSON_QUOTE('hello') FROM t WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("JSON_QUOTE('hello') = %v", rows[0][0])

	rows = afQuery(t, db, ctx, `SELECT JSON_UNQUOTE('"hello"') FROM t WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("JSON_UNQUOTE = %v", rows[0][0])
}

func TestV90_JSONValid(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	rows := afQuery(t, db, ctx, `SELECT JSON_VALID('{"a":1}') FROM t WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("JSON_VALID result = %v", rows[0][0])
}

// ============================================================
// SECTION 11: Analyze deeper paths
// ============================================================

func TestV90_AnalyzeMultipleTables(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (2, 'world')")

	afExec(t, db, ctx, "ANALYZE t1")
	afExec(t, db, ctx, "ANALYZE t2")
}

func TestV90_AnalyzeWithIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx_val ON t (val)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'name_%d')", i, i*10, i))
	}
	afExec(t, db, ctx, "ANALYZE t")
}

// ============================================================
// SECTION 12: Additional function coverage
// ============================================================

func TestV90_CastNullToInteger(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, NULL)")
	rows := afQuery(t, db, ctx, "SELECT CAST(val AS INTEGER) FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	if rows[0][0] != nil {
		t.Logf("CAST(NULL AS INTEGER) = %v (type %T)", rows[0][0], rows[0][0])
	}
}

func TestV90_MultipleStringFunctions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'Hello World')")

	// Chain multiple functions
	afExpectVal(t, db, ctx, "SELECT LEFT(val, 5) FROM t WHERE id = 1", "Hello")
	afExpectVal(t, db, ctx, "SELECT RIGHT(val, 5) FROM t WHERE id = 1", "World")
	afExpectVal(t, db, ctx, "SELECT REVERSE(val) FROM t WHERE id = 1", "dlroW olleH")
	afExpectVal(t, db, ctx, "SELECT REPEAT('x', 5) FROM t WHERE id = 1", "xxxxx")
}

func TestV90_LPADRPADVariations(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'test')")

	afExpectVal(t, db, ctx, "SELECT LPAD(val, 8, '0') FROM t WHERE id = 1", "0000test")
	afExpectVal(t, db, ctx, "SELECT RPAD(val, 8, '.') FROM t WHERE id = 1", "test....")
}

func TestV90_GroupByWithQualifiedColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, cust TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 'Alice', 200)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 'Bob', 150)")

	rows := afQuery(t, db, ctx, "SELECT orders.cust, SUM(orders.amount) FROM orders GROUP BY orders.cust ORDER BY orders.cust")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV90_CTEWithMultipleAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	rows := afQuery(t, db, ctx, "WITH data AS (SELECT val FROM t) SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM data")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	t.Logf("count=%v sum=%v avg=%v min=%v max=%v", rows[0][0], rows[0][1], rows[0][2], rows[0][3], rows[0][4])
}

func TestV90_ViewGroupConcatAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 'x')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 'y')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 'z')")

	afExec(t, db, ctx, "CREATE VIEW v AS SELECT cat, name FROM t")
	rows := afQuery(t, db, ctx, "SELECT cat, GROUP_CONCAT(name) FROM v GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	t.Logf("A=%v, B=%v", rows[0][1], rows[1][1])
}

// ============================================================
// SECTION 13: selectLocked deeper - HAVING/window paths
// ============================================================

func TestV90_SelectWithGroupByHavingCountAndSum(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	for i := 1; i <= 10; i++ {
		cat := "A"
		if i > 5 {
			cat = "B"
		}
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s', %d)", i, cat, i*10))
	}

	rows := afQuery(t, db, ctx, "SELECT cat, COUNT(*), SUM(val) FROM t GROUP BY cat HAVING COUNT(*) = 5 ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV90_SelectGroupByHavingMax(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 100)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 200)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 50)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'B', 60)")

	rows := afQuery(t, db, ctx, "SELECT cat, MAX(val) as max_val FROM t GROUP BY cat HAVING MAX(val) > 100")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
}

// ============================================================
// SECTION 14: Regex functions
// ============================================================

func TestV90_RegexMatch(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello123')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'world')")

	rows := afQuery(t, db, ctx, "SELECT REGEXP_MATCH(val, '[0-9]+') FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("REGEXP_MATCH result: %v", rows[0][0])
}

func TestV90_RegexReplace(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello 123 world 456')")

	rows := afQuery(t, db, ctx, "SELECT REGEXP_REPLACE(val, '[0-9]+', 'NUM') FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("REGEXP_REPLACE result: %v", rows[0][0])
}

func TestV90_RegexExtract(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'price: $42.50')")

	rows := afQuery(t, db, ctx, "SELECT REGEXP_EXTRACT(val, '[0-9]+') FROM t WHERE id = 1")
	if len(rows) != 1 {
		t.Fatal("expected 1 row")
	}
	t.Logf("REGEXP_EXTRACT result: %v", rows[0][0])
}
