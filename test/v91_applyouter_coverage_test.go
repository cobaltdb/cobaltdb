package test

import (
	"fmt"
	"testing"
)

// ============================================================
// v91: Targeted tests for applyOuterQuery aggregate path (3335-3510),
// computeViewAggregate (3669+), derived table aggregates,
// multi-CTE materialization, and applyGroupByOrderBy FunctionCall path
// ============================================================

// ============================================================
// SECTION 1: Complex view + outer aggregate → applyOuterQuery agg path
// A view with aliased columns is "complex", so the view gets executed first,
// then the outer aggregate goes through applyOuterQuery's hasAggregates path.
// ============================================================

func TestV91_ComplexViewWithOuterCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91a (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91a VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91a VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91a VALUES (3, 'A', 30)")

	// View with aliased columns makes it "complex"
	afExec(t, db, ctx, "CREATE VIEW v91a AS SELECT cat AS category, val AS value FROM t91a")

	// Outer aggregate on complex view → applyOuterQuery aggregate path
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v91a", float64(3))
}

func TestV91_ComplexViewWithOuterSum(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91b (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91b VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91b VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91b VALUES (3, 'A', 30)")

	afExec(t, db, ctx, "CREATE VIEW v91b AS SELECT cat AS category, val AS value FROM t91b")
	afExpectVal(t, db, ctx, "SELECT SUM(value) FROM v91b", float64(60))
}

func TestV91_ComplexViewWithOuterAvg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91c (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91c VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91c VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t91c VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE VIEW v91c AS SELECT val AS amount FROM t91c")
	afExpectVal(t, db, ctx, "SELECT AVG(amount) FROM v91c", float64(20))
}

func TestV91_ComplexViewWithOuterMinMax(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91d (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91d VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91d VALUES (2, 50)")
	afExec(t, db, ctx, "INSERT INTO t91d VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE VIEW v91d AS SELECT val AS amount FROM t91d")
	afExpectVal(t, db, ctx, "SELECT MIN(amount) FROM v91d", float64(10))
	afExpectVal(t, db, ctx, "SELECT MAX(amount) FROM v91d", float64(50))
}

func TestV91_ComplexViewWithOuterGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91e (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91e VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91e VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91e VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t91e VALUES (4, 'B', 40)")

	afExec(t, db, ctx, "CREATE VIEW v91e AS SELECT cat AS category, val AS value FROM t91e")

	// Outer GROUP BY on complex view → applyOuterQuery GROUP BY + aggregate path
	rows := afQuery(t, db, ctx, "SELECT category, SUM(value) as total FROM v91e GROUP BY category ORDER BY category")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[0][1]) != "40" {
		t.Fatalf("expected 40, got %v", rows[0][1])
	}
}

func TestV91_ComplexViewWithOuterGroupByHaving(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91f (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91f VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91f VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91f VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t91f VALUES (4, 'B', 40)")

	afExec(t, db, ctx, "CREATE VIEW v91f AS SELECT cat AS category, val AS value FROM t91f")

	// Outer GROUP BY + HAVING on complex view (use alias since HAVING evaluates on aggregated result)
	rows := afQuery(t, db, ctx, "SELECT category, SUM(value) as total FROM v91f GROUP BY category HAVING total > 50")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B, got %v", rows[0][0])
	}
}

func TestV91_ComplexViewWithOuterGroupByOrderByLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91g (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91g VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91g VALUES (2, 'B', 50)")
	afExec(t, db, ctx, "INSERT INTO t91g VALUES (3, 'C', 30)")
	afExec(t, db, ctx, "INSERT INTO t91g VALUES (4, 'A', 5)")
	afExec(t, db, ctx, "INSERT INTO t91g VALUES (5, 'B', 25)")
	afExec(t, db, ctx, "INSERT INTO t91g VALUES (6, 'C', 35)")

	afExec(t, db, ctx, "CREATE VIEW v91g AS SELECT cat AS category, val AS value FROM t91g")

	// GROUP BY + ORDER BY + LIMIT on complex view
	rows := afQuery(t, db, ctx, "SELECT category, SUM(value) as total FROM v91g GROUP BY category ORDER BY total DESC LIMIT 2")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// B: 75, C: 65, A: 15
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B first (75), got %v", rows[0][0])
	}
}

func TestV91_ComplexViewWithOuterGroupByOffset(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91h (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91h VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91h VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91h VALUES (3, 'C', 30)")

	afExec(t, db, ctx, "CREATE VIEW v91h AS SELECT cat AS category, val AS value FROM t91h")

	// GROUP BY + ORDER BY + OFFSET on complex view → applyOuterQuery OFFSET path
	rows := afQuery(t, db, ctx, "SELECT category, SUM(value) as total FROM v91h GROUP BY category ORDER BY category LIMIT 1 OFFSET 1")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B, got %v", rows[0][0])
	}
}

func TestV91_ComplexViewWithOuterWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91i (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91i VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91i VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t91i VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE VIEW v91i AS SELECT val AS amount FROM t91i")

	// WHERE on complex view + aggregate → applyOuterQuery WHERE + aggregate
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v91i WHERE amount > 15", float64(2))
}

func TestV91_ComplexViewWithGroupConcat(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91j (id INTEGER PRIMARY KEY, cat TEXT, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t91j VALUES (1, 'A', 'x')")
	afExec(t, db, ctx, "INSERT INTO t91j VALUES (2, 'A', 'y')")
	afExec(t, db, ctx, "INSERT INTO t91j VALUES (3, 'B', 'z')")

	afExec(t, db, ctx, "CREATE VIEW v91j AS SELECT cat AS category, name AS item FROM t91j")

	rows := afQuery(t, db, ctx, "SELECT category, GROUP_CONCAT(item) FROM v91j GROUP BY category ORDER BY category")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	t.Logf("A: %v, B: %v", rows[0][1], rows[1][1])
}

// ============================================================
// SECTION 2: Derived tables with aggregates → applyOuterQuery
// ============================================================

func TestV91_DerivedTableWithCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91k (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91k VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91k VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t91k VALUES (3, 30)")

	// Derived table with aggregate in outer query
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM (SELECT val AS amount FROM t91k) AS sub", float64(3))
}

func TestV91_DerivedTableWithSum(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91l (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91l VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91l VALUES (2, 20)")

	afExpectVal(t, db, ctx, "SELECT SUM(amount) FROM (SELECT val AS amount FROM t91l) AS sub", float64(30))
}

func TestV91_DerivedTableWithGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91m (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91m VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91m VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91m VALUES (3, 'A', 30)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM (SELECT cat, val FROM t91m) AS sub GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV91_DerivedTableWithWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91n (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91n VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91n VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t91n VALUES (3, 30)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM (SELECT val AS amount FROM t91n) AS sub WHERE amount > 15", float64(2))
}

// ============================================================
// SECTION 3: Multi-CTE materialization → cteResults path
// ============================================================

func TestV91_MultiCTEWithAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91o (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91o VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91o VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91o VALUES (3, 'A', 30)")

	// Multiple CTEs: first gets materialized, second references first
	rows := afQuery(t, db, ctx, `
		WITH
			base AS (SELECT cat, val FROM t91o),
			sums AS (SELECT cat, SUM(val) as total FROM base GROUP BY cat)
		SELECT cat, total FROM sums ORDER BY cat
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
}

func TestV91_MultiCTEWithOuterAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91p (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91p VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91p VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t91p VALUES (3, 30)")

	// Multi-CTE, outer query with aggregate
	afExpectVal(t, db, ctx, `
		WITH
			base AS (SELECT val FROM t91p),
			doubled AS (SELECT val FROM base)
		SELECT SUM(val) FROM doubled
	`, float64(60))
}

// ============================================================
// SECTION 4: View with GROUP BY (complex) + outer query aggregates
// This targets computeViewAggregate
// ============================================================

func TestV91_GroupByViewWithOuterCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91q (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91q VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91q VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91q VALUES (3, 'A', 30)")

	// View with GROUP BY is "complex" → outer aggregate goes through computeViewAggregate
	afExec(t, db, ctx, "CREATE VIEW v91q AS SELECT cat, SUM(val) AS total FROM t91q GROUP BY cat")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v91q", float64(2))
}

func TestV91_GroupByViewWithOuterSum(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91r (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91r VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91r VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91r VALUES (3, 'A', 30)")

	afExec(t, db, ctx, "CREATE VIEW v91r AS SELECT cat, SUM(val) AS total FROM t91r GROUP BY cat")

	// SUM of aggregated view
	afExpectVal(t, db, ctx, "SELECT SUM(total) FROM v91r", float64(60))
}

func TestV91_GroupByViewWithOuterWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91s (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91s VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t91s VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO t91s VALUES (3, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t91s VALUES (4, 'B', 40)")

	afExec(t, db, ctx, "CREATE VIEW v91s AS SELECT cat, SUM(val) AS total FROM t91s GROUP BY cat")

	// WHERE on aggregated view
	rows := afQuery(t, db, ctx, "SELECT cat, total FROM v91s WHERE total > 50")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "B" {
		t.Fatalf("expected B, got %v", rows[0][0])
	}
}

// ============================================================
// SECTION 5: Distinct view + outer aggregate
// ============================================================

func TestV91_DistinctViewWithOuterCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91t (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO t91t VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t91t VALUES (2, 'A')")
	afExec(t, db, ctx, "INSERT INTO t91t VALUES (3, 'B')")

	afExec(t, db, ctx, "CREATE VIEW v91t AS SELECT DISTINCT cat FROM t91t")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v91t", float64(2))
}

// ============================================================
// SECTION 6: applyOuterQuery non-aggregate column projection
// ============================================================

func TestV91_ComplexViewProjectColumns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91u (id INTEGER PRIMARY KEY, first TEXT, last TEXT)")
	afExec(t, db, ctx, "INSERT INTO t91u VALUES (1, 'John', 'Doe')")
	afExec(t, db, ctx, "INSERT INTO t91u VALUES (2, 'Jane', 'Smith')")

	// View with aliased columns → complex → applyOuterQuery non-aggregate projection
	afExec(t, db, ctx, "CREATE VIEW v91u AS SELECT first AS fname, last AS lname FROM t91u")

	rows := afQuery(t, db, ctx, "SELECT fname, lname FROM v91u ORDER BY fname")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Jane" {
		t.Fatalf("expected Jane, got %v", rows[0][0])
	}
}

func TestV91_ComplexViewProjectWithWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91v (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t91v VALUES (1, 10, 'alpha')")
	afExec(t, db, ctx, "INSERT INTO t91v VALUES (2, 20, 'beta')")
	afExec(t, db, ctx, "INSERT INTO t91v VALUES (3, 30, 'gamma')")

	afExec(t, db, ctx, "CREATE VIEW v91v AS SELECT val AS amount, name AS label FROM t91v")

	rows := afQuery(t, db, ctx, "SELECT label FROM v91v WHERE amount > 15 ORDER BY label")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV91_ComplexViewWithStarSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91w (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91w VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91w VALUES (2, 20)")

	afExec(t, db, ctx, "CREATE VIEW v91w AS SELECT val AS amount FROM t91w")

	rows := afQuery(t, db, ctx, "SELECT * FROM v91w ORDER BY amount")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV91_ComplexViewWithLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91x (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91x VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91x VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t91x VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE VIEW v91x AS SELECT val AS amount FROM t91x")

	rows := afQuery(t, db, ctx, "SELECT amount FROM v91x ORDER BY amount LIMIT 2")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV91_ComplexViewWithOffset(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91y (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t91y VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t91y VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t91y VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE VIEW v91y AS SELECT val AS amount FROM t91y")

	rows := afQuery(t, db, ctx, "SELECT amount FROM v91y ORDER BY amount LIMIT 1 OFFSET 1")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "20" {
		t.Fatalf("expected 20, got %v", rows[0][0])
	}
}

func TestV91_ComplexViewWithDistinctOuterQuery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t91z (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO t91z VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t91z VALUES (2, 'A')")
	afExec(t, db, ctx, "INSERT INTO t91z VALUES (3, 'B')")

	afExec(t, db, ctx, "CREATE VIEW v91z AS SELECT cat AS category FROM t91z")

	rows := afQuery(t, db, ctx, "SELECT DISTINCT category FROM v91z ORDER BY category")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}
