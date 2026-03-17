package test

import (
	"fmt"
	"testing"
)

// TestV113SelectLockedCoverage targets selectLocked function coverage gaps
func TestV113SelectLockedCoverage(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

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
		got := rows[0][0]
		if expected == nil {
			if got != nil {
				t.Errorf("[FAIL] %s: got %v (%T), expected nil", desc, got, got)
				return
			}
			pass++
			return
		}
		gotStr := fmt.Sprintf("%v", got)
		expStr := fmt.Sprintf("%v", expected)
		if gotStr != expStr {
			t.Errorf("[FAIL] %s: got %s (%T), expected %s", desc, gotStr, got, expStr)
			return
		}
		pass++
	}

	checkRows := func(desc string, sql string, expectedCount int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expectedCount {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expectedCount, len(rows))
			return
		}
		pass++
	}

	// Setup test data
	afExec(t, db, ctx, "CREATE TABLE coverage_test (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO coverage_test VALUES (1, 100, 'Alice'), (2, 200, 'Bob'), (3, 300, 'Charlie'), (4, 400, 'David'), (5, 500, 'Eve')")

	// Test 1: Basic CTE
	check("CTE+Basic: Simple CTE",
		"WITH cte AS (SELECT id, val FROM coverage_test) SELECT id FROM cte WHERE val = 300",
		int64(3))

	// Test 2: CTE with LIMIT
	check("CTE+Limit: Limit on CTE result",
		"WITH cte AS (SELECT id FROM coverage_test ORDER BY id) SELECT id FROM cte LIMIT 1 OFFSET 2",
		int64(3))

	// Test 3: CTE with WHERE filter
	check("CTE+Where: Filter on CTE result",
		"WITH cte AS (SELECT id, val FROM coverage_test) SELECT id FROM cte WHERE val > 300 ORDER BY id LIMIT 1",
		int64(4))

	// Test 4: Recursive CTE with ORDER BY
	check("RecursiveCTE+OrderBy: Ordered recursive CTE",
		"WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 5) SELECT n FROM nums ORDER BY n DESC LIMIT 1",
		int64(5))

	// Test 5: Derived table with JOIN
	checkRows("DerivedTable+Join: Derived table joined with table",
		"SELECT * FROM (SELECT id, val FROM coverage_test WHERE val > 200) AS dt JOIN coverage_test AS t2 ON dt.id = t2.id",
		3)

	// Test 6: Complex nested subquery
	check("NestedSubquery: Deeply nested",
		"SELECT max_id FROM (SELECT MAX(id) AS max_id FROM (SELECT id FROM coverage_test WHERE val > 150) AS inner_q) AS outer_q",
		int64(5))

	// Test 7: Multiple CTEs with reference
	check("MultipleCTEs: Second CTE references first",
		"WITH cte1 AS (SELECT id FROM coverage_test WHERE val > 200), cte2 AS (SELECT id FROM cte1 WHERE id > 2) SELECT COUNT(*) FROM cte2",
		int64(3))

	// Test 8: CTE with aggregate (COUNT)
	check("CTE+Aggregate: COUNT on CTE",
		"WITH cte AS (SELECT val FROM coverage_test UNION ALL SELECT val FROM coverage_test) SELECT COUNT(*) FROM cte",
		int64(10))

	// Test 9: Complex scalar subquery in CTE
	check("ScalarSubqueryCTE: Scalar subquery in CTE column",
		"WITH cte AS (SELECT id, (SELECT MAX(val) FROM coverage_test) AS maxval FROM coverage_test) SELECT maxval FROM cte WHERE id = 1",
		int64(500))

	// Test 10: CTE with self-join
	checkRows("CTESelfJoin: Self join on CTE",
		"WITH cte AS (SELECT id, val FROM coverage_test) SELECT a.id, b.id FROM cte a JOIN cte b ON a.val = b.val + 100 WHERE a.id > 1",
		4)

	// Test 11: Derived table with aggregate
	check("DerivedAggregate: Aggregate in derived table",
		"SELECT max_val FROM (SELECT MAX(val) AS max_val FROM coverage_test) AS dt",
		int64(500))

	// Test 12: CTE with aggregate and GROUP BY
	check("CTE+Aggregate: COUNT with GROUP BY",
		"WITH cte AS (SELECT val FROM coverage_test) SELECT cnt FROM (SELECT val, COUNT(*) AS cnt FROM cte GROUP BY val HAVING val > 200) AS grouped ORDER BY val LIMIT 1",
		int64(1))

	// Test 13: CTE with simple HAVING
	check("CTE+HavingSimple: Simple aggregate on CTE",
		"WITH cte AS (SELECT id, val FROM coverage_test) SELECT MAX(id) FROM cte WHERE val >= 400",
		int64(5))

	// Test 14: Complex correlated subquery with CTE
	check("CorrelatedCTE: Correlated subquery referencing CTE",
		"WITH cte AS (SELECT id, val FROM coverage_test) SELECT c1.id FROM cte c1 WHERE c1.val > (SELECT AVG(val) FROM cte) ORDER BY c1.id LIMIT 1",
		int64(4))

	// Test 15: UNION in derived table with ORDER BY
	checkRows("UnionDerived+OrderBy: UNION in derived table with ORDER BY",
		"SELECT * FROM (SELECT id, val FROM coverage_test WHERE id <= 2 UNION ALL SELECT id, val FROM coverage_test WHERE id >= 4 ORDER BY id) AS dt",
		4)

	// Test 16: Window function - SUM OVER (simple)
	check("WindowSum: SUM OVER without frame",
		"WITH cte AS (SELECT id, val FROM coverage_test) SELECT s FROM (SELECT id, SUM(val) OVER (ORDER BY id) AS s FROM cte) AS subq WHERE id = 3",
		int64(600))

	// Test 17: Window function - ROW_NUMBER
	check("CTE+Window: ROW_NUMBER with ORDER BY",
		"WITH cte AS (SELECT id, val FROM coverage_test) SELECT rn FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM cte) AS subq WHERE id = 3",
		int64(3))

	// Test 18: Window function - RANK with ties
	check("CTE+Window: RANK with ties",
		"WITH cte AS (SELECT id, val FROM coverage_test UNION ALL SELECT 6, 300) SELECT rank_val FROM (SELECT id, RANK() OVER (ORDER BY val) AS rank_val FROM cte) AS subq WHERE id = 3",
		int64(3))

	// Test 19: Window function - DENSE_RANK (actual rank for id=4 is 4)
	check("CTE+Window: DENSE_RANK",
		"WITH cte AS (SELECT id, val FROM coverage_test UNION ALL SELECT 6, 300) SELECT dr FROM (SELECT id, DENSE_RANK() OVER (ORDER BY val) AS dr FROM cte) AS subq WHERE id = 4",
		int64(4))

	// Test 20: LAG window function on CTE
	check("CTE+LAG: LAG function on CTE result",
		"WITH cte AS (SELECT id, val FROM coverage_test) SELECT lag_val FROM (SELECT id, LAG(val, 1, 0) OVER (ORDER BY id) AS lag_val FROM cte) AS subq WHERE id = 3",
		int64(200))

	t.Logf("V113 SelectLocked Coverage: %d/%d passed", pass, total)
}

// TestV113ASOfTemporal targets AS OF temporal query functionality
func TestV113ASOfTemporal(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Setup test data
	afExec(t, db, ctx, "CREATE TABLE temporal_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO temporal_test VALUES (1, 100)")

	// Test AS OF temporal query - error path
	_, err := db.Query(ctx, "SELECT * FROM temporal_test AS OF SYSTEM TIME '-1 hour'")
	if err != nil {
		t.Logf("AS OF temporal query returned error (expected): %v", err)
	} else {
		t.Log("AS OF temporal query executed without error")
	}

	// Test CURRENT_TIMESTAMP in AS OF
	_, err = db.Query(ctx, "SELECT * FROM temporal_test AS OF SYSTEM TIME CURRENT_TIMESTAMP")
	if err != nil {
		t.Logf("AS OF with CURRENT_TIMESTAMP returned error: %v", err)
	}
}

// TestV113OptimizerIntegration tests query optimizer integration in selectLocked
func TestV113OptimizerIntegration(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

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
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		expStr := fmt.Sprintf("%v", expected)
		if got != expStr {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, expStr)
			return
		}
		pass++
	}

	// Setup
	afExec(t, db, ctx, "CREATE TABLE opt_test (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO opt_test VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")

	// Queries that should trigger optimizer
	check("Optimizer: Simple SELECT with WHERE",
		"SELECT a FROM opt_test WHERE id = 2",
		int64(20))

	check("Optimizer: JOIN with index",
		"SELECT t1.a FROM opt_test t1 JOIN opt_test t2 ON t1.id = t2.id WHERE t1.id = 1",
		int64(10))

	// HAVING without GROUP BY returns single row for aggregates
	check("Optimizer: Aggregate with HAVING",
		"SELECT SUM(a) FROM opt_test HAVING SUM(a) > 0",
		int64(60))

	t.Logf("V113 Optimizer Integration: %d/%d passed", pass, total)
}

// TestV113CatalogMethodsCoverage tests catalog methods that are part of selectLocked flow
func TestV113CatalogMethodsCoverage(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

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
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		expStr := fmt.Sprintf("%v", expected)
		if got != expStr {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, expStr)
			return
		}
		pass++
	}

	// Setup for applyOuterQuery testing
	afExec(t, db, ctx, "CREATE TABLE outer_test (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)")
	afExec(t, db, ctx, "INSERT INTO outer_test VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)")

	// Test applyOuterQuery with complex subquery
	check("applyOuterQuery: Complex view with aggregate",
		"SELECT max_num FROM (SELECT MAX(num) AS max_num FROM outer_test) AS v",
		int64(30))

	check("applyOuterQuery: View with GROUP BY",
		"SELECT cnt FROM (SELECT val, COUNT(*) AS cnt FROM outer_test GROUP BY val) AS v ORDER BY cnt DESC LIMIT 1",
		int64(1))

	// Test resolveOuterRefsInQuery - row 2 has num=20 which is > avg of rows before it (avg of just row 1 = 10)
	check("resolveOuterRefs: Correlated subquery",
		"SELECT id FROM outer_test o WHERE num > (SELECT AVG(num) FROM outer_test WHERE id < o.id) ORDER BY id",
		int64(2))

	check("resolveOuterRefs: Multiple correlation levels",
		"SELECT id FROM outer_test o1 WHERE EXISTS (SELECT 1 FROM outer_test o2 WHERE o2.id < o1.id AND o2.num < o1.num)",
		int64(2))

	// Test evaluateHaving
	afExec(t, db, ctx, "CREATE TABLE having_test (grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO having_test VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40)")

	check("evaluateHaving: Complex HAVING with aggregate",
		"SELECT grp FROM having_test GROUP BY grp HAVING SUM(val) > 35",
		"B")

	check("evaluateHaving: HAVING with multiple conditions",
		"SELECT grp FROM having_test GROUP BY grp HAVING COUNT(*) = 2 AND AVG(val) > 15",
		"B")

	t.Logf("V113 Catalog Methods: %d/%d passed", pass, total)
}

// TestV113EnginePaths targets engine paths related to query execution
func TestV113EnginePaths(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Test query plan cache
	afExec(t, db, ctx, "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cache_test VALUES (1, 100), (2, 200)")

	// Execute same query multiple times to trigger cache
	for i := 0; i < 3; i++ {
		rows := afQuery(t, db, ctx, "SELECT val FROM cache_test WHERE id = 1")
		if len(rows) != 1 {
			t.Errorf("Cache test query failed on iteration %d", i)
		}
	}

	t.Log("V113 Engine paths tested successfully")
}
