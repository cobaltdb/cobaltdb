package test

import (
	"testing"
)

// compareValues handles numeric type comparisons
func compareValues(got, expected interface{}) bool {
	if got == nil && expected == nil {
		return true
	}
	if got == nil || expected == nil {
		return false
	}

	switch e := expected.(type) {
	case int64:
		switch g := got.(type) {
		case int64:
			return g == e
		case int:
			return int64(g) == e
		case int32:
			return int64(g) == e
		case float64:
			return int64(g) == e
		case float32:
			return int64(g) == e
		}
	case int:
		switch g := got.(type) {
		case int64:
			return g == int64(e)
		case int:
			return g == e
		case float64:
			return int(g) == e
		}
	case float64:
		switch g := got.(type) {
		case float64:
			return g == e
		case int64:
			return float64(g) == e
		case int:
			return float64(g) == e
		}
	case string:
		return got == e
	}

	// Fallback to direct comparison
	return got == expected
}

// TestV114SelectLockedComplexJoins tests complex JOIN scenarios for selectLocked coverage
func TestV114SelectLockedComplexJoins(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Setup test tables
	afExec(t, db, ctx, "CREATE TABLE join_a (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE join_b (id INTEGER PRIMARY KEY, a_id INTEGER, data TEXT)")
	afExec(t, db, ctx, "CREATE TABLE join_c (id INTEGER PRIMARY KEY, b_id INTEGER, info TEXT)")
	afExec(t, db, ctx, "INSERT INTO join_a VALUES (1, 10, 'Alice'), (2, 20, 'Bob'), (3, 30, 'Charlie')")
	afExec(t, db, ctx, "INSERT INTO join_b VALUES (1, 1, 'Data1'), (2, 1, 'Data2'), (3, 2, 'Data3'), (4, 3, 'Data4')")
	afExec(t, db, ctx, "INSERT INTO join_c VALUES (1, 1, 'Info1'), (2, 2, 'Info2'), (3, 4, 'Info3')")

	pass := 0
	total := 0

	checkRows := func(desc string, sql string, minRows int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) >= minRows {
			pass++
		} else {
			t.Errorf("[FAIL] %s: expected at least %d rows, got %d", desc, minRows, len(rows))
		}
	}

	// Three-way join
	checkRows("Three-way INNER JOIN",
		"SELECT a.name, b.data, c.info FROM join_a a JOIN join_b b ON a.id = b.a_id JOIN join_c c ON b.id = c.b_id",
		1)

	// LEFT JOIN with NULLs
	checkRows("LEFT JOIN",
		"SELECT a.name, b.data FROM join_a a LEFT JOIN join_b b ON a.id = b.a_id",
		3)

	// Self join
	checkRows("Self JOIN",
		"SELECT a1.name, a2.name FROM join_a a1 JOIN join_a a2 ON a1.val < a2.val",
		1)

	// Cross join (limited)
	checkRows("CROSS JOIN",
		"SELECT COUNT(*) FROM join_a CROSS JOIN join_b",
		1)

	t.Logf("V114 Complex Joins: %d/%d passed", pass, total)
}

// TestV114SelectLockedWhereClauses tests WHERE clause variations
func TestV114SelectLockedWhereClauses(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE where_test (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO where_test VALUES (1, 10, 'Alice'), (2, 20, 'Bob'), (3, 30, 'Charlie'), (4, NULL, 'David')")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("WHERE with AND/OR",
		"SELECT COUNT(*) FROM where_test WHERE (val > 10 AND val < 30) OR name = 'Alice'",
		int64(2))

	check("WHERE with IN",
		"SELECT COUNT(*) FROM where_test WHERE id IN (1, 2, 5)",
		int64(2))

	check("WHERE with BETWEEN",
		"SELECT COUNT(*) FROM where_test WHERE val BETWEEN 15 AND 25",
		int64(1))

	check("WHERE with LIKE",
		"SELECT COUNT(*) FROM where_test WHERE name LIKE 'A%'",
		int64(1))

	check("WHERE with IS NOT NULL",
		"SELECT COUNT(*) FROM where_test WHERE val IS NOT NULL",
		int64(3))

	check("WHERE with EXISTS",
		"SELECT COUNT(*) FROM where_test w WHERE EXISTS (SELECT 1 FROM where_test w2 WHERE w2.val > w.val)",
		int64(2))

	t.Logf("V114 WHERE Clauses: %d/%d passed", pass, total)
}

// TestV114SelectLockedAggregates tests aggregate functions
func TestV114SelectLockedAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE agg_test (id INTEGER PRIMARY KEY, group_id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO agg_test VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 2, 50)")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("COUNT with GROUP BY",
		"SELECT COUNT(*) FROM agg_test GROUP BY group_id HAVING COUNT(*) > 1 LIMIT 1",
		int64(2))

	check("SUM",
		"SELECT SUM(val) FROM agg_test WHERE group_id = 2",
		int64(120))

	check("MAX",
		"SELECT MAX(val) FROM agg_test",
		int64(50))

	check("MIN",
		"SELECT MIN(val) FROM agg_test",
		int64(10))

	check("HAVING clause",
		"SELECT group_id FROM agg_test GROUP BY group_id HAVING SUM(val) > 50 LIMIT 1",
		int64(2))

	// AVG returns float64 - use approximate check
	checkFloat := func(desc string, sql string, expected float64) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		var gotFloat float64
		switch v := got.(type) {
		case float64:
			gotFloat = v
		case int64:
			gotFloat = float64(v)
		case int:
			gotFloat = float64(v)
		default:
			t.Errorf("[FAIL] %s: unexpected type %T", desc, got)
			return
		}
		// Allow 0.1% tolerance for float comparison
		if gotFloat < expected*0.999 || gotFloat > expected*1.001 {
			t.Errorf("[FAIL] %s: got %v, expected %v", desc, gotFloat, expected)
			return
		}
		pass++
	}

	checkFloat("AVG", "SELECT AVG(val) FROM agg_test", 30.0)

	t.Logf("V114 Aggregates: %d/%d passed", pass, total)
}

// TestV114SelectLockedWindowFunctions tests window functions
func TestV114SelectLockedWindowFunctions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE window_test (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO window_test VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'C')")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("ROW_NUMBER",
		"SELECT rn FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM window_test) AS subq WHERE id = 2",
		int64(2))

	check("RANK",
		"SELECT r FROM (SELECT id, RANK() OVER (ORDER BY val) AS r FROM window_test) AS subq WHERE id = 3",
		int64(3))

	check("DENSE_RANK",
		"SELECT dr FROM (SELECT id, DENSE_RANK() OVER (ORDER BY val) AS dr FROM window_test) AS subq WHERE id = 2",
		int64(2))

	check("LAG",
		"SELECT prev FROM (SELECT id, LAG(val, 1, 0) OVER (ORDER BY id) AS prev FROM window_test) AS subq WHERE id = 2",
		int64(10))

	check("LEAD",
		"SELECT next_val FROM (SELECT id, LEAD(val, 1, 999) OVER (ORDER BY id) AS next_val FROM window_test) AS subq WHERE id = 3",
		int64(999))

	check("SUM OVER",
		"SELECT s FROM (SELECT id, SUM(val) OVER (ORDER BY id) AS s FROM window_test) AS subq WHERE id = 3",
		int64(60))

	t.Logf("V114 Window Functions: %d/%d passed", pass, total)
}

// TestV114SelectLockedCTEs tests Common Table Expressions
func TestV114SelectLockedCTEs(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE cte_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cte_test VALUES (1, 10), (2, 20), (3, 30)")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("Simple CTE",
		"WITH cte AS (SELECT * FROM cte_test WHERE val > 15) SELECT COUNT(*) FROM cte",
		int64(2))

	check("Multiple CTEs",
		"WITH cte1 AS (SELECT id FROM cte_test), cte2 AS (SELECT id FROM cte_test WHERE val > 20) SELECT COUNT(*) FROM cte1, cte2 WHERE cte1.id = cte2.id",
		int64(1))

	check("Recursive CTE",
		"WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 5) SELECT COUNT(*) FROM nums",
		int64(5))

	check("CTE with JOIN",
		"WITH cte AS (SELECT id, val FROM cte_test) SELECT COUNT(*) FROM cte c JOIN cte_test t ON c.id = t.id",
		int64(3))

	t.Logf("V114 CTEs: %d/%d passed", pass, total)
}

// TestV114SelectLockedSubqueries tests subquery patterns
func TestV114SelectLockedSubqueries(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE sub_a (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE sub_b (id INTEGER PRIMARY KEY, a_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO sub_a VALUES (1, 10), (2, 20), (3, 30)")
	afExec(t, db, ctx, "INSERT INTO sub_b VALUES (1, 1), (2, 1), (3, 2)")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("Scalar subquery",
		"SELECT cnt FROM (SELECT id, (SELECT COUNT(*) FROM sub_b WHERE a_id = sub_a.id) AS cnt FROM sub_a) AS subq WHERE id = 1",
		int64(2))

	check("IN subquery",
		"SELECT COUNT(*) FROM sub_a WHERE id IN (SELECT a_id FROM sub_b)",
		int64(2))

	check("EXISTS subquery",
		"SELECT COUNT(*) FROM sub_a a WHERE EXISTS (SELECT 1 FROM sub_b b WHERE b.a_id = a.id)",
		int64(2))

	check("Correlated subquery",
		"SELECT COUNT(*) FROM sub_a WHERE val > (SELECT AVG(val) FROM sub_a)",
		int64(1))

	t.Logf("V114 Subqueries: %d/%d passed", pass, total)
}

// TestV114SelectLockedSetOperations tests UNION/INTERSECT/EXCEPT
func TestV114SelectLockedSetOperations(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE set_a (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "CREATE TABLE set_b (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO set_a VALUES (1), (2), (3)")
	afExec(t, db, ctx, "INSERT INTO set_b VALUES (2), (3), (4)")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("UNION",
		"SELECT COUNT(*) FROM (SELECT id FROM set_a UNION SELECT id FROM set_b) AS subq",
		int64(4))

	check("UNION ALL",
		"SELECT COUNT(*) FROM (SELECT id FROM set_a UNION ALL SELECT id FROM set_b) AS subq",
		int64(6))

	check("INTERSECT",
		"SELECT COUNT(*) FROM (SELECT id FROM set_a INTERSECT SELECT id FROM set_b) AS subq",
		int64(2))

	check("EXCEPT",
		"SELECT COUNT(*) FROM (SELECT id FROM set_a EXCEPT SELECT id FROM set_b) AS subq",
		int64(1))

	t.Logf("V114 Set Operations: %d/%d passed", pass, total)
}

// TestV114SelectLockedOrderByLimit tests ORDER BY and LIMIT
func TestV114SelectLockedOrderByLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE sort_test (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO sort_test VALUES (1, 30, 'C'), (2, 10, 'A'), (3, 20, 'B')")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("ORDER BY ASC",
		"SELECT id FROM sort_test ORDER BY val ASC LIMIT 1",
		int64(2))

	check("ORDER BY DESC",
		"SELECT id FROM sort_test ORDER BY val DESC LIMIT 1",
		int64(1))

	check("LIMIT OFFSET",
		"SELECT id FROM sort_test ORDER BY id LIMIT 1 OFFSET 1",
		int64(2))

	check("Multiple ORDER BY",
		"SELECT id FROM sort_test ORDER BY val ASC, name DESC LIMIT 1",
		int64(2))

	t.Logf("V114 ORDER BY/LIMIT: %d/%d passed", pass, total)
}

// TestV114SelectLockedDistinct tests DISTINCT
func TestV114SelectLockedDistinct(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE distinct_test (id INTEGER PRIMARY KEY, group_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO distinct_test VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 2)")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("DISTINCT",
		"SELECT COUNT(DISTINCT group_id) FROM distinct_test",
		int64(2))

	check("DISTINCT with multiple columns",
		"SELECT COUNT(*) FROM (SELECT DISTINCT group_id, id FROM distinct_test) AS subq",
		int64(5))

	t.Logf("V114 DISTINCT: %d/%d passed", pass, total)
}

// TestV114SelectLockedViews tests views
func TestV114SelectLockedViews(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE view_base (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO view_base VALUES (1, 10), (2, 20), (3, 30)")
	afExec(t, db, ctx, "CREATE VIEW test_view AS SELECT * FROM view_base WHERE val > 15")

	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows", desc)
			return
		}
		got := rows[0][0]
		if !compareValues(got, expected) {
			t.Errorf("[FAIL] %s: got %v (%T), expected %v (%T)", desc, got, got, expected, expected)
			return
		}
		pass++
	}

	check("Query view",
		"SELECT COUNT(*) FROM test_view",
		int64(2))

	check("View with aggregate",
		"SELECT SUM(val) FROM test_view",
		int64(50))

	t.Logf("V114 Views: %d/%d passed", pass, total)
}
