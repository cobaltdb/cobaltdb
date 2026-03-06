package test

import (
	"fmt"
	"testing"
)

// TestV104JoinGroupByOrderBy specifically targets:
// - evaluateExprWithGroupAggregatesJoin (68.4%) - AVG/MIN/MAX/COUNT on joined data
// - applyGroupByOrderBy (67.8%) - ORDER BY with aggregate functions, positional refs, strings
// - evaluateFunctionCall (77.7%) - PRINTF, CONCAT_WS, LEFT, RIGHT, LPAD, RPAD, HEX, CHAR, QUOTE, GLOB, UNICODE
// - evaluateJSONFunction (71.8%) - JSON_PRETTY, JSON_MINIFY, JSON_QUOTE, JSON_KEYS
// - applyOrderBy (73.1%) - Expression ORDER BY, QualifiedIdentifier ORDER BY
func TestV104JoinGroupByOrderBy(t *testing.T) {
	db, ctx := af(t)
	_ = fmt.Sprintf

	// Setup: two tables for JOIN queries
	afExec(t, db, ctx, "CREATE TABLE v104_depts (id INTEGER PRIMARY KEY, dname TEXT)")
	afExec(t, db, ctx, "INSERT INTO v104_depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v104_depts VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO v104_depts VALUES (3, 'Marketing')")

	afExec(t, db, ctx, "CREATE TABLE v104_emps (id INTEGER PRIMARY KEY, dept_id INTEGER, salary REAL, bonus REAL)")
	afExec(t, db, ctx, "INSERT INTO v104_emps VALUES (1, 1, 100.0, 10.0)")
	afExec(t, db, ctx, "INSERT INTO v104_emps VALUES (2, 1, 200.0, 20.0)")
	afExec(t, db, ctx, "INSERT INTO v104_emps VALUES (3, 1, 150.0, NULL)")
	afExec(t, db, ctx, "INSERT INTO v104_emps VALUES (4, 2, 80.0, 5.0)")
	afExec(t, db, ctx, "INSERT INTO v104_emps VALUES (5, 2, 120.0, 15.0)")
	afExec(t, db, ctx, "INSERT INTO v104_emps VALUES (6, 3, 90.0, NULL)")

	t.Run("JoinGroupByAVG", func(t *testing.T) {
		// Hit evaluateExprWithGroupAggregatesJoin AVG path
		rows := afQuery(t, db, ctx, "SELECT d.dname, AVG(e.salary) FROM v104_depts d JOIN v104_emps e ON d.id = e.dept_id GROUP BY d.dname ORDER BY d.dname")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("JoinGroupByMINMAX", func(t *testing.T) {
		// Hit evaluateExprWithGroupAggregatesJoin MIN/MAX paths
		rows := afQuery(t, db, ctx, "SELECT d.dname, MIN(e.salary), MAX(e.salary) FROM v104_depts d JOIN v104_emps e ON d.id = e.dept_id GROUP BY d.dname ORDER BY d.dname")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("JoinGroupByCOUNTColumn", func(t *testing.T) {
		// Hit evaluateExprWithGroupAggregatesJoin COUNT(column) with NULLs
		rows := afQuery(t, db, ctx, "SELECT d.dname, COUNT(e.bonus) FROM v104_depts d JOIN v104_emps e ON d.id = e.dept_id GROUP BY d.dname ORDER BY d.dname")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("JoinGroupBySUM_OrderByAggregate", func(t *testing.T) {
		// Hit applyGroupByOrderBy FunctionCall+Identifier arg in ORDER BY
		rows := afQuery(t, db, ctx, "SELECT d.dname, SUM(e.salary) FROM v104_depts d JOIN v104_emps e ON d.id = e.dept_id GROUP BY d.dname ORDER BY SUM(e.salary) DESC")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("JoinGroupByOrderByPositional", func(t *testing.T) {
		// Hit applyGroupByOrderBy positional ORDER BY (NumberLiteral)
		rows := afQuery(t, db, ctx, "SELECT d.dname, SUM(e.salary) FROM v104_depts d JOIN v104_emps e ON d.id = e.dept_id GROUP BY d.dname ORDER BY 2 DESC")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("JoinGroupByOrderByString", func(t *testing.T) {
		// Hit applyGroupByOrderBy string comparison path
		rows := afQuery(t, db, ctx, "SELECT d.dname, COUNT(*) FROM v104_depts d JOIN v104_emps e ON d.id = e.dept_id GROUP BY d.dname ORDER BY d.dname DESC")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("JoinGroupByOrderByExpressionArg", func(t *testing.T) {
		// Hit applyGroupByOrderBy expression-arg aggregate path: SUM(salary * bonus)
		rows := afQuery(t, db, ctx, "SELECT d.dname, SUM(e.salary + e.bonus) FROM v104_depts d JOIN v104_emps e ON d.id = e.dept_id GROUP BY d.dname ORDER BY SUM(e.salary + e.bonus) DESC")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("JoinGroupByNullInOrderBy", func(t *testing.T) {
		// NULL values in ORDER BY after GROUP BY
		afExec(t, db, ctx, "CREATE TABLE v104_nulldept (id INTEGER PRIMARY KEY, dept_id INTEGER, val INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v104_nulldept VALUES (1, 1, 10)")
		afExec(t, db, ctx, "INSERT INTO v104_nulldept VALUES (2, 1, NULL)")
		afExec(t, db, ctx, "INSERT INTO v104_nulldept VALUES (3, 2, 20)")
		rows := afQuery(t, db, ctx, "SELECT d.dname, SUM(n.val) FROM v104_depts d JOIN v104_nulldept n ON d.id = n.dept_id GROUP BY d.dname ORDER BY SUM(n.val)")
		if len(rows) < 2 {
			t.Fatalf("expected at least 2 rows, got %d", len(rows))
		}
	})

	// SQL functions coverage
	t.Run("FunctionCoverage", func(t *testing.T) {
		// PRINTF
		afExpectVal(t, db, ctx, "SELECT PRINTF('%s has %d items', 'Store', 42)", "Store has 42 items")

		// CONCAT_WS
		afExpectVal(t, db, ctx, "SELECT CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c")

		// LEFT / RIGHT
		afExpectVal(t, db, ctx, "SELECT LEFT('hello', 3)", "hel")
		afExpectVal(t, db, ctx, "SELECT RIGHT('hello', 3)", "llo")

		// LPAD / RPAD
		afExpectVal(t, db, ctx, "SELECT LPAD('hi', 5, '0')", "000hi")
		afExpectVal(t, db, ctx, "SELECT RPAD('hi', 5, '0')", "hi000")

		// HEX
		afExpectVal(t, db, ctx, "SELECT HEX(255)", "FF")

		// CHAR
		afExpectVal(t, db, ctx, "SELECT CHAR(65, 66, 67)", "ABC")

		// UNICODE
		afExpectVal(t, db, ctx, "SELECT UNICODE('A')", float64(65))

		// QUOTE
		afExpectVal(t, db, ctx, "SELECT QUOTE(NULL)", "NULL")

		// GLOB
		afExpectVal(t, db, ctx, "SELECT GLOB('*.txt', 'file.txt')", true)
		afExpectVal(t, db, ctx, "SELECT GLOB('*.txt', 'file.csv')", false)

		// ZEROBLOB - returns a string of null bytes
		rows := afQuery(t, db, ctx, "SELECT LENGTH(ZEROBLOB(4))")
		if len(rows) < 1 {
			t.Fatal("expected result from ZEROBLOB")
		}
	})

	// JSON functions
	t.Run("JSONFunctionCoverage", func(t *testing.T) {
		// JSON_PRETTY
		rows := afQuery(t, db, ctx, "SELECT JSON_PRETTY('{\"a\":1}')")
		if len(rows) < 1 {
			t.Fatal("expected result from JSON_PRETTY")
		}

		// JSON_MINIFY
		rows = afQuery(t, db, ctx, "SELECT JSON_MINIFY('{  \"a\" :  1  }')")
		if len(rows) < 1 {
			t.Fatal("expected result from JSON_MINIFY")
		}

		// JSON_QUOTE
		afExpectVal(t, db, ctx, "SELECT JSON_QUOTE('hello')", "\"hello\"")

		// JSON_KEYS
		rows = afQuery(t, db, ctx, "SELECT JSON_KEYS('{\"a\":1,\"b\":2}')")
		if len(rows) < 1 {
			t.Fatal("expected result from JSON_KEYS")
		}
	})

	// Expression ORDER BY (applyOrderBy paths)
	t.Run("ExpressionOrderBy", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v104_items (name TEXT, price REAL, qty INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v104_items VALUES ('a', 10.0, 5)")
		afExec(t, db, ctx, "INSERT INTO v104_items VALUES ('b', 5.0, 10)")
		afExec(t, db, ctx, "INSERT INTO v104_items VALUES ('c', 7.0, 3)")

		// Expression ORDER BY (non-column expression)
		rows := afQuery(t, db, ctx, "SELECT name, price * qty AS total FROM v104_items ORDER BY price * qty DESC")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	// Derived table with UNION (executeDerivedTable paths)
	t.Run("DerivedTableUnion", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT val FROM (SELECT 1 AS val UNION SELECT 2 UNION SELECT 3) AS sub ORDER BY val")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	// Recursive CTE (ExecuteCTE paths)
	t.Run("RecursiveCTE", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5) SELECT x FROM cnt")
		if len(rows) != 5 {
			t.Fatalf("expected 5 rows, got %d", len(rows))
		}
	})

	// ANALYZE with the fixed code (should now populate column stats)
	t.Run("AnalyzeWithData", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v104_stats (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
		afExec(t, db, ctx, "INSERT INTO v104_stats VALUES (1, 'alice', 85.5)")
		afExec(t, db, ctx, "INSERT INTO v104_stats VALUES (2, 'bob', 92.3)")
		afExec(t, db, ctx, "INSERT INTO v104_stats VALUES (3, 'carol', NULL)")
		afExec(t, db, ctx, "INSERT INTO v104_stats VALUES (4, NULL, 78.1)")
		afExec(t, db, ctx, "ANALYZE v104_stats")
		// Just verify it doesn't error
	})

	// Window function edge cases (evalWindowExprOnRow)
	t.Run("WindowFunctions", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v104_wf (grp TEXT, val INTEGER, label TEXT)")
		afExec(t, db, ctx, "INSERT INTO v104_wf VALUES ('A', 1, 'first')")
		afExec(t, db, ctx, "INSERT INTO v104_wf VALUES ('A', 2, 'second')")
		afExec(t, db, ctx, "INSERT INTO v104_wf VALUES ('B', 3, 'third')")
		afExec(t, db, ctx, "INSERT INTO v104_wf VALUES ('B', 4, 'fourth')")

		// Running SUM with PARTITION BY + ORDER BY
		rows := afQuery(t, db, ctx, "SELECT grp, val, SUM(val) OVER (PARTITION BY grp ORDER BY val) FROM v104_wf")
		if len(rows) != 4 {
			t.Fatalf("expected 4 rows, got %d", len(rows))
		}

		// Running COUNT with ORDER BY
		rows = afQuery(t, db, ctx, "SELECT grp, val, COUNT(*) OVER (PARTITION BY grp ORDER BY val) FROM v104_wf")
		if len(rows) != 4 {
			t.Fatalf("expected 4 rows, got %d", len(rows))
		}

		// AVG over partition (no ORDER BY = whole partition)
		rows = afQuery(t, db, ctx, "SELECT grp, val, AVG(val) OVER (PARTITION BY grp) FROM v104_wf")
		if len(rows) != 4 {
			t.Fatalf("expected 4 rows, got %d", len(rows))
		}

		// MIN/MAX over partition
		rows = afQuery(t, db, ctx, "SELECT grp, val, MIN(val) OVER (PARTITION BY grp), MAX(val) OVER (PARTITION BY grp) FROM v104_wf")
		if len(rows) != 4 {
			t.Fatalf("expected 4 rows, got %d", len(rows))
		}
	})

	// AlterTableRenameColumn
	t.Run("AlterTableRenameColumn", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v104_rename (id INTEGER PRIMARY KEY, old_col TEXT, other TEXT)")
		afExec(t, db, ctx, "INSERT INTO v104_rename VALUES (1, 'test', 'x')")
		afExec(t, db, ctx, "ALTER TABLE v104_rename RENAME COLUMN old_col TO new_col")
		afExpectVal(t, db, ctx, "SELECT new_col FROM v104_rename WHERE id = 1", "test")
	})

	// CTE with UNION (non-recursive)
	t.Run("CTEWithUnion", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "WITH combined AS (SELECT 1 AS v UNION SELECT 2 UNION SELECT 3) SELECT v FROM combined ORDER BY v")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})
}
