package test

import (
	"fmt"
	"testing"
)

func TestOrderByEdgeCases(t *testing.T) {
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

	checkRows := func(desc string, sql string, expectedCount int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expectedCount {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expectedCount)
			return
		}
		pass++
	}
	_ = checkRows

	// Setup
	afExec(t, db, ctx, "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (1, 'Alice', 'Eng', 130000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (2, 'Bob', 'Eng', 110000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (3, 'Carol', 'Mkt', 95000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (4, 'Dave', 'Sales', 100000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (5, 'Eve', 'Mkt', 88000)")

	// 1. ORDER BY column name
	check("ORDER BY name ASC", "SELECT name FROM emp ORDER BY name ASC LIMIT 1", "Alice")
	check("ORDER BY name DESC", "SELECT name FROM emp ORDER BY name DESC LIMIT 1", "Eve")

	// 2. ORDER BY column not in SELECT
	check("ORDER BY hidden col", "SELECT name FROM emp ORDER BY salary DESC LIMIT 1", "Alice")
	check("ORDER BY hidden col ASC", "SELECT name FROM emp ORDER BY salary ASC LIMIT 1", "Eve")

	// 3. ORDER BY expression
	rows := afQuery(t, db, ctx, "SELECT name, salary FROM emp ORDER BY salary * -1 LIMIT 1")
	t.Logf("ORDER BY expr: %v", rows)
	total++
	if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) == "Alice" {
		pass++
	} else {
		t.Errorf("[FAIL] ORDER BY expression: expected Alice, got %v", rows)
	}

	// 4. ORDER BY alias
	rows = afQuery(t, db, ctx, "SELECT name, salary AS pay FROM emp ORDER BY pay DESC LIMIT 1")
	t.Logf("ORDER BY alias: %v", rows)
	total++
	if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) == "Alice" {
		pass++
	} else {
		t.Errorf("[FAIL] ORDER BY alias: expected Alice, got %v", rows)
	}

	// 5. ORDER BY column number (SQL standard)
	rows = afQuery(t, db, ctx, "SELECT name, salary FROM emp ORDER BY 2 DESC LIMIT 1")
	t.Logf("ORDER BY number: %v", rows)
	total++
	if len(rows) > 0 && fmt.Sprintf("%v", rows[0][0]) == "Alice" {
		pass++
	} else {
		t.Errorf("[FAIL] ORDER BY column number: expected Alice, got %v", rows)
	}

	// 6. Multiple ORDER BY with mixed directions
	rows = afQuery(t, db, ctx, "SELECT dept, name FROM emp ORDER BY dept ASC, salary DESC")
	t.Logf("Multi ORDER BY: %v", rows)
	total++
	if len(rows) == 5 && fmt.Sprintf("%v", rows[0][0]) == "Eng" && fmt.Sprintf("%v", rows[0][1]) == "Alice" {
		pass++
	} else {
		t.Errorf("[FAIL] Multi ORDER BY: expected [Eng Alice] first, got %v", rows)
	}

	// 7. ORDER BY with DISTINCT
	rows = afQuery(t, db, ctx, "SELECT DISTINCT dept FROM emp ORDER BY dept")
	t.Logf("DISTINCT ORDER BY: %v", rows)
	total++
	if len(rows) == 3 && fmt.Sprintf("%v", rows[0][0]) == "Eng" {
		pass++
	} else {
		t.Errorf("[FAIL] DISTINCT ORDER BY: %v", rows)
	}

	// 8. ORDER BY with GROUP BY
	rows = afQuery(t, db, ctx, "SELECT dept, COUNT(*) as cnt FROM emp GROUP BY dept ORDER BY cnt DESC")
	t.Logf("GROUP BY ORDER BY: %v", rows)
	total++
	if len(rows) == 3 && fmt.Sprintf("%v", rows[0][0]) == "Eng" {
		pass++
	} else {
		t.Errorf("[FAIL] GROUP BY ORDER BY: expected Eng first (count=2), got %v", rows)
	}

	// 9. ORDER BY with UNION
	rows = afQuery(t, db, ctx, "SELECT name FROM emp WHERE dept = 'Eng' UNION SELECT name FROM emp WHERE dept = 'Mkt' ORDER BY name LIMIT 3")
	t.Logf("UNION ORDER BY: %v", rows)
	total++
	if len(rows) == 3 && fmt.Sprintf("%v", rows[0][0]) == "Alice" {
		pass++
	} else {
		t.Errorf("[FAIL] UNION ORDER BY: expected Alice first, got %v", rows)
	}

	t.Logf("\n=== ORDER BY EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
