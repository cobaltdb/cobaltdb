package test

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestNaturalJoin_Basic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create test tables with common column names
	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE departments (dept_id INTEGER PRIMARY KEY, dept_name TEXT, location TEXT)")

	// Insert test data
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'Alice', 1, 50000), (2, 'Bob', 1, 60000), (3, 'Charlie', 2, 55000)")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (1, 'Engineering', 'Building A'), (2, 'Sales', 'Building B'), (3, 'HR', 'Building C')")

	t.Run("NATURAL JOIN basic", func(t *testing.T) {
		// NATURAL JOIN should match dept_id column automatically
		rows := afQuery(t, db, ctx, "SELECT * FROM employees NATURAL JOIN departments")

		// Should have 3 rows (Alice, Bob, Charlie all have matching dept_id)
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows from NATURAL JOIN, got %d", len(rows))
		}
	})

	t.Run("NATURAL JOIN with column selection", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT id, name, location FROM employees NATURAL JOIN departments")

		// Should have 3 rows
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("NATURAL LEFT JOIN", func(t *testing.T) {
		// NATURAL LEFT JOIN should include all employees even without matching dept
		rows := afQuery(t, db, ctx, "SELECT * FROM employees NATURAL LEFT JOIN departments")

		// Should have 3 rows (all employees - all have matching depts)
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows from NATURAL LEFT JOIN, got %d", len(rows))
		}
	})

	t.Run("NATURAL INNER JOIN explicit", func(t *testing.T) {
		// NATURAL INNER JOIN should be same as NATURAL JOIN
		rows := afQuery(t, db, ctx, "SELECT * FROM employees NATURAL INNER JOIN departments")

		// Should have 3 rows
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows from NATURAL INNER JOIN, got %d", len(rows))
		}
	})

	t.Run("NATURAL JOIN with WHERE clause", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM employees NATURAL JOIN departments WHERE location = 'Building A'")

		// Should have 2 rows (Alice and Bob in Building A)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})
}

func TestNaturalJoin_WithNulls(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create test tables
	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE departments (dept_id INTEGER PRIMARY KEY, dept_name TEXT)")

	// Insert test data with NULL dept_id
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', NULL), (3, 'Charlie', 2)")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")

	t.Run("NATURAL JOIN excludes NULL matches", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM employees NATURAL JOIN departments")

		// Should have 2 rows (Alice and Charlie - Bob has NULL dept_id)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("NATURAL LEFT JOIN includes unmatched", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM employees NATURAL LEFT JOIN departments")

		// Should have 3 rows (all employees)
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(rows))
		}
	})
}

func TestNaturalJoin_MultipleCommonColumns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create tables with multiple common column names
	afExec(t, db, ctx, "CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t2 (a INTEGER, b INTEGER, d INTEGER)")

	// Insert test data - rows match only when both a AND b match
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 1, 100), (1, 2, 200), (2, 1, 300)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 1, 1000), (1, 2, 2000), (3, 1, 3000)")

	t.Run("NATURAL JOIN with multiple common columns", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM t1 NATURAL JOIN t2")

		// Should have 2 rows where both a and b match: (1,1) and (1,2)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})
}

func TestNaturalJoin_NoCommonColumns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create tables with no common column names
	afExec(t, db, ctx, "CREATE TABLE t1 (x INTEGER, y INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t2 (a INTEGER, b INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 2), (3, 4)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (10, 20), (30, 40)")

	t.Run("NATURAL JOIN with no common columns should be cross join", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM t1 NATURAL JOIN t2")

		// Should have 4 rows (cross product since no common columns)
		if len(rows) != 4 {
			t.Errorf("Expected 4 rows (cross product), got %d", len(rows))
		}
	})
}

func TestNaturalJoin_Chained(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create three tables
	afExec(t, db, ctx, "CREATE TABLE a (a_id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE b (a_id INTEGER, b_id INTEGER, value TEXT)")
	afExec(t, db, ctx, "CREATE TABLE c (b_id INTEGER, c_name TEXT)")

	afExec(t, db, ctx, "INSERT INTO a VALUES (1, 'A1'), (2, 'A2')")
	afExec(t, db, ctx, "INSERT INTO b VALUES (1, 10, 'B1'), (1, 11, 'B2'), (2, 12, 'B3')")
	afExec(t, db, ctx, "INSERT INTO c VALUES (10, 'C10'), (11, 'C11'), (12, 'C12')")

	t.Run("Chained NATURAL JOINs", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM a NATURAL JOIN b NATURAL JOIN c")

		// Should have 3 rows - all combinations chain through
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(rows))
		}
	})
}
