package test

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestRightJoin_Basic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create test tables
	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE departments (id INTEGER PRIMARY KEY, dept_name TEXT)")

	// Insert test data - some departments have no employees
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2)")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")

	t.Run("RIGHT JOIN basic", func(t *testing.T) {
		// RIGHT JOIN should return all departments even without employees
		rows := afQuery(t, db, ctx, "SELECT * FROM employees RIGHT JOIN departments ON employees.dept_id = departments.id")

		// Should have 4 rows (Alice, Bob, Charlie in depts 1,2 + HR with NULL employee)
		if len(rows) != 4 {
			t.Errorf("Expected 4 rows from RIGHT JOIN, got %d", len(rows))
		}
	})

	t.Run("RIGHT OUTER JOIN explicit", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM employees RIGHT OUTER JOIN departments ON employees.dept_id = departments.id")

		if len(rows) != 4 {
			t.Errorf("Expected 4 rows from RIGHT OUTER JOIN, got %d", len(rows))
		}
	})

	t.Run("RIGHT JOIN with column selection", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name, dept_name FROM employees RIGHT JOIN departments ON employees.dept_id = departments.id")

		if len(rows) != 4 {
			t.Errorf("Expected 4 rows, got %d", len(rows))
		}
	})

	t.Run("RIGHT JOIN with WHERE clause", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM employees RIGHT JOIN departments ON employees.dept_id = departments.id WHERE dept_name = 'HR'")

		// Should have 1 row (HR department with no employee)
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})
}

func TestRightJoin_EmptyLeftTable(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER, value TEXT)")

	// Only insert into right table
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 'A'), (2, 'B')")

	t.Run("RIGHT JOIN with empty left table", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id")

		// Should have 2 rows (all from t2 with NULL t1 values)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})
}

func TestRightJoin_Chained(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE a (a_id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE b (b_id INTEGER, a_id INTEGER, value TEXT)")
	afExec(t, db, ctx, "CREATE TABLE c (c_id INTEGER, b_id INTEGER, data TEXT)")

	afExec(t, db, ctx, "INSERT INTO a VALUES (1, 'A1'), (2, 'A2')")
	afExec(t, db, ctx, "INSERT INTO b VALUES (10, 1, 'B10'), (20, 2, 'B20'), (30, 99, 'B30')") // 30 has no matching a
	afExec(t, db, ctx, "INSERT INTO c VALUES (100, 10, 'C100'), (200, 20, 'C200'), (300, 999, 'C300')") // 300 has no matching b

	t.Run("Chained RIGHT JOINs", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM a RIGHT JOIN b ON a.a_id = b.a_id RIGHT JOIN c ON b.b_id = c.b_id")

		// Should include all rows from c (3 rows)
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(rows))
		}
	})
}
