package test

import (
	"fmt"
	"testing"
)

func TestV14ForeignKeyCascade(t *testing.T) {
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

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expected, len(rows))
			return
		}
		pass++
	}

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
			return
		}
		pass++
	}

	// ============================================================
	// === FK VALIDATION ON INSERT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (2, 'Marketing')")

	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id))")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (1, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (2, 'Bob', 2)")

	// FK violation on INSERT
	checkError("FK violation on INSERT", "INSERT INTO employees VALUES (3, 'Charlie', 999)")

	// NULL FK value should be allowed
	afExec(t, db, ctx, "INSERT INTO employees VALUES (3, 'Charlie', NULL)")
	checkRowCount("NULL FK accepted", "SELECT * FROM employees WHERE dept_id IS NULL", 1)

	// ============================================================
	// === FK VALIDATION ON UPDATE ===
	// ============================================================
	// Valid FK update
	afExec(t, db, ctx, "UPDATE employees SET dept_id = 2 WHERE id = 1")
	check("FK update accepted", "SELECT dept_id FROM employees WHERE id = 1", 2)

	// Invalid FK update - reference to non-existent department
	checkError("FK violation on UPDATE", "UPDATE employees SET dept_id = 999 WHERE id = 1")

	// Verify original value preserved after failed update
	check("Original FK preserved", "SELECT dept_id FROM employees WHERE id = 1", 2)

	// Update FK to NULL should succeed
	afExec(t, db, ctx, "UPDATE employees SET dept_id = NULL WHERE id = 1")
	checkRowCount("FK set to NULL", "SELECT * FROM employees WHERE id = 1 AND dept_id IS NULL", 1)

	// Restore original value
	afExec(t, db, ctx, "UPDATE employees SET dept_id = 1 WHERE id = 1")

	// ============================================================
	// === CASCADE DELETE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'Parent1')")
	afExec(t, db, ctx, "INSERT INTO parents VALUES (2, 'Parent2')")

	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT, FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE)")
	afExec(t, db, ctx, "INSERT INTO children VALUES (1, 1, 'Child1A')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (2, 1, 'Child1B')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (3, 2, 'Child2A')")

	// Delete parent should cascade to children
	afExec(t, db, ctx, "DELETE FROM parents WHERE id = 1")
	checkRowCount("Parent deleted", "SELECT * FROM parents WHERE id = 1", 0)
	checkRowCount("Children cascaded", "SELECT * FROM children WHERE parent_id = 1", 0)
	checkRowCount("Other children intact", "SELECT * FROM children WHERE parent_id = 2", 1)

	// ============================================================
	// === SET NULL ON DELETE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (1, 'Cat1')")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (2, 'Cat2')")

	afExec(t, db, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT, FOREIGN KEY (cat_id) REFERENCES categories(id) ON DELETE SET NULL)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (1, 1, 'Item1')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (2, 1, 'Item2')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (3, 2, 'Item3')")

	// Delete category should set FK to NULL in items
	afExec(t, db, ctx, "DELETE FROM categories WHERE id = 1")
	checkRowCount("Category deleted", "SELECT * FROM categories WHERE id = 1", 0)
	checkRowCount("Items with NULL cat_id", "SELECT * FROM items WHERE cat_id IS NULL", 2)
	check("Item3 still has cat", "SELECT cat_id FROM items WHERE id = 3", 2)

	// ============================================================
	// === RESTRICT ON DELETE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE courses (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO courses VALUES (1, 'Math')")

	afExec(t, db, ctx, "CREATE TABLE enrollments (id INTEGER PRIMARY KEY, course_id INTEGER, FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE RESTRICT)")
	afExec(t, db, ctx, "INSERT INTO enrollments VALUES (1, 1)")

	// RESTRICT should prevent deletion when references exist
	checkError("RESTRICT prevents delete", "DELETE FROM courses WHERE id = 1")
	checkRowCount("Course still exists", "SELECT * FROM courses WHERE id = 1", 1)

	// ============================================================
	// === UPDATE CHECK CONSTRAINT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, price INTEGER CHECK (price >= 0))")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 100)")

	// Valid update
	afExec(t, db, ctx, "UPDATE products SET price = 50 WHERE id = 1")
	check("Check passed update", "SELECT price FROM products WHERE id = 1", 50)

	// Invalid update violates CHECK
	checkError("CHECK violation on UPDATE", "UPDATE products SET price = -1 WHERE id = 1")
	check("Price unchanged after CHECK fail", "SELECT price FROM products WHERE id = 1", 50)

	// ============================================================
	// === UPDATE UNIQUE CONSTRAINT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
	afExec(t, db, ctx, "INSERT INTO users VALUES (1, 'alice@test.com')")
	afExec(t, db, ctx, "INSERT INTO users VALUES (2, 'bob@test.com')")

	// Valid unique update
	afExec(t, db, ctx, "UPDATE users SET email = 'alice2@test.com' WHERE id = 1")
	check("Unique update accepted", "SELECT email FROM users WHERE id = 1", "alice2@test.com")

	// Duplicate email update should fail
	checkError("UNIQUE violation on UPDATE", "UPDATE users SET email = 'bob@test.com' WHERE id = 1")

	t.Logf("\n=== V14 FK CASCADE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
