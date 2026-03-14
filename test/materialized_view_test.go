package test

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestMaterializedView_Basic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create base table
	afExec(t, db, ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)")

	// Create materialized view
	afExec(t, db, ctx, "CREATE MATERIALIZED VIEW mv_sales_summary AS SELECT product, SUM(amount) as total FROM sales GROUP BY product")

	t.Run("Query materialized view", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM mv_sales_summary")
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("Query materialized view with WHERE", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM mv_sales_summary WHERE product = 'A'")
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})

	t.Run("Refresh materialized view", func(t *testing.T) {
		// Add more data
		afExec(t, db, ctx, "INSERT INTO sales VALUES (4, 'C', 300)")

		// Refresh the view
		afExec(t, db, ctx, "REFRESH MATERIALIZED VIEW mv_sales_summary")

		// Check refreshed data
		rows := afQuery(t, db, ctx, "SELECT * FROM mv_sales_summary")
		if len(rows) != 3 {
			t.Errorf("Expected 3 rows after refresh, got %d", len(rows))
		}
	})

	t.Run("Drop materialized view", func(t *testing.T) {
		afExec(t, db, ctx, "DROP MATERIALIZED VIEW mv_sales_summary")

		// Query should fail after drop
		_, err := db.Query(ctx, "SELECT * FROM mv_sales_summary")
		if err == nil {
			t.Error("Expected error querying dropped materialized view")
		}
	})
}

func TestMaterializedView_JoinWithTable(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create tables
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")

	afExec(t, db, ctx, "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")

	// Create materialized view of order totals
	afExec(t, db, ctx, "CREATE MATERIALIZED VIEW mv_customer_totals AS SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id")

	t.Run("JOIN materialized view with table", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT c.name, m.total FROM customers c JOIN mv_customer_totals m ON c.id = m.customer_id")
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})
}

func TestMaterializedView_IF_NOT_EXISTS(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1), (2)")

	// Create view
	afExec(t, db, ctx, "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_test AS SELECT * FROM t")

	// Should not error when creating again with IF NOT EXISTS
	afExec(t, db, ctx, "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_test AS SELECT * FROM t")

	rows := afQuery(t, db, ctx, "SELECT * FROM mv_test")
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

func TestMaterializedView_IF_EXISTS(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER)")
	afExec(t, db, ctx, "CREATE MATERIALIZED VIEW mv_test AS SELECT * FROM t")

	// Drop with IF EXISTS
	afExec(t, db, ctx, "DROP MATERIALIZED VIEW IF EXISTS mv_test")

	// Should not error dropping again with IF EXISTS
	afExec(t, db, ctx, "DROP MATERIALIZED VIEW IF EXISTS mv_test")
}
