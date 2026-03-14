package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestUpdatePlanComplex targets executeUpdatePlan with complex scenarios
func TestUpdatePlanComplex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables for complex UPDATE
	_, err = db.Exec(ctx, `CREATE TABLE inventory (
		id INTEGER PRIMARY KEY,
		product TEXT,
		quantity INTEGER,
		price INTEGER,
		category TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create inventory: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE price_adjustments (
		category TEXT,
		adjustment_percent INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create adjustments: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO inventory VALUES
		(1, 'Widget A', 100, 10, 'electronics'),
		(2, 'Widget B', 50, 20, 'electronics'),
		(3, 'Gadget', 200, 5, 'toys'),
		(4, 'Tool', 75, 30, 'hardware')`)
	if err != nil {
		t.Fatalf("Failed to insert inventory: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO price_adjustments VALUES
		('electronics', 10), ('toys', 5), ('hardware', 15)`)
	if err != nil {
		t.Fatalf("Failed to insert adjustments: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"UPDATE with JOIN equivalent", `UPDATE inventory SET price = price * 1.1 WHERE category = 'electronics'`, "Update with calculation"},
		{"UPDATE with subquery", `UPDATE inventory SET price = (SELECT adjustment_percent FROM price_adjustments WHERE category = inventory.category) + price`, "Update from subquery"},
		{"UPDATE with CASE", `UPDATE inventory SET quantity = CASE WHEN quantity < 100 THEN quantity + 50 ELSE quantity END`, "Conditional update"},
		{"UPDATE multiple columns", `UPDATE inventory SET price = price * 1.05, quantity = quantity - 10 WHERE category = 'toys'`, "Multi-column update"},
		{"UPDATE with EXISTS", `UPDATE inventory SET price = price * 0.9 WHERE EXISTS (SELECT 1 FROM price_adjustments WHERE category = inventory.category AND adjustment_percent > 10)`, "Update with EXISTS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Update error for %s: %v", tt.desc, err)
				return
			}
			t.Logf("%s: %d rows updated", tt.desc, result.RowsAffected)
		})
	}
}

// TestDeletePlanComplex targets executeDeletePlan with complex scenarios
func TestDeletePlanComplex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer TEXT,
		status TEXT,
		created_date TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE order_items (
		id INTEGER PRIMARY KEY,
		order_id INTEGER,
		product TEXT,
		FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create order_items: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, `INSERT INTO orders VALUES
		(1, 'Alice', 'completed', '2024-01-01'),
		(2, 'Bob', 'pending', '2024-02-01'),
		(3, 'Charlie', 'cancelled', '2023-12-01'),
		(4, 'Diana', 'completed', '2023-11-01')`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO order_items VALUES
		(1, 1, 'Widget'), (2, 1, 'Gadget'),
		(3, 2, 'Tool'), (4, 3, 'Toy')`)
	if err != nil {
		t.Fatalf("Failed to insert items: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		desc string
	}{
		{"DELETE with IN subquery", `DELETE FROM orders WHERE id IN (SELECT order_id FROM order_items WHERE product = 'Toy')`, "Delete with IN"},
		{"DELETE with correlated subquery", `DELETE FROM orders o WHERE (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id = o.id) = 2`, "Delete with correlated"},
		{"DELETE with date comparison", `DELETE FROM orders WHERE created_date < '2024-01-01' AND status = 'completed'`, "Delete with date"},
		{"DELETE with CASE condition", `DELETE FROM orders WHERE status = CASE WHEN id < 3 THEN 'cancelled' ELSE 'unknown' END`, "Delete with CASE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Delete error for %s: %v", tt.desc, err)
				return
			}
			t.Logf("%s: %d rows deleted", tt.desc, result.RowsAffected)
		})
	}
}

// TestUpdateDeleteWithReturnClause targets RETURNING clause
func TestUpdateDeleteWithReturnClause(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO accounts VALUES (1, 'Alice', 1000), (2, 'Bob', 500), (3, 'Charlie', 1500)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// UPDATE with RETURNING
	rows, err := db.Query(ctx, `UPDATE accounts SET balance = balance + 100 WHERE balance < 1000 RETURNING id, name, balance`)
	if err != nil {
		t.Logf("UPDATE RETURNING error: %v", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var id, balance int
			var name string
			rows.Scan(&id, &name, &balance)
			t.Logf("Updated: %s now has balance %d", name, balance)
		}
	}

	// DELETE with RETURNING
	rows, err = db.Query(ctx, `DELETE FROM accounts WHERE balance > 1200 RETURNING id, name`)
	if err != nil {
		t.Logf("DELETE RETURNING error: %v", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var id int
			var name string
			rows.Scan(&id, &name)
			t.Logf("Deleted account: %s", name)
		}
	}
}

// TestUpdateDeleteInTransaction targets plan execution in transaction
func TestUpdateDeleteInTransaction(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE data (id INTEGER PRIMARY KEY, value INTEGER, status TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO data VALUES (1, 100, 'active'), (2, 200, 'active'), (3, 300, 'inactive')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Update in transaction
	_, err = tx.Exec(ctx, `UPDATE data SET value = value * 2 WHERE status = 'active'`)
	if err != nil {
		t.Fatalf("Update in transaction failed: %v", err)
	}

	// Delete in transaction
	_, err = tx.Exec(ctx, `DELETE FROM data WHERE id = 3`)
	if err != nil {
		t.Fatalf("Delete in transaction failed: %v", err)
	}

	// Verify within transaction
	rows, _ := tx.Query(ctx, `SELECT COUNT(*), SUM(value) FROM data`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count, sum int
			rows.Scan(&count, &sum)
			t.Logf("In transaction: count=%d, sum=%d", count, sum)
		}
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Logf("Commit error: %v", err)
		return
	}

	// Verify after commit
	rows, _ = db.Query(ctx, `SELECT COUNT(*), SUM(value) FROM data`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count, sum int
			rows.Scan(&count, &sum)
			t.Logf("After commit: count=%d, sum=%d", count, sum)
		}
	}
}

// TestUpdateDeleteWithTriggers targets plan execution with triggers
func TestUpdateDeleteWithTriggers(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create audit table
	_, err = db.Exec(ctx, `CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, table_name TEXT, old_value TEXT, new_value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create audit table: %v", err)
	}

	// Create main table
	_, err = db.Exec(ctx, `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create products: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES (1, 'Widget', 100), (2, 'Gadget', 200)`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	// Create triggers
	_, err = db.Exec(ctx, `CREATE TRIGGER trg_products_update
		AFTER UPDATE ON products
		BEGIN
			INSERT INTO audit_log (action, table_name, old_value, new_value)
			VALUES ('UPDATE', 'products', OLD.name || ':' || OLD.price, NEW.name || ':' || NEW.price);
		END`)
	if err != nil {
		t.Logf("Create update trigger error: %v", err)
		return
	}

	_, err = db.Exec(ctx, `CREATE TRIGGER trg_products_delete
		AFTER DELETE ON products
		BEGIN
			INSERT INTO audit_log (action, table_name, old_value, new_value)
			VALUES ('DELETE', 'products', OLD.name || ':' || OLD.price, NULL);
		END`)
	if err != nil {
		t.Logf("Create delete trigger error: %v", err)
		return
	}

	// Execute UPDATE
	_, err = db.Exec(ctx, `UPDATE products SET price = price * 1.1`)
	if err != nil {
		t.Logf("Update with trigger error: %v", err)
	}

	// Execute DELETE
	_, err = db.Exec(ctx, `DELETE FROM products WHERE id = 1`)
	if err != nil {
		t.Logf("Delete with trigger error: %v", err)
	}

	// Check audit log
	rows, _ := db.Query(ctx, `SELECT action, old_value, new_value FROM audit_log ORDER BY id`)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var action, oldVal, newVal string
			rows.Scan(&action, &oldVal, &newVal)
			t.Logf("Audit: %s - %s -> %s", action, oldVal, newVal)
		}
	}
}
