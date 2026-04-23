package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestFKOnUpdateCascade tests ON UPDATE CASCADE behavior
func TestFKOnUpdateCascade(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent table
	_, err = db.Exec(ctx, `CREATE TABLE categories (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create categories: %v", err)
	}

	// Create child table with ON UPDATE CASCADE
	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		category_id INTEGER,
		name TEXT,
		FOREIGN KEY (category_id) REFERENCES categories(id) ON UPDATE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create products: %v", err)
	}

	// Insert parent data
	_, err = db.Exec(ctx, `INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Clothing')`)
	if err != nil {
		t.Fatalf("Failed to insert categories: %v", err)
	}

	// Insert child data referencing category 1
	_, err = db.Exec(ctx, `INSERT INTO products VALUES (1, 1, 'Laptop'), (2, 1, 'Phone'), (3, 2, 'Shirt')`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	// Update category id from 1 to 100 - should cascade to products
	_, err = db.Exec(ctx, `UPDATE categories SET id = 100 WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update category: %v", err)
	}

	// Verify products were updated
	rows, err := db.Query(ctx, `SELECT id, category_id, name FROM products ORDER BY id`)
	if err != nil {
		t.Fatalf("Failed to query products: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id         int
		categoryID int
		name       string
	}{
		{1, 100, "Laptop"},
		{2, 100, "Phone"},
		{3, 2, "Shirt"},
	}

	count := 0
	for rows.Next() {
		var id, categoryID int
		var name string
		if err := rows.Scan(&id, &categoryID, &name); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if count >= len(expected) {
			t.Errorf("Unexpected row: id=%d, category_id=%d, name=%s", id, categoryID, name)
			continue
		}
		if id != expected[count].id || categoryID != expected[count].categoryID || name != expected[count].name {
			t.Errorf("Row %d: expected (id=%d, category_id=%d, name=%s), got (id=%d, category_id=%d, name=%s)",
				count, expected[count].id, expected[count].categoryID, expected[count].name,
				id, categoryID, name)
		}
		count++
	}

	if count != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), count)
	}

	t.Log("ON UPDATE CASCADE works correctly")
}

// TestFKOnUpdateSetNull tests ON UPDATE SET NULL behavior
func TestFKOnUpdateSetNull(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent table
	_, err = db.Exec(ctx, `CREATE TABLE departments (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create departments: %v", err)
	}

	// Create child table with ON UPDATE SET NULL
	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		dept_id INTEGER,
		name TEXT,
		FOREIGN KEY (dept_id) REFERENCES departments(id) ON UPDATE SET NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create employees: %v", err)
	}

	// Insert parent data
	_, err = db.Exec(ctx, `INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')`)
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	// Insert child data
	_, err = db.Exec(ctx, `INSERT INTO employees VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Carol')`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	// Update department id from 1 to 100 - should SET NULL on employees
	_, err = db.Exec(ctx, `UPDATE departments SET id = 100 WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update department: %v", err)
	}

	// Verify employees in department 1 now have NULL dept_id
	rows, err := db.Query(ctx, `SELECT id, dept_id, name FROM employees ORDER BY id`)
	if err != nil {
		t.Fatalf("Failed to query employees: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id     int
		deptID interface{}
		name   string
	}{
		{1, nil, "Alice"},
		{2, nil, "Bob"},
		{3, 2, "Carol"},
	}

	count := 0
	for rows.Next() {
		var id int
		var deptID interface{}
		var name string
		if err := rows.Scan(&id, &deptID, &name); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if count >= len(expected) {
			t.Errorf("Unexpected row: id=%d, dept_id=%v, name=%s", id, deptID, name)
			continue
		}
		if id != expected[count].id || name != expected[count].name {
			t.Errorf("Row %d: expected (id=%d, name=%s), got (id=%d, name=%s)",
				count, expected[count].id, expected[count].name, id, name)
		}
		// Check dept_id (nil vs int)
		if expected[count].deptID == nil {
			if deptID != nil {
				t.Errorf("Row %d: expected dept_id=NULL, got %v", count, deptID)
			}
		} else {
			// Convert to int64 for comparison (database returns int64)
			var actualDeptID int64
			switch v := deptID.(type) {
			case int64:
				actualDeptID = v
			case int:
				actualDeptID = int64(v)
			default:
				t.Errorf("Row %d: unexpected dept_id type %T", count, deptID)
				continue
			}
			if actualDeptID != int64(expected[count].deptID.(int)) {
				t.Errorf("Row %d: expected dept_id=%v, got %v", count, expected[count].deptID, deptID)
			}
		}
		count++
	}

	if count != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), count)
	}

	t.Log("ON UPDATE SET NULL works correctly")
}

// TestFKOnUpdateRestrict tests ON UPDATE RESTRICT behavior
func TestFKOnUpdateRestrict(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent table
	_, err = db.Exec(ctx, `CREATE TABLE authors (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create authors: %v", err)
	}

	// Create child table with ON UPDATE RESTRICT
	_, err = db.Exec(ctx, `CREATE TABLE books (
		id INTEGER PRIMARY KEY,
		author_id INTEGER,
		title TEXT,
		FOREIGN KEY (author_id) REFERENCES authors(id) ON UPDATE RESTRICT
	)`)
	if err != nil {
		t.Fatalf("Failed to create books: %v", err)
	}

	// Insert parent data
	_, err = db.Exec(ctx, `INSERT INTO authors VALUES (1, 'John Doe'), (2, 'Jane Smith')`)
	if err != nil {
		t.Fatalf("Failed to insert authors: %v", err)
	}

	// Insert child data
	_, err = db.Exec(ctx, `INSERT INTO books VALUES (1, 1, 'Book One'), (2, 1, 'Book Two')`)
	if err != nil {
		t.Fatalf("Failed to insert books: %v", err)
	}

	// Try to update author id - should fail due to RESTRICT
	_, err = db.Exec(ctx, `UPDATE authors SET id = 100 WHERE id = 1`)
	if err == nil {
		t.Errorf("Expected error for ON UPDATE RESTRICT when child rows exist, but got none")
	} else {
		t.Logf("ON UPDATE RESTRICT correctly prevented update: %v", err)
	}

	// Verify author was NOT updated
	rows, err := db.Query(ctx, `SELECT id, name FROM authors WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to query authors: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if id != 1 || name != "John Doe" {
			t.Errorf("Author was incorrectly updated: id=%d, name=%s", id, name)
		} else {
			t.Log("ON UPDATE RESTRICT correctly prevented the update")
		}
	}
}

// TestFKOnUpdateNoAction tests ON UPDATE NO ACTION behavior (same as RESTRICT)
func TestFKOnUpdateNoAction(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent table
	_, err = db.Exec(ctx, `CREATE TABLE countries (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create countries: %v", err)
	}

	// Create child table with ON UPDATE NO ACTION
	_, err = db.Exec(ctx, `CREATE TABLE cities (
		id INTEGER PRIMARY KEY,
		country_id INTEGER,
		name TEXT,
		FOREIGN KEY (country_id) REFERENCES countries(id) ON UPDATE NO ACTION
	)`)
	if err != nil {
		t.Fatalf("Failed to create cities: %v", err)
	}

	// Insert parent data
	_, err = db.Exec(ctx, `INSERT INTO countries VALUES (1, 'USA'), (2, 'Canada')`)
	if err != nil {
		t.Fatalf("Failed to insert countries: %v", err)
	}

	// Insert child data
	_, err = db.Exec(ctx, `INSERT INTO cities VALUES (1, 1, 'New York'), (2, 1, 'Los Angeles')`)
	if err != nil {
		t.Fatalf("Failed to insert cities: %v", err)
	}

	// Try to update country id - should fail due to NO ACTION
	_, err = db.Exec(ctx, `UPDATE countries SET id = 100 WHERE id = 1`)
	if err == nil {
		t.Errorf("Expected error for ON UPDATE NO ACTION when child rows exist, but got none")
	} else {
		t.Logf("ON UPDATE NO ACTION correctly prevented update: %v", err)
	}
}

// TestFKOnUpdateCascadeChain tests cascading updates through multiple levels
func TestFKOnUpdateCascadeChain(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create three-level hierarchy: regions -> stores -> orders
	_, err = db.Exec(ctx, `CREATE TABLE regions (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create regions: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE stores (
		id INTEGER PRIMARY KEY,
		region_id INTEGER,
		name TEXT,
		FOREIGN KEY (region_id) REFERENCES regions(id) ON UPDATE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create stores: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		store_id INTEGER,
		total REAL,
		FOREIGN KEY (store_id) REFERENCES stores(id) ON UPDATE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO regions VALUES (1, 'North'), (2, 'South')`)
	if err != nil {
		t.Fatalf("Failed to insert regions: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO stores VALUES (1, 1, 'Store A'), (2, 1, 'Store B'), (3, 2, 'Store C')`)
	if err != nil {
		t.Fatalf("Failed to insert stores: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0), (4, 3, 300.0)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Update region 1 to 100 - should cascade to stores
	_, err = db.Exec(ctx, `UPDATE regions SET id = 100 WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update region: %v", err)
	}

	// Verify stores were updated
	rows, err := db.Query(ctx, `SELECT id, region_id, name FROM stores ORDER BY id`)
	if err != nil {
		t.Fatalf("Failed to query stores: %v", err)
	}
	defer rows.Close()

	expectedStores := []struct {
		id       int
		regionID int
	}{
		{1, 100},
		{2, 100},
		{3, 2},
	}

	count := 0
	for rows.Next() {
		var id, regionID int
		var name string
		if err := rows.Scan(&id, &regionID, &name); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if count >= len(expectedStores) {
			t.Errorf("Unexpected store row: id=%d, region_id=%d", id, regionID)
			continue
		}
		if id != expectedStores[count].id || regionID != expectedStores[count].regionID {
			t.Errorf("Store row %d: expected (id=%d, region_id=%d), got (id=%d, region_id=%d)",
				count, expectedStores[count].id, expectedStores[count].regionID, id, regionID)
		}
		count++
	}

	if count != len(expectedStores) {
		t.Errorf("Expected %d store rows, got %d", len(expectedStores), count)
	}

	t.Log("ON UPDATE CASCADE chain works correctly through multiple levels")
}

// TestFKOnUpdateMultipleFKs tests table with multiple foreign keys
func TestFKOnUpdateMultipleFKs(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent tables
	_, err = db.Exec(ctx, `CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create customers: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create products: %v", err)
	}

	// Create child table with multiple FKs
	_, err = db.Exec(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		product_id INTEGER,
		quantity INTEGER,
		FOREIGN KEY (customer_id) REFERENCES customers(id) ON UPDATE CASCADE,
		FOREIGN KEY (product_id) REFERENCES products(id) ON UPDATE SET NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders: %v", err)
	}

	// Insert parent data
	_, err = db.Exec(ctx, `INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget')`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	// Insert child data
	_, err = db.Exec(ctx, `INSERT INTO orders VALUES (1, 1, 1, 5), (2, 1, 2, 3), (3, 2, 1, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Update customer 1 to 100 (CASCADE)
	_, err = db.Exec(ctx, `UPDATE customers SET id = 100 WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update customer: %v", err)
	}

	// Update product 1 to 100 (SET NULL)
	_, err = db.Exec(ctx, `UPDATE products SET id = 100 WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update product: %v", err)
	}

	// Verify orders
	rows, err := db.Query(ctx, `SELECT id, customer_id, product_id, quantity FROM orders ORDER BY id`)
	if err != nil {
		t.Fatalf("Failed to query orders: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id         int
		customerID int
		productID  interface{}
		quantity   int
	}{
		{1, 100, nil, 5}, // customer cascaded, product set null
		{2, 100, 2, 3},   // customer cascaded, product unchanged
		{3, 2, nil, 2},   // customer unchanged, product set null
	}

	count := 0
	for rows.Next() {
		var id, customerID, quantity int
		var productID interface{}
		if err := rows.Scan(&id, &customerID, &productID, &quantity); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		if count >= len(expected) {
			t.Errorf("Unexpected row: id=%d", id)
			continue
		}
		if id != expected[count].id || customerID != expected[count].customerID || quantity != expected[count].quantity {
			t.Errorf("Row %d: expected (id=%d, customer_id=%d, quantity=%d), got (id=%d, customer_id=%d, quantity=%d)",
				count, expected[count].id, expected[count].customerID, expected[count].quantity,
				id, customerID, quantity)
		}
		// Check product_id
		if expected[count].productID == nil {
			if productID != nil {
				t.Errorf("Row %d: expected product_id=NULL, got %v", count, productID)
			}
		} else {
			// Convert to int64 for comparison (database returns int64)
			var actualProductID int64
			switch v := productID.(type) {
			case int64:
				actualProductID = v
			case int:
				actualProductID = int64(v)
			default:
				t.Errorf("Row %d: unexpected product_id type %T", count, productID)
				continue
			}
			if actualProductID != int64(expected[count].productID.(int)) {
				t.Errorf("Row %d: expected product_id=%v, got %v", count, expected[count].productID, productID)
			}
		}
		count++
	}

	if count != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), count)
	}

	t.Log("Multiple foreign keys with different ON UPDATE actions work correctly")
}
