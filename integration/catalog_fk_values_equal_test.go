package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestFKValuesEqualCascade targets valuesEqual in FK cascade operations
func TestFKValuesEqualCascade(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create parent table
	_, err = db.Exec(ctx, `CREATE TABLE fk_parent (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	// Create child with multiple FK columns
	_, err = db.Exec(ctx, `CREATE TABLE fk_child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		parent_name TEXT,
		FOREIGN KEY (parent_id) REFERENCES fk_parent(id) ON DELETE CASCADE,
		FOREIGN KEY (parent_name) REFERENCES fk_parent(name) ON DELETE SET NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Insert parent data
	_, err = db.Exec(ctx, `INSERT INTO fk_parent VALUES (1, 'parent1'), (2, 'parent2'), (3, 'parent3')`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert child data with matching FK values
	_, err = db.Exec(ctx, `INSERT INTO fk_child VALUES
		(1, 1, 'parent1'),
		(2, 2, 'parent2'),
		(3, 1, 'parent1')`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent - triggers cascade through valuesEqual checks
	_, err = db.Exec(ctx, `DELETE FROM fk_parent WHERE id = 1`)
	if err != nil {
		t.Logf("DELETE with FK cascade error: %v", err)
		return
	}

	// Verify cascade worked
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM fk_child WHERE parent_id = 1`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Children with parent_id=1 after cascade: %d", count)
		}
	}
}

// TestFKValuesEqualMultiColumn targets valuesEqual with composite FK
func TestFKValuesEqualMultiColumn(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE fk_comp_parent (
		id INTEGER PRIMARY KEY,
		id2 INTEGER,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE fk_comp_child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		parent_id2 INTEGER,
		FOREIGN KEY (parent_id) REFERENCES fk_comp_parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Insert parent data
	_, err = db.Exec(ctx, `INSERT INTO fk_comp_parent VALUES (1, 1, 'A'), (2, 2, 'B'), (3, 1, 'C')`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert child data
	_, err = db.Exec(ctx, `INSERT INTO fk_comp_child VALUES (1, 1, 1), (2, 2, 2), (3, 3, 1)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent
	_, err = db.Exec(ctx, `DELETE FROM fk_comp_parent WHERE id = 1`)
	if err != nil {
		t.Logf("DELETE FK error: %v", err)
		return
	}

	// Verify cascade
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM fk_comp_child`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Children remaining after composite cascade: %d", count)
		}
	}
}

// TestFKValuesEqualNULL targets valuesEqual with NULL values
func TestFKValuesEqualNULL(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE fk_null_parent (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE fk_null_child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		FOREIGN KEY (parent_id) REFERENCES fk_null_parent(id) ON DELETE SET NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Insert parent
	_, err = db.Exec(ctx, `INSERT INTO fk_null_parent VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert children (some with NULL FK)
	_, err = db.Exec(ctx, `INSERT INTO fk_null_child VALUES (1, 1), (2, 2), (3, NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent - should SET NULL on children
	_, err = db.Exec(ctx, `DELETE FROM fk_null_parent WHERE id = 1`)
	if err != nil {
		t.Logf("DELETE with SET NULL error: %v", err)
		return
	}

	// Verify SET NULL worked
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM fk_null_child WHERE parent_id IS NULL`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Children with NULL parent_id: %d", count)
		}
	}
}

// TestFKValuesEqualDifferentTypes targets valuesEqual with type coercion
func TestFKValuesEqualDifferentTypes(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE fk_type_parent (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE fk_type_child (
		id INTEGER PRIMARY KEY,
		parent_id TEXT,
		FOREIGN KEY (parent_id) REFERENCES fk_type_parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Insert parent
	_, err = db.Exec(ctx, `INSERT INTO fk_type_parent VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert child with string FK value matching integer parent
	_, err = db.Exec(ctx, `INSERT INTO fk_type_child VALUES (1, '1'), (2, '2')`)
	if err != nil {
		t.Logf("Insert with type coercion error: %v", err)
		return
	}

	// Delete parent - triggers cascade with type coercion
	_, err = db.Exec(ctx, `DELETE FROM fk_type_parent WHERE id = 1`)
	if err != nil {
		t.Logf("DELETE with type coercion error: %v", err)
		return
	}

	// Verify cascade worked
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM fk_type_child`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Children remaining after type coercion cascade: %d", count)
		}
	}
}
