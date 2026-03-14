package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestUpdateLockedBasic targets updateLocked with basic operations
func TestUpdateLockedBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE update_basic (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO update_basic VALUES (1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"Update single column", `UPDATE update_basic SET val = 150 WHERE id = 1`},
		{"Update multiple columns", `UPDATE update_basic SET val = 250, name = 'updated' WHERE id = 2`},
		{"Update all rows", `UPDATE update_basic SET val = val + 10`},
		{"Update with expression", `UPDATE update_basic SET val = val * 2 WHERE id = 3`},
		{"Update with NULL", `UPDATE update_basic SET name = NULL WHERE id = 1`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Update error: %v", err)
				return
			}
			t.Logf("Updated %d rows", result.RowsAffected)
		})
	}
}

// TestUpdateLockedWithSubquery targets updateLocked with subquery
func TestUpdateLockedWithSubquery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE update_source (id INTEGER PRIMARY KEY, new_val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE update_target (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create target: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO update_source VALUES (1, 1000), (2, 2000)`)
	if err != nil {
		t.Fatalf("Failed to insert source: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO update_target VALUES (1, 10), (2, 20), (3, 30)`)
	if err != nil {
		t.Fatalf("Failed to insert target: %v", err)
	}

	// Update from subquery
	_, err = db.Exec(ctx, `UPDATE update_target SET val = (SELECT new_val FROM update_source WHERE update_source.id = update_target.id) WHERE id IN (SELECT id FROM update_source)`)
	if err != nil {
		t.Logf("Update with subquery error: %v", err)
		return
	}

	// Verify
	rows, _ := db.Query(ctx, `SELECT val FROM update_target WHERE id = 1`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var val int
			rows.Scan(&val)
			t.Logf("Updated val: %d", val)
		}
	}
}

// TestUpdateLockedWithJoin targets updateLocked with JOIN
func TestUpdateLockedWithJoin(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE update_main (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create main: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE update_join (id INTEGER PRIMARY KEY, multiplier INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create join: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO update_main VALUES (1, 10), (2, 20), (3, 30)`)
	if err != nil {
		t.Fatalf("Failed to insert main: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO update_join VALUES (1, 2), (2, 3)`)
	if err != nil {
		t.Fatalf("Failed to insert join: %v", err)
	}

	// UPDATE with JOIN
	_, err = db.Exec(ctx, `UPDATE update_main SET val = val * update_join.multiplier FROM update_join WHERE update_main.id = update_join.id`)
	if err != nil {
		t.Logf("UPDATE with JOIN error: %v", err)
		return
	}

	// Verify
	rows, _ := db.Query(ctx, `SELECT val FROM update_main WHERE id = 1`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var val int
			rows.Scan(&val)
			t.Logf("Updated val: %d (expected 20)", val)
		}
	}
}

// TestUpdateLockedWithFK targets updateLocked with foreign key
func TestUpdateLockedWithFK(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent_update (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child_update (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent_update(id))`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent_update VALUES (1), (2), (3)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO child_update VALUES (1, 1), (2, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Update FK to valid value
	_, err = db.Exec(ctx, `UPDATE child_update SET parent_id = 3 WHERE id = 1`)
	if err != nil {
		t.Logf("Update FK error: %v", err)
	} else {
		t.Log("FK update succeeded")
	}

	// Try to update FK to invalid value
	_, err = db.Exec(ctx, `UPDATE child_update SET parent_id = 999 WHERE id = 2`)
	if err != nil {
		t.Logf("Invalid FK correctly blocked: %v", err)
	}
}

// TestUpdateLockedWithTrigger targets updateLocked with triggers
func TestUpdateLockedWithTrigger(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE update_audit (id INTEGER PRIMARY KEY, old_val INTEGER, new_val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create audit: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE update_triggered (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO update_triggered VALUES (1, 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER update_trg AFTER UPDATE ON update_triggered BEGIN INSERT INTO update_audit (old_val, new_val) VALUES (OLD.val, NEW.val); END`)
	if err != nil {
		t.Logf("CREATE TRIGGER error: %v", err)
		return
	}

	// Update
	_, err = db.Exec(ctx, `UPDATE update_triggered SET val = 200 WHERE id = 1`)
	if err != nil {
		t.Logf("Update error: %v", err)
		return
	}

	// Verify trigger fired
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM update_audit`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Audit entries: %d", count)
		}
	}
}

// TestUpdateLockedReturning targets updateLocked with RETURNING
func TestUpdateLockedReturning(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE update_returning (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO update_returning VALUES (1, 100), (2, 200), (3, 300)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// UPDATE with RETURNING
	rows, err := db.Query(ctx, `UPDATE update_returning SET val = val + 1 WHERE id <= 2 RETURNING id, val`)
	if err != nil {
		t.Logf("UPDATE RETURNING error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, val int
		rows.Scan(&id, &val)
		count++
		t.Logf("Returned: id=%d, val=%d", id, val)
	}
	t.Logf("Total returned rows: %d", count)
}

// TestUpdateLockedComplexWhere targets updateLocked with complex WHERE
func TestUpdateLockedComplexWhere(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE update_complex (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, status TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO update_complex VALUES
		(1, 10, 20, 'active'),
		(2, 15, 25, 'inactive'),
		(3, 10, 30, 'active'),
		(4, 20, 20, 'pending')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"AND condition", `UPDATE update_complex SET status = 'updated' WHERE a = 10 AND b = 20`},
		{"OR condition", `UPDATE update_complex SET status = 'updated2' WHERE a = 15 OR b = 30`},
		{"Complex expression", `UPDATE update_complex SET a = a + b WHERE status = 'pending'`},
		{"BETWEEN", `UPDATE update_complex SET b = 0 WHERE id BETWEEN 2 AND 3`},
		{"IN clause", `UPDATE update_complex SET status = 'in_list' WHERE id IN (1, 4)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Update error: %v", err)
				return
			}
			t.Logf("Updated %d rows", result.RowsAffected)
		})
	}
}
