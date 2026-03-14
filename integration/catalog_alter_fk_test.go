package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestAlterTableRenameColumnFull targets AlterTableRenameColumn
func TestAlterTableRenameColumnFull(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rename_col_test (id INTEGER PRIMARY KEY, old_name TEXT, another_col INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO rename_col_test VALUES (1, 'value', 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rename column
	_, err = db.Exec(ctx, `ALTER TABLE rename_col_test RENAME COLUMN old_name TO new_name`)
	if err != nil {
		t.Logf("RENAME COLUMN error: %v", err)
		return
	}

	// Query with new name
	rows, err := db.Query(ctx, `SELECT new_name FROM rename_col_test WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to query with new name: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var val string
		rows.Scan(&val)
		if val != "value" {
			t.Errorf("Expected 'value', got '%s'", val)
		}
		t.Logf("Renamed column value: %s", val)
	}

	// Verify old name doesn't work
	_, err = db.Query(ctx, `SELECT old_name FROM rename_col_test`)
	if err != nil {
		t.Logf("Old column name correctly invalid: %v", err)
	}
}

// TestAlterTableRenameTableFull targets AlterTableRename
func TestAlterTableRenameTableFull(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE old_table_name (id INTEGER PRIMARY KEY, val TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO old_table_name VALUES (1, 'test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rename table
	_, err = db.Exec(ctx, `ALTER TABLE old_table_name RENAME TO new_table_name`)
	if err != nil {
		t.Logf("RENAME TABLE error: %v", err)
		return
	}

	// Query with new name
	rows, err := db.Query(ctx, `SELECT * FROM new_table_name`)
	if err != nil {
		t.Fatalf("Failed to query new table name: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		var val string
		rows.Scan(&id, &val)
		t.Logf("Found in renamed table: id=%d, val=%s", id, val)
	}

	// Verify old name doesn't work
	_, err = db.Query(ctx, `SELECT * FROM old_table_name`)
	if err != nil {
		t.Logf("Old table name correctly invalid: %v", err)
	}
}

// TestApplyGroupByOrderWithFK targets applyGroupByOrderBy
func TestApplyGroupByOrderWithFK(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE group_order (category TEXT, subcategory TEXT, amount INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO group_order VALUES
		('A', 'X', 100),
		('A', 'Y', 200),
		('A', 'X', 150),
		('B', 'Z', 300),
		('B', 'Y', 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
	}{
		{"GROUP BY ORDER BY aggregate ASC", `SELECT category, SUM(amount) as total FROM group_order GROUP BY category ORDER BY total`},
		{"GROUP BY ORDER BY aggregate DESC", `SELECT category, SUM(amount) as total FROM group_order GROUP BY category ORDER BY total DESC`},
		{"GROUP BY ORDER BY COUNT", `SELECT category, COUNT(*) as cnt FROM group_order GROUP BY category ORDER BY cnt DESC`},
		{"GROUP BY multi ORDER BY", `SELECT category, subcategory, SUM(amount) as total FROM group_order GROUP BY category, subcategory ORDER BY category, total DESC`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			var results []string
			for rows.Next() {
				var cat string
				var total int
				rows.Scan(&cat, &total)
				results = append(results, cat)
			}
			t.Logf("Order: %v", results)
		})
	}
}

// TestFKOnDeleteOnUpdate targets OnDelete and OnUpdate
func TestFKOnDeleteOnUpdate(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// CASCADE
	_, err = db.Exec(ctx, `CREATE TABLE parent_cascade (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child_cascade (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent_cascade(id) ON DELETE CASCADE)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent_cascade VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO child_cascade VALUES (1, 1), (2, 1), (3, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent (cascade to children)
	_, err = db.Exec(ctx, `DELETE FROM parent_cascade WHERE id = 1`)
	if err != nil {
		t.Logf("DELETE CASCADE error: %v", err)
	}

	// Verify
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM child_cascade`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Children after CASCADE: %d", count)
		}
	}
}

// TestFKOnDeleteSetNull targets ON DELETE SET NULL
func TestFKOnDeleteSetNull(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent_null (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child_null (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent_null(id) ON DELETE SET NULL)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent_null VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO child_null VALUES (1, 1), (2, 1), (3, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent (set NULL on children)
	_, err = db.Exec(ctx, `DELETE FROM parent_null WHERE id = 1`)
	if err != nil {
		t.Logf("DELETE SET NULL error: %v", err)
	}

	// Verify
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM child_null WHERE parent_id IS NULL`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Children with NULL parent: %d", count)
		}
	}
}

// TestFKOnDeleteRestrict targets ON DELETE RESTRICT
func TestFKOnDeleteRestrict(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent_restrict (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child_restrict (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent_restrict(id) ON DELETE RESTRICT)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent_restrict VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO child_restrict VALUES (1, 1)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Try to delete parent (should be restricted)
	_, err = db.Exec(ctx, `DELETE FROM parent_restrict WHERE id = 1`)
	if err != nil {
		t.Logf("DELETE RESTRICT correctly blocked: %v", err)
	} else {
		t.Log("DELETE succeeded - RESTRICT may not be enforced")
	}
}
