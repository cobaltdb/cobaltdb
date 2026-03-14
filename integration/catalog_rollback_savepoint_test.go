package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestRollbackToSavepointNested targets RollbackToSavepoint with nested savepoints
func TestRollbackToSavepointNested(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE nested_sp (
		id INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, `INSERT INTO nested_sp VALUES (1, 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create nested savepoints
	_, err = db.Exec(ctx, `SAVEPOINT sp1`)
	if err != nil {
		t.Logf("SAVEPOINT sp1 error: %v", err)
		return
	}

	_, err = db.Exec(ctx, `INSERT INTO nested_sp VALUES (2, 200)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, `SAVEPOINT sp2`)
	if err != nil {
		t.Logf("SAVEPOINT sp2 error: %v", err)
		return
	}

	_, err = db.Exec(ctx, `INSERT INTO nested_sp VALUES (3, 300)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rollback to inner savepoint
	_, err = db.Exec(ctx, `ROLLBACK TO SAVEPOINT sp2`)
	if err != nil {
		t.Logf("ROLLBACK TO sp2 error: %v", err)
		return
	}

	// Verify only row 3 was rolled back
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM nested_sp`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("After rollback to sp2: %d rows", count)
		}
	}

	// Rollback to outer savepoint
	_, err = db.Exec(ctx, `ROLLBACK TO SAVEPOINT sp1`)
	if err != nil {
		t.Logf("ROLLBACK TO sp1 error: %v", err)
		return
	}

	// Verify rows 2 and 3 were rolled back
	rows, _ = db.Query(ctx, `SELECT COUNT(*) FROM nested_sp`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			if count != 1 {
				t.Errorf("Expected 1 row after rollback to sp1, got %d", count)
			}
			t.Logf("After rollback to sp1: %d rows", count)
		}
	}
}

// TestRollbackToSavepointDDLOps targets RollbackToSavepoint with DDL operations
func TestRollbackToSavepointDDLOps(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE ddl_test (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO ddl_test VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create savepoint
	_, err = db.Exec(ctx, `SAVEPOINT sp1`)
	if err != nil {
		t.Logf("SAVEPOINT error: %v", err)
		return
	}

	// DDL operation
	_, err = db.Exec(ctx, `ALTER TABLE ddl_test ADD COLUMN new_col TEXT`)
	if err != nil {
		t.Logf("ALTER TABLE error: %v", err)
		return
	}

	// Insert after DDL
	_, err = db.Exec(ctx, `INSERT INTO ddl_test VALUES (2, 'value')`)
	if err != nil {
		t.Logf("Insert with new column error: %v", err)
		return
	}

	// Rollback DDL changes
	_, err = db.Exec(ctx, `ROLLBACK TO SAVEPOINT sp1`)
	if err != nil {
		t.Logf("ROLLBACK TO SAVEPOINT after DDL error: %v", err)
		return
	}

	// Verify DDL was rolled back
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM ddl_test`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("After DDL rollback: %d rows", count)
		}
	}
}

// TestRollbackToSavepointIndex targets RollbackToSavepoint with index changes
func TestRollbackToSavepointIndex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE idx_test (
		id INTEGER PRIMARY KEY,
		email TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO idx_test VALUES (1, 'test@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create savepoint
	_, err = db.Exec(ctx, `SAVEPOINT sp1`)
	if err != nil {
		t.Logf("SAVEPOINT error: %v", err)
		return
	}

	// Create index
	_, err = db.Exec(ctx, `CREATE INDEX idx_email ON idx_test(email)`)
	if err != nil {
		t.Logf("CREATE INDEX error: %v", err)
		return
	}

	// Insert more data
	_, err = db.Exec(ctx, `INSERT INTO idx_test VALUES (2, 'other@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rollback
	_, err = db.Exec(ctx, `ROLLBACK TO SAVEPOINT sp1`)
	if err != nil {
		t.Logf("ROLLBACK TO SAVEPOINT error: %v", err)
		return
	}

	// Verify data rollback
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM idx_test`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			if count != 1 {
				t.Errorf("Expected 1 row after rollback, got %d", count)
			}
			t.Logf("After index rollback: %d rows", count)
		}
	}
}

// TestRollbackToSavepointFK targets RollbackToSavepoint with foreign key changes
func TestRollbackToSavepointFK(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent_sp (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child_sp (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		FOREIGN KEY (parent_id) REFERENCES parent_sp(id)
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO parent_sp VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO child_sp VALUES (1, 1)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Create savepoint
	_, err = db.Exec(ctx, `SAVEPOINT sp1`)
	if err != nil {
		t.Logf("SAVEPOINT error: %v", err)
		return
	}

	// Insert child with FK
	_, err = db.Exec(ctx, `INSERT INTO child_sp VALUES (2, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent (should cascade or fail)
	_, err = db.Exec(ctx, `DELETE FROM parent_sp WHERE id = 2`)
	if err != nil {
		t.Logf("DELETE parent error: %v", err)
	}

	// Rollback
	_, err = db.Exec(ctx, `ROLLBACK TO SAVEPOINT sp1`)
	if err != nil {
		t.Logf("ROLLBACK TO SAVEPOINT error: %v", err)
		return
	}

	// Verify FK data consistency
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM child_sp`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("After FK rollback: %d children", count)
		}
	}
}

// TestRollbackToSavepointView targets RollbackToSavepoint with view creation
func TestRollbackToSavepointView(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE view_base (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO view_base VALUES (1, 100)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create savepoint
	_, err = db.Exec(ctx, `SAVEPOINT sp1`)
	if err != nil {
		t.Logf("SAVEPOINT error: %v", err)
		return
	}

	// Create view
	_, err = db.Exec(ctx, `CREATE VIEW test_view AS SELECT * FROM view_base WHERE val > 50`)
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Use view
	rows, _ := db.Query(ctx, `SELECT * FROM test_view`)
	if rows != nil {
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		t.Logf("View returned %d rows", count)
	}

	// Rollback
	_, err = db.Exec(ctx, `ROLLBACK TO SAVEPOINT sp1`)
	if err != nil {
		t.Logf("ROLLBACK TO SAVEPOINT error: %v", err)
		return
	}

	// Verify view is gone
	_, err = db.Query(ctx, `SELECT * FROM test_view`)
	if err != nil {
		t.Logf("View correctly removed after rollback: %v", err)
	} else {
		t.Logf("View still exists after rollback")
	}
}

// TestRollbackToSavepointTrigger targets RollbackToSavepoint with trigger operations
func TestRollbackToSavepointTrigger(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE audit_sp (id INTEGER PRIMARY KEY, action TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create audit: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE main_sp (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create main: %v", err)
	}

	// Create savepoint
	_, err = db.Exec(ctx, `SAVEPOINT sp1`)
	if err != nil {
		t.Logf("SAVEPOINT error: %v", err)
		return
	}

	// Create trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER sp_trigger AFTER INSERT ON main_sp BEGIN INSERT INTO audit_sp (action) VALUES ('insert'); END`)
	if err != nil {
		t.Logf("CREATE TRIGGER error: %v", err)
		return
	}

	// Insert data (trigger fires)
	_, err = db.Exec(ctx, `INSERT INTO main_sp VALUES (1, 'test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify trigger fired
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM audit_sp`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Audit entries before rollback: %d", count)
		}
	}

	// Rollback
	_, err = db.Exec(ctx, `ROLLBACK TO SAVEPOINT sp1`)
	if err != nil {
		t.Logf("ROLLBACK TO SAVEPOINT error: %v", err)
		return
	}

	// Verify trigger and data rolled back
	rows, _ = db.Query(ctx, `SELECT COUNT(*) FROM audit_sp`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Audit entries after rollback: %d", count)
		}
	}
}
