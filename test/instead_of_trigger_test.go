package test

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestInsteadOfTrigger_Insert(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create base table
	afExec(t, db, ctx, "CREATE TABLE real_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

	// Create view
	afExec(t, db, ctx, "CREATE VIEW user_view AS SELECT id, name, email FROM real_users")

	// Create INSTEAD OF INSERT trigger on view
	afExec(t, db, ctx, "CREATE TRIGGER trg_instead_insert INSTEAD OF INSERT ON user_view BEGIN INSERT INTO real_users VALUES (NEW.id, NEW.name, NEW.email); END")

	// Insert into view - should trigger INSTEAD OF INSERT
	afExec(t, db, ctx, "INSERT INTO user_view VALUES (1, 'Alice', 'alice@example.com')")

	// Verify data in base table
	rows := afQuery(t, db, ctx, "SELECT * FROM real_users")
	if len(rows) != 1 {
		t.Errorf("Expected 1 row in real_users, got %d", len(rows))
	}
}

func TestInsteadOfTrigger_Update(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create base table and insert data
	afExec(t, db, ctx, "CREATE TABLE real_users (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO real_users VALUES (1, 'Alice')")

	// Create view
	afExec(t, db, ctx, "CREATE VIEW user_view AS SELECT id, name FROM real_users")

	// Create INSTEAD OF UPDATE trigger
	afExec(t, db, ctx, "CREATE TRIGGER trg_instead_update INSTEAD OF UPDATE ON user_view BEGIN UPDATE real_users SET name = NEW.name WHERE id = OLD.id; END")

	// Update view - should trigger INSTEAD OF UPDATE
	afExec(t, db, ctx, "UPDATE user_view SET name = 'Alicia' WHERE id = 1")

	// Verify data in base table
	rows := afQuery(t, db, ctx, "SELECT name FROM real_users WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}
	name := rows[0][0]
	if name != "Alicia" {
		t.Errorf("Expected name='Alicia', got %v", name)
	}
}

func TestInsteadOfTrigger_Delete(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create base table and insert data
	afExec(t, db, ctx, "CREATE TABLE real_users (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO real_users VALUES (1, 'Alice'), (2, 'Bob')")

	// Create view
	afExec(t, db, ctx, "CREATE VIEW user_view AS SELECT id, name FROM real_users")

	// Create INSTEAD OF DELETE trigger
	afExec(t, db, ctx, "CREATE TRIGGER trg_instead_delete INSTEAD OF DELETE ON user_view BEGIN DELETE FROM real_users WHERE id = OLD.id; END")

	// Delete from view - should trigger INSTEAD OF DELETE
	afExec(t, db, ctx, "DELETE FROM user_view WHERE id = 1")

	// Verify data in base table
	rows := afQuery(t, db, ctx, "SELECT * FROM real_users")
	if len(rows) != 1 {
		t.Errorf("Expected 1 row in real_users, got %d", len(rows))
	}
}

func TestInsteadOfTrigger_MultiStatement(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create audit table
	afExec(t, db, ctx, "CREATE TABLE audit_log (action TEXT, user_id INTEGER, timestamp TEXT)")
	afExec(t, db, ctx, "CREATE TABLE real_users (id INTEGER PRIMARY KEY, name TEXT)")

	// Create view
	afExec(t, db, ctx, "CREATE VIEW user_view AS SELECT id, name FROM real_users")

	// Create INSTEAD OF INSERT trigger with multiple statements
	afExec(t, db, ctx, "CREATE TRIGGER trg_audit_insert INSTEAD OF INSERT ON user_view BEGIN INSERT INTO real_users VALUES (NEW.id, NEW.name); INSERT INTO audit_log VALUES ('INSERT', NEW.id, datetime('now')); END")

	// Insert into view
	afExec(t, db, ctx, "INSERT INTO user_view VALUES (1, 'Alice')")

	// Verify data in both tables
	users := afQuery(t, db, ctx, "SELECT * FROM real_users")
	if len(users) != 1 {
		t.Errorf("Expected 1 user, got %d", len(users))
	}

	audit := afQuery(t, db, ctx, "SELECT * FROM audit_log")
	if len(audit) != 1 {
		t.Errorf("Expected 1 audit entry, got %d", len(audit))
	}
}
