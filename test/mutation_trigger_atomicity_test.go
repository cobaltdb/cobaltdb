package test

import (
	"context"
	"testing"
)

func TestUpdateRollsBackOnAfterTriggerError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE update_trigger_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE update_trigger_audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
	execSQL(t, db, "INSERT INTO update_trigger_atomicity VALUES (1, 'old'), (2, 'old')")
	execSQL(t, db, `CREATE TRIGGER update_bad_after
		AFTER UPDATE ON update_trigger_atomicity
		BEGIN
			INSERT INTO update_trigger_audit VALUES (NEW.id, NULL);
		END`)

	_, err := db.Exec(ctx, "UPDATE update_trigger_atomicity SET name = 'new'")
	if err == nil {
		t.Fatal("expected AFTER UPDATE trigger failure")
	}
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_trigger_atomicity WHERE name = 'new'", int64(0))
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_trigger_atomicity WHERE name = 'old'", int64(2))
	expectRowCount(t, db, "SELECT * FROM update_trigger_audit", 0)
}

func TestDeleteRollsBackOnAfterTriggerError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE delete_trigger_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE delete_trigger_audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
	execSQL(t, db, "INSERT INTO delete_trigger_atomicity VALUES (1, 'one'), (2, 'two')")
	execSQL(t, db, `CREATE TRIGGER delete_bad_after
		AFTER DELETE ON delete_trigger_atomicity
		BEGIN
			INSERT INTO delete_trigger_audit VALUES (OLD.id, NULL);
		END`)

	_, err := db.Exec(ctx, "DELETE FROM delete_trigger_atomicity")
	if err == nil {
		t.Fatal("expected AFTER DELETE trigger failure")
	}
	expectRowCount(t, db, "SELECT * FROM delete_trigger_atomicity", 2)
	expectRowCount(t, db, "SELECT * FROM delete_trigger_audit", 0)
}
