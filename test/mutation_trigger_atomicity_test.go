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

func TestInsertTriggerBodyRollsBackSideEffectsOnLaterFailure(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE insert_trigger_body_main (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE insert_trigger_body_audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
	execSQL(t, db, `CREATE TRIGGER insert_body_bad_after
		AFTER INSERT ON insert_trigger_body_main
		BEGIN
			INSERT INTO insert_trigger_body_audit VALUES (NEW.id, 'seen');
			INSERT INTO insert_trigger_body_audit VALUES (NEW.id + 100, NULL);
		END`)

	_, err := db.Exec(ctx, "INSERT INTO insert_trigger_body_main VALUES (1, 'one')")
	if err == nil {
		t.Fatal("expected AFTER INSERT trigger body failure")
	}
	expectRowCount(t, db, "SELECT * FROM insert_trigger_body_main", 0)
	expectRowCount(t, db, "SELECT * FROM insert_trigger_body_audit", 0)
}

func TestUpdateTriggerBodyRollsBackSideEffectsOnLaterFailure(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE update_trigger_body_main (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE update_trigger_body_audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
	execSQL(t, db, "INSERT INTO update_trigger_body_main VALUES (1, 'old')")
	execSQL(t, db, `CREATE TRIGGER update_body_bad_after
		AFTER UPDATE ON update_trigger_body_main
		BEGIN
			INSERT INTO update_trigger_body_audit VALUES (NEW.id, 'seen');
			INSERT INTO update_trigger_body_audit VALUES (NEW.id + 100, NULL);
		END`)

	_, err := db.Exec(ctx, "UPDATE update_trigger_body_main SET name = 'new'")
	if err == nil {
		t.Fatal("expected AFTER UPDATE trigger body failure")
	}
	expectSingleValue(t, db, "SELECT name FROM update_trigger_body_main WHERE id = 1", "old")
	expectRowCount(t, db, "SELECT * FROM update_trigger_body_audit", 0)
}

func TestUpdateFromFiresUpdateTriggers(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE update_from_trigger_main (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE update_from_trigger_src (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "CREATE TABLE update_from_trigger_audit (id INTEGER PRIMARY KEY, old_name TEXT, new_name TEXT)")
	execSQL(t, db, "INSERT INTO update_from_trigger_main VALUES (1, 'old'), (2, 'old')")
	execSQL(t, db, "INSERT INTO update_from_trigger_src VALUES (1), (2)")
	execSQL(t, db, `CREATE TRIGGER update_from_audit
		AFTER UPDATE ON update_from_trigger_main
		BEGIN
			INSERT INTO update_from_trigger_audit VALUES (NEW.id, OLD.name, NEW.name);
		END`)

	_, err := db.Exec(ctx, `UPDATE update_from_trigger_main
		SET name = 'new'
		FROM update_from_trigger_src
		WHERE update_from_trigger_main.id = update_from_trigger_src.id`)
	if err != nil {
		t.Fatalf("UPDATE FROM failed: %v", err)
	}
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_from_trigger_audit WHERE old_name = 'old' AND new_name = 'new'", int64(2))
}

func TestDeleteUsingFiresDeleteTriggers(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE delete_using_trigger_main (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE delete_using_trigger_src (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "CREATE TABLE delete_using_trigger_audit (id INTEGER PRIMARY KEY, old_name TEXT)")
	execSQL(t, db, "INSERT INTO delete_using_trigger_main VALUES (1, 'one'), (2, 'two')")
	execSQL(t, db, "INSERT INTO delete_using_trigger_src VALUES (1), (2)")
	execSQL(t, db, `CREATE TRIGGER delete_using_audit
		AFTER DELETE ON delete_using_trigger_main
		BEGIN
			INSERT INTO delete_using_trigger_audit VALUES (OLD.id, OLD.name);
		END`)

	_, err := db.Exec(ctx, `DELETE FROM delete_using_trigger_main
		USING delete_using_trigger_src
		WHERE delete_using_trigger_main.id = delete_using_trigger_src.id`)
	if err != nil {
		t.Fatalf("DELETE USING failed: %v", err)
	}
	expectSingleValue(t, db, "SELECT COUNT(*) FROM delete_using_trigger_audit", int64(2))
}

func TestUpdateFromRollsBackOnAfterTriggerError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE update_from_trigger_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE update_from_trigger_atomicity_src (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "CREATE TABLE update_from_trigger_atomicity_audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
	execSQL(t, db, "INSERT INTO update_from_trigger_atomicity VALUES (1, 'old'), (2, 'old')")
	execSQL(t, db, "INSERT INTO update_from_trigger_atomicity_src VALUES (1), (2)")
	execSQL(t, db, `CREATE TRIGGER update_from_bad_after
		AFTER UPDATE ON update_from_trigger_atomicity
		BEGIN
			INSERT INTO update_from_trigger_atomicity_audit VALUES (NEW.id, NULL);
		END`)

	_, err := db.Exec(ctx, `UPDATE update_from_trigger_atomicity
		SET name = 'new'
		FROM update_from_trigger_atomicity_src
		WHERE update_from_trigger_atomicity.id = update_from_trigger_atomicity_src.id`)
	if err == nil {
		t.Fatal("expected UPDATE FROM AFTER trigger failure")
	}
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_from_trigger_atomicity WHERE name = 'new'", int64(0))
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_from_trigger_atomicity WHERE name = 'old'", int64(2))
	expectRowCount(t, db, "SELECT * FROM update_from_trigger_atomicity_audit", 0)
}

func TestDeleteUsingRollsBackOnAfterTriggerError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE delete_using_trigger_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE delete_using_trigger_atomicity_src (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "CREATE TABLE delete_using_trigger_atomicity_audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
	execSQL(t, db, "INSERT INTO delete_using_trigger_atomicity VALUES (1, 'one'), (2, 'two')")
	execSQL(t, db, "INSERT INTO delete_using_trigger_atomicity_src VALUES (1), (2)")
	execSQL(t, db, `CREATE TRIGGER delete_using_bad_after
		AFTER DELETE ON delete_using_trigger_atomicity
		BEGIN
			INSERT INTO delete_using_trigger_atomicity_audit VALUES (OLD.id, NULL);
		END`)

	_, err := db.Exec(ctx, `DELETE FROM delete_using_trigger_atomicity
		USING delete_using_trigger_atomicity_src
		WHERE delete_using_trigger_atomicity.id = delete_using_trigger_atomicity_src.id`)
	if err == nil {
		t.Fatal("expected DELETE USING AFTER trigger failure")
	}
	expectRowCount(t, db, "SELECT * FROM delete_using_trigger_atomicity", 2)
	expectRowCount(t, db, "SELECT * FROM delete_using_trigger_atomicity_audit", 0)
}
