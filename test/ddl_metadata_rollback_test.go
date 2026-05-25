package test

import (
	"context"
	"testing"
)

func TestCreateViewRollsBack(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE ddl_view_rb_base (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "BEGIN")
	execSQL(t, db, "CREATE VIEW ddl_view_rb AS SELECT * FROM ddl_view_rb_base")
	execSQL(t, db, "ROLLBACK")

	if _, err := db.Query(ctx, "SELECT * FROM ddl_view_rb"); err == nil {
		t.Fatal("view created inside rolled-back transaction should not exist")
	}
}

func TestDropViewRollsBack(t *testing.T) {
	db := newTestDB(t)

	execSQL(t, db, "CREATE TABLE ddl_drop_view_rb_base (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "INSERT INTO ddl_drop_view_rb_base VALUES (1)")
	execSQL(t, db, "CREATE VIEW ddl_drop_view_rb AS SELECT * FROM ddl_drop_view_rb_base")
	execSQL(t, db, "BEGIN")
	execSQL(t, db, "DROP VIEW ddl_drop_view_rb")
	execSQL(t, db, "ROLLBACK")

	expectSingleValue(t, db, "SELECT COUNT(*) FROM ddl_drop_view_rb", int64(1))
}

func TestCreateTriggerRollsBack(t *testing.T) {
	db := newTestDB(t)

	execSQL(t, db, "CREATE TABLE ddl_trig_rb_main (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "CREATE TABLE ddl_trig_rb_audit (id INTEGER PRIMARY KEY, action TEXT)")
	execSQL(t, db, "BEGIN")
	execSQL(t, db, "CREATE TRIGGER ddl_trig_rb AFTER INSERT ON ddl_trig_rb_main BEGIN INSERT INTO ddl_trig_rb_audit VALUES (NEW.id, 'insert'); END")
	execSQL(t, db, "ROLLBACK")

	execSQL(t, db, "INSERT INTO ddl_trig_rb_main VALUES (1)")
	expectRowCount(t, db, "SELECT * FROM ddl_trig_rb_audit", 0)
}

func TestDropTriggerRollsBack(t *testing.T) {
	db := newTestDB(t)

	execSQL(t, db, "CREATE TABLE ddl_drop_trig_rb_main (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "CREATE TABLE ddl_drop_trig_rb_audit (id INTEGER PRIMARY KEY, action TEXT)")
	execSQL(t, db, "CREATE TRIGGER ddl_drop_trig_rb AFTER INSERT ON ddl_drop_trig_rb_main BEGIN INSERT INTO ddl_drop_trig_rb_audit VALUES (NEW.id, 'insert'); END")
	execSQL(t, db, "BEGIN")
	execSQL(t, db, "DROP TRIGGER ddl_drop_trig_rb")
	execSQL(t, db, "ROLLBACK")

	execSQL(t, db, "INSERT INTO ddl_drop_trig_rb_main VALUES (1)")
	expectSingleValue(t, db, "SELECT COUNT(*) FROM ddl_drop_trig_rb_audit", int64(1))
}
