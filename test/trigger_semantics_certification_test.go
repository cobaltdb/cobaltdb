package test

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestTrigger_BeforeAfterOrderAndRowImages(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE trig_target (id INTEGER PRIMARY KEY, value INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE trigger_log (seq INTEGER, phase TEXT, row_id INTEGER, old_value INTEGER, new_value INTEGER)")

	afExec(t, db, ctx, `CREATE TRIGGER trg_before_insert BEFORE INSERT ON trig_target
		BEGIN INSERT INTO trigger_log VALUES (1, 'before_insert', NEW.id, NULL, NEW.value); END`)
	afExec(t, db, ctx, `CREATE TRIGGER trg_after_insert AFTER INSERT ON trig_target
		BEGIN INSERT INTO trigger_log VALUES (2, 'after_insert', NEW.id, NULL, NEW.value); END`)
	afExec(t, db, ctx, `CREATE TRIGGER trg_before_update BEFORE UPDATE ON trig_target
		BEGIN INSERT INTO trigger_log VALUES (3, 'before_update', NEW.id, OLD.value, NEW.value); END`)
	afExec(t, db, ctx, `CREATE TRIGGER trg_after_update AFTER UPDATE ON trig_target
		BEGIN INSERT INTO trigger_log VALUES (4, 'after_update', NEW.id, OLD.value, NEW.value); END`)
	afExec(t, db, ctx, `CREATE TRIGGER trg_before_delete BEFORE DELETE ON trig_target
		BEGIN INSERT INTO trigger_log VALUES (5, 'before_delete', OLD.id, OLD.value, NULL); END`)
	afExec(t, db, ctx, `CREATE TRIGGER trg_after_delete AFTER DELETE ON trig_target
		BEGIN INSERT INTO trigger_log VALUES (6, 'after_delete', OLD.id, OLD.value, NULL); END`)

	afExec(t, db, ctx, "INSERT INTO trig_target VALUES (7, 70)")
	afExec(t, db, ctx, "UPDATE trig_target SET value = 90 WHERE id = 7")
	afExec(t, db, ctx, "DELETE FROM trig_target WHERE id = 7")

	rows := afQuery(t, db, ctx, "SELECT phase, row_id, old_value, new_value FROM trigger_log ORDER BY seq")
	expected := [][]interface{}{
		{"before_insert", int64(7), nil, int64(70)},
		{"after_insert", int64(7), nil, int64(70)},
		{"before_update", int64(7), int64(70), int64(90)},
		{"after_update", int64(7), int64(70), int64(90)},
		{"before_delete", int64(7), int64(90), nil},
		{"after_delete", int64(7), int64(90), nil},
	}
	if len(rows) != len(expected) {
		t.Fatalf("Expected %d trigger log rows, got %d: %v", len(expected), len(rows), rows)
	}
	for i := range expected {
		for j := range expected[i] {
			if fmt.Sprint(rows[i][j]) != fmt.Sprint(expected[i][j]) {
				t.Fatalf("Row %d col %d: expected %v, got %v; rows=%v", i, j, expected[i][j], rows[i][j], rows)
			}
		}
	}
}
