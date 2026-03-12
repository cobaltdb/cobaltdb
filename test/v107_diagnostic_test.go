package test

import (
	"fmt"
	"testing"
)

func TestV107Diagnostic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v107d_a (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE v107d_b (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO v107d_a VALUES (1, 'old')")
	afExec(t, db, ctx, "INSERT INTO v107d_a VALUES (2, 'old')")
	afExec(t, db, ctx, "INSERT INTO v107d_a VALUES (3, 'old')")
	afExec(t, db, ctx, "INSERT INTO v107d_b VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v107d_b VALUES (2, 'Bob')")

	// Test: UPDATE...FROM with WHERE filtering to specific row
	_, err := db.Exec(ctx, "UPDATE v107d_a SET val = 'new' FROM v107d_b WHERE v107d_a.id = v107d_b.id AND v107d_b.name = 'Alice'")
	t.Logf("UPDATE FROM result: err=%v", err)

	rows := afQuery(t, db, ctx, "SELECT id, val FROM v107d_a ORDER BY id")
	for _, row := range rows {
		t.Logf("  id=%v val=%v", row[0], row[1])
	}

	// Test: UPDATE...FROM that should affect all matched rows
	_, err = db.Exec(ctx, "UPDATE v107d_a SET val = 'matched' FROM v107d_b WHERE v107d_a.id = v107d_b.id")
	t.Logf("UPDATE FROM all: err=%v", err)

	rows2 := afQuery(t, db, ctx, "SELECT id, val FROM v107d_a ORDER BY id")
	for _, row := range rows2 {
		t.Logf("  id=%v val=%v", row[0], row[1])
	}

	// Test: Does UPDATE without FROM still work?
	_, err = db.Exec(ctx, "UPDATE v107d_a SET val = 'plain' WHERE id = 3")
	t.Logf("Plain UPDATE: err=%v", err)
	
	rows3 := afQuery(t, db, ctx, "SELECT id, val FROM v107d_a ORDER BY id")
	for _, row := range rows3 {
		t.Logf("  id=%v val=%v", row[0], row[1])
	}

	_ = fmt.Sprint("x")
}
