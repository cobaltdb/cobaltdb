package test

import (
	"context"
	"testing"
)

func TestMultiInsertRowsAffectedAndReturning(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE multi_insert_regression (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

	result, err := db.Exec(ctx, "INSERT INTO multi_insert_regression VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	if err != nil {
		t.Fatalf("multi-row insert failed: %v", err)
	}
	if result.RowsAffected != 3 {
		t.Fatalf("RowsAffected = %d, want 3", result.RowsAffected)
	}
	expectRowCount(t, db, "SELECT * FROM multi_insert_regression", 3)

	rows, err := db.Query(ctx, "INSERT INTO multi_insert_regression (id, name) VALUES (4, 'dave'), (5, 'erin') RETURNING id, name")
	if err != nil {
		t.Fatalf("multi-row INSERT RETURNING failed: %v", err)
	}
	defer rows.Close()

	type returnedRow struct {
		id   int64
		name string
	}
	var got []returnedRow
	for rows.Next() {
		var row returnedRow
		if err := rows.Scan(&row.id, &row.name); err != nil {
			t.Fatalf("scan returning row: %v", err)
		}
		got = append(got, row)
	}

	want := []returnedRow{{id: 4, name: "dave"}, {id: 5, name: "erin"}}
	if len(got) != len(want) {
		t.Fatalf("RETURNING row count = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RETURNING row %d = %#v, want %#v", i, got[i], want[i])
		}
	}
	expectRowCount(t, db, "SELECT * FROM multi_insert_regression", 5)
}

func TestMultiInsertRollsBackOnLaterValueCountError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE multi_insert_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

	_, err := db.Exec(ctx, "INSERT INTO multi_insert_atomicity VALUES (1, 'first'), (2)")
	if err == nil {
		t.Fatal("expected multi-row insert to fail on the second row")
	}
	expectRowCount(t, db, "SELECT * FROM multi_insert_atomicity", 0)
}

func TestMultiInsertRollsBackOnReturningError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE multi_insert_returning_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

	rows, err := db.Query(ctx, "INSERT INTO multi_insert_returning_atomicity VALUES (1, 'first'), (2, 'second') RETURNING missing_column")
	if err == nil {
		rows.Close()
		t.Fatal("expected multi-row INSERT RETURNING to fail for an unknown column")
	}
	expectRowCount(t, db, "SELECT * FROM multi_insert_returning_atomicity", 0)
}

func TestMultiInsertRollsBackOnAfterTriggerError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE multi_insert_trigger_main (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE multi_insert_trigger_audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
	execSQL(t, db, `CREATE TRIGGER multi_insert_bad_after
		AFTER INSERT ON multi_insert_trigger_main
		BEGIN
			INSERT INTO multi_insert_trigger_audit VALUES (NEW.id, NULL);
		END`)

	_, err := db.Exec(ctx, "INSERT INTO multi_insert_trigger_main VALUES (1, 'first'), (2, 'second')")
	if err == nil {
		t.Fatal("expected AFTER INSERT trigger failure")
	}
	expectRowCount(t, db, "SELECT * FROM multi_insert_trigger_main", 0)
	expectRowCount(t, db, "SELECT * FROM multi_insert_trigger_audit", 0)
}
