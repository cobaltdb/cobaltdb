package test

import (
	"context"
	"testing"
)

func TestUpdateRollsBackOnReturningError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE update_returning_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "INSERT INTO update_returning_atomicity VALUES (1, 'old'), (2, 'old')")

	rows, err := db.Query(ctx, "UPDATE update_returning_atomicity SET name = 'new' RETURNING missing_column")
	if err == nil {
		rows.Close()
		t.Fatal("expected UPDATE RETURNING to fail for an unknown column")
	}
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_returning_atomicity WHERE name = 'new'", int64(0))
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_returning_atomicity WHERE name = 'old'", int64(2))
}

func TestDeleteRollsBackOnReturningError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE delete_returning_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "INSERT INTO delete_returning_atomicity VALUES (1, 'one'), (2, 'two')")

	rows, err := db.Query(ctx, "DELETE FROM delete_returning_atomicity RETURNING missing_column")
	if err == nil {
		rows.Close()
		t.Fatal("expected DELETE RETURNING to fail for an unknown column")
	}
	expectRowCount(t, db, "SELECT * FROM delete_returning_atomicity", 2)
}

func TestUpdateFromRollsBackOnReturningError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE update_from_returning_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE update_from_returning_src (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "INSERT INTO update_from_returning_atomicity VALUES (1, 'old'), (2, 'old')")
	execSQL(t, db, "INSERT INTO update_from_returning_src VALUES (1), (2)")

	rows, err := db.Query(ctx, `UPDATE update_from_returning_atomicity
		SET name = 'new'
		FROM update_from_returning_src
		WHERE update_from_returning_atomicity.id = update_from_returning_src.id
		RETURNING missing_column`)
	if err == nil {
		rows.Close()
		t.Fatal("expected UPDATE FROM RETURNING to fail for an unknown column")
	}
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_from_returning_atomicity WHERE name = 'new'", int64(0))
	expectSingleValue(t, db, "SELECT COUNT(*) FROM update_from_returning_atomicity WHERE name = 'old'", int64(2))
}

func TestDeleteUsingRollsBackOnReturningError(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	execSQL(t, db, "CREATE TABLE delete_using_returning_atomicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execSQL(t, db, "CREATE TABLE delete_using_returning_src (id INTEGER PRIMARY KEY)")
	execSQL(t, db, "INSERT INTO delete_using_returning_atomicity VALUES (1, 'one'), (2, 'two')")
	execSQL(t, db, "INSERT INTO delete_using_returning_src VALUES (1), (2)")

	rows, err := db.Query(ctx, `DELETE FROM delete_using_returning_atomicity
		USING delete_using_returning_src
		WHERE delete_using_returning_atomicity.id = delete_using_returning_src.id
		RETURNING missing_column`)
	if err == nil {
		rows.Close()
		t.Fatal("expected DELETE USING RETURNING to fail for an unknown column")
	}
	expectRowCount(t, db, "SELECT * FROM delete_using_returning_atomicity", 2)
}
