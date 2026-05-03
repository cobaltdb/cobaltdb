package test

import "testing"

func TestSharedHelpers(t *testing.T) {
	db, ctx := TestDB(t)

	Exec(t, db, ctx, "CREATE TABLE helper_smoke (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	Exec(t, db, ctx, "INSERT INTO helper_smoke (id, name) VALUES (?, ?)", 1, "alice")

	ExpectRows(t, db, ctx, "SELECT * FROM helper_smoke", 1)
	ExpectVal(t, db, ctx, "SELECT name FROM helper_smoke WHERE id = 1", "alice")
	ExpectError(t, db, ctx, "SELECT * FROM helper_missing", "unsupported statement type")
}
