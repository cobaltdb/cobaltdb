package test

import (
	"fmt"
	"testing"
)

func TestInsertOrReplace(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	checkRows := func(desc string, sql string, expectedCount int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expectedCount {
			t.Errorf("[FAIL] %s: got %d rows, expected %d", desc, len(rows), expectedCount)
			return
		}
		pass++
	}

	// Setup
	afExec(t, db, ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	afExec(t, db, ctx, "INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')")
	afExec(t, db, ctx, "INSERT INTO users VALUES (2, 'Bob', 'bob@test.com')")
	afExec(t, db, ctx, "INSERT INTO users VALUES (3, 'Carol', 'carol@test.com')")

	// 1. INSERT OR REPLACE with duplicate PK - should replace the row
	afExec(t, db, ctx, "INSERT OR REPLACE INTO users VALUES (1, 'Alice Updated', 'alice_new@test.com')")
	check("REPLACE updates name", "SELECT name FROM users WHERE id = 1", "Alice Updated")
	check("REPLACE updates email", "SELECT email FROM users WHERE id = 1", "alice_new@test.com")
	checkRows("REPLACE same count", "SELECT * FROM users", 3)

	// 2. INSERT OR REPLACE with new key - should insert normally
	afExec(t, db, ctx, "INSERT OR REPLACE INTO users VALUES (4, 'Dave', 'dave@test.com')")
	checkRows("REPLACE new row", "SELECT * FROM users", 4)
	check("REPLACE new row value", "SELECT name FROM users WHERE id = 4", "Dave")

	// 3. INSERT OR IGNORE with duplicate PK - should skip silently
	afExec(t, db, ctx, "INSERT OR IGNORE INTO users VALUES (2, 'Bob Updated', 'bob_new@test.com')")
	check("IGNORE keeps old name", "SELECT name FROM users WHERE id = 2", "Bob")
	check("IGNORE keeps old email", "SELECT email FROM users WHERE id = 2", "bob@test.com")
	checkRows("IGNORE same count", "SELECT * FROM users", 4)

	// 4. INSERT OR IGNORE with new key - should insert normally
	afExec(t, db, ctx, "INSERT OR IGNORE INTO users VALUES (5, 'Eve', 'eve@test.com')")
	checkRows("IGNORE new row", "SELECT * FROM users", 5)
	check("IGNORE new row value", "SELECT name FROM users WHERE id = 5", "Eve")

	// 5. INSERT OR REPLACE with multiple rows
	afExec(t, db, ctx, "INSERT OR REPLACE INTO users VALUES (2, 'Bob2', 'bob2@test.com'), (6, 'Frank', 'frank@test.com')")
	check("REPLACE multi - updated", "SELECT name FROM users WHERE id = 2", "Bob2")
	check("REPLACE multi - new", "SELECT name FROM users WHERE id = 6", "Frank")
	checkRows("REPLACE multi count", "SELECT * FROM users", 6)

	// 6. INSERT OR IGNORE with multiple rows
	afExec(t, db, ctx, "INSERT OR IGNORE INTO users VALUES (3, 'Carol Updated', 'carol_new@test.com'), (7, 'Grace', 'grace@test.com')")
	check("IGNORE multi - kept", "SELECT name FROM users WHERE id = 3", "Carol")
	check("IGNORE multi - new", "SELECT name FROM users WHERE id = 7", "Grace")
	checkRows("IGNORE multi count", "SELECT * FROM users", 7)

	// 7. Regular INSERT with duplicate should still fail
	total++
	_, err := db.Exec(ctx, "INSERT INTO users VALUES (1, 'Fail', 'fail@test.com')")
	if err != nil {
		pass++
	} else {
		t.Errorf("[FAIL] Regular INSERT with duplicate PK should fail")
	}

	t.Logf("\n=== UPSERT TESTS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed!")
	}
}
