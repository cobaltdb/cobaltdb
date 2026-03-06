package test

import (
	"fmt"
	"testing"
)

func TestV6TransactionDDLRollback(t *testing.T) {
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

	checkFail := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: should have failed", desc)
		}
	}

	// === 1. CREATE TABLE + ROLLBACK: table should not exist ===
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO rollback_test VALUES (1, 'hello')")
	// Verify it works within the transaction
	check("Table exists in txn", "SELECT val FROM rollback_test WHERE id = 1", "hello")
	afExec(t, db, ctx, "ROLLBACK")
	// After rollback, table should not exist
	checkFail("Table gone after rollback", "SELECT * FROM rollback_test")

	// === 2. DROP TABLE + ROLLBACK: table should be restored ===
	afExec(t, db, ctx, "CREATE TABLE persist_test (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO persist_test VALUES (1, 'keep me')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "DROP TABLE persist_test")
	// Verify it's gone within the transaction
	checkFail("Table dropped in txn", "SELECT * FROM persist_test")
	afExec(t, db, ctx, "ROLLBACK")
	// After rollback, table should be back
	check("Table restored after rollback", "SELECT name FROM persist_test WHERE id = 1", "keep me")

	// === 3. INSERT + ROLLBACK: data should not persist ===
	afExec(t, db, ctx, "CREATE TABLE insert_rollback (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO insert_rollback VALUES (1, 'committed')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO insert_rollback VALUES (2, 'uncommitted')")
	afExec(t, db, ctx, "ROLLBACK")

	check("Committed row persists", "SELECT val FROM insert_rollback WHERE id = 1", "committed")
	rows := afQuery(t, db, ctx, "SELECT * FROM insert_rollback WHERE id = 2")
	total++
	if len(rows) == 0 {
		pass++
	} else {
		t.Errorf("[FAIL] Rolled back INSERT should not persist")
	}

	// === 4. UPDATE + ROLLBACK: original value should be restored ===
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "UPDATE insert_rollback SET val = 'modified' WHERE id = 1")
	check("Updated value in txn", "SELECT val FROM insert_rollback WHERE id = 1", "modified")
	afExec(t, db, ctx, "ROLLBACK")
	check("Original value restored", "SELECT val FROM insert_rollback WHERE id = 1", "committed")

	// === 5. DELETE + ROLLBACK: deleted row should be restored ===
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "DELETE FROM insert_rollback WHERE id = 1")
	rows = afQuery(t, db, ctx, "SELECT * FROM insert_rollback WHERE id = 1")
	total++
	if len(rows) == 0 {
		pass++
	} else {
		t.Errorf("[FAIL] Deleted row should not exist in txn")
	}
	afExec(t, db, ctx, "ROLLBACK")
	check("Deleted row restored", "SELECT val FROM insert_rollback WHERE id = 1", "committed")

	// === 6. COMMIT should persist changes ===
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO insert_rollback VALUES (3, 'persisted')")
	afExec(t, db, ctx, "COMMIT")
	check("Committed INSERT persists", "SELECT val FROM insert_rollback WHERE id = 3", "persisted")

	// === 7. VARCHAR(255) and DECIMAL(10,2) type parameters ===
	afExec(t, db, ctx, "CREATE TABLE types_test (id INTEGER PRIMARY KEY, name CHAR(50), price DECIMAL(10,2), code NVARCHAR(100), age SMALLINT, big BIGINT)")
	afExec(t, db, ctx, "INSERT INTO types_test VALUES (1, 'test', 19.99, 'ABC', 25, 1000000)")
	check("CHAR(50) works", "SELECT name FROM types_test WHERE id = 1", "test")
	check("DECIMAL(10,2) works", "SELECT price FROM types_test WHERE id = 1", 19.99)

	t.Logf("\n=== V6 TRANSACTION DDL: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
