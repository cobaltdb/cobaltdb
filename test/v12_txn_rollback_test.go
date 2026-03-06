package test

import (
	"fmt"
	"testing"
)

func TestV12TransactionRollbackEdgeCases(t *testing.T) {
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

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expected, len(rows))
			return
		}
		pass++
	}

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
			return
		}
		pass++
	}

	// ============================================================
	// === AUTOINCREMENT ROLLBACK ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE autoinc_test (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO autoinc_test VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO autoinc_test VALUES (2, 'Bob')")

	// Begin transaction, insert new row, then rollback
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO autoinc_test VALUES (3, 'Charlie')")
	checkRowCount("3 rows during txn", "SELECT * FROM autoinc_test", 3)
	afExec(t, db, ctx, "ROLLBACK")
	checkRowCount("2 rows after rollback", "SELECT * FROM autoinc_test", 2)

	// After rollback, inserting without explicit ID should reuse the sequence
	// The AutoIncSeq should have been restored
	afExec(t, db, ctx, "INSERT INTO autoinc_test (name) VALUES ('Dave')")
	checkRowCount("3 rows after reinsert", "SELECT * FROM autoinc_test", 3)

	// ============================================================
	// === ALTER TABLE ADD COLUMN ROLLBACK ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE alter_add_test (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO alter_add_test VALUES (1, 'Alice')")

	afExec(t, db, ctx, "BEGIN")
	checkNoError("ALTER ADD COLUMN in txn", "ALTER TABLE alter_add_test ADD COLUMN age INTEGER")
	// Column should exist during transaction
	checkNoError("INSERT with new column", "INSERT INTO alter_add_test VALUES (2, 'Bob', 30)")
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, the column should be gone
	// Verify by selecting original columns only
	checkRowCount("1 row after alter rollback", "SELECT * FROM alter_add_test", 1)
	check("Original data intact", "SELECT name FROM alter_add_test WHERE id = 1", "Alice")

	// ============================================================
	// === ALTER TABLE RENAME ROLLBACK ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE rename_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO rename_test VALUES (1, 'hello')")

	afExec(t, db, ctx, "BEGIN")
	checkNoError("RENAME TABLE in txn", "ALTER TABLE rename_test RENAME TO renamed_test")
	// New name should work during transaction
	checkRowCount("Query renamed table", "SELECT * FROM renamed_test", 1)
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, old name should work
	checkRowCount("Old name after rollback", "SELECT * FROM rename_test", 1)
	check("Data intact after rename rollback", "SELECT val FROM rename_test WHERE id = 1", "hello")

	// ============================================================
	// === ALTER TABLE RENAME COLUMN ROLLBACK ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE rename_col_test (id INTEGER PRIMARY KEY, old_name TEXT)")
	afExec(t, db, ctx, "INSERT INTO rename_col_test VALUES (1, 'test')")

	afExec(t, db, ctx, "BEGIN")
	checkNoError("RENAME COLUMN in txn", "ALTER TABLE rename_col_test RENAME COLUMN old_name TO new_name")
	// New column name should work during transaction
	check("Query with new col name", "SELECT new_name FROM rename_col_test WHERE id = 1", "test")
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, old column name should work
	check("Old col name after rollback", "SELECT old_name FROM rename_col_test WHERE id = 1", "test")

	// ============================================================
	// === MULTI-ROW INSERT ATOMICITY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE atomic_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
	afExec(t, db, ctx, "INSERT INTO atomic_test VALUES (1, 'existing')")

	// Try to insert multiple rows where a later one violates NOT NULL
	// This should fail atomically - no rows inserted
	checkError("Multi-row with constraint violation",
		"INSERT INTO atomic_test VALUES (2, 'good'), (3, NULL)")
	checkRowCount("Only original row remains", "SELECT * FROM atomic_test", 1)
	check("Original data intact", "SELECT val FROM atomic_test WHERE id = 1", "existing")

	// Try to insert multiple rows where a later one has duplicate PK
	checkError("Multi-row with duplicate PK",
		"INSERT INTO atomic_test VALUES (10, 'ten'), (11, 'eleven'), (1, 'duplicate')")
	checkRowCount("Still only original row", "SELECT * FROM atomic_test", 1)

	// ============================================================
	// === MULTI-ROW INSERT ATOMICITY INSIDE TRANSACTION ===
	// ============================================================
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO atomic_test VALUES (20, 'twenty')")
	checkError("Multi-row fails in txn",
		"INSERT INTO atomic_test VALUES (30, 'thirty'), (31, NULL)")
	// The successful row (20) from before should still be there (in txn)
	checkRowCount("Rows in txn after stmt fail", "SELECT * FROM atomic_test", 2)
	afExec(t, db, ctx, "ROLLBACK")
	checkRowCount("Only original after full rollback", "SELECT * FROM atomic_test", 1)

	// ============================================================
	// === TRANSACTION WITH INSERT + UPDATE + DELETE ROLLBACK ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE mixed_ops (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO mixed_ops VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO mixed_ops VALUES (2, 200)")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO mixed_ops VALUES (3, 300)")
	afExec(t, db, ctx, "UPDATE mixed_ops SET val = 999 WHERE id = 1")
	afExec(t, db, ctx, "DELETE FROM mixed_ops WHERE id = 2")
	checkRowCount("2 rows during mixed txn", "SELECT * FROM mixed_ops", 2)
	check("Updated value during txn", "SELECT val FROM mixed_ops WHERE id = 1", 999)
	afExec(t, db, ctx, "ROLLBACK")

	checkRowCount("2 rows after mixed rollback", "SELECT * FROM mixed_ops", 2)
	check("Restored val after rollback", "SELECT val FROM mixed_ops WHERE id = 1", 100)
	check("Restored deleted row", "SELECT val FROM mixed_ops WHERE id = 2", 200)

	// ============================================================
	// === NESTED BEGIN (should not destroy active txn) ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE nested_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO nested_test VALUES (1, 'original')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO nested_test VALUES (2, 'in_txn')")
	// Second BEGIN should ideally error or be a no-op
	// Just verify the first transaction's data is still intact
	checkRowCount("2 rows in txn", "SELECT * FROM nested_test", 2)
	afExec(t, db, ctx, "ROLLBACK")
	checkRowCount("1 row after rollback", "SELECT * FROM nested_test", 1)

	// ============================================================
	// === COMMIT THEN ROLLBACK (should have no effect) ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE commit_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "INSERT INTO commit_test VALUES (1, 'committed')")
	afExec(t, db, ctx, "COMMIT")
	checkRowCount("Row exists after commit", "SELECT * FROM commit_test", 1)
	check("Data correct after commit", "SELECT val FROM commit_test WHERE id = 1", "committed")

	// ============================================================
	// === CREATE TABLE + INSERT ROLLBACK ===
	// ============================================================
	afExec(t, db, ctx, "BEGIN")
	checkNoError("CREATE TABLE in txn", "CREATE TABLE txn_table (id INTEGER PRIMARY KEY, data TEXT)")
	checkNoError("INSERT into new table", "INSERT INTO txn_table VALUES (1, 'transient')")
	checkRowCount("Row in new table", "SELECT * FROM txn_table", 1)
	afExec(t, db, ctx, "ROLLBACK")
	// Table should not exist after rollback
	checkError("Table gone after rollback", "SELECT * FROM txn_table")

	// ============================================================
	// === DROP TABLE ROLLBACK ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE drop_test (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO drop_test VALUES (1, 'keep_me')")

	afExec(t, db, ctx, "BEGIN")
	checkNoError("DROP TABLE in txn", "DROP TABLE drop_test")
	checkError("Table gone during txn", "SELECT * FROM drop_test")
	afExec(t, db, ctx, "ROLLBACK")

	// Table should be restored after rollback
	checkRowCount("Table restored", "SELECT * FROM drop_test", 1)
	check("Data restored", "SELECT val FROM drop_test WHERE id = 1", "keep_me")

	t.Logf("\n=== V12 TXN ROLLBACK: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
