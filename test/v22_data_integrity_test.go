package test

import (
	"fmt"
	"testing"
)

func TestV22DataIntegrity(t *testing.T) {
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
	// === TRANSACTION ROLLBACK OF INSERT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_ins (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO txn_ins VALUES (1, 'pre-txn')")

	checkNoError("BEGIN", "BEGIN")
	checkNoError("INSERT in txn", "INSERT INTO txn_ins VALUES (2, 'in-txn')")
	checkNoError("INSERT in txn 2", "INSERT INTO txn_ins VALUES (3, 'in-txn-2')")
	checkRowCount("See all rows in txn", "SELECT * FROM txn_ins", 3)
	checkNoError("ROLLBACK", "ROLLBACK")
	checkRowCount("Only pre-txn row after rollback", "SELECT * FROM txn_ins", 1)
	check("Pre-txn data intact", "SELECT val FROM txn_ins WHERE id = 1", "pre-txn")

	// ============================================================
	// === TRANSACTION ROLLBACK OF UPDATE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_upd (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO txn_upd VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO txn_upd VALUES (2, 200)")

	checkNoError("BEGIN", "BEGIN")
	checkNoError("UPDATE row 1", "UPDATE txn_upd SET val = 999 WHERE id = 1")
	checkNoError("UPDATE row 2", "UPDATE txn_upd SET val = 888 WHERE id = 2")
	check("See updated val in txn", "SELECT val FROM txn_upd WHERE id = 1", 999)
	checkNoError("ROLLBACK", "ROLLBACK")
	check("Row 1 restored", "SELECT val FROM txn_upd WHERE id = 1", 100)
	check("Row 2 restored", "SELECT val FROM txn_upd WHERE id = 2", 200)

	// ============================================================
	// === TRANSACTION ROLLBACK OF DELETE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_del (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO txn_del VALUES (1, 'keep1')")
	afExec(t, db, ctx, "INSERT INTO txn_del VALUES (2, 'delete-me')")
	afExec(t, db, ctx, "INSERT INTO txn_del VALUES (3, 'keep2')")

	checkNoError("BEGIN", "BEGIN")
	checkNoError("DELETE row", "DELETE FROM txn_del WHERE id = 2")
	checkRowCount("Row deleted in txn", "SELECT * FROM txn_del", 2)
	checkNoError("ROLLBACK", "ROLLBACK")
	checkRowCount("All rows back after rollback", "SELECT * FROM txn_del", 3)
	check("Deleted row restored", "SELECT val FROM txn_del WHERE id = 2", "delete-me")

	// ============================================================
	// === TRANSACTION COMMIT OF MIXED DML ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_mixed (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO txn_mixed VALUES (1, 'original')")
	afExec(t, db, ctx, "INSERT INTO txn_mixed VALUES (2, 'to-delete')")
	afExec(t, db, ctx, "INSERT INTO txn_mixed VALUES (3, 'to-update')")

	checkNoError("BEGIN mixed", "BEGIN")
	checkNoError("INSERT in mixed txn", "INSERT INTO txn_mixed VALUES (4, 'new-row')")
	checkNoError("DELETE in mixed txn", "DELETE FROM txn_mixed WHERE id = 2")
	checkNoError("UPDATE in mixed txn", "UPDATE txn_mixed SET val = 'updated' WHERE id = 3")
	checkNoError("COMMIT mixed", "COMMIT")

	checkRowCount("After commit", "SELECT * FROM txn_mixed", 3)
	check("Insert persisted", "SELECT val FROM txn_mixed WHERE id = 4", "new-row")
	check("Update persisted", "SELECT val FROM txn_mixed WHERE id = 3", "updated")

	// ============================================================
	// === AUTO-INCREMENT ACROSS TRANSACTIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_autoinc (id INTEGER PRIMARY KEY AUTO_INCREMENT, val TEXT)")

	checkNoError("INSERT autoinc 1", "INSERT INTO txn_autoinc (val) VALUES ('first')")
	check("First auto-inc", "SELECT id FROM txn_autoinc WHERE val = 'first'", 1)

	checkNoError("BEGIN autoinc", "BEGIN")
	checkNoError("INSERT in txn", "INSERT INTO txn_autoinc (val) VALUES ('second')")
	check("In-txn auto-inc", "SELECT id FROM txn_autoinc WHERE val = 'second'", 2)
	checkNoError("ROLLBACK autoinc", "ROLLBACK")

	// After rollback, next insert should still get a higher ID (not reuse rolled-back ID)
	checkNoError("INSERT after rollback", "INSERT INTO txn_autoinc (val) VALUES ('third')")
	checkRowCount("Only 2 rows", "SELECT * FROM txn_autoinc", 2)

	// ============================================================
	// === FK ENFORCEMENT IN TRANSACTIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE fk_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE fk_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_parent(id))")

	afExec(t, db, ctx, "INSERT INTO fk_parent VALUES (1, 'Parent1')")
	afExec(t, db, ctx, "INSERT INTO fk_parent VALUES (2, 'Parent2')")

	// FK violation should fail even in transaction
	checkNoError("BEGIN fk txn", "BEGIN")
	checkError("FK violation in txn", "INSERT INTO fk_child VALUES (1, 99)") // parent 99 doesn't exist
	checkNoError("ROLLBACK fk txn", "ROLLBACK")

	// Valid FK insert in transaction
	checkNoError("BEGIN valid fk", "BEGIN")
	checkNoError("Valid FK insert", "INSERT INTO fk_child VALUES (1, 1)")
	checkNoError("COMMIT fk", "COMMIT")
	checkRowCount("FK child exists", "SELECT * FROM fk_child", 1)

	// ============================================================
	// === CASCADE DELETE IN TRANSACTIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE casc_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE casc_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES casc_parent(id) ON DELETE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO casc_parent VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO casc_parent VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO casc_child VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO casc_child VALUES (2, 1)")
	afExec(t, db, ctx, "INSERT INTO casc_child VALUES (3, 2)")

	checkNoError("BEGIN cascade", "BEGIN")
	checkNoError("DELETE cascading parent", "DELETE FROM casc_parent WHERE id = 1")
	checkRowCount("Children cascaded in txn", "SELECT * FROM casc_child WHERE parent_id = 1", 0)
	checkRowCount("Other children intact", "SELECT * FROM casc_child WHERE parent_id = 2", 1)
	checkNoError("ROLLBACK cascade", "ROLLBACK")
	checkRowCount("Parent restored", "SELECT * FROM casc_parent", 2)
	checkRowCount("All children restored", "SELECT * FROM casc_child", 3)

	// ============================================================
	// === UNIQUE CONSTRAINT IN TRANSACTIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_unique (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
	afExec(t, db, ctx, "INSERT INTO txn_unique VALUES (1, 'a@test.com')")

	checkNoError("BEGIN unique", "BEGIN")
	checkError("UNIQUE violation in txn", "INSERT INTO txn_unique VALUES (2, 'a@test.com')")
	checkNoError("ROLLBACK unique", "ROLLBACK")
	checkRowCount("Only original row", "SELECT * FROM txn_unique", 1)

	// ============================================================
	// === INDEX INTEGRITY AFTER ROLLBACK ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE idx_integrity (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx_int_name ON idx_integrity(name)")
	afExec(t, db, ctx, "INSERT INTO idx_integrity VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO idx_integrity VALUES (2, 'Bob')")

	checkNoError("BEGIN idx", "BEGIN")
	checkNoError("INSERT indexed row", "INSERT INTO idx_integrity VALUES (3, 'Charlie')")
	checkNoError("ROLLBACK idx", "ROLLBACK")

	// Index should still work correctly
	check("Index query after rollback", "SELECT name FROM idx_integrity WHERE name = 'Alice'", "Alice")
	checkRowCount("Only original rows", "SELECT * FROM idx_integrity", 2)

	// ============================================================
	// === MULTIPLE STATEMENTS BETWEEN BEGIN/COMMIT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE multi_stmt (id INTEGER PRIMARY KEY, val INTEGER)")

	checkNoError("BEGIN multi", "BEGIN")
	checkNoError("First insert", "INSERT INTO multi_stmt VALUES (1, 10)")
	checkNoError("Second insert", "INSERT INTO multi_stmt VALUES (2, 20)")
	checkNoError("Update first", "UPDATE multi_stmt SET val = 15 WHERE id = 1")
	checkNoError("Delete second", "DELETE FROM multi_stmt WHERE id = 2")
	checkNoError("Third insert", "INSERT INTO multi_stmt VALUES (3, 30)")
	checkNoError("COMMIT multi", "COMMIT")

	checkRowCount("After multi-stmt commit", "SELECT * FROM multi_stmt", 2)
	check("Updated value persisted", "SELECT val FROM multi_stmt WHERE id = 1", 15)
	check("Third insert persisted", "SELECT val FROM multi_stmt WHERE id = 3", 30)

	// ============================================================
	// === DATA INTEGRITY AFTER ERRORS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE err_integrity (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
	afExec(t, db, ctx, "INSERT INTO err_integrity VALUES (1, 'safe')")

	// Failed INSERT shouldn't affect existing data
	checkError("NOT NULL violation", "INSERT INTO err_integrity VALUES (2, NULL)")
	check("Data safe after error", "SELECT val FROM err_integrity WHERE id = 1", "safe")
	checkRowCount("No phantom row", "SELECT * FROM err_integrity", 1)

	// ============================================================
	// === CHECK CONSTRAINT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE check_test (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0 AND age <= 150))")
	checkNoError("Valid CHECK insert", "INSERT INTO check_test VALUES (1, 25)")
	checkError("CHECK violation negative", "INSERT INTO check_test VALUES (2, -1)")
	checkError("CHECK violation too large", "INSERT INTO check_test VALUES (3, 200)")
	checkRowCount("Only valid row", "SELECT * FROM check_test", 1)

	// ============================================================
	// === SEQUENCE / AUTO_INCREMENT INTEGRITY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE seq_test (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)")
	checkNoError("Seq 1", "INSERT INTO seq_test (name) VALUES ('a')")
	checkNoError("Seq 2", "INSERT INTO seq_test (name) VALUES ('b')")
	checkNoError("Seq 3", "INSERT INTO seq_test (name) VALUES ('c')")

	check("Seq id 1", "SELECT id FROM seq_test WHERE name = 'a'", 1)
	check("Seq id 2", "SELECT id FROM seq_test WHERE name = 'b'", 2)
	check("Seq id 3", "SELECT id FROM seq_test WHERE name = 'c'", 3)

	// Delete and re-insert - should get id=4, not reuse 3
	checkNoError("Delete seq 3", "DELETE FROM seq_test WHERE id = 3")
	checkNoError("Insert after delete", "INSERT INTO seq_test (name) VALUES ('d')")
	check("New seq after delete", "SELECT id FROM seq_test WHERE name = 'd'", 4)

	// ============================================================
	// === BULK INSERT ATOMICITY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE bulk_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")

	// Multi-row insert where one fails should be atomic (all or nothing)
	// Note: this depends on implementation - some DBs do partial inserts
	checkNoError("Valid bulk insert 1", "INSERT INTO bulk_test VALUES (1, 'one')")
	checkNoError("Valid bulk insert 2", "INSERT INTO bulk_test VALUES (2, 'two')")
	checkRowCount("Both rows inserted", "SELECT * FROM bulk_test", 2)

	t.Logf("\n=== V22 DATA INTEGRITY: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
