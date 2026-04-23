package test

import (
	"fmt"
	"testing"
)

// TestV72TxnIsolation tests transaction isolation, nested savepoints,
// complex multi-statement transactions, and DML within transactions.
func TestV72TxnIsolation(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			if expected == nil {
				pass++
				return
			}
			t.Errorf("[FAIL] %s: no rows returned, expected %v", desc, expected)
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

	// ============================================================
	// === BASIC TRANSACTION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v72_basic (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v72_basic VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO v72_basic VALUES (2, 200)")
	afExec(t, db, ctx, "INSERT INTO v72_basic VALUES (3, 300)")

	// BT1: Commit preserves all changes
	checkNoError("BT1 BEGIN", "BEGIN")
	checkNoError("BT1 INSERT", "INSERT INTO v72_basic VALUES (4, 400)")
	checkNoError("BT1 UPDATE", "UPDATE v72_basic SET val = 150 WHERE id = 1")
	checkNoError("BT1 DELETE", "DELETE FROM v72_basic WHERE id = 3")
	checkNoError("BT1 COMMIT", "COMMIT")
	check("BT1 count", "SELECT COUNT(*) FROM v72_basic", 3) // 1,2,4
	check("BT1 val 1", "SELECT val FROM v72_basic WHERE id = 1", 150)
	check("BT1 val 4", "SELECT val FROM v72_basic WHERE id = 4", 400)

	// BT2: Rollback undoes all changes
	checkNoError("BT2 BEGIN", "BEGIN")
	checkNoError("BT2 INSERT", "INSERT INTO v72_basic VALUES (5, 500)")
	checkNoError("BT2 UPDATE", "UPDATE v72_basic SET val = 999 WHERE id = 1")
	checkNoError("BT2 DELETE", "DELETE FROM v72_basic WHERE id = 2")
	checkNoError("BT2 ROLLBACK", "ROLLBACK")
	check("BT2 count", "SELECT COUNT(*) FROM v72_basic", 3)           // unchanged
	check("BT2 val 1", "SELECT val FROM v72_basic WHERE id = 1", 150) // unchanged
	check("BT2 val 2", "SELECT val FROM v72_basic WHERE id = 2", 200) // not deleted

	// ============================================================
	// === SAVEPOINT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v72_sp (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v72_sp VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v72_sp VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v72_sp VALUES (3, 30)")

	// SP1: SAVEPOINT + ROLLBACK TO preserves earlier changes
	checkNoError("SP1 BEGIN", "BEGIN")
	checkNoError("SP1 UPDATE 1", "UPDATE v72_sp SET val = 100 WHERE id = 1")
	checkNoError("SP1 SAVEPOINT", "SAVEPOINT sp1")
	checkNoError("SP1 UPDATE 2", "UPDATE v72_sp SET val = 200 WHERE id = 2")
	checkNoError("SP1 INSERT", "INSERT INTO v72_sp VALUES (4, 40)")
	checkNoError("SP1 ROLLBACK TO", "ROLLBACK TO sp1")
	// After rollback to sp1: id=1 should be 100, id=2 should be 20 (reverted), id=4 gone
	checkNoError("SP1 COMMIT", "COMMIT")
	check("SP1 val 1", "SELECT val FROM v72_sp WHERE id = 1", 100) // kept
	check("SP1 val 2", "SELECT val FROM v72_sp WHERE id = 2", 20)  // reverted
	check("SP1 count", "SELECT COUNT(*) FROM v72_sp", 3)           // id=4 not inserted

	// SP2: Multiple savepoints
	checkNoError("SP2 BEGIN", "BEGIN")
	checkNoError("SP2 UPDATE 1", "UPDATE v72_sp SET val = 11 WHERE id = 1")
	checkNoError("SP2 SAVEPOINT a", "SAVEPOINT spa")
	checkNoError("SP2 UPDATE 2", "UPDATE v72_sp SET val = 22 WHERE id = 2")
	checkNoError("SP2 SAVEPOINT b", "SAVEPOINT spb")
	checkNoError("SP2 UPDATE 3", "UPDATE v72_sp SET val = 33 WHERE id = 3")
	checkNoError("SP2 ROLLBACK TO b", "ROLLBACK TO spb")
	// id=3 should be reverted to 30, id=2 is 22, id=1 is 11
	checkNoError("SP2 COMMIT", "COMMIT")
	check("SP2 val 1", "SELECT val FROM v72_sp WHERE id = 1", 11)
	check("SP2 val 2", "SELECT val FROM v72_sp WHERE id = 2", 22)
	check("SP2 val 3", "SELECT val FROM v72_sp WHERE id = 3", 30) // reverted

	// ============================================================
	// === MULTI-TABLE TRANSACTION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v72_accounts (id INTEGER PRIMARY KEY, balance INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v72_accounts VALUES (1, 1000)")
	afExec(t, db, ctx, "INSERT INTO v72_accounts VALUES (2, 500)")

	afExec(t, db, ctx, `CREATE TABLE v72_transfers (id INTEGER PRIMARY KEY, from_id INTEGER, to_id INTEGER, amount INTEGER)`)

	// MT1: Transfer money (atomic)
	checkNoError("MT1 BEGIN", "BEGIN")
	checkNoError("MT1 debit", "UPDATE v72_accounts SET balance = balance - 200 WHERE id = 1")
	checkNoError("MT1 credit", "UPDATE v72_accounts SET balance = balance + 200 WHERE id = 2")
	checkNoError("MT1 log", "INSERT INTO v72_transfers VALUES (1, 1, 2, 200)")
	checkNoError("MT1 COMMIT", "COMMIT")

	check("MT1 balance 1", "SELECT balance FROM v72_accounts WHERE id = 1", 800)
	check("MT1 balance 2", "SELECT balance FROM v72_accounts WHERE id = 2", 700)
	check("MT1 transfer log", "SELECT amount FROM v72_transfers WHERE id = 1", 200)

	// MT2: Failed transfer (rollback)
	checkNoError("MT2 BEGIN", "BEGIN")
	checkNoError("MT2 debit", "UPDATE v72_accounts SET balance = balance - 5000 WHERE id = 1")
	checkNoError("MT2 credit", "UPDATE v72_accounts SET balance = balance + 5000 WHERE id = 2")
	// Oops, balance would be negative — rollback
	checkNoError("MT2 ROLLBACK", "ROLLBACK")

	check("MT2 balance 1", "SELECT balance FROM v72_accounts WHERE id = 1", 800) // unchanged
	check("MT2 balance 2", "SELECT balance FROM v72_accounts WHERE id = 2", 700) // unchanged

	// ============================================================
	// === DML IN TRANSACTIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v72_dml (id INTEGER PRIMARY KEY, status TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v72_dml VALUES (1, 'new', 10)")
	afExec(t, db, ctx, "INSERT INTO v72_dml VALUES (2, 'new', 20)")
	afExec(t, db, ctx, "INSERT INTO v72_dml VALUES (3, 'new', 30)")

	// DM1: Complex DML in transaction
	checkNoError("DM1 BEGIN", "BEGIN")
	// Update status
	checkNoError("DM1 UPDATE status",
		"UPDATE v72_dml SET status = 'processing' WHERE val > 15")
	// Delete low-value entries
	checkNoError("DM1 DELETE low",
		"DELETE FROM v72_dml WHERE val < 15")
	// Insert new entry
	checkNoError("DM1 INSERT",
		"INSERT INTO v72_dml VALUES (4, 'new', 40)")
	checkNoError("DM1 COMMIT", "COMMIT")

	check("DM1 count", "SELECT COUNT(*) FROM v72_dml", 3) // 2,3,4
	check("DM1 status 2", "SELECT status FROM v72_dml WHERE id = 2", "processing")
	check("DM1 status 3", "SELECT status FROM v72_dml WHERE id = 3", "processing")
	check("DM1 status 4", "SELECT status FROM v72_dml WHERE id = 4", "new")

	// ============================================================
	// === TRANSACTION + SUBQUERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v72_tsub (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v72_tsub VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v72_tsub VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v72_tsub VALUES (3, 30)")

	// TS1: UPDATE with subquery in transaction
	checkNoError("TS1 BEGIN", "BEGIN")
	checkNoError("TS1 UPDATE",
		"UPDATE v72_tsub SET val = (SELECT MAX(val) FROM v72_tsub) WHERE id = 1")
	check("TS1 mid-txn verify", "SELECT val FROM v72_tsub WHERE id = 1", 30)
	checkNoError("TS1 COMMIT", "COMMIT")
	check("TS1 post-commit", "SELECT val FROM v72_tsub WHERE id = 1", 30)

	// TS2: DELETE with subquery in transaction, then rollback
	checkNoError("TS2 BEGIN", "BEGIN")
	checkNoError("TS2 DELETE",
		"DELETE FROM v72_tsub WHERE val = (SELECT MIN(val) FROM v72_tsub)")
	check("TS2 mid-txn count", "SELECT COUNT(*) FROM v72_tsub", 2)
	checkNoError("TS2 ROLLBACK", "ROLLBACK")
	check("TS2 post-rollback count", "SELECT COUNT(*) FROM v72_tsub", 3) // restored

	// ============================================================
	// === TRANSACTION + AGGREGATES ===
	// ============================================================

	// TA1: Aggregate visible during transaction
	// After TS1: id=1 val=30, id=2 val=20, id=3 val=30 → SUM=80
	checkNoError("TA1 BEGIN", "BEGIN")
	checkNoError("TA1 INSERT", "INSERT INTO v72_tsub VALUES (4, 40)")
	check("TA1 SUM in txn", "SELECT SUM(val) FROM v72_tsub", 120) // 80+40=120
	checkNoError("TA1 ROLLBACK", "ROLLBACK")
	check("TA1 SUM after rollback", "SELECT SUM(val) FROM v72_tsub", 80) // 30+20+30=80

	// ============================================================
	// === CONSECUTIVE TRANSACTIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v72_seq (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v72_seq VALUES (1, 0)")

	// CT1: Multiple sequential transactions
	for i := 1; i <= 5; i++ {
		checkNoError(fmt.Sprintf("CT1 BEGIN %d", i), "BEGIN")
		checkNoError(fmt.Sprintf("CT1 UPDATE %d", i),
			fmt.Sprintf("UPDATE v72_seq SET val = %d WHERE id = 1", i*10))
		checkNoError(fmt.Sprintf("CT1 COMMIT %d", i), "COMMIT")
	}
	check("CT1 final value", "SELECT val FROM v72_seq WHERE id = 1", 50)

	// CT2: Alternating commit and rollback
	checkNoError("CT2 BEGIN 1", "BEGIN")
	checkNoError("CT2 UPDATE 1", "UPDATE v72_seq SET val = 100 WHERE id = 1")
	checkNoError("CT2 COMMIT 1", "COMMIT")

	checkNoError("CT2 BEGIN 2", "BEGIN")
	checkNoError("CT2 UPDATE 2", "UPDATE v72_seq SET val = 200 WHERE id = 1")
	checkNoError("CT2 ROLLBACK 2", "ROLLBACK")

	checkNoError("CT2 BEGIN 3", "BEGIN")
	checkNoError("CT2 UPDATE 3", "UPDATE v72_seq SET val = 300 WHERE id = 1")
	checkNoError("CT2 COMMIT 3", "COMMIT")

	check("CT2 final", "SELECT val FROM v72_seq WHERE id = 1", 300)

	// ============================================================
	// === CREATE TABLE IN TRANSACTION ===
	// ============================================================

	// CR1: CREATE TABLE + INSERT in transaction, then commit
	checkNoError("CR1 BEGIN", "BEGIN")
	checkNoError("CR1 CREATE", "CREATE TABLE v72_new (id INTEGER PRIMARY KEY, val TEXT)")
	checkNoError("CR1 INSERT", "INSERT INTO v72_new VALUES (1, 'hello')")
	checkNoError("CR1 COMMIT", "COMMIT")
	check("CR1 verify", "SELECT val FROM v72_new WHERE id = 1", "hello")

	// ============================================================
	// === BULK OPERATIONS IN TRANSACTION ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v72_bulk (id INTEGER PRIMARY KEY, val INTEGER)`)

	// BK1: Many inserts in single transaction
	checkNoError("BK1 BEGIN", "BEGIN")
	for i := 1; i <= 100; i++ {
		checkNoError(fmt.Sprintf("BK1 INSERT %d", i),
			fmt.Sprintf("INSERT INTO v72_bulk VALUES (%d, %d)", i, i*10))
	}
	checkNoError("BK1 COMMIT", "COMMIT")
	check("BK1 count", "SELECT COUNT(*) FROM v72_bulk", 100)
	check("BK1 sum", "SELECT SUM(val) FROM v72_bulk", 50500)

	t.Logf("\n=== V72 TRANSACTION ISOLATION: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
