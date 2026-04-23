package test

import (
	"fmt"
	"testing"
)

// TestV46Savepoint tests SAVEPOINT, RELEASE SAVEPOINT, and ROLLBACK TO SAVEPOINT
func TestV46Savepoint(t *testing.T) {
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
			t.Errorf("[FAIL] %s: expected error but got nil", desc)
			return
		}
		pass++
	}

	// ============================================================
	// Setup
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE v46_accounts (id INTEGER PRIMARY KEY, name TEXT, balance INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v46_accounts VALUES (1, 'Alice', 1000)")
	afExec(t, db, ctx, "INSERT INTO v46_accounts VALUES (2, 'Bob', 2000)")
	afExec(t, db, ctx, "INSERT INTO v46_accounts VALUES (3, 'Charlie', 3000)")

	// ============================================================
	// === BASIC SAVEPOINT ===
	// ============================================================

	// SP1: Simple savepoint + rollback to savepoint
	checkNoError("SP1 BEGIN", "BEGIN")
	checkNoError("SP1 SAVEPOINT", "SAVEPOINT sp1")
	checkNoError("SP1 UPDATE", "UPDATE v46_accounts SET balance = 500 WHERE id = 1")
	check("SP1 balance changed", "SELECT balance FROM v46_accounts WHERE id = 1", 500)
	checkNoError("SP1 ROLLBACK TO", "ROLLBACK TO SAVEPOINT sp1")
	check("SP1 balance restored", "SELECT balance FROM v46_accounts WHERE id = 1", 1000)
	checkNoError("SP1 COMMIT", "COMMIT")
	check("SP1 balance still original", "SELECT balance FROM v46_accounts WHERE id = 1", 1000)

	// SP2: Savepoint keeps changes before savepoint
	checkNoError("SP2 BEGIN", "BEGIN")
	checkNoError("SP2 first update", "UPDATE v46_accounts SET balance = 1500 WHERE id = 1")
	checkNoError("SP2 SAVEPOINT", "SAVEPOINT sp2")
	checkNoError("SP2 second update", "UPDATE v46_accounts SET balance = 999 WHERE id = 1")
	check("SP2 second update visible", "SELECT balance FROM v46_accounts WHERE id = 1", 999)
	checkNoError("SP2 ROLLBACK TO", "ROLLBACK TO sp2") // Test without SAVEPOINT keyword
	check("SP2 first update kept", "SELECT balance FROM v46_accounts WHERE id = 1", 1500)
	checkNoError("SP2 COMMIT", "COMMIT")
	check("SP2 first update committed", "SELECT balance FROM v46_accounts WHERE id = 1", 1500)

	// Reset
	afExec(t, db, ctx, "UPDATE v46_accounts SET balance = 1000 WHERE id = 1")

	// SP3: Multiple savepoints
	checkNoError("SP3 BEGIN", "BEGIN")
	checkNoError("SP3 SAVEPOINT a", "SAVEPOINT a")
	checkNoError("SP3 update Alice", "UPDATE v46_accounts SET balance = 100 WHERE id = 1")
	checkNoError("SP3 SAVEPOINT b", "SAVEPOINT b")
	checkNoError("SP3 update Bob", "UPDATE v46_accounts SET balance = 200 WHERE id = 2")
	check("SP3 Alice at 100", "SELECT balance FROM v46_accounts WHERE id = 1", 100)
	check("SP3 Bob at 200", "SELECT balance FROM v46_accounts WHERE id = 2", 200)
	checkNoError("SP3 ROLLBACK TO b", "ROLLBACK TO SAVEPOINT b")
	check("SP3 Alice still 100", "SELECT balance FROM v46_accounts WHERE id = 1", 100) // Before savepoint b
	check("SP3 Bob restored to 2000", "SELECT balance FROM v46_accounts WHERE id = 2", 2000)
	checkNoError("SP3 ROLLBACK TO a", "ROLLBACK TO SAVEPOINT a")
	check("SP3 Alice restored to 1000", "SELECT balance FROM v46_accounts WHERE id = 1", 1000)
	check("SP3 Bob still 2000", "SELECT balance FROM v46_accounts WHERE id = 2", 2000)
	checkNoError("SP3 COMMIT", "COMMIT")

	// SP4: RELEASE SAVEPOINT
	checkNoError("SP4 BEGIN", "BEGIN")
	checkNoError("SP4 SAVEPOINT", "SAVEPOINT sp4")
	checkNoError("SP4 update", "UPDATE v46_accounts SET balance = 5000 WHERE id = 3")
	checkNoError("SP4 RELEASE", "RELEASE SAVEPOINT sp4")
	check("SP4 changes kept after release", "SELECT balance FROM v46_accounts WHERE id = 3", 5000)
	checkNoError("SP4 COMMIT", "COMMIT")
	check("SP4 changes committed", "SELECT balance FROM v46_accounts WHERE id = 3", 5000)

	// Reset Charlie
	afExec(t, db, ctx, "UPDATE v46_accounts SET balance = 3000 WHERE id = 3")

	// SP5: INSERT then ROLLBACK TO SAVEPOINT
	checkNoError("SP5 BEGIN", "BEGIN")
	checkNoError("SP5 SAVEPOINT", "SAVEPOINT sp5")
	checkNoError("SP5 INSERT", "INSERT INTO v46_accounts VALUES (4, 'Diana', 4000)")
	check("SP5 new row visible", "SELECT balance FROM v46_accounts WHERE id = 4", 4000)
	checkNoError("SP5 ROLLBACK TO", "ROLLBACK TO SAVEPOINT sp5")
	check("SP5 row count restored", "SELECT COUNT(*) FROM v46_accounts", 3) // Diana gone
	checkNoError("SP5 COMMIT", "COMMIT")

	// SP6: DELETE then ROLLBACK TO SAVEPOINT
	checkNoError("SP6 BEGIN", "BEGIN")
	checkNoError("SP6 SAVEPOINT", "SAVEPOINT sp6")
	checkNoError("SP6 DELETE", "DELETE FROM v46_accounts WHERE id = 3")
	check("SP6 row deleted", "SELECT COUNT(*) FROM v46_accounts", 2)
	checkNoError("SP6 ROLLBACK TO", "ROLLBACK TO SAVEPOINT sp6")
	check("SP6 row restored", "SELECT COUNT(*) FROM v46_accounts", 3)
	check("SP6 Charlie restored", "SELECT balance FROM v46_accounts WHERE id = 3", 3000)
	checkNoError("SP6 COMMIT", "COMMIT")

	// SP7: Nested savepoints - rollback only inner
	checkNoError("SP7 BEGIN", "BEGIN")
	checkNoError("SP7 SAVEPOINT outer", "SAVEPOINT outer")
	checkNoError("SP7 update1", "UPDATE v46_accounts SET balance = 9999 WHERE id = 1")
	checkNoError("SP7 SAVEPOINT inner", "SAVEPOINT inner")
	checkNoError("SP7 update2", "UPDATE v46_accounts SET balance = 8888 WHERE id = 2")
	checkNoError("SP7 ROLLBACK TO inner", "ROLLBACK TO SAVEPOINT inner")
	check("SP7 outer change kept", "SELECT balance FROM v46_accounts WHERE id = 1", 9999)
	check("SP7 inner change rolled back", "SELECT balance FROM v46_accounts WHERE id = 2", 2000)
	checkNoError("SP7 COMMIT", "COMMIT")

	// Reset
	afExec(t, db, ctx, "UPDATE v46_accounts SET balance = 1000 WHERE id = 1")

	// SP8: Full transaction rollback clears savepoints
	checkNoError("SP8 BEGIN", "BEGIN")
	checkNoError("SP8 SAVEPOINT", "SAVEPOINT sp8")
	checkNoError("SP8 update", "UPDATE v46_accounts SET balance = 7777 WHERE id = 1")
	checkNoError("SP8 ROLLBACK", "ROLLBACK") // Full rollback, not to savepoint
	check("SP8 full rollback", "SELECT balance FROM v46_accounts WHERE id = 1", 1000)

	// ============================================================
	// === ERROR CASES ===
	// ============================================================

	// ER1: SAVEPOINT outside transaction
	checkError("ER1 SAVEPOINT no txn", "SAVEPOINT bad")

	// ER2: ROLLBACK TO non-existent savepoint
	checkNoError("ER2 BEGIN", "BEGIN")
	checkError("ER2 ROLLBACK TO nonexistent", "ROLLBACK TO SAVEPOINT nosuchsavepoint")
	checkNoError("ER2 ROLLBACK", "ROLLBACK")

	// ER3: RELEASE non-existent savepoint
	checkNoError("ER3 BEGIN", "BEGIN")
	checkError("ER3 RELEASE nonexistent", "RELEASE SAVEPOINT nosuchsavepoint")
	checkNoError("ER3 ROLLBACK", "ROLLBACK")

	// ER4: RELEASE outside transaction
	checkError("ER4 RELEASE no txn", "RELEASE SAVEPOINT bad")

	// ER5: ROLLBACK TO outside transaction
	checkError("ER5 ROLLBACK TO no txn", "ROLLBACK TO SAVEPOINT bad")

	// ============================================================
	// === ADVANCED SCENARIOS ===
	// ============================================================

	// AS1: Savepoint with reuse after rollback to it
	checkNoError("AS1 BEGIN", "BEGIN")
	checkNoError("AS1 SAVEPOINT", "SAVEPOINT retry")
	checkNoError("AS1 update1", "UPDATE v46_accounts SET balance = 111 WHERE id = 1")
	checkNoError("AS1 ROLLBACK TO", "ROLLBACK TO SAVEPOINT retry")
	check("AS1 rolled back", "SELECT balance FROM v46_accounts WHERE id = 1", 1000)
	// Try again after rollback to savepoint
	checkNoError("AS1 update2", "UPDATE v46_accounts SET balance = 222 WHERE id = 1")
	checkNoError("AS1 COMMIT", "COMMIT")
	check("AS1 second attempt committed", "SELECT balance FROM v46_accounts WHERE id = 1", 222)

	// Reset
	afExec(t, db, ctx, "UPDATE v46_accounts SET balance = 1000 WHERE id = 1")

	// AS2: Multiple updates then partial rollback
	checkNoError("AS2 BEGIN", "BEGIN")
	checkNoError("AS2 update Alice", "UPDATE v46_accounts SET balance = balance + 100 WHERE id = 1")
	checkNoError("AS2 update Bob", "UPDATE v46_accounts SET balance = balance + 200 WHERE id = 2")
	checkNoError("AS2 SAVEPOINT", "SAVEPOINT mid")
	checkNoError("AS2 update Charlie", "UPDATE v46_accounts SET balance = balance + 300 WHERE id = 3")
	checkNoError("AS2 ROLLBACK TO mid", "ROLLBACK TO SAVEPOINT mid")
	check("AS2 Alice kept", "SELECT balance FROM v46_accounts WHERE id = 1", 1100) // 1000+100
	check("AS2 Bob kept", "SELECT balance FROM v46_accounts WHERE id = 2", 2200)   // 2000+200
	check("AS2 Charlie rolled back", "SELECT balance FROM v46_accounts WHERE id = 3", 3000)
	checkNoError("AS2 COMMIT", "COMMIT")

	// Reset
	afExec(t, db, ctx, "UPDATE v46_accounts SET balance = 1000 WHERE id = 1")
	afExec(t, db, ctx, "UPDATE v46_accounts SET balance = 2000 WHERE id = 2")

	t.Logf("\n=== V46 SAVEPOINT: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
