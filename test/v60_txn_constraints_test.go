package test

import (
	"fmt"
	"testing"
)

// TestV60TxnConstraints tests transaction rollback/commit, constraint violations,
// complex DML with subqueries, INSERT OR REPLACE, CHECK constraints behavior,
// and multi-statement transaction patterns.
func TestV60TxnConstraints(t *testing.T) {
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
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
		}
	}
	_ = checkNoError
	_ = checkError

	// ============================================================
	// === TRANSACTION ROLLBACK ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_txn (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v60_txn VALUES (1, 'original')")

	// TX1: Transaction commit
	checkNoError("TX1 BEGIN", "BEGIN")
	checkNoError("TX1 INSERT", "INSERT INTO v60_txn VALUES (2, 'committed')")
	checkNoError("TX1 COMMIT", "COMMIT")
	check("TX1 verify", "SELECT val FROM v60_txn WHERE id = 2", "committed")

	// TX2: Transaction rollback
	checkNoError("TX2 BEGIN", "BEGIN")
	checkNoError("TX2 INSERT", "INSERT INTO v60_txn VALUES (3, 'rolled_back')")
	check("TX2 mid-txn visible", "SELECT val FROM v60_txn WHERE id = 3", "rolled_back")
	checkNoError("TX2 ROLLBACK", "ROLLBACK")
	checkRowCount("TX2 rollback verify", "SELECT * FROM v60_txn WHERE id = 3", 0)

	// TX3: Rollback undoes UPDATE
	checkNoError("TX3 BEGIN", "BEGIN")
	checkNoError("TX3 UPDATE", "UPDATE v60_txn SET val = 'modified' WHERE id = 1")
	check("TX3 mid-txn", "SELECT val FROM v60_txn WHERE id = 1", "modified")
	checkNoError("TX3 ROLLBACK", "ROLLBACK")
	check("TX3 rollback verify", "SELECT val FROM v60_txn WHERE id = 1", "original")

	// TX4: Rollback undoes DELETE
	checkNoError("TX4 BEGIN", "BEGIN")
	checkNoError("TX4 DELETE", "DELETE FROM v60_txn WHERE id = 1")
	checkRowCount("TX4 mid-txn", "SELECT * FROM v60_txn WHERE id = 1", 0)
	checkNoError("TX4 ROLLBACK", "ROLLBACK")
	check("TX4 rollback verify", "SELECT val FROM v60_txn WHERE id = 1", "original")

	// ============================================================
	// === CONSTRAINT VIOLATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_pk (id INTEGER PRIMARY KEY, val TEXT NOT NULL)`)
	afExec(t, db, ctx, "INSERT INTO v60_pk VALUES (1, 'first')")

	// CV1: Duplicate PK
	checkError("CV1 dup PK", "INSERT INTO v60_pk VALUES (1, 'duplicate')")

	// CV2: NULL in NOT NULL
	checkError("CV2 NOT NULL", "INSERT INTO v60_pk VALUES (2, NULL)")

	// CV3: UNIQUE constraint
	afExec(t, db, ctx, `CREATE TABLE v60_uniq (id INTEGER PRIMARY KEY, code TEXT UNIQUE)`)
	afExec(t, db, ctx, "INSERT INTO v60_uniq VALUES (1, 'ABC')")
	checkError("CV3 UNIQUE dup", "INSERT INTO v60_uniq VALUES (2, 'ABC')")

	// CV4: UNIQUE allows different values
	checkNoError("CV4 UNIQUE diff", "INSERT INTO v60_uniq VALUES (2, 'DEF')")

	// CV5: After constraint error, table is unchanged
	check("CV5 integrity after error",
		"SELECT COUNT(*) FROM v60_uniq", 2)

	// ============================================================
	// === INSERT OR REPLACE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_replace (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v60_replace VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO v60_replace VALUES (2, 'Bob', 200)")

	// IR1: REPLACE on existing key
	checkNoError("IR1 REPLACE existing",
		"INSERT OR REPLACE INTO v60_replace VALUES (1, 'Alice_updated', 150)")
	check("IR1 verify name", "SELECT name FROM v60_replace WHERE id = 1", "Alice_updated")
	check("IR1 verify val", "SELECT val FROM v60_replace WHERE id = 1", 150)
	check("IR1 count unchanged", "SELECT COUNT(*) FROM v60_replace", 2)

	// IR2: REPLACE on new key (acts as INSERT)
	checkNoError("IR2 REPLACE new",
		"INSERT OR REPLACE INTO v60_replace VALUES (3, 'Carol', 300)")
	check("IR2 verify", "SELECT name FROM v60_replace WHERE id = 3", "Carol")
	check("IR2 count", "SELECT COUNT(*) FROM v60_replace", 3)

	// ============================================================
	// === INSERT OR IGNORE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_ignore (id INTEGER PRIMARY KEY, val TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v60_ignore VALUES (1, 'keep')")

	// II1: IGNORE on duplicate key
	checkNoError("II1 IGNORE dup",
		"INSERT OR IGNORE INTO v60_ignore VALUES (1, 'ignored')")
	check("II1 original kept", "SELECT val FROM v60_ignore WHERE id = 1", "keep")

	// II2: IGNORE on new key (acts as INSERT)
	checkNoError("II2 IGNORE new",
		"INSERT OR IGNORE INTO v60_ignore VALUES (2, 'new')")
	check("II2 verify", "SELECT val FROM v60_ignore WHERE id = 2", "new")

	// ============================================================
	// === COMPLEX UPDATE WITH SUBQUERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_emp (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v60_emp VALUES (1, 'Alice', 'Eng', 80000)")
	afExec(t, db, ctx, "INSERT INTO v60_emp VALUES (2, 'Bob', 'Eng', 75000)")
	afExec(t, db, ctx, "INSERT INTO v60_emp VALUES (3, 'Carol', 'Sales', 70000)")
	afExec(t, db, ctx, "INSERT INTO v60_emp VALUES (4, 'Dave', 'Sales', 65000)")
	afExec(t, db, ctx, "INSERT INTO v60_emp VALUES (5, 'Eve', 'HR', 60000)")

	// US1: UPDATE with IN subquery
	checkNoError("US1 UPDATE IN subquery",
		`UPDATE v60_emp SET salary = salary + 5000
		 WHERE dept IN (SELECT dept FROM v60_emp GROUP BY dept HAVING COUNT(*) > 1)`)
	check("US1 Alice", "SELECT salary FROM v60_emp WHERE name = 'Alice'", 85000)
	check("US1 Eve unchanged", "SELECT salary FROM v60_emp WHERE name = 'Eve'", 60000)

	// US2: UPDATE with scalar subquery in WHERE (non-correlated)
	checkNoError("US2 UPDATE scalar sub",
		`UPDATE v60_emp SET salary = salary + 10000
		 WHERE salary = (SELECT MAX(salary) FROM v60_emp)`)
	// Alice(85000) is global max → becomes 95000
	check("US2 Alice", "SELECT salary FROM v60_emp WHERE name = 'Alice'", 95000)
	check("US2 Bob unchanged", "SELECT salary FROM v60_emp WHERE name = 'Bob'", 80000)
	check("US2 Eve unchanged", "SELECT salary FROM v60_emp WHERE name = 'Eve'", 60000)

	// ============================================================
	// === DELETE WITH SUBQUERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_del (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v60_del VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v60_del VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO v60_del VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO v60_del VALUES (4, 'b', 40)")
	afExec(t, db, ctx, "INSERT INTO v60_del VALUES (5, 'c', 50)")

	// DS1: DELETE with IN subquery
	checkNoError("DS1 DELETE IN sub",
		"DELETE FROM v60_del WHERE cat IN (SELECT cat FROM v60_del GROUP BY cat HAVING SUM(val) > 40)")
	// cat totals: a=40, b=60, c=50 → delete b and c
	checkRowCount("DS1 verify", "SELECT * FROM v60_del", 2)
	// Remaining: id=1(a,10), id=3(a,30)

	// ============================================================
	// === INSERT INTO SELECT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_archive (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER)`)

	// IS1: INSERT INTO SELECT
	checkNoError("IS1 INSERT SELECT",
		"INSERT INTO v60_archive SELECT id, name, salary FROM v60_emp WHERE salary >= 80000")
	checkRowCount("IS1 verify", "SELECT * FROM v60_archive", 2)
	// Alice(95000) and Bob(80000) qualify

	// IS2: INSERT INTO SELECT with expression
	afExec(t, db, ctx, `CREATE TABLE v60_summary (dept TEXT, avg_salary INTEGER)`)
	checkNoError("IS2 INSERT SELECT agg",
		"INSERT INTO v60_summary SELECT dept, AVG(salary) FROM v60_emp GROUP BY dept")
	checkRowCount("IS2 verify", "SELECT * FROM v60_summary", 3)

	// ============================================================
	// === MULTIPLE UPDATES SAME TABLE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_multi (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v60_multi VALUES (1, 10, 100)")
	afExec(t, db, ctx, "INSERT INTO v60_multi VALUES (2, 20, 200)")

	// MU1: Update multiple columns
	checkNoError("MU1 multi col update",
		"UPDATE v60_multi SET a = a + 5, b = b - 50 WHERE id = 1")
	check("MU1 verify a", "SELECT a FROM v60_multi WHERE id = 1", 15)
	check("MU1 verify b", "SELECT b FROM v60_multi WHERE id = 1", 50)

	// MU2: Update all rows
	checkNoError("MU2 update all",
		"UPDATE v60_multi SET a = a * 2")
	check("MU2 verify 1", "SELECT a FROM v60_multi WHERE id = 1", 30)
	check("MU2 verify 2", "SELECT a FROM v60_multi WHERE id = 2", 40)

	// ============================================================
	// === TRANSACTION WITH MULTIPLE STATEMENTS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_account (id INTEGER PRIMARY KEY, balance INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v60_account VALUES (1, 1000)")
	afExec(t, db, ctx, "INSERT INTO v60_account VALUES (2, 500)")

	// TM1: Multi-statement transaction (transfer)
	checkNoError("TM1 BEGIN", "BEGIN")
	checkNoError("TM1 debit", "UPDATE v60_account SET balance = balance - 200 WHERE id = 1")
	checkNoError("TM1 credit", "UPDATE v60_account SET balance = balance + 200 WHERE id = 2")
	checkNoError("TM1 COMMIT", "COMMIT")
	check("TM1 balance 1", "SELECT balance FROM v60_account WHERE id = 1", 800)
	check("TM1 balance 2", "SELECT balance FROM v60_account WHERE id = 2", 700)
	check("TM1 total", "SELECT SUM(balance) FROM v60_account", 1500)

	// TM2: Multi-statement rollback (failed transfer)
	checkNoError("TM2 BEGIN", "BEGIN")
	checkNoError("TM2 debit", "UPDATE v60_account SET balance = balance - 300 WHERE id = 1")
	checkNoError("TM2 credit", "UPDATE v60_account SET balance = balance + 300 WHERE id = 2")
	checkNoError("TM2 ROLLBACK", "ROLLBACK")
	check("TM2 balance 1", "SELECT balance FROM v60_account WHERE id = 1", 800)
	check("TM2 balance 2", "SELECT balance FROM v60_account WHERE id = 2", 700)

	// ============================================================
	// === SAVEPOINT ===
	// ============================================================

	checkNoError("SP BEGIN", "BEGIN")
	checkNoError("SP insert", "INSERT INTO v60_account VALUES (3, 300)")
	checkNoError("SP SAVEPOINT", "SAVEPOINT sp1")
	checkNoError("SP update", "UPDATE v60_account SET balance = 999 WHERE id = 3")
	check("SP mid", "SELECT balance FROM v60_account WHERE id = 3", 999)
	checkNoError("SP ROLLBACK TO", "ROLLBACK TO sp1")
	check("SP rollback to sp", "SELECT balance FROM v60_account WHERE id = 3", 300)
	checkNoError("SP COMMIT", "COMMIT")
	check("SP final", "SELECT balance FROM v60_account WHERE id = 3", 300)

	// ============================================================
	// === COMPLEX QUERY AFTER DML ===
	// ============================================================

	// CQ1: CTE after multiple DML operations
	check("CQ1 CTE after DML",
		`WITH totals AS (
			SELECT SUM(balance) AS total FROM v60_account
		)
		SELECT total FROM totals`, 1800)

	// CQ2: Window function after DML
	check("CQ2 window after DML",
		`WITH ranked AS (
			SELECT id, balance, RANK() OVER (ORDER BY balance DESC) AS rnk
			FROM v60_account
		)
		SELECT id FROM ranked WHERE rnk = 1`, 1)
	// id=1:800, id=2:700, id=3:300 → id=1 has highest

	// ============================================================
	// === CREATE TABLE AS SELECT (if supported) ===
	// ============================================================

	// This is an advanced feature — test if it works
	// afExec(t, db, ctx, "CREATE TABLE v60_derived AS SELECT dept, AVG(salary) as avg_sal FROM v60_emp GROUP BY dept")

	// ============================================================
	// === EDGE: UPDATE SET to NULL ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v60_nullable (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v60_nullable VALUES (1, 42)")

	checkNoError("NU1 SET NULL", "UPDATE v60_nullable SET val = NULL WHERE id = 1")
	checkRowCount("NU1 verify IS NULL", "SELECT * FROM v60_nullable WHERE val IS NULL", 1)

	// Then update it back
	checkNoError("NU2 SET non-null", "UPDATE v60_nullable SET val = 99 WHERE id = 1")
	check("NU2 verify", "SELECT val FROM v60_nullable WHERE id = 1", 99)

	t.Logf("\n=== V60 TXN & CONSTRAINTS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
