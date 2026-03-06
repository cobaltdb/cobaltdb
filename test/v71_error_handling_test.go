package test

import (
	"fmt"
	"strings"
	"testing"
)

// TestV71ErrorHandling tests error handling, constraint violations, recovery after errors,
// and edge cases that should produce errors or graceful handling.
func TestV71ErrorHandling(t *testing.T) {
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

	checkQueryError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Query(ctx, sql)
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
		}
	}
	_ = checkQueryError

	// ============================================================
	// === TABLE ERRORS ===
	// ============================================================

	// TE1: SELECT from non-existent table
	checkError("TE1 non-existent table", "SELECT * FROM no_such_table")

	// TE2: INSERT into non-existent table
	checkError("TE2 INSERT non-existent", "INSERT INTO no_such_table VALUES (1)")

	// TE3: UPDATE non-existent table
	checkError("TE3 UPDATE non-existent", "UPDATE no_such_table SET x = 1")

	// TE4: DELETE from non-existent table
	checkError("TE4 DELETE non-existent", "DELETE FROM no_such_table")

	// TE5: DROP non-existent table (without IF EXISTS)
	checkError("TE5 DROP non-existent", "DROP TABLE no_such_table")

	// TE6: DROP IF EXISTS non-existent (should NOT error)
	checkNoError("TE6 DROP IF EXISTS", "DROP TABLE IF EXISTS no_such_table")

	// TE7: CREATE TABLE that already exists
	checkNoError("TE7 create first", "CREATE TABLE v71_dup (id INTEGER PRIMARY KEY)")
	checkError("TE7 create duplicate", "CREATE TABLE v71_dup (id INTEGER PRIMARY KEY)")

	// TE8: CREATE TABLE IF NOT EXISTS (should NOT error)
	checkNoError("TE8 IF NOT EXISTS",
		"CREATE TABLE IF NOT EXISTS v71_dup (id INTEGER PRIMARY KEY)")

	// ============================================================
	// === PRIMARY KEY VIOLATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_pk (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v71_pk VALUES (1, 'alice')")

	// PK1: Duplicate PK
	checkError("PK1 duplicate PK", "INSERT INTO v71_pk VALUES (1, 'bob')")

	// PK2: Table still usable after PK error
	check("PK2 table ok", "SELECT name FROM v71_pk WHERE id = 1", "alice")
	check("PK2 count ok", "SELECT COUNT(*) FROM v71_pk", 1)

	// PK3: Can insert different PK
	checkNoError("PK3 different PK", "INSERT INTO v71_pk VALUES (2, 'bob')")
	check("PK3 verify", "SELECT COUNT(*) FROM v71_pk", 2)

	// ============================================================
	// === NOT NULL VIOLATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_nn (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)

	// NN1: NULL in NOT NULL column
	checkError("NN1 NULL not null", "INSERT INTO v71_nn VALUES (1, NULL)")

	// NN2: Table still usable
	checkNoError("NN2 valid insert", "INSERT INTO v71_nn VALUES (1, 'alice')")
	check("NN2 verify", "SELECT name FROM v71_nn WHERE id = 1", "alice")

	// ============================================================
	// === UNIQUE VIOLATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_uq (id INTEGER PRIMARY KEY, email TEXT UNIQUE)`)
	afExec(t, db, ctx, "INSERT INTO v71_uq VALUES (1, 'alice@test.com')")

	// UQ1: Duplicate unique value
	checkError("UQ1 duplicate unique", "INSERT INTO v71_uq VALUES (2, 'alice@test.com')")

	// UQ2: Table still usable
	check("UQ2 count", "SELECT COUNT(*) FROM v71_uq", 1)
	checkNoError("UQ2 different value", "INSERT INTO v71_uq VALUES (2, 'bob@test.com')")
	check("UQ2 count after", "SELECT COUNT(*) FROM v71_uq", 2)

	// ============================================================
	// === CHECK VIOLATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_chk (
		id INTEGER PRIMARY KEY,
		age INTEGER CHECK(age >= 0),
		score INTEGER CHECK(score >= 0 AND score <= 100)
	)`)

	// CK1: Valid data
	checkNoError("CK1 valid", "INSERT INTO v71_chk VALUES (1, 25, 85)")

	// CK2: Negative age
	checkError("CK2 negative age", "INSERT INTO v71_chk VALUES (2, -1, 50)")

	// CK3: Score > 100
	checkError("CK3 score > 100", "INSERT INTO v71_chk VALUES (3, 20, 101)")

	// CK4: Score < 0
	checkError("CK4 score < 0", "INSERT INTO v71_chk VALUES (4, 20, -5)")

	// CK5: Table still usable
	check("CK5 count", "SELECT COUNT(*) FROM v71_chk", 1)

	// ============================================================
	// === TRANSACTION ERROR RECOVERY ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_txn (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v71_txn VALUES (1, 100)")

	// TR1: Error in transaction, then rollback
	checkNoError("TR1 BEGIN", "BEGIN")
	checkNoError("TR1 UPDATE", "UPDATE v71_txn SET val = 200 WHERE id = 1")
	checkError("TR1 bad insert", "INSERT INTO v71_txn VALUES (1, 999)") // PK violation
	checkNoError("TR1 ROLLBACK", "ROLLBACK")
	check("TR1 verify original", "SELECT val FROM v71_txn WHERE id = 1", 100)

	// TR2: Successful transaction after failed one
	checkNoError("TR2 BEGIN", "BEGIN")
	checkNoError("TR2 UPDATE", "UPDATE v71_txn SET val = 300 WHERE id = 1")
	checkNoError("TR2 COMMIT", "COMMIT")
	check("TR2 verify", "SELECT val FROM v71_txn WHERE id = 1", 300)

	// ============================================================
	// === INSERT OR REPLACE / INSERT OR IGNORE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_upsert (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v71_upsert VALUES (1, 100)")

	// IR1: INSERT OR REPLACE
	checkNoError("IR1 REPLACE", "INSERT OR REPLACE INTO v71_upsert VALUES (1, 200)")
	check("IR1 verify", "SELECT val FROM v71_upsert WHERE id = 1", 200)

	// IR2: INSERT OR IGNORE
	checkNoError("IR2 IGNORE", "INSERT OR IGNORE INTO v71_upsert VALUES (1, 300)")
	check("IR2 verify unchanged", "SELECT val FROM v71_upsert WHERE id = 1", 200)

	// IR3: INSERT OR IGNORE with new key
	checkNoError("IR3 IGNORE new", "INSERT OR IGNORE INTO v71_upsert VALUES (2, 400)")
	check("IR3 verify new", "SELECT val FROM v71_upsert WHERE id = 2", 400)

	// ============================================================
	// === COLUMN COUNT MISMATCH ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_cols (id INTEGER PRIMARY KEY, a TEXT, b TEXT)`)

	// CC1: Too few values
	checkError("CC1 too few values", "INSERT INTO v71_cols VALUES (1, 'a')")

	// CC2: Too many values
	checkError("CC2 too many values", "INSERT INTO v71_cols VALUES (1, 'a', 'b', 'c')")

	// CC3: Correct count works
	checkNoError("CC3 correct", "INSERT INTO v71_cols VALUES (1, 'a', 'b')")

	// ============================================================
	// === RECOVERY AFTER ERRORS ===
	// ============================================================

	// RE1: Multiple errors then success
	checkError("RE1 error 1", "SELECT * FROM nonexistent1")
	checkError("RE1 error 2", "INSERT INTO nonexistent2 VALUES (1)")

	// RE2: Error doesn't corrupt state - table still queryable
	check("RE2 verify state", "SELECT COUNT(*) FROM v71_cols", 1)

	// ============================================================
	// === UPDATE/DELETE EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_upd (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v71_upd VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v71_upd VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO v71_upd VALUES (3, 30)")

	// UE1: UPDATE with WHERE that matches nothing
	checkNoError("UE1 UPDATE no match",
		"UPDATE v71_upd SET val = 999 WHERE id = 100")
	check("UE1 verify unchanged", "SELECT SUM(val) FROM v71_upd", 60)

	// UE2: DELETE with WHERE that matches nothing
	checkNoError("UE2 DELETE no match",
		"DELETE FROM v71_upd WHERE id = 100")
	check("UE2 verify unchanged", "SELECT COUNT(*) FROM v71_upd", 3)

	// UE3: UPDATE all rows
	checkNoError("UE3 UPDATE all", "UPDATE v71_upd SET val = 0")
	check("UE3 verify", "SELECT SUM(val) FROM v71_upd", 0)

	// UE4: DELETE all rows
	checkNoError("UE4 DELETE all", "DELETE FROM v71_upd")
	check("UE4 verify", "SELECT COUNT(*) FROM v71_upd", 0)

	// UE5: Operations on empty table
	checkNoError("UE5 UPDATE empty",
		"UPDATE v71_upd SET val = 1 WHERE id = 1")
	checkNoError("UE5 DELETE empty",
		"DELETE FROM v71_upd WHERE id = 1")

	// ============================================================
	// === COMPLEX ERROR SCENARIOS ===
	// ============================================================

	// CS1: SAVEPOINT rollback preserves data
	afExec(t, db, ctx, `CREATE TABLE v71_sp (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v71_sp VALUES (1, 100)")

	checkNoError("CS1 BEGIN", "BEGIN")
	checkNoError("CS1 UPDATE", "UPDATE v71_sp SET val = 200 WHERE id = 1")
	checkNoError("CS1 SAVEPOINT", "SAVEPOINT sp1")
	checkNoError("CS1 UPDATE 2", "UPDATE v71_sp SET val = 300 WHERE id = 1")
	checkNoError("CS1 ROLLBACK TO", "ROLLBACK TO sp1")
	checkNoError("CS1 COMMIT", "COMMIT")
	check("CS1 verify savepoint", "SELECT val FROM v71_sp WHERE id = 1", 200)

	// ============================================================
	// === ALTER TABLE EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_alter (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v71_alter VALUES (1, 'test')")

	// AT1: ADD COLUMN with default
	checkNoError("AT1 ADD COLUMN",
		"ALTER TABLE v71_alter ADD COLUMN val INTEGER DEFAULT 42")
	check("AT1 verify default", "SELECT val FROM v71_alter WHERE id = 1", 42)

	// AT2: Insert with new column
	checkNoError("AT2 insert new col",
		"INSERT INTO v71_alter VALUES (2, 'test2', 99)")
	check("AT2 verify", "SELECT val FROM v71_alter WHERE id = 2", 99)

	// ============================================================
	// === EDGE CASE QUERIES ===
	// ============================================================

	// EQ1: SELECT with no results
	checkRowCount("EQ1 empty WHERE",
		"SELECT * FROM v71_pk WHERE 1 = 0", 0)

	// EQ2: COUNT of empty result
	check("EQ2 COUNT empty WHERE",
		"SELECT COUNT(*) FROM v71_pk WHERE 1 = 0", 0)

	// EQ3: Aggregate of empty result
	check("EQ3 SUM empty WHERE", "SELECT SUM(val) FROM v71_upd", nil)

	// EQ4: SELECT DISTINCT on single value
	afExec(t, db, ctx, `CREATE TABLE v71_single (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v71_single VALUES (1, 42)")
	afExec(t, db, ctx, "INSERT INTO v71_single VALUES (2, 42)")
	afExec(t, db, ctx, "INSERT INTO v71_single VALUES (3, 42)")
	checkRowCount("EQ4 DISTINCT single", "SELECT DISTINCT val FROM v71_single", 1)

	// ============================================================
	// === LONG STRINGS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v71_longstr (id INTEGER PRIMARY KEY, data TEXT)`)

	// LS1: Very long string (1000 chars)
	longStr := strings.Repeat("abcdefghij", 100) // 1000 chars
	checkNoError("LS1 long string",
		fmt.Sprintf("INSERT INTO v71_longstr VALUES (1, '%s')", longStr))
	check("LS1 verify length",
		"SELECT LENGTH(data) FROM v71_longstr WHERE id = 1", 1000)

	// LS2: String with spaces
	checkNoError("LS2 spaces",
		"INSERT INTO v71_longstr VALUES (2, 'hello world test 123')")
	check("LS2 verify", "SELECT data FROM v71_longstr WHERE id = 2", "hello world test 123")

	// ============================================================
	// === MULTIPLE TABLES ===
	// ============================================================

	// MT1: Many tables in sequence
	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("v71_multi_%d", i)
		checkNoError(fmt.Sprintf("MT1 create %d", i),
			fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, val INTEGER)", name))
		checkNoError(fmt.Sprintf("MT1 insert %d", i),
			fmt.Sprintf("INSERT INTO %s VALUES (1, %d)", name, i*10))
	}
	check("MT1 verify 5", "SELECT val FROM v71_multi_5 WHERE id = 1", 50)
	check("MT1 verify 10", "SELECT val FROM v71_multi_10 WHERE id = 1", 100)

	t.Logf("\n=== V71 ERROR HANDLING: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
