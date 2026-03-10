package test

import (
	"fmt"
	"strings"
	"testing"
)

// TestV34DataIntegrityEdgeCases covers a broad sweep of constraint enforcement,
// default values, auto-increment behaviour, cascade semantics, transaction
// isolation, large/special-character data, numeric boundaries, empty-table
// aggregates, multi-column indexes, ALTER TABLE, upsert modes, multi-column
// ORDER BY, scalar subquery semantics, BETWEEN, complex WHERE nesting,
// COUNT(*) vs COUNT(col), and table re-creation.
//
// All table names are prefixed with v34_ to avoid collisions with other tests.
func TestV34DataIntegrityEdgeCases(t *testing.T) {
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
	// === SECTION 1: UNIQUE CONSTRAINT ENFORCEMENT ===
	// ============================================================
	//
	// Table: v34_unique_emails (id PK, email UNIQUE, name TEXT)
	// Verify that duplicate emails are rejected, and that re-inserting
	// an email that was previously deleted is accepted.

	afExec(t, db, ctx, `CREATE TABLE v34_unique_emails (
		id    INTEGER PRIMARY KEY,
		email TEXT    UNIQUE,
		name  TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_unique_emails VALUES (1, 'alice@example.com', 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v34_unique_emails VALUES (2, 'bob@example.com',   'Bob')")

	// Test 1: duplicate email must fail
	checkError("UNIQUE violation on duplicate email",
		"INSERT INTO v34_unique_emails VALUES (3, 'alice@example.com', 'Imposter')")

	// Test 2: table still has exactly 2 rows after failed insert
	checkRowCount("Table unchanged after UNIQUE violation",
		"SELECT * FROM v34_unique_emails", 2)

	// Test 3: different email succeeds
	checkNoError("Insert distinct email succeeds",
		"INSERT INTO v34_unique_emails VALUES (3, 'carol@example.com', 'Carol')")

	// Test 4: delete alice's row, then re-insert with her email must succeed
	checkNoError("Delete alice row",
		"DELETE FROM v34_unique_emails WHERE id = 1")
	checkNoError("Re-insert freed UNIQUE email",
		"INSERT INTO v34_unique_emails VALUES (4, 'alice@example.com', 'Alice-New')")
	check("Re-inserted row present",
		"SELECT name FROM v34_unique_emails WHERE email = 'alice@example.com'", "Alice-New")

	// Test 5: UPDATE that collides with existing UNIQUE value must fail
	checkError("UNIQUE violation on UPDATE to duplicate email",
		"UPDATE v34_unique_emails SET email = 'bob@example.com' WHERE id = 4")

	// ============================================================
	// === SECTION 2: NOT NULL CONSTRAINT ===
	// ============================================================
	//
	// Table: v34_notnull (id PK, label TEXT NOT NULL, score INTEGER)

	afExec(t, db, ctx, `CREATE TABLE v34_notnull (
		id    INTEGER PRIMARY KEY,
		label TEXT    NOT NULL,
		score INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_notnull VALUES (1, 'first', 10)")

	// Test 6: NULL into NOT NULL column must fail
	checkError("NOT NULL violation on INSERT",
		"INSERT INTO v34_notnull VALUES (2, NULL, 20)")

	// Test 7: existing row unaffected after failed insert
	checkRowCount("Only original row after NOT NULL failure",
		"SELECT * FROM v34_notnull", 1)

	// Test 8: UPDATE to NULL on NOT NULL column must fail
	checkError("NOT NULL violation on UPDATE",
		"UPDATE v34_notnull SET label = NULL WHERE id = 1")

	// Test 9: original value preserved after failed UPDATE
	check("Label unchanged after NOT NULL UPDATE failure",
		"SELECT label FROM v34_notnull WHERE id = 1", "first")

	// Test 10: nullable column (score) accepts NULL
	checkNoError("NULL into nullable column is accepted",
		"INSERT INTO v34_notnull VALUES (3, 'third', NULL)")
	checkRowCount("Nullable NULL insert gives 2 rows",
		"SELECT * FROM v34_notnull WHERE score IS NULL", 1)

	// ============================================================
	// === SECTION 3: DEFAULT VALUES ===
	// ============================================================
	//
	// Table: v34_defaults (id PK, status TEXT DEFAULT 'pending',
	//                      priority INTEGER DEFAULT 5, ratio REAL DEFAULT 1.0)

	afExec(t, db, ctx, `CREATE TABLE v34_defaults (
		id       INTEGER PRIMARY KEY,
		status   TEXT    DEFAULT 'pending',
		priority INTEGER DEFAULT 5,
		ratio    REAL    DEFAULT 1.0
	)`)

	// Test 11: omitting columns uses DEFAULT values
	checkNoError("INSERT using all DEFAULT values",
		"INSERT INTO v34_defaults (id) VALUES (1)")
	check("Default text value applied",
		"SELECT status FROM v34_defaults WHERE id = 1", "pending")
	check("Default integer value applied",
		"SELECT priority FROM v34_defaults WHERE id = 1", 5)

	// Test 12: explicit value overrides DEFAULT
	checkNoError("INSERT overriding defaults",
		"INSERT INTO v34_defaults (id, status, priority) VALUES (2, 'active', 10)")
	check("Overridden text default",
		"SELECT status FROM v34_defaults WHERE id = 2", "active")
	check("Overridden integer default",
		"SELECT priority FROM v34_defaults WHERE id = 2", 10)

	// Test 13: mix of explicit and default columns
	checkNoError("INSERT mixing explicit and default",
		"INSERT INTO v34_defaults (id, status) VALUES (3, 'done')")
	check("Explicit status column value",
		"SELECT status FROM v34_defaults WHERE id = 3", "done")
	check("Defaulted priority on mixed insert",
		"SELECT priority FROM v34_defaults WHERE id = 3", 5)

	// Test 14: DEFAULT integer 0 is a valid default (not the same as no default)
	afExec(t, db, ctx, "CREATE TABLE v34_zero_default (id INTEGER PRIMARY KEY, count INTEGER DEFAULT 0)")
	checkNoError("INSERT using zero DEFAULT",
		"INSERT INTO v34_zero_default (id) VALUES (1)")
	check("Default zero is applied",
		"SELECT count FROM v34_zero_default WHERE id = 1", 0)

	// ============================================================
	// === SECTION 4: AUTO_INCREMENT EDGE CASES ===
	// ============================================================
	//
	// Table: v34_autoinc (id INTEGER PRIMARY KEY AUTO_INCREMENT, val TEXT)
	// After deleting the highest ID the sequence must continue upward,
	// not recycle the deleted ID.

	afExec(t, db, ctx, "CREATE TABLE v34_autoinc (id INTEGER PRIMARY KEY AUTO_INCREMENT, val TEXT)")

	// Test 15: first three rows get IDs 1, 2, 3
	checkNoError("Auto-inc insert 1", "INSERT INTO v34_autoinc (val) VALUES ('a')")
	checkNoError("Auto-inc insert 2", "INSERT INTO v34_autoinc (val) VALUES ('b')")
	checkNoError("Auto-inc insert 3", "INSERT INTO v34_autoinc (val) VALUES ('c')")
	check("Auto-inc id 1", "SELECT id FROM v34_autoinc WHERE val = 'a'", 1)
	check("Auto-inc id 2", "SELECT id FROM v34_autoinc WHERE val = 'b'", 2)
	check("Auto-inc id 3", "SELECT id FROM v34_autoinc WHERE val = 'c'", 3)

	// Test 16: delete highest ID (3) and insert again - expect id 4, not 3
	checkNoError("Delete highest auto-inc row",
		"DELETE FROM v34_autoinc WHERE id = 3")
	checkNoError("Insert after deleting highest id",
		"INSERT INTO v34_autoinc (val) VALUES ('d')")
	check("New auto-inc is 4, not recycled 3",
		"SELECT id FROM v34_autoinc WHERE val = 'd'", 4)

	// Test 17: delete all rows and insert - ID continues from high-water mark
	checkNoError("Delete all auto-inc rows",
		"DELETE FROM v34_autoinc")
	checkNoError("Insert after deleting all rows",
		"INSERT INTO v34_autoinc (val) VALUES ('e')")
	// ID must be > 4 (the previous high-water mark); we check it is not 1
	checkRowCount("Exactly 1 row after re-insert",
		"SELECT * FROM v34_autoinc", 1)

	// Test 18: multiple delete-insert cycles keep IDs strictly increasing
	checkNoError("Second cycle insert 1", "INSERT INTO v34_autoinc (val) VALUES ('f')")
	checkNoError("Second cycle insert 2", "INSERT INTO v34_autoinc (val) VALUES ('g')")
	checkRowCount("Three rows after second cycle",
		"SELECT * FROM v34_autoinc", 3)
	// The MIN id of the new rows must be > 4 (greater than old high-water mark 4).
	// Verified by confirming no row with id <= 4 exists in the table.
	checkRowCount("No row with recycled id (id <= 4) in new cycle",
		"SELECT * FROM v34_autoinc WHERE id <= 4", 0)

	// ============================================================
	// === SECTION 5: FOREIGN KEY CONSTRAINT VIOLATIONS ===
	// ============================================================
	//
	// Tables: v34_fk_parent (id PK, name), v34_fk_child (id PK, parent_id FK)

	afExec(t, db, ctx, `CREATE TABLE v34_fk_parent (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, `CREATE TABLE v34_fk_child (
		id        INTEGER PRIMARY KEY,
		parent_id INTEGER,
		FOREIGN KEY (parent_id) REFERENCES v34_fk_parent(id)
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_fk_parent VALUES (1, 'Parent-One')")
	afExec(t, db, ctx, "INSERT INTO v34_fk_parent VALUES (2, 'Parent-Two')")

	// Test 19: valid FK insert succeeds
	checkNoError("Valid FK insert",
		"INSERT INTO v34_fk_child VALUES (1, 1)")

	// Test 20: FK violation (non-existent parent) fails
	checkError("FK violation - non-existent parent",
		"INSERT INTO v34_fk_child VALUES (2, 999)")

	// Test 21: child table unchanged after FK violation
	checkRowCount("Child table unchanged after FK violation",
		"SELECT * FROM v34_fk_child", 1)

	// Test 22: NULL FK value is always accepted (no parent required)
	checkNoError("NULL FK value accepted",
		"INSERT INTO v34_fk_child VALUES (3, NULL)")
	checkRowCount("Child row with NULL FK exists",
		"SELECT * FROM v34_fk_child WHERE parent_id IS NULL", 1)

	// Test 23: FK violation on UPDATE
	checkError("FK violation on UPDATE to non-existent parent",
		"UPDATE v34_fk_child SET parent_id = 888 WHERE id = 1")
	check("FK child unchanged after failed UPDATE",
		"SELECT parent_id FROM v34_fk_child WHERE id = 1", 1)

	// ============================================================
	// === SECTION 6: CASCADE BEHAVIORS (MULTI-LEVEL) ===
	// ============================================================
	//
	// Three-level hierarchy: grandparent -> parent -> grandchild
	// ON DELETE CASCADE propagates all the way down.

	afExec(t, db, ctx, "CREATE TABLE v34_gp (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v34_par (
		id INTEGER PRIMARY KEY,
		gp_id INTEGER,
		name TEXT,
		FOREIGN KEY (gp_id) REFERENCES v34_gp(id) ON DELETE CASCADE
	)`)
	afExec(t, db, ctx, `CREATE TABLE v34_gc (
		id INTEGER PRIMARY KEY,
		par_id INTEGER,
		name TEXT,
		FOREIGN KEY (par_id) REFERENCES v34_par(id) ON DELETE CASCADE
	)`)

	afExec(t, db, ctx, "INSERT INTO v34_gp VALUES (1, 'GP1')")
	afExec(t, db, ctx, "INSERT INTO v34_gp VALUES (2, 'GP2')")
	afExec(t, db, ctx, "INSERT INTO v34_par VALUES (10, 1, 'P10')")
	afExec(t, db, ctx, "INSERT INTO v34_par VALUES (11, 1, 'P11')")
	afExec(t, db, ctx, "INSERT INTO v34_par VALUES (12, 2, 'P12')")
	afExec(t, db, ctx, "INSERT INTO v34_gc VALUES (100, 10, 'GC100')")
	afExec(t, db, ctx, "INSERT INTO v34_gc VALUES (101, 10, 'GC101')")
	afExec(t, db, ctx, "INSERT INTO v34_gc VALUES (102, 11, 'GC102')")
	afExec(t, db, ctx, "INSERT INTO v34_gc VALUES (103, 12, 'GC103')")

	// Test 24: verify initial state
	checkRowCount("Initial grandchild count", "SELECT * FROM v34_gc", 4)

	// Test 25: delete GP1 cascades through parents 10, 11 into grandchildren 100, 101, 102
	checkNoError("Delete GP1 with cascade",
		"DELETE FROM v34_gp WHERE id = 1")
	checkRowCount("GP1 deleted", "SELECT * FROM v34_gp WHERE id = 1", 0)
	checkRowCount("Parents of GP1 cascaded", "SELECT * FROM v34_par WHERE gp_id = 1", 0)
	checkRowCount("GC via GP1 parents cascaded",
		"SELECT * FROM v34_gc WHERE par_id IN (10, 11)", 0)

	// Test 26: GP2 hierarchy (P12, GC103) must be untouched
	checkRowCount("GP2 still present", "SELECT * FROM v34_gp WHERE id = 2", 1)
	checkRowCount("Parent of GP2 still present", "SELECT * FROM v34_par WHERE gp_id = 2", 1)
	checkRowCount("Grandchild of GP2 still present", "SELECT * FROM v34_gc WHERE par_id = 12", 1)

	// Test 27: ON DELETE SET NULL on a second FK table
	afExec(t, db, ctx, "CREATE TABLE v34_tag_owner (id INTEGER PRIMARY KEY, tag TEXT)")
	afExec(t, db, ctx, `CREATE TABLE v34_tagged (
		id       INTEGER PRIMARY KEY,
		owner_id INTEGER,
		label    TEXT,
		FOREIGN KEY (owner_id) REFERENCES v34_tag_owner(id) ON DELETE SET NULL
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_tag_owner VALUES (1, 'owner-a')")
	afExec(t, db, ctx, "INSERT INTO v34_tagged VALUES (1, 1, 'item1')")
	afExec(t, db, ctx, "INSERT INTO v34_tagged VALUES (2, 1, 'item2')")

	checkNoError("Delete owner triggers SET NULL",
		"DELETE FROM v34_tag_owner WHERE id = 1")
	checkRowCount("Tagged rows with NULL owner_id after SET NULL",
		"SELECT * FROM v34_tagged WHERE owner_id IS NULL", 2)

	// ============================================================
	// === SECTION 7: TRANSACTION ISOLATION ===
	// ============================================================
	//
	// Multiple DML operations in one transaction; partial rollback verification.

	afExec(t, db, ctx, "CREATE TABLE v34_txn (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v34_txn VALUES (1, 'stable')")
	afExec(t, db, ctx, "INSERT INTO v34_txn VALUES (2, 'stable-2')")

	// Test 28: BEGIN then multiple inserts visible within txn
	checkNoError("BEGIN transaction", "BEGIN")
	checkNoError("Insert in txn row 3", "INSERT INTO v34_txn VALUES (3, 'txn-row')")
	checkNoError("Insert in txn row 4", "INSERT INTO v34_txn VALUES (4, 'txn-row-2')")
	checkRowCount("4 rows visible inside txn", "SELECT * FROM v34_txn", 4)

	// Test 29: ROLLBACK removes the in-transaction inserts
	checkNoError("ROLLBACK transaction", "ROLLBACK")
	checkRowCount("Only 2 rows after rollback", "SELECT * FROM v34_txn", 2)
	check("Pre-txn data intact after rollback",
		"SELECT val FROM v34_txn WHERE id = 1", "stable")

	// Test 30: BEGIN, UPDATE then ROLLBACK restores original values
	checkNoError("BEGIN for UPDATE test", "BEGIN")
	checkNoError("UPDATE in txn", "UPDATE v34_txn SET val = 'changed' WHERE id = 1")
	check("Updated value visible inside txn",
		"SELECT val FROM v34_txn WHERE id = 1", "changed")
	checkNoError("ROLLBACK UPDATE", "ROLLBACK")
	check("Original value restored after rollback",
		"SELECT val FROM v34_txn WHERE id = 1", "stable")

	// Test 31: BEGIN, DELETE then ROLLBACK restores deleted row
	checkNoError("BEGIN for DELETE test", "BEGIN")
	checkNoError("DELETE in txn", "DELETE FROM v34_txn WHERE id = 2")
	checkRowCount("1 row visible inside txn after delete", "SELECT * FROM v34_txn", 1)
	checkNoError("ROLLBACK DELETE", "ROLLBACK")
	checkRowCount("Both rows restored after rollback", "SELECT * FROM v34_txn", 2)

	// Test 32: mixed DML in one COMMIT persists all changes
	checkNoError("BEGIN mixed DML", "BEGIN")
	checkNoError("INSERT mixed", "INSERT INTO v34_txn VALUES (5, 'committed')")
	checkNoError("UPDATE mixed", "UPDATE v34_txn SET val = 'updated' WHERE id = 1")
	checkNoError("DELETE mixed", "DELETE FROM v34_txn WHERE id = 2")
	checkNoError("COMMIT mixed DML", "COMMIT")
	checkRowCount("Mixed DML committed: 2 rows remain", "SELECT * FROM v34_txn", 2)
	check("Committed INSERT present",
		"SELECT val FROM v34_txn WHERE id = 5", "committed")
	check("Committed UPDATE present",
		"SELECT val FROM v34_txn WHERE id = 1", "updated")

	// ============================================================
	// === SECTION 8: LARGE STRING VALUES AND SPECIAL CHARACTERS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v34_large_text (id INTEGER PRIMARY KEY, content TEXT)")

	// Test 33: very long ASCII string (500 chars)
	longStr := strings.Repeat("x", 500)
	checkNoError("Insert 500-char string",
		fmt.Sprintf("INSERT INTO v34_large_text VALUES (1, '%s')", longStr))
	check("500-char string length",
		"SELECT LENGTH(content) FROM v34_large_text WHERE id = 1", 500)

	// Test 34: UTF-8 multibyte characters
	checkNoError("Insert UTF-8 content",
		"INSERT INTO v34_large_text VALUES (2, 'Héllo Wörld - こんにちは - 你好')")
	checkRowCount("UTF-8 row present",
		"SELECT * FROM v34_large_text WHERE id = 2", 1)

	// Test 35: string with SQL-special characters stored via escaped literal
	checkNoError("Insert string with apostrophe",
		"INSERT INTO v34_large_text VALUES (3, 'it''s a test')")
	check("Apostrophe in string retrieved correctly",
		"SELECT content FROM v34_large_text WHERE id = 3", "it's a test")

	// Test 36: string with newline and tab characters
	checkNoError("Insert string with whitespace",
		"INSERT INTO v34_large_text VALUES (4, 'line1\nline2\ttabbed')")
	checkRowCount("Whitespace string row present",
		"SELECT * FROM v34_large_text WHERE id = 4", 1)

	// Test 37: empty string is distinct from NULL
	checkNoError("Insert empty string",
		"INSERT INTO v34_large_text VALUES (5, '')")
	check("Empty string has length 0",
		"SELECT LENGTH(content) FROM v34_large_text WHERE id = 5", 0)
	checkRowCount("Empty string is NOT NULL",
		"SELECT * FROM v34_large_text WHERE content IS NOT NULL AND id = 5", 1)

	// ============================================================
	// === SECTION 9: BOUNDARY NUMBERS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v34_numbers (id INTEGER PRIMARY KEY, ival INTEGER, fval REAL)")

	// Test 38: large positive integer
	checkNoError("Insert large positive integer",
		"INSERT INTO v34_numbers VALUES (1, 9999999999, 0.0)")
	check("Large positive integer retrieved",
		"SELECT ival FROM v34_numbers WHERE id = 1", 9999999999)

	// Test 39: large negative integer
	checkNoError("Insert large negative integer",
		"INSERT INTO v34_numbers VALUES (2, -9999999999, 0.0)")
	check("Large negative integer retrieved",
		"SELECT ival FROM v34_numbers WHERE id = 2", -9999999999)

	// Test 40: zero values
	checkNoError("Insert zero values",
		"INSERT INTO v34_numbers VALUES (3, 0, 0.0)")
	check("Zero integer retrieved",
		"SELECT ival FROM v34_numbers WHERE id = 3", 0)

	// Test 41: float precision edge cases
	checkNoError("Insert float 3.14",
		"INSERT INTO v34_numbers VALUES (4, 0, 3.14)")
	checkRowCount("Float 3.14 round-trips",
		"SELECT * FROM v34_numbers WHERE fval > 3.1 AND fval < 3.2", 1)

	// Test 42: arithmetic on boundary values
	check("Boundary integer arithmetic: 9999999999 + 1",
		"SELECT 9999999999 + 1", 10000000000)

	// Test 43: negative arithmetic
	check("Negative arithmetic: -100 - 50",
		"SELECT -100 - 50", -150)

	// Test 44: integer division - this engine performs float division, yielding 3.5
	check("Integer division: 7 / 2 = 3.5 (engine uses float division)",
		"SELECT 7 / 2", 3.5)

	// ============================================================
	// === SECTION 10: EMPTY TABLE OPERATIONS ===
	// ============================================================

	afExec(t, db, ctx, "CREATE TABLE v34_empty (id INTEGER PRIMARY KEY, val INTEGER)")

	// Test 45: SELECT from empty table returns 0 rows
	checkRowCount("SELECT from empty table returns 0 rows",
		"SELECT * FROM v34_empty", 0)

	// Test 46: COUNT(*) on empty table returns 0
	check("COUNT(*) on empty table is 0",
		"SELECT COUNT(*) FROM v34_empty", 0)

	// Test 47: SUM on empty table returns NULL
	check("SUM on empty table is NULL",
		"SELECT COALESCE(SUM(val), -1) FROM v34_empty", -1)

	// Test 48: AVG on empty table returns NULL
	check("AVG on empty table is NULL",
		"SELECT COALESCE(AVG(val), -1) FROM v34_empty", -1)

	// Test 49: MIN on empty table returns NULL
	check("MIN on empty table is NULL",
		"SELECT COALESCE(MIN(val), -1) FROM v34_empty", -1)

	// Test 50: MAX on empty table returns NULL
	check("MAX on empty table is NULL",
		"SELECT COALESCE(MAX(val), -1) FROM v34_empty", -1)

	// Test 51: DELETE from empty table is a no-op (no error)
	checkNoError("DELETE from empty table is a no-op",
		"DELETE FROM v34_empty")

	// ============================================================
	// === SECTION 11: MULTI-COLUMN INDEX ===
	// ============================================================
	//
	// Table: v34_idx (id PK, last_name TEXT, first_name TEXT, score INTEGER)
	// Create a composite index on (last_name, first_name) and verify
	// queries using those columns return correct results.

	afExec(t, db, ctx, `CREATE TABLE v34_idx (
		id         INTEGER PRIMARY KEY,
		last_name  TEXT,
		first_name TEXT,
		score      INTEGER
	)`)
	checkNoError("Create composite index",
		"CREATE INDEX v34_idx_name ON v34_idx (last_name, first_name)")

	afExec(t, db, ctx, "INSERT INTO v34_idx VALUES (1, 'Smith', 'Alice',   90)")
	afExec(t, db, ctx, "INSERT INTO v34_idx VALUES (2, 'Smith', 'Bob',     85)")
	afExec(t, db, ctx, "INSERT INTO v34_idx VALUES (3, 'Jones', 'Carol',   78)")
	afExec(t, db, ctx, "INSERT INTO v34_idx VALUES (4, 'Jones', 'Dave',    92)")
	afExec(t, db, ctx, "INSERT INTO v34_idx VALUES (5, 'Brown', 'Eve',     88)")

	// Test 52: query on indexed first column (last_name)
	checkRowCount("Query on indexed last_name = Smith",
		"SELECT * FROM v34_idx WHERE last_name = 'Smith'", 2)

	// Test 53: query on both indexed columns
	checkRowCount("Query on both indexed columns",
		"SELECT * FROM v34_idx WHERE last_name = 'Jones' AND first_name = 'Carol'", 1)

	// Test 54: aggregate on indexed table
	check("MAX score among Smiths",
		"SELECT MAX(score) FROM v34_idx WHERE last_name = 'Smith'", 90)

	// Test 55: ORDER BY indexed columns
	check("First Smith alphabetically by first_name",
		"SELECT first_name FROM v34_idx WHERE last_name = 'Smith' ORDER BY first_name ASC LIMIT 1",
		"Alice")

	// ============================================================
	// === SECTION 12: ALTER TABLE EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v34_alter (
		id   INTEGER PRIMARY KEY,
		name TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_alter VALUES (1, 'original')")

	// Test 56: ADD COLUMN with DEFAULT - the engine backfills existing rows with the default.
	checkNoError("ALTER TABLE ADD COLUMN with DEFAULT",
		"ALTER TABLE v34_alter ADD COLUMN status TEXT DEFAULT 'new'")
	// Existing row gets the default value for the newly added column.
	checkRowCount("Existing row has default status after ADD COLUMN",
		"SELECT * FROM v34_alter WHERE id = 1 AND status = 'new'", 1)

	// Test 57: new insert after ADD COLUMN includes new column
	checkNoError("Insert into table after ADD COLUMN",
		"INSERT INTO v34_alter VALUES (2, 'second', 'active')")
	check("New row has explicit status value",
		"SELECT status FROM v34_alter WHERE id = 2", "active")

	// Test 58: ADD COLUMN without DEFAULT (nullable column, default NULL)
	checkNoError("ALTER TABLE ADD COLUMN without DEFAULT",
		"ALTER TABLE v34_alter ADD COLUMN score INTEGER")
	checkRowCount("Existing rows have NULL in new score column",
		"SELECT * FROM v34_alter WHERE score IS NULL", 2)

	// Test 59: RENAME TABLE
	checkNoError("ALTER TABLE RENAME TO",
		"ALTER TABLE v34_alter RENAME TO v34_alter_renamed")
	checkRowCount("Renamed table is queryable",
		"SELECT * FROM v34_alter_renamed", 2)

	// Test 60: RENAME COLUMN
	checkNoError("ALTER TABLE RENAME COLUMN",
		"ALTER TABLE v34_alter_renamed RENAME COLUMN name TO full_name")
	check("Renamed column is queryable",
		"SELECT full_name FROM v34_alter_renamed WHERE id = 1", "original")

	// ============================================================
	// === SECTION 13: INSERT OR REPLACE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v34_upsert (
		id    INTEGER PRIMARY KEY,
		email TEXT UNIQUE,
		score INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_upsert VALUES (1, 'x@ex.com', 10)")
	afExec(t, db, ctx, "INSERT INTO v34_upsert VALUES (2, 'y@ex.com', 20)")

	// Test 61: INSERT OR REPLACE on PK conflict replaces the row
	checkNoError("INSERT OR REPLACE on PK conflict",
		"INSERT OR REPLACE INTO v34_upsert VALUES (1, 'x@ex.com', 99)")
	check("Replaced row has updated score",
		"SELECT score FROM v34_upsert WHERE id = 1", 99)
	checkRowCount("Row count unchanged after REPLACE",
		"SELECT * FROM v34_upsert", 2)

	// Test 62: INSERT OR REPLACE with new PK inserts a new row
	checkNoError("INSERT OR REPLACE with new PK inserts",
		"INSERT OR REPLACE INTO v34_upsert VALUES (3, 'z@ex.com', 30)")
	checkRowCount("New row added by REPLACE on new PK",
		"SELECT * FROM v34_upsert", 3)

	// Test 63: INSERT OR REPLACE on UNIQUE column conflict replaces the row
	checkNoError("INSERT OR REPLACE on UNIQUE email conflict",
		"INSERT OR REPLACE INTO v34_upsert VALUES (4, 'y@ex.com', 55)")
	// The row with email 'y@ex.com' (id=2) must have been replaced by id=4
	checkRowCount("Old row with conflicting UNIQUE gone after REPLACE",
		"SELECT * FROM v34_upsert WHERE email = 'y@ex.com'", 1)
	check("New id has replaced score",
		"SELECT score FROM v34_upsert WHERE email = 'y@ex.com'", 55)

	// ============================================================
	// === SECTION 14: INSERT OR IGNORE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v34_ignore (
		id    INTEGER PRIMARY KEY,
		label TEXT UNIQUE,
		val   INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_ignore VALUES (1, 'alpha', 100)")
	afExec(t, db, ctx, "INSERT INTO v34_ignore VALUES (2, 'beta',  200)")

	// Test 64: INSERT OR IGNORE on PK conflict silently skips
	checkNoError("INSERT OR IGNORE on PK conflict is silent",
		"INSERT OR IGNORE INTO v34_ignore VALUES (1, 'alpha-dup', 999)")
	check("Original row unchanged after IGNORE on PK",
		"SELECT label FROM v34_ignore WHERE id = 1", "alpha")
	checkRowCount("Row count unchanged after IGNORE on PK",
		"SELECT * FROM v34_ignore", 2)

	// Test 65: INSERT OR IGNORE on UNIQUE conflict silently skips
	checkNoError("INSERT OR IGNORE on UNIQUE conflict is silent",
		"INSERT OR IGNORE INTO v34_ignore VALUES (3, 'beta', 888)")
	check("Original UNIQUE row unchanged after IGNORE",
		"SELECT val FROM v34_ignore WHERE label = 'beta'", 200)
	checkRowCount("Row count unchanged after IGNORE on UNIQUE",
		"SELECT * FROM v34_ignore", 2)

	// Test 66: INSERT OR IGNORE with new PK and label inserts normally
	checkNoError("INSERT OR IGNORE with no conflict inserts",
		"INSERT OR IGNORE INTO v34_ignore VALUES (3, 'gamma', 300)")
	checkRowCount("New row present after IGNORE on no conflict",
		"SELECT * FROM v34_ignore", 3)
	check("New IGNORE row value correct",
		"SELECT val FROM v34_ignore WHERE label = 'gamma'", 300)

	// ============================================================
	// === SECTION 15: MULTIPLE ORDER BY COLUMNS ===
	// ============================================================
	//
	// Table: v34_orderby (id PK, dept TEXT, salary INTEGER, name TEXT)
	// Tests multi-column ORDER BY with ties broken by secondary sort.

	afExec(t, db, ctx, `CREATE TABLE v34_orderby (
		id     INTEGER PRIMARY KEY,
		dept   TEXT,
		salary INTEGER,
		name   TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_orderby VALUES (1, 'Eng',   90000, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v34_orderby VALUES (2, 'Eng',   75000, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v34_orderby VALUES (3, 'Eng',   75000, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO v34_orderby VALUES (4, 'Sales', 70000, 'Dave')")
	afExec(t, db, ctx, "INSERT INTO v34_orderby VALUES (5, 'Sales', 60000, 'Eve')")

	// Test 67: ORDER BY dept ASC, salary DESC - first row: Eng dept, highest salary = Alice
	check("ORDER BY dept ASC, salary DESC - first is Alice",
		"SELECT name FROM v34_orderby ORDER BY dept ASC, salary DESC LIMIT 1",
		"Alice")

	// Test 68: ORDER BY dept ASC, salary DESC, name ASC - tie-break Bob vs Carol (same salary 75000)
	// After Alice(90k), then 75k tie: Bob < Carol alphabetically
	check("ORDER BY dept ASC, salary DESC, name ASC - second is Bob",
		`SELECT name FROM v34_orderby
		 ORDER BY dept ASC, salary DESC, name ASC
		 LIMIT 1 OFFSET 1`,
		"Bob")

	// Test 69: ORDER BY salary ASC, name ASC - lowest salary first
	// Eve(60k), then 75k tie Bob before Carol, Dave(70k), Alice(90k)
	check("ORDER BY salary ASC, name ASC - lowest salary first is Eve",
		"SELECT name FROM v34_orderby ORDER BY salary ASC, name ASC LIMIT 1",
		"Eve")

	// Test 70: ORDER BY dept DESC, salary ASC - Sales dept first (DESC), lowest salary first
	// Sales: Eve(60k), Dave(70k); Eng: Bob(75k), Carol(75k), Alice(90k)
	// With dept DESC: Sales comes before Eng; within Sales salary ASC: Eve(60k) first
	check("ORDER BY dept DESC, salary ASC - first is Eve",
		"SELECT name FROM v34_orderby ORDER BY dept DESC, salary ASC LIMIT 1",
		"Eve")

	// ============================================================
	// === SECTION 16: BETWEEN WITH VARIOUS TYPES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v34_between (
		id   INTEGER PRIMARY KEY,
		ival INTEGER,
		sval TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_between VALUES (1, 10, 'apple')")
	afExec(t, db, ctx, "INSERT INTO v34_between VALUES (2, 20, 'banana')")
	afExec(t, db, ctx, "INSERT INTO v34_between VALUES (3, 30, 'cherry')")
	afExec(t, db, ctx, "INSERT INTO v34_between VALUES (4, 40, 'date')")
	afExec(t, db, ctx, "INSERT INTO v34_between VALUES (5, 50, 'elderberry')")

	// Test 71: integer BETWEEN inclusive lower bound
	checkRowCount("INTEGER BETWEEN 10 AND 30 (3 rows)",
		"SELECT * FROM v34_between WHERE ival BETWEEN 10 AND 30", 3)

	// Test 72: integer BETWEEN inclusive upper bound
	checkRowCount("INTEGER BETWEEN 30 AND 50 (3 rows)",
		"SELECT * FROM v34_between WHERE ival BETWEEN 30 AND 50", 3)

	// Test 73: integer BETWEEN with no results
	checkRowCount("INTEGER BETWEEN 15 AND 19 (0 rows)",
		"SELECT * FROM v34_between WHERE ival BETWEEN 15 AND 19", 0)

	// Test 74: string BETWEEN lexicographic (apple <= x <= cherry)
	checkRowCount("STRING BETWEEN 'apple' AND 'cherry' (3 rows)",
		"SELECT * FROM v34_between WHERE sval BETWEEN 'apple' AND 'cherry'", 3)

	// Test 75: NOT BETWEEN
	checkRowCount("INTEGER NOT BETWEEN 20 AND 40 (2 rows: 10, 50)",
		"SELECT * FROM v34_between WHERE ival NOT BETWEEN 20 AND 40", 2)

	// ============================================================
	// === SECTION 17: COMPLEX NESTED WHERE (AND/OR WITH PARENS) ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v34_nested_where (
		id       INTEGER PRIMARY KEY,
		category TEXT,
		priority INTEGER,
		active   INTEGER,
		score    INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_nested_where VALUES (1, 'A', 1, 1, 80)")
	afExec(t, db, ctx, "INSERT INTO v34_nested_where VALUES (2, 'A', 2, 0, 90)")
	afExec(t, db, ctx, "INSERT INTO v34_nested_where VALUES (3, 'B', 1, 1, 70)")
	afExec(t, db, ctx, "INSERT INTO v34_nested_where VALUES (4, 'B', 2, 1, 60)")
	afExec(t, db, ctx, "INSERT INTO v34_nested_where VALUES (5, 'C', 1, 0, 85)")
	afExec(t, db, ctx, "INSERT INTO v34_nested_where VALUES (6, 'C', 2, 1, 95)")

	// Test 76: (A AND B) OR (C AND D) grouping
	// Rows where (category='A' AND active=1) OR (category='C' AND priority=2):
	//   id=1: A,active=1 YES
	//   id=2: A,active=0 NO; A AND active=1? NO. C AND priority=2? NO. -> excluded
	//   id=6: C,priority=2 YES
	// Result: ids 1, 6 -> 2 rows
	checkRowCount("(cat=A AND active=1) OR (cat=C AND priority=2) = 2 rows",
		`SELECT * FROM v34_nested_where
		 WHERE (category = 'A' AND active = 1)
		    OR (category = 'C' AND priority = 2)`,
		2)

	// Test 77: triple nesting
	// (A OR B) AND (priority=1 OR score > 85)
	// A or B: ids 1,2,3,4
	// priority=1 OR score>85: id=1(p=1 YES), id=2(score=90>85 YES), id=3(p=1 YES), id=4(score=60 NO, p=2 NO)
	// Intersection: 1,2,3 -> 3 rows
	checkRowCount("(cat=A OR cat=B) AND (priority=1 OR score>85) = 3 rows",
		`SELECT * FROM v34_nested_where
		 WHERE (category = 'A' OR category = 'B')
		   AND (priority = 1 OR score > 85)`,
		3)

	// Test 78: deep three-level nesting
	// ((A AND priority=1) OR (B AND active=1)) AND score >= 70
	//   (A AND p=1): id=1 (score=80 >=70 YES)
	//   (B AND active=1): id=3 (score=70 >=70 YES), id=4 (score=60 < 70 NO)
	// Result: ids 1, 3 -> 2 rows
	checkRowCount("((A AND p=1) OR (B AND active=1)) AND score>=70 = 2 rows",
		`SELECT * FROM v34_nested_where
		 WHERE ((category = 'A' AND priority = 1) OR (category = 'B' AND active = 1))
		   AND score >= 70`,
		2)

	// ============================================================
	// === SECTION 18: COUNT(*) vs COUNT(column) - NULL HANDLING ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v34_nullcount (
		id  INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_nullcount VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO v34_nullcount VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO v34_nullcount VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO v34_nullcount VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO v34_nullcount VALUES (5, 50)")

	// Test 79: COUNT(*) counts all rows including those with NULLs
	check("COUNT(*) counts all 5 rows including NULLs",
		"SELECT COUNT(*) FROM v34_nullcount", 5)

	// Test 80: COUNT(val) skips NULLs - only 3 non-null values
	check("COUNT(val) skips NULL rows: 3",
		"SELECT COUNT(val) FROM v34_nullcount", 3)

	// Test 81: SUM skips NULLs: 10 + 30 + 50 = 90
	check("SUM(val) skips NULLs: 90",
		"SELECT SUM(val) FROM v34_nullcount", 90)

	// Test 82: AVG skips NULLs: (10+30+50)/3 = 30
	check("AVG(val) skips NULLs: 30",
		"SELECT AVG(val) FROM v34_nullcount", 30)

	// Test 83: COUNT DISTINCT skips NULLs and dedups
	// Non-null distinct values: 10, 30, 50 -> 3
	check("COUNT(DISTINCT val) skips NULLs: 3",
		"SELECT COUNT(DISTINCT val) FROM v34_nullcount", 3)

	// ============================================================
	// === SECTION 19: RE-CREATING TABLES (DROP THEN CREATE) ===
	// ============================================================

	// Test 84: create, populate, drop, re-create with same name - starts fresh
	afExec(t, db, ctx, "CREATE TABLE v34_recreate (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, "INSERT INTO v34_recreate VALUES (1, 'before-drop')")
	afExec(t, db, ctx, "INSERT INTO v34_recreate VALUES (2, 'before-drop-2')")
	checkRowCount("Table has 2 rows before drop",
		"SELECT * FROM v34_recreate", 2)

	checkNoError("DROP TABLE",
		"DROP TABLE v34_recreate")
	checkNoError("CREATE TABLE with same name after drop",
		"CREATE TABLE v34_recreate (id INTEGER PRIMARY KEY, data TEXT, extra INTEGER)")

	// Test 85: after re-creation the table is empty
	checkRowCount("Re-created table is empty",
		"SELECT * FROM v34_recreate", 0)

	// Test 86: old data is gone after DROP + CREATE
	checkRowCount("Old data not present in re-created table",
		"SELECT * FROM v34_recreate WHERE data = 'before-drop'", 0)

	// Test 87: new schema (extra column) is available
	checkNoError("Insert into re-created table with new schema",
		"INSERT INTO v34_recreate VALUES (1, 'after-recreate', 42)")
	check("New schema column present",
		"SELECT extra FROM v34_recreate WHERE id = 1", 42)

	// Test 88: DROP TABLE that does not exist returns an error (or is silently ignored)
	// We verify DROP TABLE IF EXISTS is accepted without error
	checkNoError("DROP TABLE IF EXISTS on missing table is no-op",
		"DROP TABLE IF EXISTS v34_never_existed")

	// ============================================================
	// === SECTION 20: SUBQUERY RETURNING MULTIPLE ROWS IN SCALAR CONTEXT ===
	// ============================================================
	//
	// A scalar subquery (in SELECT or WHERE = context) that returns more than
	// one row should produce an error.  We verify the engine handles it gracefully
	// by returning an error rather than silently producing wrong results.

	afExec(t, db, ctx, `CREATE TABLE v34_scalar_sub (id INTEGER PRIMARY KEY, grp INTEGER, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v34_scalar_sub VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO v34_scalar_sub VALUES (2, 1, 20)")
	afExec(t, db, ctx, "INSERT INTO v34_scalar_sub VALUES (3, 2, 30)")

	// Test 89: scalar subquery returning exactly 1 row succeeds
	check("Scalar subquery returning 1 row is OK",
		"SELECT (SELECT val FROM v34_scalar_sub WHERE grp = 2)", 30)

	// Test 90: subquery in WHERE with IN works when multi-row subquery used
	checkRowCount("IN subquery with multiple rows from same group",
		"SELECT * FROM v34_scalar_sub WHERE val IN (SELECT val FROM v34_scalar_sub WHERE grp = 1)",
		2)

	// Test 91: scalar equality subquery with multiple rows should error or return first row
	// We document this as an error-expected case
	checkError("Scalar subquery with multiple rows in WHERE = context errors",
		"SELECT * FROM v34_scalar_sub WHERE val = (SELECT val FROM v34_scalar_sub WHERE grp = 1)")

	// ============================================================
	// === SECTION 21: ADDITIONAL INTEGRITY VERIFICATIONS ===
	// ============================================================

	// Test 92: UNIQUE constraint across UPDATE - self-update to same UNIQUE value is OK
	afExec(t, db, ctx, "CREATE TABLE v34_selfupdate (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	afExec(t, db, ctx, "INSERT INTO v34_selfupdate VALUES (1, 'AAA')")
	afExec(t, db, ctx, "INSERT INTO v34_selfupdate VALUES (2, 'BBB')")

	checkNoError("UPDATE to own existing UNIQUE value is a no-op OK",
		"UPDATE v34_selfupdate SET code = 'AAA' WHERE id = 1")
	check("Self-update leaves UNIQUE value unchanged",
		"SELECT code FROM v34_selfupdate WHERE id = 1", "AAA")

	// Test 93: DELETE then reuse PK that was freed - must succeed
	afExec(t, db, ctx, "CREATE TABLE v34_pk_reuse (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v34_pk_reuse VALUES (1, 'first')")
	afExec(t, db, ctx, "INSERT INTO v34_pk_reuse VALUES (2, 'second')")
	checkNoError("Delete row to free PK",
		"DELETE FROM v34_pk_reuse WHERE id = 1")
	checkNoError("Re-insert freed manual PK",
		"INSERT INTO v34_pk_reuse VALUES (1, 'reused')")
	check("Re-used PK row present",
		"SELECT val FROM v34_pk_reuse WHERE id = 1", "reused")

	// Test 94: Multiple UNIQUE columns - each enforced independently
	afExec(t, db, ctx, `CREATE TABLE v34_multi_unique (
		id   INTEGER PRIMARY KEY,
		u1   TEXT UNIQUE,
		u2   TEXT UNIQUE,
		data TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v34_multi_unique VALUES (1, 'x', 'p', 'row1')")
	afExec(t, db, ctx, "INSERT INTO v34_multi_unique VALUES (2, 'y', 'q', 'row2')")

	checkError("Duplicate u1 is rejected",
		"INSERT INTO v34_multi_unique VALUES (3, 'x', 'r', 'bad')")
	checkError("Duplicate u2 is rejected",
		"INSERT INTO v34_multi_unique VALUES (4, 'z', 'p', 'bad')")
	checkNoError("Both UNIQUE cols distinct insert OK",
		"INSERT INTO v34_multi_unique VALUES (5, 'z', 'r', 'good')")
	checkRowCount("Multi-UNIQUE table has 3 rows",
		"SELECT * FROM v34_multi_unique", 3)

	// Test 95: Aggregate on GROUP BY with HAVING after join
	afExec(t, db, ctx, "CREATE TABLE v34_grp_agg (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v34_grp_agg VALUES (1, 'X', 10)")
	afExec(t, db, ctx, "INSERT INTO v34_grp_agg VALUES (2, 'X', 20)")
	afExec(t, db, ctx, "INSERT INTO v34_grp_agg VALUES (3, 'Y', 5)")
	afExec(t, db, ctx, "INSERT INTO v34_grp_agg VALUES (4, 'Y', 15)")
	afExec(t, db, ctx, "INSERT INTO v34_grp_agg VALUES (5, 'Z', 100)")

	// Categories with SUM > 20: X(30), Z(100) -> 2 groups; Y(20) is NOT > 20
	checkRowCount("HAVING SUM > 20 returns 2 groups",
		"SELECT category, SUM(amount) FROM v34_grp_agg GROUP BY category HAVING SUM(amount) > 20",
		2)

	check("Top category by SUM amount is Z",
		"SELECT category FROM v34_grp_agg GROUP BY category ORDER BY SUM(amount) DESC LIMIT 1",
		"Z")

	t.Logf("\n=== V34 DATA INTEGRITY EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
