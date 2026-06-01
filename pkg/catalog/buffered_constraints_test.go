package catalog

import (
	"fmt"
	"testing"
)

// These tests exercise the buffered/MVCC write path's constraint-checking
// helpers (checkUniqueConstraintsSnapshot, checkForeignKeyConstraintsSnapshot,
// validateInsertRowSnapshot, processUpdateRowDataSnapshot), which were barely
// covered even by the full suite. They strengthen the safety net before any
// decomposition of the insert/update write paths. createCatalogWithTxnManager
// wires a txn manager and enables buffered writes.

func bcExec(t *testing.T, c *Catalog, sql string) *QueryResult {
	t.Helper()
	r, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	return r
}

// UNIQUE conflict against a buffered (read-your-writes) row in the same txn.
func TestBufferedInsertUniqueConflictWithinTxn(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_uniq (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")

	c.BeginTransaction(1)
	bcExec(t, c, "INSERT INTO bu_uniq VALUES (1, 'A')")
	if _, err := c.ExecuteQuery("INSERT INTO bu_uniq VALUES (2, 'A')"); err == nil {
		t.Fatal("expected UNIQUE constraint failure for duplicate code within buffered txn")
	}
	_ = c.RollbackTransaction()
}

// UNIQUE conflict against an already-committed row, checked from a new txn's
// buffered insert path.
func TestBufferedInsertUniqueConflictWithCommitted(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_uniq2 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")

	c.BeginTransaction(1)
	bcExec(t, c, "INSERT INTO bu_uniq2 VALUES (1, 'X')")
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("INSERT INTO bu_uniq2 VALUES (2, 'X')"); err == nil {
		t.Fatal("expected UNIQUE constraint failure against committed row")
	}
	_ = c.RollbackTransaction()

	// The conflicting row must not have been committed.
	r := bcExec(t, c, "SELECT id FROM bu_uniq2")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after rejected duplicate, got %d", len(r.Rows))
	}
}

// Foreign-key validation on the buffered insert path: a valid reference
// succeeds and a dangling reference is rejected.
func TestBufferedInsertForeignKeyValidation(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_par (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE bu_chd (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES bu_par(id))")

	// Commit a parent row outside the buffered txn first.
	bcExec(t, c, "INSERT INTO bu_par VALUES (1)")

	c.BeginTransaction(1)
	if _, err := c.ExecuteQuery("INSERT INTO bu_chd VALUES (10, 1)"); err != nil {
		t.Fatalf("valid FK insert should succeed in buffered mode: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO bu_chd VALUES (11, 999)"); err == nil {
		t.Fatal("expected FK violation for dangling reference in buffered mode")
	}
	_ = c.RollbackTransaction()
}

// Buffered UPDATE drives processUpdateRowDataSnapshot.
func TestBufferedUpdateAppliesAndCommits(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_upd (id INTEGER PRIMARY KEY, val TEXT)")
	bcExec(t, c, "INSERT INTO bu_upd VALUES (1, 'a')")
	bcExec(t, c, "INSERT INTO bu_upd VALUES (2, 'b')")

	c.BeginTransaction(1)
	bcExec(t, c, "UPDATE bu_upd SET val = 'z' WHERE id = 1")
	// read-your-writes inside the txn
	rr := bcExec(t, c, "SELECT val FROM bu_upd WHERE id = 1")
	if len(rr.Rows) != 1 || toStr(rr.Rows[0][0]) != "z" {
		t.Fatalf("expected read-your-writes val 'z' inside txn, got %v", rr.Rows)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	after := bcExec(t, c, "SELECT val FROM bu_upd WHERE id = 1")
	if len(after.Rows) != 1 || toStr(after.Rows[0][0]) != "z" {
		t.Fatalf("expected val 'z' after committed buffered update, got %v", after.Rows)
	}
}

// Buffered UPDATE rolled back must not change committed state.
func TestBufferedUpdateRollbackDiscards(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_upd2 (id INTEGER PRIMARY KEY, val TEXT)")
	bcExec(t, c, "INSERT INTO bu_upd2 VALUES (1, 'keep')")

	c.BeginTransaction(1)
	bcExec(t, c, "UPDATE bu_upd2 SET val = 'changed' WHERE id = 1")
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	after := bcExec(t, c, "SELECT val FROM bu_upd2 WHERE id = 1")
	if len(after.Rows) != 1 || toStr(after.Rows[0][0]) != "keep" {
		t.Fatalf("expected val unchanged after rollback, got %v", after.Rows)
	}
}

// Buffered UPDATE that changes a UNIQUE column drives the constraint-recheck
// branches of processUpdateRowDataSnapshot.
func TestBufferedUpdateUniqueColumnNonConflicting(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_uupd (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	bcExec(t, c, "INSERT INTO bu_uupd VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO bu_uupd VALUES (2, 'B')")

	c.BeginTransaction(1)
	bcExec(t, c, "UPDATE bu_uupd SET code = 'C' WHERE id = 1")
	rr := bcExec(t, c, "SELECT code FROM bu_uupd WHERE id = 1")
	if len(rr.Rows) != 1 || toStr(rr.Rows[0][0]) != "C" {
		t.Fatalf("expected code 'C' after update, got %v", rr.Rows)
	}
	_ = c.RollbackTransaction()
}

func toStr(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// countWithCode returns how many result rows have column index 1 equal to want.
func countWithCode(r *QueryResult, want string) int {
	n := 0
	for _, row := range r.Rows {
		if len(row) > 1 && fmtV(row[1]) == want {
			n++
		}
	}
	return n
}

func fmtV(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

// Regression for the buffered UPDATE UNIQUE gap (refactor.md §1.8): two rows
// driven to the same unique value in two statements of one txn must be rejected
// at statement time, not silently committed as a duplicate.
func TestBufferedUpdateUniqueConflictCrossStatement(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_x (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	bcExec(t, c, "INSERT INTO bu_x VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO bu_x VALUES (2, 'B')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE bu_x SET code = 'C' WHERE id = 1"); err != nil {
		t.Fatalf("first update should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("UPDATE bu_x SET code = 'C' WHERE id = 2"); err == nil {
		t.Fatal("expected UNIQUE failure for second row taking the same in-txn value")
	}
	_ = c.RollbackTransaction()

	r := bcExec(t, c, "SELECT id, code FROM bu_x ORDER BY id")
	if got := countWithCode(r, "C"); got != 0 {
		t.Fatalf("rolled-back txn must leave no 'C' rows, got %d", got)
	}
}

// A single UPDATE statement that drives multiple rows to the same unique value
// must be rejected (both buffered and autocommit paths).
func TestUpdateUniqueConflictWithinStatement(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		c, _ := createCatalogWithTxnManager(t)
		bcExec(t, c, "CREATE TABLE bu_w (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
		bcExec(t, c, "INSERT INTO bu_w VALUES (1, 'A')")
		bcExec(t, c, "INSERT INTO bu_w VALUES (2, 'B')")

		if buffered {
			c.BeginTransaction(2)
		}
		if _, err := c.ExecuteQuery("UPDATE bu_w SET code = 'C'"); err == nil {
			t.Fatalf("buffered=%v: expected UNIQUE failure setting all rows to one value", buffered)
		}
		if buffered {
			_ = c.RollbackTransaction()
		}

		r := bcExec(t, c, "SELECT id, code FROM bu_w ORDER BY id")
		if got := countWithCode(r, "C"); got != 0 {
			t.Fatalf("buffered=%v: rejected UPDATE must leave no 'C' rows, got %d", buffered, got)
		}
	}
}

// Read-your-writes FK on DELETE: a child inserted earlier in the same txn must
// block deleting its parent, otherwise commit leaves a dangling foreign key.
func TestBufferedDeleteParentWithPendingChildRejected(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_p(id))")
	bcExec(t, c, "INSERT INTO fk_p VALUES (1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("INSERT INTO fk_c VALUES (10, 1)"); err != nil {
		t.Fatalf("child insert should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("DELETE FROM fk_p WHERE id = 1"); err == nil {
		t.Fatal("expected FK violation deleting a parent referenced by a pending child")
	}
	_ = c.RollbackTransaction()

	if r := bcExec(t, c, "SELECT id FROM fk_p"); len(r.Rows) != 1 {
		t.Fatalf("parent must survive the rejected delete, got %d rows", len(r.Rows))
	}
}

// Legitimate: if the referencing child is also removed in the same txn, deleting
// the parent must succeed (the pending delete supersedes the committed child).
func TestBufferedDeleteParentAfterChildDeletedAllowed(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_p2 (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_c2 (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_p2(id))")
	bcExec(t, c, "INSERT INTO fk_p2 VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_c2 VALUES (10, 1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_c2 WHERE id = 10"); err != nil {
		t.Fatalf("child delete should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("DELETE FROM fk_p2 WHERE id = 1"); err != nil {
		t.Fatalf("deleting parent after its only child was removed in-txn should succeed: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if r := bcExec(t, c, "SELECT id FROM fk_p2"); len(r.Rows) != 0 {
		t.Fatalf("parent should be deleted, got %d rows", len(r.Rows))
	}
}

// A legitimate value hand-off (one row vacates a value, another reuses it) within
// the same transaction must still succeed via read-your-writes.
func TestBufferedUpdateUniqueValueHandoff(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_h (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	bcExec(t, c, "INSERT INTO bu_h VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO bu_h VALUES (2, 'B')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE bu_h SET code = 'X' WHERE id = 1"); err != nil {
		t.Fatalf("vacating update should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("UPDATE bu_h SET code = 'A' WHERE id = 2"); err != nil {
		t.Fatalf("reusing the vacated value should succeed: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT id, code FROM bu_h ORDER BY id")
	if len(r.Rows) != 2 || fmtV(r.Rows[0][1]) != "X" || fmtV(r.Rows[1][1]) != "A" {
		t.Fatalf("expected rows [1=X, 2=A] after hand-off, got %v", r.Rows)
	}
}
