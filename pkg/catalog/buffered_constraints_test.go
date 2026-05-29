package catalog

import "testing"

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
//
// NOTE: this asserts only the non-conflicting case, which succeeds. An in-txn
// UPDATE that sets a UNIQUE column to a value already held by another row in the
// same buffered transaction is NOT rejected at statement time (observed while
// writing this test). That is a possible correctness gap in the buffered UPDATE
// path — see refactor.md §5. It is recorded as a lead rather than asserted as
// correct here, so this test does not lock in that behavior.
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
