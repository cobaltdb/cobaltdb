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

// Read-your-writes PK: deleting a row then re-inserting the same PK in one txn
// must succeed (the pending delete frees the key), and genuine duplicates must
// still be rejected.
func TestBufferedDeleteThenReinsertSamePK(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE pk_re (id INTEGER PRIMARY KEY, v TEXT)")
	bcExec(t, c, "INSERT INTO pk_re VALUES (1, 'old')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM pk_re WHERE id = 1"); err != nil {
		t.Fatalf("delete should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO pk_re VALUES (1, 'new')"); err != nil {
		t.Fatalf("re-inserting a PK freed by a pending delete should succeed: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT id, v FROM pk_re ORDER BY id")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][1]) != "new" {
		t.Fatalf("expected exactly one row [1 new] after delete+reinsert, got %v", r.Rows)
	}
}

func TestBufferedDoubleInsertSamePKRejected(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE pk_dup (id INTEGER PRIMARY KEY, v TEXT)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("INSERT INTO pk_dup VALUES (1, 'a')"); err != nil {
		t.Fatalf("first insert should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO pk_dup VALUES (1, 'b')"); err == nil {
		t.Fatal("expected duplicate PK rejection for a live pending row")
	}
	_ = c.RollbackTransaction()
}

// A UNIQUE INDEX slot (single- and multi-column) freed by a pending delete in
// the same txn can be reused, while genuine duplicates are still rejected.
func TestBufferedUniqueIndexValueFreedByDelete(t *testing.T) {
	for _, idx := range []string{
		"CREATE UNIQUE INDEX uix_s ON uix(code)",
		"CREATE UNIQUE INDEX uix_c ON uix(code, grp)",
	} {
		c, _ := createCatalogWithTxnManager(t)
		bcExec(t, c, "CREATE TABLE uix (id INTEGER PRIMARY KEY, code TEXT, grp TEXT)")
		bcExec(t, c, idx)
		bcExec(t, c, "INSERT INTO uix VALUES (1, 'A', 'g')")

		c.BeginTransaction(2)
		if _, err := c.ExecuteQuery("DELETE FROM uix WHERE id = 1"); err != nil {
			t.Fatalf("%s: delete should succeed: %v", idx, err)
		}
		if _, err := c.ExecuteQuery("INSERT INTO uix VALUES (2, 'A', 'g')"); err != nil {
			t.Fatalf("%s: reusing an index value freed by a pending delete should succeed: %v", idx, err)
		}
		if err := c.CommitTransaction(); err != nil {
			t.Fatalf("%s: commit: %v", idx, err)
		}
		if r := bcExec(t, c, "SELECT id FROM uix"); len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "2" {
			t.Fatalf("%s: expected one row id=2, got %v", idx, r.Rows)
		}

		// A live in-txn duplicate is still rejected.
		c.BeginTransaction(3)
		if _, err := c.ExecuteQuery("INSERT INTO uix VALUES (3, 'A', 'g')"); err == nil {
			t.Fatalf("%s: expected duplicate rejection against the live row", idx)
		}
		_ = c.RollbackTransaction()
	}
}

// A UNIQUE value freed by a pending delete in the same txn can be reused.
func TestBufferedUniqueValueFreedByDelete(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE uq_free (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	bcExec(t, c, "INSERT INTO uq_free VALUES (1, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM uq_free WHERE id = 1"); err != nil {
		t.Fatalf("delete should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO uq_free VALUES (2, 'A')"); err != nil {
		t.Fatalf("reusing a UNIQUE value freed by a pending delete should succeed: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT id, code FROM uq_free ORDER BY id")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "2" || fmtV(r.Rows[0][1]) != "A" {
		t.Fatalf("expected exactly one row [2 A], got %v", r.Rows)
	}
}

// Inserting a child referencing a parent deleted earlier in the same txn must be
// rejected, otherwise commit leaves a dangling foreign key.
func TestBufferedInsertChildAfterParentDeletedRejected(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE pdp (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE pdc (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES pdp(id))")
	bcExec(t, c, "INSERT INTO pdp VALUES (1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM pdp WHERE id = 1"); err != nil {
		t.Fatalf("parent delete should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO pdc VALUES (10, 1)"); err == nil {
		t.Fatal("expected FK violation inserting a child for a parent deleted in this txn")
	}
	_ = c.RollbackTransaction()
}

func TestBufferedInsertChildAfterNonPrimaryParentReferenceDeletedRejected(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE npdp (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE npdc (id INTEGER PRIMARY KEY, parent_code TEXT, FOREIGN KEY (parent_code) REFERENCES npdp(code))")
	bcExec(t, c, "INSERT INTO npdp VALUES (1, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM npdp WHERE id = 1"); err != nil {
		t.Fatalf("parent delete should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO npdc VALUES (10, 'A')"); err == nil {
		t.Fatal("expected FK violation inserting a child for a non-primary parent value deleted in this txn")
	}
	_ = c.RollbackTransaction()
}

func TestBufferedInsertChildSeesPendingParentPrimaryKeyUpdate(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE upp (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE upc (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES upp(id))")
	bcExec(t, c, "INSERT INTO upp VALUES (1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE upp SET id = 2 WHERE id = 1"); err != nil {
		t.Fatalf("parent update should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upc VALUES (10, 1)"); err == nil {
		t.Fatal("expected FK violation for old parent PK value after pending update")
	}
	if _, err := c.ExecuteQuery("INSERT INTO upc VALUES (11, 2)"); err != nil {
		t.Fatalf("expected FK insert to see pending parent PK value: %v", err)
	}
	_ = c.RollbackTransaction()
}

func TestBufferedInsertChildSeesPendingParentNonPrimaryReferenceUpdate(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE npupp (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE npupc (id INTEGER PRIMARY KEY, parent_code TEXT, FOREIGN KEY (parent_code) REFERENCES npupp(code))")
	bcExec(t, c, "INSERT INTO npupp VALUES (1, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE npupp SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("parent update should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO npupc VALUES (10, 'A')"); err == nil {
		t.Fatal("expected FK violation for old non-primary parent value after pending update")
	}
	if _, err := c.ExecuteQuery("INSERT INTO npupc VALUES (11, 'B')"); err != nil {
		t.Fatalf("expected FK insert to see pending non-primary parent value: %v", err)
	}
	_ = c.RollbackTransaction()
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

func TestBufferedCascadeDeleteRemovesPendingChild(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_cascade_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_cascade_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_cascade_p(id) ON DELETE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_cascade_p VALUES (1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("INSERT INTO fk_cascade_c VALUES (10, 1)"); err != nil {
		t.Fatalf("pending child insert should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("DELETE FROM fk_cascade_p WHERE id = 1"); err != nil {
		t.Fatalf("parent delete should cascade to pending child: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if r := bcExec(t, c, "SELECT id FROM fk_cascade_p"); len(r.Rows) != 0 {
		t.Fatalf("parent should be deleted, got %d rows", len(r.Rows))
	}
	if r := bcExec(t, c, "SELECT id FROM fk_cascade_c"); len(r.Rows) != 0 {
		t.Fatalf("pending child should be cascaded away, got %d rows", len(r.Rows))
	}
}

func TestBufferedSetNullUpdatesPendingChild(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_setnull_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_setnull_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_setnull_p(id) ON DELETE SET NULL)")
	bcExec(t, c, "INSERT INTO fk_setnull_p VALUES (1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("INSERT INTO fk_setnull_c VALUES (10, 1)"); err != nil {
		t.Fatalf("pending child insert should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("DELETE FROM fk_setnull_p WHERE id = 1"); err != nil {
		t.Fatalf("parent delete should set pending child FK to NULL: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT pid FROM fk_setnull_c WHERE id = 10")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("pending child FK should be NULL after SET NULL, got %v", r.Rows)
	}
}

func TestBufferedCascadeDeleteRemovesCommittedChild(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_del_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_del_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_buf_del_p(id) ON DELETE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_del_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_del_c VALUES (10, 1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_del_p WHERE id = 1"); err != nil {
		t.Fatalf("buffered parent delete should cascade to committed child: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if r := bcExec(t, c, "SELECT id FROM fk_buf_del_c"); len(r.Rows) != 0 {
		t.Fatalf("committed child should be cascaded away, got %v", r.Rows)
	}
}

func TestBufferedSetNullDeleteUpdatesCommittedChild(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_del_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_del_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_buf_setnull_del_p(id) ON DELETE SET NULL)")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_del_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_del_c VALUES (10, 1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_setnull_del_p WHERE id = 1"); err != nil {
		t.Fatalf("buffered parent delete should set committed child FK to NULL: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT pid FROM fk_buf_setnull_del_c WHERE id = 10")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("committed child FK should be NULL after buffered SET NULL delete, got %v", r.Rows)
	}
}

func TestBufferedForeignKeyActionsRollbackWithTransaction(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_rb_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_rb_c (id INTEGER PRIMARY KEY, pcode TEXT UNIQUE, FOREIGN KEY (pcode) REFERENCES fk_buf_rb_p(code) ON UPDATE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_rb_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_rb_c VALUES (10, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_rb_p SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("buffered cascade update should succeed: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	r := bcExec(t, c, "SELECT pcode FROM fk_buf_rb_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "A" {
		t.Fatalf("rollback should discard buffered child cascade update, got %v", r.Rows)
	}
	if _, err := c.ExecuteQuery("INSERT INTO fk_buf_rb_c VALUES (11, 'A')"); err == nil {
		t.Fatal("rollback should leave committed child UNIQUE index entry for A")
	}
}

func TestBufferedForeignKeyActionsRollbackToSavepoint(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_sp_upd_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_sp_upd_c (id INTEGER PRIMARY KEY, pcode TEXT UNIQUE, FOREIGN KEY (pcode) REFERENCES fk_buf_sp_upd_p(code) ON UPDATE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_sp_upd_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_sp_upd_c VALUES (10, 'A')")

	c.BeginTransaction(2)
	if err := c.Savepoint("fk_upd"); err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	if _, err := c.ExecuteQuery("UPDATE fk_buf_sp_upd_p SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("buffered cascade update should succeed: %v", err)
	}
	if err := c.RollbackToSavepoint("fk_upd"); err != nil {
		t.Fatalf("rollback to savepoint: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	r := bcExec(t, c, "SELECT pcode FROM fk_buf_sp_upd_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "A" {
		t.Fatalf("savepoint rollback should discard buffered child cascade update, got %v", r.Rows)
	}
	if _, err := c.ExecuteQuery("INSERT INTO fk_buf_sp_upd_c VALUES (11, 'A')"); err == nil {
		t.Fatal("savepoint rollback should leave committed child UNIQUE index entry for A")
	}

	bcExec(t, c, "CREATE TABLE fk_buf_sp_del_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_sp_del_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_buf_sp_del_p(id) ON DELETE SET NULL)")
	bcExec(t, c, "INSERT INTO fk_buf_sp_del_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_sp_del_c VALUES (10, 1)")

	c.BeginTransaction(3)
	if err := c.Savepoint("fk_del"); err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_sp_del_p WHERE id = 1"); err != nil {
		t.Fatalf("buffered SET NULL delete should succeed: %v", err)
	}
	if err := c.RollbackToSavepoint("fk_del"); err != nil {
		t.Fatalf("rollback to savepoint: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	r = bcExec(t, c, "SELECT pid FROM fk_buf_sp_del_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "1" {
		t.Fatalf("savepoint rollback should discard buffered SET NULL child action, got %v", r.Rows)
	}
}

func TestBufferedCascadeUpdateUniqueFailureRollsBackStatement(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_unique_stmt_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_unique_stmt_c (id INTEGER PRIMARY KEY, pcode TEXT UNIQUE, FOREIGN KEY (pcode) REFERENCES fk_buf_unique_stmt_p(code) ON UPDATE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_unique_stmt_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_unique_stmt_p VALUES (2, 'B')")
	bcExec(t, c, "INSERT INTO fk_buf_unique_stmt_c VALUES (10, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_unique_stmt_c VALUES (20, 'B')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_unique_stmt_p SET code = 'Z'"); err == nil {
		t.Fatal("buffered multi-row cascade update bypassed child UNIQUE constraint")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected statement: %v", err)
	}

	r := bcExec(t, c, "SELECT code FROM fk_buf_unique_stmt_p ORDER BY id")
	if len(r.Rows) != 2 || fmtV(r.Rows[0][0]) != "A" || fmtV(r.Rows[1][0]) != "B" {
		t.Fatalf("rejected buffered cascade should leave parent rows unchanged, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT pcode FROM fk_buf_unique_stmt_c ORDER BY id")
	if len(r.Rows) != 2 || fmtV(r.Rows[0][0]) != "A" || fmtV(r.Rows[1][0]) != "B" {
		t.Fatalf("rejected buffered cascade should leave child rows unchanged, got %v", r.Rows)
	}
	if r = bcExec(t, c, "SELECT id FROM fk_buf_unique_stmt_c WHERE pcode = 'Z'"); len(r.Rows) != 0 {
		t.Fatalf("rejected buffered cascade should leave no child rows with pcode Z, got %v", r.Rows)
	}
}

func TestBufferedCascadeDeleteHonorsDownstreamRestrict(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_restrict_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_restrict_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_buf_restrict_p(id) ON DELETE CASCADE)")
	bcExec(t, c, "CREATE TABLE fk_buf_restrict_g (id INTEGER PRIMARY KEY, cid INTEGER, FOREIGN KEY (cid) REFERENCES fk_buf_restrict_c(id) ON DELETE RESTRICT)")
	bcExec(t, c, "INSERT INTO fk_buf_restrict_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_restrict_c VALUES (10, 1)")
	bcExec(t, c, "INSERT INTO fk_buf_restrict_g VALUES (100, 10)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_restrict_p WHERE id = 1"); err == nil {
		t.Fatal("buffered cascade delete bypassed downstream RESTRICT constraint")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected statement: %v", err)
	}

	if r := bcExec(t, c, "SELECT id FROM fk_buf_restrict_p"); len(r.Rows) != 1 {
		t.Fatalf("rejected buffered cascade should leave parent row, got %v", r.Rows)
	}
	if r := bcExec(t, c, "SELECT id FROM fk_buf_restrict_c"); len(r.Rows) != 1 {
		t.Fatalf("rejected buffered cascade should leave child row, got %v", r.Rows)
	}
	if r := bcExec(t, c, "SELECT id FROM fk_buf_restrict_g"); len(r.Rows) != 1 {
		t.Fatalf("rejected buffered cascade should leave grandchild row, got %v", r.Rows)
	}
}

func TestBufferedSelfReferentialCascadeDeleteTerminates(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_self_cycle (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_buf_self_cycle(id) ON DELETE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_self_cycle VALUES (1, NULL)")
	bcExec(t, c, "UPDATE fk_buf_self_cycle SET parent_id = 1 WHERE id = 1")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_self_cycle WHERE id = 1"); err != nil {
		t.Fatalf("buffered self-referential cascade delete should terminate: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if r := bcExec(t, c, "SELECT id FROM fk_buf_self_cycle"); len(r.Rows) != 0 {
		t.Fatalf("self-referential cascade should delete the row, got %v", r.Rows)
	}
}

func TestBufferedCyclicCascadeDeleteTerminates(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_cycle (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_buf_cycle(id) ON DELETE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_cycle VALUES (1, NULL)")
	bcExec(t, c, "INSERT INTO fk_buf_cycle VALUES (2, 1)")
	bcExec(t, c, "UPDATE fk_buf_cycle SET parent_id = 2 WHERE id = 1")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_cycle WHERE id = 1"); err != nil {
		t.Fatalf("buffered cyclic cascade delete should terminate: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if r := bcExec(t, c, "SELECT id FROM fk_buf_cycle"); len(r.Rows) != 0 {
		t.Fatalf("cyclic cascade should delete both rows, got %v", r.Rows)
	}
}

func TestBufferedSelfReferentialCascadeUpdateUpdatesLocalFK(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_self_update (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_buf_self_update(id) ON UPDATE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_self_update VALUES (1, NULL)")
	bcExec(t, c, "UPDATE fk_buf_self_update SET parent_id = 1 WHERE id = 1")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_self_update SET id = 2 WHERE id = 1"); err != nil {
		t.Fatalf("buffered self-referential cascade update should succeed: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	r := bcExec(t, c, "SELECT parent_id FROM fk_buf_self_update WHERE id = 2")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "2" {
		t.Fatalf("buffered self-referential cascade should update local FK to 2, got %v", r.Rows)
	}
}

func TestBufferedSelfReferentialCascadeUpdateSameColumnTerminates(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_self_same_col (id INTEGER PRIMARY KEY, code TEXT UNIQUE, FOREIGN KEY (code) REFERENCES fk_buf_self_same_col(code) ON UPDATE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_self_same_col VALUES (1, NULL)")
	bcExec(t, c, "UPDATE fk_buf_self_same_col SET code = 'A' WHERE id = 1")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_self_same_col SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("buffered same-column self-referential cascade update should terminate: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	r := bcExec(t, c, "SELECT code FROM fk_buf_self_same_col WHERE id = 1")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "B" {
		t.Fatalf("buffered same-column self-referential cascade should update code to B, got %v", r.Rows)
	}
}

func TestBufferedCascadeUpdateUpdatesCommittedChild(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_upd_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_upd_c (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES fk_buf_upd_p(code) ON UPDATE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_upd_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_upd_c VALUES (10, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_upd_p SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("buffered parent update should cascade: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT pcode FROM fk_buf_upd_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "B" {
		t.Fatalf("committed child FK should cascade to B, got %v", r.Rows)
	}
}

func TestBufferedCascadeUpdatePropagatesThroughChain(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_chain_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_chain_c (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES fk_buf_chain_p(code) ON UPDATE CASCADE)")
	bcExec(t, c, "CREATE TABLE fk_buf_chain_g (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES fk_buf_chain_c(pcode) ON UPDATE CASCADE)")
	bcExec(t, c, "INSERT INTO fk_buf_chain_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_chain_c VALUES (10, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_chain_g VALUES (100, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_chain_p SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("buffered parent update should cascade through chain: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT pcode FROM fk_buf_chain_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "B" {
		t.Fatalf("buffered child FK should cascade to B, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT ccode FROM fk_buf_chain_g WHERE id = 100")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "B" {
		t.Fatalf("buffered grandchild FK should cascade through child to B, got %v", r.Rows)
	}
}

func TestBufferedSetNullUpdatePropagatesThroughChain(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_chain_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_chain_c (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES fk_buf_setnull_chain_p(code) ON UPDATE SET NULL)")
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_chain_g (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES fk_buf_setnull_chain_c(pcode) ON UPDATE SET NULL)")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_chain_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_chain_c VALUES (10, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_chain_g VALUES (100, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_setnull_chain_p SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("buffered parent update should propagate SET NULL through chain: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT pcode FROM fk_buf_setnull_chain_c WHERE id = 10")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("buffered child FK should become NULL, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT ccode FROM fk_buf_setnull_chain_g WHERE id = 100")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("buffered grandchild FK should become NULL through child SET NULL, got %v", r.Rows)
	}
}

func TestBufferedSetNullDeletePropagatesThroughChain(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_del_chain_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_del_chain_c (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES fk_buf_setnull_del_chain_p(code) ON DELETE SET NULL)")
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_del_chain_g (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES fk_buf_setnull_del_chain_c(pcode) ON UPDATE SET NULL)")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_del_chain_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_del_chain_c VALUES (10, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_del_chain_g VALUES (100, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_setnull_del_chain_p WHERE id = 1"); err != nil {
		t.Fatalf("buffered parent delete should propagate SET NULL through chain: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT pcode FROM fk_buf_setnull_del_chain_c WHERE id = 10")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("buffered child FK should become NULL after parent delete, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT ccode FROM fk_buf_setnull_del_chain_g WHERE id = 100")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("buffered grandchild FK should become NULL through child SET NULL delete, got %v", r.Rows)
	}
}

func TestBufferedCascadeUpdateChainRestrictRollsBackStatement(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_restrict_chain_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_restrict_chain_c (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES fk_buf_restrict_chain_p(code) ON UPDATE CASCADE)")
	bcExec(t, c, "CREATE TABLE fk_buf_restrict_chain_g (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES fk_buf_restrict_chain_c(pcode) ON UPDATE RESTRICT)")
	bcExec(t, c, "INSERT INTO fk_buf_restrict_chain_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_restrict_chain_c VALUES (10, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_restrict_chain_g VALUES (100, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_restrict_chain_p SET code = 'B' WHERE id = 1"); err == nil {
		t.Fatal("buffered chained ON UPDATE CASCADE bypassed downstream RESTRICT")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected statement: %v", err)
	}

	r := bcExec(t, c, "SELECT code FROM fk_buf_restrict_chain_p WHERE id = 1")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "A" {
		t.Fatalf("buffered parent update should roll back after downstream RESTRICT, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT pcode FROM fk_buf_restrict_chain_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "A" {
		t.Fatalf("buffered child cascade should roll back after downstream RESTRICT, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT ccode FROM fk_buf_restrict_chain_g WHERE id = 100")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "A" {
		t.Fatalf("buffered grandchild should remain unchanged after downstream RESTRICT, got %v", r.Rows)
	}
}

func TestBufferedSetNullDeleteChainRestrictRollsBackStatement(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_del_restrict_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_del_restrict_c (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES fk_buf_setnull_del_restrict_p(code) ON DELETE SET NULL)")
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_del_restrict_g (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES fk_buf_setnull_del_restrict_c(pcode) ON UPDATE RESTRICT)")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_del_restrict_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_del_restrict_c VALUES (10, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_del_restrict_g VALUES (100, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_setnull_del_restrict_p WHERE id = 1"); err == nil {
		t.Fatal("buffered chained ON DELETE SET NULL bypassed downstream RESTRICT")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected statement: %v", err)
	}

	r := bcExec(t, c, "SELECT code FROM fk_buf_setnull_del_restrict_p WHERE id = 1")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "A" {
		t.Fatalf("buffered parent delete should roll back after downstream RESTRICT, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT pcode FROM fk_buf_setnull_del_restrict_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "A" {
		t.Fatalf("buffered child SET NULL should roll back after downstream RESTRICT, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT ccode FROM fk_buf_setnull_del_restrict_g WHERE id = 100")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "A" {
		t.Fatalf("buffered grandchild should remain unchanged after downstream RESTRICT, got %v", r.Rows)
	}
}

func TestBufferedSetNullUpdateUpdatesCommittedChild(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_upd_p (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE TABLE fk_buf_setnull_upd_c (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES fk_buf_setnull_upd_p(code) ON UPDATE SET NULL)")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_upd_p VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO fk_buf_setnull_upd_c VALUES (10, 'A')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_setnull_upd_p SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("buffered parent update should set child FK to NULL: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT pcode FROM fk_buf_setnull_upd_c WHERE id = 10")
	if len(r.Rows) != 1 || r.Rows[0][0] != nil {
		t.Fatalf("committed child FK should be NULL after buffered SET NULL, got %v", r.Rows)
	}
}

func TestBufferedSetDefaultActionsUpdateCommittedChild(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_c (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 0, FOREIGN KEY (pid) REFERENCES fk_buf_setdefault_p(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_p VALUES (0)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_p VALUES (2)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_c VALUES (10, 1)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_c VALUES (20, 2)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_setdefault_p WHERE id = 1"); err != nil {
		t.Fatalf("buffered ON DELETE SET DEFAULT should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("UPDATE fk_buf_setdefault_p SET id = 3 WHERE id = 2"); err != nil {
		t.Fatalf("buffered ON UPDATE SET DEFAULT should succeed: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT pid FROM fk_buf_setdefault_c ORDER BY id")
	if len(r.Rows) != 2 || fmtV(r.Rows[0][0]) != "0" || fmtV(r.Rows[1][0]) != "0" {
		t.Fatalf("buffered SET DEFAULT should set child FKs to 0, got %v", r.Rows)
	}
}

func TestBufferedCompositeSetDefaultActionsUpdateCommittedChild(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_comp_setdefault_p (id INTEGER PRIMARY KEY, tenant_id INTEGER, code INTEGER)")
	bcExec(t, c, "CREATE TABLE fk_buf_comp_setdefault_c (id INTEGER PRIMARY KEY, tenant_id INTEGER DEFAULT 0, code INTEGER DEFAULT 0, FOREIGN KEY (tenant_id, code) REFERENCES fk_buf_comp_setdefault_p(tenant_id, code) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_p VALUES (1, 0, 0)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_p VALUES (2, 1, 10)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_p VALUES (3, 2, 20)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_c VALUES (10, 1, 10)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_c VALUES (20, 2, 20)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_comp_setdefault_p WHERE id = 2"); err != nil {
		t.Fatalf("buffered composite ON DELETE SET DEFAULT should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("UPDATE fk_buf_comp_setdefault_p SET code = 21 WHERE id = 3"); err != nil {
		t.Fatalf("buffered composite ON UPDATE SET DEFAULT should succeed: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := bcExec(t, c, "SELECT tenant_id, code FROM fk_buf_comp_setdefault_c ORDER BY id")
	if len(r.Rows) != 2 ||
		fmtV(r.Rows[0][0]) != "0" || fmtV(r.Rows[0][1]) != "0" ||
		fmtV(r.Rows[1][0]) != "0" || fmtV(r.Rows[1][1]) != "0" {
		t.Fatalf("buffered composite SET DEFAULT should set child FK tuples to 0:0, got %v", r.Rows)
	}
}

func TestBufferedSetDefaultInvalidDefaultRollsBackStatement(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_bad_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_bad_c (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 99, FOREIGN KEY (pid) REFERENCES fk_buf_setdefault_bad_p(id) ON DELETE SET DEFAULT)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_bad_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_bad_c VALUES (10, 1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_setdefault_bad_p WHERE id = 1"); err == nil {
		t.Fatal("buffered ON DELETE SET DEFAULT accepted default value with no referenced parent")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected statement: %v", err)
	}

	r := bcExec(t, c, "SELECT id FROM fk_buf_setdefault_bad_p WHERE id = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("buffered parent delete should roll back after invalid SET DEFAULT, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT pid FROM fk_buf_setdefault_bad_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "1" {
		t.Fatalf("buffered child FK should remain unchanged after invalid SET DEFAULT, got %v", r.Rows)
	}
}

func TestBufferedCompositeSetDefaultRejectsDanglingDefaults(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_comp_setdefault_bad_p (id INTEGER PRIMARY KEY, tenant_id INTEGER, code INTEGER)")
	bcExec(t, c, "CREATE TABLE fk_buf_comp_setdefault_bad_c (id INTEGER PRIMARY KEY, tenant_id INTEGER DEFAULT 9, code INTEGER DEFAULT 9, FOREIGN KEY (tenant_id, code) REFERENCES fk_buf_comp_setdefault_bad_p(tenant_id, code) ON DELETE SET DEFAULT)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_bad_p VALUES (1, 1, 10)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_bad_c VALUES (10, 1, 10)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_comp_setdefault_bad_p WHERE id = 1"); err == nil {
		t.Fatal("buffered composite SET DEFAULT accepted default tuple with no referenced parent")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected statement: %v", err)
	}
	r := bcExec(t, c, "SELECT tenant_id, code FROM fk_buf_comp_setdefault_bad_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "1" || fmtV(r.Rows[0][1]) != "10" {
		t.Fatalf("buffered composite child FK should remain unchanged after invalid SET DEFAULT, got %v", r.Rows)
	}

	bcExec(t, c, "CREATE TABLE fk_buf_comp_setdefault_multi_p (id INTEGER PRIMARY KEY, tenant_id INTEGER, code INTEGER)")
	bcExec(t, c, "CREATE TABLE fk_buf_comp_setdefault_multi_c (id INTEGER PRIMARY KEY, tenant_id INTEGER DEFAULT 0, code INTEGER DEFAULT 0, FOREIGN KEY (tenant_id, code) REFERENCES fk_buf_comp_setdefault_multi_p(tenant_id, code) ON DELETE SET DEFAULT)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_multi_p VALUES (1, 0, 0)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_multi_p VALUES (2, 1, 10)")
	bcExec(t, c, "INSERT INTO fk_buf_comp_setdefault_multi_c VALUES (10, 1, 10)")

	c.BeginTransaction(3)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_comp_setdefault_multi_p WHERE id IN (1, 2)"); err == nil {
		t.Fatal("buffered composite SET DEFAULT allowed default parent tuple to be deleted in the same statement")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected multi-row delete: %v", err)
	}
	r = bcExec(t, c, "SELECT id FROM fk_buf_comp_setdefault_multi_p ORDER BY id")
	if len(r.Rows) != 2 || fmtV(r.Rows[0][0]) != "1" || fmtV(r.Rows[1][0]) != "2" {
		t.Fatalf("buffered composite parent delete should roll back, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT tenant_id, code FROM fk_buf_comp_setdefault_multi_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "1" || fmtV(r.Rows[0][1]) != "10" {
		t.Fatalf("buffered composite child FK should remain unchanged after rejected multi-row delete, got %v", r.Rows)
	}
}

func TestBufferedSetDefaultRejectsChangingDefaultParent(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_old_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_old_c (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 1, FOREIGN KEY (pid) REFERENCES fk_buf_setdefault_old_p(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_old_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_old_p VALUES (2)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_old_c VALUES (10, 1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_setdefault_old_p WHERE id = 1"); err == nil {
		t.Fatal("buffered ON DELETE SET DEFAULT allowed default to reference deleted parent")
	}
	if _, err := c.ExecuteQuery("UPDATE fk_buf_setdefault_old_p SET id = 3 WHERE id = 2"); err != nil {
		t.Fatalf("transaction should remain usable after rejected SET DEFAULT delete: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected statement: %v", err)
	}

	r := bcExec(t, c, "SELECT id FROM fk_buf_setdefault_old_p ORDER BY id")
	if len(r.Rows) != 2 || fmtV(r.Rows[0][0]) != "1" || fmtV(r.Rows[1][0]) != "3" {
		t.Fatalf("buffered rejected delete should preserve parent 1 and allow later update, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT pid FROM fk_buf_setdefault_old_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "1" {
		t.Fatalf("buffered child FK should remain unchanged after rejected delete, got %v", r.Rows)
	}

	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_upd_old_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_upd_old_c (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 1, FOREIGN KEY (pid) REFERENCES fk_buf_setdefault_upd_old_p(id) ON UPDATE SET DEFAULT)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_upd_old_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_upd_old_c VALUES (10, 1)")

	c.BeginTransaction(3)
	if _, err := c.ExecuteQuery("UPDATE fk_buf_setdefault_upd_old_p SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("buffered ON UPDATE SET DEFAULT allowed default to reference old parent value")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected update statement: %v", err)
	}
	r = bcExec(t, c, "SELECT id FROM fk_buf_setdefault_upd_old_p WHERE id = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("buffered parent update should roll back when default references old value, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT pid FROM fk_buf_setdefault_upd_old_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "1" {
		t.Fatalf("buffered child FK should remain unchanged after rejected update, got %v", r.Rows)
	}
}

func TestBufferedSetDefaultRejectsDefaultParentDeletedInSameStatement(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_multi_p (id INTEGER PRIMARY KEY)")
	bcExec(t, c, "CREATE TABLE fk_buf_setdefault_multi_c (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 0, FOREIGN KEY (pid) REFERENCES fk_buf_setdefault_multi_p(id) ON DELETE SET DEFAULT)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_multi_p VALUES (0)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_multi_p VALUES (1)")
	bcExec(t, c, "INSERT INTO fk_buf_setdefault_multi_c VALUES (10, 1)")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("DELETE FROM fk_buf_setdefault_multi_p WHERE id IN (0, 1)"); err == nil {
		t.Fatal("buffered ON DELETE SET DEFAULT allowed default parent to be deleted in the same statement")
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit after rejected multi-row delete: %v", err)
	}

	r := bcExec(t, c, "SELECT id FROM fk_buf_setdefault_multi_p ORDER BY id")
	if len(r.Rows) != 2 || fmtV(r.Rows[0][0]) != "0" || fmtV(r.Rows[1][0]) != "1" {
		t.Fatalf("buffered multi-row parent delete should roll back, got %v", r.Rows)
	}
	r = bcExec(t, c, "SELECT pid FROM fk_buf_setdefault_multi_c WHERE id = 10")
	if len(r.Rows) != 1 || fmtV(r.Rows[0][0]) != "1" {
		t.Fatalf("buffered child FK should remain unchanged after rejected multi-row delete, got %v", r.Rows)
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

func TestBufferedUpdateUniqueIndexConflictCrossStatement(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE bu_idx_x (id INTEGER PRIMARY KEY, code TEXT)")
	bcExec(t, c, "CREATE UNIQUE INDEX bu_idx_x_code ON bu_idx_x(code)")
	bcExec(t, c, "INSERT INTO bu_idx_x VALUES (1, 'A')")
	bcExec(t, c, "INSERT INTO bu_idx_x VALUES (2, 'B')")

	c.BeginTransaction(2)
	if _, err := c.ExecuteQuery("UPDATE bu_idx_x SET code = 'C' WHERE id = 1"); err != nil {
		t.Fatalf("first update should succeed: %v", err)
	}
	if _, err := c.ExecuteQuery("UPDATE bu_idx_x SET code = 'C' WHERE id = 2"); err == nil {
		t.Fatal("expected UNIQUE INDEX failure for second row taking the same in-txn value")
	}
	_ = c.RollbackTransaction()

	r := bcExec(t, c, "SELECT id, code FROM bu_idx_x ORDER BY id")
	if got := countWithCode(r, "C"); got != 0 {
		t.Fatalf("rolled-back txn must leave no 'C' rows, got %d", got)
	}
}

func TestUpdateUniqueIndexConflictWithinStatement(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		c, _ := createCatalogWithTxnManager(t)
		bcExec(t, c, "CREATE TABLE bu_idx_w (id INTEGER PRIMARY KEY, code TEXT, grp TEXT)")
		bcExec(t, c, "CREATE UNIQUE INDEX bu_idx_w_code_grp ON bu_idx_w(code, grp)")
		bcExec(t, c, "INSERT INTO bu_idx_w VALUES (1, 'A', 'g')")
		bcExec(t, c, "INSERT INTO bu_idx_w VALUES (2, 'B', 'g')")

		if buffered {
			c.BeginTransaction(2)
		}
		if _, err := c.ExecuteQuery("UPDATE bu_idx_w SET code = 'C'"); err == nil {
			t.Fatalf("buffered=%v: expected UNIQUE INDEX failure setting all rows to one composite value", buffered)
		}
		if buffered {
			_ = c.RollbackTransaction()
		}

		r := bcExec(t, c, "SELECT id, code FROM bu_idx_w ORDER BY id")
		if got := countWithCode(r, "C"); got != 0 {
			t.Fatalf("buffered=%v: rejected UPDATE must leave no 'C' rows, got %d", buffered, got)
		}
	}
}
