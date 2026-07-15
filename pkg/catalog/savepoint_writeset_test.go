package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// TestSavepointRollbackClearsManagerWriteSet verifies that ROLLBACK TO SAVEPOINT
// removes the rolled-back row from the manager transaction's WriteSet. Without
// this, the row stays in the WriteSet and is WAL-logged / version-published at
// COMMIT, resurrecting on crash recovery.
func TestSavepointRollbackClearsManagerWriteSet(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	if _, err := c.ExecuteQuery("CREATE TABLE sp_t (id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	c.BeginTransaction(1)
	// A write before the savepoint must survive.
	if _, err := c.ExecuteQuery("INSERT INTO sp_t VALUES (1, 'keep')"); err != nil {
		t.Fatalf("INSERT keep: %v", err)
	}
	if err := c.Savepoint("s1"); err != nil {
		t.Fatalf("SAVEPOINT: %v", err)
	}
	// A write after the savepoint must be rolled back.
	if _, err := c.ExecuteQuery("INSERT INTO sp_t VALUES (2, 'rollback')"); err != nil {
		t.Fatalf("INSERT rollback: %v", err)
	}

	mt := c.getCurrentTxn().managerTxn.(*txn.Transaction)
	if len(mt.WriteSet) != 2 {
		t.Fatalf("expected 2 writes before rollback, got %d", len(mt.WriteSet))
	}

	if err := c.RollbackToSavepoint("s1"); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT: %v", err)
	}

	// The post-savepoint write must be gone from the manager WriteSet; the
	// pre-savepoint write must remain (so it is still durably committed).
	if len(mt.WriteSet) != 1 {
		t.Fatalf("manager WriteSet has %d entries after rollback, want 1 (rolled-back row leaked into WAL path)", len(mt.WriteSet))
	}

	// The rolled-back row must not be visible to the transaction either.
	r, err := c.ExecuteQuery("SELECT id FROM sp_t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 visible row after rollback, got %d", len(r.Rows))
	}
}
