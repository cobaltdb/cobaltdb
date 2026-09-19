package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// Rollback paths that replay DML undo entries through applyDMLUndoEntry (full
// ROLLBACK and ROLLBACK TO SAVEPOINT for DML-only logs) must handle vector
// indexes exactly like applyUndoEntry does: undoInsert removes the inserted
// row's HNSW entries (else rolled-back rows stay searchable), and undoDelete
// re-indexes the restored row's original vector (else the restored row keeps
// the replacing statement's stale vector content).

func setupVectorTxnTest(t *testing.T) (*Catalog, *HNSWIndex, string) {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	t.Cleanup(func() { _ = pool.Close() })
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	ct, err := query.Parse("CREATE TABLE t (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("parse create table: %v", err)
	}
	if err := c.CreateTable(ct.(*query.CreateTableStmt)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := c.CreateVectorIndex("idx_vec", "t", "embedding"); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	hnsw := c.vectorIndexes["idx_vec"].HNSW
	if hnsw == nil {
		t.Fatal("vector index missing HNSW graph")
	}
	return c, hnsw, "idx_vec"
}

func mustInsertParsed(t *testing.T, c *Catalog, sql string) {
	t.Helper()
	ins, err := query.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	if _, _, err := c.Insert(context.Background(), ins.(*query.InsertStmt), nil); err != nil {
		t.Fatalf("insert %q: %v", sql, err)
	}
}

// TestRollbackToSavepointRemovesVectorEntry pins that ROLLBACK TO SAVEPOINT
// removes the HNSW entry of an insert rolled back to the savepoint. A
// vector-indexed table bypasses buffering (the insert applies directly and
// indexes its vector immediately), so the savepoint rollback must undo that
// vector entry — otherwise a rolled-back row stays searchable via vector
// queries as a phantom.
func TestRollbackToSavepointRemovesVectorEntry(t *testing.T) {
	c, hnsw, _ := setupVectorTxnTest(t)

	c.BeginTransaction(1)
	if err := c.Savepoint("s1"); err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	mustInsertParsed(t, c, "INSERT INTO t VALUES (1, [0.1, 0.2, 0.3])")

	if len(hnsw.Nodes) != 1 {
		t.Fatalf("sanity: expected exactly one HNSW node after insert, got %d", len(hnsw.Nodes))
	}

	if err := c.RollbackToSavepoint("s1"); err != nil {
		t.Fatalf("rollback to savepoint: %v", err)
	}

	if n := len(hnsw.Nodes); n != 0 {
		t.Fatalf("FAIL: rolled-back insert's HNSW entry survived ROLLBACK TO SAVEPOINT (%d node(s) remain)", n)
	}
}

// TestReplaceRollbackRestoresVectorContent pins that REPLACE + ROLLBACK
// restores the evicted row's ORIGINAL vector in the HNSW graph. The replacing
// insert overwrites the HNSW entry at the same key (insertLocked replaces
// existing nodes), and the rollback must re-index the restored row's old
// vector — asserting only key existence would miss stale vector content.
func TestReplaceRollbackRestoresVectorContent(t *testing.T) {
	c, hnsw, _ := setupVectorTxnTest(t)

	mustInsertParsed(t, c, "INSERT INTO t VALUES (1, [0.1, 0.2, 0.3])")

	if len(hnsw.Nodes) != 1 {
		t.Fatalf("sanity: expected exactly one HNSW node after insert, got %d", len(hnsw.Nodes))
	}
	var rowKey string
	for rowKey = range hnsw.Nodes {
	}

	c.BeginTransaction(2)
	mustInsertParsed(t, c, "INSERT OR REPLACE INTO t VALUES (1, [0.9, 0.9, 0.9])")
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	node, ok := hnsw.Nodes[rowKey]
	if !ok {
		t.Fatalf("FAIL: restored row's HNSW entry missing after REPLACE rollback")
	}
	want := []float64{0.1, 0.2, 0.3}
	for i := range want {
		if node.Vector[i] != want[i] {
			t.Fatalf("FAIL: restored row carries stale vector content: got %v, want %v", node.Vector, want)
		}
	}
}
