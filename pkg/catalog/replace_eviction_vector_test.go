package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestReplaceEvictionRestoresVectorIndex pins that ROLLBACK of a REPLACE that
// evicted a committed row restores BOTH the row and its original HNSW vector
// content. A REPLACE on a vector-indexed table takes the direct path inside
// transactions (vector tables bypass buffering, and ConflictReplace forces it
// regardless): the eviction records an undoDelete, the replacing row applies
// immediately and overwrites the HNSW node at the same key (insertLocked
// replaces existing nodes), and the rollback's DML undo replay must re-index
// the restored row's ORIGINAL vector — asserting only key existence would
// miss stale vector content left behind by the replacing statement.
func TestReplaceEvictionRestoresVectorIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
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

	ins, err := query.Parse("INSERT INTO t VALUES (1, [0.1, 0.2, 0.3])")
	if err != nil {
		t.Fatalf("parse insert: %v", err)
	}
	if _, _, err := c.Insert(ctx, ins.(*query.InsertStmt), nil); err != nil {
		t.Fatalf("insert: %v", err)
	}

	hnsw := c.vectorIndexes["idx_vec"].HNSW
	if hnsw == nil {
		t.Fatal("vector index missing HNSW graph")
	}
	if len(hnsw.Nodes) != 1 {
		t.Fatalf("sanity: expected exactly one HNSW node after insert, got %d", len(hnsw.Nodes))
	}
	var rowKey string
	for rowKey = range hnsw.Nodes {
	}

	c.BeginTransaction(1)
	rep, err := query.Parse("INSERT OR REPLACE INTO t VALUES (1, [0.9, 0.9, 0.9])")
	if err != nil {
		t.Fatalf("parse replace: %v", err)
	}
	if _, _, err := c.Insert(ctx, rep.(*query.InsertStmt), nil); err != nil {
		t.Fatalf("replace insert: %v", err)
	}
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
