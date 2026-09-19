package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// A PK-changing UPDATE moves the row's B-tree key from oldKey to newKey, but
// applyUpdateEntryDirect re-indexed the row's vector at the OLD key
// unconditionally (updateVectorIndexesForUpdate with string(entry.key)). The
// HNSW node key IS the row's B-tree key, so the entry must follow the row to
// newKey — otherwise vector searches return a dangling old key and the
// updated row vanishes from vector results. The rollback path must mirror
// this: remove both keys before restoring the pre-update vector at oldKey.

func setupPKChangeVectorTest(t *testing.T) (*Catalog, *HNSWIndex, string) {
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

	ins, err := query.Parse("INSERT INTO t VALUES (1, [0.1, 0.2, 0.3])")
	if err != nil {
		t.Fatalf("parse insert: %v", err)
	}
	if _, _, err := c.Insert(context.Background(), ins.(*query.InsertStmt), nil); err != nil {
		t.Fatalf("insert: %v", err)
	}

	hnsw := c.vectorIndexes["idx_vec"].HNSW
	if hnsw == nil {
		t.Fatal("vector index missing HNSW graph")
	}
	if len(hnsw.Nodes) != 1 {
		t.Fatalf("sanity: expected exactly one HNSW node after insert, got %d", len(hnsw.Nodes))
	}
	var oldKey string
	for oldKey = range hnsw.Nodes {
	}
	return c, hnsw, oldKey
}

// TestPKChangeUpdateRekeysVectorEntry pins the apply side: after
// `UPDATE t SET id = 99 WHERE id = 1`, the single HNSW node must be keyed by
// the row's NEW key (the node follows the row), still carrying the row's
// vector.
func TestPKChangeUpdateRekeysVectorEntry(t *testing.T) {
	c, hnsw, oldKey := setupPKChangeVectorTest(t)

	upd, err := query.Parse("UPDATE t SET id = 99 WHERE id = 1")
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}
	if _, _, err := c.Update(context.Background(), upd.(*query.UpdateStmt), nil); err != nil {
		t.Fatalf("pk-change update: %v", err)
	}

	if len(hnsw.Nodes) != 1 {
		t.Fatalf("FAIL: expected exactly one HNSW node after PK-change update, got %d", len(hnsw.Nodes))
	}
	newNodeKey := ""
	for k := range hnsw.Nodes {
		newNodeKey = k
	}
	if newNodeKey == oldKey {
		t.Fatalf("FAIL: vector entry left at the OLD key %q after the row moved to a new primary key (dangling entry; no node at the new key)", oldKey)
	}
	node := hnsw.Nodes[newNodeKey]
	want := []float64{0.1, 0.2, 0.3}
	for i := range want {
		if node.Vector[i] != want[i] {
			t.Fatalf("FAIL: node carries wrong vector after PK-change update: got %v, want %v", node.Vector, want)
		}
	}
}

// TestPKChangeUpdateRollbackRestoresVectorEntry pins the undo side: a
// rolled-back PK-changing UPDATE must restore exactly one HNSW node at the
// ORIGINAL key with the original vector — the undo must remove the new-key
// entry the (fixed) apply created, not only the old-key entry.
func TestPKChangeUpdateRollbackRestoresVectorEntry(t *testing.T) {
	c, hnsw, oldKey := setupPKChangeVectorTest(t)

	c.BeginTransaction(1)
	upd, err := query.Parse("UPDATE t SET id = 99 WHERE id = 1")
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}
	if _, _, err := c.Update(context.Background(), upd.(*query.UpdateStmt), nil); err != nil {
		t.Fatalf("pk-change update: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if len(hnsw.Nodes) != 1 {
		t.Fatalf("FAIL: expected exactly one HNSW node after rolled-back PK-change update, got %d", len(hnsw.Nodes))
	}
	node, ok := hnsw.Nodes[oldKey]
	if !ok {
		t.Fatalf("FAIL: restored row's HNSW entry missing at the original key after rollback")
	}
	want := []float64{0.1, 0.2, 0.3}
	for i := range want {
		if node.Vector[i] != want[i] {
			t.Fatalf("FAIL: restored node carries wrong vector: got %v, want %v", node.Vector, want)
		}
	}
}
