package catalog

import (
	"context"
	"reflect"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// Buffered vector-table writes must refresh the HNSW entry at COMMIT time
// (refactor.md §1.16, Phase 1, option A): before the commit the graph still
// holds the old embedding (concurrent searches never observe uncommitted
// vectors), after the commit it holds the new one, and a rollback leaves it
// untouched. Assertions read the HNSW node map directly so they do not depend
// on approximate-search recall.
func TestVectorBufferedWriteCommitAppliesHNSW(t *testing.T) {
	c := newTestCatalog(t)
	mgr := txn.NewManager(nil)
	c.SetTxnManager(mgr)
	c.EnableBufferedWrites()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "vup",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	vi := &VectorIndexDef{
		Name:       "idx_vup_vec",
		TableName:  "vup",
		ColumnName: "embedding",
		Dimensions: 3,
		IndexType:  "hnsw",
		HNSW:       NewHNSWIndex("idx_vup_vec", "vup", "embedding", 3),
	}
	c.vectorIndexes[vi.Name] = vi

	parse := func(sql string) query.Statement {
		t.Helper()
		stmt, err := query.Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		return stmt
	}
	// soleNodeVector returns the vector of the single HNSW node, or nil.
	soleNodeVector := func() []float64 {
		h := vi.HNSW
		if len(h.Nodes) != 1 {
			return nil
		}
		for _, n := range h.Nodes {
			return n.Vector
		}
		return nil
	}
	vecA := []float64{1, 0, 0}
	vecB := []float64{0, 1, 0}
	vecC := []float64{0, 0, 1}

	// Baseline: committed insert is indexed.
	c.BeginTransaction(1)
	if _, _, err := c.Insert(context.Background(), parse(`INSERT INTO vup VALUES (1, '[1.0, 0.0, 0.0]')`).(*query.InsertStmt), nil); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("baseline commit: %v", err)
	}
	if got := soleNodeVector(); got == nil || !reflect.DeepEqual(got, vecA) {
		t.Fatalf("baseline: HNSW node = %v, want %v", got, vecA)
	}

	// PROBE 1 (pre-commit invisibility + commit-time refresh): update the
	// embedding inside a transaction. Before COMMIT the graph must still hold
	// the OLD embedding; after COMMIT it must hold the NEW one.
	c.BeginTransaction(2)
	if _, _, err := c.Update(context.Background(), parse(`UPDATE vup SET embedding = '[0.0, 1.0, 0.0]' WHERE id = 1`).(*query.UpdateStmt), nil); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := soleNodeVector(); got == nil || !reflect.DeepEqual(got, vecA) {
		t.Fatalf("PRE-COMMIT: HNSW node = %v, want the OLD embedding %v — buffered vector writes must not touch HNSW until COMMIT (refactor.md §1.16 option A)", got, vecA)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := soleNodeVector(); got == nil || !reflect.DeepEqual(got, vecB) {
		t.Fatalf("POST-COMMIT: HNSW node = %v, want the NEW embedding %v — commit must refresh the vector index", got, vecB)
	}

	// PROBE 2 (rollback leaves no residue): a rolled-back update must leave
	// the graph at the last committed embedding.
	c.BeginTransaction(3)
	if _, _, err := c.Update(context.Background(), parse(`UPDATE vup SET embedding = '[0.0, 0.0, 1.0]' WHERE id = 1`).(*query.UpdateStmt), nil); err != nil {
		t.Fatalf("update 2: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got := soleNodeVector(); got == nil || !reflect.DeepEqual(got, vecB) {
		t.Fatalf("POST-ROLLBACK: HNSW node = %v, want the committed embedding %v (no residue)", got, vecB)
	}
	_ = vecC
}
