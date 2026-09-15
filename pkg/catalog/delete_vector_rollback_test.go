package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestDeleteVectorIndexRestoredOnStatementFailure verifies that a DELETE whose
// statement fails after the vector-index deletion step does not leave the
// (still-live) rows missing from the vector index.
//
// applyDeleteEntryDirect/applyDeleteEntryBuffered remove the row key from
// every vector index immediately (HNSW.Delete + persisted def), but the
// statement rollback paths only restore B-tree rows
// (rollbackAppliedDeleteEntries) and btree indexes
// (rebuildTableIndexesLocked iterates c.indexes, not c.vectorIndexes) — or,
// on the buffered path, truncate ts.pendingWrites. The vector deletion
// leaked, so a rolled-back DELETE left live rows invisible to vector search.
func TestDeleteVectorIndexRestoredOnStatementFailure(t *testing.T) {
	c := newTestCatalog(t)

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "vdel_vec",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	keyOf := func(id int64) string { return fmt.Sprintf("%020d", id) }

	// Inject the vector index BEFORE seeding so the insert path
	// (updateVectorIndexesForInsert) populates the HNSW entries.
	vi := &VectorIndexDef{
		Name:       "idx_vdel_vec",
		TableName:  "vdel_vec",
		ColumnName: "embedding",
		Dimensions: 3,
		IndexType:  "hnsw",
		HNSW:       NewHNSWIndex("idx_vdel_vec", "vdel_vec", "embedding", 3),
	}
	c.vectorIndexes[vi.Name] = vi

	// Seed two rows with real vectors; the insert path indexes them into the
	// vector index (catalog_insert.go calls updateVectorIndexesForInsert).
	seed := func(id int64, vec []float64) {
		st := &query.InsertStmt{
			Table:   "vdel_vec",
			Columns: []string{"id", "embedding"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(id)},
				&query.VectorLiteral{Values: vec},
			}},
		}
		if _, _, err := c.Insert(context.Background(), st, nil); err != nil {
			t.Fatalf("seed %d: %v", id, err)
		}
	}
	seed(1, []float64{1, 0, 0})
	seed(2, []float64{0, 1, 0})

	// Guard: both rows indexed before the delete.
	contains := func(id int64) bool {
		keys, _, err := vi.HNSW.SearchKNN([]float64{float64(id), 0, 0}, 10)
		if err != nil {
			t.Fatalf("SearchKNN guard: %v", err)
		}
		for _, k := range keys {
			if k == keyOf(id) {
				return true
			}
		}
		return false
	}
	for _, id := range []int64{1, 2} {
		if !contains(id) {
			t.Fatalf("pre-delete guard: vector index missing row %d (insert seeding did not reach HNSW)", id)
		}
	}

	// Inject an AFTER DELETE trigger whose body always fails (INSERT into a
	// missing table) so the statement fails after the vector-index deletion.
	c.triggers["boom"] = &query.CreateTriggerStmt{
		Name:  "boom",
		Table: "vdel_vec",
		Time:  "AFTER",
		Event: "DELETE",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "no_such_table",
				Columns: []string{"id"},
				Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
			},
		},
	}

	_, _, err := c.Delete(context.Background(), &query.DeleteStmt{Table: "vdel_vec"}, nil)
	if err == nil {
		t.Fatal("expected the failing AFTER DELETE trigger to fail the DELETE statement")
	}

	// The statement rolled back: both rows must still be live in the B-tree.
	table, err := c.getTableLocked("vdel_vec")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	tree := c.tableTrees["vdel_vec"]
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	liveRows := 0
	for iter.HasNext() {
		_, valueData, iterErr := iter.NextString()
		if iterErr != nil {
			iter.Close()
			t.Fatalf("scan read: %v", iterErr)
		}
		_, _, live, err := decodeLiveRowFull([]byte(valueData), len(table.Columns))
		if err != nil {
			iter.Close()
			t.Fatalf("decode: %v", err)
		}
		if live {
			liveRows++
		}
	}
	iter.Close()
	if liveRows != 2 {
		t.Fatalf("live rows after rolled-back DELETE = %d, want 2 (B-tree rollback broken)", liveRows)
	}

	// AND the vector index must still contain both row keys: the rows are
	// live, so vector searches must find them.
	for _, id := range []int64{1, 2} {
		if !contains(id) {
			t.Errorf("vector index lost row %d (key %q) after rolled-back DELETE — live row invisible to vector search",
				id, keyOf(id))
		}
	}
}
