package catalog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestInsertVectorEntriesRemovedOnStatementFailure verifies that a multi-row
// INSERT whose statement fails after the rows were applied (failing AFTER
// INSERT trigger) does not leave the rolled-back rows in the table's vector
// index. applyInsertRowDirect inserts the row key into every vector index
// immediately (updateVectorIndexesForInsert → HNSW.Insert), but the
// statement rollback (rollbackStatementInserts) only removed the B-tree rows
// and btree index entries — the HNSW entries leaked, so rolled-back rows
// remained searchable via vector queries: dead rows returned by vector search.
func TestInsertVectorEntriesRemovedOnStatementFailure(t *testing.T) {
	c := newTestCatalog(t)

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "vins",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Inject the vector index so applyInsertRowDirect inserts into HNSW.
	vi := &VectorIndexDef{
		Name:       "idx_vins_vec",
		TableName:  "vins",
		ColumnName: "embedding",
		Dimensions: 3,
		IndexType:  "hnsw",
		HNSW:       NewHNSWIndex("idx_vins_vec", "vins", "embedding", 3),
	}
	c.vectorIndexes[vi.Name] = vi

	// Inject an AFTER INSERT trigger whose body always fails so the
	// statement fails in finalizeInsert after all rows were applied.
	c.triggers["boom"] = &query.CreateTriggerStmt{
		Name:  "boom",
		Table: "vins",
		Time:  "AFTER",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "no_such_table",
				Columns: []string{"id"},
				Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
			},
		},
	}

	st := &query.InsertStmt{
		Table:   "vins",
		Columns: []string{"id", "embedding"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.VectorLiteral{Values: []float64{1, 0, 0}}},
			{&query.NumberLiteral{Value: 2}, &query.VectorLiteral{Values: []float64{0, 1, 0}}},
		},
	}
	_, _, err := c.Insert(context.Background(), st, nil)
	if err == nil {
		t.Fatal("expected the failing AFTER INSERT trigger to fail the INSERT statement")
	}
	if !strings.Contains(err.Error(), "AFTER INSERT trigger failed") {
		t.Fatalf("error = %v, want AFTER INSERT trigger failure", err)
	}

	// The statement rolled back: no rows may remain live in the B-tree.
	if got := countLiveRows(t, c, "vins"); got != 0 {
		t.Errorf("live rows after rolled-back INSERT = %d, want 0 (B-tree rollback broken)", got)
	}

	// AND the rolled-back rows must be gone from the vector index: they no
	// longer exist, so vector searches must not return them.
	keyOf := func(id int64) string { return fmt.Sprintf("%020d", id) }
	probes := map[int64][]float64{1: {1, 0, 0}, 2: {0, 1, 0}}
	for _, id := range []int64{1, 2} {
		if vectorKeyContains(t, vi, probes[id], keyOf(id)) {
			t.Errorf("vector index retained rolled-back INSERT row %d (key %q) — dead row returned by vector search",
				id, keyOf(id))
		}
	}
}
