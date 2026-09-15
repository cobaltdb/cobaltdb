package catalog

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestUpdateVectorRestoredOnFKMidApplyFailure proves that an UPDATE whose
// failure lands MID-APPLY — row 1 fully applied (including its vector-index
// rewrite), row 2 rejected by an ON UPDATE RESTRICT child — restores the
// rolled-back rows' vector entries along with their B-tree rows.
//
// Why this construction: checkConstraintsForUpdate (the scan phase) verifies
// UNIQUE, NOT NULL/CHECK, and only the CHILD side of FKs (this table's
// references to parents). The PARENT side — children referencing this table
// — is enforced by fke.OnUpdateRow inside the applyUpdateEntries loop, AFTER
// earlier rows have been applied via applyUpdateEntryDirect (which rewrites
// the row key's HNSW entry immediately). rollbackAppliedUpdateEntries
// restored only B-tree rows and btree indexes, so row 1's HNSW entry kept
// the NEW (rolled-back) vector — stale vector content for a live row.
func TestUpdateVectorRestoredOnFKMidApplyFailure(t *testing.T) {
	c := newTestCatalog(t)

	// Parent p: vector-indexed, PK is the FK-referenced column.
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "p",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}); err != nil {
		t.Fatalf("CreateTable p: %v", err)
	}

	// Inject the vector index BEFORE seeding so the insert path populates
	// the HNSW entries.
	vi := &VectorIndexDef{
		Name:       "idx_p_vec",
		TableName:  "p",
		ColumnName: "embedding",
		Dimensions: 3,
		IndexType:  "hnsw",
		HNSW:       NewHNSWIndex("idx_p_vec", "p", "embedding", 3),
	}
	c.vectorIndexes[vi.Name] = vi

	// Child c: RESTRICT on parent PK updates, referencing parent 2 only.
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "c",
		Columns: []*query.ColumnDef{
			{Name: "cid", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "p",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "RESTRICT",
		}},
	}); err != nil {
		t.Fatalf("CreateTable c: %v", err)
	}

	if _, err := c.ExecuteQuery("INSERT INTO p VALUES (1, [1, 0, 0])"); err != nil {
		t.Fatalf("seed p row 1: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO p VALUES (2, [0, 1, 0])"); err != nil {
		t.Fatalf("seed p row 2: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO c VALUES (1, 2)"); err != nil {
		t.Fatalf("seed c row: %v", err)
	}

	keyOf := func(id int64) string { return fmt.Sprintf("%020d", id) }
	nodeVector := func(id int64) []float64 {
		node := vi.HNSW.Nodes[keyOf(id)]
		if node == nil {
			t.Fatalf("vector index missing row %d", id)
		}
		return node.Vector
	}

	// Guards: both parent rows indexed with their seed vectors.
	if got := nodeVector(1); !reflect.DeepEqual(got, []float64{1, 0, 0}) {
		t.Fatalf("pre-update guard: row 1 vector = %v, want [1 0 0]", got)
	}
	if got := nodeVector(2); !reflect.DeepEqual(got, []float64{0, 1, 0}) {
		t.Fatalf("pre-update guard: row 2 vector = %v, want [0 1 0]", got)
	}

	// UPDATE both parents: row 1 applies fully (vector rewritten), row 2 is
	// rejected by the RESTRICT child mid-apply.
	_, err := c.ExecuteQuery("UPDATE p SET id = id + 100, embedding = [0.9, 0, 0] WHERE id IN (1, 2)")
	if err == nil {
		t.Fatal("expected the ON UPDATE RESTRICT child to fail the UPDATE statement")
	}
	if !strings.Contains(err.Error(), "foreign key constraint") { // applyUpdateEntries' mid-loop wrapper
		t.Fatalf("error = %v, want a foreign key constraint failure", err)
	}

	// The statement rolled back: both parent rows must be live.
	if got := countLiveRows(t, c, "p"); got != 2 {
		t.Errorf("live rows after rolled-back UPDATE = %d, want 2 (B-tree rollback broken)", got)
	}

	// AND the vector index must hold the OLD (pre-update) vectors: the
	// mid-apply failure must not leave row 1's rolled-back vector behind.
	wantVecs := map[int64][]float64{1: {1, 0, 0}, 2: {0, 1, 0}}
	for _, id := range []int64{1, 2} {
		got := nodeVector(id)
		if !reflect.DeepEqual(got, wantVecs[id]) {
			t.Errorf("vector index retains rolled-back UPDATE content for row %d: vector = %v, want %v",
				id, got, wantVecs[id])
		}
	}
}
