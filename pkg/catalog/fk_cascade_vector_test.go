package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// setupFKVectorCascade builds a parent/child pair where the child carries a
// vector index and an ON DELETE CASCADE foreign key to the parent.
func setupFKVectorCascade(t *testing.T, c *Catalog, parent, child string) *VectorIndexDef {
	t.Helper()
	mustCreateTable(t, c, parent+" (id INTEGER PRIMARY KEY)")
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: child,
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "pid", Type: query.TokenInteger},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"pid"},
			ReferencedTable:   parent,
			ReferencedColumns: []string{"id"},
			OnDelete:          "CASCADE",
		}},
	}); err != nil {
		t.Fatalf("CreateTable %s: %v", child, err)
	}

	// Inject the vector index BEFORE seeding so the insert path
	// (updateVectorIndexesForInsert) populates the HNSW entries.
	vi := &VectorIndexDef{
		Name:       "idx_" + child + "_vec",
		TableName:  child,
		ColumnName: "embedding",
		Dimensions: 3,
		IndexType:  "hnsw",
		HNSW:       NewHNSWIndex("idx_"+child+"_vec", child, "embedding", 3),
	}
	c.vectorIndexes[vi.Name] = vi
	return vi
}

func seedRow(t *testing.T, c *Catalog, table string, columns []string, values []query.Expression) {
	t.Helper()
	st := &query.InsertStmt{
		Table:   table,
		Columns: columns,
		Values:  [][]query.Expression{values},
	}
	if _, _, err := c.Insert(context.Background(), st, nil); err != nil {
		t.Fatalf("seed %s: %v", table, err)
	}
}

func fkNum(v float64) query.Expression { return &query.NumberLiteral{Value: v} }

func vectorKeyContains(t *testing.T, vi *VectorIndexDef, probe []float64, key string) bool {
	t.Helper()
	keys, _, err := vi.HNSW.SearchKNN(probe, 10)
	if err != nil {
		t.Fatalf("SearchKNN: %v", err)
	}
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

func countLiveRows(t *testing.T, c *Catalog, table string) int {
	t.Helper()
	tbl, err := c.getTableLocked(table)
	if err != nil {
		t.Fatalf("getTableLocked %s: %v", table, err)
	}
	tree := c.tableTrees[table]
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		t.Fatalf("scan %s: %v", table, err)
	}
	defer iter.Close()
	live := 0
	for iter.HasNext() {
		_, valueData, iterErr := iter.NextString()
		if iterErr != nil {
			t.Fatalf("scan read %s: %v", table, iterErr)
		}
		_, _, isLive, err := decodeLiveRowFull([]byte(valueData), len(tbl.Columns))
		if err != nil {
			t.Fatalf("decode %s: %v", table, err)
		}
		if isLive {
			live++
		}
	}
	return live
}

// TestFKCascadeDeleteRemovesVectorEntries verifies that rows removed by an
// ON DELETE CASCADE are also removed from the child table's vector index.
// The FK enforcer's cascade paths (deleteRow for direct mode) soft-deleted
// the child rows and maintained the btree secondary indexes but never
// touched the vector indexes, so cascade-deleted child rows remained
// searchable via HNSW — dead rows returned by vector search.
func TestFKCascadeDeleteRemovesVectorEntries(t *testing.T) {
	c := newTestCatalog(t)
	vi := setupFKVectorCascade(t, c, "fkv_parent", "fkv_child")

	seedRow(t, c, "fkv_parent", []string{"id"}, []query.Expression{fkNum(1)})
	seedRow(t, c, "fkv_child", []string{"id", "pid", "embedding"}, []query.Expression{fkNum(10), fkNum(1), &query.VectorLiteral{Values: []float64{1, 0, 0}}})
	seedRow(t, c, "fkv_child", []string{"id", "pid", "embedding"}, []query.Expression{fkNum(11), fkNum(1), &query.VectorLiteral{Values: []float64{0, 1, 0}}})

	keyOf := func(id int64) string { return fmt.Sprintf("%020d", id) }

	// Guard: both child rows indexed before the delete.
	for _, id := range []int64{10, 11} {
		if !vectorKeyContains(t, vi, []float64{1, 0, 0}, keyOf(id)) {
			t.Fatalf("pre-delete guard: vector index missing child row %d", id)
		}
	}

	// DELETE the parent: the cascade must soft-delete both child rows AND
	// remove them from the child table's vector index.
	if _, _, err := c.Delete(context.Background(), &query.DeleteStmt{Table: "fkv_parent"}, nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if got := countLiveRows(t, c, "fkv_parent"); got != 0 {
		t.Errorf("parent live rows = %d, want 0", got)
	}
	if got := countLiveRows(t, c, "fkv_child"); got != 0 {
		t.Errorf("child live rows = %d, want 0 (cascade did not fire)", got)
	}

	// The cascade-deleted child rows must be gone from the vector index too:
	// they no longer exist, so vector searches must not return them.
	for _, id := range []int64{10, 11} {
		if vectorKeyContains(t, vi, []float64{1, 0, 0}, keyOf(id)) {
			t.Errorf("vector index retained cascade-deleted child row %d (key %q) — dead row returned by vector search",
				id, keyOf(id))
		}
	}
}

// TestFKCascadeRollbackRestoresVectorEntries is the rollback-side companion:
// when a CASCADE fails mid-way (a grandchild RESTRICT), the already-cascaded
// child rows are restored by the FK rollback — and their vector index
// entries, which the cascade path removes immediately, must be restored with
// them.
func TestFKCascadeRollbackRestoresVectorEntries(t *testing.T) {
	c := newTestCatalog(t)
	vi := setupFKVectorCascade(t, c, "fkr_parent", "fkr_child")
	mustCreateTable(t, c, "fkr_grand (id INTEGER PRIMARY KEY, cid INTEGER, FOREIGN KEY (cid) REFERENCES fkr_child(id) ON DELETE RESTRICT)")

	seedRow(t, c, "fkr_parent", []string{"id"}, []query.Expression{fkNum(1)})
	seedRow(t, c, "fkr_child", []string{"id", "pid", "embedding"}, []query.Expression{fkNum(10), fkNum(1), &query.VectorLiteral{Values: []float64{1, 0, 0}}})
	seedRow(t, c, "fkr_child", []string{"id", "pid", "embedding"}, []query.Expression{fkNum(11), fkNum(1), &query.VectorLiteral{Values: []float64{0, 1, 0}}})
	seedRow(t, c, "fkr_grand", []string{"id", "cid"}, []query.Expression{fkNum(100), fkNum(11)})

	keyOf := func(id int64) string { return fmt.Sprintf("%020d", id) }
	for _, id := range []int64{10, 11} {
		if !vectorKeyContains(t, vi, []float64{1, 0, 0}, keyOf(id)) {
			t.Fatalf("pre-delete guard: vector index missing child row %d", id)
		}
	}

	// DELETE the parent: cascading to child 11 hits the grandchild RESTRICT,
	// so the whole statement must fail and roll back.
	_, _, err := c.Delete(context.Background(), &query.DeleteStmt{Table: "fkr_parent"}, nil)
	if err == nil {
		t.Fatal("expected the CASCADE into the RESTRICT-protected child row to fail the statement")
	}
	if !errors.Is(err, ErrReferencedRowExists) {
		t.Fatalf("error = %v, want ErrReferencedRowExists", err)
	}

	// Everything must be back: parent, both child rows, grandchild.
	if got := countLiveRows(t, c, "fkr_parent"); got != 1 {
		t.Errorf("parent live rows = %d, want 1", got)
	}
	if got := countLiveRows(t, c, "fkr_child"); got != 2 {
		t.Errorf("child live rows = %d, want 2 (FK rollback broken)", got)
	}
	if got := countLiveRows(t, c, "fkr_grand"); got != 1 {
		t.Errorf("grand live rows = %d, want 1", got)
	}

	// And the restored child rows must still be searchable.
	for _, id := range []int64{10, 11} {
		if !vectorKeyContains(t, vi, []float64{1, 0, 0}, keyOf(id)) {
			t.Errorf("vector index lost child row %d (key %q) after rolled-back CASCADE — live row invisible to vector search",
				id, keyOf(id))
		}
	}
}
