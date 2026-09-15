package catalog

import (
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ==================== frameRowBound tests ====================

func TestFrameRowBound_Nil(t *testing.T) {
	result := frameRowBound(nil, 5, 10)
	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
}

func TestFrameRowBound_UnboundedPreceding(t *testing.T) {
	b := &query.WindowFrameBound{Type: "UNBOUNDED_PRECEDING"}
	result := frameRowBound(b, 5, 10)
	if result != 0 {
		t.Errorf("expected 0, got %d", result)
	}
}

func TestFrameRowBound_Preceding(t *testing.T) {
	b := &query.WindowFrameBound{Type: "PRECEDING", Offset: 3}
	result := frameRowBound(b, 5, 10)
	if result != 2 {
		t.Errorf("expected 2, got %d", result)
	}
}

func TestFrameRowBound_CurrentRow(t *testing.T) {
	b := &query.WindowFrameBound{Type: "CURRENT_ROW"}
	result := frameRowBound(b, 5, 10)
	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
}

func TestFrameRowBound_Following(t *testing.T) {
	b := &query.WindowFrameBound{Type: "FOLLOWING", Offset: 2}
	result := frameRowBound(b, 5, 10)
	if result != 7 {
		t.Errorf("expected 7, got %d", result)
	}
}

func TestFrameRowBound_UnboundedFollowing(t *testing.T) {
	b := &query.WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"}
	result := frameRowBound(b, 3, 10)
	if result != 9 {
		t.Errorf("expected 9, got %d", result)
	}
}

func TestFrameRowBound_UnknownType(t *testing.T) {
	b := &query.WindowFrameBound{Type: "UNKNOWN"}
	result := frameRowBound(b, 5, 10)
	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
}

// ==================== materializedViewColumnNames tests ====================

func TestMaterializedViewColumnNames_Nil(t *testing.T) {
	names := materializedViewColumnNames(nil)
	if names != nil {
		t.Errorf("expected nil, got %v", names)
	}
}

func TestMaterializedViewColumnNames_WithColumns(t *testing.T) {
	mv := &MaterializedViewDef{
		Columns: []string{"a", "b", "c"},
	}
	names := materializedViewColumnNames(mv)
	if len(names) != 3 || names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Errorf("expected [a b c], got %v", names)
	}
}

func TestMaterializedViewColumnNames_NoColumnsNoData(t *testing.T) {
	mv := &MaterializedViewDef{}
	names := materializedViewColumnNames(mv)
	if names != nil {
		t.Errorf("expected nil, got %v", names)
	}
}

func TestMaterializedViewColumnNames_FromData(t *testing.T) {
	mv := &MaterializedViewDef{
		Data: []map[string]interface{}{
			{"x": 1, "y": 2},
		},
	}
	names := materializedViewColumnNames(mv)
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %v", names)
	}
	seen := make(map[string]bool)
	for _, n := range names {
		seen[n] = true
	}
	if !seen["x"] || !seen["y"] {
		t.Errorf("expected names to contain x and y, got %v", names)
	}
}

// ==================== materializedViewColumnDefs tests ====================

func TestMaterializedViewColumnDefs(t *testing.T) {
	mv := &MaterializedViewDef{
		Columns: []string{"a", "b"},
	}
	defs := materializedViewColumnDefs(mv)
	if len(defs) != 2 {
		t.Fatalf("expected 2 defs, got %d", len(defs))
	}
	if defs[0].Name != "a" || defs[0].Type != "TEXT" {
		t.Errorf("expected {a TEXT}, got %+v", defs[0])
	}
	if defs[1].Name != "b" || defs[1].Type != "TEXT" {
		t.Errorf("expected {b TEXT}, got %+v", defs[1])
	}
}

func TestMaterializedViewColumnDefs_Nil(t *testing.T) {
	defs := materializedViewColumnDefs(nil)
	if len(defs) != 0 {
		t.Errorf("expected 0 defs, got %d", len(defs))
	}
}

// ==================== materializedViewColumnsAndRows tests ====================

func TestMaterializedViewColumnsAndRows_Nil(t *testing.T) {
	cols, rows := materializedViewColumnsAndRows(nil)
	if len(cols) != 0 {
		t.Errorf("expected 0 cols, got %d", len(cols))
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestMaterializedViewColumnsAndRows_WithData(t *testing.T) {
	mv := &MaterializedViewDef{
		Columns: []string{"name", "val"},
		Data: []map[string]interface{}{
			{"name": "foo", "val": int64(10)},
			{"name": "bar", "val": int64(20)},
		},
	}
	cols, rows := materializedViewColumnsAndRows(mv)
	if len(cols) != 2 {
		t.Fatalf("expected 2 cols, got %d", len(cols))
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] != "foo" || rows[0][1] != int64(10) {
		t.Errorf("row 0: expected [foo 10], got %v", rows[0])
	}
	if rows[1][0] != "bar" || rows[1][1] != int64(20) {
		t.Errorf("row 1: expected [bar 20], got %v", rows[1])
	}
}

// ==================== storeMaterializedViewDef tests ====================

func TestStoreMaterializedViewDef_NilTree(t *testing.T) {
	c := &Catalog{}
	err := c.storeMaterializedViewDef("test_mv", "SELECT 1", nil)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestStoreMaterializedViewDef_WithTree(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	mv := &MaterializedViewDef{
		Columns:     []string{"a", "b"},
		Data:        []map[string]interface{}{{"a": int64(1), "b": int64(2)}},
		LastRefresh: now,
	}

	err = c.storeMaterializedViewDef("test_mv", "SELECT a, b FROM t", mv)
	if err != nil {
		t.Fatalf("storeMaterializedViewDef failed: %v", err)
	}

	// Verify via tree (storeMaterializedViewDef persists to B-tree, not in-memory map)
	val, err := tree.Get([]byte("mv:test_mv"))
	if err != nil {
		t.Fatalf("tree.Get failed: %v", err)
	}
	if len(val) == 0 {
		t.Fatal("expected stored materialized view data")
	}
}

func TestStoreMaterializedViewDef_NilMV(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	err = c.storeMaterializedViewDef("test_mv", "SELECT 1", nil)
	if err != nil {
		t.Fatalf("storeMaterializedViewDef failed: %v", err)
	}

	// Verify via tree
	val, err := tree.Get([]byte("mv:test_mv"))
	if err != nil {
		t.Fatalf("tree.Get failed: %v", err)
	}
	if len(val) == 0 {
		t.Fatal("expected stored materialized view data with nil MV")
	}
}

// ==================== storeFTSIndexDef tests ====================

func TestStoreFTSIndexDef_NilTree(t *testing.T) {
	c := &Catalog{}
	err := c.storeFTSIndexDef(&FTSIndexDef{Name: "fts_idx"})
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestStoreFTSIndexDef_WithTree(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	fts := &FTSIndexDef{
		Name:      "my_fts",
		TableName: "articles",
		Columns:   []string{"title", "body"},
		Index:     map[string][]int64{"hello": {1, 2}},
	}

	err = c.storeFTSIndexDef(fts)
	if err != nil {
		t.Fatalf("storeFTSIndexDef failed: %v", err)
	}

	val, err := tree.Get([]byte("fts:my_fts"))
	if err != nil {
		t.Fatalf("tree.Get failed: %v", err)
	}
	if val == nil {
		t.Fatal("expected stored FTS index")
	}
}
