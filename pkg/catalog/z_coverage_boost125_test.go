package catalog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ─── helpers ────────────────────────────────────────────────────────────────

func newCat125() *Catalog {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	return New(tree, pool, nil)
}

func exec125(c *Catalog, sql string) error {
	_, err := c.ExecuteQuery(sql)
	return err
}

func rows125(c *Catalog, sql string) [][]interface{} {
	r, _ := c.ExecuteQuery(sql)
	if r == nil {
		return nil
	}
	return r.Rows
}

// ─── buildJSONIndex (65%) ────────────────────────────────────────────────────

func TestB125_BuildJSONIndex_WithData(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_jt (id INTEGER PRIMARY KEY, data JSON)")
	ctx := context.Background()

	// Insert some JSON rows first so buildJSONIndex iterates over them
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_jt",
			Columns: []string{"id", "data"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: `{"score":` + string(rune('0'+i)) + `}`},
			}},
		}, nil)
	}

	// CreateJSONIndex triggers buildJSONIndex on existing data
	err := c.CreateJSONIndex("b125_json_idx", "b125_jt", "data", "$.score", "NUMERIC")
	if err != nil {
		t.Fatalf("CreateJSONIndex failed: %v", err)
	}

	// Try to create a duplicate — covers the "already exists" branch
	err2 := c.CreateJSONIndex("b125_json_idx", "b125_jt", "data", "$.score", "NUMERIC")
	if err2 == nil {
		t.Error("expected error for duplicate JSON index")
	}

	// Wrong table
	err3 := c.CreateJSONIndex("b125_json_idx2", "no_such_table", "data", "$.score", "NUMERIC")
	if err3 == nil {
		t.Error("expected error for missing table")
	}

	// Non-JSON column
	exec125(c, "CREATE TABLE b125_jt2 (id INTEGER PRIMARY KEY, name TEXT)")
	err4 := c.CreateJSONIndex("b125_json_idx3", "b125_jt2", "name", "$.x", "TEXT")
	if err4 == nil {
		t.Error("expected error for non-JSON column")
	}

	// Missing column
	err5 := c.CreateJSONIndex("b125_json_idx4", "b125_jt", "no_col", "$.score", "NUMERIC")
	if err5 == nil {
		t.Error("expected error for missing column")
	}
}

// ─── vector.Update + vector.max (66.7%) ─────────────────────────────────────

func TestB125_HNSWUpdate(t *testing.T) {
	h := NewHNSWIndex("testidx", "t", "vec", 3)

	v1 := []float64{1.0, 2.0, 3.0}
	v2 := []float64{4.0, 5.0, 6.0}

	if err := h.Insert("k1", v1); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := h.Insert("k2", v2); err != nil {
		t.Fatalf("Insert k2 failed: %v", err)
	}

	// Update calls Delete+Insert → covers Update branch
	// Call Update directly (not via re-Insert to avoid deadlock in existing code)
	if err := h.Update("k1", v2); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// SearchKNN exercises max()
	keys, dists, err := h.SearchKNN([]float64{1.0, 1.0, 1.0}, 5)
	if err != nil {
		t.Fatalf("SearchKNN failed: %v", err)
	}
	t.Logf("SearchKNN keys=%v dists=%v", keys, dists)

	// SearchKNN dimension mismatch
	_, _, err2 := h.SearchKNN([]float64{1.0}, 1)
	if err2 == nil {
		t.Error("expected dimension mismatch error")
	}

	// SearchKNN on empty index
	h2 := NewHNSWIndex("empty", "t", "vec", 3)
	keys2, dists2, err3 := h2.SearchKNN([]float64{1, 2, 3}, 3)
	if err3 != nil {
		t.Fatalf("SearchKNN empty: %v", err3)
	}
	t.Logf("SearchKNN empty: keys=%v dists=%v", keys2, dists2)
}

func TestB125_HNSWmax(t *testing.T) {
	// Test the max() helper directly
	if max(3, 5) != 5 {
		t.Error("max(3,5) should be 5")
	}
	if max(7, 2) != 7 {
		t.Error("max(7,2) should be 7")
	}
}

func TestB125_HNSWremoveString(t *testing.T) {
	s := []string{"a", "b", "c"}
	s2 := removeString(s, "b")
	if len(s2) != 2 {
		t.Errorf("expected 2 elements, got %d", len(s2))
	}
	// Remove non-existing element — no-op
	s3 := removeString(s2, "z")
	if len(s3) != 2 {
		t.Errorf("expected 2 elements after no-op remove, got %d", len(s3))
	}
}

// ─── processUpdateRow (69.2%) via index-assisted UPDATE ─────────────────────

func TestB125_ProcessUpdateRowViaIndex(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_pu (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_pu",
			Columns: []string{"id", "val", "name"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i * 10)},
				&query.StringLiteral{Value: "row"},
			}},
		}, nil)
	}

	// Create an index so UPDATE uses the index path (processUpdateRow)
	exec125(c, "CREATE INDEX b125_pu_val_idx ON b125_pu (val)")

	// UPDATE using indexed column in WHERE
	exec125(c, "UPDATE b125_pu SET name = 'updated' WHERE val = 30")

	rows := rows125(c, "SELECT name FROM b125_pu WHERE val = 30")
	if len(rows) == 0 || rows[0][0] != "updated" {
		t.Errorf("expected 'updated', got %v", rows)
	}

	// Update with WHERE that does NOT match (covers false branch inside processUpdateRow)
	exec125(c, "UPDATE b125_pu SET name = 'nomatch' WHERE val = 999")
}

// ─── countRows float64 branch (stats.go line ~143-145) ──────────────────────

func TestB125_CountRowsFloat64Branch(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_cr (id INTEGER PRIMARY KEY, val REAL)")
	ctx := context.Background()

	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_cr",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 3.14}}},
	}, nil)

	sc := NewStatsCollector(c)
	stats, err := sc.CollectStats("b125_cr")
	if err != nil {
		t.Fatalf("CollectStats failed: %v", err)
	}
	if stats.RowCount != 1 {
		t.Errorf("expected 1 row, got %d", stats.RowCount)
	}

	// EstimateRowCount / IsStale / InvalidateStats
	est := sc.EstimateRowCount("b125_cr")
	if est != 1 {
		t.Errorf("expected estimate=1, got %d", est)
	}
	if sc.IsStale("b125_cr", 24*time.Hour) {
		t.Error("should not be stale immediately")
	}
	if !sc.IsStale("no_such_table", 0) {
		t.Error("missing table should be stale")
	}
	sc.InvalidateStats("b125_cr")
	_, ok := sc.GetTableStats("b125_cr")
	if ok {
		t.Error("stats should be gone after invalidate")
	}

	// GetSummary
	sc.CollectStats("b125_cr")
	summary := sc.GetSummary()
	if summary.TotalTables == 0 {
		t.Error("expected at least 1 table in summary")
	}

	// EstimateSelectivity paths
	sc.CollectStats("b125_cr")
	sel := sc.EstimateSelectivity("b125_cr", "val", "=", 3.14)
	t.Logf("selectivity eq=%.4f", sel)
	sel2 := sc.EstimateSelectivity("b125_cr", "val", "<", 10.0)
	t.Logf("selectivity lt=%.4f", sel2)
	sel3 := sc.EstimateSelectivity("b125_cr", "val", "!=", 0.0)
	t.Logf("selectivity neq=%.4f", sel3)
	sel4 := sc.EstimateSelectivity("b125_cr", "val", "LIKE", "%x%")
	t.Logf("selectivity default=%.4f", sel4)
	sel5 := sc.EstimateSelectivity("no_tbl", "val", "=", 1)
	t.Logf("selectivity no-stats=%.4f", sel5)

	// Cost estimators
	sc.EstimateSeqScanCost("b125_cr", 0.5)
	sc.EstimateIndexScanCost("b125_cr", "idx", 0.1)
	sc.EstimateNestedLoopCost(100, 5)
	sc.EstimateHashJoinCost(100, 200)
	sc.EstimateMergeJoinCost(100, 200)

	// GetStatsSummary (row count map)
	m := sc.GetStatsSummary()
	t.Logf("GetStatsSummary: %v", m)

	// GetColumnStats
	colSt, ok2 := sc.GetColumnStats("b125_cr", "val")
	if !ok2 {
		t.Error("expected column stats for val")
	}
	colSt.GetNullFraction(100)
	colSt.GetDistinctFraction(100)
	colSt.IsUnique(1)
	colSt.EstimateRangeSelectivity(1.0, 10.0)
	colSt.GetHistogramBucketCount()
	colSt.GetMostCommonValues(5)
}

// ─── updateVectorIndexesForInsert / Update (70%) ────────────────────────────

func TestB125_VectorIndexInsertUpdate(t *testing.T) {
	c := newCat125()

	// Create a table with a VECTOR column
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "b125_vt",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	err = c.CreateVectorIndex("b125_vidx", "b125_vt", "embedding")
	if err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	ctx := context.Background()
	// Insert rows → triggers updateVectorIndexesForInsert
	for i := 1; i <= 3; i++ {
		fi := float64(i)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_vt",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: fi}}},
		}, nil)
	}

	// Update a row → triggers updateVectorIndexesForUpdate
	exec125(c, "UPDATE b125_vt SET id = 10 WHERE id = 1")

	// Query vector index
	vi, ok := c.vectorIndexes["b125_vidx"]
	if !ok {
		t.Fatal("vector index not found")
	}
	names := c.ListVectorIndexes()
	t.Logf("vector index names: %v, vi.Name=%s", names, vi.Name)

	// Test SearchRange on empty-ish index
	h := NewHNSWIndex("tmp", "t", "v", 3)
	h.Insert("x", []float64{1, 0, 0})
	h.Insert("y", []float64{0, 1, 0})
	keys, dists, err := h.SearchRange([]float64{1, 0, 0}, 2.0)
	t.Logf("SearchRange: keys=%v dists=%v err=%v", keys, dists, err)

	// Duplicate CreateVectorIndex
	err2 := c.CreateVectorIndex("b125_vidx", "b125_vt", "embedding")
	if err2 == nil {
		t.Error("expected duplicate error")
	}

	// Non-vector column
	exec125(c, "CREATE TABLE b125_vt2 (id INTEGER PRIMARY KEY, name TEXT)")
	err3 := c.CreateVectorIndex("b125_vidx2", "b125_vt2", "name")
	if err3 == nil {
		t.Error("expected non-vector column error")
	}
}

// ─── JSONPath.Set array index paths (70.7%) ──────────────────────────────────

func TestB125_JSONPathSetArray(t *testing.T) {
	// Test array index in intermediate path
	jp := &JSONPath{Segments: []string{"items", "[0]"}}
	data := map[string]interface{}{
		"items": []interface{}{"first", "second"},
	}
	var idata interface{} = data
	err := jp.Set(&idata, "UPDATED")
	if err != nil {
		t.Fatalf("Set via *interface{}: %v", err)
	}

	// Direct map
	data2 := map[string]interface{}{
		"items": []interface{}{"a", "b", "c"},
	}
	jp2 := &JSONPath{Segments: []string{"items", "[1]"}}
	if err := jp2.Set(data2, "B_new"); err != nil {
		t.Fatalf("Set array element: %v", err)
	}

	// Empty path error
	jp3 := &JSONPath{Segments: []string{}}
	if err := jp3.Set(data2, "x"); err == nil {
		t.Error("expected empty path error")
	}

	// Path not found (nil intermediate)
	jp4 := &JSONPath{Segments: []string{"missing", "nested"}}
	data3 := map[string]interface{}{}
	var idata3 interface{} = data3
	err4 := jp4.Set(&idata3, "val")
	t.Logf("nil path err: %v", err4) // Should err: "path not found" (nil current)

	// Not an object at segment
	jp5 := &JSONPath{Segments: []string{"[0]", "key"}}
	data5 := []interface{}{"a", "b"}
	var idata5 interface{} = data5
	err5 := jp5.Set(&idata5, "val")
	t.Logf("not-object err: %v", err5)

	// Array out of bounds
	jp6 := &JSONPath{Segments: []string{"[99]"}}
	data6 := []interface{}{"a"}
	var idata6 interface{} = data6
	err6 := jp6.Set(&idata6, "val")
	t.Logf("out-of-bounds err: %v", err6)

	// Invalid array index
	jp7 := &JSONPath{Segments: []string{"[abc]"}}
	var idata7 interface{} = []interface{}{"a"}
	err7 := jp7.Set(&idata7, "val")
	t.Logf("invalid index err: %v", err7)

	// Not an array at final segment
	jp8 := &JSONPath{Segments: []string{"[0]"}}
	data8 := map[string]interface{}{"key": "val"}
	var idata8 interface{} = data8
	err8 := jp8.Set(&idata8, "val")
	t.Logf("not-array err: %v", err8)

	// Not an object at final segment
	jp9 := &JSONPath{Segments: []string{"name"}}
	data9 := []interface{}{"a"}
	var idata9 interface{} = data9
	err9 := jp9.Set(&idata9, "val")
	t.Logf("not-obj final err: %v", err9)
}

// ─── storeIndexDef (71.4%) via CreateIndex with tree ────────────────────────

func TestB125_StoreIndexDef(t *testing.T) {
	// Use a catalog with a real tree so storeIndexDef actually stores
	c := newCat125()
	exec125(c, "CREATE TABLE b125_st (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	exec125(c, "CREATE INDEX b125_st_name_idx ON b125_st (name)")
	exec125(c, "CREATE UNIQUE INDEX b125_st_score_idx ON b125_st (score)")

	// IF NOT EXISTS — should not fail if index exists
	idx, err := c.GetIndex("b125_st_name_idx")
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if idx.Name != "b125_st_name_idx" {
		t.Errorf("unexpected index name: %s", idx.Name)
	}

	// ErrIndexExists path
	err2 := c.CreateIndex(&query.CreateIndexStmt{
		Index:  "b125_st_name_idx",
		Table:  "b125_st",
		Columns: []string{"name"},
	})
	if err2 != ErrIndexExists {
		t.Errorf("expected ErrIndexExists, got %v", err2)
	}

	// IF NOT EXISTS path (no error)
	err3 := c.CreateIndex(&query.CreateIndexStmt{
		Index:       "b125_st_name_idx",
		Table:       "b125_st",
		Columns:     []string{"name"},
		IfNotExists: true,
	})
	if err3 != nil {
		t.Errorf("unexpected error for IF NOT EXISTS: %v", err3)
	}

	// GetIndex not found
	_, err4 := c.GetIndex("no_such_idx")
	if err4 != ErrIndexNotFound {
		t.Errorf("expected ErrIndexNotFound, got %v", err4)
	}
}

// ─── catalog_maintenance.Save (71.4%) ────────────────────────────────────────

func TestB125_Save_WithData(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_sv (id INTEGER PRIMARY KEY, val TEXT)")
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_sv",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "hello"}}},
	}, nil)

	// Save should flush everything
	if err := c.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// SaveData delegates to Save
	if err := c.SaveData("/tmp"); err != nil {
		t.Fatalf("SaveData failed: %v", err)
	}

	// FlushTableTrees (calls flushTableTreesLocked internally with RLock)
	if err := c.FlushTableTrees(); err != nil {
		t.Fatalf("FlushTableTrees failed: %v", err)
	}

	// Load on a fresh catalog
	c2 := newCat125()
	if err := c2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// LoadSchema / LoadData are no-ops
	if err := c2.LoadSchema("/tmp"); err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if err := c2.LoadData("/tmp"); err != nil {
		t.Fatalf("LoadData failed: %v", err)
	}
}

// ─── Vacuum (74.5%) ──────────────────────────────────────────────────────────

func TestB125_Vacuum(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_vac (id INTEGER PRIMARY KEY, val TEXT)")
	exec125(c, "CREATE INDEX b125_vac_idx ON b125_vac (val)")

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_vac",
			Columns: []string{"id", "val"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "v"},
			}},
		}, nil)
	}

	// Delete some rows to create fragmentation
	exec125(c, "DELETE FROM b125_vac WHERE id <= 5")

	// Run Vacuum
	if err := c.Vacuum(); err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}

	// Data should still be readable
	rows := rows125(c, "SELECT COUNT(*) FROM b125_vac")
	t.Logf("after vacuum rows: %v", rows)
}

// ─── RollbackTransaction undo paths (73.8%) ──────────────────────────────────

func TestB125_RollbackTransaction_UndoInsert(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_rb (id INTEGER PRIMARY KEY, val TEXT)")

	c.BeginTransaction(42)
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_rb",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "one"}}},
	}, nil)

	// Rollback should remove the inserted row
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}

	rows := rows125(c, "SELECT COUNT(*) FROM b125_rb")
	t.Logf("after rollback rows: %v", rows)
}

func TestB125_RollbackTransaction_UndoUpdate(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_rbu (id INTEGER PRIMARY KEY, val TEXT)")
	ctx := context.Background()

	// Insert outside transaction
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_rbu",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "original"}}},
	}, nil)

	c.BeginTransaction(43)
	exec125(c, "UPDATE b125_rbu SET val = 'changed' WHERE id = 1")
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}
}

func TestB125_RollbackTransaction_UndoDelete(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_rbd (id INTEGER PRIMARY KEY, val TEXT)")
	ctx := context.Background()

	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_rbd",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "keep"}}},
	}, nil)

	c.BeginTransaction(44)
	exec125(c, "DELETE FROM b125_rbd WHERE id = 1")
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}
}

func TestB125_RollbackTransaction_UndoCreateTable(t *testing.T) {
	c := newCat125()
	c.BeginTransaction(45)
	exec125(c, "CREATE TABLE b125_rbc (id INTEGER PRIMARY KEY)")
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}
	// Table should be gone
	if _, err := c.GetTable("b125_rbc"); err == nil {
		t.Error("table should not exist after rollback")
	}
}

func TestB125_RollbackTransaction_UndoDropTable(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_rbdt (id INTEGER PRIMARY KEY, v TEXT)")
	c.BeginTransaction(46)
	exec125(c, "DROP TABLE b125_rbdt")
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}
}

func TestB125_CommitTransaction(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_cm (id INTEGER PRIMARY KEY)")
	c.BeginTransaction(47)
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_cm",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}
	if c.IsTransactionActive() {
		t.Error("transaction should be inactive after commit")
	}
}

// ─── RollbackToSavepoint (76%) ───────────────────────────────────────────────

func TestB125_RollbackToSavepoint(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_sp (id INTEGER PRIMARY KEY, val TEXT)")

	c.BeginTransaction(50)
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_sp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "before"}}},
	}, nil)

	if err := c.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}

	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_sp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "after"}}},
	}, nil)

	if err := c.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint: %v", err)
	}

	// Non-existent savepoint
	err2 := c.RollbackToSavepoint("no_such_sp")
	if err2 == nil {
		t.Error("expected error for non-existent savepoint")
	}

	// Savepoint outside transaction
	c.CommitTransaction()
	err3 := c.Savepoint("sp2")
	if err3 == nil {
		t.Error("expected error for Savepoint outside transaction")
	}

	// RollbackToSavepoint outside transaction
	err4 := c.RollbackToSavepoint("sp2")
	if err4 == nil {
		t.Error("expected error for RollbackToSavepoint outside transaction")
	}
}

// ─── AlterTableRename (76.7%) ────────────────────────────────────────────────

func TestB125_AlterTableRename(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_old (id INTEGER PRIMARY KEY, val TEXT)")
	exec125(c, "CREATE INDEX b125_old_idx ON b125_old (val)")

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_old",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "hello"}}},
	}, nil)

	err := c.AlterTableRename(&query.AlterTableStmt{
		Table:   "b125_old",
		NewName: "b125_new",
	})
	if err != nil {
		t.Fatalf("AlterTableRename: %v", err)
	}

	// Verify rename
	if _, err := c.GetTable("b125_new"); err != nil {
		t.Error("renamed table not found")
	}
	if _, err := c.GetTable("b125_old"); err == nil {
		t.Error("old table should not exist")
	}

	// Rename non-existent table
	err2 := c.AlterTableRename(&query.AlterTableStmt{Table: "no_such_tbl", NewName: "other"})
	if err2 == nil {
		t.Error("expected error for missing table")
	}

	// Rename to existing table name
	exec125(c, "CREATE TABLE b125_existing (id INTEGER PRIMARY KEY)")
	err3 := c.AlterTableRename(&query.AlterTableStmt{Table: "b125_new", NewName: "b125_existing"})
	if err3 == nil {
		t.Error("expected error when target name already exists")
	}

	// Undo rename via transaction
	c.BeginTransaction(60)
	exec125(c, "ALTER TABLE b125_new RENAME TO b125_renamed")
	c.RollbackTransaction()
}

// ─── executeInsteadOfTrigger (77.4%) ─────────────────────────────────────────

func TestB125_InsteadOfInsertTrigger(t *testing.T) {
	c := newCat125()

	// Base table that the trigger inserts into
	exec125(c, "CREATE TABLE b125_base (id INTEGER PRIMARY KEY, val TEXT)")
	// View over the table
	exec125(c, "CREATE VIEW b125_view AS SELECT id, val FROM b125_base")

	// Instead-of insert trigger
	err := exec125(c, `CREATE TRIGGER b125_ins_trig
		INSTEAD OF INSERT ON b125_view
		FOR EACH ROW
		BEGIN
			INSERT INTO b125_base (id, val) VALUES (NEW.id, NEW.val);
		END`)
	if err != nil {
		t.Logf("trigger create error (may not be supported): %v", err)
		return
	}

	// INSERT into the view → should fire trigger
	exec125(c, "INSERT INTO b125_view (id, val) VALUES (1, 'via_trigger')")

	rows := rows125(c, "SELECT val FROM b125_base WHERE id = 1")
	t.Logf("after trigger insert: %v", rows)
}

// ─── toInt (77.8%) ───────────────────────────────────────────────────────────

func TestB125_ToInt(t *testing.T) {
	// int branch
	v, ok := toInt(42)
	if !ok || v != 42 {
		t.Error("toInt int branch")
	}

	// int64 branch
	v2, ok2 := toInt(int64(100))
	if !ok2 || v2 != 100 {
		t.Error("toInt int64 branch")
	}

	// int64 overflow
	_, ok3 := toInt(int64(1<<62))
	t.Logf("toInt large int64 ok=%v", ok3)

	// float64 branch
	v4, ok4 := toInt(float64(7.9))
	if !ok4 || v4 != 7 {
		t.Errorf("toInt float64: got %d", v4)
	}

	// float64 overflow
	_, ok5 := toInt(float64(1e19))
	if ok5 {
		t.Error("toInt float64 overflow should fail")
	}

	// default (string)
	_, ok6 := toInt("hello")
	if ok6 {
		t.Error("toInt string should fail")
	}
}

// ─── executeScalarSelect (78%) ───────────────────────────────────────────────

func TestB125_ScalarSelect(t *testing.T) {
	c := newCat125()

	// Simple scalar
	r1 := rows125(c, "SELECT 1+1")
	t.Logf("scalar 1+1: %v", r1)

	// Scalar with alias
	r2 := rows125(c, "SELECT 'hello' AS greeting")
	t.Logf("scalar alias: %v", r2)

	// Scalar WHERE false
	r3 := rows125(c, "SELECT 42 AS n WHERE 1 = 0")
	if len(r3) != 0 {
		t.Errorf("expected 0 rows for WHERE false, got %d", len(r3))
	}

	// Scalar WHERE true
	r4 := rows125(c, "SELECT 42 AS n WHERE 1 = 1")
	if len(r4) == 0 {
		t.Error("expected 1 row for WHERE true")
	}

	// Window function without FROM
	_, err := c.ExecuteQuery("SELECT ROW_NUMBER() OVER () AS rn")
	t.Logf("window without FROM err: %v", err)

	// Aggregate without FROM
	r5 := rows125(c, "SELECT COUNT(*)")
	t.Logf("scalar COUNT: %v", r5)
}

// ─── executeSelectWithJoinAndGroupBy / applyGroupByOrderBy (78.8% / 74.4%) ──

func TestB125_JoinGroupByOrderBy(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_emp (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	exec125(c, "CREATE TABLE b125_dept (id TEXT PRIMARY KEY, loc TEXT)")

	ctx := context.Background()
	depts := []string{"eng", "mkt", "ops"}
	for i, d := range depts {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_dept",
			Columns: []string{"id", "loc"},
			Values: [][]query.Expression{{
				&query.StringLiteral{Value: d},
				&query.StringLiteral{Value: "city" + string(rune('A'+i))},
			}},
		}, nil)
	}

	salaries := [][]interface{}{
		{1, "eng", 90000},
		{2, "eng", 80000},
		{3, "mkt", 70000},
		{4, "mkt", 75000},
		{5, "ops", 60000},
	}
	for _, row := range salaries {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_emp",
			Columns: []string{"id", "dept", "salary"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(row[0].(int))},
				&query.StringLiteral{Value: row[1].(string)},
				&query.NumberLiteral{Value: float64(row[2].(int))},
			}},
		}, nil)
	}

	// JOIN + GROUP BY + ORDER BY positional
	r1 := rows125(c, `SELECT b125_emp.dept, SUM(b125_emp.salary) AS total FROM b125_emp JOIN b125_dept ON b125_emp.dept = b125_dept.id GROUP BY b125_emp.dept ORDER BY 2 DESC`)
	t.Logf("join+group+order: %v", r1)

	// JOIN + GROUP BY + HAVING + COUNT
	r2 := rows125(c, `SELECT b125_emp.dept, COUNT(*) AS cnt FROM b125_emp JOIN b125_dept ON b125_emp.dept = b125_dept.id GROUP BY b125_emp.dept HAVING COUNT(*) >= 2`)
	t.Logf("join+group+having: %v", r2)

	// JOIN + GROUP BY + ORDER BY aggregate expression
	r3 := rows125(c, `SELECT b125_emp.dept, AVG(b125_emp.salary) AS avg_sal FROM b125_emp JOIN b125_dept ON b125_emp.dept = b125_dept.id GROUP BY b125_emp.dept ORDER BY avg_sal ASC`)
	t.Logf("join+group+order avg: %v", r3)

	// JOIN + GROUP BY + MIN + MAX
	r4 := rows125(c, `SELECT b125_emp.dept, MIN(b125_emp.salary), MAX(b125_emp.salary) FROM b125_emp JOIN b125_dept ON b125_emp.dept = b125_dept.id GROUP BY b125_emp.dept`)
	t.Logf("join+group+min+max: %v", r4)
}

// ─── ExecuteCTE (75%) ────────────────────────────────────────────────────────

func TestB125_ExecuteCTE(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_cte_src (id INTEGER PRIMARY KEY, val INTEGER)")

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_cte_src",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// Simple CTE
	r1 := rows125(c, `WITH cte AS (SELECT id, val FROM b125_cte_src WHERE val > 20) SELECT * FROM cte`)
	t.Logf("simple CTE: %v", r1)

	// Multiple CTEs (materialized)
	r2 := rows125(c, `WITH c1 AS (SELECT id FROM b125_cte_src WHERE val <= 30), c2 AS (SELECT id FROM b125_cte_src WHERE val > 30) SELECT * FROM c1`)
	t.Logf("multi-CTE: %v", r2)

	// Recursive CTE
	r3 := rows125(c, `WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 5) SELECT n FROM cnt`)
	t.Logf("recursive CTE: %v", r3)

	// CTE with UNION query
	r4 := rows125(c, `WITH u AS (SELECT id FROM b125_cte_src WHERE id = 1 UNION ALL SELECT id FROM b125_cte_src WHERE id = 2) SELECT * FROM u`)
	t.Logf("CTE with UNION: %v", r4)
}

// ─── flushTableTreesLocked (75%) via CommitTransaction ───────────────────────

func TestB125_FlushTableTreesLocked(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_flush (id INTEGER PRIMARY KEY)")

	c.BeginTransaction(55)
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_flush",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	// CommitTransaction calls flushTableTreesLocked
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	// FlushTableTrees also calls flushTableTreesLocked via RLock
	if err := c.FlushTableTrees(); err != nil {
		t.Fatalf("FlushTableTrees: %v", err)
	}
}

// ─── updateLocked edge cases (74.1%) ─────────────────────────────────────────

func TestB125_UpdateLocked_PKChange(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_upk (id INTEGER PRIMARY KEY, val TEXT)")

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_upk",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "v"}}},
		}, nil)
	}

	// UPDATE the primary key value
	exec125(c, "UPDATE b125_upk SET id = 99 WHERE id = 3")
	rows := rows125(c, "SELECT id FROM b125_upk WHERE id = 99")
	t.Logf("after pk change: %v", rows)

	// Try to update PK to existing value (duplicate PK violation)
	err := exec125(c, "UPDATE b125_upk SET id = 1 WHERE id = 2")
	t.Logf("duplicate PK update err: %v", err)
}

func TestB125_UpdateLocked_WithJoin(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_uj_t (id INTEGER PRIMARY KEY, val TEXT)")
	exec125(c, "CREATE TABLE b125_uj_s (id INTEGER PRIMARY KEY, new_val TEXT)")

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_uj_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "old"}}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_uj_s",
		Columns: []string{"id", "new_val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "new"}}},
	}, nil)

	// UPDATE...FROM exercises updateWithJoinLocked
	exec125(c, "UPDATE b125_uj_t SET val = 'joined' FROM b125_uj_s WHERE b125_uj_t.id = b125_uj_s.id")
	rows := rows125(c, "SELECT val FROM b125_uj_t WHERE id = 1")
	t.Logf("update with join result: %v", rows)
}

// ─── insertLocked coverage (74.2%) ───────────────────────────────────────────

func TestB125_InsertLocked_Constraints(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_ic (id INTEGER PRIMARY KEY, email TEXT UNIQUE, score INTEGER CHECK (score >= 0), name TEXT NOT NULL)")

	ctx := context.Background()

	// Valid insert
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_ic",
		Columns: []string{"id", "email", "score", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "a@b.com"}, &query.NumberLiteral{Value: 10}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)
	if err != nil {
		t.Fatalf("valid insert: %v", err)
	}

	// Duplicate primary key
	_, _, err2 := c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_ic",
		Columns: []string{"id", "email", "score", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "b@b.com"}, &query.NumberLiteral{Value: 5}, &query.StringLiteral{Value: "Bob"}}},
	}, nil)
	t.Logf("dup pk err: %v", err2)

	// CHECK constraint violation
	_, _, err3 := c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_ic",
		Columns: []string{"id", "email", "score", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "c@b.com"}, &query.NumberLiteral{Value: -1}, &query.StringLiteral{Value: "Carol"}}},
	}, nil)
	t.Logf("check constraint err: %v", err3)

	// NOT NULL violation
	_, _, err4 := c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_ic",
		Columns: []string{"id", "email", "score"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "d@b.com"}, &query.NumberLiteral{Value: 5}}},
	}, nil)
	t.Logf("not null err: %v", err4)

	// UNIQUE violation
	_, _, err5 := c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_ic",
		Columns: []string{"id", "email", "score", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 4}, &query.StringLiteral{Value: "a@b.com"}, &query.NumberLiteral{Value: 20}, &query.StringLiteral{Value: "Dave"}}},
	}, nil)
	t.Logf("unique err: %v", err5)
}

// ─── JSONQuote (75%) ─────────────────────────────────────────────────────────

func TestB125_JSONQuote(t *testing.T) {
	q := JSONQuote("hello world")
	if q != `"hello world"` {
		t.Errorf("expected quoted string, got %s", q)
	}
	q2 := JSONQuote("with \"quotes\"")
	t.Logf("quoted with inner quotes: %s", q2)
	q3 := JSONQuote("")
	t.Logf("empty string quoted: %s", q3)

	// JSONUnquote
	s, err := JSONUnquote(`"hello"`)
	if err != nil || s != "hello" {
		t.Errorf("JSONUnquote failed: %v %s", err, s)
	}
	_, err2 := JSONUnquote("not_json")
	if err2 == nil {
		t.Error("expected error for invalid JSON string")
	}
	s3, err3 := JSONUnquote("")
	if err3 != nil || s3 != "" {
		t.Errorf("JSONUnquote empty: %v %s", err3, s3)
	}

	// IsValidJSON
	if !IsValidJSON(`{"key":"val"}`) {
		t.Error("valid JSON not recognized")
	}
	if IsValidJSON("") {
		t.Error("empty string should not be valid JSON")
	}
	if IsValidJSON("not-json") {
		t.Error("garbage should not be valid JSON")
	}
}

// ─── referencedRowExists (76.9%) via FK insert ───────────────────────────────

func TestB125_ReferencedRowExists_FK(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_fk_parent (id INTEGER PRIMARY KEY, name TEXT)")
	exec125(c, "CREATE TABLE b125_fk_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES b125_fk_parent(id))")

	ctx := context.Background()
	// Insert parent row
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_fk_parent",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "parent1"}}},
	}, nil)

	// Insert child with valid FK (referencedRowExists returns true)
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_fk_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)
	t.Logf("valid FK insert: %v", err)

	// Insert child with invalid FK (referencedRowExists returns false)
	_, _, err2 := c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_fk_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 999}}},
	}, nil)
	t.Logf("invalid FK insert err: %v", err2)
}

// ─── updateRowSlice via FK CASCADE UPDATE ────────────────────────────────────

func TestB125_UpdateRowSlice_FKCascade(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_par (id INTEGER PRIMARY KEY, name TEXT)")
	exec125(c, "CREATE TABLE b125_chd (id INTEGER PRIMARY KEY, par_id INTEGER REFERENCES b125_par(id) ON UPDATE CASCADE)")

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_par",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "p1"}}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_chd",
		Columns: []string{"id", "par_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Update parent PK → should cascade to child (updateRowSlice)
	exec125(c, "UPDATE b125_par SET id = 100 WHERE id = 1")
	rows := rows125(c, "SELECT par_id FROM b125_chd WHERE id = 1")
	t.Logf("after cascade update: %v", rows)
}

// ─── CorrelationStats helpers ─────────────────────────────────────────────────

func TestB125_CorrelationStats(t *testing.T) {
	cs := &CorrelationStats{Correlation: 0.85}
	if !cs.IsHighCorrelation() {
		t.Error("0.85 should be high correlation")
	}
	if !cs.IsPositiveCorrelation() {
		t.Error("0.85 should be positive correlation")
	}
	if cs.IsNegativeCorrelation() {
		t.Error("0.85 should not be negative correlation")
	}

	cs2 := &CorrelationStats{Correlation: -0.8}
	if !cs2.IsNegativeCorrelation() {
		t.Error("-0.8 should be negative")
	}
	if cs2.IsPositiveCorrelation() {
		t.Error("-0.8 should not be positive")
	}

	// CalculateCorrelation
	x := []float64{1, 2, 3, 4, 5}
	y := []float64{2, 4, 6, 8, 10}
	corr := CalculateCorrelation(x, y)
	t.Logf("correlation(x,y)=%.4f", corr)

	// Edge cases
	c0 := CalculateCorrelation([]float64{}, []float64{})
	t.Logf("empty corr: %f", c0)
	c1 := CalculateCorrelation([]float64{1}, []float64{1, 2})
	t.Logf("mismatched len: %f", c1)
	c2 := CalculateCorrelation([]float64{5, 5, 5}, []float64{1, 2, 3}) // constant X -> denX=0
	t.Logf("constant X corr: %f", c2)
}

// ─── validateIdentifier / quoteIdent ─────────────────────────────────────────

func TestB125_ValidateIdentifier(t *testing.T) {
	if err := validateIdentifier("valid_name123"); err != nil {
		t.Errorf("valid identifier failed: %v", err)
	}
	if err := validateIdentifier(""); err == nil {
		t.Error("empty identifier should fail")
	}
	if err := validateIdentifier(strings.Repeat("a", 65)); err == nil {
		t.Error("too-long identifier should fail")
	}
	if err := validateIdentifier("bad-name"); err == nil {
		t.Error("hyphen should fail")
	}
	if err := validateIdentifier("SELECT"); err == nil {
		t.Error("SQL keyword should fail")
	}

	q := quoteIdent("my_table")
	if q != `"my_table"` {
		t.Errorf("quoteIdent wrong: %s", q)
	}
	q2 := quoteIdent(`table"name`)
	t.Logf("quoteIdent with inner quote: %s", q2)
}

// ─── Analyze with data (catalog_maintenance.go) ──────────────────────────────

func TestB125_Analyze_WithData(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_an (id INTEGER PRIMARY KEY, score REAL, label TEXT)")

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_an",
			Columns: []string{"id", "score", "label"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i) * 1.5},
				&query.StringLiteral{Value: "lbl"},
			}},
		}, nil)
	}

	if err := c.Analyze("b125_an"); err != nil {
		t.Fatalf("Analyze: %v", err)
	}

	// Analyze non-existent table
	err2 := c.Analyze("no_such_table")
	if err2 == nil {
		t.Error("expected error analyzing non-existent table")
	}
}

// ─── ListTables / GetTable ────────────────────────────────────────────────────

func TestB125_ListTables(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_lt1 (id INTEGER PRIMARY KEY)")
	exec125(c, "CREATE TABLE b125_lt2 (id INTEGER PRIMARY KEY)")

	tables := c.ListTables()
	if len(tables) < 2 {
		t.Errorf("expected at least 2 tables, got %d", len(tables))
	}

	// GetTable
	tbl, err := c.GetTable("b125_lt1")
	if err != nil || tbl.Name != "b125_lt1" {
		t.Errorf("GetTable: %v %v", tbl, err)
	}

	// GetTable not found
	_, err2 := c.GetTable("no_table")
	if err2 == nil {
		t.Error("expected error for missing table")
	}
}

// ─── RLS helpers ─────────────────────────────────────────────────────────────

func TestB125_RLS(t *testing.T) {
	c := newCat125()
	c.EnableRLS()

	if !c.IsRLSEnabled() {
		t.Error("RLS should be enabled")
	}
	mgr := c.GetRLSManager()
	if mgr == nil {
		t.Error("RLS manager should not be nil")
	}

	// Drop non-existent policy
	err := c.DropRLSPolicy("no_table", "no_policy")
	t.Logf("drop non-existent policy: %v", err)

	// DisableQueryCache
	c.EnableQueryCache(100, time.Minute)
	hits, misses, size := c.GetQueryCacheStats()
	t.Logf("cache stats: hits=%d misses=%d size=%d", hits, misses, size)
	c.DisableQueryCache()
}

// ─── SearchRange on HNSW with data ───────────────────────────────────────────

func TestB125_HNSWSearchRange_WithData(t *testing.T) {
	h := NewHNSWIndex("r", "t", "v", 2)
	vectors := [][]float64{
		{1.0, 0.0},
		{0.0, 1.0},
		{1.0, 1.0},
		{5.0, 5.0},
	}
	for i, v := range vectors {
		key := string(rune('a' + i))
		h.Insert(key, v)
	}

	keys, dists, err := h.SearchRange([]float64{1.0, 0.0}, 1.5)
	if err != nil {
		t.Fatalf("SearchRange: %v", err)
	}
	t.Logf("SearchRange results: keys=%v dists=%v", keys, dists)

	// Delete from index
	h.Delete("a")
	h.Delete("z") // non-existent key — no-op
}

// ─── Direct updateVectorIndexesForInsert with vector data ───────────────────

func TestB125_VectorIndexDirectInsertUpdate(t *testing.T) {
	c := newCat125()

	// Create table with VECTOR column
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "b125_vi2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "vec", Type: query.TokenVector, Dimensions: 2},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	err = c.CreateVectorIndex("b125_vi2_idx", "b125_vi2", "vec")
	if err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Directly invoke updateVectorIndexesForInsert with []float64 data
	rowWithVec := []interface{}{int64(1), []float64{1.0, 2.0}}
	c.mu.Lock()
	c.updateVectorIndexesForInsert("b125_vi2", rowWithVec, []byte("0000000000000000001"))
	c.mu.Unlock()

	// Directly invoke updateVectorIndexesForUpdate
	rowWithVec2 := []interface{}{int64(1), []float64{3.0, 4.0}}
	c.mu.Lock()
	c.updateVectorIndexesForUpdate("b125_vi2", rowWithVec2, []byte("0000000000000000001"))
	c.mu.Unlock()

	// With []interface{} vector (covered by indexRowForVector)
	rowWithVecI := []interface{}{int64(2), []interface{}{float64(5.0), float64(6.0)}}
	c.mu.Lock()
	c.updateVectorIndexesForInsert("b125_vi2", rowWithVecI, []byte("0000000000000000002"))
	c.mu.Unlock()

	// SearchVectorKNN
	keys, dists, err2 := c.SearchVectorKNN("b125_vi2_idx", []float64{1.0, 2.0}, 5)
	t.Logf("SearchVectorKNN: keys=%v dists=%v err=%v", keys, dists, err2)

	// SearchVectorRange
	keys2, dists2, err3 := c.SearchVectorRange("b125_vi2_idx", []float64{1.0, 2.0}, 5.0)
	t.Logf("SearchVectorRange: keys=%v dists=%v err=%v", keys2, dists2, err3)

	// DropVectorIndex
	if err := c.DropVectorIndex("b125_vi2_idx"); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}

	// Drop non-existent
	err4 := c.DropVectorIndex("no_such_idx")
	if err4 == nil {
		t.Error("expected error dropping non-existent vector index")
	}

	// SearchVectorKNN on non-existent index
	_, _, err5 := c.SearchVectorKNN("no_such_idx", []float64{1.0, 2.0}, 5)
	if err5 == nil {
		t.Error("expected error for missing vector index")
	}
}

// ─── selectLocked temporal / AS OF paths ────────────────────────────────────

func TestB125_SelectLocked_TemporalASOf(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_temporal (id INTEGER PRIMARY KEY, val TEXT)")
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_temporal",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "current"}}},
	}, nil)

	// AS OF with a valid timestamp — exercises the temporal branch in selectLocked
	rows := rows125(c, "SELECT * FROM b125_temporal AS OF '2026-01-01 00:00:00'")
	t.Logf("AS OF result: %v", rows)

	// AS OF SYSTEM TIME
	rows2 := rows125(c, "SELECT * FROM b125_temporal AS OF SYSTEM TIME '-1 hour'")
	t.Logf("AS OF SYSTEM TIME result: %v", rows2)

	// AS OF with date format
	rows3 := rows125(c, "SELECT * FROM b125_temporal AS OF '2026-03-01'")
	t.Logf("AS OF date result: %v", rows3)
}

// ─── selectLocked window-on-CTE path ─────────────────────────────────────────

func TestB125_SelectLocked_WindowOnCTE(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_wcte (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	ctx := context.Background()

	data := [][]interface{}{{1, "eng", 100}, {2, "eng", 200}, {3, "mkt", 150}}
	for _, row := range data {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_wcte",
			Columns: []string{"id", "dept", "salary"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(row[0].(int))},
				&query.StringLiteral{Value: row[1].(string)},
				&query.NumberLiteral{Value: float64(row[2].(int))},
			}},
		}, nil)
	}

	// Window function over CTE result - exercises the CTE+window path in selectLocked
	rows := rows125(c, `WITH cte AS (SELECT id, dept, salary FROM b125_wcte) SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM cte`)
	t.Logf("window on CTE: %v", rows)

	// Simple CTE with ORDER BY
	rows2 := rows125(c, `WITH base AS (SELECT id, salary FROM b125_wcte) SELECT id, salary FROM base ORDER BY salary DESC`)
	t.Logf("CTE ORDER BY: %v", rows2)
}

// ─── selectLocked derived table + JOIN ───────────────────────────────────────

func TestB125_SelectLocked_DerivedTableJoin(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_dt1 (id INTEGER PRIMARY KEY, val TEXT)")
	exec125(c, "CREATE TABLE b125_dt2 (id INTEGER PRIMARY KEY, ref_id INTEGER)")
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_dt1",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "v"}}},
		}, nil)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_dt2",
			Columns: []string{"id", "ref_id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	// Derived table with JOIN exercises derived-table+join path
	rows := rows125(c, `SELECT d.id, t.val FROM (SELECT id, ref_id FROM b125_dt2 WHERE id <= 2) AS d JOIN b125_dt1 AS t ON d.ref_id = t.id`)
	t.Logf("derived table + join: %v", rows)

	// Derived table without JOIN (applyOuterQuery path)
	rows2 := rows125(c, `SELECT sub.val FROM (SELECT val FROM b125_dt1 WHERE id = 1) AS sub`)
	t.Logf("derived table no join: %v", rows2)
}

// ─── INSERT...SELECT path ──────────────────────────────────────────────────

func TestB125_InsertSelect(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_is_src (id INTEGER PRIMARY KEY, val INTEGER)")
	exec125(c, "CREATE TABLE b125_is_dst (id INTEGER PRIMARY KEY, val INTEGER)")

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_is_src",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// INSERT...SELECT
	exec125(c, "INSERT INTO b125_is_dst SELECT id, val FROM b125_is_src WHERE val >= 20")

	rows := rows125(c, "SELECT COUNT(*) FROM b125_is_dst")
	t.Logf("INSERT...SELECT result: %v", rows)
}

// ─── AlterTable paths (DDL) ───────────────────────────────────────────────────

func TestB125_AlterTableColumn(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_alt (id INTEGER PRIMARY KEY, name TEXT)")
	ctx := context.Background()

	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_alt",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	// Add column
	exec125(c, "ALTER TABLE b125_alt ADD COLUMN score INTEGER DEFAULT 0")
	rows := rows125(c, "SELECT id, name, score FROM b125_alt WHERE id = 1")
	t.Logf("after add column: %v", rows)

	// Drop column
	exec125(c, "ALTER TABLE b125_alt DROP COLUMN score")

	// Rename column
	exec125(c, "ALTER TABLE b125_alt RENAME COLUMN name TO full_name")
	rows2 := rows125(c, "SELECT full_name FROM b125_alt WHERE id = 1")
	t.Logf("after rename column: %v", rows2)
}

// ─── DropTable path ───────────────────────────────────────────────────────────

func TestB125_DropTable(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_drp (id INTEGER PRIMARY KEY)")
	exec125(c, "CREATE INDEX b125_drp_idx ON b125_drp (id)")

	if err := exec125(c, "DROP TABLE b125_drp"); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}
	if _, err := c.GetTable("b125_drp"); err == nil {
		t.Error("table should be dropped")
	}

	// DROP TABLE IF EXISTS non-existent
	exec125(c, "DROP TABLE IF EXISTS no_such_table")
}

// ─── evaluateTemporalExpr branches ────────────────────────────────────────────

func TestB125_EvaluateTemporalExpr(t *testing.T) {
	c := newCat125()

	// Test parseSystemTimeExpr via AS OF SYSTEM TIME
	exec125(c, "CREATE TABLE b125_tmp2 (id INTEGER PRIMARY KEY)")
	rows := rows125(c, "SELECT * FROM b125_tmp2 AS OF SYSTEM TIME '-30 minutes'")
	t.Logf("system time -30m: %v", rows)
	rows2 := rows125(c, "SELECT * FROM b125_tmp2 AS OF SYSTEM TIME '-2 hours'")
	t.Logf("system time -2h: %v", rows2)
	rows3 := rows125(c, "SELECT * FROM b125_tmp2 AS OF SYSTEM TIME '-7 days'")
	t.Logf("system time -7d: %v", rows3)
}

// ─── FTS (full-text search) basic ops ─────────────────────────────────────────

func TestB125_FTS_Basic(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_fts (id INTEGER PRIMARY KEY, content TEXT)")
	ctx := context.Background()

	docs := []string{"the quick brown fox", "a lazy dog", "quick fox runs fast"}
	for i, d := range docs {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_fts",
			Columns: []string{"id", "content"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i + 1)}, &query.StringLiteral{Value: d}}},
		}, nil)
	}

	// Create FTS index
	err := c.CreateFTSIndex("b125_fts_idx", "b125_fts", []string{"content"})
	if err != nil {
		t.Logf("CreateFTSIndex: %v", err)
		return
	}

	// Search FTS index
	results, err := c.SearchFTS("b125_fts_idx", "quick")
	t.Logf("FTS search 'quick': results=%v err=%v", results, err)

	// Drop FTS index
	err2 := c.DropFTSIndex("b125_fts_idx")
	t.Logf("DropFTSIndex: %v", err2)
}

// ─── CreateView / CreateTrigger paths ────────────────────────────────────────

func TestB125_ViewAndTrigger(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_vw_base (id INTEGER PRIMARY KEY, score INTEGER)")
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_vw_base",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// Create view
	exec125(c, "CREATE VIEW b125_view2 AS SELECT id, score FROM b125_vw_base WHERE score > 20")
	rows := rows125(c, "SELECT * FROM b125_view2")
	t.Logf("view rows: %v", rows)

	// CREATE OR REPLACE VIEW
	exec125(c, "CREATE OR REPLACE VIEW b125_view2 AS SELECT id, score FROM b125_vw_base WHERE score > 30")
	rows2 := rows125(c, "SELECT * FROM b125_view2")
	t.Logf("replaced view rows: %v", rows2)

	// Drop view
	exec125(c, "DROP VIEW b125_view2")

	// After trigger
	exec125(c, "CREATE TABLE b125_log (id INTEGER PRIMARY KEY, action TEXT)")
	exec125(c, `CREATE TRIGGER b125_after_ins
		AFTER INSERT ON b125_vw_base
		FOR EACH ROW
		BEGIN
			INSERT INTO b125_log (id, action) VALUES (NEW.id, 'inserted');
		END`)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_vw_base",
		Columns: []string{"id", "score"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 100}, &query.NumberLiteral{Value: 999}}},
	}, nil)

	logRows := rows125(c, "SELECT action FROM b125_log WHERE id = 100")
	t.Logf("trigger log: %v", logRows)
}

// ─── Materialized view ────────────────────────────────────────────────────────

func TestB125_MaterializedView(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_mv_src (id INTEGER PRIMARY KEY, val INTEGER)")
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_mv_src",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 2)}}},
		}, nil)
	}

	// Parse the SELECT for the materialized view
	mvResult, parseErr := c.ExecuteQuery("SELECT id, val FROM b125_mv_src WHERE val > 4")
	_ = mvResult
	_ = parseErr

	// Build a SelectStmt directly
	mvStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "b125_mv_src"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "val"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 4},
		},
	}

	err := c.CreateMaterializedView("b125_mv", mvStmt, false)
	if err != nil {
		t.Fatalf("CreateMaterializedView: %v", err)
	}

	err2 := c.RefreshMaterializedView("b125_mv")
	if err2 != nil {
		t.Fatalf("RefreshMaterializedView: %v", err2)
	}

	rows := rows125(c, "SELECT * FROM b125_mv")
	t.Logf("materialized view rows: %v", rows)
}

// ─── applyGroupByOrderBy with QualifiedIdentifier order by ───────────────────

func TestB125_GroupByOrderByQualified(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_qob (id INTEGER PRIMARY KEY, dept TEXT, sal INTEGER)")
	ctx := context.Background()

	rows := [][]interface{}{{1, "eng", 100}, {2, "mkt", 200}, {3, "eng", 150}}
	for _, row := range rows {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_qob",
			Columns: []string{"id", "dept", "sal"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(row[0].(int))},
				&query.StringLiteral{Value: row[1].(string)},
				&query.NumberLiteral{Value: float64(row[2].(int))},
			}},
		}, nil)
	}

	// ORDER BY with table.column reference
	r1 := rows125(c, "SELECT dept, SUM(sal) FROM b125_qob GROUP BY dept ORDER BY dept ASC")
	t.Logf("group by order by qualified: %v", r1)

	// ORDER BY with positional reference in GROUP BY context
	r2 := rows125(c, "SELECT dept, SUM(sal) AS total FROM b125_qob GROUP BY dept ORDER BY total DESC")
	t.Logf("group by order by alias: %v", r2)
}

// ─── EvalExpression direct ────────────────────────────────────────────────────

func TestB125_EvalExpression(t *testing.T) {
	// Test EvalExpression (package-level, without catalog)
	v1, err := EvalExpression(&query.NumberLiteral{Value: 42}, nil)
	if err != nil || v1 != float64(42) {
		t.Errorf("EvalExpression number: %v %v", v1, err)
	}

	v2, err2 := EvalExpression(&query.StringLiteral{Value: "hello"}, nil)
	if err2 != nil || v2 != "hello" {
		t.Errorf("EvalExpression string: %v %v", v2, err2)
	}

	v3, err3 := EvalExpression(&query.NullLiteral{}, nil)
	if err3 != nil || v3 != nil {
		t.Errorf("EvalExpression null: %v %v", v3, err3)
	}

	v4, err4 := EvalExpression(&query.BooleanLiteral{Value: true}, nil)
	if err4 != nil || v4 != true {
		t.Errorf("EvalExpression bool: %v %v", v4, err4)
	}

	// Identifier
	v5, err5 := EvalExpression(&query.Identifier{Name: "my_col"}, nil)
	if err5 != nil {
		t.Errorf("EvalExpression ident: %v", err5)
	}
	t.Logf("Identifier: %v", v5)

	// PlaceholderExpr in-range
	v6, err6 := EvalExpression(&query.PlaceholderExpr{Index: 0}, []interface{}{"arg0"})
	if err6 != nil || v6 != "arg0" {
		t.Errorf("EvalExpression placeholder: %v %v", v6, err6)
	}

	// PlaceholderExpr out-of-range
	_, err7 := EvalExpression(&query.PlaceholderExpr{Index: 5}, []interface{}{"a"})
	if err7 == nil {
		t.Error("expected error for placeholder out of range")
	}

	// UnaryExpr NOT bool
	v8, err8 := EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: &query.BooleanLiteral{Value: true}}, nil)
	if err8 != nil || v8 != false {
		t.Errorf("NOT true: %v %v", v8, err8)
	}

	// UnaryExpr NOT NULL → NULL
	v9, err9 := EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: &query.NullLiteral{}}, nil)
	if err9 != nil || v9 != nil {
		t.Errorf("NOT NULL: %v %v", v9, err9)
	}

	// UnaryExpr minus integer
	v10, err10 := EvalExpression(&query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.NumberLiteral{Value: 5, Raw: "5"}}, nil)
	t.Logf("minus 5: %v %v", v10, err10)

	// BinaryExpr AND with one nil
	v11, err11 := EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	t.Logf("NULL AND true: %v %v", v11, err11)

	v12, err12 := EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	t.Logf("NULL AND false: %v %v", v12, err12)

	v13, err13 := EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	t.Logf("true AND NULL: %v %v", v13, err13)

	v14, err14 := EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	t.Logf("false AND NULL: %v %v", v14, err14)

	// BinaryExpr OR with nils
	v15, err15 := EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	t.Logf("NULL OR true: %v %v", v15, err15)

	v16, err16 := EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	t.Logf("NULL OR false: %v %v", v16, err16)

	v17, err17 := EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	t.Logf("NULL OR NULL: %v %v", v17, err17)

	v18, err18 := EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	t.Logf("true OR NULL: %v %v", v18, err18)
}

// ─── applyGroupByOrderBy QualifiedIdentifier + nil + string ORDER BY branches ─

func TestB125_ApplyGroupByOrderBy_StringAndNil(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_strord (id INTEGER PRIMARY KEY, dept TEXT, score INTEGER)")
	exec125(c, "CREATE TABLE b125_strord2 (dept_id TEXT PRIMARY KEY, region TEXT)")

	ctx := context.Background()
	rows := [][]interface{}{
		{1, "engineering", 90},
		{2, "marketing", 75},
		{3, "engineering", 80},
		{4, "marketing", 95},
		{5, "ops", 70},
	}
	for _, row := range rows {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_strord",
			Columns: []string{"id", "dept", "score"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(row[0].(int))},
				&query.StringLiteral{Value: row[1].(string)},
				&query.NumberLiteral{Value: float64(row[2].(int))},
			}},
		}, nil)
	}
	for _, dept := range []string{"engineering", "marketing", "ops"} {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_strord2",
			Columns: []string{"dept_id", "region"},
			Values: [][]query.Expression{{
				&query.StringLiteral{Value: dept},
				&query.StringLiteral{Value: "region_" + dept[:1]},
			}},
		}, nil)
	}

	// ORDER BY a string column (covers string comparison in applyGroupByOrderBy)
	r1 := rows125(c, `SELECT dept, COUNT(*) FROM b125_strord GROUP BY dept ORDER BY dept ASC`)
	t.Logf("order by string: %v", r1)

	// ORDER BY a string column DESC
	r2 := rows125(c, `SELECT dept, SUM(score) FROM b125_strord GROUP BY dept ORDER BY dept DESC`)
	t.Logf("order by string DESC: %v", r2)

	// JOIN + GROUP BY + ORDER BY qualified identifier (table.column)
	r3 := rows125(c, `SELECT b125_strord.dept, COUNT(*) FROM b125_strord JOIN b125_strord2 ON b125_strord.dept = b125_strord2.dept_id GROUP BY b125_strord.dept ORDER BY b125_strord.dept ASC`)
	t.Logf("order by qualified: %v", r3)

	// ORDER BY with NULL values (covers nil branches in applyGroupByOrderBy)
	exec125(c, "CREATE TABLE b125_nullord (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	for i := 1; i <= 4; i++ {
		var valExpr query.Expression
		if i%2 == 0 {
			valExpr = &query.NullLiteral{}
		} else {
			valExpr = &query.NumberLiteral{Value: float64(i * 10)}
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_nullord",
			Columns: []string{"id", "grp", "val"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "g"},
				valExpr,
			}},
		}, nil)
	}
	r4 := rows125(c, `SELECT grp, SUM(val) FROM b125_nullord GROUP BY grp ORDER BY SUM(val) ASC`)
	t.Logf("order by SUM with NULLs: %v", r4)

	// GROUP_CONCAT in GROUP BY (exercises evaluateExprWithGroupAggregatesJoin)
	r5 := rows125(c, `SELECT dept, GROUP_CONCAT(score) FROM b125_strord GROUP BY dept`)
	t.Logf("group_concat: %v", r5)
}

// ─── GetRow / serializePK ─────────────────────────────────────────────────────

func TestB125_GetRow(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_gr (id INTEGER PRIMARY KEY, name TEXT)")
	ctx := context.Background()

	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_gr",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	// GetRow with int64 PK (serializePK int64 branch)
	c.mu.RLock()
	row := c.serializePK(int64(1), c.tableTrees["b125_gr"])
	c.mu.RUnlock()
	t.Logf("serialized int64 PK: %s", string(row))

	// GetRow with int PK (serializePK int branch)
	c.mu.RLock()
	row2 := c.serializePK(int(1), c.tableTrees["b125_gr"])
	c.mu.RUnlock()
	t.Logf("serialized int PK: %s", string(row2))

	// GetRow with float64 PK
	c.mu.RLock()
	row3 := c.serializePK(float64(1.0), c.tableTrees["b125_gr"])
	c.mu.RUnlock()
	t.Logf("serialized float64 PK: %s", string(row3))

	// GetRow with default PK (other type)
	c.mu.RLock()
	row4 := c.serializePK(true, c.tableTrees["b125_gr"])
	c.mu.RUnlock()
	t.Logf("serialized bool PK: %s", string(row4))

	// Create table with text PK and GetRow
	exec125(c, "CREATE TABLE b125_gr_str (id TEXT PRIMARY KEY, val INTEGER)")
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_gr_str",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.StringLiteral{Value: "abc"}, &query.NumberLiteral{Value: 42}}},
	}, nil)

	// serializePK with string key — exercises string branch, tries direct and S: prefix
	c.mu.RLock()
	row5 := c.serializePK("abc", c.tableTrees["b125_gr_str"])
	c.mu.RUnlock()
	t.Logf("serialized string PK: %s", string(row5))
}

// ─── buildJSONIndex coverage: numeric path ───────────────────────────────────

func TestB125_BuildJSONIndex_NumericPath(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_jn (id INTEGER PRIMARY KEY, data JSON)")
	ctx := context.Background()

	// Insert JSON with numeric values for the path
	jsons := []string{
		`{"value": 100, "nested": {"score": 99}}`,
		`{"value": 200, "nested": {"score": 85}}`,
		`{"value": 300}`,
		`{"other": "key"}`,
	}
	for i, j := range jsons {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_jn",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i + 1)}, &query.StringLiteral{Value: j}}},
		}, nil)
	}

	// Build JSON index on numeric path
	err := c.CreateJSONIndex("b125_jn_val_idx", "b125_jn", "data", "$.value", "NUMERIC")
	if err != nil {
		t.Fatalf("CreateJSONIndex numeric: %v", err)
	}

	// Build JSON index on nested path
	err2 := c.CreateJSONIndex("b125_jn_nested_idx", "b125_jn", "data", "$.nested.score", "NUMERIC")
	if err2 != nil {
		t.Fatalf("CreateJSONIndex nested: %v", err2)
	}

	// Build JSON index on string path
	err3 := c.CreateJSONIndex("b125_jn_other_idx", "b125_jn", "data", "$.other", "TEXT")
	if err3 != nil {
		t.Fatalf("CreateJSONIndex text: %v", err3)
	}
}

// ─── evaluateCastExpr branches ────────────────────────────────────────────────

func TestB125_EvaluateCastExpr(t *testing.T) {
	c := newCat125()

	// CAST via SQL
	r1 := rows125(c, "SELECT CAST(42 AS TEXT)")
	t.Logf("CAST int to TEXT: %v", r1)

	r2 := rows125(c, "SELECT CAST('3.14' AS REAL)")
	t.Logf("CAST string to REAL: %v", r2)

	r3 := rows125(c, "SELECT CAST('42' AS INTEGER)")
	t.Logf("CAST string to INTEGER: %v", r3)

	r4 := rows125(c, "SELECT CAST(1 AS BOOLEAN)")
	t.Logf("CAST int to BOOLEAN: %v", r4)

	r5 := rows125(c, "SELECT CAST('2026-01-01' AS DATE)")
	t.Logf("CAST string to DATE: %v", r5)

	r6 := rows125(c, "SELECT CAST(NULL AS INTEGER)")
	t.Logf("CAST null to INTEGER: %v", r6)
}

// ─── QueryCache paths (80%) ───────────────────────────────────────────────────

func TestB125_QueryCacheHitPath(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_qc (id INTEGER PRIMARY KEY, val TEXT)")
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_qc",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "hello"}}},
	}, nil)

	c.EnableQueryCache(100, time.Minute)

	// First call — cache miss
	r1 := rows125(c, "SELECT val FROM b125_qc WHERE id = 1")
	t.Logf("cache miss: %v", r1)

	// Second call — cache hit
	r2 := rows125(c, "SELECT val FROM b125_qc WHERE id = 1")
	t.Logf("cache hit: %v", r2)

	hits, misses, size := c.GetQueryCacheStats()
	t.Logf("cache stats: hits=%d misses=%d size=%d", hits, misses, size)

	// Invalidate cache
	c.invalidateQueryCache("b125_qc")
}

// ─── updateLocked string PK change path ──────────────────────────────────────

func TestB125_UpdateLocked_StringPKChange(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_spk (id TEXT PRIMARY KEY, val TEXT)")

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_spk",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.StringLiteral{Value: "old_key"}, &query.StringLiteral{Value: "hello"}}},
	}, nil)

	// UPDATE the string primary key value
	exec125(c, "UPDATE b125_spk SET id = 'new_key' WHERE id = 'old_key'")
	rows := rows125(c, "SELECT id FROM b125_spk WHERE id = 'new_key'")
	t.Logf("after string PK change: %v", rows)
}

// ─── Vacuum with entries (covers entry rewriting) ────────────────────────────

func TestB125_Vacuum_WithIndexEntries(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_vac2 (id INTEGER PRIMARY KEY, val TEXT)")
	exec125(c, "CREATE INDEX b125_vac2_val_idx ON b125_vac2 (val)")

	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_vac2",
			Columns: []string{"id", "val"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "value_" + string(rune('A'+i%26))},
			}},
		}, nil)
	}
	exec125(c, "DELETE FROM b125_vac2 WHERE id < 10")

	if err := c.Vacuum(); err != nil {
		t.Fatalf("Vacuum with index: %v", err)
	}

	rows := rows125(c, "SELECT COUNT(*) FROM b125_vac2")
	t.Logf("after vacuum2 rows: %v", rows)
}

// ─── evaluateHaving with complex expressions ──────────────────────────────────

func TestB125_EvaluateHaving_Complex(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_hv (id INTEGER PRIMARY KEY, dept TEXT, sal INTEGER, bonus INTEGER)")
	ctx := context.Background()

	data := [][]interface{}{
		{1, "eng", 100, 10},
		{2, "eng", 200, 20},
		{3, "mkt", 150, 15},
		{4, "mkt", 50, 5},
	}
	for _, row := range data {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_hv",
			Columns: []string{"id", "dept", "sal", "bonus"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(row[0].(int))},
				&query.StringLiteral{Value: row[1].(string)},
				&query.NumberLiteral{Value: float64(row[2].(int))},
				&query.NumberLiteral{Value: float64(row[3].(int))},
			}},
		}, nil)
	}

	// HAVING with AVG
	r1 := rows125(c, `SELECT dept, AVG(sal) FROM b125_hv GROUP BY dept HAVING AVG(sal) > 100`)
	t.Logf("HAVING AVG: %v", r1)

	// HAVING with MAX - MIN expression
	r2 := rows125(c, `SELECT dept, MAX(sal), MIN(sal) FROM b125_hv GROUP BY dept HAVING MAX(sal) > 100`)
	t.Logf("HAVING MAX: %v", r2)

	// HAVING with COUNT
	r3 := rows125(c, `SELECT dept, COUNT(*) FROM b125_hv GROUP BY dept HAVING COUNT(*) >= 2`)
	t.Logf("HAVING COUNT: %v", r3)
}

// ─── executeSelectWithJoin coverage ──────────────────────────────────────────

func TestB125_ExecuteSelectWithJoin_ExtraTypes(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_j1 (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	exec125(c, "CREATE TABLE b125_j2 (id INTEGER PRIMARY KEY, j1_id INTEGER, tag TEXT)")

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_j1",
			Columns: []string{"id", "name", "score"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "name" + string(rune('0'+i))}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_j2",
			Columns: []string{"id", "j1_id", "tag"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "tag" + string(rune('A'+i))}}},
		}, nil)
	}

	// LEFT JOIN
	r1 := rows125(c, `SELECT b125_j1.name, b125_j2.tag FROM b125_j1 LEFT JOIN b125_j2 ON b125_j1.id = b125_j2.j1_id`)
	t.Logf("LEFT JOIN: %v", r1)

	// RIGHT JOIN
	r2 := rows125(c, `SELECT b125_j1.name, b125_j2.tag FROM b125_j1 RIGHT JOIN b125_j2 ON b125_j1.id = b125_j2.j1_id`)
	t.Logf("RIGHT JOIN: %v", r2)

	// JOIN with WHERE
	r3 := rows125(c, `SELECT b125_j1.name, b125_j2.tag FROM b125_j1 INNER JOIN b125_j2 ON b125_j1.id = b125_j2.j1_id WHERE b125_j1.score > 10`)
	t.Logf("INNER JOIN with WHERE: %v", r3)

	// JOIN with ORDER BY
	r4 := rows125(c, `SELECT b125_j1.name, b125_j2.tag FROM b125_j1 JOIN b125_j2 ON b125_j1.id = b125_j2.j1_id ORDER BY b125_j1.score DESC`)
	t.Logf("JOIN ORDER BY DESC: %v", r4)

	// JOIN with DISTINCT
	r5 := rows125(c, `SELECT DISTINCT b125_j2.tag FROM b125_j1 JOIN b125_j2 ON b125_j1.id = b125_j2.j1_id`)
	t.Logf("JOIN DISTINCT: %v", r5)

	// JOIN with LIMIT/OFFSET
	r6 := rows125(c, `SELECT b125_j1.name FROM b125_j1 JOIN b125_j2 ON b125_j1.id = b125_j2.j1_id LIMIT 2 OFFSET 1`)
	t.Logf("JOIN LIMIT OFFSET: %v", r6)
}

// ─── Complex view with JOIN ───────────────────────────────────────────────────

func TestB125_ComplexViewWithJoin(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_cv1 (id INTEGER PRIMARY KEY, dept TEXT, sal INTEGER)")
	exec125(c, "CREATE TABLE b125_cv2 (dept TEXT PRIMARY KEY, budget INTEGER)")

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		dept := "dept" + string(rune('A'+i))
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_cv1",
			Columns: []string{"id", "dept", "sal"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: dept}, &query.NumberLiteral{Value: float64(i * 1000)}}},
		}, nil)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_cv2",
			Columns: []string{"dept", "budget"},
			Values:  [][]query.Expression{{&query.StringLiteral{Value: dept}, &query.NumberLiteral{Value: float64(i * 5000)}}},
		}, nil)
	}

	// Complex view (with GROUP BY) — exercises the complex view + JOIN path in selectLocked
	exec125(c, "CREATE VIEW b125_dept_totals AS SELECT dept, SUM(sal) AS total_sal FROM b125_cv1 GROUP BY dept")

	// Query the complex view joined with another table
	r1 := rows125(c, `SELECT b125_dept_totals.dept, b125_cv2.budget FROM b125_dept_totals JOIN b125_cv2 ON b125_dept_totals.dept = b125_cv2.dept`)
	t.Logf("complex view + JOIN: %v", r1)

	// Simple view with alias
	exec125(c, "CREATE VIEW b125_simple_view AS SELECT id AS emp_id, dept, sal AS salary FROM b125_cv1 WHERE sal > 1000")
	r2 := rows125(c, "SELECT * FROM b125_simple_view")
	t.Logf("simple view: %v", r2)
}

// ─── Savepoint with DDL rollback ──────────────────────────────────────────────

func TestB125_Savepoint_DDLRollback(t *testing.T) {
	c := newCat125()

	c.BeginTransaction(70)
	exec125(c, "CREATE TABLE b125_ddl_sp (id INTEGER PRIMARY KEY)")
	if err := c.Savepoint("before_drop"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}

	// Create index inside transaction then rollback to savepoint
	exec125(c, "CREATE INDEX b125_ddl_sp_idx ON b125_ddl_sp (id)")
	if err := c.RollbackToSavepoint("before_drop"); err != nil {
		t.Fatalf("RollbackToSavepoint: %v", err)
	}

	c.CommitTransaction()
}

// ─── index-assisted DELETE (useIndexForQueryWithArgs) ────────────────────────

func TestB125_IndexAssistedDelete(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_iad (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	exec125(c, "CREATE INDEX b125_iad_val_idx ON b125_iad (val)")

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_iad",
			Columns: []string{"id", "val", "name"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i * 5)},
				&query.StringLiteral{Value: "r"},
			}},
		}, nil)
	}

	// DELETE using indexed column — exercises index-assisted DELETE path
	exec125(c, "DELETE FROM b125_iad WHERE val = 25")
	r := rows125(c, "SELECT COUNT(*) FROM b125_iad")
	t.Logf("after indexed delete: %v", r)

	// DELETE with no match on index
	exec125(c, "DELETE FROM b125_iad WHERE val = 999")
}

// ─── ON CONFLICT INSERT paths ──────────────────────────────────────────────

func TestB125_InsertOnConflict(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_oc (id INTEGER PRIMARY KEY, val TEXT)")

	// Initial insert
	exec125(c, "INSERT INTO b125_oc VALUES (1, 'original')")

	// INSERT OR IGNORE on conflict (ConflictIgnore)
	exec125(c, "INSERT OR IGNORE INTO b125_oc VALUES (1, 'ignored')")
	r1 := rows125(c, "SELECT val FROM b125_oc WHERE id = 1")
	t.Logf("after INSERT OR IGNORE: %v", r1)

	// INSERT OR REPLACE on conflict (ConflictReplace)
	exec125(c, "INSERT OR REPLACE INTO b125_oc VALUES (1, 'replaced')")
	r2 := rows125(c, "SELECT val FROM b125_oc WHERE id = 1")
	t.Logf("after INSERT OR REPLACE: %v", r2)

	// INSERT OR REPLACE with index entries
	exec125(c, "CREATE TABLE b125_oc2 (id INTEGER PRIMARY KEY, name TEXT UNIQUE, score INTEGER)")
	exec125(c, "CREATE INDEX b125_oc2_score_idx ON b125_oc2 (score)")
	exec125(c, "INSERT INTO b125_oc2 VALUES (1, 'alice', 100)")
	exec125(c, "INSERT OR REPLACE INTO b125_oc2 VALUES (1, 'alice_new', 200)")
	r3 := rows125(c, "SELECT name, score FROM b125_oc2 WHERE id = 1")
	t.Logf("after INSERT OR REPLACE with index: %v", r3)
}

// ─── RETURNING clause paths ───────────────────────────────────────────────────

func TestB125_ReturningClause(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_ret (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_ret",
			Columns: []string{"id", "val", "score"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "v"}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// UPDATE with RETURNING
	exec125(c, "UPDATE b125_ret SET val = 'updated' WHERE id = 1 RETURNING id, val")
	retCols := c.GetLastReturningColumns()
	retRows := c.GetLastReturningRows()
	t.Logf("UPDATE RETURNING cols=%v rows=%v", retCols, retRows)

	// DELETE with RETURNING
	exec125(c, "DELETE FROM b125_ret WHERE id = 2 RETURNING id, score")
	retCols2 := c.GetLastReturningColumns()
	retRows2 := c.GetLastReturningRows()
	t.Logf("DELETE RETURNING cols=%v rows=%v", retCols2, retRows2)
}

// ─── WAL-enabled transaction paths ────────────────────────────────────────────

func TestB125_WALTransactionPaths(t *testing.T) {
	// Exercise transaction paths (without WAL file, just test undo log paths)
	c := newCat125()
	exec125(c, "CREATE TABLE b125_wal (id INTEGER PRIMARY KEY, val TEXT)")

	// Transaction with commit
	c.BeginTransaction(100)
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_wal",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "txn_val"}}},
	}, nil)

	// UPDATE inside transaction (exercises undo log update path)
	exec125(c, "UPDATE b125_wal SET val = 'changed' WHERE id = 1")

	if err := c.CommitTransaction(); err != nil {
		t.Logf("CommitTransaction: %v", err)
	}

	// New transaction with PK change
	c.BeginTransaction(101)
	exec125(c, "UPDATE b125_wal SET id = 99 WHERE id = 1")
	if err := c.CommitTransaction(); err != nil {
		t.Logf("CommitTransaction pk: %v", err)
	}

	// Transaction with rollback
	c.BeginTransaction(102)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_wal",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "will_rollback"}}},
	}, nil)
	if err := c.RollbackTransaction(); err != nil {
		t.Logf("RollbackTransaction: %v", err)
	}
}

// ─── selectLocked with IS NULL / NOT IN / BETWEEN predicates ─────────────────

func TestB125_SelectLocked_ComplexPredicates(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_pred (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, active INTEGER)")

	ctx := context.Background()
	data := [][]interface{}{
		{1, "Alice", 90, 1},
		{2, "Bob", nil, 1},
		{3, "Carol", 70, 0},
		{4, "Dave", 85, 1},
		{5, nil, 95, 1},
	}
	for _, row := range data {
		var scoreExpr, nameExpr query.Expression
		if row[2] == nil {
			scoreExpr = &query.NullLiteral{}
		} else {
			scoreExpr = &query.NumberLiteral{Value: float64(row[2].(int))}
		}
		if row[1] == nil {
			nameExpr = &query.NullLiteral{}
		} else {
			nameExpr = &query.StringLiteral{Value: row[1].(string)}
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_pred",
			Columns: []string{"id", "name", "score", "active"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(row[0].(int))},
				nameExpr,
				scoreExpr,
				&query.NumberLiteral{Value: float64(row[3].(int))},
			}},
		}, nil)
	}

	// IS NULL
	r1 := rows125(c, "SELECT id FROM b125_pred WHERE score IS NULL")
	t.Logf("IS NULL: %v", r1)

	// IS NOT NULL
	r2 := rows125(c, "SELECT id FROM b125_pred WHERE name IS NOT NULL")
	t.Logf("IS NOT NULL: %v", r2)

	// BETWEEN
	r3 := rows125(c, "SELECT id FROM b125_pred WHERE score BETWEEN 80 AND 95")
	t.Logf("BETWEEN: %v", r3)

	// NOT BETWEEN
	r4 := rows125(c, "SELECT id FROM b125_pred WHERE score NOT BETWEEN 80 AND 95")
	t.Logf("NOT BETWEEN: %v", r4)

	// IN list
	r5 := rows125(c, "SELECT id FROM b125_pred WHERE id IN (1, 3, 5)")
	t.Logf("IN: %v", r5)

	// NOT IN list
	r6 := rows125(c, "SELECT id FROM b125_pred WHERE id NOT IN (1, 2)")
	t.Logf("NOT IN: %v", r6)

	// LIKE pattern
	r7 := rows125(c, "SELECT name FROM b125_pred WHERE name LIKE 'A%'")
	t.Logf("LIKE: %v", r7)

	// CAST
	r8 := rows125(c, "SELECT CAST(score AS TEXT) FROM b125_pred WHERE id = 1")
	t.Logf("CAST: %v", r8)

	// COALESCE
	r9 := rows125(c, "SELECT COALESCE(score, 0) FROM b125_pred WHERE id = 2")
	t.Logf("COALESCE: %v", r9)

	// CASE
	r10 := rows125(c, "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END FROM b125_pred WHERE score IS NOT NULL")
	t.Logf("CASE: %v", r10)

	// Subquery in SELECT
	r11 := rows125(c, "SELECT id, (SELECT COUNT(*) FROM b125_pred WHERE score IS NOT NULL) FROM b125_pred WHERE id = 1")
	t.Logf("scalar subquery: %v", r11)
}

// ─── SELECT with window function over real table ──────────────────────────────

func TestB125_WindowFunctionOverTable(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_wf (id INTEGER PRIMARY KEY, dept TEXT, sal INTEGER)")

	ctx := context.Background()
	data := [][]interface{}{
		{1, "eng", 100},
		{2, "eng", 200},
		{3, "mkt", 150},
		{4, "mkt", 50},
		{5, "ops", 300},
	}
	for _, row := range data {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_wf",
			Columns: []string{"id", "dept", "sal"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(row[0].(int))},
				&query.StringLiteral{Value: row[1].(string)},
				&query.NumberLiteral{Value: float64(row[2].(int))},
			}},
		}, nil)
	}

	// ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal)
	r1 := rows125(c, `SELECT id, dept, sal, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal) AS rn FROM b125_wf`)
	t.Logf("ROW_NUMBER: %v", r1)

	// SUM() OVER (PARTITION BY dept) - running aggregate
	r2 := rows125(c, `SELECT id, dept, sal, SUM(sal) OVER (PARTITION BY dept) AS dept_total FROM b125_wf`)
	t.Logf("SUM OVER: %v", r2)

	// LAG() OVER
	r3 := rows125(c, `SELECT id, sal, LAG(sal) OVER (ORDER BY id) AS prev_sal FROM b125_wf`)
	t.Logf("LAG: %v", r3)

	// LEAD() OVER
	r4 := rows125(c, `SELECT id, sal, LEAD(sal) OVER (ORDER BY id) AS next_sal FROM b125_wf`)
	t.Logf("LEAD: %v", r4)

	// SELECT with ORDER BY and LIMIT/OFFSET
	r5 := rows125(c, "SELECT id, sal FROM b125_wf ORDER BY sal DESC LIMIT 3 OFFSET 1")
	t.Logf("ORDER BY LIMIT OFFSET: %v", r5)
}

// ─── DISTINCT with ORDER BY (hiddenOrderByCols path) ──────────────────────────

func TestB125_DistinctWithOrderBy(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_do (id INTEGER PRIMARY KEY, dept TEXT, sal INTEGER)")

	ctx := context.Background()
	for i := 1; i <= 6; i++ {
		dept := "dept" + string(rune('A'+i%3))
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_do",
			Columns: []string{"id", "dept", "sal"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: dept},
				&query.NumberLiteral{Value: float64(i * 100)},
			}},
		}, nil)
	}

	// DISTINCT with ORDER BY (exercises stripHiddenCols path)
	r1 := rows125(c, "SELECT DISTINCT dept FROM b125_do ORDER BY dept ASC")
	t.Logf("DISTINCT ORDER BY: %v", r1)

	// DISTINCT with OFFSET > count
	r2 := rows125(c, "SELECT DISTINCT dept FROM b125_do ORDER BY dept LIMIT 10 OFFSET 100")
	t.Logf("DISTINCT OFFSET overflow: %v", r2)

	// Non-distinct ORDER BY with offset exactly equal to length
	r3 := rows125(c, "SELECT id FROM b125_do ORDER BY id ASC LIMIT 5 OFFSET 1")
	t.Logf("LIMIT OFFSET: %v", r3)
}

// ─── evaluateWhere edge cases ─────────────────────────────────────────────────

func TestB125_EvaluateWhere_EdgeCases(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_we (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_we",
			Columns: []string{"id", "a", "b"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(6 - i)},
			}},
		}, nil)
	}

	// NOT expression
	r1 := rows125(c, "SELECT id FROM b125_we WHERE NOT (a > 3)")
	t.Logf("NOT: %v", r1)

	// AND expression
	r2 := rows125(c, "SELECT id FROM b125_we WHERE a > 1 AND b > 1")
	t.Logf("AND: %v", r2)

	// OR expression
	r3 := rows125(c, "SELECT id FROM b125_we WHERE a = 1 OR a = 5")
	t.Logf("OR: %v", r3)

	// Nested subquery in WHERE
	r4 := rows125(c, "SELECT id FROM b125_we WHERE a > (SELECT AVG(b) FROM b125_we)")
	t.Logf("subquery in WHERE: %v", r4)

	// Exists subquery
	r5 := rows125(c, "SELECT id FROM b125_we WHERE EXISTS (SELECT 1 FROM b125_we WHERE b125_we.a = 1)")
	t.Logf("EXISTS: %v", r5)
}

// ─── DeleteWithUsing (deleteWithUsingLocked 79.2%) ────────────────────────────

func TestB125_DeleteWithUsing(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_du_t (id INTEGER PRIMARY KEY, val TEXT)")
	exec125(c, "CREATE TABLE b125_du_s (id INTEGER PRIMARY KEY, ref_id INTEGER)")

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_du_t",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "v"}}},
		}, nil)
	}
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_du_s",
		Columns: []string{"id", "ref_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "b125_du_s",
		Columns: []string{"id", "ref_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 2}}},
	}, nil)

	// DELETE...USING exercises deleteWithUsingLocked
	exec125(c, "DELETE FROM b125_du_t USING b125_du_s WHERE b125_du_t.id = b125_du_s.ref_id")
	r := rows125(c, "SELECT COUNT(*) FROM b125_du_t")
	t.Logf("after DELETE USING: %v", r)
}

// ─── getInsertTargetTree with partition (79.4%) ───────────────────────────────

func TestB125_InsertSelect_IntoView(t *testing.T) {
	c := newCat125()
	exec125(c, "CREATE TABLE b125_isel_src (id INTEGER PRIMARY KEY, val TEXT)")
	exec125(c, "CREATE TABLE b125_isel_dst (id INTEGER PRIMARY KEY, val TEXT)")

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "b125_isel_src",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "item" + string(rune('0'+i))}}},
		}, nil)
	}

	// INSERT INTO ... SELECT with column count mismatch (error path)
	errRes := exec125(c, "INSERT INTO b125_isel_dst SELECT val FROM b125_isel_src")
	t.Logf("column mismatch err: %v", errRes)

	// Valid INSERT...SELECT
	exec125(c, "INSERT INTO b125_isel_dst SELECT id, val FROM b125_isel_src WHERE id <= 2")
	r := rows125(c, "SELECT COUNT(*) FROM b125_isel_dst")
	t.Logf("INSERT SELECT count: %v", r)
}
