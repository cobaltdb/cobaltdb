package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func setupFinalCov(t *testing.T) (*Catalog, *btree.BTree, *storage.BufferPool) {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)
	return cat, tree, pool
}

func TestFinalCoverage_CountRows(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table:   "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	sc := NewStatsCollector(cat)
	count, err := sc.countRows("t1")
	if err != nil {
		t.Fatalf("countRows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
	stats, err := sc.CollectStats("t1")
	if err != nil {
		t.Fatalf("CollectStats: %v", err)
	}
	if stats.RowCount != 0 {
		t.Errorf("expected 0, got %d", stats.RowCount)
	}
	_, err = sc.CollectStats("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestFinalCoverage_CollectColumnStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table: "multi",
		Columns: []*query.ColumnDef{
			{Name: "a", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "b", Type: query.TokenText},
		},
	})
	sc := NewStatsCollector(cat)
	stats, err := sc.CollectStats("multi")
	if err != nil {
		t.Fatalf("CollectStats: %v", err)
	}
	if stats.ColumnStats == nil {
		t.Log("ColumnStats is nil (expected from stub)")
	}
}

func TestFinalCoverage_Save(t *testing.T) {
	cat, _, _ := setupFinalCov(t)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	_, _, err := cat.Insert(&query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "alice"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := cat.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func TestFinalCoverage_Load(t *testing.T) {
	cat, tree, pool := setupFinalCov(t)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table: "loadtest",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "age", Type: query.TokenInteger, Default: &query.NumberLiteral{Value: 18}},
			{Name: "status", Type: query.TokenText, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "status"},
				Operator: query.TokenNeq,
				Right:    &query.StringLiteral{Value: ""},
			}},
		},
	})
	_, _, _ = cat.Insert(&query.InsertStmt{
		Table:   "loadtest",
		Columns: []string{"id", "age", "status"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 25}, &query.StringLiteral{Value: "active"}},
		},
	}, nil)
	if err := cat.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	tbl, err := cat2.GetTable("loadtest")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	if len(tbl.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(tbl.Columns))
	}
	if tbl.Columns[1].Default == "" {
		t.Error("expected Default on age column")
	}
	if tbl.Columns[2].CheckStr == "" {
		t.Error("expected CheckStr on status column")
	}
	catNil := New(nil, pool, nil)
	if err := catNil.Load(); err != nil {
		t.Fatalf("Load nil tree: %v", err)
	}
}
func TestFinalCoverage_Analyze(t *testing.T) {
	cat, _, _ := setupFinalCov(t)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table: "analytics",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "score", Type: query.TokenInteger},
			{Name: "label", Type: query.TokenText},
		},
	})
	for i := 0; i < 5; i++ {
		_, _, _ = cat.Insert(&query.InsertStmt{
			Table:   "analytics",
			Columns: []string{"id", "score", "label"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i * 10)},
				&query.StringLiteral{Value: "item"},
			}},
		}, nil)
	}
	if err := cat.Analyze("analytics"); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	stats, err := cat.GetTableStats("analytics")
	if err != nil {
		t.Fatalf("GetTableStats: %v", err)
	}
	if stats.RowCount == 0 {
		t.Error("expected non-zero RowCount")
	}
	if err := cat.Analyze("nonexistent"); err == nil {
		t.Error("expected error for nonexistent")
	}
}

func TestFinalCoverage_JSONQuote(t *testing.T) {
	if got := JSONQuote("hello"); got != "\"hello\"" {
		t.Errorf("got %q", got)
	}
	if got := JSONQuote(""); got != "\"\"" {
		t.Errorf("got %q", got)
	}
}

func TestFinalCoverage_JSONExtract(t *testing.T) {
	r, err := JSONExtract("", "$.foo")
	if err != nil {
		t.Errorf("empty data err: %v", err)
	}
	if r != nil {
		t.Errorf("expected nil, got %v", r)
	}
	_, err = JSONExtract("not json", "$.foo")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
	_, err = JSONExtract("{\"a\":1}", "")
	if err == nil {
		t.Error("expected error for empty path")
	}
	r, err = JSONExtract("{\"a\":{\"b\":42}}", "$.a.b")
	if err != nil {
		t.Fatalf("nested: %v", err)
	}
	if r != float64(42) {
		t.Errorf("expected 42, got %v", r)
	}
	r, err = JSONExtract("[10,20,30]", "$[1]")
	if err != nil {
		t.Fatalf("array: %v", err)
	}
	if r != float64(20) {
		t.Errorf("expected 20, got %v", r)
	}
}

func TestFinalCoverage_JSONRemove(t *testing.T) {
	r, err := JSONRemove("", "$.foo")
	if err != nil {
		t.Errorf("empty err: %v", err)
	}
	if r != "" {
		t.Errorf("expected empty, got %q", r)
	}
	_, err = JSONRemove("not json", "$.foo")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
	_, err = JSONRemove("{\"a\":1}", "")
	if err == nil {
		t.Error("expected error for empty path")
	}
	r, err = JSONRemove("{\"a\":1,\"b\":2}", "$.a")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if r != "{\"b\":2}" {
		t.Errorf("expected {\"b\":2}, got %s", r)
	}
}

func TestFinalCoverage_FlushTableTrees(t *testing.T) {
	cat, _, _ := setupFinalCov(t)
	if err := cat.FlushTableTrees(); err != nil {
		t.Fatalf("empty: %v", err)
	}
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table:   "ft",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	_, _, _ = cat.Insert(&query.InsertStmt{
		Table: "ft", Columns: []string{"id"},
		Values: [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	if err := cat.FlushTableTrees(); err != nil {
		t.Fatalf("with data: %v", err)
	}
}
func TestFinalCoverage_Rollback_AlterRename(t *testing.T) {
	cat, _, _ := setupFinalCov(t)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table: "orig",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	cat.BeginTransaction(1)
	_ = cat.AlterTableRename(&query.AlterTableStmt{Table: "orig", Action: "RENAME_TABLE", NewName: "renamed"})
	if _, err := cat.GetTable("renamed"); err != nil {
		t.Fatalf("rename failed: %v", err)
	}
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := cat.GetTable("orig"); err != nil {
		t.Errorf("orig not restored: %v", err)
	}
}

func TestFinalCoverage_Rollback_RenameColumn(t *testing.T) {
	cat, _, _ := setupFinalCov(t)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table: "ct",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "old_col", Type: query.TokenText},
		},
	})
	cat.BeginTransaction(2)
	_ = cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table: "ct", Action: "RENAME_COLUMN", OldName: "old_col", NewName: "new_col",
	})
	tbl, _ := cat.GetTable("ct")
	if tbl.Columns[1].Name != "new_col" {
		t.Errorf("expected new_col, got %s", tbl.Columns[1].Name)
	}
	_ = cat.RollbackTransaction()
	tbl, _ = cat.GetTable("ct")
	if tbl.Columns[1].Name != "old_col" {
		t.Errorf("expected old_col, got %s", tbl.Columns[1].Name)
	}
}

func TestFinalCoverage_Rollback_DropColumn(t *testing.T) {
	cat, _, _ := setupFinalCov(t)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table: "dc",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "extra", Type: query.TokenText},
		},
	})
	_, _, _ = cat.Insert(&query.InsertStmt{
		Table: "dc", Columns: []string{"id", "extra"},
		Values: [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "keep"}}},
	}, nil)
	cat.BeginTransaction(3)
	_ = cat.AlterTableDropColumn(&query.AlterTableStmt{Table: "dc", Action: "DROP", NewName: "extra"})
	tbl, _ := cat.GetTable("dc")
	if len(tbl.Columns) != 1 {
		t.Errorf("expected 1 col, got %d", len(tbl.Columns))
	}
	_ = cat.RollbackTransaction()
	tbl, _ = cat.GetTable("dc")
	if len(tbl.Columns) != 2 {
		t.Errorf("expected 2 cols after rollback, got %d", len(tbl.Columns))
	}
}

func TestFinalCoverage_Rollback_DropTable(t *testing.T) {
	cat, _, _ := setupFinalCov(t)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table:   "td",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	cat.BeginTransaction(4)
	_ = cat.DropTable(&query.DropTableStmt{Table: "td"})
	if _, err := cat.GetTable("td"); err == nil {
		t.Error("expected table gone")
	}
	_ = cat.RollbackTransaction()
	if _, err := cat.GetTable("td"); err != nil {
		t.Errorf("expected restored: %v", err)
	}
}

func TestFinalCoverage_Rollback_CreateTable(t *testing.T) {
	cat, _, _ := setupFinalCov(t)
	cat.BeginTransaction(5)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table:   "txnc",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	_ = cat.RollbackTransaction()
	if _, err := cat.GetTable("txnc"); err == nil {
		t.Error("expected table removed after rollback")
	}
}
func TestFinalCoverage_StatsCollectorMisc(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table:   "s",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	sc := NewStatsCollector(cat)
	_, _ = sc.CollectStats("s")
	if _, ok := sc.GetTableStats("s"); !ok {
		t.Error("expected stats")
	}
	sc.GetColumnStats("s", "id")
	if c := sc.EstimateRowCount("unknown"); c != 1000 {
		t.Errorf("expected 1000, got %d", c)
	}
	summary := sc.GetStatsSummary()
	if _, ok := summary["s"]; !ok {
		t.Error("expected s in summary")
	}
	sc.InvalidateStats("s")
	if _, ok := sc.GetTableStats("s"); ok {
		t.Error("expected gone after invalidate")
	}
	if !sc.IsStale("s", 0) {
		t.Error("expected stale")
	}
	_ = sc.EstimateSelectivity("s", "id", "=", 1)
	_ = sc.EstimateSeqScanCost("s", 0.5)
	_ = sc.EstimateIndexScanCost("s", "idx", 0.5)
	_ = sc.EstimateNestedLoopCost(100, 10)
	_ = sc.EstimateHashJoinCost(100, 200)
	_ = sc.EstimateMergeJoinCost(100, 200)
}

func TestFinalCoverage_ExecuteQuery(t *testing.T) {
	cat := New(nil, nil, nil)
	r, err := cat.ExecuteQuery("SELECT 1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if r == nil || len(r.Rows) != 0 {
		t.Error("expected empty result")
	}
}

func TestFinalCoverage_SaveLoadRoundTrip(t *testing.T) {
	cat, tree, pool := setupFinalCov(t)
	_ = cat.CreateTable(&query.CreateTableStmt{
		Table: "rt",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText, Default: &query.StringLiteral{Value: "def"}},
		},
	})
	for i := 0; i < 3; i++ {
		_, _, _ = cat.Insert(&query.InsertStmt{
			Table: "rt", Columns: []string{"id", "val"},
			Values: [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "d"}}},
		}, nil)
	}
	_ = cat.Save()
	cat2 := New(tree, pool, nil)
	_ = cat2.Load()
	tbl, err := cat2.GetTable("rt")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	if tbl.RootPageID == 0 {
		t.Error("expected non-zero RootPageID")
	}
}
