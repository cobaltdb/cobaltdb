package catalog

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// JSON Index: CreateJSONIndex, buildJSONIndex, extractJSONValue
// ============================================================

func TestCoverage_CreateJSONIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create table with JSON column
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "jidx_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert JSON data
	ctx := context.Background()
	jsonVal := map[string]interface{}{"name": "alice", "age": float64(30), "nested": map[string]interface{}{"x": float64(1)}}
	jsonBytes, _ := json.Marshal(jsonVal)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "jidx_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: string(jsonBytes)},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create JSON index on nonexistent table
	err = cat.CreateJSONIndex("idx1", "nonexistent", "data", "$.name", "text")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}

	// Create JSON index on non-JSON column
	err = cat.CreateJSONIndex("idx1", "jidx_t", "id", "$.x", "integer")
	if err == nil {
		t.Error("expected error for non-JSON column")
	}

	// Create JSON index on nonexistent column
	err = cat.CreateJSONIndex("idx1", "jidx_t", "nope", "$.x", "text")
	if err == nil {
		t.Error("expected error for nonexistent column")
	}

	// Create valid JSON index
	err = cat.CreateJSONIndex("idx_name", "jidx_t", "data", "$.name", "text")
	if err != nil {
		t.Errorf("CreateJSONIndex: %v", err)
	}

	// Duplicate index
	err = cat.CreateJSONIndex("idx_name", "jidx_t", "data", "$.name", "text")
	if err == nil {
		t.Error("expected error for duplicate JSON index")
	}

	// Create index with nested path
	err = cat.CreateJSONIndex("idx_nested", "jidx_t", "data", "$.nested.x", "integer")
	if err != nil {
		t.Errorf("CreateJSONIndex nested: %v", err)
	}

	// SearchJSONIndex
	results, err := cat.QueryJSONIndex("idx_name", "alice")
	if err != nil {
		t.Errorf("SearchJSONIndex: %v", err)
	}
	_ = results

	// SearchJSONIndex for nonexistent index
	_, err = cat.QueryJSONIndex("nonexistent", "value")
	if err == nil {
		t.Error("expected error for nonexistent JSON index")
	}

	// DropJSONIndex
	err = cat.DropJSONIndex("idx_name")
	if err != nil {
		t.Errorf("DropJSONIndex: %v", err)
	}
	err = cat.DropJSONIndex("nonexistent")
	if err == nil {
		t.Error("expected error for dropping nonexistent JSON index")
	}

	pool.Close()
}

// ============================================================
// storeIndexDef directly
// ============================================================

func TestCoverage_storeIndexDef(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Store index def with tree
	err = cat.storeIndexDef(&IndexDef{
		Name:      "test_idx",
		TableName: "t",
		Columns:   []string{"col1"},
		Unique:    true,
	})
	if err != nil {
		t.Errorf("storeIndexDef: %v", err)
	}

	// Store with nil tree
	cat2 := New(nil, pool, nil)
	err = cat2.storeIndexDef(&IndexDef{Name: "test", TableName: "t", Columns: []string{"a"}})
	if err != nil {
		t.Errorf("storeIndexDef nil tree: %v", err)
	}

	pool.Close()
}

// ============================================================
// CreateIndex & DropIndex error paths
// ============================================================

func TestCoverage_CreateDropIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "idx_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data for index building
	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "idx_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "alice"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create index on nonexistent table
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx1", Table: "nonexistent", Columns: []string{"col"}})
	if err == nil {
		t.Error("expected error for nonexistent table")
	}

	// Create index on nonexistent column
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx1", Table: "idx_t", Columns: []string{"nope"}})
	if err == nil {
		t.Error("expected error for nonexistent column")
	}

	// Create valid index
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_name", Table: "idx_t", Columns: []string{"name"}})
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate index
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_name", Table: "idx_t", Columns: []string{"name"}})
	if err == nil {
		t.Error("expected error for duplicate index")
	}

	// IF NOT EXISTS on duplicate
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_name", Table: "idx_t", Columns: []string{"name"}, IfNotExists: true})
	if err != nil {
		t.Errorf("IF NOT EXISTS should not error: %v", err)
	}

	// Create unique index
	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_name_u", Table: "idx_t", Columns: []string{"name"}, Unique: true})
	if err != nil {
		t.Fatal(err)
	}

	// GetIndex
	_, err = cat.GetIndex("idx_name")
	if err != nil {
		t.Errorf("GetIndex: %v", err)
	}
	_, err = cat.GetIndex("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent index")
	}

	// DropIndex
	err = cat.DropIndex("idx_name")
	if err != nil {
		t.Errorf("DropIndex: %v", err)
	}
	err = cat.DropIndex("nonexistent")
	if err == nil {
		t.Error("expected error for dropping nonexistent index")
	}

	// Drop + txn
	cat.BeginTransaction(1)
	err = cat.DropIndex("idx_name_u")
	if err != nil {
		t.Fatal(err)
	}
	_ = cat.RollbackTransaction()
	// Index should be restored
	_, err = cat.GetIndex("idx_name_u")
	if err != nil {
		t.Error("index should exist after rollback")
	}

	pool.Close()
}

// ============================================================
// DropTable with indexes and FK cascades
// ============================================================

func TestCoverage_DropTable_WithIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "drop_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_drop_val", Table: "drop_t", Columns: []string{"val"}})
	if err != nil {
		t.Fatal(err)
	}

	// Drop table with index in txn
	cat.BeginTransaction(1)
	err = cat.DropTable(&query.DropTableStmt{Table: "drop_t"})
	if err != nil {
		t.Fatal(err)
	}
	_ = cat.RollbackTransaction()

	// Table and index should be restored
	_, err = cat.GetTable("drop_t")
	if err != nil {
		t.Error("table should exist after drop rollback")
	}

	// Drop nonexistent table
	err = cat.DropTable(&query.DropTableStmt{Table: "nonexistent"})
	if err == nil {
		t.Error("expected error for dropping nonexistent table")
	}

	// Drop IF EXISTS nonexistent
	err = cat.DropTable(&query.DropTableStmt{Table: "nonexistent", IfExists: true})
	if err != nil {
		t.Errorf("IF EXISTS should not error: %v", err)
	}

	pool.Close()
}

// ============================================================
// AlterTableDropColumn
// ============================================================

func TestCoverage_AlterTableDropColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "altdrop_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "extra", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "altdrop_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "alice"},
			&query.StringLiteral{Value: "info"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Drop column in txn and rollback
	cat.BeginTransaction(1)
	err = cat.AlterTableDropColumn(&query.AlterTableStmt{Table: "altdrop_t", Action: "DROP", NewName: "extra"})
	if err != nil {
		t.Fatal(err)
	}
	_ = cat.RollbackTransaction()

	// Column should be restored
	tbl, _ := cat.GetTable("altdrop_t")
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "extra" {
			found = true
		}
	}
	if !found {
		t.Error("extra column should be restored after rollback")
	}

	// Drop nonexistent column
	err = cat.AlterTableDropColumn(&query.AlterTableStmt{Table: "altdrop_t", Action: "DROP", NewName: "nope"})
	if err == nil {
		t.Error("expected error for dropping nonexistent column")
	}

	// Drop PK column
	err = cat.AlterTableDropColumn(&query.AlterTableStmt{Table: "altdrop_t", Action: "DROP", NewName: "id"})
	if err == nil {
		t.Error("expected error for dropping PK column")
	}

	pool.Close()
}

// ============================================================
// FTS: CreateFTSIndex, SearchFTS, DropFTSIndex
// ============================================================

func TestCoverage_FTS(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "fts_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "body", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "fts_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "the quick brown fox jumps over the lazy dog"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table: "fts_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 2},
			&query.StringLiteral{Value: "hello world foo bar"},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create FTS index on nonexistent table
	err = cat.CreateFTSIndex("fts1", "nonexistent", []string{"body"})
	if err == nil {
		t.Error("expected error for nonexistent table")
	}

	// Create FTS index on nonexistent column
	err = cat.CreateFTSIndex("fts1", "fts_t", []string{"nope"})
	if err == nil {
		t.Error("expected error for nonexistent column")
	}

	// Create valid FTS index
	err = cat.CreateFTSIndex("fts1", "fts_t", []string{"body"})
	if err != nil {
		t.Errorf("CreateFTSIndex: %v", err)
	}

	// Duplicate
	err = cat.CreateFTSIndex("fts1", "fts_t", []string{"body"})
	if err == nil {
		t.Error("expected error for duplicate FTS index")
	}

	// Search
	results, err := cat.SearchFTS("fts1", "fox")
	if err != nil {
		t.Errorf("SearchFTS: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for 'fox', got %d", len(results))
	}

	// Search multiple words
	results, err = cat.SearchFTS("fts1", "the dog")
	if err != nil {
		t.Errorf("SearchFTS multi: %v", err)
	}

	// Search nonexistent index
	_, err = cat.SearchFTS("nonexistent", "query")
	if err == nil {
		t.Error("expected error for nonexistent FTS index")
	}

	// Drop FTS index
	err = cat.DropFTSIndex("fts1")
	if err != nil {
		t.Errorf("DropFTSIndex: %v", err)
	}
	err = cat.DropFTSIndex("nonexistent")
	if err == nil {
		t.Error("expected error for dropping nonexistent FTS index")
	}

	pool.Close()
}

// ============================================================
// CreateView, DropView, GetView, TableOrViewExists
// ============================================================

func TestCoverage_Views(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "view_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create view
	err := cat.CreateView("v1", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "view_t"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate view
	err = cat.CreateView("v1", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "view_t"},
	})
	if err == nil {
		t.Error("expected error for duplicate view")
	}

	// GetView
	v, err := cat.GetView("v1")
	if err != nil || v == nil {
		t.Error("expected to get view")
	}
	_, err = cat.GetView("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent view")
	}

	// TableOrViewExists
	if !cat.HasTableOrView("view_t") {
		t.Error("expected table to exist")
	}
	if !cat.HasTableOrView("v1") {
		t.Error("expected view to exist")
	}
	if cat.HasTableOrView("nonexistent") {
		t.Error("expected nonexistent to not exist")
	}

	// DropView
	err = cat.DropView("v1")
	if err != nil {
		t.Errorf("DropView: %v", err)
	}
	err = cat.DropView("nonexistent")
	if err == nil {
		t.Error("expected error for dropping nonexistent view")
	}
}

// ============================================================
// Materialized View: Create, Refresh, Drop
// ============================================================

func TestCoverage_MaterializedView(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "mv_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "score", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, _, _ = cat.Insert(ctx, &query.InsertStmt{
		Table: "mv_t",
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 100},
		}},
	}, nil)

	// Create materialized view
	err = cat.CreateMaterializedView("mv1", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "score"}},
		From:    &query.TableRef{Name: "mv_t"},
	})
	if err != nil {
		t.Errorf("CreateMaterializedView: %v", err)
	}

	// Duplicate
	err = cat.CreateMaterializedView("mv1", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "mv_t"},
	})
	if err == nil {
		t.Error("expected error for duplicate materialized view")
	}

	// Refresh
	err = cat.RefreshMaterializedView("mv1")
	if err != nil {
		t.Errorf("RefreshMaterializedView: %v", err)
	}
	err = cat.RefreshMaterializedView("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent materialized view")
	}

	// Drop
	err = cat.DropMaterializedView("mv1")
	if err != nil {
		t.Errorf("DropMaterializedView: %v", err)
	}
	err = cat.DropMaterializedView("nonexistent")
	if err == nil {
		t.Error("expected error for dropping nonexistent materialized view")
	}

	pool.Close()
}

// ============================================================
// Procedures: Create, Get, Drop, Execute
// ============================================================

func TestCoverage_Procedures(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "proc_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Create procedure
	err := cat.CreateProcedure(&query.CreateProcedureStmt{
		Name: "proc1",
	})
	if err != nil {
		t.Errorf("CreateProcedure: %v", err)
	}

	// Duplicate
	err = cat.CreateProcedure(&query.CreateProcedureStmt{Name: "proc1"})
	if err == nil {
		t.Error("expected error for duplicate procedure")
	}

	// Get
	p, err := cat.GetProcedure("proc1")
	if err != nil || p == nil {
		t.Error("expected to get procedure")
	}
	_, err = cat.GetProcedure("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent procedure")
	}

	// Drop
	err = cat.DropProcedure("proc1")
	if err != nil {
		t.Errorf("DropProcedure: %v", err)
	}
	err = cat.DropProcedure("nonexistent")
	if err == nil {
		t.Error("expected error for dropping nonexistent procedure")
	}
}

// ============================================================
// findUsableIndexWithArgs
// ============================================================

func TestCoverage_findUsableIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "fidx_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_fidx_name", Table: "fidx_t", Columns: []string{"name"}})
	if err != nil {
		t.Fatal(err)
	}

	// nil where
	cat.mu.RLock()
	n, _, _ := cat.findUsableIndexWithArgs("fidx_t", nil, nil)
	cat.mu.RUnlock()
	if n != "" {
		t.Error("expected empty for nil where")
	}

	// WHERE name = 'alice'
	cat.mu.RLock()
	n, col, val := cat.findUsableIndexWithArgs("fidx_t", &query.BinaryExpr{
		Left:     &query.Identifier{Name: "name"},
		Operator: query.TokenEq,
		Right:    &query.StringLiteral{Value: "alice"},
	}, nil)
	cat.mu.RUnlock()
	if n == "" {
		t.Error("expected to find index for name = alice")
	}
	_ = col
	_ = val

	// WHERE name = 'alice' AND id = 1 (AND condition)
	cat.mu.RLock()
	n, _, _ = cat.findUsableIndexWithArgs("fidx_t", &query.BinaryExpr{
		Left: &query.BinaryExpr{
			Left: &query.Identifier{Name: "name"}, Operator: query.TokenEq,
			Right: &query.StringLiteral{Value: "alice"},
		},
		Operator: query.TokenAnd,
		Right: &query.BinaryExpr{
			Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq,
			Right: &query.NumberLiteral{Value: 1},
		},
	}, nil)
	cat.mu.RUnlock()
	if n == "" {
		t.Error("expected to find index through AND")
	}

	pool.Close()
}
