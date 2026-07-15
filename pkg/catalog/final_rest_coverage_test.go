package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ==================== storeJSONIndexDef tests ====================

func TestStoreJSONIndexDef_NilTree(t *testing.T) {
	c := &Catalog{}
	err := c.storeJSONIndexDef(&JSONIndexDef{Name: "json_idx"})
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestStoreJSONIndexDef_WithTree(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	ji := &JSONIndexDef{
		Name:      "my_json",
		TableName: "data",
		Column:    "json_col",
		Path:      "$.name",
		DataType:  "string",
		Index:     map[string][]int64{"alice": {1}},
	}

	err = c.storeJSONIndexDef(ji)
	if err != nil {
		t.Fatalf("storeJSONIndexDef failed: %v", err)
	}

	val, err := tree.Get([]byte("json:my_json"))
	if err != nil {
		t.Fatalf("tree.Get failed: %v", err)
	}
	if val == nil {
		t.Fatal("expected stored JSON index")
	}
}

// ==================== loadMainTableRows tests ====================

func TestLoadMainTableRows_TableNotFound(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	cols, rows, err := c.loadMainTableRows(&query.TableRef{Name: "nonexistent"})
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
	if cols != nil {
		t.Errorf("expected nil cols, got %v", cols)
	}
	if rows != nil {
		t.Errorf("expected nil rows, got %v", rows)
	}
}

func TestLoadMainTableRows_Success(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	err = c.CreateTable(&query.CreateTableStmt{
		Table: "mytable",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	_, _, err = c.Insert(context.Background(), &query.InsertStmt{
		Table: "mytable",
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "hello"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	cols, rows, err := c.loadMainTableRows(&query.TableRef{Name: "mytable"})
	if err != nil {
		t.Fatalf("loadMainTableRows failed: %v", err)
	}
	if len(cols) != 2 {
		t.Errorf("expected 2 cols, got %d", len(cols))
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestLoadMainTableRows_EmptyTable(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	err = c.CreateTable(&query.CreateTableStmt{
		Table: "empty",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cols, rows, err := c.loadMainTableRows(&query.TableRef{Name: "empty"})
	if err != nil {
		t.Fatalf("loadMainTableRows failed: %v", err)
	}
	if len(cols) != 1 {
		t.Errorf("expected 1 col, got %d", len(cols))
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

// ==================== computeWindowExprColumn tests ====================

func TestComputeWindowExprColumn_NoRows(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	we := &query.WindowExpr{
		Function: "ROW_NUMBER",
		Args:     []query.Expression{&query.StarExpr{}},
	}

	result := c.computeWindowExprColumn(we, nil, nil, nil, nil, nil)
	if len(result) != 0 {
		t.Errorf("expected 0 results, got %d", len(result))
	}
}

func TestComputeWindowExprColumn_RowNumber(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	err = c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "val", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	tbl, err := c.getTableLocked("test")
	if err != nil {
		t.Fatalf("getTableLocked failed: %v", err)
	}

	rows := [][]interface{}{
		{int64(10)},
		{int64(20)},
		{int64(30)},
	}

	we := &query.WindowExpr{
		Function: "ROW_NUMBER",
		Args:     []query.Expression{&query.StarExpr{}},
	}

	selectCols := []selectColInfo{{name: "val", index: 0}}
	result := c.computeWindowExprColumn(we, rows, rows, selectCols, tbl, nil)
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}
	for i, v := range result {
		if i64, ok := v.(int64); !ok || i64 != int64(i+1) {
			t.Errorf("row %d: expected %d, got %v (%T)", i, i+1, v, v)
		}
	}
}

func TestComputeWindowExprColumn_PartitionBy(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	err = c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "grp", Type: query.TokenInteger},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	tbl, err := c.getTableLocked("test")
	if err != nil {
		t.Fatalf("getTableLocked failed: %v", err)
	}

	rows := [][]interface{}{
		{int64(1), int64(100)},
		{int64(1), int64(200)},
		{int64(2), int64(300)},
	}

	we := &query.WindowExpr{
		Function:    "ROW_NUMBER",
		Args:        []query.Expression{&query.StarExpr{}},
		PartitionBy: []query.Expression{&query.Identifier{Name: "grp"}},
	}

	selectCols := []selectColInfo{
		{name: "grp", index: 0},
		{name: "val", index: 1},
	}
	result := c.computeWindowExprColumn(we, rows, rows, selectCols, tbl, nil)
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}
	for i, v := range result {
		if v == nil {
			t.Errorf("row %d: unexpected nil result", i)
		}
	}
}
