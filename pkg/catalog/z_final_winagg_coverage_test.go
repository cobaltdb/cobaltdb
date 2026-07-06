package catalog

import (
	"math"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ==================== evalWindowAggFrame tests ====================

func TestEvalWindowAggFrame_UnsupportedFunction(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	c := New(tree, pool, nil)

	we := &query.WindowExpr{Function: "ROW_NUMBER"}
	result := c.evalWindowAggFrame(nil, 0, nil, we, nil, nil, nil)
	if result {
		t.Error("expected false for unsupported function")
	}
}

func TestEvalWindowAggFrame_COUNT(t *testing.T) {
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

	entries := []windowPartEntry{
		{originalIdx: 0, row: []interface{}{int64(10)}},
		{originalIdx: 1, row: []interface{}{int64(20)}},
		{originalIdx: 2, row: []interface{}{int64(30)}},
	}

	we := &query.WindowExpr{
		Function: "COUNT",
		Args:     []query.Expression{&query.StarExpr{}},
		Frame: &query.WindowFrame{
			Start: &query.WindowFrameBound{Type: "UNBOUNDED_PRECEDING"},
			End:   &query.WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"},
		},
	}

	rows := make([][]interface{}, 3)
	for i := range rows {
		rows[i] = make([]interface{}, 1)
	}

	selectCols := []selectColInfo{{name: "val", index: 0}}
	result := c.evalWindowAggFrame(rows, 0, entries, we, selectCols, tbl, nil)
	if !result {
		t.Fatal("expected true for COUNT")
	}
	if rows[0][0] != int64(3) {
		t.Errorf("expected COUNT=3, got %v", rows[0][0])
	}
}

func TestEvalWindowAggFrame_SUM(t *testing.T) {
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

	entries := []windowPartEntry{
		{originalIdx: 0, row: []interface{}{int64(10)}},
		{originalIdx: 1, row: []interface{}{float64(20.5)}},
		{originalIdx: 2, row: []interface{}{int64(30)}},
	}

	we := &query.WindowExpr{
		Function: "SUM",
		Args:     []query.Expression{&query.Identifier{Name: "val"}},
		Frame: &query.WindowFrame{
			Start: &query.WindowFrameBound{Type: "UNBOUNDED_PRECEDING"},
			End:   &query.WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"},
		},
	}

	rows := make([][]interface{}, 3)
	for i := range rows {
		rows[i] = make([]interface{}, 1)
	}

	selectCols := []selectColInfo{{name: "val", index: 0}}
	result := c.evalWindowAggFrame(rows, 0, entries, we, selectCols, tbl, nil)
	if !result {
		t.Fatal("expected true for SUM")
	}
	if rows[0][0] != float64(60.5) {
		t.Errorf("expected SUM=60.5, got %v", rows[0][0])
	}
}

func TestEvalWindowAggFrame_AVG(t *testing.T) {
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

	entries := []windowPartEntry{
		{originalIdx: 0, row: []interface{}{int64(10)}},
		{originalIdx: 1, row: []interface{}{int64(20)}},
		{originalIdx: 2, row: []interface{}{int64(30)}},
	}

	we := &query.WindowExpr{
		Function: "AVG",
		Args:     []query.Expression{&query.Identifier{Name: "val"}},
		Frame: &query.WindowFrame{
			Start: &query.WindowFrameBound{Type: "UNBOUNDED_PRECEDING"},
			End:   &query.WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"},
		},
	}

	rows := make([][]interface{}, 3)
	for i := range rows {
		rows[i] = make([]interface{}, 1)
	}

	selectCols := []selectColInfo{{name: "val", index: 0}}
	result := c.evalWindowAggFrame(rows, 0, entries, we, selectCols, tbl, nil)
	if !result {
		t.Fatal("expected true for AVG")
	}
	avg, ok := rows[0][0].(float64)
	if !ok || math.Abs(avg-20.0) > 1e-9 {
		t.Errorf("expected AVG=20, got %v", rows[0][0])
	}
}

func TestEvalWindowAggFrame_MIN(t *testing.T) {
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

	entries := []windowPartEntry{
		{originalIdx: 0, row: []interface{}{int64(30)}},
		{originalIdx: 1, row: []interface{}{int64(10)}},
		{originalIdx: 2, row: []interface{}{int64(20)}},
	}

	we := &query.WindowExpr{
		Function: "MIN",
		Args:     []query.Expression{&query.Identifier{Name: "val"}},
		Frame: &query.WindowFrame{
			Start: &query.WindowFrameBound{Type: "UNBOUNDED_PRECEDING"},
			End:   &query.WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"},
		},
	}

	rows := make([][]interface{}, 3)
	for i := range rows {
		rows[i] = make([]interface{}, 1)
	}

	selectCols := []selectColInfo{{name: "val", index: 0}}
	result := c.evalWindowAggFrame(rows, 0, entries, we, selectCols, tbl, nil)
	if !result {
		t.Fatal("expected true for MIN")
	}
	if rows[0][0] != int64(10) {
		t.Errorf("expected MIN=10, got %v", rows[0][0])
	}
}

func TestEvalWindowAggFrame_MAX(t *testing.T) {
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

	entries := []windowPartEntry{
		{originalIdx: 0, row: []interface{}{int64(30)}},
		{originalIdx: 1, row: []interface{}{int64(10)}},
		{originalIdx: 2, row: []interface{}{int64(20)}},
	}

	we := &query.WindowExpr{
		Function: "MAX",
		Args:     []query.Expression{&query.Identifier{Name: "val"}},
		Frame: &query.WindowFrame{
			Start: &query.WindowFrameBound{Type: "UNBOUNDED_PRECEDING"},
			End:   &query.WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"},
		},
	}

	rows := make([][]interface{}, 3)
	for i := range rows {
		rows[i] = make([]interface{}, 1)
	}

	selectCols := []selectColInfo{{name: "val", index: 0}}
	result := c.evalWindowAggFrame(rows, 0, entries, we, selectCols, tbl, nil)
	if !result {
		t.Fatal("expected true for MAX")
	}
	if rows[0][0] != int64(30) {
		t.Errorf("expected MAX=30, got %v", rows[0][0])
	}
}

func TestEvalWindowAggFrame_FrameBounds(t *testing.T) {
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

	entries := []windowPartEntry{
		{originalIdx: 0, row: []interface{}{int64(10)}},
		{originalIdx: 1, row: []interface{}{int64(20)}},
		{originalIdx: 2, row: []interface{}{int64(30)}},
	}

	we := &query.WindowExpr{
		Function: "COUNT",
		Args:     []query.Expression{&query.Identifier{Name: "val"}},
		Frame: &query.WindowFrame{
			Mode:  "ROWS",
			Start: &query.WindowFrameBound{Type: "PRECEDING", Offset: 1},
			End:   &query.WindowFrameBound{Type: "FOLLOWING", Offset: 1},
		},
	}

	rows := make([][]interface{}, 3)
	for i := range rows {
		rows[i] = make([]interface{}, 1)
	}

	selectCols := []selectColInfo{{name: "val", index: 0}}
	result := c.evalWindowAggFrame(rows, 0, entries, we, selectCols, tbl, nil)
	if !result {
		t.Fatal("expected true for COUNT with frame bounds")
	}

	// Row 0: frame = [0, 1] → count 2 (rows 10, 20)
	// Row 1: frame = [0, 2] → count 3 (rows 10, 20, 30)
	// Row 2: frame = [1, 2] → count 2 (rows 20, 30)
	if rows[0][0] != int64(2) {
		t.Errorf("expected COUNT=2 for row 0, got %v", rows[0][0])
	}
	if rows[1][0] != int64(3) {
		t.Errorf("expected COUNT=3 for row 1, got %v", rows[1][0])
	}
	if rows[2][0] != int64(2) {
		t.Errorf("expected COUNT=2 for row 2, got %v", rows[2][0])
	}
}

func TestEvalWindowAggFrame_NilFilter(t *testing.T) {
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

	entries := []windowPartEntry{
		{originalIdx: 0, row: []interface{}{int64(10)}},
		{originalIdx: 1, row: []interface{}{int64(20)}},
		{originalIdx: 2, row: []interface{}{int64(30)}},
	}

	we := &query.WindowExpr{
		Function: "COUNT",
		Args:     []query.Expression{&query.StarExpr{}},
		Filter:   nil,
		Frame: &query.WindowFrame{
			Start: &query.WindowFrameBound{Type: "UNBOUNDED_PRECEDING"},
			End:   &query.WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"},
		},
	}

	rows := make([][]interface{}, 3)
	for i := range rows {
		rows[i] = make([]interface{}, 1)
	}

	selectCols := []selectColInfo{{name: "val", index: 0}}
	result := c.evalWindowAggFrame(rows, 0, entries, we, selectCols, tbl, nil)
	if !result {
		t.Fatal("expected true for COUNT with nil filter")
	}
	if rows[0][0] != int64(3) {
		t.Errorf("expected COUNT=3 with nil filter, got %v", rows[0][0])
	}
}
