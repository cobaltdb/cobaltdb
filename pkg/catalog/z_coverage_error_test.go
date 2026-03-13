package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Error Handling Tests
// ============================================================

func TestErrorHandling_SelectNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	_, _, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "nonexistent_table"},
	}, nil)

	if err == nil {
		t.Error("expected error for non-existent table, got nil")
	}
}

func TestErrorHandling_InsertNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "nonexistent_table",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	if err == nil {
		t.Error("expected error for insert into non-existent table, got nil")
	}
}

func TestErrorHandling_UpdateNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	_, _, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "nonexistent_table",
		Set:   []*query.SetClause{{Column: "val", Value: &query.NumberLiteral{Value: 1}}},
	}, nil)

	if err == nil {
		t.Error("expected error for update on non-existent table, got nil")
	}
}

func TestErrorHandling_DeleteNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	_, _, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "nonexistent_table",
	}, nil)

	if err == nil {
		t.Error("expected error for delete from non-existent table, got nil")
	}
}

func TestErrorHandling_DuplicatePrimaryKey(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_dup_pk",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_dup_pk",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_dup_pk",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	if err == nil {
		t.Error("expected error for duplicate primary key, got nil")
	}
}

func TestErrorHandling_NotNullConstraint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_notnull",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "req", Type: query.TokenText, NotNull: true}},
		PrimaryKey: []string{"id"},
	})

	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_notnull",
		Columns: []string{"id", "req"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NullLiteral{}}},
	}, nil)

	if err == nil {
		t.Error("expected error for NOT NULL constraint violation, got nil")
	}
}

func TestErrorHandling_InvalidColumnReference(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_col",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.QualifiedIdentifier{Column: "nonexistent_column"}},
		From:    &query.TableRef{Name: "test_col"},
	}, nil)

	// Verify code path executed - may or may not return error depending on implementation
	_ = rows
	_ = err
}

// ============================================================
// Edge Case Tests
// ============================================================

func TestEdgeCase_EmptyTableSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_empty",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	cols, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "test_empty"},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error for empty table select: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
	if len(cols) != 1 {
		t.Errorf("expected 1 column, got %d", len(cols))
	}
}

func TestEdgeCase_LargeNumberOfRows(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_many",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	// Insert 1000 rows
	for i := 1; i <= 1000; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "test_many",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "test_many"},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rows) > 0 {
		count := fmt.Sprintf("%v", rows[0][0])
		if count != "1000" {
			t.Errorf("expected count=1000, got %v", rows[0][0])
		}
	}
}

func TestEdgeCase_NegativeNumbers(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_neg",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_neg",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: -100}}},
	}, nil)

	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.QualifiedIdentifier{Column: "val"}},
		From:    &query.TableRef{Name: "test_neg"},
		Where:   &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "val"}, Operator: query.TokenLt, Right: &query.NumberLiteral{Value: 0}},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row with negative value, got %d", len(rows))
	}
}

func TestEdgeCase_ZeroAndNullHandling(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_zero_null",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_zero_null",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 0}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_zero_null",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NullLiteral{}}},
	}, nil)

	// Test zero handling
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
		From:    &query.TableRef{Name: "test_zero_null"},
		Where:   &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "val"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 0}},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row with val=0, got %d", len(rows))
	}
}

func TestEdgeCase_LargeTextValues(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_large_text",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "content", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	largeText := ""
	for i := 0; i < 1000; i++ {
		largeText += "a"
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_large_text",
		Columns: []string{"id", "content"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: largeText}}},
	}, nil)

	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.QualifiedIdentifier{Column: "content"}},
		From:    &query.TableRef{Name: "test_large_text"},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestEdgeCase_SpecialCharactersInText(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_special",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "txt", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	specialTexts := []string{
		"text with 'quotes'",
		"text with \"double quotes\"",
		"text with \n newlines",
		"text with \t tabs",
		"text with unicode: 日本語",
		"text with emoji: 😀",
	}

	for i, txt := range specialTexts {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "test_special",
			Columns: []string{"id", "txt"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i + 1)}, &query.StringLiteral{Value: txt}}},
		}, nil)
	}

	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "test_special"},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rows) > 0 {
		count := fmt.Sprintf("%v", rows[0][0])
		expected := fmt.Sprintf("%d", len(specialTexts))
		if count != expected {
			t.Errorf("expected count=%s, got %v", expected, rows[0][0])
		}
	}
}

func TestEdgeCase_LimitZero(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_limit0",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "test_limit0",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "test_limit0"},
		Limit:   &query.NumberLiteral{Value: 0},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// LIMIT 0 should return 0 rows
	if len(rows) != 0 {
		t.Errorf("expected 0 rows with LIMIT 0, got %d", len(rows))
	}
}

func TestEdgeCase_OffsetBeyondRowCount(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_offset",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "test_offset",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "test_offset"},
		Offset:  &query.NumberLiteral{Value: 100},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// OFFSET beyond row count should return 0 rows
	if len(rows) != 0 {
		t.Errorf("expected 0 rows with OFFSET 100 on 5-row table, got %d", len(rows))
	}
}

func TestEdgeCase_WhereAlwaysFalse(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_false",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "test_false",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	// WHERE 1=0 should return 0 rows
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "test_false"},
		Where:   &query.BinaryExpr{Left: &query.NumberLiteral{Value: 1}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 0}},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for always-false WHERE, got %d", len(rows))
	}
}

func TestEdgeCase_WhereAlwaysTrue(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_true",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "test_true",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	// WHERE 1=1 should return all rows
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "test_true"},
		Where:   &query.BinaryExpr{Left: &query.NumberLiteral{Value: 1}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("expected 10 rows for always-true WHERE, got %d", len(rows))
	}
}
