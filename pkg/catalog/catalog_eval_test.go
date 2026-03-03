package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestEvaluateWhereNil tests evaluateWhere with nil where clause
func TestEvaluateWhereNil(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "TEXT"},
	}
	row := []interface{}{int64(1), "test"}

	// nil where should return true
	result, err := evaluateWhere(cat, row, columns, nil, nil)
	if err != nil {
		t.Fatalf("evaluateWhere failed: %v", err)
	}
	if !result {
		t.Error("Expected true for nil where clause")
	}
}

// TestEvaluateWhereBoolean tests evaluateWhere with boolean result
func TestEvaluateWhereBoolean(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(1)}

	// Test with boolean literal true
	expr := &query.BooleanLiteral{Value: true}
	result, err := evaluateWhere(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateWhere failed: %v", err)
	}
	if !result {
		t.Error("Expected true for boolean literal true")
	}

	// Test with boolean literal false
	expr = &query.BooleanLiteral{Value: false}
	result, err = evaluateWhere(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateWhere failed: %v", err)
	}
	if result {
		t.Error("Expected false for boolean literal false")
	}
}

// TestEvaluateWhereNumeric tests evaluateWhere with numeric results
func TestEvaluateWhereNumeric(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(1)}

	// Test with number literal 1 (truthy)
	expr := &query.NumberLiteral{Value: 1}
	result, err := evaluateWhere(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateWhere failed: %v", err)
	}
	if !result {
		t.Error("Expected true for numeric literal 1")
	}

	// Test with number literal 0 (falsy)
	expr = &query.NumberLiteral{Value: 0}
	result, err = evaluateWhere(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateWhere failed: %v", err)
	}
	if result {
		t.Error("Expected false for numeric literal 0")
	}
}

// TestEvaluateWhereString tests evaluateWhere with string results
func TestEvaluateWhereString(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "name", Type: "TEXT"},
	}
	row := []interface{}{"test"}

	// Test with non-empty string (truthy)
	expr := &query.StringLiteral{Value: "hello"}
	result, err := evaluateWhere(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateWhere failed: %v", err)
	}
	if !result {
		t.Error("Expected true for non-empty string")
	}

	// Test with empty string (falsy)
	expr = &query.StringLiteral{Value: ""}
	result, err = evaluateWhere(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateWhere failed: %v", err)
	}
	if result {
		t.Error("Expected false for empty string")
	}
}

// TestEvaluateHavingNil tests evaluateHaving with nil having clause
func TestEvaluateHavingNil(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(1)}
	selectCols := []selectColInfo{{index: 0, name: "id"}}

	// nil having should return true
	result, err := evaluateHaving(cat, row, selectCols, columns, nil, nil)
	if err != nil {
		t.Fatalf("evaluateHaving failed: %v", err)
	}
	if !result {
		t.Error("Expected true for nil having clause")
	}
}

// TestEvaluateHavingBoolean tests evaluateHaving with boolean result
func TestEvaluateHavingBoolean(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(1)}
	selectCols := []selectColInfo{{index: 0, name: "id"}}

	// Test with boolean literal true
	expr := &query.BooleanLiteral{Value: true}
	result, err := evaluateHaving(cat, row, selectCols, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateHaving failed: %v", err)
	}
	if !result {
		t.Error("Expected true for boolean literal true")
	}
}

// TestEvaluateHavingNumeric tests evaluateHaving with numeric results
func TestEvaluateHavingNumeric(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(1)}
	selectCols := []selectColInfo{{index: 0, name: "id"}}

	// Test with number literal 1 (truthy)
	expr := &query.NumberLiteral{Value: 1}
	result, err := evaluateHaving(cat, row, selectCols, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateHaving failed: %v", err)
	}
	if !result {
		t.Error("Expected true for numeric literal 1")
	}

	// Test with number literal 0 (falsy)
	expr = &query.NumberLiteral{Value: 0}
	result, err = evaluateHaving(cat, row, selectCols, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateHaving failed: %v", err)
	}
	if result {
		t.Error("Expected false for numeric literal 0")
	}
}

// TestEvaluateInSimple tests evaluateIn with simple list
func TestEvaluateInSimple(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(2)}

	// Test IN with matching value
	expr := &query.InExpr{
		Expr: &query.Identifier{Name: "id"},
		List: []query.Expression{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 2},
			&query.NumberLiteral{Value: 3},
		},
		Not: false,
	}

	result, err := evaluateIn(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateIn failed: %v", err)
	}
	if !result {
		t.Error("Expected true for IN with matching value")
	}
}

// TestEvaluateInNotFound tests evaluateIn with non-matching value
func TestEvaluateInNotFound(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(5)}

	// Test IN with non-matching value
	expr := &query.InExpr{
		Expr: &query.Identifier{Name: "id"},
		List: []query.Expression{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 2},
			&query.NumberLiteral{Value: 3},
		},
		Not: false,
	}

	result, err := evaluateIn(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateIn failed: %v", err)
	}
	if result {
		t.Error("Expected false for IN with non-matching value")
	}
}

// TestEvaluateInNot tests evaluateIn with NOT IN
func TestEvaluateInNot(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(5)}

	// Test NOT IN with non-matching value
	expr := &query.InExpr{
		Expr: &query.Identifier{Name: "id"},
		List: []query.Expression{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 2},
			&query.NumberLiteral{Value: 3},
		},
		Not: true,
	}

	result, err := evaluateIn(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateIn failed: %v", err)
	}
	if !result {
		t.Error("Expected true for NOT IN with non-matching value")
	}
}

// TestEvaluateInNotWithMatch tests NOT IN with matching value
func TestEvaluateInNotWithMatch(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
	}
	row := []interface{}{int64(2)}

	// Test NOT IN with matching value
	expr := &query.InExpr{
		Expr: &query.Identifier{Name: "id"},
		List: []query.Expression{
			&query.NumberLiteral{Value: 1},
			&query.NumberLiteral{Value: 2},
			&query.NumberLiteral{Value: 3},
		},
		Not: true,
	}

	result, err := evaluateIn(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateIn failed: %v", err)
	}
	if result {
		t.Error("Expected false for NOT IN with matching value")
	}
}

// TestEvaluateInWithString tests evaluateIn with string values
func TestEvaluateInWithString(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()

	cat := New(nil, pool, nil)

	columns := []ColumnDef{
		{Name: "name", Type: "TEXT"},
	}
	row := []interface{}{"Bob"}

	// Test IN with string matching value
	expr := &query.InExpr{
		Expr: &query.Identifier{Name: "name"},
		List: []query.Expression{
			&query.StringLiteral{Value: "Alice"},
			&query.StringLiteral{Value: "Bob"},
			&query.StringLiteral{Value: "Charlie"},
		},
		Not: false,
	}

	result, err := evaluateIn(cat, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateIn failed: %v", err)
	}
	if !result {
		t.Error("Expected true for IN with matching string value")
	}
}
