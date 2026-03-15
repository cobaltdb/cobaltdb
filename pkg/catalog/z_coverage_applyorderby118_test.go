package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogOrderBy(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestApplyOrderByDottedIdentifier118 tests ORDER BY with table.column syntax
func TestApplyOrderByDottedIdentifier118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	// Test data
	rows := [][]interface{}{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 35},
	}

	selectCols := []selectColInfo{
		{name: "id", index: 0, tableName: "users"},
		{name: "name", index: 1, tableName: "users"},
		{name: "age", index: 2, tableName: "users"},
	}

	// ORDER BY with dotted identifier "users.age"
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "users.age"}},
	}

	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	// Should be sorted by age: Bob(25), Alice(30), Charlie(35)
	if result[0][1] != "Bob" {
		t.Errorf("Expected first row to be Bob, got %v", result[0][1])
	}
	if result[2][1] != "Charlie" {
		t.Errorf("Expected last row to be Charlie, got %v", result[2][1])
	}
}

// TestApplyOrderByDottedIdentifierFallback118 tests fallback when table name doesn't match
func TestApplyOrderByDottedIdentifierFallback118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	// No table name in selectCols
	selectCols := []selectColInfo{
		{name: "id", index: 0},
		{name: "name", index: 1},
	}

	// ORDER BY with dotted identifier but table doesn't match - should fallback to column only
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "other_table.name"}},
	}

	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	// Should still sort by name
	if result[0][1] != "Alice" {
		t.Errorf("Expected first row to be Alice, got %v", result[0][1])
	}
}

// TestApplyOrderByQualifiedIdentifier118 tests ORDER BY with QualifiedIdentifier
func TestApplyOrderByQualifiedIdentifier118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{1, "Zebra"},
		{2, "Apple"},
		{3, "Mango"},
	}

	selectCols := []selectColInfo{
		{name: "id", index: 0, tableName: "products"},
		{name: "name", index: 1, tableName: "products"},
	}

	// ORDER BY with QualifiedIdentifier
	orderBy := []*query.OrderByExpr{
		{Expr: &query.QualifiedIdentifier{Table: "products", Column: "name"}},
	}

	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	// Should be sorted: Apple, Mango, Zebra
	if result[0][1] != "Apple" {
		t.Errorf("Expected first row to be Apple, got %v", result[0][1])
	}
	if result[2][1] != "Zebra" {
		t.Errorf("Expected last row to be Zebra, got %v", result[2][1])
	}
}

// TestApplyOrderByQualifiedIdentifierFallback118 tests fallback for QualifiedIdentifier
func TestApplyOrderByQualifiedIdentifierFallback118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{3, "Charlie"},
		{1, "Alice"},
		{2, "Bob"},
	}

	// No table name in selectCols
	selectCols := []selectColInfo{
		{name: "id", index: 0},
		{name: "name", index: 1},
	}

	// ORDER BY with QualifiedIdentifier but table doesn't match - should fallback
	orderBy := []*query.OrderByExpr{
		{Expr: &query.QualifiedIdentifier{Table: "wrong_table", Column: "name"}},
	}

	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	// Should still sort by name: Alice, Bob, Charlie
	if result[0][1] != "Alice" {
		t.Errorf("Expected first row to be Alice, got %v", result[0][1])
	}
}

// TestApplyOrderByExpression118 tests ORDER BY with expression
func TestApplyOrderByExpression118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{1, 10, 100},
		{2, 20, 30},
		{3, 5, 200},
	}

	selectCols := []selectColInfo{
		{name: "id", index: 0},
		{name: "price", index: 1},
		{name: "quantity", index: 2},
	}

	// ORDER BY with expression (not Identifier or QualifiedIdentifier)
	// This tests the default case path
	orderBy := []*query.OrderByExpr{
		{Expr: &query.StringLiteral{Value: "test"}},
	}

	// Should not panic and return rows
	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}
}

// TestApplyOrderByEmptyRows118 tests with empty rows
func TestApplyOrderByEmptyRows118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{}

	selectCols := []selectColInfo{
		{name: "id", index: 0},
	}

	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "id"}},
	}

	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result))
	}
}

// TestApplyOrderByNilOrderBy118 tests with nil ORDER BY
func TestApplyOrderByNilOrderBy118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{3, "Charlie"},
		{1, "Alice"},
		{2, "Bob"},
	}

	selectCols := []selectColInfo{
		{name: "id", index: 0},
		{name: "name", index: 1},
	}

	// Nil ORDER BY - should return rows unchanged
	result := c.applyOrderBy(rows, selectCols, nil)
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	// Should be unchanged
	if result[0][0] != 3 {
		t.Errorf("Expected first row ID to be 3, got %v", result[0][0])
	}
}

// TestApplyOrderByDescending118 tests DESC ordering
func TestApplyOrderByDescending118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	selectCols := []selectColInfo{
		{name: "id", index: 0},
		{name: "name", index: 1},
	}

	// ORDER BY id DESC
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "id"}, Desc: true},
	}

	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	// Should be sorted descending: Charlie(3), Bob(2), Alice(1)
	if result[0][1] != "Charlie" {
		t.Errorf("Expected first row to be Charlie, got %v", result[0][1])
	}
	if result[2][1] != "Alice" {
		t.Errorf("Expected last row to be Alice, got %v", result[2][1])
	}
}

// TestApplyOrderByMultipleColumns118 tests ORDER BY with multiple columns
func TestApplyOrderByMultipleColumns118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{1, "Sales", 100},
		{2, "Sales", 50},
		{3, "Marketing", 200},
		{4, "Sales", 150},
	}

	selectCols := []selectColInfo{
		{name: "id", index: 0},
		{name: "dept", index: 1},
		{name: "amount", index: 2},
	}

	// ORDER BY dept, amount DESC
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "dept"}},
		{Expr: &query.Identifier{Name: "amount"}, Desc: true},
	}

	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(result))
	}

	// Marketing first (alphabetically), then Sales
	// Within Sales: 150, 100, 50 (descending)
	if result[0][1] != "Marketing" {
		t.Errorf("Expected first row dept to be Marketing, got %v", result[0][1])
	}
	if result[1][2] != 150 {
		t.Errorf("Expected second row amount to be 150, got %v", result[1][2])
	}
	if result[3][2] != 50 {
		t.Errorf("Expected last row amount to be 50, got %v", result[3][2])
	}
}

// TestApplyOrderByColumnNotFound118 tests when ORDER BY column doesn't exist
func TestApplyOrderByColumnNotFound118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	selectCols := []selectColInfo{
		{name: "id", index: 0},
		{name: "name", index: 1},
	}

	// ORDER BY non-existent column - column index will be -1
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "nonexistent"}},
	}

	// Should not panic and return rows
	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result))
	}
}

// TestApplyOrderByPositional118 tests ORDER BY with positional reference
func TestApplyOrderByPositional118(t *testing.T) {
	c := newTestCatalogOrderBy(t)

	rows := [][]interface{}{
		{"Charlie", 30},
		{"Alice", 25},
		{"Bob", 35},
	}

	selectCols := []selectColInfo{
		{name: "name", index: 0},
		{name: "age", index: 1},
	}

	// ORDER BY 2 (positional - refers to age column)
	orderBy := []*query.OrderByExpr{
		{Expr: &query.NumberLiteral{Value: 2}},
	}

	result := c.applyOrderBy(rows, selectCols, orderBy)
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	// Should be sorted by age: Alice(25), Charlie(30), Bob(35)
	if result[0][0] != "Alice" {
		t.Errorf("Expected first row to be Alice, got %v", result[0][0])
	}
}
