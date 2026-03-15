package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogMatch(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestEvaluateMatchExprFallback113 tests evaluateMatchExpr without FTS index (simple text search)
func TestEvaluateMatchExprFallback113(t *testing.T) {
	c := newTestCatalogMatch(t)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "title", Type: "TEXT"},
		{Name: "content", Type: "TEXT"},
	}

	tests := []struct {
		name     string
		row      []interface{}
		pattern  string
		expected bool
	}{
		{
			name:     "Simple match",
			row:      []interface{}{1, "Hello World", "First article"},
			pattern:  "hello",
			expected: true,
		},
		{
			name:     "No match",
			row:      []interface{}{1, "Hello World", "First article"},
			pattern:  "xyz",
			expected: false,
		},
		{
			name:     "Case insensitive match",
			row:      []interface{}{1, "HELLO World", "First article"},
			pattern:  "hello",
			expected: true,
		},
		{
			name:     "Multiple words match",
			row:      []interface{}{1, "Hello World Test", "First article"},
			pattern:  "hello world",
			expected: true,
		},
		{
			name:     "Multiple words one missing",
			row:      []interface{}{1, "Hello Test", "First article"},
			pattern:  "hello world",
			expected: false,
		},
		{
			name:     "Partial word match",
			row:      []interface{}{1, "Hello World", "First article"},
			pattern:  "wor",
			expected: true, // substring match
		},
		{
			name:     "Number in text",
			row:      []interface{}{1, "Item 123", "Description"},
			pattern:  "123",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &query.MatchExpr{
				Columns: []query.Expression{
					&query.Identifier{Name: "title"},
					&query.Identifier{Name: "content"},
				},
				Pattern: &query.StringLiteral{Value: tt.pattern},
			}
			result, err := evaluateMatchExpr(c, tt.row, columns, expr, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v for pattern '%s'", tt.expected, result, tt.pattern)
			}
		})
	}
}

// TestEvaluateMatchExprWithQualifiedIdentifier113 tests with qualified identifiers
func TestEvaluateMatchExprWithQualifiedIdentifier113(t *testing.T) {
	c := newTestCatalogMatch(t)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "title", Type: "TEXT"},
	}

	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Table: "articles", Column: "title"},
		},
		Pattern: &query.StringLiteral{Value: "test"},
	}

	row := []interface{}{1, "Test Article"}
	result, err := evaluateMatchExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Error("Expected true for qualified identifier match")
	}
}

// TestEvaluateMatchExprWithNumberPattern113 tests numeric pattern
func TestEvaluateMatchExprWithNumberPattern113(t *testing.T) {
	c := newTestCatalogMatch(t)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "code", Type: "TEXT"},
	}

	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "code"},
		},
		Pattern: &query.NumberLiteral{Value: 42},
	}

	row := []interface{}{1, "Code 42 is the answer"}
	result, err := evaluateMatchExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Error("Expected true for number pattern in text")
	}
}
