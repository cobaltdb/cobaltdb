package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogFTS(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestEvaluateMatchExprWithFTSIndex115 tests evaluateMatchExpr using FTS index
func TestEvaluateMatchExprWithFTSIndex115(t *testing.T) {
	c := newTestCatalogFTS(t)
	ctx := context.Background()

	// Create articles table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "articles",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "title", Type: query.TokenText},
			{Name: "content", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create articles table: %v", err)
	}

	// Insert test data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "articles",
		Columns: []string{"id", "title", "content"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Hello World"}, &query.StringLiteral{Value: "First article about Go"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Database Tutorial"}, &query.StringLiteral{Value: "Learn SQL basics"}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Go Programming"}, &query.StringLiteral{Value: "Advanced Go tips"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create FTS index on title and content
	err = c.CreateFTSIndex("idx_articles_fts", "articles", []string{"title", "content"})
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Get columns for the table
	table, _ := c.getTableLocked("articles")
	columns := table.Columns

	tests := []struct {
		name     string
		row      []interface{}
		pattern  string
		expected bool
	}{
		{
			name:     "Match word in title",
			row:      []interface{}{1, "Hello World", "First article about Go"},
			pattern:  "hello",
			expected: true,
		},
		{
			name:     "Match word in content",
			row:      []interface{}{2, "Database Tutorial", "Learn SQL basics"},
			pattern:  "sql",
			expected: true,
		},
		{
			name:     "No match",
			row:      []interface{}{1, "Hello World", "First article about Go"},
			pattern:  "python",
			expected: false,
		},
		{
			name:     "Multiple words match",
			row:      []interface{}{3, "Go Programming", "Advanced Go tips"},
			pattern:  "go advanced",
			expected: true,
		},
		{
			name:     "Multiple words one missing",
			row:      []interface{}{1, "Hello World", "First article about Go"},
			pattern:  "hello python",
			expected: false,
		},
		{
			name:     "Case insensitive match",
			row:      []interface{}{2, "Database Tutorial", "Learn SQL basics"},
			pattern:  "DATABASE",
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

// TestEvaluateMatchExprFTSWithQualifiedId115 tests FTS index with qualified identifiers
func TestEvaluateMatchExprFTSWithQualifiedId115(t *testing.T) {
	c := newTestCatalogFTS(t)
	ctx := context.Background()

	// Create articles table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "articles2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "title", Type: query.TokenText},
			{Name: "body", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "articles2",
		Columns: []string{"id", "title", "body"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Test Title"}, &query.StringLiteral{Value: "Test Body Content"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create FTS index
	err = c.CreateFTSIndex("idx_articles2_fts", "articles2", []string{"title", "body"})
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Get columns
	table, _ := c.getTableLocked("articles2")
	columns := table.Columns

	// Test with qualified identifiers
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Table: "articles2", Column: "title"},
			&query.QualifiedIdentifier{Table: "articles2", Column: "body"},
		},
		Pattern: &query.StringLiteral{Value: "test"},
	}

	row := []interface{}{1, "Test Title", "Test Body Content"}
	result, err := evaluateMatchExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Errorf("Expected true for qualified identifier match, got %v", result)
	}
}

// TestEvaluateMatchExprFTSNoMatchingColumns115 tests when FTS index columns don't match
func TestEvaluateMatchExprFTSNoMatchingColumns115(t *testing.T) {
	c := newTestCatalogFTS(t)
	ctx := context.Background()

	// Create table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "docs",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "title", Type: query.TokenText},
			{Name: "content", Type: query.TokenText},
			{Name: "author", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "docs",
		Columns: []string{"id", "title", "content", "author"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Title"}, &query.StringLiteral{Value: "Content"}, &query.StringLiteral{Value: "John"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create FTS index on title and content only (not author)
	err = c.CreateFTSIndex("idx_docs_fts", "docs", []string{"title", "content"})
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Get columns
	table, _ := c.getTableLocked("docs")
	columns := table.Columns

	// Test with different columns than FTS index (should use fallback)
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "author"},
		},
		Pattern: &query.StringLiteral{Value: "john"},
	}

	row := []interface{}{1, "Title", "Content", "John"}
	result, err := evaluateMatchExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Errorf("Expected true for author match via fallback, got %v", result)
	}
}

// TestEvaluateMatchExprFTSNullPattern115 tests with null pattern
func TestEvaluateMatchExprFTSNullPattern115(t *testing.T) {
	c := newTestCatalogFTS(t)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "title", Type: "TEXT"},
	}

	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "title"},
		},
		Pattern: &query.NullLiteral{},
	}

	row := []interface{}{1, "Hello World"}
	result, err := evaluateMatchExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != false {
		t.Errorf("Expected false for null pattern, got %v", result)
	}
}

// TestEvaluateMatchExprFTSEmptyPattern115 tests with empty pattern
func TestEvaluateMatchExprFTSEmptyPattern115(t *testing.T) {
	c := newTestCatalogFTS(t)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "title", Type: "TEXT"},
	}

	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "title"},
		},
		Pattern: &query.StringLiteral{Value: ""},
	}

	row := []interface{}{1, "Hello World"}
	result, err := evaluateMatchExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != false {
		t.Errorf("Expected false for empty pattern, got %v", result)
	}
}

// TestEvaluateMatchExprFTSWhitespaceOnly115 tests with whitespace-only pattern
func TestEvaluateMatchExprFTSWhitespaceOnly115(t *testing.T) {
	c := newTestCatalogFTS(t)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "title", Type: "TEXT"},
	}

	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "title"},
		},
		Pattern: &query.StringLiteral{Value: "   "},
	}

	row := []interface{}{1, "Hello World"}
	result, err := evaluateMatchExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != false {
		t.Errorf("Expected false for whitespace-only pattern, got %v", result)
	}
}

// TestEvaluateMatchExprFTSNoMatchingColumnsEmptyText115 tests with no matching columns and empty text
func TestEvaluateMatchExprFTSNoMatchingColumnsEmptyText115(t *testing.T) {
	c := newTestCatalogFTS(t)
	ctx := context.Background()

	// Create table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "empty_docs",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "title", Type: query.TokenText},
			{Name: "content", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with empty content
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "empty_docs",
		Columns: []string{"id", "title", "content"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Title"}, &query.NullLiteral{}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create FTS index
	err = c.CreateFTSIndex("idx_empty_docs", "empty_docs", []string{"title", "content"})
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Get columns
	table, _ := c.getTableLocked("empty_docs")
	columns := table.Columns

	// Test with pattern that doesn't match
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "title"},
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.StringLiteral{Value: "nonexistent"},
	}

	row := []interface{}{1, "Title", nil}
	result, err := evaluateMatchExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != false {
		t.Errorf("Expected false for no match, got %v", result)
	}
}
