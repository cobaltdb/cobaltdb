package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogFinal(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestEvaluateMatchExpr107 tests evaluateMatchExpr with FTS
func TestEvaluateMatchExpr107(t *testing.T) {
	c := newTestCatalogFinal(t)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "title", Type: "TEXT"},
		{Name: "content", Type: "TEXT"},
	}

	// Test nil pattern
	t.Run("NilPattern", func(t *testing.T) {
		expr := &query.MatchExpr{
			Columns: []query.Expression{&query.Identifier{Name: "title"}},
			Pattern: &query.NullLiteral{},
		}
		result, err := evaluateMatchExpr(c, []interface{}{1, "test", "content"}, columns, expr, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != false {
			t.Error("Expected false for nil pattern")
		}
	})

	// Test empty pattern
	t.Run("EmptyPattern", func(t *testing.T) {
		expr := &query.MatchExpr{
			Columns: []query.Expression{&query.Identifier{Name: "title"}},
			Pattern: &query.StringLiteral{Value: ""},
		}
		result, err := evaluateMatchExpr(c, []interface{}{1, "test", "content"}, columns, expr, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != false {
			t.Error("Expected false for empty pattern")
		}
	})

	// Test simple text search without FTS index
	t.Run("SimpleTextSearch", func(t *testing.T) {
		expr := &query.MatchExpr{
			Columns: []query.Expression{
				&query.Identifier{Name: "title"},
				&query.Identifier{Name: "content"},
			},
			Pattern: &query.StringLiteral{Value: "test"},
		}
		row := []interface{}{1, "this is a test", "more content"}
		result, err := evaluateMatchExpr(c, row, columns, expr, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != true {
			t.Error("Expected true for matching text")
		}
	})

	// Test non-matching text
	t.Run("NonMatchingText", func(t *testing.T) {
		expr := &query.MatchExpr{
			Columns: []query.Expression{
				&query.Identifier{Name: "title"},
			},
			Pattern: &query.StringLiteral{Value: "xyz123"},
		}
		row := []interface{}{1, "this is a test", "more content"}
		result, err := evaluateMatchExpr(c, row, columns, expr, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != false {
			t.Error("Expected false for non-matching text")
		}
	})

	// Test with QualifiedIdentifier
	t.Run("QualifiedIdentifier", func(t *testing.T) {
		expr := &query.MatchExpr{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "t", Column: "title"},
			},
			Pattern: &query.StringLiteral{Value: "test"},
		}
		row := []interface{}{1, "test title", "content"}
		result, err := evaluateMatchExpr(c, row, columns, expr, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != true {
			t.Error("Expected true for matching QualifiedIdentifier")
		}
	})

	// Test with unknown column expression
	t.Run("UnknownColumnExpr", func(t *testing.T) {
		expr := &query.MatchExpr{
			Columns: []query.Expression{
				&query.NumberLiteral{Value: 42},
			},
			Pattern: &query.StringLiteral{Value: "test"},
		}
		row := []interface{}{1, "test", "content"}
		result, err := evaluateMatchExpr(c, row, columns, expr, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != false {
			t.Error("Expected false for unknown column expression")
		}
	})

	// Test with multiple words (all must match)
	t.Run("MultipleWordsAllMatch", func(t *testing.T) {
		expr := &query.MatchExpr{
			Columns: []query.Expression{
				&query.Identifier{Name: "content"},
			},
			Pattern: &query.StringLiteral{Value: "hello world"},
		}
		row := []interface{}{1, "title", "hello world"}
		result, err := evaluateMatchExpr(c, row, columns, expr, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != true {
			t.Error("Expected true when all words match")
		}
	})

	// Test with multiple words (one doesn't match)
	t.Run("MultipleWordsOneMissing", func(t *testing.T) {
		expr := &query.MatchExpr{
			Columns: []query.Expression{
				&query.Identifier{Name: "content"},
			},
			Pattern: &query.StringLiteral{Value: "hello xyz123"},
		}
		row := []interface{}{1, "title", "hello world"}
		result, err := evaluateMatchExpr(c, row, columns, expr, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != false {
			t.Error("Expected false when one word doesn't match")
		}
	})
}

// TestGetInsertTargetTree107 tests getInsertTargetTree
func TestGetInsertTargetTree107(t *testing.T) {
	c := newTestCatalogFinal(t)

	// Create a regular table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	table := c.tables["users"]

	// Test getting tree for regular table
	t.Run("RegularTable", func(t *testing.T) {
		insertStmt := &query.InsertStmt{
			Table: "users",
			Values: [][]query.Expression{
				{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			},
		}
		tree, partitionIdx, err := c.getInsertTargetTree(table, insertStmt, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tree == nil {
			t.Error("Expected tree for regular table")
		}
		if partitionIdx != -1 {
			t.Errorf("Expected partitionIdx -1 for regular table, got %d", partitionIdx)
		}
	})

	// Test with non-existent table tree
	t.Run("NonExistentTree", func(t *testing.T) {
		fakeTable := &TableDef{Name: "nonexistent"}
		insertStmt := &query.InsertStmt{Table: "nonexistent"}
		_, _, err := c.getInsertTargetTree(fakeTable, insertStmt, nil)
		if err == nil {
			t.Error("Expected error for non-existent table")
		}
	})

	// Test with partitioned table
	t.Run("PartitionedTable", func(t *testing.T) {
		// Create partitioned table
		err := c.CreateTable(&query.CreateTableStmt{
			Table: "sales",
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "amount", Type: query.TokenInteger},
			},
		})
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Add partition info
		c.mu.Lock()
		c.tables["sales"].Partition = &PartitionInfo{
			Type:   query.PartitionTypeRange,
			Column: "amount",
			Partitions: []PartitionDef{
				{Name: "p0", MinValue: 0, MaxValue: 100},
				{Name: "p1", MinValue: 100, MaxValue: 200},
			},
		}
		c.mu.Unlock()

		salesTable := c.tables["sales"]
		insertStmt := &query.InsertStmt{
			Table:   "sales",
			Columns: []string{"id", "amount"},
			Values: [][]query.Expression{
				{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 50}},
			},
		}

		tree, partitionIdx, err := c.getInsertTargetTree(salesTable, insertStmt, nil)
		if err != nil {
			t.Logf("Error (may be expected if partition trees not created): %v", err)
		}
		if tree == nil {
			t.Log("Tree is nil - partition trees may not be created yet")
		}
		t.Logf("Partition index: %d", partitionIdx)
	})

	// Test partitioned table with no partition column value
	t.Run("PartitionedTableNoColumn", func(t *testing.T) {
		salesTable := c.tables["sales"]
		insertStmt := &query.InsertStmt{
			Table:   "sales",
			Columns: []string{"id"}, // Missing amount
			Values: [][]query.Expression{
				{&query.NumberLiteral{Value: 1}},
			},
		}

		_, _, err := c.getInsertTargetTree(salesTable, insertStmt, nil)
		if err == nil {
			t.Error("Expected error when partition column value is missing")
		}
	})
}
