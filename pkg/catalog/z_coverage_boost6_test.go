package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_AlterTable tests ALTER TABLE operations
func TestCoverage_AlterTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create initial table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "alter_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert some data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "alter_test",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)

	// Add column
	err := cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "alter_test",
		Action: "ADD",
		Column: query.ColumnDef{
			Name: "new_col",
			Type: query.TokenInteger,
		},
	})
	if err != nil {
		t.Logf("Add column error: %v", err)
	}

	// Rename column
	err = cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "alter_test",
		Action:  "RENAME_COLUMN",
		OldName: "name",
		NewName: "full_name",
	})
	if err != nil {
		t.Logf("Rename column error: %v", err)
	}

	// Rename table
	err = cat.AlterTableRename(&query.AlterTableStmt{
		Table:   "alter_test",
		Action:  "RENAME_TABLE",
		NewName: "alter_test_renamed",
	})
	if err != nil {
		t.Logf("Rename table error: %v", err)
	}
}

// TestCoverage_Views tests view operations
func TestCoverage_Views(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create base table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "view_base",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "view_base",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(200)}},
	}, nil)

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "view_base"},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "val"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 100},
		},
	}
	err := cat.CreateView("test_view", viewStmt)
	if err != nil {
		t.Logf("Create view error: %v", err)
	}

	// Query view
	result, err := cat.ExecuteQuery("SELECT * FROM test_view")
	if err != nil {
		t.Logf("Query view error: %v", err)
	} else {
		t.Logf("View returned %d rows", len(result.Rows))
	}

	// Get view
	_, err = cat.GetView("test_view")
	if err != nil {
		t.Logf("Get view error: %v", err)
	}

	// Drop view
	err = cat.DropView("test_view")
	if err != nil {
		t.Logf("Drop view error: %v", err)
	}

	// Try to get non-existent view
	_, err = cat.GetView("non_existent_view")
	if err == nil {
		t.Error("Expected error for non-existent view")
	}
}

// TestCoverage_Triggers tests trigger operations
func TestCoverage_Triggers(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create tables
	cat.CreateTable(&query.CreateTableStmt{
		Table: "trig_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	cat.CreateTable(&query.CreateTableStmt{
		Table: "trig_audit",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "action", Type: query.TokenText},
		},
	})

	// Create trigger
	triggerStmt := &query.CreateTriggerStmt{
		Name:  "test_trigger",
		Table: "trig_main",
		Time:  "AFTER",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "trig_audit",
				Columns: []string{"id", "action"},
				Values:  [][]query.Expression{{numReal(1), strReal("inserted")}},
			},
		},
	}
	err := cat.CreateTrigger(triggerStmt)
	if err != nil {
		t.Logf("Create trigger error: %v", err)
	}

	// Get trigger
	_, err = cat.GetTrigger("test_trigger")
	if err != nil {
		t.Logf("Get trigger error: %v", err)
	}

	// Drop trigger
	err = cat.DropTrigger("test_trigger")
	if err != nil {
		t.Logf("Drop trigger error: %v", err)
	}

	// Try to get non-existent trigger
	_, err = cat.GetTrigger("non_existent_trigger")
	if err == nil {
		t.Error("Expected error for non-existent trigger")
	}
}

// TestCoverage_FTS tests full-text search operations
func TestCoverage_FTS(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create table with text column
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fts_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "content", Type: query.TokenText},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fts_test",
		Columns: []string{"id", "content"},
		Values:  [][]query.Expression{{numReal(1), strReal("hello world")}, {numReal(2), strReal("hello universe")}},
	}, nil)

	// Create FTS index
	err := cat.CreateFTSIndex("fts_idx", "fts_test", []string{"content"})
	if err != nil {
		t.Logf("Create FTS index error: %v", err)
	}

	// Search FTS
	_, err = cat.SearchFTS("fts_idx", "hello")
	if err != nil {
		t.Logf("FTS search error: %v", err)
	}

	// Get FTS index
	_, err = cat.GetFTSIndex("fts_idx")
	if err != nil {
		t.Logf("Get FTS index error: %v", err)
	}

	// List FTS indexes
	indexes := cat.ListFTSIndexes()
	t.Logf("FTS indexes: %v", indexes)

	// Drop FTS index
	err = cat.DropFTSIndex("fts_idx")
	if err != nil {
		t.Logf("Drop FTS index error: %v", err)
	}
}
