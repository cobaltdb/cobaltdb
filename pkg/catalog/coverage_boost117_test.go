package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointAlterAddColumn tests rollback of ALTER TABLE ADD COLUMN
func TestCoverage_RollbackToSavepointAlterAddColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "alter_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "alter_test",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Begin transaction
	c.BeginTransaction(1)
	c.Savepoint("sp1")

	// Add column after savepoint
	c.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "alter_test",
		Action: "ADD",
		Column: query.ColumnDef{
			Name: "new_col",
			Type: query.TokenInteger,
		},
	})

	// Verify column exists
	tbl, _ := c.getTableLocked("alter_test")
	if len(tbl.Columns) != 3 {
		t.Errorf("Expected 3 columns after add, got %d", len(tbl.Columns))
	}

	// Rollback to savepoint
	err := c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	// Verify column was removed
	tbl, _ = c.getTableLocked("alter_test")
	if len(tbl.Columns) != 2 {
		t.Errorf("Expected 2 columns after rollback, got %d", len(tbl.Columns))
	}

	c.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointAlterDropColumn tests rollback of ALTER TABLE DROP COLUMN
func TestCoverage_RollbackToSavepointAlterDropColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with multiple columns
	c.CreateTable(&query.CreateTableStmt{
		Table: "alter_drop_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "extra", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "alter_drop_test",
		Columns: []string{"id", "name", "extra"},
		Values:  [][]query.Expression{{numReal(1), strReal("test"), numReal(100)}},
	}, nil)

	// Create index on the column we'll drop (to test index restoration)
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_extra",
		Table:   "alter_drop_test",
		Columns: []string{"extra"},
	})

	// Begin transaction
	c.BeginTransaction(1)
	c.Savepoint("sp1")

	// Drop column after savepoint
	c.AlterTableDropColumn(&query.AlterTableStmt{
		Table:  "alter_drop_test",
		Action: "DROP",
		Column: query.ColumnDef{Name: "extra"},
	})

	// Verify column was dropped
	tbl, _ := c.getTableLocked("alter_drop_test")
	if len(tbl.Columns) != 2 {
		t.Errorf("Expected 2 columns after drop, got %d", len(tbl.Columns))
	}

	// Rollback to savepoint
	err := c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	// Verify column was restored
	tbl, _ = c.getTableLocked("alter_drop_test")
	if len(tbl.Columns) != 3 {
		t.Errorf("Expected 3 columns after rollback, got %d", len(tbl.Columns))
	}

	// Verify index was restored
	if _, exists := c.indexes["idx_extra"]; !exists {
		t.Error("Index should be restored after rollback")
	}

	c.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointAlterRenameTable tests rollback of ALTER TABLE RENAME
func TestCoverage_RollbackToSavepointAlterRenameTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "old_name",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "old_name",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Create index
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_old",
		Table:   "old_name",
		Columns: []string{"id"},
	})

	// Begin transaction
	c.BeginTransaction(1)
	c.Savepoint("sp1")

	// Rename table after savepoint
	c.AlterTableRename(&query.AlterTableStmt{
		Table:   "old_name",
		Action:  "RENAME_TABLE",
		NewName: "new_name",
	})

	// Verify old name doesn't exist
	_, err := c.getTableLocked("old_name")
	if err == nil {
		t.Error("Old table name should not exist after rename")
	}

	// Verify new name exists
	_, err = c.getTableLocked("new_name")
	if err != nil {
		t.Errorf("New table name should exist: %v", err)
	}

	// Rollback to savepoint
	err = c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	// Verify old name is restored
	_, err = c.getTableLocked("old_name")
	if err != nil {
		t.Errorf("Old table name should exist after rollback: %v", err)
	}

	// Verify new name doesn't exist
	_, err = c.getTableLocked("new_name")
	if err == nil {
		t.Error("New table name should not exist after rollback")
	}

	// Verify index table name was restored
	if c.indexes["idx_old"].TableName != "old_name" {
		t.Error("Index table name should be restored to old_name")
	}

	c.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointAlterRenameColumn tests rollback of ALTER TABLE RENAME COLUMN
func TestCoverage_RollbackToSavepointAlterRenameColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "rename_col_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "old_col", Type: query.TokenText},
		},
	})

	// Create index on column
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_old_col",
		Table:   "rename_col_test",
		Columns: []string{"old_col"},
	})

	// Begin transaction
	c.BeginTransaction(1)
	c.Savepoint("sp1")

	// Rename column after savepoint
	c.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "rename_col_test",
		Action:  "RENAME_COLUMN",
		OldName: "old_col",
		NewName: "new_col",
	})

	// Verify column was renamed
	tbl, _ := c.getTableLocked("rename_col_test")
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "new_col" {
			found = true
			break
		}
	}
	if !found {
		t.Error("New column name should exist after rename")
	}

	// Rollback to savepoint
	err := c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	// Verify old column name is restored
	tbl, _ = c.getTableLocked("rename_col_test")
	found = false
	for _, col := range tbl.Columns {
		if col.Name == "old_col" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Old column name should exist after rollback")
	}

	// Verify index column name was restored
	if c.indexes["idx_old_col"].Columns[0] != "old_col" {
		t.Error("Index column name should be restored to old_col")
	}

	c.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointErrors tests error handling in rollback
func TestCoverage_RollbackToSavepointErrors2(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Test rollback without transaction
	err := c.RollbackToSavepoint("sp1")
	if err == nil {
		t.Error("Expected error when rolling back without transaction")
	}

	// Begin transaction
	c.BeginTransaction(1)

	// Test rollback to non-existent savepoint
	err = c.RollbackToSavepoint("nonexistent")
	if err == nil {
		t.Error("Expected error when rolling back to non-existent savepoint")
	}

	c.RollbackTransaction()
}

// TestCoverage_ReleaseSavepoint tests RELEASE SAVEPOINT functionality
func TestCoverage_ReleaseSavepoint2(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Test release without transaction
	err := c.ReleaseSavepoint("sp1")
	if err == nil {
		t.Error("Expected error when releasing savepoint without transaction")
	}

	// Begin transaction
	c.BeginTransaction(1)

	// Create savepoints
	c.Savepoint("sp1")
	c.Savepoint("sp2")
	c.Savepoint("sp3")

	// Release sp2 (should also release sp3)
	err = c.ReleaseSavepoint("sp2")
	if err != nil {
		t.Errorf("ReleaseSavepoint failed: %v", err)
	}

	// sp1 should still exist
	err = c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Errorf("Rollback to sp1 should succeed: %v", err)
	}

	// sp3 should not exist anymore
	err = c.RollbackToSavepoint("sp3")
	if err == nil {
		t.Error("Expected error when rolling back to released savepoint sp3")
	}

	c.RollbackTransaction()
}

// TestCoverage_ReleaseSavepointNonExistent tests releasing non-existent savepoint
func TestCoverage_ReleaseSavepointNonExistent(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.BeginTransaction(1)
	c.Savepoint("sp1")

	// Try to release non-existent savepoint
	err := c.ReleaseSavepoint("nonexistent")
	if err == nil {
		t.Error("Expected error when releasing non-existent savepoint")
	}

	c.RollbackTransaction()
}
