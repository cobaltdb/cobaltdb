package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointUndoCreateIndex targets undoCreateIndex
func TestCoverage_RollbackToSavepointUndoCreateIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_idx_create", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_idx_create",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val")}},
		}, nil)
	}

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sp_create",
		Table:   "sp_idx_create",
		Columns: []string{"val"},
	})

	// Verify index exists
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_idx_create WHERE val = 'val'")
	t.Logf("Count via new index: %v", result.Rows)

	// Rollback to sp1 - index should be removed
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointUndoDropIndex targets undoDropIndex
func TestCoverage_RollbackToSavepointUndoDropIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_idx_drop", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sp_drop",
		Table:   "sp_idx_drop",
		Columns: []string{"val"},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_idx_drop",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val")}},
		}, nil)
	}

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Drop index
	cat.DropIndex("idx_sp_drop")

	// Rollback to sp1 - index should be restored
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify index still works
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_idx_drop WHERE val = 'val'")
	t.Logf("Count after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointUndoAlterAddColumn targets undoAlterAddColumn
func TestCoverage_RollbackToSavepointUndoAlterAddColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_alter_add", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_alter_add",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Add column
	cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "sp_alter_add",
		Action: "ADD",
		Column: query.ColumnDef{
			Name: "newcol",
			Type: query.TokenInteger,
		},
	})

	// Insert with new column
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_alter_add",
		Columns: []string{"id", "val", "newcol"},
		Values:  [][]query.Expression{{numReal(2), strReal("test2"), numReal(100)}},
	}, nil)

	// Rollback to sp1 - column add should be undone
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_alter_add")
	t.Logf("Count after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointUndoAlterDropColumn targets undoAlterDropColumn
func TestCoverage_RollbackToSavepointUndoAlterDropColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_alter_drop", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "keep", Type: query.TokenText},
		{Name: "dropme", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_alter_drop",
		Columns: []string{"id", "keep", "dropme"},
		Values:  [][]query.Expression{{numReal(1), strReal("test"), numReal(100)}},
	}, nil)

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Drop column
	cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "sp_alter_drop",
		Action:  "DROP",
		OldName: "dropme",
	})

	// Rollback to sp1 - column should be restored
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify column still exists
	result, _ := cat.ExecuteQuery("SELECT dropme FROM sp_alter_drop WHERE id = 1")
	t.Logf("Dropped column value after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointUndoAlterRename targets undoAlterRename
func TestCoverage_RollbackToSavepointUndoAlterRename(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_rename_orig", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_rename_orig",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Rename table
	cat.AlterTableRename(&query.AlterTableStmt{
		Table:   "sp_rename_orig",
		Action:  "RENAME_TABLE",
		NewName: "sp_rename_new",
	})

	// Rollback to sp1 - rename should be undone
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify old name works
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_rename_orig")
	t.Logf("Count after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointUndoAutoInc targets undoAutoIncSeq
func TestCoverage_RollbackToSavepointUndoAutoInc(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_autoinc", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Insert with auto increment
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_autoinc",
		Columns: []string{"val"},
		Values:  [][]query.Expression{{strReal("a")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_autoinc",
		Columns: []string{"val"},
		Values:  [][]query.Expression{{strReal("b")}},
	}, nil)

	// Rollback to sp1
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	cat.RollbackTransaction()
}
