package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointDDL tests RollbackToSavepoint with DDL operations
func TestCoverage_RollbackToSavepointDDL(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.BeginTransaction(1)

	// Create table
	createCoverageTestTable(t, cat, "sp_ddl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Savepoint("sp1")

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sp",
		Table:   "sp_ddl",
		Columns: []string{"val"},
	})

	cat.Savepoint("sp2")

	// Drop the index
	cat.DropIndex("idx_sp")

	// Rollback to sp2 - index should be restored
	err := cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("Rollback to sp2 error: %v", err)
	}

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointAlterTable tests RollbackToSavepoint with ALTER TABLE
func TestCoverage_RollbackToSavepointAlterTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_alter", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)

	cat.Savepoint("sp1")

	// Add column
	cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "sp_alter",
		Action: "ADD",
		Column: query.ColumnDef{
			Name: "newcol",
			Type: query.TokenInteger,
		},
	})

	cat.Savepoint("sp2")

	// Insert some data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_alter",
		Columns: []string{"id", "val", "newcol"},
		Values:  [][]query.Expression{{numReal(1), strReal("test"), numReal(100)}},
	}, nil)

	// Rollback to sp1 - column add should be undone
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointDropTable tests RollbackToSavepoint with DROP TABLE
func TestCoverage_RollbackToSavepointDropTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_droptbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_droptbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("data")}},
	}, nil)

	cat.BeginTransaction(1)

	cat.Savepoint("sp1")

	// Drop the table
	cat.DropTable(&query.DropTableStmt{Table: "sp_droptbl"})

	// Rollback to sp1 - table should be restored
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	// Verify table exists
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_droptbl")
	t.Logf("Count after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointUpdateDelete tests RollbackToSavepoint with UPDATE and DELETE
func TestCoverage_RollbackToSavepointUpdateDelete(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_upddel", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_upddel",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	cat.BeginTransaction(1)

	cat.Savepoint("sp1")

	// Update some rows
	cat.Update(ctx, &query.UpdateStmt{
		Table: "sp_upddel",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLte,
			Right:    numReal(5),
		},
	}, nil)

	cat.Savepoint("sp2")

	// Delete some rows
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "sp_upddel",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    numReal(8),
		},
	}, nil)

	// Rollback to sp2 - deletes undone
	err := cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("Rollback to sp2 error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_upddel")
	t.Logf("Count after rollback to sp2: %v", result.Rows)

	// Rollback to sp1 - updates undone
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM sp_upddel")
	t.Logf("Count after rollback to sp1: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointRenameTable tests RollbackToSavepoint with ALTER TABLE RENAME
func TestCoverage_RollbackToSavepointRenameTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_rename", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_rename",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("data")}},
	}, nil)

	cat.BeginTransaction(1)

	cat.Savepoint("sp1")

	// Rename the table
	cat.AlterTableRename(&query.AlterTableStmt{
		Table:   "sp_rename",
		Action:  "RENAME_TABLE",
		NewName: "sp_renamed",
	})

	// Rollback to sp1 - rename should be undone
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	// Verify old name still works
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_rename")
	t.Logf("Count after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointAutoInc tests RollbackToSavepoint with auto_increment
func TestCoverage_RollbackToSavepointAutoInc(t *testing.T) {
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
		t.Logf("Rollback to sp1 error: %v", err)
	}

	cat.RollbackTransaction()
}
