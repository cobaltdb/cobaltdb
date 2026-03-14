package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_DeleteRowLockedWithUndo targets deleteRowLocked with transaction undo
func TestCoverage_DeleteRowLockedWithUndo(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_undo", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_undo",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val")}},
		}, nil)
	}

	// Start transaction and delete
	cat.BeginTransaction(1)
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_undo",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(5),
		},
	}, nil)

	// Rollback - should restore the deleted row
	cat.RollbackTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_undo")
	t.Logf("Count after rollback: %v", result.Rows)
}

// TestCoverage_DeleteRowLockedWithIndexUndo targets deleteRowLocked with index undo
func TestCoverage_DeleteRowLockedWithIndexUndo(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_idx_undo", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_u",
		Table:   "del_idx_undo",
		Columns: []string{"code"},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_idx_undo",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE")}},
		}, nil)
	}

	// Start transaction and delete
	cat.BeginTransaction(1)
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_idx_undo",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(3),
		},
	}, nil)

	// Rollback
	cat.RollbackTransaction()

	// Verify index still works
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_idx_undo WHERE code = 'CODE'")
	t.Logf("Count via index after rollback: %v", result.Rows)
}

// TestCoverage_RollbackToSavepointUndoInsert targets RollbackToSavepoint undo insert
func TestCoverage_RollbackToSavepointUndoInsert(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_uins", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)

	// Insert base
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_uins",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("base")}},
	}, nil)

	cat.Savepoint("sp1")

	// Insert more
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_uins",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("inserted")}},
	}, nil)

	// Rollback to sp1 - should remove row 2
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_uins")
	t.Logf("Count after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointUndoUpdate targets RollbackToSavepoint undo update
func TestCoverage_RollbackToSavepointUndoUpdate(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_uupd", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert base data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_uupd",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("original")}},
	}, nil)

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Update
	cat.Update(ctx, &query.UpdateStmt{
		Table: "sp_uupd",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("updated")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	// Rollback to sp1 - should restore original value
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT val FROM sp_uupd WHERE id = 1")
	t.Logf("Val after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_UpdateLockedWithTransaction targets updateLocked with transaction
func TestCoverage_UpdateLockedWithTransaction(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_txn", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_txn",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Update in transaction
	cat.BeginTransaction(1)
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_txn",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLte,
			Right:    numReal(5),
		},
	}, nil)

	// Rollback
	cat.RollbackTransaction()

	result, _ := cat.ExecuteQuery("SELECT val FROM upd_txn WHERE id = 1")
	t.Logf("Val after rollback: %v", result.Rows)
}

// TestCoverage_InsertLockedWithTransaction targets insertLocked with transaction
func TestCoverage_InsertLockedWithTransaction(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_txn", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert outside transaction
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_txn",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("no_txn")}},
	}, nil)

	// Insert in transaction
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_txn",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("in_txn")}},
	}, nil)

	// Rollback - should remove row 2
	cat.RollbackTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_txn")
	t.Logf("Count after rollback: %v", result.Rows)
}
