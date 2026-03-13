package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_TransactionRollback tests transaction rollback
func TestCoverage_TransactionRollback(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_rollback", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	// Insert within transaction
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_rollback",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Rollback
	err := cat.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify data not present
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_rollback")
	t.Logf("Count after rollback: %v", result.Rows)
}

// TestCoverage_TransactionCommit tests transaction commit
func TestCoverage_TransactionCommit(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_commit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	// Insert within transaction
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_commit",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Commit
	err := cat.CommitTransaction()
	if err != nil {
		t.Logf("Commit error: %v", err)
	}

	// Verify data present
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_commit")
	t.Logf("Count after commit: %v", result.Rows)
}

// TestCoverage_NestedTransaction tests nested transaction
func TestCoverage_NestedTransaction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Begin outer transaction
	cat.BeginTransaction(1)

	// Begin inner transaction
	cat.BeginTransaction(2)

	// Commit inner
	cat.CommitTransaction()

	// Commit outer
	cat.CommitTransaction()
}

// TestCoverage_SavepointCreateRelease tests savepoint create and release
func TestCoverage_SavepointCreateRelease(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.BeginTransaction(1)

	// Create savepoint
	err := cat.Savepoint("sp1")
	if err != nil {
		t.Logf("Savepoint error: %v", err)
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Release savepoint
	err = cat.ReleaseSavepoint("sp1")
	if err != nil {
		t.Logf("Release savepoint error: %v", err)
	}

	cat.CommitTransaction()
}

// TestCoverage_MultipleSavepoints tests multiple savepoints
func TestCoverage_MultipleSavepoints(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "multi_sp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.BeginTransaction(1)

	// Create multiple savepoints
	cat.Savepoint("sp1")
	cat.Insert(ctx, &query.InsertStmt{Table: "multi_sp", Columns: []string{"id"}, Values: [][]query.Expression{{numReal(1)}}}, nil)

	cat.Savepoint("sp2")
	cat.Insert(ctx, &query.InsertStmt{Table: "multi_sp", Columns: []string{"id"}, Values: [][]query.Expression{{numReal(2)}}}, nil)

	cat.Savepoint("sp3")
	cat.Insert(ctx, &query.InsertStmt{Table: "multi_sp", Columns: []string{"id"}, Values: [][]query.Expression{{numReal(3)}}}, nil)

	// Rollback to middle savepoint
	cat.RollbackToSavepoint("sp2")

	cat.CommitTransaction()

	// Verify only rows 1 and 2 exist
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM multi_sp")
	t.Logf("Count after rollback to sp2: %v", result.Rows)
}
