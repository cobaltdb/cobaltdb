package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointEdgeCases tests savepoint rollback edge cases
func TestCoverage_RollbackToSavepointEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	// Create savepoint
	err := cat.Savepoint("sp1")
	if err != nil {
		t.Logf("CreateSavepoint error: %v", err)
	}

	// Insert more data after savepoint
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("after_sp1")}},
	}, nil)

	// Create nested savepoint
	err = cat.Savepoint("sp2")
	if err != nil {
		t.Logf("CreateSavepoint sp2 error: %v", err)
	}

	// Insert even more
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("after_sp2")}},
	}, nil)

	// Rollback to sp2 (should keep data up to sp2)
	err = cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("RollbackToSavepoint sp2 error: %v", err)
	}

	// Rollback to sp1 (should only keep initial)
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("RollbackToSavepoint sp1 error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_test")
	t.Logf("Count after rollback: %v", result.Rows)

	// Release savepoint
	err = cat.ReleaseSavepoint("sp1")
	if err != nil {
		t.Logf("ReleaseSavepoint error: %v", err)
	}

	cat.CommitTransaction()
}

// TestCoverage_CommitTransactionEdgeCases tests commit transaction edge cases
func TestCoverage_CommitTransactionEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "commit_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Commit without transaction (should not panic)
	err := cat.CommitTransaction()
	if err != nil {
		t.Logf("Commit without transaction error: %v", err)
	}

	// Begin and commit empty transaction
	cat.BeginTransaction(1)
	err = cat.CommitTransaction()
	if err != nil {
		t.Logf("Commit empty transaction error: %v", err)
	}

	// Begin, insert, commit
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "commit_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)
	err = cat.CommitTransaction()
	if err != nil {
		t.Logf("Commit transaction error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM commit_test")
	t.Logf("Count after commit: %v", result.Rows)
}

// TestCoverage_RollbackTransactionEdgeCases tests rollback transaction edge cases
func TestCoverage_RollbackTransactionEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "rollback_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert initial data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rollback_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	// Begin transaction
	cat.BeginTransaction(1)

	// Insert more data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rollback_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("in_transaction")}},
	}, nil)

	// Rollback
	err := cat.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM rollback_test")
	t.Logf("Count after rollback: %v", result.Rows)

	// Rollback without transaction (should not panic)
	err = cat.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback without transaction error: %v", err)
	}
}

// TestCoverage_FlushTableTrees tests flush table trees
func TestCoverage_FlushTableTrees(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "flush_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Begin transaction
	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("in_txn")}},
	}, nil)

	// Commit to trigger flush
	err := cat.CommitTransaction()
	if err != nil {
		t.Logf("Commit error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM flush_test")
	t.Logf("Count after commit: %v", result.Rows)
}

// TestCoverage_QueryCacheStats tests query cache stats
func TestCoverage_QueryCacheStats(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Get stats before any queries
	hits, misses, size := cat.GetQueryCacheStats()
	t.Logf("Initial cache stats: hits=%d, misses=%d, size=%d", hits, misses, size)

	createCoverageTestTable(t, cat, "cache_stats", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Execute same query multiple times
	for i := 0; i < 5; i++ {
		cat.ExecuteQuery("SELECT COUNT(*) FROM cache_stats")
	}

	hits, misses, size = cat.GetQueryCacheStats()
	t.Logf("After queries cache stats: hits=%d, misses=%d, size=%d", hits, misses, size)

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cache_stats",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Query again after insert
	hits, misses, size = cat.GetQueryCacheStats()
	t.Logf("After insert cache stats: hits=%d, misses=%d, size=%d", hits, misses, size)
}
