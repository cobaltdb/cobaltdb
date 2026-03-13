package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_SavepointEdgeCases tests transaction savepoint edge cases
func TestCoverage_SavepointEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Begin transaction
	cat.BeginTransaction(1)

	// Create savepoint
	err := cat.Savepoint("sp1")
	if err != nil {
		t.Logf("Savepoint error: %v", err)
	}

	// Rollback to non-existent savepoint
	err = cat.RollbackToSavepoint("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent savepoint")
	}

	// Release non-existent savepoint
	err = cat.ReleaseSavepoint("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent savepoint release")
	}

	// Commit transaction
	cat.CommitTransaction()
}

// TestCoverage_NestedSavepoints tests nested transaction savepoints
func TestCoverage_NestedSavepoints(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "nested_sp",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	cat.BeginTransaction(1)

	// Create nested savepoints
	cat.Savepoint("sp1")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	cat.Savepoint("sp2")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}},
	}, nil)

	// Rollback to outer savepoint
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to outer savepoint: %v", err)
	}

	cat.CommitTransaction()
}

// TestCoverage_CreateIndexDuplicate tests create index with duplicate name
func TestCoverage_CreateIndexDuplicate(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "idx_dup",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Create first index
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index: "idx_val",
		Table: "idx_dup",
		Columns: []string{"val"},
	})
	if err != nil {
		t.Fatalf("First index creation failed: %v", err)
	}

	// Create duplicate index name
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index: "idx_val",
		Table: "idx_dup",
		Columns: []string{"val"},
	})
	if err == nil {
		t.Error("Expected error for duplicate index name")
	}
}

// TestCoverage_DropNonExistentIndex tests drop non-existent index
func TestCoverage_DropNonExistentIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "drop_idx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Drop non-existent index
	err := cat.DropIndex("non_existent_idx")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

// TestCoverage_SubqueryInFrom tests query with subquery in FROM
func TestCoverage_SubqueryInFrom(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "outer_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "outer_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(200)}},
	}, nil)

	// Query with subquery in FROM
	result, err := cat.ExecuteQuery("SELECT * FROM (SELECT id, val FROM outer_tbl) AS sub WHERE val > 100")
	if err != nil {
		t.Logf("Subquery in FROM error: %v", err)
	} else {
		t.Logf("Subquery result rows: %d", len(result.Rows))
	}
}

// TestCoverage_JoinUsingClause tests JOIN with USING clause
func TestCoverage_JoinUsingClause(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	cat.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "user_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}},
	}, nil)

	// JOIN with USING clause
	result, err := cat.ExecuteQuery("SELECT * FROM users JOIN orders USING (user_id)")
	if err != nil {
		t.Logf("JOIN USING error: %v", err)
	} else {
		t.Logf("JOIN result rows: %d", len(result.Rows))
	}
}
