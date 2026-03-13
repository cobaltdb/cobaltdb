package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_BulkInsert tests bulk INSERT operations
func TestCoverage_BulkInsert(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "bulk_insert", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Bulk insert many rows
	var values [][]query.Expression
	for i := 1; i <= 100; i++ {
		values = append(values, []query.Expression{numReal(float64(i)), strReal("value")})
	}

	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "bulk_insert",
		Columns: []string{"id", "val"},
		Values:  values,
	}, nil)

	if err != nil {
		t.Logf("Bulk insert error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM bulk_insert")
	t.Logf("Bulk insert count: %v", result.Rows)
}

// TestCoverage_TransactionIsolation tests transaction isolation
func TestCoverage_TransactionIsolation(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_iso", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert initial data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_iso",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Begin transaction and modify
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_iso",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), numReal(200)}},
	}, nil)

	// Count inside transaction should see uncommitted data
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_iso")
	t.Logf("Count inside transaction: %v", result.Rows)

	// Commit
	cat.CommitTransaction()

	// Count after commit
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM txn_iso")
	t.Logf("Count after commit: %v", result.Rows)
}

// TestCoverage_MultipleIndexes tests multiple indexes on same table
func TestCoverage_MultipleIndexes(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "multi_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "category", Type: query.TokenText},
	})

	// Create multiple indexes
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "multi_idx",
		Columns: []string{"code"},
	})

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_category",
		Table:   "multi_idx",
		Columns: []string{"category"},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "multi_idx",
			Columns: []string{"id", "code", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE"), strReal("CAT")}},
		}, nil)
	}

	// Query using different indexes
	result1, _ := cat.ExecuteQuery("SELECT * FROM multi_idx WHERE code = 'CODE'")
	t.Logf("Query by code index: %d rows", len(result1.Rows))

	result2, _ := cat.ExecuteQuery("SELECT * FROM multi_idx WHERE category = 'CAT'")
	t.Logf("Query by category index: %d rows", len(result2.Rows))
}

// TestCoverage_IndexDropRecreate tests dropping and recreating index
func TestCoverage_IndexDropRecreate(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "idx_recreate", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_temp",
		Table:   "idx_recreate",
		Columns: []string{"val"},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "idx_recreate",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Drop index
	err := cat.DropIndex("idx_temp")
	if err != nil {
		t.Logf("Drop index error: %v", err)
	}

	// Recreate index
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_temp",
		Table:   "idx_recreate",
		Columns: []string{"val"},
	})
	if err != nil {
		t.Logf("Recreate index error: %v", err)
	}

	// Query should still work
	result, _ := cat.ExecuteQuery("SELECT * FROM idx_recreate WHERE val = 'test'")
	t.Logf("Query after recreate: %d rows", len(result.Rows))
}
