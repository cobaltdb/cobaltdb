package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_SelectWithIndex tests SELECT with index usage
func TestCoverage_SelectWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "idx_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "idx_test",
		Columns: []string{"code"},
	})

	// Insert data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "idx_test",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Query using index
	result, err := cat.ExecuteQuery("SELECT * FROM idx_test WHERE code = 'CODE'")
	if err != nil {
		t.Logf("Index query error: %v", err)
	} else {
		t.Logf("Index query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_MultiColumnIndex tests multi-column index
func TestCoverage_MultiColumnIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "multi_idx_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "col1", Type: query.TokenText},
		{Name: "col2", Type: query.TokenInteger},
	})

	// Create multi-column index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_multi",
		Table:   "multi_idx_test",
		Columns: []string{"col1", "col2"},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "multi_idx_test",
			Columns: []string{"id", "col1", "col2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("A"), numReal(float64(i))}},
		}, nil)
	}

	result, err := cat.ExecuteQuery("SELECT * FROM multi_idx_test WHERE col1 = 'A' AND col2 = 25")
	if err != nil {
		t.Logf("Multi-column index query error: %v", err)
	} else {
		t.Logf("Multi-column index query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_UniqueIndex tests unique index
func TestCoverage_UniqueIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "uniq_idx_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText},
	})

	// Create unique index
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_email",
		Table:   "uniq_idx_test",
		Columns: []string{"email"},
		Unique:  true,
	})
	if err != nil {
		t.Logf("Create unique index error: %v", err)
	}

	// Insert first record
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "uniq_idx_test",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{numReal(1), strReal("test@example.com")}},
	}, nil)
	if err != nil {
		t.Logf("First insert error: %v", err)
	}

	// Try duplicate (should fail)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "uniq_idx_test",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{numReal(2), strReal("test@example.com")}},
	}, nil)
	if err == nil {
		t.Error("Expected error for duplicate unique value")
	}
}

// TestCoverage_GetIndex tests GetIndex function
func TestCoverage_GetIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "get_idx_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "test_idx",
		Table:   "get_idx_test",
		Columns: []string{"val"},
	})

	// Get existing index
	idx, err := cat.GetIndex("test_idx")
	if err != nil {
		t.Logf("GetIndex error: %v", err)
	} else {
		t.Logf("Got index: %s on table %s", idx.Name, idx.TableName)
	}

	// Get non-existent index
	_, err = cat.GetIndex("non_existent_idx")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}
