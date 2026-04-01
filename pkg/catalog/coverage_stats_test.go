package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestStatsCollectorCountRows tests the countRows function
func TestStatsCollectorCountRows(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	sc := NewStatsCollector(c)

	// Test with invalid table name (contains special chars)
	_, err := sc.countRows("table;drop")
	if err == nil {
		t.Error("Expected error for invalid table name")
	}

	// Test with non-existent table (should return error from ExecuteQuery)
	_, err = sc.countRows("nonexistent_table_xyz")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	// Create a table and test normal operation
	c.CreateTable(&query.CreateTableStmt{
		Table: "test_count",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	count, err := sc.countRows("test_count")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows, got %d", count)
	}

	// Insert rows and count again
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test_count",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)

	count, err = sc.countRows("test_count")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

// TestStatsCollectorCountRowsWithFloatResult tests countRows when result is float64
func TestStatsCollectorCountRowsWithFloatResult(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	sc := NewStatsCollector(c)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test_float_count",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert large number of rows
	values := make([][]query.Expression, 100)
	for i := 0; i < 100; i++ {
		values[i] = []query.Expression{numReal(float64(i + 1))}
	}
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test_float_count",
		Columns: []string{"id"},
		Values:  values,
	}, nil)

	count, err := sc.countRows("test_float_count")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if count != 100 {
		t.Errorf("Expected 100 rows, got %d", count)
	}
}
