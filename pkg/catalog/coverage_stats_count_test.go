package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestStatsCountRowsWithManyRows tests countRows with large dataset
func TestStatsCountRowsWithManyRows(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	sc := NewStatsCollector(c)

	c.CreateTable(&query.CreateTableStmt{
		Table: "count_many",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert 1000 rows
	for i := 1; i <= 1000; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "count_many",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	count, err := sc.countRows("count_many")
	if err != nil {
		t.Errorf("countRows failed: %v", err)
	}
	if count != 1000 {
		t.Errorf("Expected 1000 rows, got %d", count)
	}
}

// TestStatsCountRowsAfterDelete tests countRows after deletions
func TestStatsCountRowsAfterDelete(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	sc := NewStatsCollector(c)

	c.CreateTable(&query.CreateTableStmt{
		Table: "count_deltest",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert 10 rows
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "count_deltest",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	// Delete some rows
	c.Delete(ctx, &query.DeleteStmt{
		Table: "count_deltest",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLt,
			Right:    &query.NumberLiteral{Value: 5},
		},
	}, nil)

	count, err := sc.countRows("count_deltest")
	if err != nil {
		t.Errorf("countRows after delete failed: %v", err)
	}
	if count != 6 { // 5,6,7,8,9,10 = 6 rows
		t.Errorf("Expected 6 rows after delete, got %d", count)
	}
}

// TestStatsCountRowsEmptyTable tests countRows on empty table
func TestStatsCountRowsEmptyTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	sc := NewStatsCollector(c)

	c.CreateTable(&query.CreateTableStmt{
		Table: "count_empty",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	count, err := sc.countRows("count_empty")
	if err != nil {
		t.Errorf("countRows on empty table failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows on empty table, got %d", count)
	}
}

// TestStatsCountRowsWithFloatResult tests countRows returning float64
func TestStatsCountRowsWithFloatResult(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	sc := NewStatsCollector(c)

	c.CreateTable(&query.CreateTableStmt{
		Table: "count_float",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenReal, PrimaryKey: true},
		},
	})

	// Insert with float IDs
	c.Insert(ctx, &query.InsertStmt{
		Table:   "count_float",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1.5)}, {numReal(2.5)}},
	}, nil)

	count, err := sc.countRows("count_float")
	if err != nil {
		t.Errorf("countRows with float failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestStatsCountRowsInvalidTableName(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)
	sc := NewStatsCollector(c)

	_, err := sc.countRows("DROP")
	if err == nil {
		t.Error("expected error for invalid table name")
	}
}
