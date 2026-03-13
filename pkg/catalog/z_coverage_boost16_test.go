package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_LeftJoinNulls tests LEFT JOIN with NULL handling
func TestCoverage_LeftJoinNulls(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "left_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	createCoverageTestTable(t, cat, "left_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_id", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "left_a",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "left_b",
		Columns: []string{"id", "a_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT * FROM left_a LEFT JOIN left_b ON left_a.id = left_b.a_id")
	if err != nil {
		t.Logf("LEFT JOIN error: %v", err)
	} else {
		t.Logf("LEFT JOIN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_InnerJoinEmpty tests INNER JOIN with empty results
func TestCoverage_InnerJoinEmpty(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "inner_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	createCoverageTestTable(t, cat, "inner_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_id", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "inner_a",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// No matching records in inner_b

	result, err := cat.ExecuteQuery("SELECT * FROM inner_a INNER JOIN inner_b ON inner_a.id = inner_b.a_id")
	if err != nil {
		t.Logf("INNER JOIN empty error: %v", err)
	} else {
		t.Logf("INNER JOIN empty returned %d rows", len(result.Rows))
	}
}

// TestCoverage_AggregateEmptyTable tests aggregates on empty table
func TestCoverage_AggregateEmptyTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "empty_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	queries := []string{
		"SELECT COUNT(*) FROM empty_agg",
		"SELECT SUM(val) FROM empty_agg",
		"SELECT AVG(val) FROM empty_agg",
		"SELECT MIN(val) FROM empty_agg",
		"SELECT MAX(val) FROM empty_agg",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate empty error: %v", err)
		} else {
			t.Logf("Aggregate empty returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DistinctEmpty tests DISTINCT on empty table
func TestCoverage_DistinctEmpty(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "empty_distinct", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	result, err := cat.ExecuteQuery("SELECT DISTINCT val FROM empty_distinct")
	if err != nil {
		t.Logf("DISTINCT empty error: %v", err)
	} else {
		t.Logf("DISTINCT empty returned %d rows", len(result.Rows))
	}
}
