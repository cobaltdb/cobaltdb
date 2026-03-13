package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_CTEUnionDistinct tests CTE UNION with DISTINCT
func TestCoverage_CTEUnionDistinct(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_u1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "cte_u2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_u1",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_u2",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("B")}, {numReal(3), strReal("C")}},
	}, nil)

	queries := []string{
		"WITH u AS (SELECT val FROM cte_u1 UNION SELECT val FROM cte_u2) SELECT * FROM u ORDER BY val",
		"WITH u AS (SELECT val FROM cte_u1 UNION ALL SELECT val FROM cte_u2) SELECT * FROM u ORDER BY val",
		"WITH u AS (SELECT * FROM cte_u1 UNION DISTINCT SELECT * FROM cte_u2) SELECT COUNT(*) FROM u",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CTE UNION error: %v", err)
		} else {
			t.Logf("CTE UNION returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SaveAndLoad tests database save and load
func TestCoverage_SaveAndLoad(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "save_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "save_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("data")}},
	}, nil)

	// Save
	err := cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Load (reusing same catalog for simplicity)
	err = cat.Load()
	if err != nil {
		t.Logf("Load error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM save_test")
	t.Logf("Count after load: %v", result.Rows)
}

// TestCoverage_InsertLockedEdgeCases tests insert locked with edge cases
func TestCoverage_InsertLockedEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Table with unique constraint
	createCoverageTestTable(t, cat, "insert_unique", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText, Unique: true},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "insert_unique",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("ABC")}},
	}, nil)

	// Try duplicate unique
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "insert_unique",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(2), strReal("ABC")}},
	}, nil)
	if err != nil {
		t.Logf("Duplicate unique error (expected): %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM insert_unique")
	t.Logf("Count: %v", result.Rows)
}

// TestCoverage_DeleteRowLocked tests delete row locked
func TestCoverage_DeleteRowLocked(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_row", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_row",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Delete specific rows
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(3),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_row")
	t.Logf("Count after delete: %v", result.Rows)
}

// TestCoverage_ApplyOuterQuery tests outer query application
func TestCoverage_ApplyOuterQuery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_base",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Create a view with DISTINCT
	err := cat.CreateView("dist_view", &query.SelectStmt{
		Columns:  []query.Expression{&query.Identifier{Name: "val"}},
		From:     &query.TableRef{Name: "outer_base"},
		Distinct: true,
	})
	if err != nil {
		t.Logf("Create view error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT * FROM dist_view")
	t.Logf("Distinct view returned %d rows", len(result.Rows))
}

// TestCoverage_RecursiveCTEEdgeCases tests recursive CTE edge cases
func TestCoverage_RecursiveCTEEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Fibonacci-like recursive CTE
	result, err := cat.ExecuteQuery(`
		WITH RECURSIVE fib(n, val) AS (
			SELECT 1, 1
			UNION ALL
			SELECT n + 1, val + n FROM fib WHERE n < 5
		)
		SELECT * FROM fib
	`)
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE returned %d rows", len(result.Rows))
	}
}
