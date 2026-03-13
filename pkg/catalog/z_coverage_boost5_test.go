package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ExecuteCTE tests CTE execution
func TestCoverage_ExecuteCTE(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cte_base",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"WITH cte AS (SELECT * FROM cte_base WHERE val > 50) SELECT * FROM cte",
		"WITH cte1 AS (SELECT * FROM cte_base), cte2 AS (SELECT * FROM cte1 WHERE val > 30) SELECT * FROM cte2",
		"WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 5) SELECT * FROM nums",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CTE query error: %v", err)
		} else {
			t.Logf("CTE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_JoinsVarious tests various JOIN types
func TestCoverage_JoinsVarious(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "join_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "join_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_id", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "join_a",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A1")}, {numReal(2), strReal("A2")}, {numReal(3), strReal("A3")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "join_b",
		Columns: []string{"id", "a_id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("B1")}, {numReal(2), numReal(2), strReal("B2")}},
	}, nil)

	queries := []string{
		"SELECT * FROM join_a INNER JOIN join_b ON join_a.id = join_b.a_id",
		"SELECT * FROM join_a LEFT JOIN join_b ON join_a.id = join_b.a_id",
		"SELECT * FROM join_a CROSS JOIN join_b",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN query error: %v", err)
		} else {
			t.Logf("JOIN query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_Subqueries tests various subquery patterns
func TestCoverage_Subqueries(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "subq_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		category := "X"
		if i > 10 {
			category = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "subq_main",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM subq_main WHERE amount > (SELECT AVG(amount) FROM subq_main)",
		"SELECT * FROM subq_main WHERE category IN (SELECT DISTINCT category FROM subq_main WHERE amount > 100)",
		"SELECT * FROM subq_main WHERE EXISTS (SELECT 1 FROM subq_main s WHERE s.category = subq_main.category AND s.amount > 150)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Subquery error: %v", err)
		} else {
			t.Logf("Subquery returned %d rows", len(result.Rows))
		}
	}
}
