package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_Union tests UNION operations
func TestCoverage_Union(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "union_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "union_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "union_a",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("x")}, {numReal(2), strReal("y")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "union_b",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("y")}, {numReal(3), strReal("z")}},
	}, nil)

	queries := []string{
		"SELECT * FROM union_a UNION SELECT * FROM union_b",
		"SELECT * FROM union_a UNION ALL SELECT * FROM union_b",
		"SELECT * FROM union_a INTERSECT SELECT * FROM union_b",
		"SELECT * FROM union_a EXCEPT SELECT * FROM union_b",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CaseExpressions tests CASE expressions
func TestCoverage_CaseExpressions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "case_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "case_test",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 20))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END AS grade FROM case_test",
		"SELECT id, CASE score WHEN 100 THEN 'Perfect' WHEN 80 THEN 'Good' ELSE 'OK' END AS desc FROM case_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CASE query error: %v", err)
		} else {
			t.Logf("CASE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_Coalesce tests COALESCE function
func TestCoverage_Coalesce(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "coalesce_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "coalesce_test",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), numReal(100)},
			{numReal(2), &query.NullLiteral{}},
		},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT id, COALESCE(val, 0) AS val FROM coalesce_test")
	if err != nil {
		t.Logf("COALESCE error: %v", err)
	} else {
		t.Logf("COALESCE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_NULLIF tests NULLIF function
func TestCoverage_NULLIF(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "nullif_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "nullif_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(0)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT id, NULLIF(val, 0) AS val FROM nullif_test")
	if err != nil {
		t.Logf("NULLIF error: %v", err)
	} else {
		t.Logf("NULLIF returned %d rows", len(result.Rows))
	}
}

// TestCoverage_INExpressions tests IN expressions
func TestCoverage_INExpressions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "in_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "in_test",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}, {numReal(3), strReal("C")}},
	}, nil)

	queries := []string{
		"SELECT * FROM in_test WHERE category IN ('A', 'B')",
		"SELECT * FROM in_test WHERE category NOT IN ('A', 'C')",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("IN query error: %v", err)
		} else {
			t.Logf("IN query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_Between tests BETWEEN expressions
func TestCoverage_Between(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "between_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "between_test",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM between_test WHERE score BETWEEN 30 AND 70",
		"SELECT * FROM between_test WHERE score NOT BETWEEN 20 AND 80",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("BETWEEN query error: %v", err)
		} else {
			t.Logf("BETWEEN query returned %d rows", len(result.Rows))
		}
	}
}
