package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_SelectLockedWithWhereComplex targets selectLocked with WHERE
func TestCoverage_SelectLockedWithWhereComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_where", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_where",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("val")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM sel_where WHERE a > 10",
		"SELECT * FROM sel_where WHERE a > 10 AND a < 30",
		"SELECT * FROM sel_where WHERE a = 50",
		"SELECT * FROM sel_where WHERE b = 'val' AND a > 50",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE error: %v", err)
		} else {
			t.Logf("WHERE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SelectLockedWithAggregates targets selectLocked with aggregates
func TestCoverage_SelectLockedWithAggregates(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_agg",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*) FROM sel_agg",
		"SELECT SUM(val) FROM sel_agg",
		"SELECT AVG(val) FROM sel_agg",
		"SELECT MIN(val), MAX(val) FROM sel_agg",
		"SELECT COUNT(*) FROM sel_agg WHERE val > 500",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate error: %v", err)
		} else {
			t.Logf("Aggregate result: %v", result.Rows)
		}
	}
}

// TestCoverage_ExecuteScalarSelectComplex targets executeScalarSelect
func TestCoverage_ExecuteScalarSelectComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		grp := "A"
		if i > 25 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_complex",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*) FROM scalar_complex",
		"SELECT COUNT(*) FROM scalar_complex WHERE grp = 'A'",
		"SELECT SUM(val) FROM scalar_complex",
		"SELECT AVG(val) FROM scalar_complex WHERE grp = 'B'",
		"SELECT MIN(val), MAX(val) FROM scalar_complex",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Scalar error: %v", err)
		} else {
			t.Logf("Scalar result: %v", result.Rows)
		}
	}
}

// TestCoverage_InsertLockedComplexData targets insertLocked with complex data
func TestCoverage_InsertLockedComplexData(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_complex_data", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "int_col", Type: query.TokenInteger},
		{Name: "text_col", Type: query.TokenText},
	})

	// Single insert
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_complex_data",
		Columns: []string{"id", "int_col", "text_col"},
		Values:  [][]query.Expression{{numReal(1), numReal(100), strReal("test")}},
	}, nil)

	// Multi-row insert
	for i := 2; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ins_complex_data",
			Columns: []string{"id", "int_col", "text_col"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("text")}},
		}, nil)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_complex_data")
	t.Logf("Count after inserts: %v", result.Rows)
}

// TestCoverage_EvaluateLikeComplex targets evaluateLike with patterns
func TestCoverage_EvaluateLikeComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "like_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "txt", Type: query.TokenText},
	})

	values := []string{"hello world", "hello", "world", "HELLO", "test", "testing", "best", "rest", "jest"}
	for i, v := range values {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "like_complex",
			Columns: []string{"id", "txt"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(v)}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM like_complex WHERE txt LIKE 'hello%'",
		"SELECT * FROM like_complex WHERE txt LIKE '%world'",
		"SELECT * FROM like_complex WHERE txt LIKE '%ell%'",
		"SELECT * FROM like_complex WHERE txt LIKE '%est'",
		"SELECT * FROM like_complex WHERE txt LIKE 'test%'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIKE error: %v", err)
		} else {
			t.Logf("LIKE returned %d rows", len(result.Rows))
		}
	}
}
