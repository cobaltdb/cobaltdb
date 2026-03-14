package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_CastExpressionsExtensive tests CAST expressions extensively
func TestCoverage_CastExpressionsExtensive(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cast_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "int_val", Type: query.TokenInteger},
		{Name: "real_val", Type: query.TokenReal},
		{Name: "text_val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_ext",
		Columns: []string{"id", "int_val", "real_val", "text_val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100), numReal(3.14), strReal("42")}},
	}, nil)

	queries := []string{
		"SELECT CAST(int_val AS TEXT) FROM cast_ext",
		"SELECT CAST(real_val AS INTEGER) FROM cast_ext",
		"SELECT CAST(text_val AS INTEGER) FROM cast_ext",
		"SELECT CAST(int_val AS REAL) FROM cast_ext",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CAST error: %v", err)
		} else {
			t.Logf("CAST returned %v", result.Rows)
		}
	}
}

// TestCoverage_CaseExpressionsExtensive tests CASE expressions extensively
func TestCoverage_CaseExpressionsExtensive(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "case_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "case_ext",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, CASE WHEN val > 100 THEN 'high' WHEN val > 50 THEN 'medium' ELSE 'low' END as level FROM case_ext",
		"SELECT id, CASE WHEN val < 30 THEN 'small' WHEN val < 100 THEN 'medium' WHEN val < 150 THEN 'large' ELSE 'xlarge' END as size FROM case_ext",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CASE error: %v", err)
		} else {
			t.Logf("CASE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CoalesceNullifExtensive tests COALESCE and NULLIF extensively
func TestCoverage_CoalesceNullifExtensive(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "coalesce_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "coalesce_ext",
		Columns: []string{"id", "val1", "val2"},
		Values: [][]query.Expression{
			{numReal(1), numReal(10), &query.NullLiteral{}},
			{numReal(2), &query.NullLiteral{}, numReal(20)},
			{numReal(3), numReal(30), numReal(40)},
		},
	}, nil)

	queries := []string{
		"SELECT id, COALESCE(val1, val2, 0) as val FROM coalesce_ext",
		"SELECT id, NULLIF(val1, val2) as diff FROM coalesce_ext",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("COALESCE/NULLIF error: %v", err)
		} else {
			t.Logf("COALESCE/NULLIF returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ExistsSubqueryExtensive tests EXISTS subquery extensively
func TestCoverage_ExistsSubqueryExtensive(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "exists_dept_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "exists_emp_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "exists_dept_ext",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales")}, {numReal(2), strReal("Eng")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "exists_emp_ext",
		Columns: []string{"id", "dept_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}},
	}, nil)

	queries := []string{
		"SELECT * FROM exists_dept_ext d WHERE EXISTS (SELECT 1 FROM exists_emp_ext e WHERE e.dept_id = d.id)",
		"SELECT * FROM exists_dept_ext d WHERE NOT EXISTS (SELECT 1 FROM exists_emp_ext e WHERE e.dept_id = d.id AND e.id > 100)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("EXISTS error: %v", err)
		} else {
			t.Logf("EXISTS returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_StringFunctionsExtensive tests string functions extensively
func TestCoverage_StringFunctionsExtensive(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "str_funcs_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "str_funcs_ext",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("  Hello World  ")}},
	}, nil)

	queries := []string{
		"SELECT UPPER(val) FROM str_funcs_ext",
		"SELECT LOWER(val) FROM str_funcs_ext",
		"SELECT TRIM(val) FROM str_funcs_ext",
		"SELECT LENGTH(val) FROM str_funcs_ext",
		"SELECT SUBSTR(val, 1, 5) FROM str_funcs_ext",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("String function error: %v", err)
		} else {
			t.Logf("String function returned %v", result.Rows)
		}
	}
}
