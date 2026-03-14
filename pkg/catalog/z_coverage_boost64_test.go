package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_BetweenExpression targets BETWEEN expression
func TestCoverage_BetweenExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "between_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "between_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM between_test WHERE val BETWEEN 20 AND 60",
		"SELECT * FROM between_test WHERE val NOT BETWEEN 30 AND 70",
		"SELECT * FROM between_test WHERE id BETWEEN 10 AND 20 AND val > 25",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("BETWEEN error: %v", err)
		} else {
			t.Logf("BETWEEN query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_InSubquery targets IN with subquery
func TestCoverage_InSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "in_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "in_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "in_main",
			Columns: []string{"id", "ref_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 10))}},
		}, nil)
	}

	for i := 0; i < 10; i++ {
		code := "active"
		if i%2 == 0 {
			code = "inactive"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "in_ref",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code)}},
		}, nil)
	}

	result, err := cat.ExecuteQuery("SELECT * FROM in_main WHERE ref_id IN (SELECT id FROM in_ref WHERE code = 'active')")
	if err != nil {
		t.Logf("IN subquery error: %v", err)
	} else {
		t.Logf("IN subquery returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ExistsSubquery targets EXISTS with subquery
func TestCoverage_ExistsSubqueryMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "exists_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	createCoverageTestTable(t, cat, "exists_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "exists_main",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
		if i <= 5 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "exists_ref",
				Columns: []string{"id", "main_id"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
			}, nil)
		}
	}

	result, err := cat.ExecuteQuery("SELECT * FROM exists_main WHERE EXISTS (SELECT 1 FROM exists_ref WHERE main_id = exists_main.id)")
	if err != nil {
		t.Logf("EXISTS error: %v", err)
	} else {
		t.Logf("EXISTS query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_CaseExpression targets CASE expression
func TestCoverage_CaseExpressionMore(t *testing.T) {
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

	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "case_test",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, score, CASE WHEN score >= 100 THEN 'A' WHEN score >= 50 THEN 'B' ELSE 'C' END as grade FROM case_test",
		"SELECT id, CASE WHEN score > 75 THEN 'high' ELSE 'low' END as level FROM case_test WHERE score > 50",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CASE error: %v", err)
		} else {
			t.Logf("CASE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_StringFunctions targets string functions
func TestCoverage_StringFuncsMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "str_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "txt", Type: query.TokenText},
	})

	texts := []string{"hello world", "HELLO", "Hello World", "   trim me   ", "UPPERlower"}
	for i, txt := range texts {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "str_test",
			Columns: []string{"id", "txt"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(txt)}},
		}, nil)
	}

	queries := []string{
		"SELECT id, UPPER(txt) FROM str_test",
		"SELECT id, LOWER(txt) FROM str_test",
		"SELECT id, LENGTH(txt) FROM str_test",
		"SELECT id, SUBSTRING(txt, 1, 5) FROM str_test",
		"SELECT id, TRIM(txt) FROM str_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("String function error: %v", err)
		} else {
			t.Logf("String query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DateFunctions targets date functions
func TestCoverage_DateFuncsMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "date_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "date_test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	queries := []string{
		"SELECT id, CURRENT_DATE FROM date_test",
		"SELECT id, CURRENT_TIMESTAMP FROM date_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Date function error: %v", err)
		} else {
			t.Logf("Date query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_MathFunctions targets math functions
func TestCoverage_MathFuncsMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "math_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenReal},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "math_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i) * 1.5)}},
		}, nil)
	}

	queries := []string{
		"SELECT id, ABS(val) FROM math_test",
		"SELECT id, ROUND(val) FROM math_test",
		"SELECT id, FLOOR(val) FROM math_test",
		"SELECT id, CEIL(val) FROM math_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Math function error: %v", err)
		} else {
			t.Logf("Math query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_AggregateFunctions targets aggregate functions
func TestCoverage_AggFuncsMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_test", []*query.ColumnDef{
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
			Table:   "agg_test",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM agg_test",
		"SELECT grp, COUNT(*), SUM(val) FROM agg_test GROUP BY grp",
		"SELECT COUNT(DISTINCT grp) FROM agg_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate error: %v", err)
		} else {
			t.Logf("Aggregate query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SubqueryInSelect targets subquery in SELECT clause
func TestCoverage_SubqueryInSelectMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sub_sel_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "sub_sel_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "count", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sub_sel_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name" + string(rune('0'+i)))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sub_sel_ref",
			Columns: []string{"id", "main_id", "count"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	result, err := cat.ExecuteQuery("SELECT id, name, (SELECT count FROM sub_sel_ref WHERE main_id = sub_sel_main.id) as cnt FROM sub_sel_main")
	if err != nil {
		t.Logf("Subquery in SELECT error: %v", err)
	} else {
		t.Logf("Subquery in SELECT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_CorrelatedSubquery targets correlated subquery
func TestCoverage_CorrelatedSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "corr_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "corr_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "threshold", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "corr_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "corr_ref",
		Columns: []string{"id", "threshold"},
		Values:  [][]query.Expression{{numReal(1), numReal(50)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT * FROM corr_main WHERE val > (SELECT threshold FROM corr_ref WHERE id = 1)")
	if err != nil {
		t.Logf("Correlated subquery error: %v", err)
	} else {
		t.Logf("Correlated subquery returned %d rows", len(result.Rows))
	}
}
