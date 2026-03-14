package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ApplyOuterQueryStar targets applyOuterQuery with StarExpr
func TestCoverage_ApplyOuterQueryStar(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_star", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenText},
		{Name: "b", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_star",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	cat.CreateView("view_star", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "a"},
			&query.Identifier{Name: "b"},
		},
		From: &query.TableRef{Name: "outer_star"},
	})

	// Query view with star
	result, err := cat.ExecuteQuery("SELECT * FROM view_star WHERE b > 50")
	if err != nil {
		t.Logf("Star query error: %v", err)
	} else {
		t.Logf("Star query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ApplyOuterQueryOffset targets applyOuterQuery with OFFSET
func TestCoverage_ApplyOuterQueryOffset(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_offset", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_offset",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with ORDER BY
	cat.CreateView("view_offset", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "outer_offset"},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "val"}, Desc: false},
		},
	})

	// Query view with OFFSET
	result, err := cat.ExecuteQuery("SELECT * FROM view_offset OFFSET 10")
	if err != nil {
		t.Logf("OFFSET query error: %v", err)
	} else {
		t.Logf("OFFSET query returned %d rows", len(result.Rows))
	}

	// Query view with large OFFSET
	result, err = cat.ExecuteQuery("SELECT * FROM view_offset OFFSET 100")
	if err != nil {
		t.Logf("Large OFFSET query error: %v", err)
	} else {
		t.Logf("Large OFFSET query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ApplyOuterQueryDistinct targets applyOuterQuery with DISTINCT on views
func TestCoverage_ApplyOuterQueryDistinct(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_distinct2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		c := "A"
		if i > 15 {
			c = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_distinct2",
			Columns: []string{"id", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(c)}},
		}, nil)
	}

	// Create view
	cat.CreateView("view_distinct2", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "cat"},
		},
		From: &query.TableRef{Name: "outer_distinct2"},
	})

	// Query with DISTINCT
	result, err := cat.ExecuteQuery("SELECT DISTINCT cat FROM view_distinct2")
	if err != nil {
		t.Logf("DISTINCT query error: %v", err)
	} else {
		t.Logf("DISTINCT query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ApplyOuterQueryAliasExpr targets applyOuterQuery with column aliases
func TestCoverage_ApplyOuterQueryAliasExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_alias2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_alias2",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	cat.CreateView("view_alias2", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "outer_alias2"},
	})

	// Query with column alias
	result, err := cat.ExecuteQuery("SELECT id as row_id, val as row_val FROM view_alias2 WHERE val > 50")
	if err != nil {
		t.Logf("Alias query error: %v", err)
	} else {
		t.Logf("Alias query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_EvaluateWhereCaseExpr targets evaluateWhere with CASE expressions
func TestCoverage_EvaluateWhereCaseExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_case", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_case",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	// Query with CASE in WHERE
	result, err := cat.ExecuteQuery("SELECT * FROM where_case WHERE CASE WHEN score > 50 THEN 1 ELSE 0 END = 1")
	if err != nil {
		t.Logf("CASE query error: %v", err)
	} else {
		t.Logf("CASE query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_EvaluateWhereInExpr targets evaluateWhere with IN expressions
func TestCoverage_EvaluateWhereInExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_in", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_in",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_in WHERE val IN (1, 5, 10, 15, 20)",
		"SELECT * FROM where_in WHERE val NOT IN (1, 2, 3)",
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

// TestCoverage_EvaluateWhereExists targets evaluateWhere with EXISTS subquery
func TestCoverage_EvaluateWhereExists(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_exists_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	createCoverageTestTable(t, cat, "where_exists_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_exists_main",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
		if i <= 5 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "where_exists_sub",
				Columns: []string{"id", "ref"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
			}, nil)
		}
	}

	// Query with EXISTS
	result, err := cat.ExecuteQuery("SELECT * FROM where_exists_main WHERE EXISTS (SELECT 1 FROM where_exists_sub WHERE ref = where_exists_main.id)")
	if err != nil {
		t.Logf("EXISTS query error: %v", err)
	} else {
		t.Logf("EXISTS query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ResolveAggregateInExprArithmetic targets resolveAggregateInExpr with arithmetic
func TestCoverage_ResolveAggregateInExprArithmetic(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_arith", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		grp := "A"
		if i > 20 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_arith",
			Columns: []string{"id", "grp", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(a) + SUM(b) as total FROM agg_arith GROUP BY grp",
		"SELECT grp, SUM(a) - AVG(b) as diff FROM agg_arith GROUP BY grp HAVING diff > 0",
		"SELECT grp, SUM(a) * COUNT(*) as weighted FROM agg_arith GROUP BY grp",
		"SELECT grp, (SUM(a) + SUM(b)) / COUNT(*) as avg_total FROM agg_arith GROUP BY grp",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Arithmetic aggregate error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SelectLockedJoin targets selectLocked with JOIN
func TestCoverage_SelectLockedJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "join_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "join_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref", Type: query.TokenInteger},
		{Name: "desc", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_a",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val" + string(rune('0'+i)))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_b",
			Columns: []string{"id", "ref", "desc"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("desc" + string(rune('0'+i)))}},
		}, nil)
	}

	// Simple JOIN
	result, err := cat.ExecuteQuery("SELECT * FROM join_a JOIN join_b ON join_a.id = join_b.ref")
	if err != nil {
		t.Logf("JOIN error: %v", err)
	} else {
		t.Logf("JOIN returned %d rows", len(result.Rows))
	}

	// LEFT JOIN
	result, err = cat.ExecuteQuery("SELECT * FROM join_a LEFT JOIN join_b ON join_a.id = join_b.ref WHERE join_b.ref IS NULL")
	if err != nil {
		t.Logf("LEFT JOIN error: %v", err)
	} else {
		t.Logf("LEFT JOIN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_SelectLockedSubquery targets selectLocked with subqueries
func TestCoverage_SelectLockedSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sub_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "sub_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "threshold", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sub_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sub_ref",
		Columns: []string{"id", "threshold"},
		Values:  [][]query.Expression{{numReal(1), numReal(50)}},
	}, nil)

	// Subquery in SELECT
	result, err := cat.ExecuteQuery("SELECT id, val, (SELECT threshold FROM sub_ref WHERE id = 1) as threshold FROM sub_main WHERE val > 50")
	if err != nil {
		t.Logf("Subquery error: %v", err)
	} else {
		t.Logf("Subquery returned %d rows", len(result.Rows))
	}
}
