package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Deep coverage for resolveOuterRefsInQuery and related functions
// ============================================================

// TestResolveOuterRefsInQuery_NilCases - nil parameter handling
func TestResolveOuterRefsInQuery_NilCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	_ = New(tree, pool, nil)

	// Create sample data for outer references
	outerRow := []interface{}{int64(1), "test"}
	outerColumns := []ColumnDef{
		{Name: "id"},
		{Name: "name"},
	}

	// Test with nil subquery - should return nil
	result := resolveOuterRefsInQuery(nil, outerRow, outerColumns)
	if result != nil {
		t.Error("Expected nil for nil subquery")
	}

	// Test with nil outerRow - should return original
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
		From:    &query.TableRef{Name: "test"},
	}
	result = resolveOuterRefsInQuery(stmt, nil, outerColumns)
	if result != stmt {
		t.Error("Expected original stmt for nil outerRow")
	}

	// Test with empty outerColumns - should return original
	result = resolveOuterRefsInQuery(stmt, outerRow, nil)
	if result != stmt {
		t.Error("Expected original stmt for empty outerColumns")
	}

	// Test with empty outerColumns slice
	result = resolveOuterRefsInQuery(stmt, outerRow, []ColumnDef{})
	if result != stmt {
		t.Error("Expected original stmt for empty outerColumns slice")
	}
}

// TestResolveOuterRefsInQuery_WithJoins - join condition resolution
func TestResolveOuterRefsInQuery_WithJoins(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_main_join", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "outer_sub_join", []*query.ColumnDef{
		{Name: "sub_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "outer_other", []*query.ColumnDef{
		{Name: "other_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_main_join",
			Columns: []string{"id", "ref_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_sub_join",
			Columns: []string{"sub_id", "ref_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
		code := "A"
		if i > 5 {
			code = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_other",
			Columns: []string{"other_id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code)}},
		}, nil)
	}

	// Correlated subquery with JOIN
	queries := []string{
		`SELECT * FROM outer_main_join m WHERE val > (SELECT SUM(s.amount) FROM outer_sub_join s JOIN outer_other o ON s.ref_id = o.other_id WHERE o.code = 'A' AND s.ref_id = m.ref_id)`,
		`SELECT * FROM outer_main_join m WHERE EXISTS (SELECT 1 FROM outer_sub_join s LEFT JOIN outer_other o ON s.ref_id = o.other_id WHERE s.ref_id = m.ref_id)`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestResolveOuterRefsInQuery_HavingGroupBy - HAVING and GROUP BY resolution
func TestResolveOuterRefsInQuery_HavingGroupBy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_having_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "outer_having_sub", []*query.ColumnDef{
		{Name: "sub_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_having_main",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_having_sub",
			Columns: []string{"sub_id", "grp", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 5))}},
		}, nil)
	}

	// Correlated subquery with GROUP BY and HAVING
	queries := []string{
		`SELECT * FROM outer_having_main m WHERE val > (SELECT SUM(amount) FROM outer_having_sub s WHERE s.grp = m.grp)`,
		`SELECT * FROM outer_having_main m WHERE grp IN (SELECT grp FROM outer_having_sub GROUP BY grp HAVING SUM(amount) > 100)`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestResolveOuterRefsInQuery_OrderBy - ORDER BY resolution
func TestResolveOuterRefsInQuery_OrderBy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_order_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "sort_key", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "outer_order_sub", []*query.ColumnDef{
		{Name: "sub_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "sort_val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_order_main",
			Columns: []string{"id", "sort_key"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(100 - i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_order_sub",
			Columns: []string{"sub_id", "main_id", "sort_val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Correlated subquery with ORDER BY
	queries := []string{
		`SELECT * FROM outer_order_main m WHERE id IN (SELECT main_id FROM outer_order_sub WHERE main_id = m.id ORDER BY sort_val)`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestResolveOuterRefsInQuery_TableAlias - table alias handling
func TestResolveOuterRefsInQuery_TableAlias(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_alias_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "outer_alias_sub", []*query.ColumnDef{
		{Name: "sub_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_alias_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_alias_sub",
			Columns: []string{"sub_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	// Subquery with table alias
	queries := []string{
		`SELECT * FROM outer_alias_main m WHERE val > (SELECT SUM(s.val) FROM outer_alias_sub AS s WHERE s.sub_id = m.id)`,
		`SELECT * FROM outer_alias_main AS main WHERE val > (SELECT val FROM outer_alias_sub AS sub WHERE sub.sub_id = main.id)`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestComputeAggregatesWithGroupBy_MoreCoverage - additional GROUP BY coverage
func TestComputeAggregatesWithGroupBy_MoreCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		if i > 20 {
			grp = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_more",
			Columns: []string{"id", "grp", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i)), strReal("name" + string(rune('A'+i%5)))}},
		}, nil)
	}

	// Queries targeting specific GROUP BY paths
	queries := []string{
		`SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM gb_more GROUP BY grp HAVING cnt > 0`,
		`SELECT grp, MIN(name), MAX(name) FROM gb_more GROUP BY grp`,
		`SELECT grp, COUNT(DISTINCT name) FROM gb_more GROUP BY grp`,
		`SELECT grp, SUM(val), AVG(val) FROM gb_more GROUP BY grp ORDER BY AVG(val)`,
		`SELECT grp FROM gb_more GROUP BY grp ORDER BY SUM(val) DESC`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestApplyGroupByOrderBy_MoreCoverage - additional ORDER BY coverage
func TestApplyGroupByOrderBy_MoreCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_ob_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		category := "X"
		if i%2 == 0 {
			category = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_ob_more",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		`SELECT category, COUNT(*) FROM gb_ob_more GROUP BY category ORDER BY category ASC`,
		`SELECT category, SUM(amount) FROM gb_ob_more GROUP BY category ORDER BY SUM(amount)`,
		`SELECT category, AVG(amount) as avg_amt FROM gb_ob_more GROUP BY category ORDER BY avg_amt DESC`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestApplyOuterQuery_CompleteCoverage - comprehensive applyOuterQuery tests
func TestApplyOuterQuery_CompleteCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_complete", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_complete",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "grp"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "aoq_complete"},
	}
	cat.CreateView("aoq_complete_view", viewStmt)

	// Cover all applyOuterQuery branches
	queries := []string{
		// Non-aggregate paths
		`SELECT * FROM aoq_complete_view WHERE val > 50`,
		`SELECT id, val + 1 as val_plus FROM aoq_complete_view`,
		`SELECT * FROM aoq_complete_view ORDER BY val DESC`,
		`SELECT * FROM aoq_complete_view LIMIT 5 OFFSET 5`,
		`SELECT DISTINCT grp FROM aoq_complete_view`,
		`SELECT * FROM aoq_complete_view OFFSET 100`,

		// Aggregate paths
		`SELECT grp, COUNT(*) FROM aoq_complete_view GROUP BY grp`,
		`SELECT SUM(val) FROM aoq_complete_view`,
		`SELECT grp, SUM(val) as total FROM aoq_complete_view GROUP BY grp HAVING total > 500`,
		`SELECT grp, COUNT(*) FROM aoq_complete_view GROUP BY grp ORDER BY COUNT(*) DESC`,
		`SELECT grp, SUM(val) FROM aoq_complete_view GROUP BY grp LIMIT 1`,
		`SELECT grp, SUM(val) FROM aoq_complete_view GROUP BY grp OFFSET 1`,

		// Mixed
		`SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM aoq_complete_view`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestEvaluateExprWithGroupAggregates_More - additional aggregate expression tests
func TestEvaluateExprWithGroupAggregates_More(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "expr_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "expr_agg",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		`SELECT grp, SUM(val) as total, AVG(val) as avg_val, total + avg_val FROM expr_agg GROUP BY grp`,
		`SELECT grp, COUNT(*) * 10 FROM expr_agg GROUP BY grp`,
		`SELECT grp, SUM(val) / COUNT(*) FROM expr_agg GROUP BY grp`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}
