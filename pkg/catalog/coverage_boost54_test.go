package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Deep coverage for applyOuterQuery - targeting all branches
// ============================================================

// TestApplyOuterQuery_GroupByEvaluationError - GROUP BY with expression errors
func TestApplyOuterQuery_GroupByEvaluationError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_gb_err", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 15; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		if i > 10 {
			grp = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_gb_err",
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
		From: &query.TableRef{Name: "aoq_gb_err"},
	}
	cat.CreateView("aoq_gb_err_view", viewStmt)

	// Various GROUP BY scenarios
	queries := []string{
		`SELECT grp, COUNT(*) FROM aoq_gb_err_view GROUP BY grp`,
		`SELECT grp, SUM(val) FROM aoq_gb_err_view GROUP BY grp`,
		`SELECT grp, AVG(val), MIN(val), MAX(val) FROM aoq_gb_err_view GROUP BY grp`,
		`SELECT COUNT(*) FROM aoq_gb_err_view`,
		`SELECT SUM(val), AVG(val) FROM aoq_gb_err_view`,
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

// TestApplyOuterQuery_NonAggregateColumn - non-aggregate column handling
func TestApplyOuterQuery_NonAggregateColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_non_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_non_agg",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i))}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "grp"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "aoq_non_agg"},
	}
	cat.CreateView("aoq_non_agg_view", viewStmt)

	// Non-aggregate columns alongside aggregates
	queries := []string{
		`SELECT grp, COUNT(*), val FROM aoq_non_agg_view GROUP BY grp`,
		`SELECT grp, val, SUM(val) FROM aoq_non_agg_view GROUP BY grp`,
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

// TestApplyOuterQuery_ColumnMapping - column mapping variations
func TestApplyOuterQuery_ColumnMapping(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_map", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "col_a", Type: query.TokenText},
		{Name: "col_b", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_map",
			Columns: []string{"id", "col_a", "col_b"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val"), numReal(float64(i * 10))}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "col_a"},
			&query.QualifiedIdentifier{Column: "col_b"},
		},
		From: &query.TableRef{Name: "aoq_map"},
	}
	cat.CreateView("aoq_map_view", viewStmt)

	// Various column mappings
	queries := []string{
		`SELECT id, col_a, col_b FROM aoq_map_view`,
		`SELECT col_a AS name, col_b AS value FROM aoq_map_view`,
		`SELECT col_a, col_b + 1 AS col_b_plus FROM aoq_map_view`,
		`SELECT * FROM aoq_map_view`,
		`SELECT col_a FROM aoq_map_view WHERE col_b > 50`,
		`SELECT col_a, col_b FROM aoq_map_view ORDER BY col_b DESC`,
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

// TestApplyOuterQuery_WhereEvaluation - WHERE clause variations
func TestApplyOuterQuery_WhereEvaluation(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_where", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		status := "active"
		if i%3 == 0 {
			status = "inactive"
		}
		if i%5 == 0 {
			status = "deleted"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_where",
			Columns: []string{"id", "status", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(status), numReal(float64(i * 10))}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "status"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "aoq_where"},
	}
	cat.CreateView("aoq_where_view", viewStmt)

	// Various WHERE conditions
	queries := []string{
		`SELECT * FROM aoq_where_view WHERE status = 'active'`,
		`SELECT * FROM aoq_where_view WHERE status != 'active'`,
		`SELECT * FROM aoq_where_view WHERE val > 50`,
		`SELECT * FROM aoq_where_view WHERE val BETWEEN 50 AND 100`,
		`SELECT * FROM aoq_where_view WHERE status IN ('active', 'inactive')`,
		`SELECT * FROM aoq_where_view WHERE status LIKE 'act%'`,
		`SELECT * FROM aoq_where_view WHERE val > 50 AND status = 'active'`,
		`SELECT * FROM aoq_where_view WHERE val < 50 OR status = 'deleted'`,
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

// TestApplyOuterQuery_OrderByVariations - ORDER BY variations
func TestApplyOuterQuery_OrderByVariations(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_order", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "priority", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 15; i++ {
		priority := i % 5
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_order",
			Columns: []string{"id", "priority", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(priority)), strReal("item")}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "priority"},
			&query.QualifiedIdentifier{Column: "name"},
		},
		From: &query.TableRef{Name: "aoq_order"},
	}
	cat.CreateView("aoq_order_view", viewStmt)

	// Various ORDER BY scenarios
	queries := []string{
		`SELECT * FROM aoq_order_view ORDER BY id`,
		`SELECT * FROM aoq_order_view ORDER BY id DESC`,
		`SELECT * FROM aoq_order_view ORDER BY priority, id`,
		`SELECT * FROM aoq_order_view ORDER BY priority DESC, id ASC`,
		`SELECT priority, COUNT(*) FROM aoq_order_view GROUP BY priority ORDER BY priority`,
		`SELECT priority, COUNT(*) AS cnt FROM aoq_order_view GROUP BY priority ORDER BY cnt DESC`,
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

// TestApplyOuterQuery_LimitOffsetMore - LIMIT/OFFSET edge cases
func TestApplyOuterQuery_LimitOffsetMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_limit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_limit",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
		From:    &query.TableRef{Name: "aoq_limit"},
	}
	cat.CreateView("aoq_limit_view", viewStmt)

	// Edge cases
	queries := []string{
		`SELECT * FROM aoq_limit_view LIMIT 5`,
		`SELECT * FROM aoq_limit_view LIMIT 0`,
		`SELECT * FROM aoq_limit_view OFFSET 5`,
		`SELECT * FROM aoq_limit_view LIMIT 5 OFFSET 5`,
		`SELECT * FROM aoq_limit_view LIMIT 100`,
		`SELECT * FROM aoq_limit_view OFFSET 100`,
		`SELECT * FROM aoq_limit_view ORDER BY id LIMIT 3 OFFSET 3`,
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

// TestApplyOuterQuery_DistinctCases - DISTINCT variations
func TestApplyOuterQuery_DistinctCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_distinct", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "subcategory", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		category := "A"
		if i > 10 {
			category = "B"
		}
		subcategory := "X"
		if i%2 == 0 {
			subcategory = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_distinct",
			Columns: []string{"id", "category", "subcategory"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), strReal(subcategory)}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "category"},
			&query.QualifiedIdentifier{Column: "subcategory"},
		},
		From: &query.TableRef{Name: "aoq_distinct"},
	}
	cat.CreateView("aoq_distinct_view", viewStmt)

	queries := []string{
		`SELECT DISTINCT category FROM aoq_distinct_view`,
		`SELECT DISTINCT category, subcategory FROM aoq_distinct_view`,
		`SELECT DISTINCT category FROM aoq_distinct_view ORDER BY category`,
		`SELECT DISTINCT category FROM aoq_distinct_view LIMIT 1`,
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

// TestApplyOuterQuery_ComplexAggregates - complex aggregate scenarios
func TestApplyOuterQuery_ComplexAggregates(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_complex_agg", []*query.ColumnDef{
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
			Table:   "aoq_complex_agg",
			Columns: []string{"id", "grp", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), strReal("name")}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "grp"},
			&query.QualifiedIdentifier{Column: "val"},
			&query.QualifiedIdentifier{Column: "name"},
		},
		From: &query.TableRef{Name: "aoq_complex_agg"},
	}
	cat.CreateView("aoq_complex_agg_view", viewStmt)

	queries := []string{
		`SELECT grp, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM aoq_complex_agg_view GROUP BY grp`,
		`SELECT COUNT(*), SUM(val), AVG(val) FROM aoq_complex_agg_view`,
		`SELECT grp, COUNT(DISTINCT name) FROM aoq_complex_agg_view GROUP BY grp`,
		`SELECT grp, SUM(val) as total FROM aoq_complex_agg_view GROUP BY grp HAVING total > 1000`,
		`SELECT grp, AVG(val) as avg_val FROM aoq_complex_agg_view GROUP BY grp HAVING avg_val > 150`,
		`SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM aoq_complex_agg_view GROUP BY grp HAVING cnt > 5 AND total > 1000`,
		`SELECT grp, SUM(val) FROM aoq_complex_agg_view GROUP BY grp ORDER BY SUM(val) DESC`,
		`SELECT grp, COUNT(*) FROM aoq_complex_agg_view GROUP BY grp LIMIT 2`,
		`SELECT grp, COUNT(*) FROM aoq_complex_agg_view GROUP BY grp OFFSET 1`,
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

// TestApplyOuterQuery_ExpressionInSelect - expressions in SELECT
func TestApplyOuterQuery_ExpressionInSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_expr_sel", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_expr_sel",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2)), numReal(float64(i * 3))}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "a"},
			&query.QualifiedIdentifier{Column: "b"},
		},
		From: &query.TableRef{Name: "aoq_expr_sel"},
	}
	cat.CreateView("aoq_expr_sel_view", viewStmt)

	queries := []string{
		`SELECT a + b as sum_ab FROM aoq_expr_sel_view`,
		`SELECT a - b as diff FROM aoq_expr_sel_view`,
		`SELECT a * b as product FROM aoq_expr_sel_view`,
		`SELECT a / 2 as half_a FROM aoq_expr_sel_view`,
		`SELECT a + b, a - b, a * b FROM aoq_expr_sel_view`,
		`SELECT id, (a + b) * 2 as doubled FROM aoq_expr_sel_view`,
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
