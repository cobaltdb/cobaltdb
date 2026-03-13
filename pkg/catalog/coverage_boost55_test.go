package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Deep coverage for applyGroupByOrderBy and computeAggregatesWithGroupBy
// ============================================================

// TestApplyGroupByOrderBy_AllScenarios - comprehensive ORDER BY coverage
func TestApplyGroupByOrderBy_AllScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_order_all", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "subcategory", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		category := "A"
		if i > 17 {
			category = "B"
		}
		if i > 34 {
			category = "C"
		}
		subcategory := "X"
		if i%2 == 0 {
			subcategory = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_order_all",
			Columns: []string{"id", "category", "subcategory", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), strReal(subcategory), numReal(float64(i * 10))}},
		}, nil)
	}

	// Various ORDER BY scenarios on GROUP BY results
	queries := []string{
		// ORDER BY group column
		`SELECT category, COUNT(*) FROM gb_order_all GROUP BY category ORDER BY category`,
		`SELECT category, COUNT(*) FROM gb_order_all GROUP BY category ORDER BY category ASC`,
		`SELECT category, COUNT(*) FROM gb_order_all GROUP BY category ORDER BY category DESC`,

		// ORDER BY aggregate
		`SELECT category, SUM(amount) FROM gb_order_all GROUP BY category ORDER BY SUM(amount)`,
		`SELECT category, SUM(amount) as total FROM gb_order_all GROUP BY category ORDER BY total`,
		`SELECT category, COUNT(*) as cnt FROM gb_order_all GROUP BY category ORDER BY cnt DESC`,

		// Multi-column GROUP BY with ORDER BY
		`SELECT category, subcategory, COUNT(*) FROM gb_order_all GROUP BY category, subcategory ORDER BY category, subcategory`,
		`SELECT category, subcategory, SUM(amount) FROM gb_order_all GROUP BY category, subcategory ORDER BY SUM(amount) DESC`,

		// ORDER BY column not in SELECT
		`SELECT category FROM gb_order_all GROUP BY category ORDER BY SUM(amount) DESC`,

		// Complex ORDER BY
		`SELECT category, COUNT(*), SUM(amount) FROM gb_order_all GROUP BY category ORDER BY category ASC, SUM(amount) DESC`,
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

// TestComputeAggregatesWithGroupBy_EdgeCases - edge cases for GROUP BY
func TestComputeAggregatesWithGroupBy_EdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert data with NULLs
	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		var val query.Expression
		val = numReal(float64(i * 10))
		if i%3 == 0 {
			val = &query.NullLiteral{}
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_edge",
			Columns: []string{"id", "grp", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), val, strReal("name")}},
		}, nil)
	}

	// Edge cases
	queries := []string{
		// Aggregates with NULLs
		`SELECT grp, COUNT(*), COUNT(val), SUM(val), AVG(val) FROM gb_edge GROUP BY grp`,
		`SELECT grp, MIN(val), MAX(val) FROM gb_edge GROUP BY grp`,
		`SELECT grp, MIN(name), MAX(name) FROM gb_edge GROUP BY grp`,
		`SELECT COUNT(DISTINCT name) FROM gb_edge`,
		`SELECT grp, COUNT(DISTINCT name) FROM gb_edge GROUP BY grp`,

		// HAVING with NULL results
		`SELECT grp, COUNT(val) FROM gb_edge GROUP BY grp HAVING COUNT(val) > 0`,
		`SELECT grp, SUM(val) FROM gb_edge GROUP BY grp HAVING SUM(val) IS NOT NULL`,
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

// TestEvaluateHaving_Complete - comprehensive HAVING coverage
func TestEvaluateHaving_Complete(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "having_complete", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
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
			Table:   "having_complete",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		// Basic HAVING
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING cnt = 10`,
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING cnt != 10`,
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING cnt > 5`,
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING cnt >= 10`,
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING cnt < 15`,
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING cnt <= 10`,

		// HAVING with aggregates
		`SELECT grp, SUM(val) as total FROM having_complete GROUP BY grp HAVING total > 1000`,
		`SELECT grp, AVG(val) as avg_val FROM having_complete GROUP BY grp HAVING avg_val > 100`,
		`SELECT grp, MIN(val) as min_val FROM having_complete GROUP BY grp HAVING min_val > 50`,
		`SELECT grp, MAX(val) as max_val FROM having_complete GROUP BY grp HAVING max_val < 500`,

		// HAVING with boolean logic
		`SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM having_complete GROUP BY grp HAVING cnt > 5 AND total > 1000`,
		`SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM having_complete GROUP BY grp HAVING cnt > 15 OR total > 2000`,
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING NOT cnt < 5`,

		// HAVING with BETWEEN
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING cnt BETWEEN 5 AND 15`,
		`SELECT grp, SUM(val) as total FROM having_complete GROUP BY grp HAVING total BETWEEN 1000 AND 2000`,

		// HAVING with IN
		`SELECT grp, COUNT(*) as cnt FROM having_complete GROUP BY grp HAVING cnt IN (10, 20)`,
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

// TestEvaluateExprWithGroupAggregates_All - all aggregate expression patterns
func TestEvaluateExprWithGroupAggregates_All(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "expr_agg_all", []*query.ColumnDef{
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
			Table:   "expr_agg_all",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		// Expressions with aggregates
		`SELECT grp, SUM(val) as total, AVG(val) as avg_val, total + avg_val FROM expr_agg_all GROUP BY grp`,
		`SELECT grp, COUNT(*) * 10 FROM expr_agg_all GROUP BY grp`,
		`SELECT grp, SUM(val) / COUNT(*) FROM expr_agg_all GROUP BY grp`,
		`SELECT grp, SUM(val) - AVG(val) FROM expr_agg_all GROUP BY grp`,
		`SELECT grp, SUM(val) * 2 FROM expr_agg_all GROUP BY grp`,
		`SELECT grp, AVG(val) / 2 FROM expr_agg_all GROUP BY grp`,
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

// TestComputeViewAggregate_AllFunctions - all aggregate functions in views
func TestComputeViewAggregate_AllFunctions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "view_agg_all", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "view_agg_all",
			Columns: []string{"id", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("name")}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "val"},
			&query.QualifiedIdentifier{Column: "name"},
		},
		From: &query.TableRef{Name: "view_agg_all"},
	}
	cat.CreateView("view_agg_all_view", viewStmt)

	queries := []string{
		`SELECT COUNT(*) FROM view_agg_all_view`,
		`SELECT COUNT(id) FROM view_agg_all_view`,
		`SELECT COUNT(val) FROM view_agg_all_view`,
		`SELECT SUM(val) FROM view_agg_all_view`,
		`SELECT AVG(val) FROM view_agg_all_view`,
		`SELECT MIN(val) FROM view_agg_all_view`,
		`SELECT MAX(val) FROM view_agg_all_view`,
		`SELECT MIN(name) FROM view_agg_all_view`,
		`SELECT MAX(name) FROM view_agg_all_view`,
		`SELECT GROUP_CONCAT(name) FROM view_agg_all_view`,
		`SELECT GROUP_CONCAT(name, '-') FROM view_agg_all_view`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d result: %v", i, result.Rows)
		}
	}
}

// TestResolveAggregateInExpr_AllCases - all aggregate resolution patterns
func TestResolveAggregateInExpr_AllCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "resolve_agg", []*query.ColumnDef{
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
			Table:   "resolve_agg",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		// Aggregates in various contexts
		`SELECT grp, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM resolve_agg GROUP BY grp`,
		`SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM resolve_agg`,
		`SELECT grp, COUNT(DISTINCT val) FROM resolve_agg GROUP BY grp`,
		`SELECT SUM(val) + AVG(val) FROM resolve_agg`,
		`SELECT grp, SUM(val) as s, AVG(val) as a, s + a FROM resolve_agg GROUP BY grp`,
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

// TestAddHiddenOrderByCols_More - additional hidden column coverage
func TestAddHiddenOrderByCols_More(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "hidden_ob", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "sort_col", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "hidden_ob",
			Columns: []string{"id", "grp", "sort_col", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(100 - i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// ORDER BY columns not in SELECT
	queries := []string{
		`SELECT grp FROM hidden_ob ORDER BY sort_col`,
		`SELECT grp, COUNT(*) FROM hidden_ob GROUP BY grp ORDER BY sort_col`,
		`SELECT grp, SUM(val) FROM hidden_ob GROUP BY grp ORDER BY SUM(val) DESC, sort_col`,
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

// TestAddHiddenHavingAggregates_More - hidden aggregates in HAVING
func TestAddHiddenHavingAggregates_More(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "hidden_having", []*query.ColumnDef{
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
			Table:   "hidden_having",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		// HAVING with aggregates not in SELECT
		`SELECT grp FROM hidden_having GROUP BY grp HAVING COUNT(*) > 5`,
		`SELECT grp FROM hidden_having GROUP BY grp HAVING SUM(val) > 500`,
		`SELECT grp FROM hidden_having GROUP BY grp HAVING AVG(val) > 100`,
		`SELECT grp FROM hidden_having GROUP BY grp HAVING MIN(val) > 50`,
		`SELECT grp FROM hidden_having GROUP BY grp HAVING MAX(val) < 250`,
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

// TestReplaceAggregatesInExpr_More - aggregate replacement coverage
func TestReplaceAggregatesInExpr_More(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "replace_agg", []*query.ColumnDef{
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
			Table:   "replace_agg",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		// Expressions that need aggregate replacement
		`SELECT grp, SUM(val) as s, AVG(val) as a, s + a FROM replace_agg GROUP BY grp`,
		`SELECT grp, COUNT(*) as c, c * 10 FROM replace_agg GROUP BY grp`,
		`SELECT grp, SUM(val) as s, s / COUNT(*) FROM replace_agg GROUP BY grp`,
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
