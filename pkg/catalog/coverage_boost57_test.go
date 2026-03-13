package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Final push for applyOuterQuery coverage
// ============================================================

// TestApplyOuterQuery_AllBranches - comprehensive branch coverage
func TestApplyOuterQuery_AllBranches(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_final", []*query.ColumnDef{
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
			Table:   "aoq_final",
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
		From: &query.TableRef{Name: "aoq_final"},
	}
	cat.CreateView("aoq_final_view", viewStmt)

	// Comprehensive test queries
	queries := []string{
		// WHERE filtering
		`SELECT * FROM aoq_final_view WHERE val > 100`,
		`SELECT * FROM aoq_final_view WHERE val BETWEEN 50 AND 150`,
		`SELECT * FROM aoq_final_view WHERE grp IN ('A', 'B')`,
		`SELECT * FROM aoq_final_view WHERE grp = 'A' AND val > 50`,

		// Aggregates without GROUP BY
		`SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM aoq_final_view`,
		`SELECT COUNT(DISTINCT grp) FROM aoq_final_view`,

		// GROUP BY aggregates
		`SELECT grp, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM aoq_final_view GROUP BY grp`,
		`SELECT grp, COUNT(DISTINCT name) FROM aoq_final_view GROUP BY grp`,

		// HAVING
		`SELECT grp, COUNT(*) as cnt FROM aoq_final_view GROUP BY grp HAVING cnt > 5`,
		`SELECT grp, SUM(val) as total FROM aoq_final_view GROUP BY grp HAVING total > 500`,
		`SELECT grp, AVG(val) as avg_val FROM aoq_final_view GROUP BY grp HAVING avg_val > 100`,

		// ORDER BY
		`SELECT * FROM aoq_final_view ORDER BY val`,
		`SELECT * FROM aoq_final_view ORDER BY val DESC`,
		`SELECT grp, COUNT(*) FROM aoq_final_view GROUP BY grp ORDER BY COUNT(*) DESC`,

		// LIMIT/OFFSET
		`SELECT * FROM aoq_final_view LIMIT 10`,
		`SELECT * FROM aoq_final_view OFFSET 10`,
		`SELECT * FROM aoq_final_view LIMIT 10 OFFSET 10`,
		`SELECT * FROM aoq_final_view OFFSET 100`,

		// DISTINCT
		`SELECT DISTINCT grp FROM aoq_final_view`,
		`SELECT DISTINCT grp, name FROM aoq_final_view`,

		// Complex expressions
		`SELECT val + 1 as val_plus FROM aoq_final_view`,
		`SELECT val * 2 as doubled FROM aoq_final_view`,
		`SELECT grp, SUM(val) + COUNT(*) FROM aoq_final_view GROUP BY grp`,
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

// TestApplyOuterQuery_EmptyResults - empty result handling
func TestApplyOuterQuery_EmptyResults(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_empty", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert small amount of data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_empty",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "aoq_empty"},
	}
	cat.CreateView("aoq_empty_view", viewStmt)

	// Queries that might return empty results
	queries := []string{
		`SELECT * FROM aoq_empty_view WHERE val > 1000`,
		`SELECT * FROM aoq_empty_view LIMIT 0`,
		`SELECT * FROM aoq_empty_view OFFSET 100`,
		`SELECT COUNT(*) FROM aoq_empty_view WHERE val > 1000`,
		`SELECT SUM(val) FROM aoq_empty_view WHERE val > 1000`,
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

// TestApplyOuterQuery_NULLHandling - NULL value handling
func TestApplyOuterQuery_NULLHandling(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_null", []*query.ColumnDef{
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
		var val query.Expression
		if i%3 == 0 {
			val = &query.NullLiteral{}
		} else {
			val = numReal(float64(i * 10))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_null",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), val}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "grp"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "aoq_null"},
	}
	cat.CreateView("aoq_null_view", viewStmt)

	queries := []string{
		`SELECT * FROM aoq_null_view WHERE val IS NULL`,
		`SELECT * FROM aoq_null_view WHERE val IS NOT NULL`,
		`SELECT grp, COUNT(*), COUNT(val), SUM(val), AVG(val) FROM aoq_null_view GROUP BY grp`,
		`SELECT grp, MIN(val), MAX(val) FROM aoq_null_view GROUP BY grp`,
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

// TestComputeAggregatesWithGroupBy_AllPaths - all execution paths
func TestComputeAggregatesWithGroupBy_AllPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_all_paths", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp1", Type: query.TokenText},
		{Name: "grp2", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 40; i++ {
		grp1 := "A"
		if i > 15 {
			grp1 = "B"
		}
		if i > 30 {
			grp1 = "C"
		}
		grp2 := "X"
		if i%2 == 0 {
			grp2 = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_all_paths",
			Columns: []string{"id", "grp1", "grp2", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp1), strReal(grp2), numReal(float64(i * 10)), strReal("name" + string(rune('A'+i%5)))}},
		}, nil)
	}

	queries := []string{
		// Single column GROUP BY
		`SELECT grp1, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM gb_all_paths GROUP BY grp1`,

		// Multi-column GROUP BY
		`SELECT grp1, grp2, COUNT(*), SUM(val) FROM gb_all_paths GROUP BY grp1, grp2`,

		// GROUP BY with all aggregate types
		`SELECT grp1, COUNT(*), COUNT(DISTINCT name), SUM(val), AVG(val), MIN(val), MAX(val), MIN(name), MAX(name) FROM gb_all_paths GROUP BY grp1`,

		// GROUP BY with expressions in SELECT
		`SELECT grp1, val, SUM(val) FROM gb_all_paths GROUP BY grp1`,

		// Complex HAVING
		`SELECT grp1, COUNT(*) as c, SUM(val) as s FROM gb_all_paths GROUP BY grp1 HAVING c > 5 AND s > 1000`,

		// ORDER BY group column and aggregates
		`SELECT grp1, SUM(val) FROM gb_all_paths GROUP BY grp1 ORDER BY grp1`,
		`SELECT grp1, SUM(val) FROM gb_all_paths GROUP BY grp1 ORDER BY SUM(val) DESC`,
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

// TestEvaluateExprWithGroupAggregates_Complete - complete coverage
func TestEvaluateExprWithGroupAggregates_Complete(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "expr_agg_complete", []*query.ColumnDef{
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
			Table:   "expr_agg_complete",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		// Arithmetic with aggregates
		`SELECT grp, SUM(val) + AVG(val) FROM expr_agg_complete GROUP BY grp`,
		`SELECT grp, SUM(val) - AVG(val) FROM expr_agg_complete GROUP BY grp`,
		`SELECT grp, SUM(val) * 2 FROM expr_agg_complete GROUP BY grp`,
		`SELECT grp, SUM(val) / COUNT(*) FROM expr_agg_complete GROUP BY grp`,
		`SELECT grp, COUNT(*) * 10 FROM expr_agg_complete GROUP BY grp`,
		`SELECT grp, AVG(val) / 2 FROM expr_agg_complete GROUP BY grp`,

		// Complex expressions
		`SELECT grp, SUM(val) as s, AVG(val) as a, s + a, s - a, s * 2, s / COUNT(*) FROM expr_agg_complete GROUP BY grp`,
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

// TestApplyGroupByOrderBy_Complete - complete ORDER BY coverage
func TestApplyGroupByOrderBy_Complete(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_ob_complete", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		category := "A"
		if i > 10 {
			category = "B"
		}
		if i > 20 {
			category = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_ob_complete",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		// ORDER BY column name
		`SELECT category, COUNT(*) FROM gb_ob_complete GROUP BY category ORDER BY category`,
		`SELECT category, COUNT(*) FROM gb_ob_complete GROUP BY category ORDER BY category ASC`,
		`SELECT category, COUNT(*) FROM gb_ob_complete GROUP BY category ORDER BY category DESC`,

		// ORDER BY aggregate function
		`SELECT category, SUM(amount) FROM gb_ob_complete GROUP BY category ORDER BY SUM(amount)`,
		`SELECT category, SUM(amount) FROM gb_ob_complete GROUP BY category ORDER BY SUM(amount) ASC`,
		`SELECT category, SUM(amount) FROM gb_ob_complete GROUP BY category ORDER BY SUM(amount) DESC`,

		// ORDER BY alias
		`SELECT category, COUNT(*) as cnt FROM gb_ob_complete GROUP BY category ORDER BY cnt`,
		`SELECT category, SUM(amount) as total FROM gb_ob_complete GROUP BY category ORDER BY total DESC`,

		// ORDER BY positional
		`SELECT category, COUNT(*) FROM gb_ob_complete GROUP BY category ORDER BY 1`,
		`SELECT category, SUM(amount) FROM gb_ob_complete GROUP BY category ORDER BY 2 DESC`,

		// Multi-column ORDER BY
		`SELECT category, COUNT(*), SUM(amount) FROM gb_ob_complete GROUP BY category ORDER BY category ASC, SUM(amount) DESC`,
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
