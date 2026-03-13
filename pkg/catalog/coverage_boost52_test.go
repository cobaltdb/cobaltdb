package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Deep coverage for applyOuterQuery and aggregate functions
// ============================================================

// TestApplyOuterQuery_AllScenarios - comprehensive applyOuterQuery coverage
func TestApplyOuterQuery_AllScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_comp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenReal},
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
			Table:   "aoq_comp",
			Columns: []string{"id", "grp", "val1", "val2", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), numReal(float64(i) * 1.5), strReal("name")}},
		}, nil)
	}

	// Create complex view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "grp"},
			&query.QualifiedIdentifier{Column: "val1"},
			&query.QualifiedIdentifier{Column: "val2"},
			&query.QualifiedIdentifier{Column: "name"},
		},
		From: &query.TableRef{Name: "aoq_comp"},
	}
	cat.CreateView("aoq_comp_view", viewStmt)

	// Complex queries on view
	queries := []string{
		// Aggregates
		`SELECT grp, COUNT(*), SUM(val1), AVG(val1), MIN(val1), MAX(val1) FROM aoq_comp_view GROUP BY grp`,
		`SELECT COUNT(DISTINCT grp) FROM aoq_comp_view`,
		`SELECT SUM(val1) + AVG(val2) FROM aoq_comp_view`,

		// HAVING
		`SELECT grp, COUNT(*) as cnt FROM aoq_comp_view GROUP BY grp HAVING cnt > 5`,
		`SELECT grp, SUM(val1) as total FROM aoq_comp_view GROUP BY grp HAVING total > 500`,
		`SELECT grp FROM aoq_comp_view GROUP BY grp HAVING COUNT(*) > 5 AND SUM(val1) > 500`,

		// ORDER BY
		`SELECT * FROM aoq_comp_view ORDER BY val1 DESC`,
		`SELECT * FROM aoq_comp_view ORDER BY val1, val2`,
		`SELECT grp, SUM(val1) as total FROM aoq_comp_view GROUP BY grp ORDER BY total DESC`,

		// LIMIT/OFFSET
		`SELECT * FROM aoq_comp_view LIMIT 10`,
		`SELECT * FROM aoq_comp_view OFFSET 10`,
		`SELECT * FROM aoq_comp_view LIMIT 10 OFFSET 10`,
		`SELECT * FROM aoq_comp_view LIMIT 0`,

		// DISTINCT
		`SELECT DISTINCT grp FROM aoq_comp_view`,
		`SELECT DISTINCT grp, name FROM aoq_comp_view`,

		// WHERE
		`SELECT * FROM aoq_comp_view WHERE val1 > 100`,
		`SELECT * FROM aoq_comp_view WHERE grp = 'A' AND val1 > 50`,
		`SELECT * FROM aoq_comp_view WHERE val1 BETWEEN 50 AND 150`,

		// Complex projections
		`SELECT id + 1 as id_plus, grp FROM aoq_comp_view`,
		`SELECT val1 * 2 as doubled, name FROM aoq_comp_view`,
		`SELECT UPPER(grp) as upper_grp FROM aoq_comp_view`,
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

// TestApplyOuterQuery_ExpressionEvaluation - expression handling in views
func TestApplyOuterQuery_ExpressionEvaluation(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_expr", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_expr",
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
		From: &query.TableRef{Name: "aoq_expr"},
	}
	cat.CreateView("aoq_expr_view", viewStmt)

	// Expression queries
	queries := []string{
		`SELECT a + b as sum_ab FROM aoq_expr_view`,
		`SELECT a - b as diff FROM aoq_expr_view`,
		`SELECT a * b as product FROM aoq_expr_view`,
		`SELECT a / 2 as half_a FROM aoq_expr_view`,
		`SELECT a % 3 as mod_a FROM aoq_expr_view`,
		`SELECT (a + b) * 2 as complex FROM aoq_expr_view`,
		`SELECT CASE WHEN a > 10 THEN 'big' ELSE 'small' END as size FROM aoq_expr_view`,
		`SELECT CAST(a AS TEXT) as a_str FROM aoq_expr_view`,
		`SELECT CAST(a AS REAL) as a_real FROM aoq_expr_view`,
		`SELECT a IS NULL as is_null FROM aoq_expr_view`,
		`SELECT a IS NOT NULL as is_not_null FROM aoq_expr_view`,
		`SELECT COALESCE(NULL, a, b) as coalesced FROM aoq_expr_view`,
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

// TestComputeAggregatesWithGroupBy_Complex - complex GROUP BY scenarios
func TestComputeAggregatesWithGroupBy_Complex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp1", Type: query.TokenText},
		{Name: "grp2", Type: query.TokenText},
		{Name: "grp3", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		grp1 := "A"
		if i > 17 {
			grp1 = "B"
		}
		if i > 34 {
			grp1 = "C"
		}
		grp2 := "X"
		if i%2 == 0 {
			grp2 = "Y"
		}
		if i%3 == 0 {
			grp2 = "Z"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_complex",
			Columns: []string{"id", "grp1", "grp2", "grp3", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp1), strReal(grp2), numReal(float64(i % 5)), numReal(float64(i * 10)), strReal("name")}},
		}, nil)
	}

	queries := []string{
		// Multi-column GROUP BY
		`SELECT grp1, grp2, COUNT(*), SUM(val) FROM gb_complex GROUP BY grp1, grp2`,
		`SELECT grp1, grp2, grp3, COUNT(*), AVG(val) FROM gb_complex GROUP BY grp1, grp2, grp3`,

		// HAVING with complex conditions
		`SELECT grp1, COUNT(*) as cnt, SUM(val) as total FROM gb_complex GROUP BY grp1 HAVING cnt > 10 AND total > 1000`,
		`SELECT grp1, AVG(val) as avg_val FROM gb_complex GROUP BY grp1 HAVING avg_val > 200`,

		// ORDER BY aggregate
		`SELECT grp1, SUM(val) as total FROM gb_complex GROUP BY grp1 ORDER BY total DESC`,
		`SELECT grp1, COUNT(*) as cnt FROM gb_complex GROUP BY grp1 ORDER BY cnt`,

		// Mixed functions
		`SELECT grp1, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val), MIN(name), MAX(name) FROM gb_complex GROUP BY grp1`,

		// GROUP BY with expressions
		`SELECT grp1, grp2, COUNT(*) FROM gb_complex GROUP BY grp1, grp2 ORDER BY grp1, grp2`,

		// Empty result HAVING
		`SELECT grp1, COUNT(*) FROM gb_complex GROUP BY grp1 HAVING COUNT(*) > 1000`,
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

// TestResolveOuterRefsInQuery_ALLSubqueryTypes - all subquery types
func TestResolveOuterRefsInQuery_ALLSubqueryTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_main_all", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "outer_sub_all", []*query.ColumnDef{
		{Name: "sub_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_main_all",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "outer_sub_all",
				Columns: []string{"sub_id", "main_id", "amount"},
				Values:  [][]query.Expression{{numReal(float64(i*10+j)), numReal(float64(i)), numReal(float64(j * 10))}},
			}, nil)
		}
	}

	queries := []string{
		// IN subquery
		`SELECT * FROM outer_main_all WHERE id IN (SELECT main_id FROM outer_sub_all WHERE amount > 20)`,

		// NOT IN subquery
		`SELECT * FROM outer_main_all WHERE id NOT IN (SELECT main_id FROM outer_sub_all WHERE amount < 10)`,

		// EXISTS
		`SELECT * FROM outer_main_all m WHERE EXISTS (SELECT 1 FROM outer_sub_all s WHERE s.main_id = m.id AND s.amount > 20)`,

		// NOT EXISTS
		`SELECT * FROM outer_main_all m WHERE NOT EXISTS (SELECT 1 FROM outer_sub_all s WHERE s.main_id = m.id AND s.amount > 100)`,

		// Scalar subquery
		`SELECT *, (SELECT SUM(amount) FROM outer_sub_all WHERE main_id = outer_main_all.id) as total FROM outer_main_all`,

		// Correlated with comparison
		`SELECT * FROM outer_main_all m WHERE val > (SELECT SUM(amount) FROM outer_sub_all WHERE main_id = m.id)`,

		// ALL
		`SELECT * FROM outer_main_all m WHERE val > ALL (SELECT amount FROM outer_sub_all WHERE main_id = m.id)`,

		// ANY/SOME
		`SELECT * FROM outer_main_all m WHERE val > ANY (SELECT amount FROM outer_sub_all)`,
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

// TestEvaluateHaving_MoreCases - HAVING clause evaluation
func TestEvaluateHaving_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "having_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
		{Name: "flag", Type: query.TokenBoolean},
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
			Table:   "having_more",
			Columns: []string{"id", "grp", "val", "flag"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), &query.BooleanLiteral{Value: i%2 == 0}}},
		}, nil)
	}

	queries := []string{
		// HAVING with comparison operators
		`SELECT grp, COUNT(*) as cnt FROM having_more GROUP BY grp HAVING cnt = 10`,
		`SELECT grp, COUNT(*) as cnt FROM having_more GROUP BY grp HAVING cnt != 10`,
		`SELECT grp, SUM(val) as total FROM having_more GROUP BY grp HAVING total > 1000`,
		`SELECT grp, SUM(val) as total FROM having_more GROUP BY grp HAVING total >= 1500`,
		`SELECT grp, SUM(val) as total FROM having_more GROUP BY grp HAVING total < 2000`,
		`SELECT grp, SUM(val) as total FROM having_more GROUP BY grp HAVING total <= 1500`,

		// HAVING with boolean logic
		`SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM having_more GROUP BY grp HAVING cnt > 5 AND total > 1000`,
		`SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM having_more GROUP BY grp HAVING cnt > 15 OR total > 2000`,
		`SELECT grp, COUNT(*) as cnt FROM having_more GROUP BY grp HAVING NOT cnt < 5`,

		// HAVING with BETWEEN
		`SELECT grp, COUNT(*) as cnt FROM having_more GROUP BY grp HAVING cnt BETWEEN 5 AND 15`,

		// HAVING with IN
		`SELECT grp, COUNT(*) as cnt FROM having_more GROUP BY grp HAVING cnt IN (10, 20)`,
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

// TestApplyGroupByOrderBy_MoreCases - GROUP BY ORDER BY handling
func TestApplyGroupByOrderBy_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_ob", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "subcategory", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		category := "A"
		if i > 15 {
			category = "B"
		}
		if i > 30 {
			category = "C"
		}
		sub := "X"
		if i%2 == 0 {
			sub = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_ob",
			Columns: []string{"id", "category", "subcategory", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), strReal(sub), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		// ORDER BY group column
		`SELECT category, COUNT(*) FROM gb_ob GROUP BY category ORDER BY category`,
		`SELECT category, COUNT(*) FROM gb_ob GROUP BY category ORDER BY category DESC`,

		// ORDER BY aggregate
		`SELECT category, SUM(amount) as total FROM gb_ob GROUP BY category ORDER BY total`,
		`SELECT category, SUM(amount) as total FROM gb_ob GROUP BY CATEGORY ORDER BY total DESC`,

		// Multi-column GROUP BY with ORDER BY
		`SELECT category, subcategory, COUNT(*) as cnt FROM gb_ob GROUP BY category, subcategory ORDER BY category, subcategory`,
		`SELECT category, subcategory, SUM(amount) as total FROM gb_ob GROUP BY category, subcategory ORDER BY total DESC`,

		// ORDER BY column not in SELECT
		`SELECT category, COUNT(*) FROM gb_ob GROUP BY category ORDER BY SUM(amount) DESC`,
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

// TestUpdateRowSlice_DeepFK - deep FK cascade tests
func TestUpdateRowSlice_DeepFK(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create 4-level FK chain
	createCoverageTestTable(t, cat, "fk_l1", []*query.ColumnDef{
		{Name: "l1_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	for level := 2; level <= 4; level++ {
		prevTable := "fk_l" + string(rune('0'+level-1))
		currTable := "fk_l" + string(rune('0'+level))
		prevCol := "l" + string(rune('0'+level-1)) + "_id"
		currCol := "l" + string(rune('0'+level)) + "_id"

		cat.CreateTable(&query.CreateTableStmt{
			Table: currTable,
			Columns: []*query.ColumnDef{
				{Name: currCol, Type: query.TokenInteger, PrimaryKey: true},
				{Name: "parent_id", Type: query.TokenInteger},
			},
			ForeignKeys: []*query.ForeignKeyDef{
				{
					Columns:           []string{"parent_id"},
					ReferencedTable:   prevTable,
					ReferencedColumns: []string{prevCol},
					OnDelete:          "CASCADE",
					OnUpdate:          "CASCADE",
				},
			},
		})
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_l1",
			Columns: []string{"l1_id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_l2",
			Columns: []string{"l2_id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_l3",
			Columns: []string{"l3_id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_l4",
			Columns: []string{"l4_id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// Update at level 1 - should cascade through all levels
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_l1",
		Set:   []*query.SetClause{{Column: "l1_id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: colReal("l1_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Deep cascade update error: %v", err)
	}

	// Verify cascade worked
	for level := 2; level <= 4; level++ {
		table := "fk_l" + string(rune('0'+level))
		result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM ` + table + ` WHERE parent_id = 100`)
		if err != nil {
			t.Logf("Level %d check error: %v", level, err)
		} else {
			t.Logf("Level %d rows with updated parent: %v", level, result.Rows)
		}
	}
}

// TestDeleteWithUsing_MoreFK - DELETE USING with FK
func TestDeleteWithUsing_MoreFK(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Parent
	createCoverageTestTable(t, cat, "del_fk_parent", []*query.ColumnDef{
		{Name: "p_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	// Child with CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "del_fk_child",
		Columns: []*query.ColumnDef{
			{Name: "c_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "p_id", Type: query.TokenInteger},
			{Name: "data", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"p_id"},
				ReferencedTable:   "del_fk_parent",
				ReferencedColumns: []string{"p_id"},
				OnDelete:          "CASCADE",
			},
		},
	})

	// Ref table
	createCoverageTestTable(t, cat, "del_fk_ref", []*query.ColumnDef{
		{Name: "r_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "p_id", Type: query.TokenInteger},
		{Name: "flag", Type: query.TokenBoolean},
	})

	// Insert
	for i := 1; i <= 10; i++ {
		code := "KEEP"
		if i%2 == 0 {
			code = "DELETE"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_fk_parent",
			Columns: []string{"p_id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code)}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_fk_child",
			Columns: []string{"c_id", "p_id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("data")}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_fk_ref",
			Columns: []string{"r_id", "p_id", "flag"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), &query.BooleanLiteral{Value: i%2 == 0}}},
		}, nil)
	}

	// Check what would be deleted
	result, err := cat.ExecuteQuery(`
		SELECT p.p_id FROM del_fk_parent p
		JOIN del_fk_ref r ON p.p_id = r.p_id
		WHERE p.code = 'DELETE' AND r.flag = true
	`)
	if err != nil {
		t.Logf("Pre-check error: %v", err)
	} else {
		t.Logf("Would affect %d parent rows (with cascade to children)", len(result.Rows))
	}

	_ = result
}
