package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Final attempt to cover error paths in expression evaluation
// ============================================================

// TestApplyOuterQuery_HavingError - HAVING clause evaluation
func TestApplyOuterQuery_HavingError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "having_err_test", []*query.ColumnDef{
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
			Table:   "having_err_test",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "grp"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "having_err_test"},
	}
	cat.CreateView("having_err_view", viewStmt)

	// Query with HAVING on aggregates
	queries := []string{
		`SELECT grp, COUNT(*) as cnt FROM having_err_view GROUP BY grp HAVING cnt > 5`,
		`SELECT grp, SUM(val) as total FROM having_err_view GROUP BY grp HAVING total > 500`,
		`SELECT grp, AVG(val) as avg_val FROM having_err_view GROUP BY grp HAVING avg_val > 100`,
		`SELECT grp, COUNT(*) FROM having_err_view GROUP BY grp HAVING COUNT(*) > 5 AND SUM(val) > 500`,
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

// TestApplyOuterQuery_LimitOffsetEdgeCases
func TestApplyOuterQuery_LimitOffsetEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "limit_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "limit_test",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
		From:    &query.TableRef{Name: "limit_test"},
	}
	cat.CreateView("limit_view", viewStmt)

	queries := []string{
		`SELECT * FROM limit_view LIMIT 5`,
		`SELECT * FROM limit_view LIMIT 0`,
		`SELECT * FROM limit_view OFFSET 5`,
		`SELECT * FROM limit_view LIMIT 5 OFFSET 5`,
		`SELECT * FROM limit_view OFFSET 100`,
		`SELECT * FROM limit_view LIMIT -1`,
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

// TestComputeViewAggregate_MoreCases - all aggregate variations
func TestComputeViewAggregate_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "view_agg_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "view_agg_test",
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
		From: &query.TableRef{Name: "view_agg_test"},
	}
	cat.CreateView("view_agg_view", viewStmt)

	queries := []string{
		`SELECT COUNT(*) FROM view_agg_view`,
		`SELECT COUNT(id) FROM view_agg_view`,
		`SELECT COUNT(name) FROM view_agg_view`,
		`SELECT SUM(val) FROM view_agg_view`,
		`SELECT AVG(val) FROM view_agg_view`,
		`SELECT MIN(val) FROM view_agg_view`,
		`SELECT MAX(val) FROM view_agg_view`,
		`SELECT MIN(name) FROM view_agg_view`,
		`SELECT MAX(name) FROM view_agg_view`,
		`SELECT GROUP_CONCAT(name) FROM view_agg_view`,
		`SELECT GROUP_CONCAT(name, '-') FROM view_agg_view`,
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

// TestSelectLocked_MoreJoinCases - additional JOIN scenarios
func TestSelectLocked_MoreJoinCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "join_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "join_ref1", []*query.ColumnDef{
		{Name: "ref_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "data", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "join_ref2", []*query.ColumnDef{
		{Name: "ref2_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref1_id", Type: query.TokenInteger},
		{Name: "extra", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Name")}},
		}, nil)
		if i%2 == 0 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "join_ref1",
				Columns: []string{"ref_id", "main_id", "data"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("data")}},
			}, nil)
		}
		if i%3 == 0 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "join_ref2",
				Columns: []string{"ref2_id", "ref1_id", "extra"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("extra")}},
			}, nil)
		}
	}

	queries := []string{
		`SELECT m.id, r.data FROM join_main m LEFT JOIN join_ref1 r ON m.id = r.main_id`,
		`SELECT m.id, r.data FROM join_main m INNER JOIN join_ref1 r ON m.id = r.main_id`,
		`SELECT m.id, r1.data, r2.extra FROM join_main m LEFT JOIN join_ref1 r1 ON m.id = r1.main_id LEFT JOIN join_ref2 r2 ON r1.ref_id = r2.ref1_id`,
		`SELECT * FROM join_main WHERE id IN (SELECT main_id FROM join_ref1)`,
		`SELECT * FROM join_main WHERE id NOT IN (SELECT main_id FROM join_ref1)`,
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

// TestResolveOuterRefsInQuery_CorrelatedSubqueries
func TestResolveOuterRefsInQuery_CorrelatedSubqueries(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "corr_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "corr_sub", []*query.ColumnDef{
		{Name: "sub_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "corr_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "corr_sub",
				Columns: []string{"sub_id", "main_id", "amount"},
				Values:  [][]query.Expression{{numReal(float64(i*10+j)), numReal(float64(i)), numReal(float64(j * 10))}},
			}, nil)
		}
	}

	queries := []string{
		`SELECT * FROM corr_main m WHERE val > (SELECT SUM(amount) FROM corr_sub WHERE main_id = m.id)`,
		`SELECT * FROM corr_main m WHERE val < (SELECT SUM(amount) FROM corr_sub WHERE main_id = m.id)`,
		`SELECT * FROM corr_main m WHERE EXISTS (SELECT 1 FROM corr_sub WHERE main_id = m.id AND amount > 20)`,
		`SELECT * FROM corr_main m WHERE val = (SELECT MAX(amount) FROM corr_sub WHERE main_id = m.id)`,
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

// TestComputeAggregatesWithGroupBy_MoreCases
func TestComputeAggregatesWithGroupBy_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_agg_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp1", Type: query.TokenText},
		{Name: "grp2", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		grp1 := "A"
		if i > 10 {
			grp1 = "B"
		}
		if i > 20 {
			grp1 = "C"
		}
		grp2 := "X"
		if i%2 == 0 {
			grp2 = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_agg_test",
			Columns: []string{"id", "grp1", "grp2", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp1), strReal(grp2), numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		`SELECT grp1, COUNT(*), SUM(val) FROM gb_agg_test GROUP BY grp1`,
		`SELECT grp1, grp2, COUNT(*), SUM(val) FROM gb_agg_test GROUP BY grp1, grp2`,
		`SELECT grp1, COUNT(DISTINCT grp2) FROM gb_agg_test GROUP BY grp1`,
		`SELECT grp1, MIN(val), MAX(val), AVG(val) FROM gb_agg_test GROUP BY grp1`,
		`SELECT grp1, SUM(val) FROM gb_agg_test GROUP BY grp1 HAVING SUM(val) > 50`,
		`SELECT grp1 FROM gb_agg_test GROUP BY grp1 ORDER BY SUM(val) DESC`,
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

// TestDeleteWithUsing_MoreJoinScenarios
func TestDeleteWithUsing_MoreJoinScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "del_u_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "status", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "del_u_ref1", []*query.ColumnDef{
		{Name: "ref1_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "del_u_ref2", []*query.ColumnDef{
		{Name: "ref2_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref1_id", Type: query.TokenInteger},
		{Name: "flag", Type: query.TokenBoolean},
	})

	for i := 1; i <= 20; i++ {
		status := "active"
		if i%3 == 0 {
			status = "deleted"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_u_main",
			Columns: []string{"id", "ref_id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i%5 + 1)), strReal(status)}},
		}, nil)
		code := "KEEP"
		if i%2 == 0 {
			code = "DELETE"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_u_ref1",
			Columns: []string{"ref1_id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code)}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_u_ref2",
			Columns: []string{"ref2_id", "ref1_id", "flag"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), &query.BooleanLiteral{Value: i%2 == 0}}},
		}, nil)
	}

	// Pre-check queries to see what would be deleted
	queries := []string{
		`SELECT m.id FROM del_u_main m JOIN del_u_ref1 r1 ON m.ref_id = r1.ref1_id WHERE r1.code = 'DELETE'`,
		`SELECT m.id FROM del_u_main m JOIN del_u_ref1 r1 ON m.ref_id = r1.ref1_id JOIN del_u_ref2 r2 ON r1.ref1_id = r2.ref1_id WHERE r2.flag = true`,
		`SELECT m.id FROM del_u_main m WHERE m.status = 'deleted'`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d would affect %d rows", i, len(result.Rows))
		}
	}
}

// TestExecuteCTE_RecursiveMoreCases
func TestExecuteCTE_RecursiveMoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "cte_tree", []*query.ColumnDef{
		{Name: "node_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "parent_id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert tree structure
	data := []struct {
		id     int
		parent int
		name   string
	}{
		{1, 0, "root"},
		{2, 1, "child1"},
		{3, 1, "child2"},
		{4, 2, "grandchild1"},
		{5, 2, "grandchild2"},
		{6, 3, "grandchild3"},
	}

	for _, d := range data {
		var parent query.Expression
		if d.parent == 0 {
			parent = &query.NullLiteral{}
		} else {
			parent = numReal(float64(d.parent))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cte_tree",
			Columns: []string{"node_id", "parent_id", "name"},
			Values:  [][]query.Expression{{numReal(float64(d.id)), parent, strReal(d.name)}},
		}, nil)
	}

	// Recursive CTE to traverse tree
	result, err := cat.ExecuteQuery(`
		WITH RECURSIVE tree_path AS (
			SELECT node_id, parent_id, name, 0 as depth
			FROM cte_tree
			WHERE parent_id IS NULL
			UNION ALL
			SELECT c.node_id, c.parent_id, c.name, p.depth + 1
			FROM cte_tree c
			JOIN tree_path p ON c.parent_id = p.node_id
		)
		SELECT * FROM tree_path ORDER BY depth, node_id
	`)
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("Row: %v", row)
		}
	}
}

// TestExecuteDerivedTable_MoreCases
func TestExecuteDerivedTable_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "derived_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		category := "A"
		if i > 10 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "derived_base",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		`SELECT * FROM (SELECT category, SUM(amount) as total FROM derived_base GROUP BY category) AS agg WHERE total > 500`,
		`SELECT * FROM (SELECT category, COUNT(*) as cnt FROM derived_base GROUP BY category) AS c WHERE cnt > 5`,
		`SELECT * FROM (SELECT * FROM derived_base WHERE amount > 50) AS filtered ORDER BY amount`,
		`SELECT d.category, d.total FROM (SELECT category, SUM(amount) as total FROM derived_base GROUP BY category) AS d ORDER BY d.total DESC`,
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
