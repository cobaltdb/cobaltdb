package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_CorrelatedSubqueryExtended targets resolveOuterRefsInQuery
func TestCoverage_CorrelatedSubqueryExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "corr_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "corr_dept", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "corr_dept",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales")}, {numReal(2), strReal("Eng")}},
	}, nil)

	for i := 1; i <= 20; i++ {
		deptID := 1
		if i > 10 {
			deptID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "corr_emp",
			Columns: []string{"id", "dept_id", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(deptID)), numReal(float64(3000 + i*100))}},
		}, nil)
	}

	queries := []string{
		"SELECT d.name, (SELECT AVG(e.salary) FROM corr_emp e WHERE e.dept_id = d.id) as avg_sal FROM corr_dept d",
		"SELECT d.name, (SELECT SUM(e.salary) FROM corr_emp e WHERE e.dept_id = d.id) as total FROM corr_dept d",
		"SELECT d.name FROM corr_dept d WHERE (SELECT COUNT(*) FROM corr_emp e WHERE e.dept_id = d.id) > 5",
		"SELECT e.id, e.salary FROM corr_emp e WHERE e.salary > (SELECT AVG(e2.salary) FROM corr_emp e2 WHERE e2.dept_id = e.dept_id)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Correlated subquery error: %v", err)
		} else {
			t.Logf("Correlated subquery returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DeleteWithIndex targets deleteRowLocked with index
func TestCoverage_DeleteWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_code",
		Table:   "del_idx",
		Columns: []string{"code"},
	})

	for i := 1; i <= 30; i++ {
		code := "C" + string(rune('A'+i%3))
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_idx",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code), numReal(float64(i * 10))}},
		}, nil)
	}

	// Delete using index lookup
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_idx",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "code"},
			Operator: query.TokenEq,
			Right:    strReal("CA"),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_idx")
	t.Logf("Count after delete: %v", result.Rows)
}

// TestCoverage_UpdateWithComplexWhere targets updateLocked with complex conditions
func TestCoverage_UpdateWithComplexWhere(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
		{Name: "c", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		b := "X"
		if i%2 == 0 {
			b = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_complex",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal(b), numReal(float64(i * 2))}},
		}, nil)
	}

	// Complex AND condition
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_complex",
		Set:   []*query.SetClause{{Column: "a", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "a"},
				Operator: query.TokenGt,
				Right:    numReal(10),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "a"},
				Operator: query.TokenLt,
				Right:    numReal(30),
			},
		},
	}, nil)

	// Complex OR condition
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_complex",
		Set:   []*query.SetClause{{Column: "b", Value: strReal("UPDATED")}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "a"},
				Operator: query.TokenLt,
				Right:    numReal(5),
			},
			Operator: query.TokenOr,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "a"},
				Operator: query.TokenGt,
				Right:    numReal(45),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_complex WHERE a = 999 OR b = 'UPDATED'")
	t.Logf("Count after updates: %v", result.Rows)
}

// TestCoverage_JoinAggregateExpr targets evaluateExprWithGroupAggregatesJoin
func TestCoverage_JoinAggregateExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jae_prod", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat_id", Type: query.TokenInteger},
		{Name: "price", Type: query.TokenInteger},
		{Name: "qty", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jae_cat", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jae_cat",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	for i := 1; i <= 50; i++ {
		catID := 1
		if i > 25 {
			catID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jae_prod",
			Columns: []string{"id", "cat_id", "price", "qty"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(catID)), numReal(float64(i * 10)), numReal(float64(i%5 + 1))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.name, SUM(p.price * p.qty) as revenue, AVG(p.price) as avg_price FROM jae_prod p JOIN jae_cat c ON p.cat_id = c.id GROUP BY c.name HAVING revenue > 5000",
		"SELECT c.name, COUNT(*) * 100 as scaled, SUM(p.price) / COUNT(*) as calc_avg FROM jae_prod p JOIN jae_cat c ON p.cat_id = c.id GROUP BY c.name",
		"SELECT c.name, MAX(p.price) - MIN(p.price) as range_val FROM jae_prod p JOIN jae_cat c ON p.cat_id = c.id GROUP BY c.name",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN aggregate expression error: %v", err)
		} else {
			t.Logf("JOIN aggregate expression returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOuterQueryExtended targets applyOuterQuery
func TestCoverage_ApplyOuterQueryExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "cat", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		c := "A"
		if i > 15 {
			c = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_base",
			Columns: []string{"id", "val", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(c)}},
		}, nil)
	}

	// Create view with GROUP BY
	cat.CreateView("view_grp", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "cat"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}},
		},
		From:    &query.TableRef{Name: "outer_base"},
		GroupBy: []query.Expression{&query.Identifier{Name: "cat"}},
	})

	// Query the view with outer filter
	result, _ := cat.ExecuteQuery("SELECT * FROM view_grp WHERE cat = 'A'")
	t.Logf("View query returned %d rows", len(result.Rows))

	// Create view with DISTINCT
	cat.CreateView("view_distinct", &query.SelectStmt{
		Columns:  []query.Expression{&query.Identifier{Name: "cat"}},
		From:     &query.TableRef{Name: "outer_base"},
		Distinct: true,
	})

	result, _ = cat.ExecuteQuery("SELECT * FROM view_distinct")
	t.Logf("Distinct view returned %d rows", len(result.Rows))
}
