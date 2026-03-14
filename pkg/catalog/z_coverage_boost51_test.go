package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_DeleteRowLockedComplex targets deleteRowLocked deeply
func TestCoverage_DeleteRowLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_row_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_row_complex",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("val")}},
		}, nil)
	}

	// Delete with various conditions
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row_complex",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(25),
		},
	}, nil)

	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row_complex",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "a"},
			Operator: query.TokenGt,
			Right:    numReal(40),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_row_complex")
	t.Logf("Count after deletes: %v", result.Rows)
}

// TestCoverage_UpdateLockedComplex targets updateLocked deeply
func TestCoverage_UpdateLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_lock_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "status", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		status := "active"
		if i > 30 {
			status = "inactive"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_lock_complex",
			Columns: []string{"id", "val", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(status)}},
		}, nil)
	}

	// Update single row by PK
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_lock_complex",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	// Update with range
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_lock_complex",
		Set:   []*query.SetClause{{Column: "status", Value: strReal("updated")}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(10),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenLt,
				Right:    numReal(20),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_lock_complex WHERE status = 'updated'")
	t.Logf("Count after updates: %v", result.Rows)
}

// TestCoverage_InsertLockedDeep targets insertLocked deeply
func TestCoverage_InsertLockedDeep(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_lock_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	// Single insert
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_lock_complex",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(1), numReal(100), strReal("single")}},
	}, nil)

	// Multi-row insert
	var values [][]query.Expression
	for i := 2; i <= 20; i++ {
		values = append(values, []query.Expression{numReal(float64(i)), numReal(float64(i * 10)), strReal("multi")})
	}
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_lock_complex",
		Columns: []string{"id", "a", "b"},
		Values:  values,
	}, nil)

	// Insert in transaction
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_lock_complex",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(21), numReal(210), strReal("txn")}},
	}, nil)
	cat.CommitTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_lock_complex")
	t.Logf("Count after inserts: %v", result.Rows)
}

// TestCoverage_EvaluateWhereComplex targets evaluateWhere deeply
func TestCoverage_EvaluateWhereComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_complex_deep", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenText},
	})

	for i := 1; i <= 60; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_complex_deep",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i % 10)), strReal("val")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_complex_deep WHERE a > 10 AND a < 30",
		"SELECT * FROM where_complex_deep WHERE a < 10 OR a > 50",
		"SELECT * FROM where_complex_deep WHERE NOT (a > 40)",
		"SELECT * FROM where_complex_deep WHERE a IN (5, 10, 15, 20, 25)",
		"SELECT * FROM where_complex_deep WHERE a BETWEEN 20 AND 30",
		"SELECT * FROM where_complex_deep WHERE c LIKE 'va%'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE error: %v", err)
		} else {
			t.Logf("WHERE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_RollbackToSavepointDeep targets RollbackToSavepoint deeply
func TestCoverage_RollbackToSavepointDeep(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_deep", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)

	// Insert
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_deep",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}},
	}, nil)

	cat.Savepoint("sp1")

	// Update
	cat.Update(ctx, &query.UpdateStmt{
		Table: "sp_deep",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("updated")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	cat.Savepoint("sp2")

	// Delete
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "sp_deep",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	cat.Savepoint("sp3")

	// Rollback to sp2 (undelete)
	err := cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("Rollback to sp2 error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_deep")
	t.Logf("Count after rollback to sp2: %v", result.Rows)

	// Rollback to sp1 (unupdate)
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	result, _ = cat.ExecuteQuery("SELECT val FROM sp_deep WHERE id = 1")
	t.Logf("Val after rollback to sp1: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_ApplyOuterQueryDeep targets applyOuterQuery deeply
func TestCoverage_ApplyOuterQueryDeep(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_deep", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "cat", Type: query.TokenText},
	})

	for i := 1; i <= 40; i++ {
		c := "A"
		if i > 20 {
			c = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_deep",
			Columns: []string{"id", "val", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(c)}},
		}, nil)
	}

	// Create view with GROUP BY and ORDER BY
	cat.CreateView("view_deep", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "cat"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}},
		},
		From:    &query.TableRef{Name: "outer_deep"},
		GroupBy: []query.Expression{&query.Identifier{Name: "cat"}},
		OrderBy: []*query.OrderByExpr{{Expr: &query.Identifier{Name: "cat"}}},
	})

	// Query view with filter
	result, _ := cat.ExecuteQuery("SELECT * FROM view_deep WHERE cat = 'A'")
	t.Logf("View query returned %d rows", len(result.Rows))

	// Create view with LIMIT
	cat.CreateView("view_limit", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From:  &query.TableRef{Name: "outer_deep"},
		Limit: &query.NumberLiteral{Value: 10},
	})

	result, _ = cat.ExecuteQuery("SELECT * FROM view_limit WHERE val > 100")
	t.Logf("View with limit returned %d rows", len(result.Rows))
}
