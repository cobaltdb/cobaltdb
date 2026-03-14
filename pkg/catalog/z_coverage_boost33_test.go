package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_DeleteRowLockedMore tests deleteRowLocked with more scenarios
func TestCoverage_DeleteRowLockedMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_row_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "status", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		status := "active"
		if i > 15 {
			status = "inactive"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_row_more",
			Columns: []string{"id", "val", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(status)}},
		}, nil)
	}

	// Delete with AND condition
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row_more",
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(5),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenLt,
				Right:    numReal(10),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_row_more")
	t.Logf("Count after AND delete: %v", result.Rows)

	// Delete with IN condition
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row_more",
		Where: &query.InExpr{
			Expr: &query.Identifier{Name: "id"},
			List: []query.Expression{numReal(1), numReal(2), numReal(3)},
		},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM del_row_more")
	t.Logf("Count after IN delete: %v", result.Rows)

	// Delete with OR condition
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row_more",
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "status"},
				Operator: query.TokenEq,
				Right:    strReal("inactive"),
			},
			Operator: query.TokenOr,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(18),
			},
		},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM del_row_more")
	t.Logf("Count after OR delete: %v", result.Rows)
}

// TestCoverage_CTEUnionMore tests executeCTEUnion with more scenarios
func TestCoverage_CTEUnionMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_u3", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "cte_u4", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_u3",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}, {numReal(3), numReal(30)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_u4",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), numReal(20)}, {numReal(3), numReal(30)}, {numReal(4), numReal(40)}},
	}, nil)

	queries := []string{
		"WITH u AS (SELECT val FROM cte_u3 UNION SELECT val FROM cte_u4) SELECT * FROM u ORDER BY val",
		"WITH u AS (SELECT val FROM cte_u3 UNION ALL SELECT val FROM cte_u4) SELECT COUNT(*) FROM u",
		"WITH u AS (SELECT * FROM cte_u3 INTERSECT SELECT * FROM cte_u4) SELECT * FROM u",
		"WITH u AS (SELECT * FROM cte_u3 EXCEPT SELECT * FROM cte_u4) SELECT * FROM u",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CTE UNION error: %v", err)
		} else {
			t.Logf("CTE UNION returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOuterQueryMore tests applyOuterQuery with more scenarios
func TestCoverage_ApplyOuterQueryMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_base2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "grp", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_base2",
			Columns: []string{"id", "val", "grp"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(grp)}},
		}, nil)
	}

	// Create views with different characteristics
	err := cat.CreateView("view_simple", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "outer_base2"},
	})
	if err != nil {
		t.Logf("Create simple view error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT * FROM view_simple WHERE val > 30")
	t.Logf("Simple view returned %d rows", len(result.Rows))

	// Query view with aggregate
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM view_simple")
	t.Logf("View count returned %v", result.Rows)
}

// TestCoverage_InsertLockedComplex tests insertLocked with complex scenarios
func TestCoverage_InsertLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Table with multiple constraints
	createCoverageTestTable(t, cat, "ins_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText, Unique: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert with transaction
	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_complex",
		Columns: []string{"id", "code", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("CODE1"), numReal(100)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_complex",
		Columns: []string{"id", "code", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("CODE2"), numReal(200)}},
	}, nil)

	cat.CommitTransaction()

	// Insert more without transaction
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_complex",
		Columns: []string{"id", "code", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("CODE3"), numReal(300)}},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_complex")
	t.Logf("Count after inserts: %v", result.Rows)

	// Test bulk insert
	var values [][]query.Expression
	for i := 4; i <= 10; i++ {
		values = append(values, []query.Expression{numReal(float64(i)), strReal("BULK" + string(rune('A'+i-4))), numReal(float64(i * 100))})
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_complex",
		Columns: []string{"id", "code", "val"},
		Values:  values,
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM ins_complex")
	t.Logf("Count after bulk insert: %v", result.Rows)
}

// TestCoverage_RollbackToSavepointComplex tests RollbackToSavepoint with complex scenarios
func TestCoverage_RollbackToSavepointComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_complex",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	cat.Savepoint("sp1")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_complex",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("after_sp1")}},
	}, nil)

	cat.Savepoint("sp2")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_complex",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("after_sp2")}},
	}, nil)

	cat.Savepoint("sp3")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_complex",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(4), strReal("after_sp3")}},
	}, nil)

	// Rollback to sp2 - should lose rows 3 and 4
	err := cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("RollbackToSavepoint sp2 error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_complex")
	t.Logf("Count after rollback to sp2: %v", result.Rows)

	// Rollback to sp1 - should lose row 2
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("RollbackToSavepoint sp1 error: %v", err)
	}

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM sp_complex")
	t.Logf("Count after rollback to sp1: %v", result.Rows)

	cat.CommitTransaction()
}
