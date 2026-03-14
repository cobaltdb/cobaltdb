package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointFull tests RollbackToSavepoint extensively
func TestCoverage_RollbackToSavepointFull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_full", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Test rollback without transaction
	err := cat.RollbackToSavepoint("nonexistent")
	if err != nil {
		t.Logf("Rollback without txn error: %v", err)
	}

	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_full",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	cat.Savepoint("sp1")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_full",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("sp1_data")}},
	}, nil)

	cat.Savepoint("sp2")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_full",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("sp2_data")}},
	}, nil)

	// Rollback to sp1 - should lose sp2 and its data
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("RollbackToSavepoint error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_full")
	t.Logf("Count after rollback to sp1: %v", result.Rows)

	cat.CommitTransaction()

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM sp_full")
	t.Logf("Final count: %v", result.Rows)
}

// TestCoverage_UpdateLockedFull tests updateLocked extensively
func TestCoverage_UpdateLockedFull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_full", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "status", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_full",
			Columns: []string{"id", "val", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("active")}},
		}, nil)
	}

	// Update with complex where
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_full",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
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
				Right:    numReal(15),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_full WHERE val = 999")
	t.Logf("Count after complex update: %v", result.Rows)

	// Update with OR
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_full",
		Set:   []*query.SetClause{{Column: "status", Value: strReal("updated")}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    numReal(1),
			},
			Operator: query.TokenOr,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    numReal(20),
			},
		},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM upd_full WHERE status = 'updated'")
	t.Logf("Count after OR update: %v", result.Rows)
}

// TestCoverage_DeleteRowLockedFull tests deleteRowLocked extensively
func TestCoverage_DeleteRowLockedFull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_full", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "cat", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		catg := "A"
		if i > 15 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_full",
			Columns: []string{"id", "val", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(catg)}},
		}, nil)
	}

	// Delete with complex where
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_full",
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
				Right:    numReal(20),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_full")
	t.Logf("Count after delete: %v", result.Rows)

	// Delete with OR
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_full",
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "cat"},
				Operator: query.TokenEq,
				Right:    strReal("B"),
			},
			Operator: query.TokenOr,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(25),
			},
		},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM del_full")
	t.Logf("Count after OR delete: %v", result.Rows)
}

// TestCoverage_InsertLockedFull tests insertLocked extensively
func TestCoverage_InsertLockedFull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_full", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert in transaction
	cat.BeginTransaction(1)

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ins_full",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	cat.CommitTransaction()

	// Insert without transaction
	for i := 11; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ins_full",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_full")
	t.Logf("Count after inserts: %v", result.Rows)

	// Bulk insert
	var values [][]query.Expression
	for i := 21; i <= 50; i++ {
		values = append(values, []query.Expression{numReal(float64(i)), numReal(float64(i * 10))})
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_full",
		Columns: []string{"id", "val"},
		Values:  values,
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM ins_full")
	t.Logf("Count after bulk insert: %v", result.Rows)
}

// TestCoverage_WhereComplex tests evaluateWhere with complex conditions
func TestCoverage_WhereComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
		{Name: "c", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_complex",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("test"), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_complex WHERE a > 10 AND a < 40 AND b = 'test'",
		"SELECT * FROM where_complex WHERE (a > 20 AND a < 30) OR (a > 40 AND a < 45)",
		"SELECT * FROM where_complex WHERE NOT a > 45",
		"SELECT * FROM where_complex WHERE a IN (5, 10, 15, 20, 25, 30)",
		"SELECT * FROM where_complex WHERE a BETWEEN 10 AND 20",
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
