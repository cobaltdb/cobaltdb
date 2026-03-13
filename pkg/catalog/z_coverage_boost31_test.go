package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointMore tests more RollbackToSavepoint scenarios
func TestCoverage_RollbackToSavepointMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)

	// Try to rollback to non-existent savepoint without transaction
	err := cat.RollbackToSavepoint("nonexistent")
	if err != nil {
		t.Logf("RollbackToSavepoint without txn error: %v", err)
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_more",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	cat.Savepoint("sp1")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_more",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("sp1")}},
	}, nil)

	// Try to rollback to non-existent savepoint
	err = cat.RollbackToSavepoint("nonexistent")
	if err != nil {
		t.Logf("RollbackToSavepoint nonexistent error: %v", err)
	}

	// Rollback to existing savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("RollbackToSavepoint sp1 error: %v", err)
	}

	cat.CommitTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_more")
	t.Logf("Count after rollback: %v", result.Rows)
}

// TestCoverage_UpdateLockedEdgeCases tests updateLocked edge cases
func TestCoverage_UpdateLockedEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_edge",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Update with complex where
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_edge",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    numReal(2),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_edge WHERE val = 999")
	t.Logf("Count after update: %v", result.Rows)

	// Update with expression
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_edge",
		Set:   []*query.SetClause{{Column: "val", Value: &query.BinaryExpr{Left: &query.Identifier{Name: "val"}, Operator: query.TokenPlus, Right: numReal(1)}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT val FROM upd_edge WHERE id = 1")
	t.Logf("Val after expression update: %v", result.Rows)
}

// TestCoverage_DeleteRowLockedEdgeCases tests deleteRowLocked edge cases
func TestCoverage_DeleteRowLockedEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_edge",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Delete in transaction
	cat.BeginTransaction(1)

	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_edge",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(3),
		},
	}, nil)

	cat.RollbackTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_edge")
	t.Logf("Count after rollback: %v", result.Rows)

	// Delete with OR condition
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_edge",
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
				Right:    numReal(2),
			},
		},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM del_edge")
	t.Logf("Count after OR delete: %v", result.Rows)
}

// TestCoverage_InsertLockedMore tests insertLocked with more scenarios
func TestCoverage_InsertLockedMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert in transaction
	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_more",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("txn")}},
	}, nil)

	cat.CommitTransaction()

	// Insert with subquery (if supported)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_more",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("direct")}},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_more")
	t.Logf("Count after inserts: %v", result.Rows)
}

// TestCoverage_SelectWithJoinAndGroupBy tests executeSelectWithJoinAndGroupBy
func TestCoverage_SelectWithJoinAndGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jg_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "cat_id", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jg_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_b",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_a",
		Columns: []string{"id", "name", "cat_id"},
		Values: [][]query.Expression{
			{numReal(1), strReal("item1"), numReal(1)},
			{numReal(2), strReal("item2"), numReal(1)},
			{numReal(3), strReal("item3"), numReal(2)},
			{numReal(4), strReal("item4"), numReal(2)},
		},
	}, nil)

	queries := []string{
		"SELECT b.category, COUNT(*) as cnt FROM jg_a a JOIN jg_b b ON a.cat_id = b.id GROUP BY b.category",
		"SELECT b.category, SUM(a.id) as total FROM jg_a a JOIN jg_b b ON a.cat_id = b.id GROUP BY b.category HAVING total > 3",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN+GROUP BY error: %v", err)
		} else {
			t.Logf("JOIN+GROUP BY returned %d rows", len(result.Rows))
		}
	}
}
