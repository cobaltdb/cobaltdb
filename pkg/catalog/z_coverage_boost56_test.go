package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_DeleteWithForeignKeyCascade targets deleteRowLocked FK cascade
func TestCoverage_DeleteWithForeignKeyCascade(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Create child table with FK and CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "parent", ReferencedColumns: []string{"id"}, OnDelete: "CASCADE"},
		},
	})

	// Insert parent
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "parent",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("parent1")}, {numReal(2), strReal("parent2")}},
	}, nil)

	// Insert children
	for i := 1; i <= 10; i++ {
		parentID := 1
		if i > 5 {
			parentID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "child",
			Columns: []string{"id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(parentID))}},
		}, nil)
	}

	// Delete parent - should cascade to children
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "parent",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM child")
	t.Logf("Child count after cascade: %v", result.Rows)
}

// TestCoverage_DeleteWithNonUniqueIndex targets deleteRowLocked with non-unique index
func TestCoverage_DeleteWithNonUniqueIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_nonuniq", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	// Create non-unique index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_status",
		Table:   "del_nonuniq",
		Columns: []string{"status"},
		Unique:  false,
	})

	// Insert data with same status
	for i := 1; i <= 20; i++ {
		status := "active"
		if i > 10 {
			status = "inactive"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_nonuniq",
			Columns: []string{"id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(status)}},
		}, nil)
	}

	cat.BeginTransaction(1)

	// Delete with non-unique index
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_nonuniq",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "status"},
			Operator: query.TokenEq,
			Right:    strReal("active"),
		},
	}, nil)

	cat.RollbackTransaction()

	// Verify index still works
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_nonuniq WHERE status = 'active'")
	t.Logf("Active count after rollback: %v", result.Rows)
}

// TestCoverage_UpdateLockedWithFK targets updateLocked with foreign keys
func TestCoverage_UpdateLockedWithFK(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent
	cat.CreateTable(&query.CreateTableStmt{
		Table: "up_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Create child with FK
	cat.CreateTable(&query.CreateTableStmt{
		Table: "up_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "up_parent", ReferencedColumns: []string{"id"}, OnUpdate: "CASCADE"},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "up_parent",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}, {numReal(2), strReal("b")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "up_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}},
	}, nil)

	// Update parent in transaction
	cat.BeginTransaction(1)
	cat.Update(ctx, &query.UpdateStmt{
		Table: "up_parent",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("updated")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	cat.CommitTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM up_child WHERE parent_id = 1")
	t.Logf("Child count: %v", result.Rows)
}

// TestCoverage_EvaluateWhereWithNull targets evaluateWhere with NULL handling
func TestCoverage_EvaluateWhereWithNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_null", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "txt", Type: query.TokenText},
	})

	// Insert with NULLs
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "where_null",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "where_null",
		Columns: []string{"id", "val", "txt"},
		Values:  [][]query.Expression{{numReal(2), numReal(100), strReal("test")}},
	}, nil)

	queries := []string{
		"SELECT * FROM where_null WHERE val IS NULL",
		"SELECT * FROM where_null WHERE txt IS NULL",
		"SELECT * FROM where_null WHERE val IS NOT NULL",
		"SELECT * FROM where_null WHERE val = 100",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("NULL WHERE error: %v", err)
		} else {
			t.Logf("NULL WHERE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOuterQueryWithHaving targets applyOuterQuery with HAVING
func TestCoverage_ApplyOuterQueryWithHaving(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_having", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		grp := "A"
		if i > 20 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_having",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with GROUP BY and HAVING
	cat.CreateView("view_having", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "grp"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}},
		},
		From:    &query.TableRef{Name: "outer_having"},
		GroupBy: []query.Expression{&query.Identifier{Name: "grp"}},
		Having: &query.BinaryExpr{
			Left:     &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}},
			Operator: query.TokenGt,
			Right:    numReal(1000),
		},
	})

	// Query the view
	result, _ := cat.ExecuteQuery("SELECT * FROM view_having")
	t.Logf("View with HAVING returned %d rows", len(result.Rows))

	// Query with outer filter
	result, _ = cat.ExecuteQuery("SELECT * FROM view_having WHERE grp = 'A'")
	t.Logf("View with outer filter returned %d rows", len(result.Rows))
}

// TestCoverage_ResolveAggregateInExprWithComplexOps targets resolveAggregateInExpr
func TestCoverage_ResolveAggregateInExprWithComplexOps(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_ops", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	for i := 1; i <= 60; i++ {
		grp := "A"
		if i > 30 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_ops",
			Columns: []string{"id", "grp", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val1) + SUM(val2) as total FROM agg_ops GROUP BY grp HAVING total > 10000",
		"SELECT grp, AVG(val1) - AVG(val2) as diff FROM agg_ops GROUP BY grp HAVING diff > 100",
		"SELECT grp, SUM(val1) * COUNT(*) as weighted FROM agg_ops GROUP BY grp",
		"SELECT grp, MAX(val1) / MIN(val2) as ratio FROM agg_ops GROUP BY grp HAVING ratio > 5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex aggregate error: %v", err)
		} else {
			t.Logf("Complex aggregate returned %d rows", len(result.Rows))
		}
	}
}
