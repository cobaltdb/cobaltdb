package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointUndoDropTable targets undoDropTable in RollbackToSavepoint
func TestCoverage_RollbackToSavepointUndoDropTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_drop_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_drop_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Drop table
	cat.DropTable(&query.DropTableStmt{Table: "sp_drop_tbl"})

	// Verify table is gone
	_, err := cat.ExecuteQuery("SELECT * FROM sp_drop_tbl")
	if err == nil {
		t.Error("Expected error for dropped table")
	}

	// Rollback to sp1 - table should be restored
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify table is restored
	result, err := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_drop_tbl")
	if err != nil {
		t.Logf("Table restore error: %v", err)
	} else {
		t.Logf("Table restored, count: %v", result.Rows)
	}

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointUndoCreateTable targets undoCreateTable
func TestCoverage_RollbackToSavepointUndoCreateTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Create table within savepoint
	createCoverageTestTable(t, cat, "sp_create_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_create_tbl",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Rollback to sp1 - table creation should be undone
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify table is gone
	_, err = cat.ExecuteQuery("SELECT * FROM sp_create_tbl")
	if err == nil {
		t.Error("Expected error - table should be gone after rollback")
	} else {
		t.Logf("Table correctly removed after rollback")
	}

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointMultiple targets multiple savepoints
func TestCoverage_RollbackToSavepointMultiple(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)

	// Insert first row
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_multi",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("first")}},
	}, nil)

	cat.Savepoint("sp1")

	// Insert second row
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_multi",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("second")}},
	}, nil)

	cat.Savepoint("sp2")

	// Insert third row
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_multi",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("third")}},
	}, nil)

	// Rollback to sp1 - should remove rows 2 and 3
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_multi")
	t.Logf("Count after rollback to sp1: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_ResolveAggregateInExprMore targets resolveAggregateInExpr more cases
func TestCoverage_ResolveAggregateInExprAdvanced(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_resolve", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		grp := "A"
		if i > 20 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_resolve",
			Columns: []string{"id", "grp", "val", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), numReal(float64(i))}},
		}, nil)
	}

	// Complex aggregate expressions in SELECT
	queries := []string{
		"SELECT grp, COUNT(*) as cnt, SUM(val) as s, AVG(val) as a FROM agg_resolve GROUP BY grp HAVING cnt > 5",
		"SELECT grp, MIN(val), MAX(val), MIN(val2), MAX(val2) FROM agg_resolve GROUP BY grp",
		"SELECT grp, SUM(val) + SUM(val2) as combined FROM agg_resolve GROUP BY grp HAVING combined > 1000",
		"SELECT grp, (SUM(val) - AVG(val)) / COUNT(*) as calc FROM agg_resolve GROUP BY grp",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOuterQueryOrderByAsc targets applyOuterQuery with ORDER BY ASC
func TestCoverage_ApplyOuterQueryOrderByAsc(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_asc", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_asc",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(100 - i))}},
		}, nil)
	}

	// Create view with ORDER BY ASC
	cat.CreateView("view_asc", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "score"},
		},
		From: &query.TableRef{Name: "outer_asc"},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "score"}, Desc: false},
		},
	})

	result, _ := cat.ExecuteQuery("SELECT * FROM view_asc LIMIT 5")
	t.Logf("ORDER BY ASC returned %d rows", len(result.Rows))
}

// TestCoverage_EvaluateWhereSubquery targets evaluateWhere with subqueries
func TestCoverage_EvaluateWhereSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_sub_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "where_sub_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_sub_a",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_sub_b",
			Columns: []string{"id", "ref"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Queries with IN subquery
	queries := []string{
		"SELECT * FROM where_sub_a WHERE val IN (SELECT ref FROM where_sub_b)",
		"SELECT * FROM where_sub_a WHERE EXISTS (SELECT 1 FROM where_sub_b WHERE ref = where_sub_a.val)",
		"SELECT * FROM where_sub_a WHERE val > (SELECT MIN(ref) FROM where_sub_b)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Subquery error: %v", err)
		} else {
			t.Logf("Subquery returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_EvaluateWhereBetween targets evaluateWhere with BETWEEN
func TestCoverage_EvaluateWhereBetween(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_between", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_between",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_between WHERE val BETWEEN 50 AND 150",
		"SELECT * FROM where_between WHERE id BETWEEN 10 AND 20",
		"SELECT * FROM where_between WHERE val NOT BETWEEN 100 AND 200",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("BETWEEN error: %v", err)
		} else {
			t.Logf("BETWEEN returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DeleteRowLockedFKCascadeError targets deleteRowLocked FK cascade error
func TestCoverage_DeleteRowLockedFKCascadeError(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent
	cat.CreateTable(&query.CreateTableStmt{
		Table: "cascade_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Create child with cascade
	cat.CreateTable(&query.CreateTableStmt{
		Table: "cascade_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "cascade_parent", ReferencedColumns: []string{"id"}, OnDelete: "CASCADE"},
		},
	})

	// Create grandchild with cascade
	cat.CreateTable(&query.CreateTableStmt{
		Table: "cascade_grandchild",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "child_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"child_id"}, ReferencedTable: "cascade_child", ReferencedColumns: []string{"id"}, OnDelete: "CASCADE"},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cascade_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cascade_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}, {numReal(3), numReal(2)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cascade_grandchild",
		Columns: []string{"id", "child_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(2)}},
	}, nil)

	// Delete parent with cascade - should delete child and grandchild
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "cascade_parent",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM cascade_child")
	t.Logf("Child count after cascade: %v", result.Rows)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM cascade_grandchild")
	t.Logf("Grandchild count after cascade: %v", result.Rows)
}

// TestCoverage_UpdateLockedNoTable targets updateLocked when table doesn't exist
func TestCoverage_UpdateLockedNoTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	_, _, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "nonexistent_update",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("test")}},
	}, nil)

	if err != nil {
		t.Logf("Update non-existent table error (expected): %v", err)
	}
}
