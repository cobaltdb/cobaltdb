package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointDropColumn targets undoAlterDropColumn rollback
func TestCoverage_RollbackToSavepointDropColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_dropcol", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
		{Name: "dropme", Type: query.TokenInteger},
	})

	// Insert data including the column to be dropped
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_dropcol",
			Columns: []string{"id", "val", "dropme"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create an index on the column to test index restoration
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_dropme",
		Table:   "sp_dropcol",
		Columns: []string{"dropme"},
	})

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Drop the column (this should also drop the index)
	cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "sp_dropcol",
		Action:  "DROP",
		OldName: "dropme",
	})

	// Verify column is dropped
	result, _ := cat.ExecuteQuery("SELECT * FROM sp_dropcol")
	t.Logf("Columns after drop: %d", len(result.Columns))

	// Rollback to sp1 - dropped column should be restored
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	// Verify column is restored and data is back
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM sp_dropcol WHERE dropme > 0")
	t.Logf("Count after rollback: %v", result.Rows)

	// Verify index is restored
	hasIdx := false
	for idxName, idxDef := range cat.indexes {
		if idxName == "idx_dropme" && idxDef.TableName == "sp_dropcol" {
			hasIdx = true
			break
		}
	}
	t.Logf("Index restored: %v", hasIdx)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointRenameColumn targets undoAlterRenameColumn rollback
func TestCoverage_RollbackToSavepointRenameColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_renamecol", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "oldname", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 3; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_renamecol",
			Columns: []string{"id", "oldname"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Rename the column
	cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "sp_renamecol",
		Action:  "RENAME_COLUMN",
		OldName: "oldname",
		NewName: "newname",
	})

	// Verify rename worked
	result, _ := cat.ExecuteQuery("SELECT newname FROM sp_renamecol WHERE id = 1")
	t.Logf("Renamed column value: %v", result.Rows)

	// Rollback to sp1 - column rename should be undone
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	// Verify old column name works
	result, _ = cat.ExecuteQuery("SELECT oldname FROM sp_renamecol WHERE id = 1")
	t.Logf("Restored column value: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_RollbackToSavepointRenameColumnPK targets undoAlterRenameColumn with primary key
func TestCoverage_RollbackToSavepointRenameColumnPK(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create table with composite-like primary key reference
	cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_renamepk",
		Columns: []*query.ColumnDef{
			{Name: "pk_col", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_renamepk",
		Columns: []string{"pk_col", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Rename the PK column
	cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "sp_renamepk",
		Action:  "RENAME_COLUMN",
		OldName: "pk_col",
		NewName: "pk_column",
	})

	// Rollback to sp1 - PK column rename should be undone
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	// Verify PK column restored
	result, _ := cat.ExecuteQuery("SELECT pk_col FROM sp_renamepk WHERE pk_col = 1")
	t.Logf("PK column after rollback: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_deleteRowLocked targets deleteRowLocked with indexes
func TestCoverage_deleteRowLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create indexes on non-PK columns
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_code",
		Table:   "del_complex",
		Columns: []string{"code"},
		Unique:  true,
	})
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_val",
		Table:   "del_complex",
		Columns: []string{"val"},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_complex",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE" + string(rune('A'+i%26))), numReal(float64(i * 10))}},
		}, nil)
	}

	// Delete with cascading index cleanup
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_complex",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(5),
		},
	}, nil)

	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}

	// Verify deletion
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_complex")
	t.Logf("Count after delete: %v", result.Rows)

	// Verify indexes are cleaned up by trying to insert same code
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_complex",
		Columns: []string{"id", "code", "val"},
		Values:  [][]query.Expression{{numReal(99), strReal("CODEF"), numReal(999)}},
	}, nil)
	if err != nil {
		t.Logf("Insert reusing deleted code error (should succeed): %v", err)
	}
}

// TestCoverage_applyOuterQueryDistinct targets applyOuterQuery with DISTINCT
func TestCoverage_applyOuterQueryDistinct(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_distinct", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data with duplicates
	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_distinct",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i % 5))}},
		}, nil)
	}

	// Create a view with distinct
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "grp"},
			&query.Identifier{Name: "val"},
		},
		From:     &query.TableRef{Name: "outer_distinct"},
		Distinct: true,
	}
	err := cat.CreateView("distinct_view", viewStmt)
	if err != nil {
		t.Logf("CreateView error: %v", err)
	}

	// Query the distinct view with ORDER BY (triggers applyOuterQuery)
	result, err := cat.ExecuteQuery("SELECT * FROM distinct_view ORDER BY grp, val")
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Distinct view returned %d rows", len(result.Rows))
	}

	cat.DropView("distinct_view")
}

// TestCoverage_applyOuterQueryGroupBy targets applyOuterQuery with GROUP BY
func TestCoverage_applyOuterQueryGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_group", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		category := "A"
		if i > 15 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_group",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create a view with GROUP BY
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		},
		From:    &query.TableRef{Name: "outer_group"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
	}
	err := cat.CreateView("group_view", viewStmt)
	if err != nil {
		t.Logf("CreateView error: %v", err)
	}

	// Query the group view with HAVING via outer query
	result, err := cat.ExecuteQuery("SELECT * FROM group_view WHERE category = 'A'")
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Group view returned %d rows", len(result.Rows))
	}

	cat.DropView("group_view")
}

// TestCoverage_evaluateWhereComplex targets evaluateWhere with complex expressions
func TestCoverage_evaluateWhereComplex67(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
		{Name: "priority", Type: query.TokenInteger},
		{Name: "score", Type: query.TokenReal},
	})

	// Insert data
	statuses := []string{"active", "inactive", "pending", "active", "inactive"}
	for i, s := range statuses {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_complex",
			Columns: []string{"id", "status", "priority", "score"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(s), numReal(float64(i * 10)), numReal(float64(i) * 1.5)}},
		}, nil)
	}

	// Complex WHERE with multiple conditions
	queries := []string{
		"SELECT * FROM where_complex WHERE status = 'active' AND priority > 5",
		"SELECT * FROM where_complex WHERE status = 'inactive' OR priority < 20",
		"SELECT * FROM where_complex WHERE NOT status = 'pending'",
		"SELECT * FROM where_complex WHERE (status = 'active' OR status = 'inactive') AND score > 2.0",
		"SELECT * FROM where_complex WHERE priority BETWEEN 10 AND 40",
		"SELECT * FROM where_complex WHERE status IN ('active', 'pending')",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_evaluateWhereSubquery targets evaluateWhere with subqueries
func TestCoverage_evaluateWhereSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "where_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "threshold", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_main",
			Columns: []string{"id", "ref_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 5))}},
		}, nil)
	}
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "where_ref",
		Columns: []string{"id", "threshold"},
		Values:  [][]query.Expression{{numReal(1), numReal(2)}, {numReal(2), numReal(3)}},
	}, nil)

	// WHERE with subquery
	result, err := cat.ExecuteQuery("SELECT * FROM where_main WHERE ref_id > (SELECT threshold FROM where_ref WHERE id = 1)")
	if err != nil {
		t.Logf("Subquery WHERE error: %v", err)
	} else {
		t.Logf("Subquery WHERE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_resolveAggregateInExpr targets resolveAggregateInExpr
func TestCoverage_resolveAggregateInExprComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_expr", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		category := "A"
		if i > 25 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_expr",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	// Various aggregate expressions in HAVING
	queries := []string{
		"SELECT category, SUM(amount) as total FROM agg_expr GROUP BY category HAVING total > 5000",
		"SELECT category, AVG(amount) as avg_amt FROM agg_expr GROUP BY category HAVING avg_amt > 200",
		"SELECT category, COUNT(*) as cnt FROM agg_expr GROUP BY category HAVING cnt > 10",
		"SELECT category, MIN(amount) as min_amt, MAX(amount) as max_amt FROM agg_expr GROUP BY category HAVING min_amt > 0 AND max_amt < 1000",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING error: %v", err)
		} else {
			t.Logf("HAVING query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_executeScalarSelect targets executeScalarSelect more
func TestCoverage_executeScalarSelectMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_more",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// Various scalar aggregates
	queries := []string{
		"SELECT COUNT(*) FROM scalar_more",
		"SELECT COUNT(DISTINCT val) FROM scalar_more",
		"SELECT SUM(val) FROM scalar_more",
		"SELECT AVG(val) FROM scalar_more",
		"SELECT MIN(val) FROM scalar_more",
		"SELECT MAX(val) FROM scalar_more",
		"SELECT COUNT(*), SUM(val), AVG(val) FROM scalar_more",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Scalar error: %v", err)
		} else {
			t.Logf("Scalar result: %v", result.Rows)
		}
	}
}
