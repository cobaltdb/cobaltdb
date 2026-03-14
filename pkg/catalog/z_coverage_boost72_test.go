package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ApplyOrderByText targets applyOrderBy with text columns
func TestCoverage_ApplyOrderByText(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_text", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	names := []string{"Charlie", "Alice", "Bob", "Diana", "Eve"}
	for i, name := range names {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_text",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(name)}},
		}, nil)
	}

	// ORDER BY text
	queries := []string{
		"SELECT * FROM order_text ORDER BY name ASC",
		"SELECT * FROM order_text ORDER BY name DESC",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("ORDER BY text error: %v", err)
		} else {
			t.Logf("ORDER BY text returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOrderByReal targets applyOrderBy with real columns
func TestCoverage_ApplyOrderByReal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_real", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenReal},
	})

	vals := []float64{3.14, 1.41, 2.71, 0.57, 1.61}
	for i, v := range vals {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_real",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), numReal(v)}},
		}, nil)
	}

	// ORDER BY real
	result, err := cat.ExecuteQuery("SELECT * FROM order_real ORDER BY val")
	if err != nil {
		t.Logf("ORDER BY real error: %v", err)
	} else {
		t.Logf("ORDER BY real returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ApplyOuterQueryWithWhere targets applyOuterQuery with WHERE
func TestCoverage_ApplyOuterQueryWithWhere(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_where", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_where",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "outer_where"},
	}
	cat.CreateView("view_outer", viewStmt)

	// Query view with WHERE
	queries := []string{
		"SELECT * FROM view_outer WHERE val > 50",
		"SELECT * FROM view_outer WHERE id BETWEEN 10 AND 30",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("View WHERE error: %v", err)
		} else {
			t.Logf("View WHERE returned %d rows", len(result.Rows))
		}
	}

	cat.DropView("view_outer")
}

// TestCoverage_DeleteRowLockedFKRestrict targets deleteRowLocked with FK RESTRICT
func TestCoverage_DeleteRowLockedFKRestrict(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_rest_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Create child with RESTRICT
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_rest_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_rest_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "RESTRICT",
			},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_rest_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_rest_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// Try to delete parent (should fail due to RESTRICT)
	_, _, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_rest_parent",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err != nil {
		t.Logf("Delete with RESTRICT error (expected): %v", err)
	}

	// Delete parent without child (should succeed)
	_, rows, _ := cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_rest_parent",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(2),
		},
	}, nil)
	t.Logf("Deleted %d rows", rows)
}

// TestCoverage_EvaluateWhereInList targets evaluateWhere with IN operator list
func TestCoverage_EvaluateWhereInList(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_in_list", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		code := "A"
		if i%2 == 0 {
			code = "B"
		}
		if i%3 == 0 {
			code = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_in_list",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code)}},
		}, nil)
	}

	// IN operator
	queries := []string{
		"SELECT * FROM where_in_list WHERE code IN ('A', 'B')",
		"SELECT * FROM where_in_list WHERE code NOT IN ('C')",
		"SELECT * FROM where_in_list WHERE id IN (1, 5, 10, 15)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("IN operator error: %v", err)
		} else {
			t.Logf("IN operator returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_EvaluateWhereBetweenValues targets evaluateWhere with BETWEEN values
func TestCoverage_EvaluateWhereBetweenValues(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_between_val", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_between_val",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// BETWEEN operator
	queries := []string{
		"SELECT * FROM where_between_val WHERE val BETWEEN 100 AND 500",
		"SELECT * FROM where_between_val WHERE val NOT BETWEEN 300 AND 700",
		"SELECT * FROM where_between_val WHERE id BETWEEN 10 AND 20 AND val > 100",
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

// TestCoverage_AlterTableDropColumnComplex targets AlterTableDropColumn with data
func TestCoverage_AlterTableDropColumnComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "alter_drop",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "keep1", Type: query.TokenText},
			{Name: "drop1", Type: query.TokenInteger},
			{Name: "keep2", Type: query.TokenText},
			{Name: "drop2", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "alter_drop",
			Columns: []string{"id", "keep1", "drop1", "keep2", "drop2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("k1"), numReal(float64(i * 10)), strReal("k2"), numReal(float64(i * 100))}},
		}, nil)
	}

	// Drop first column
	err := cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "alter_drop",
		Action:  "DROP",
		OldName: "drop1",
	})
	if err != nil {
		t.Logf("Drop column 1 error: %v", err)
	}

	// Drop second column
	err = cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "alter_drop",
		Action:  "DROP",
		OldName: "drop2",
	})
	if err != nil {
		t.Logf("Drop column 2 error: %v", err)
	}

	// Verify
	result, _ := cat.ExecuteQuery("SELECT * FROM alter_drop")
	t.Logf("Columns after drops: %d", len(result.Columns))
	t.Logf("Row count: %d", len(result.Rows))
}

// TestCoverage_CommitTransactionWithDDL targets CommitTransaction with DDL
func TestCoverage_CommitTransactionWithDDL(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_ddl_commit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.BeginTransaction(1)

	// DDL within transaction
	cat.CreateTable(&query.CreateTableStmt{
		Table: "txn_created",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_created",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Commit
	err := cat.CommitTransaction()
	if err != nil {
		t.Logf("Commit error: %v", err)
	}

	// Verify
	has := cat.HasTableOrView("txn_created")
	t.Logf("Has created table: %v", has)
}

// TestCoverage_RollbackToSavepointNested targets nested savepoint rollbacks
func TestCoverage_RollbackToSavepointNested(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_nested", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.BeginTransaction(1)

	// Create nested savepoints
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_nested",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	cat.Savepoint("sp1")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_nested",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}},
	}, nil)

	cat.Savepoint("sp2")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_nested",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(3)}},
	}, nil)

	// Rollback to sp1 (should remove rows 2 and 3)
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_nested")
	t.Logf("Count after rollback to sp1: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_SelectLockedWithView targets selectLocked with view
func TestCoverage_SelectLockedWithView(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_view_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_view_base",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "sel_view_base"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLte,
			Right:    numReal(10),
		},
	}
	cat.CreateView("sel_view", viewStmt)

	// Query view
	result, err := cat.ExecuteQuery("SELECT * FROM sel_view WHERE val > 50")
	if err != nil {
		t.Logf("View query error: %v", err)
	} else {
		t.Logf("View query returned %d rows", len(result.Rows))
	}

	cat.DropView("sel_view")
}
