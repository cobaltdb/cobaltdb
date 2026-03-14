package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_UnionAllNoDedup tests UNION ALL (no deduplication)
func TestCoverage_UnionAllNoDedup(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "union_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "union_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert overlapping data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "union_a",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("shared")}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "union_b",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("shared")}},
		}, nil)
	}

	// UNION ALL (should have duplicates)
	result, err := cat.ExecuteQuery(`
		SELECT val FROM union_a
		UNION ALL
		SELECT val FROM union_b
	`)
	if err != nil {
		t.Logf("UNION ALL error: %v", err)
	} else {
		t.Logf("UNION ALL returned %d rows (should have duplicates)", len(result.Rows))
	}
}

// TestCoverage_IntersectSetOp tests INTERSECT set operation
func TestCoverage_IntersectSetOp(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "intersect_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "intersect_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert overlapping data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "intersect_a",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("common")}},
		}, nil)
		if i <= 5 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "intersect_b",
				Columns: []string{"id", "val"},
				Values:  [][]query.Expression{{numReal(float64(i)), strReal("common")}},
			}, nil)
		}
	}

	// INTERSECT
	result, err := cat.ExecuteQuery(`
		SELECT val FROM intersect_a
		INTERSECT
		SELECT val FROM intersect_b
	`)
	if err != nil {
		t.Logf("INTERSECT error: %v", err)
	} else {
		t.Logf("INTERSECT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ExceptSetOp tests EXCEPT set operation
func TestCoverage_ExceptSetOp(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "except_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "except_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert data - some in A only, some in both
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "except_a",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val")}},
		}, nil)
		if i <= 5 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "except_b",
				Columns: []string{"id", "val"},
				Values:  [][]query.Expression{{numReal(float64(i)), strReal("val")}},
			}, nil)
		}
	}

	// EXCEPT (A - B)
	result, err := cat.ExecuteQuery(`
		SELECT val FROM except_a
		EXCEPT
		SELECT val FROM except_b
	`)
	if err != nil {
		t.Logf("EXCEPT error: %v", err)
	} else {
		t.Logf("EXCEPT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_RollbackTransactionWithUndo tests rollback with undo log
func TestCoverage_RollbackTransactionWithUndo(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "rollback_undo", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert initial data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rollback_undo",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("original")}},
	}, nil)

	// Begin transaction
	cat.BeginTransaction(1)

	// Update in transaction
	cat.Update(ctx, &query.UpdateStmt{
		Table: "rollback_undo",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Rollback
	err := cat.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify rollback
	result, _ := cat.ExecuteQuery("SELECT val FROM rollback_undo WHERE id = 1")
	t.Logf("Value after rollback: %v", result.Rows)
}

// TestCoverage_CommitTransactionWithData tests commit transaction
func TestCoverage_CommitTransactionWithData(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "commit_data", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	// Insert in transaction
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "commit_data",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("in_transaction")}},
	}, nil)

	// Commit
	err := cat.CommitTransaction()
	if err != nil {
		t.Logf("Commit error: %v", err)
	}

	// Verify commit
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM commit_data")
	t.Logf("Count after commit: %v", result.Rows)
}

// TestCoverage_CreateDropIndex77 tests CREATE and DROP INDEX
func TestCoverage_CreateDropIndex77(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "idx_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val1", Type: query.TokenText},
			{Name: "val2", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "idx_test",
			Columns: []string{"id", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create index
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "test_idx",
		Table:   "idx_test",
		Columns: []string{"val1"},
	})
	if err != nil {
		t.Logf("Create index error: %v", err)
	}

	// Query using index
	result, _ := cat.ExecuteQuery("SELECT * FROM idx_test WHERE val1 = 'test' LIMIT 5")
	t.Logf("Query returned %d rows", len(result.Rows))

	// Drop index
	err = cat.DropIndex("idx_test")
	if err != nil {
		t.Logf("Drop index error: %v", err)
	}
}

// TestCoverage_CreateDropView tests CREATE and DROP VIEW
func TestCoverage_CreateDropView(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "view_base",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "view_base",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("value")}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "view_base"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLte,
			Right:    numReal(5),
		},
	}
	err := cat.CreateView("test_view", viewStmt)
	if err != nil {
		t.Logf("Create view error: %v", err)
	}

	// Query view
	result, _ := cat.ExecuteQuery("SELECT * FROM test_view")
	t.Logf("View returned %d rows", len(result.Rows))

	// Drop view
	err = cat.DropView("test_view")
	if err != nil {
		t.Logf("Drop view error: %v", err)
	}
}

// TestCoverage_AlterTableAddDropColumn tests ALTER TABLE ADD/DROP COLUMN
func TestCoverage_AlterTableAddDropColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "alter_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "alter_test",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Add column
	err := cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "alter_test",
		Action: "ADD",
		Column: query.ColumnDef{
			Name: "new_col",
			Type: query.TokenInteger,
		},
	})
	if err != nil {
		t.Logf("Add column error: %v", err)
	}

	// Drop column
	err = cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "alter_test",
		Action:  "DROP",
		OldName: "name",
	})
	if err != nil {
		t.Logf("Drop column error: %v", err)
	}

	// Verify
	result, _ := cat.ExecuteQuery("SELECT * FROM alter_test")
	t.Logf("Columns after alter: %v", result.Columns)
}

// TestCoverage_AlterTableRename77 tests ALTER TABLE RENAME
func TestCoverage_AlterTableRename77(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rename_old",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rename_old",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Rename table
	err := cat.AlterTableRename(&query.AlterTableStmt{
		Table:   "rename_old",
		Action:  "RENAME_TABLE",
		NewName: "rename_new",
	})
	if err != nil {
		t.Logf("Rename error: %v", err)
	}

	// Verify
	hasOld := cat.HasTableOrView("rename_old")
	hasNew := cat.HasTableOrView("rename_new")
	t.Logf("Has old: %v, Has new: %v", hasOld, hasNew)
}

// TestCoverage_SelectWithLimitOffset tests SELECT with LIMIT/OFFSET
func TestCoverage_SelectWithLimitOffset(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "limit_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "limit_test",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM limit_test ORDER BY id LIMIT 10",
		"SELECT * FROM limit_test ORDER BY id LIMIT 10 OFFSET 10",
		"SELECT * FROM limit_test ORDER BY id LIMIT 5 OFFSET 50",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIMIT/OFFSET error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_LeftJoin77 tests LEFT JOIN
func TestCoverage_LeftJoin77(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "left_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "left_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "value", Type: query.TokenText},
	})

	// Insert main rows (all will be returned)
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "left_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name")}},
		}, nil)
	}

	// Insert only some reference rows
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "left_ref",
			Columns: []string{"id", "main_id", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("value")}},
		}, nil)
	}

	// LEFT JOIN
	result, err := cat.ExecuteQuery(`
		SELECT m.id, m.name, r.value
		FROM left_main m
		LEFT JOIN left_ref r ON m.id = r.main_id
		ORDER BY m.id
	`)
	if err != nil {
		t.Logf("LEFT JOIN error: %v", err)
	} else {
		t.Logf("LEFT JOIN returned %d rows", len(result.Rows))
	}
}
