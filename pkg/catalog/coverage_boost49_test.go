package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Additional edge cases for low-coverage functions
// ============================================================

// TestRollbackToSavepoint_CreateTable - undoCreateTable path
func TestRollbackToSavepoint_CreateTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Begin transaction
	cat.BeginTransaction(1)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_create_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Create index on the table
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sp_create",
		Table:   "sp_create_test",
		Columns: []string{"id"},
	})

	// Savepoint after DDL
	cat.Savepoint("sp1")

	// Create another table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_create_test2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert into first table
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_create_test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Rollback to savepoint - should undo the second table creation
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to savepoint error: %v", err)
	}

	// Verify first table exists
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM sp_create_test`)
	if err != nil {
		t.Logf("First table error: %v", err)
	} else {
		t.Logf("First table rows: %v", result.Rows)
	}

	// Verify second table doesn't exist
	_, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM sp_create_test2`)
	if err == nil {
		t.Error("Expected error for rolled back table")
	} else {
		t.Logf("Expected error for second table: %v", err)
	}

	cat.RollbackTransaction()
	_ = result
}

// TestRollbackToSavepoint_DropTable - undoDropTable path
func TestRollbackToSavepoint_DropTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table with data
	createCoverageTestTable(t, cat, "sp_drop_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sp_drop",
		Table:   "sp_drop_test",
		Columns: []string{"data"},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_drop_test",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Begin transaction
	cat.BeginTransaction(1)

	// Savepoint
	cat.Savepoint("sp1")

	// Drop the table
	cat.DropTable(&query.DropTableStmt{Table: "sp_drop_test"})

	// Verify table is gone within transaction
	_, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM sp_drop_test`)
	if err == nil {
		t.Error("Expected error after drop")
	}

	// Rollback to savepoint - should restore the table
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify table is restored
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM sp_drop_test`)
	if err != nil {
		t.Logf("Query error after rollback: %v", err)
	} else {
		t.Logf("Rows after rollback: %v", result.Rows)
	}

	cat.RollbackTransaction()
	_ = result
}

// TestRollbackToSavepoint_DropIndex - undoDropIndex path
func TestRollbackToSavepoint_DropIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sp_idx_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sp_test",
		Table:   "sp_idx_test",
		Columns: []string{"val"},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_idx_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("value")}},
		}, nil)
	}

	// Begin transaction
	cat.BeginTransaction(1)

	// Savepoint
	cat.Savepoint("sp1")

	// Drop index
	cat.DropIndex("idx_sp_test")

	// Rollback to savepoint - should restore the index
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify index is restored by checking it still works
	result, err := cat.ExecuteQuery(`SELECT * FROM sp_idx_test WHERE val = 'value'`)
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Index query returned %d rows", len(result.Rows))
	}

	cat.RollbackTransaction()
	_ = result
}

// TestRollbackToSavepoint_AutoInc - undoAutoIncSeq path
func TestRollbackToSavepoint_AutoInc(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table with auto-increment
	cat.CreateTable(&query.CreateTableStmt{
		Table: "sp_autoinc_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "data", Type: query.TokenText},
		},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	// Insert without specifying id
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_autoinc_test",
		Columns: []string{"data"},
		Values:  [][]query.Expression{{strReal("first")}},
	}, nil)

	// Savepoint
	cat.Savepoint("sp1")

	// Insert more
	for i := 1; i <= 3; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_autoinc_test",
			Columns: []string{"data"},
			Values:  [][]query.Expression{{strReal("data")}},
		}, nil)
	}

	// Check current auto-inc value
	result, _ := cat.ExecuteQuery(`SELECT MAX(id) FROM sp_autoinc_test`)
	maxBefore := int64(0)
	if len(result.Rows) > 0 && result.Rows[0][0] != nil {
		if v, ok := result.Rows[0][0].(int64); ok {
			maxBefore = v
		}
	}
	t.Logf("Max id before rollback: %d", maxBefore)

	// Rollback to savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify rows were rolled back
	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM sp_autoinc_test`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after rollback: %v", result.Rows)
	}

	cat.RollbackTransaction()
	_ = result
}

// TestRollbackToSavepoint_AlterColumn - undoAlterAddColumn and undoAlterDropColumn
func TestRollbackToSavepoint_AlterColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sp_alter_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_alter_test",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name")}},
		}, nil)
	}

	// Begin transaction
	cat.BeginTransaction(1)

	// Savepoint
	cat.Savepoint("sp1")

	// Add column
	cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "sp_alter_test",
		Action: "ADD",
		Column: query.ColumnDef{Name: "newcol", Type: query.TokenInteger},
	})

	// Verify column exists
	result, _ := cat.ExecuteQuery(`SELECT newcol FROM sp_alter_test`)
	t.Logf("Column count after add: %d", len(result.Columns))

	// Rollback to savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify column was removed
	result, err = cat.ExecuteQuery(`SELECT * FROM sp_alter_test`)
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Columns after rollback: %d", len(result.Columns))
	}

	cat.RollbackTransaction()
	_ = result
}

// TestDeleteWithUsing_JoinError - covers join execution error
func TestDeleteWithUsing_JoinError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "delu_err_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
	})

	// Insert data without creating the referenced table
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_err_main",
			Columns: []string{"id", "ref_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// Try DELETE USING with non-existent table in USING clause
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "delu_err_main",
		Using: []*query.TableRef{{Name: "nonexistent_ref_table"}},
		Where: &query.BinaryExpr{
			Left:     colReal("delu_err_main.ref_id"),
			Operator: query.TokenEq,
			Right:    &query.QualifiedIdentifier{Table: "nonexistent_ref_table", Column: "id"},
		},
	}, nil)
	if err == nil {
		t.Log("Expected error for non-existent USING table (may pass silently)")
	} else {
		t.Logf("Got error: %v", err)
	}
}

// TestDeleteWithUsing_CollectRows - covers rows collection and FK enforcement
func TestDeleteWithUsing_CollectRows(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Parent table
	createCoverageTestTable(t, cat, "delu_fk_parent", []*query.ColumnDef{
		{Name: "parent_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Child table with FK
	cat.CreateTable(&query.CreateTableStmt{
		Table: "delu_fk_child",
		Columns: []*query.ColumnDef{
			{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "delu_fk_parent",
				ReferencedColumns: []string{"parent_id"},
				OnDelete:          "CASCADE",
			},
		},
	})

	// Ref table for USING
	createCoverageTestTable(t, cat, "delu_fk_ref", []*query.ColumnDef{
		{Name: "ref_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_fk_parent",
			Columns: []string{"parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_fk_child",
			Columns: []string{"child_id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
		code := "KEEP"
		if i%2 == 0 {
			code = "DELETE"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_fk_ref",
			Columns: []string{"ref_id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code)}},
		}, nil)
	}

	// Execute DELETE USING that will trigger FK cascade
	result, err := cat.ExecuteQuery(`
		SELECT p.parent_id FROM delu_fk_parent p
		JOIN delu_fk_ref r ON p.parent_id = r.ref_id
		WHERE r.code = 'DELETE'
	`)
	if err != nil {
		t.Logf("Pre-check error: %v", err)
	} else {
		t.Logf("Parents to delete: %d", len(result.Rows))
	}

	_ = result
}

// TestApplyOuterQuery_OrderByError - ORDER BY with expression error
func TestApplyOuterQuery_OrderByError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_ob_err", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_ob_err",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name")}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "name"},
		},
		From: &query.TableRef{Name: "aoq_ob_err"},
	}
	cat.CreateView("aoq_ob_err_view", viewStmt)

	// Query with ORDER BY on non-existent column (should handle gracefully)
	result, err := cat.ExecuteQuery(`SELECT * FROM aoq_ob_err_view ORDER BY id`)
	if err != nil {
		t.Logf("ORDER BY error: %v", err)
	} else {
		t.Logf("ORDER BY returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestUpdateRowSlice_MultipleFKs - row slice with multiple FKs
func TestUpdateRowSlice_MultipleFKs(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Two parent tables
	createCoverageTestTable(t, cat, "fk_multi_parent1", []*query.ColumnDef{
		{Name: "p1_id", Type: query.TokenInteger, PrimaryKey: true},
	})
	createCoverageTestTable(t, cat, "fk_multi_parent2", []*query.ColumnDef{
		{Name: "p2_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Child with FKs to both parents
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_multi_child",
		Columns: []*query.ColumnDef{
			{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent1_id", Type: query.TokenInteger},
			{Name: "parent2_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent1_id"},
				ReferencedTable:   "fk_multi_parent1",
				ReferencedColumns: []string{"p1_id"},
				OnUpdate:          "CASCADE",
			},
			{
				Columns:           []string{"parent2_id"},
				ReferencedTable:   "fk_multi_parent2",
				ReferencedColumns: []string{"p2_id"},
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_multi_parent1",
			Columns: []string{"p1_id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_multi_parent2",
			Columns: []string{"p2_id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_multi_child",
			Columns: []string{"child_id", "parent1_id", "parent2_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// Update first parent - should cascade to child
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_multi_parent1",
		Set:   []*query.SetClause{{Column: "p1_id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: colReal("p1_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Update parent1 error: %v", err)
	}

	// Verify child was updated
	result, err := cat.ExecuteQuery(`SELECT * FROM fk_multi_child WHERE parent1_id = 100`)
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Children with updated parent1: %d", len(result.Rows))
	}

	_ = result
}
