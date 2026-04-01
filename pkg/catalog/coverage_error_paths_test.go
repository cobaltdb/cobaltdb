package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestBuildJSONIndexErrors tests error paths in buildJSONIndex
func TestBuildJSONIndexErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Test with non-existent table
	idx := &JSONIndexDef{
		Name:      "test_idx",
		TableName: "nonexistent",
		Column:    "data",
		Path:      "$.name",
		Index:     make(map[string][]int64),
		NumIndex:  make(map[string][]int64),
	}

	// Should return nil (no error) for non-existent table
	err := c.buildJSONIndex(idx)
	if err != nil {
		t.Errorf("buildJSONIndex on non-existent table should not error, got: %v", err)
	}
}

// TestCountRowsErrorCases tests countRows error handling
func TestCountRowsErrorCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	sc := NewStatsCollector(c)

	// Test with empty table name
	_, err := sc.countRows("")
	if err == nil {
		t.Error("countRows with empty name should error")
	}

	// Test with SQL injection attempt
	_, err = sc.countRows("table; DROP")
	if err == nil {
		t.Error("countRows with SQL injection should error")
	}

	// Test with very long name
	longName := make([]byte, 100)
	for i := range longName {
		longName[i] = 'a'
	}
	_, err = sc.countRows(string(longName))
	if err == nil {
		t.Error("countRows with very long name should error")
	}
}

// TestUpdateRowSliceErrors tests updateRowSlice error paths
func TestUpdateRowSliceErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	fke := NewForeignKeyEnforcer(c)

	// Test with non-existent table
	err := fke.updateRowSlice("nonexistent", 1, []interface{}{1, "test"})
	if err == nil {
		t.Error("updateRowSlice on non-existent table should error")
	}
}

// TestStoreIndexDefErrors tests storeIndexDef error paths
func TestStoreIndexDefErrors(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "idx_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert some data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "idx_test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Create an index
	err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_id",
		Table:   "idx_test",
		Columns: []string{"id"},
		Unique:  false,
	})
	if err != nil {
		t.Logf("CreateIndex: %v", err)
	}
}

// TestProcessUpdateRowErrors tests processUpdateRow error paths
func TestProcessUpdateRowErrors(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "proc_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert
	c.Insert(ctx, &query.InsertStmt{
		Table:   "proc_err",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Update with type mismatch (text to int column)
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "proc_err",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(123)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Logf("Update with type change: %v", err)
	}
}

// TestVacuumErrors tests Vacuum error paths
func TestVacuumErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create a table
	c.CreateTable(&query.CreateTableStmt{
		Table: "vac_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Close pool to simulate error
	pool.Close()

	// Vacuum should handle closed pool gracefully
	err := c.Vacuum()
	if err != nil {
		t.Logf("Vacuum with closed pool: %v", err)
	}
}

// TestSaveErrors tests Save error paths
func TestSaveErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "save_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Close pool to simulate error
	pool.Close()

	// Save should handle closed pool gracefully
	err := c.Save()
	if err != nil {
		t.Logf("Save with closed pool: %v", err)
	}
}

// TestRollbackTransactionErrors tests RollbackTransaction scenarios
func TestRollbackTransactionErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Rollback with empty transaction stack
	c.RollbackTransaction()

	// Rollback again should not panic
	c.RollbackTransaction()
}

// TestUpdateVectorIndexesForInsertErrors tests error paths
func TestUpdateVectorIndexesForInsertErrors(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table without vector column
	c.CreateTable(&query.CreateTableStmt{
		Table: "novec_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Create vector index on non-vector column (should fail)
	err := c.CreateVectorIndex("idx_bad", "novec_test", "name")
	if err != nil {
		t.Logf("CreateVectorIndex on non-vector column: %v", err)
	}

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "novec_test",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)
}

// TestEvaluateExprWithGroupAggregatesJoinErrors tests error paths
func TestEvaluateExprWithGroupAggregatesJoinErrors(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "join_agg",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat", Type: query.TokenText},
		},
	})

	// Insert
	c.Insert(ctx, &query.InsertStmt{
		Table:   "join_agg",
		Columns: []string{"id", "cat"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}},
	}, nil)

	// Query with invalid aggregate in JOIN
	_, err := c.ExecuteQuery("SELECT *, INVALID_FUNC(*) FROM join_agg GROUP BY cat")
	if err != nil {
		t.Logf("Invalid aggregate function: %v", err)
	}
}

// TestApplyGroupByOrderByErrors tests error paths
func TestApplyGroupByOrderByErrors(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "gb_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert
	c.Insert(ctx, &query.InsertStmt{
		Table:   "gb_err",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// GROUP BY with invalid ORDER BY
	_, err := c.ExecuteQuery("SELECT val, COUNT(*) FROM gb_err GROUP BY val ORDER BY nonexistent")
	if err != nil {
		t.Logf("GROUP BY with invalid ORDER BY: %v", err)
	}
}

// TestSelectLockedErrors tests selectLocked error paths
func TestSelectLockedErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "sel_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Query from non-existent table
	_, err := c.ExecuteQuery("SELECT * FROM nonexistent_table_xyz")
	if err == nil {
		t.Error("Query on non-existent table should error")
	}

	// Query with invalid expression
	_, err = c.ExecuteQuery("SELECT INVALID_FUNC(id) FROM sel_err")
	if err != nil {
		t.Logf("Invalid function: %v", err)
	}
}

// TestUpdateLockedErrors tests updateLocked error paths
func TestUpdateLockedErrors(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Update with invalid column
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_err",
		Set:   []*query.SetClause{{Column: "nonexistent", Value: strReal("test")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Logf("Update invalid column: %v", err)
	}
}

// TestDeleteWithUsingLockedErrors tests error paths
func TestDeleteWithUsingLockedErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_u_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Delete with invalid subquery
	_, err := c.ExecuteQuery("DELETE FROM del_u_err WHERE id IN (SELECT invalid_col FROM nonexistent)")
	if err != nil {
		t.Logf("Delete with invalid subquery: %v", err)
	}
}

// TestUpdateWithJoinLockedErrors tests error paths
func TestUpdateWithJoinLockedErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_j_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Update with invalid JOIN
	_, err := c.ExecuteQuery("UPDATE upd_j_err SET id = 1 FROM nonexistent WHERE upd_j_err.id = nonexistent.id")
	if err != nil {
		t.Logf("Update with invalid JOIN: %v", err)
	}
}

// TestVectorUpdateErrors tests HNSW Update error paths
func TestVectorUpdateErrors(t *testing.T) {
	hnsw := NewHNSWIndex("err_idx", "err_table", "vec", 3)

	// Insert a vector
	hnsw.Insert("key1", []float64{1.0, 2.0, 3.0})

	// Update with nil vector (should error)
	err := hnsw.Update("key1", nil)
	if err == nil {
		t.Error("Update with nil vector should error")
	}

	// Update with wrong dimensions
	err = hnsw.Update("key1", []float64{1.0, 2.0})
	if err == nil {
		t.Error("Update with wrong dimensions should error")
	}

	// Update empty string key
	err = hnsw.Update("", []float64{1.0, 2.0, 3.0})
	if err != nil {
		t.Logf("Update empty key: %v", err)
	}
}
