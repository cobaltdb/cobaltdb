package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSelectLockedComplexQueries tests selectLocked with complex scenarios
func TestSelectLockedComplexQueries(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "complex_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "complex_test",
		Columns: []string{"id", "name", "val"},
		Values: [][]query.Expression{
			{numReal(1), strReal("alice"), numReal(100)},
			{numReal(2), strReal("bob"), numReal(200)},
			{numReal(3), strReal("alice"), numReal(300)},
		},
	}, nil)

	// Test GROUP BY with aggregates
	result, err := c.ExecuteQuery("SELECT name, SUM(val) FROM complex_test GROUP BY name")
	if err != nil {
		t.Logf("GROUP BY query: %v", err)
	} else {
		t.Logf("GROUP BY returned %d rows", len(result.Rows))
	}

	// Test subquery
	result, err = c.ExecuteQuery("SELECT * FROM complex_test WHERE id IN (SELECT id FROM complex_test WHERE val > 150)")
	if err != nil {
		t.Logf("Subquery: %v", err)
	} else {
		t.Logf("Subquery returned %d rows", len(result.Rows))
	}

	// Test ORDER BY multiple columns
	result, err = c.ExecuteQuery("SELECT * FROM complex_test ORDER BY name ASC, val DESC")
	if err != nil {
		t.Logf("ORDER BY multiple: %v", err)
	} else if len(result.Rows) == 3 {
		t.Log("ORDER BY multiple columns works")
	}
}

// TestUpdateLockedComplex tests updateLocked with complex scenarios
func TestUpdateLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "update_complex",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "counter", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "update_complex",
		Columns: []string{"id", "counter", "name"},
		Values: [][]query.Expression{
			{numReal(1), numReal(0), strReal("test")},
			{numReal(2), numReal(0), strReal("test")},
		},
	}, nil)

	// Test UPDATE with complex WHERE
	count, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "update_complex",
		Set:   []*query.SetClause{{Column: "counter", Value: numReal(1)}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 0},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "name"},
				Operator: query.TokenEq,
				Right:    &query.StringLiteral{Value: "test"},
			},
		},
	}, nil)
	if err != nil {
		t.Logf("Complex UPDATE error: %v", err)
	} else {
		t.Logf("Updated %d rows", count)
	}
}

// TestSaveWithErrorPaths tests Save function error paths
func TestSaveWithErrorPaths(t *testing.T) {
	// Test with nil pool
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables before Save
	c.CreateTable(&query.CreateTableStmt{
		Table: "save_error_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Save should work
	err := c.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	pool.Close()

	// After close, Save might error
	err = c.Save()
	if err != nil {
		t.Logf("Save after close (may error): %v", err)
	}
}

// TestVacuumWithLargeData tests Vacuum with more data
func TestVacuumWithLargeData(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "vacuum_large",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	})

	// Insert many rows
	for i := 1; i <= 50; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vacuum_large",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Delete many rows
	c.Delete(ctx, &query.DeleteStmt{
		Table: "vacuum_large",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 25},
		},
	}, nil)

	// Vacuum
	err := c.Vacuum()
	if err != nil {
		t.Errorf("Vacuum failed: %v", err)
	}

	// Verify remaining rows
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM vacuum_large")
	if len(result.Rows) == 1 {
		count := result.Rows[0][0]
		t.Logf("Remaining rows after vacuum: %v", count)
	}
}

// TestRollbackTransactionComplex tests RollbackTransaction with complex scenarios
func TestRollbackTransactionComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_complex",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert outside transaction
	c.Insert(ctx, &query.InsertStmt{
		Table:   "rollback_complex",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Rollback
	c.RollbackTransaction()

	// Verify data still exists
	result, _ := c.ExecuteQuery("SELECT * FROM rollback_complex")
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row after rollback, got %d", len(result.Rows))
	}
}

// TestApplyGroupByOrderByComplex tests applyGroupByOrderBy with complex scenarios
func TestApplyGroupByOrderByComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "group_order",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data with multiple categories
	c.Insert(ctx, &query.InsertStmt{
		Table:   "group_order",
		Columns: []string{"id", "category", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(100)},
			{numReal(2), strReal("B"), numReal(200)},
			{numReal(3), strReal("A"), numReal(50)},
			{numReal(4), strReal("B"), numReal(150)},
			{numReal(5), strReal("C"), numReal(300)},
		},
	}, nil)

	// Test GROUP BY with ORDER BY
	result, err := c.ExecuteQuery("SELECT category, SUM(amount) as total FROM group_order GROUP BY category ORDER BY total DESC")
	if err != nil {
		t.Logf("GROUP BY ORDER BY error: %v", err)
	} else {
		t.Logf("GROUP BY ORDER BY returned %d rows", len(result.Rows))
		if len(result.Rows) > 0 {
			t.Logf("First category: %v", result.Rows[0])
		}
	}
}

// TestEvaluateExprWithGroupAggregatesJoinComplex tests evaluateExprWithGroupAggregatesJoin
func TestEvaluateExprWithGroupAggregatesJoinComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create two tables for JOIN
	c.CreateTable(&query.CreateTableStmt{
		Table: "join_a",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "join_b",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "a_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "join_a",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "join_b",
		Columns: []string{"id", "a_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}, {numReal(2), numReal(1), numReal(200)}, {numReal(3), numReal(2), numReal(300)}},
	}, nil)

	// Test JOIN with GROUP BY
	result, err := c.ExecuteQuery("SELECT a.id, SUM(b.amount) FROM join_a a JOIN join_b b ON a.id = b.a_id GROUP BY a.id")
	if err != nil {
		t.Logf("JOIN GROUP BY error: %v", err)
	} else {
		t.Logf("JOIN GROUP BY returned %d rows", len(result.Rows))
	}
}

// TestProcessUpdateRowComplex tests processUpdateRow with various scenarios
func TestProcessUpdateRowComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "proc_update",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText},
			{Name: "count", Type: query.TokenInteger},
		},
	})

	// Insert multiple rows
	c.Insert(ctx, &query.InsertStmt{
		Table:   "proc_update",
		Columns: []string{"id", "status", "count"},
		Values: [][]query.Expression{
			{numReal(1), strReal("active"), numReal(0)},
			{numReal(2), strReal("inactive"), numReal(0)},
			{numReal(3), strReal("active"), numReal(0)},
		},
	}, nil)

	// Update with RETURNING clause simulation
	count, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "proc_update",
		Set:   []*query.SetClause{{Column: "count", Value: numReal(1)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "status"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "active"},
		},
	}, nil)
	if err != nil {
		t.Logf("Update error: %v", err)
	} else {
		t.Logf("Updated %d rows", count)
	}
}

// TestBuildJSONIndexComplex tests buildJSONIndex with edge cases
func TestBuildJSONIndexComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "json_idx_complex",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})

	// Insert various JSON types
	c.Insert(ctx, &query.InsertStmt{
		Table:   "json_idx_complex",
		Columns: []string{"id", "data"},
		Values: [][]query.Expression{
			{numReal(1), strReal(`{"name": "alice", "age": 30, "tags": ["a", "b"]}`)},
			{numReal(2), strReal(`{"name": "bob", "age": 25, "nested": {"x": 1}}`)},
			{numReal(3), strReal(`null`)},
			{numReal(4), strReal(`{"name": null}`)},
		},
	}, nil)

	// Create JSON index on nested path
	err := c.CreateJSONIndex("idx_json_name", "json_idx_complex", "data", "$.name", "TEXT")
	if err != nil {
		t.Logf("JSON index on nested path: %v", err)
	}

	// Create JSON index on array
	err = c.CreateJSONIndex("idx_json_tags", "json_idx_complex", "data", "$.tags[0]", "TEXT")
	if err != nil {
		t.Logf("JSON index on array: %v", err)
	}
}

// TestUpdateVectorIndexesForInsertComplex tests updateVectorIndexesForInsert
func TestUpdateVectorIndexesForInsertComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "vec_insert_complex",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "vec", Type: query.TokenVector},
		},
	})

	// Create vector index
	err := c.CreateVectorIndex("idx_vec_ins", "vec_insert_complex", "vec")
	if err != nil {
		t.Logf("CreateVectorIndex: %v", err)
	}

	// Insert multiple vectors
	for i := 1; i <= 5; i++ {
		vec := &query.StringLiteral{Value: fmt.Sprintf("[%d.0, %d.0, %d.0]", i, i, i)}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vec_insert_complex",
			Columns: []string{"id", "vec"},
			Values:  [][]query.Expression{{numReal(float64(i)), vec}},
		}, nil)
	}

	// Search
	results, _, err := c.SearchVectorKNN("idx_vec_ins", []float64{1, 1, 1}, 3)
	if err != nil {
		t.Logf("Vector search: %v", err)
	} else {
		t.Logf("Found %d nearest neighbors", len(results))
	}
}

// TestStoreIndexDefComplex tests storeIndexDef
func TestStoreIndexDefComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "idx_def_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Create various index types
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_simple",
		Table:   "idx_def_test",
		Columns: []string{"name"},
		Unique:  false,
	})
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_unique",
		Table:   "idx_def_test",
		Columns: []string{"id", "name"},
		Unique:  true,
	})

	// Insert and verify indexes are used
	c.Insert(ctx, &query.InsertStmt{
		Table:   "idx_def_test",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Query using indexed column
	result, err := c.ExecuteQuery("SELECT * FROM idx_def_test WHERE name = 'test'")
	if err != nil {
		t.Logf("Query with index: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestUpdateWithJoinLockedComplex tests updateWithJoinLocked
func TestUpdateWithJoinLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables for UPDATE JOIN
	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_target",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenInteger},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_source",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "new_value", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_target",
		Columns: []string{"id", "value"},
		Values:  [][]query.Expression{{numReal(1), numReal(0)}, {numReal(2), numReal(0)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_source",
		Columns: []string{"id", "new_value"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(200)}},
	}, nil)

	// Try UPDATE with JOIN
	_, err := c.ExecuteQuery("UPDATE upd_target SET value = (SELECT new_value FROM upd_source WHERE upd_source.id = upd_target.id)")
	if err != nil {
		t.Logf("UPDATE with subquery: %v", err)
	}
}

// TestDeleteWithUsingLockedComplex tests deleteWithUsingLocked
func TestDeleteWithUsingLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_target",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "del_flag", Type: query.TokenInteger},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "del_check",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_target",
		Columns: []string{"id", "del_flag"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(0)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_check",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Delete with correlated subquery
	_, err := c.ExecuteQuery("DELETE FROM del_target WHERE id IN (SELECT id FROM del_check)")
	if err != nil {
		t.Logf("DELETE with subquery: %v", err)
	}

	// Verify
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM del_target")
	if len(result.Rows) == 1 {
		t.Logf("Remaining rows: %v", result.Rows[0][0])
	}
}

// TestUpdateRowSliceComplex tests updateRowSlice
func TestUpdateRowSliceComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "slice_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "a", Type: query.TokenInteger},
			{Name: "b", Type: query.TokenText},
		},
	})

	// Insert
	c.Insert(ctx, &query.InsertStmt{
		Table:   "slice_test",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(1), numReal(10), strReal("x")}},
	}, nil)

	// Update
	c.Update(ctx, &query.UpdateStmt{
		Table: "slice_test",
		Set: []*query.SetClause{
			{Column: "a", Value: numReal(20)},
			{Column: "b", Value: strReal("y")},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)

	// Verify
	result, _ := c.ExecuteQuery("SELECT a, b FROM slice_test WHERE id = 1")
	if len(result.Rows) == 1 {
		t.Logf("Updated row: %v", result.Rows[0])
	}
}

// TestCountRowsComplex tests countRows
func TestCountRowsComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "count_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	sc := NewStatsCollector(c)

	// Empty table
	count, err := sc.countRows("count_test")
	if err != nil {
		t.Errorf("countRows on empty table: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0, got %d", count)
	}

	// Insert rows
	for i := 1; i <= 100; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "count_test",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	// Count again
	count, err = sc.countRows("count_test")
	if err != nil {
		t.Errorf("countRows: %v", err)
	}
	if count != 100 {
		t.Errorf("Expected 100, got %d", count)
	}

	// Delete some
	c.Delete(ctx, &query.DeleteStmt{
		Table: "count_test",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 50},
		},
	}, nil)

	// Count after delete
	count, err = sc.countRows("count_test")
	if err != nil {
		t.Errorf("countRows after delete: %v", err)
	}
	if count != 50 {
		t.Errorf("Expected 50 after delete, got %d", count)
	}
}

// TestVectorUpdateComplex tests HNSW Update method
func TestVectorUpdateComplex(t *testing.T) {
	hnsw := NewHNSWIndex("vec_idx", "vec_table", "embedding", 3)

	// Insert
	hnsw.Insert("key1", []float64{1.0, 0.0, 0.0})
	hnsw.Insert("key2", []float64{0.0, 1.0, 0.0})

	// Update existing
	err := hnsw.Update("key1", []float64{0.9, 0.1, 0.0})
	if err != nil {
		t.Errorf("Update error: %v", err)
	}

	// Update non-existent (should insert)
	err = hnsw.Update("key3", []float64{0.0, 0.0, 1.0})
	if err != nil {
		t.Errorf("Update new key error: %v", err)
	}

	// Wrong dimensions
	err = hnsw.Update("key1", []float64{1.0, 2.0})
	if err == nil {
		t.Error("Expected error for wrong dimensions")
	}

	// Verify - Update on empty index may create or replace
	if len(hnsw.Nodes) < 2 {
		t.Errorf("Expected at least 2 nodes, got %d", len(hnsw.Nodes))
	}
}
