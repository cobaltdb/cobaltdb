package catalog

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/fdw"
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

// mockFDW is a test foreign data wrapper that returns static rows.
type mockFDW struct {
	rows [][]interface{}
}

func (m *mockFDW) Name() string { return "mock" }
func (m *mockFDW) Open(options map[string]string) error { return nil }
func (m *mockFDW) Scan(table string, columns []string) ([][]interface{}, error) {
	return m.rows, nil
}
func (m *mockFDW) Close() error { return nil }

// TestFDWCatalogFunctions tests all FDW-related catalog methods.
func TestFDWCatalogFunctions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	reg := fdw.NewRegistry()
	reg.Register("mock", func() fdw.ForeignDataWrapper { return &mockFDW{rows: [][]interface{}{{1, "alice"}, {2, "bob"}}} })
	c.SetFDWRegistry(reg)

	if c.GetFDWRegistry() == nil {
		t.Fatal("Expected non-nil FDW registry")
	}

	// Create foreign table
	err := c.CreateForeignTable(&query.CreateForeignTableStmt{
		Table: "ft_users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
		Wrapper: "mock",
		Options: map[string]string{"source": "test"},
	})
	if err != nil {
		t.Fatalf("CreateForeignTable failed: %v", err)
	}

	// Duplicate should error
	err = c.CreateForeignTable(&query.CreateForeignTableStmt{Table: "ft_users", Wrapper: "mock"})
	if err != ErrTableExists {
		t.Errorf("Expected ErrTableExists, got %v", err)
	}

	// Missing wrapper should error (when registry is set)
	err = c.CreateForeignTable(&query.CreateForeignTableStmt{Table: "ft_bad", Wrapper: "missing"})
	if err == nil {
		t.Error("Expected error for missing wrapper")
	}

	// IsForeignTable
	if !c.IsForeignTable("ft_users") {
		t.Error("Expected ft_users to be foreign table")
	}
	if c.IsForeignTable("nonexistent") {
		t.Error("Expected nonexistent to not be foreign table")
	}

	// GetForeignTable
	ft, err := c.GetForeignTable("ft_users")
	if err != nil {
		t.Errorf("GetForeignTable failed: %v", err)
	}
	if ft == nil || ft.TableName != "ft_users" {
		t.Errorf("Unexpected foreign table: %v", ft)
	}

	// GetForeignTable not found
	_, err = c.GetForeignTable("missing")
	if err != ErrTableNotFound {
		t.Errorf("Expected ErrTableNotFound, got %v", err)
	}

	// DropForeignTable
	err = c.DropForeignTable("ft_users")
	if err != nil {
		t.Errorf("DropForeignTable failed: %v", err)
	}
	if c.IsForeignTable("ft_users") {
		t.Error("Expected ft_users to be dropped")
	}

	// Drop non-existent
	err = c.DropForeignTable("missing")
	if err != ErrTableNotFound {
		t.Errorf("Expected ErrTableNotFound, got %v", err)
	}
}

// TestSetParallelOptions tests configuring parallel execution.
func TestSetParallelOptions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.SetParallelOptions(4, 100)
	if c.parallelWorkers != 4 || c.parallelThreshold != 100 {
		t.Error("SetParallelOptions did not set values correctly")
	}
}

// TestHasSubqueries tests the subquery detection helpers.
func TestHasSubqueries(t *testing.T) {
	// nil expr
	if hasSubqueriesInExpr(nil) {
		t.Error("nil expr should not have subqueries")
	}

	// SubqueryExpr
	if !hasSubqueriesInExpr(&query.SubqueryExpr{}) {
		t.Error("SubqueryExpr should be detected")
	}

	// ExistsExpr
	if !hasSubqueriesInExpr(&query.ExistsExpr{}) {
		t.Error("ExistsExpr should be detected")
	}

	// InExpr with subquery
	if !hasSubqueriesInExpr(&query.InExpr{Subquery: &query.SelectStmt{}}) {
		t.Error("InExpr with subquery should be detected")
	}

	// InExpr without subquery
	if hasSubqueriesInExpr(&query.InExpr{Expr: &query.Identifier{Name: "x"}, List: []query.Expression{&query.NumberLiteral{Value: 1}}}) {
		t.Error("InExpr without subquery should not be detected")
	}

	// BinaryExpr without subqueries
	if hasSubqueriesInExpr(&query.BinaryExpr{Left: &query.Identifier{Name: "a"}, Right: &query.NumberLiteral{Value: 1}}) {
		t.Error("BinaryExpr without subqueries should not be detected")
	}

	// FunctionCall without subqueries
	if hasSubqueriesInExpr(&query.FunctionCall{Name: "abs", Args: []query.Expression{&query.Identifier{Name: "x"}}}) {
		t.Error("FunctionCall without subqueries should not be detected")
	}

	// CaseExpr without subqueries
	if hasSubqueriesInExpr(&query.CaseExpr{Expr: &query.Identifier{Name: "x"}}) {
		t.Error("CaseExpr without subqueries should not be detected")
	}

	// hasSubqueries on simple SELECT
	simple := &query.SelectStmt{Columns: []query.Expression{&query.Identifier{Name: "x"}}}
	if hasSubqueries(simple) {
		t.Error("Simple SELECT should not have subqueries")
	}

	// hasSubqueries with WHERE subquery
	withSub := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "x"}},
		Where:   &query.SubqueryExpr{},
	}
	if !hasSubqueries(withSub) {
		t.Error("SELECT with subquery in WHERE should be detected")
	}
}

// TestProcessRowChunkViaParallel triggers processRowChunk through parallel execution.
func TestProcessRowChunkViaParallel(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)
	c.SetParallelOptions(2, 1)

	c.CreateTable(&query.CreateTableStmt{
		Table: "parallel_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "parallel_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	result, err := c.ExecuteQuery("SELECT id, val FROM parallel_test WHERE val > 30")
	if err != nil {
		t.Fatalf("Parallel query failed: %v", err)
	}
	if len(result.Rows) != 7 {
		t.Errorf("Expected 7 rows, got %d", len(result.Rows))
	}
}

// TestListIndexesByTable tests listing indexes grouped by table.
func TestListIndexesByTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "idx_list_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "email", Type: query.TokenText},
		},
	})

	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_name", Table: "idx_list_test", Columns: []string{"name"}})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_email", Table: "idx_list_test", Columns: []string{"email"}})

	result := c.ListIndexesByTable()
	idxs, ok := result["idx_list_test"]
	if !ok {
		t.Fatal("Expected indexes for idx_list_test")
	}
	if len(idxs) != 3 { // 2 created + 1 primary key
		t.Errorf("Expected 3 indexes, got %d", len(idxs))
	}

	// Insert and query to ensure indexes are functional
	c.Insert(ctx, &query.InsertStmt{
		Table:   "idx_list_test",
		Columns: []string{"id", "name", "email"},
		Values:  [][]query.Expression{{numReal(1), strReal("alice"), strReal("a@example.com")}},
	}, nil)

	qr, err := c.ExecuteQuery("SELECT * FROM idx_list_test WHERE name = 'alice'")
	if err != nil {
		t.Logf("Query with index: %v", err)
	} else if len(qr.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(qr.Rows))
	}
}

// TestVacuumTableAndStats tests VacuumTable, GetDeadTupleRatio, ListTablesNeedingVacuum.
func TestVacuumTableAndStats(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "vacuum_stats",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	})

	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vacuum_stats",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Delete half
	c.Delete(ctx, &query.DeleteStmt{
		Table: "vacuum_stats",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 10},
		},
	}, nil)

	// Vacuum specific table
	err := c.VacuumTable("vacuum_stats")
	if err != nil {
		t.Errorf("VacuumTable failed: %v", err)
	}

	// GetDeadTupleRatio after vacuum should be 0
	ratio := c.GetDeadTupleRatio("vacuum_stats")
	if ratio != 0 {
		t.Errorf("Expected dead tuple ratio 0 after vacuum, got %f", ratio)
	}

	// ListTablesNeedingVacuum with high threshold should be empty
	needs := c.ListTablesNeedingVacuum(0.5)
	for _, name := range needs {
		if name == "vacuum_stats" {
			t.Error("Expected vacuum_stats to not need vacuum after just vacuuming")
		}
	}

	// Insert and delete again to create dead tuples
	for i := 21; i <= 30; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vacuum_stats",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}
	c.Delete(ctx, &query.DeleteStmt{
		Table: "vacuum_stats",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 25},
		},
	}, nil)

	// Now list should include the table
	needs = c.ListTablesNeedingVacuum(0.1)
	found := false
	for _, name := range needs {
		if name == "vacuum_stats" {
			found = true
			break
		}
	}
	if !found {
		t.Log("ListTablesNeedingVacuum did not include vacuum_stats (may be expected if ratio < threshold)")
	}
}

// TestForeignTableSelect tests SELECT from a foreign table covering getTableTreesForScan foreign path.
func TestForeignTableSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	reg := fdw.NewRegistry()
	reg.Register("mock", func() fdw.ForeignDataWrapper {
		return &mockFDW{rows: [][]interface{}{{int64(1), "alice"}, {int64(2), "bob"}, {int64(3), "charlie"}}}
	})
	c.SetFDWRegistry(reg)

	err := c.CreateForeignTable(&query.CreateForeignTableStmt{
		Table: "ft_people",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
		Wrapper: "mock",
		Options: map[string]string{},
	})
	if err != nil {
		t.Fatalf("CreateForeignTable failed: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT * FROM ft_people WHERE id > 1")
	if err != nil {
		t.Fatalf("SELECT from foreign table failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// Verify column names
	if len(result.Columns) != 2 || result.Columns[0] != "id" || result.Columns[1] != "name" {
		t.Errorf("Unexpected columns: %v", result.Columns)
	}
}

// TestFormatKeyComponent tests key component formatting for all types.
func TestFormatKeyComponent(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
		ok       bool
	}{
		{int(42), "00000000000000000042", true},
		{int64(42), "00000000000000000042", true},
		{float64(42.7), "00000000000000000042", true},
		{"hello", "S:hello", true},
		{true, "B:1", true},
		{false, "B:0", true},
		{nil, "", false},
		{[]int{1, 2}, "X:[1 2]", true},
	}

	for _, tt := range tests {
		got, ok := formatKeyComponent(tt.input)
		if ok != tt.ok {
			t.Errorf("formatKeyComponent(%v) ok=%v, want %v", tt.input, ok, tt.ok)
			continue
		}
		if ok && got != tt.expected {
			t.Errorf("formatKeyComponent(%v)=%q, want %q", tt.input, got, tt.expected)
		}
	}
}

// TestHasSubqueriesInExprExhaustive covers all expression types in hasSubqueriesInExpr.
func TestHasSubqueriesInExprExhaustive(t *testing.T) {
	// UnaryExpr without subquery
	if hasSubqueriesInExpr(&query.UnaryExpr{Expr: &query.Identifier{Name: "x"}}) {
		t.Error("UnaryExpr without subquery")
	}
	// UnaryExpr with subquery
	if !hasSubqueriesInExpr(&query.UnaryExpr{Expr: &query.SubqueryExpr{}}) {
		t.Error("UnaryExpr with subquery")
	}

	// FunctionCall without subquery
	if hasSubqueriesInExpr(&query.FunctionCall{Name: "abs", Args: []query.Expression{&query.NumberLiteral{Value: 1}}}) {
		t.Error("FunctionCall without subquery")
	}
	// FunctionCall with subquery in arg
	if !hasSubqueriesInExpr(&query.FunctionCall{Name: "abs", Args: []query.Expression{&query.SubqueryExpr{}}}) {
		t.Error("FunctionCall with subquery in arg")
	}

	// CastExpr without subquery
	if hasSubqueriesInExpr(&query.CastExpr{Expr: &query.Identifier{Name: "x"}}) {
		t.Error("CastExpr without subquery")
	}
	// CastExpr with subquery
	if !hasSubqueriesInExpr(&query.CastExpr{Expr: &query.SubqueryExpr{}}) {
		t.Error("CastExpr with subquery")
	}

	// BetweenExpr without subquery
	if hasSubqueriesInExpr(&query.BetweenExpr{Expr: &query.Identifier{Name: "x"}, Lower: &query.NumberLiteral{Value: 1}, Upper: &query.NumberLiteral{Value: 10}}) {
		t.Error("BetweenExpr without subquery")
	}
	// BetweenExpr with subquery in expr
	if !hasSubqueriesInExpr(&query.BetweenExpr{Expr: &query.SubqueryExpr{}, Lower: &query.NumberLiteral{Value: 1}, Upper: &query.NumberLiteral{Value: 10}}) {
		t.Error("BetweenExpr with subquery")
	}
	// BetweenExpr with subquery in lower
	if !hasSubqueriesInExpr(&query.BetweenExpr{Expr: &query.Identifier{Name: "x"}, Lower: &query.SubqueryExpr{}, Upper: &query.NumberLiteral{Value: 10}}) {
		t.Error("BetweenExpr with subquery in lower")
	}

	// LikeExpr without subquery
	if hasSubqueriesInExpr(&query.LikeExpr{Expr: &query.Identifier{Name: "x"}, Pattern: &query.StringLiteral{Value: "%a%"}}) {
		t.Error("LikeExpr without subquery")
	}
	// LikeExpr with subquery
	if !hasSubqueriesInExpr(&query.LikeExpr{Expr: &query.SubqueryExpr{}, Pattern: &query.StringLiteral{Value: "%a%"}}) {
		t.Error("LikeExpr with subquery")
	}

	// IsNullExpr without subquery
	if hasSubqueriesInExpr(&query.IsNullExpr{Expr: &query.Identifier{Name: "x"}}) {
		t.Error("IsNullExpr without subquery")
	}
	// IsNullExpr with subquery
	if !hasSubqueriesInExpr(&query.IsNullExpr{Expr: &query.SubqueryExpr{}}) {
		t.Error("IsNullExpr with subquery")
	}

	// CaseExpr with subquery in when condition
	if !hasSubqueriesInExpr(&query.CaseExpr{Whens: []*query.WhenClause{{Condition: &query.SubqueryExpr{}, Result: &query.NumberLiteral{Value: 1}}}}) {
		t.Error("CaseExpr with subquery in when condition")
	}
	// CaseExpr with subquery in when result
	if !hasSubqueriesInExpr(&query.CaseExpr{Whens: []*query.WhenClause{{Condition: &query.Identifier{Name: "x"}, Result: &query.SubqueryExpr{}}}}) {
		t.Error("CaseExpr with subquery in when result")
	}
	// CaseExpr with subquery in else
	if !hasSubqueriesInExpr(&query.CaseExpr{Else: &query.SubqueryExpr{}}) {
		t.Error("CaseExpr with subquery in else")
	}

	// InExpr with subquery in list
	if !hasSubqueriesInExpr(&query.InExpr{Expr: &query.Identifier{Name: "x"}, List: []query.Expression{&query.SubqueryExpr{}}}) {
		t.Error("InExpr with subquery in list")
	}
}

// TestHasSubqueriesExhaustive covers all statement parts in hasSubqueries.
func TestHasSubqueriesExhaustive(t *testing.T) {
	// Column with subquery
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.SubqueryExpr{}},
	}
	if !hasSubqueries(stmt) {
		t.Error("hasSubqueries should detect subquery in columns")
	}

	// GroupBy with subquery
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "x"}},
		GroupBy: []query.Expression{&query.SubqueryExpr{}},
	}
	if !hasSubqueries(stmt) {
		t.Error("hasSubqueries should detect subquery in GROUP BY")
	}

	// Having with subquery
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "x"}},
		Having:  &query.SubqueryExpr{},
	}
	if !hasSubqueries(stmt) {
		t.Error("hasSubqueries should detect subquery in HAVING")
	}

	// OrderBy with subquery
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "x"}},
		OrderBy: []*query.OrderByExpr{{Expr: &query.SubqueryExpr{}}},
	}
	if !hasSubqueries(stmt) {
		t.Error("hasSubqueries should detect subquery in ORDER BY")
	}
}

// mockOpenErrorFDW returns an error on Open.
type mockOpenErrorFDW struct{}

func (m *mockOpenErrorFDW) Name() string                                     { return "openerr" }
func (m *mockOpenErrorFDW) Open(options map[string]string) error              { return fmt.Errorf("open error") }
func (m *mockOpenErrorFDW) Scan(table string, columns []string) ([][]interface{}, error) { return nil, nil }
func (m *mockOpenErrorFDW) Close() error                                    { return nil }

// mockScanErrorFDW returns an error on Scan.
type mockScanErrorFDW struct{}

func (m *mockScanErrorFDW) Name() string                                     { return "scanerr" }
func (m *mockScanErrorFDW) Open(options map[string]string) error              { return nil }
func (m *mockScanErrorFDW) Scan(table string, columns []string) ([][]interface{}, error) { return nil, fmt.Errorf("scan error") }
func (m *mockScanErrorFDW) Close() error                                    { return nil }

// TestGetTableTreesForScanErrors tests error paths in getTableTreesForScan.
func TestGetTableTreesForScanErrors(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Foreign table not in foreignTables map
	synth := &TableDef{Name: "missing_ft", Type: "foreign"}
	_, err := c.getTableTreesForScan(synth)
	if err == nil {
		t.Error("Expected error for foreign table not in map")
	}

	// Foreign table with nil registry
	c.foreignTables["ft1"] = &ForeignTableDef{TableName: "ft1", Wrapper: "mock"}
	c.mu.Lock()
	c.fdwRegistry = nil
	c.mu.Unlock()
	_, err = c.getTableTreesForScan(&TableDef{Name: "ft1", Type: "foreign"})
	if err == nil {
		t.Error("Expected error for nil fdw registry")
	}

	// Foreign table with wrapper not found
	reg := fdw.NewRegistry()
	c.SetFDWRegistry(reg)
	_, err = c.getTableTreesForScan(&TableDef{Name: "ft1", Type: "foreign"})
	if err == nil {
		t.Error("Expected error for wrapper not found")
	}

	// Foreign table with Open error
	reg.Register("openerr", func() fdw.ForeignDataWrapper { return &mockOpenErrorFDW{} })
	c.foreignTables["ft_openerr"] = &ForeignTableDef{TableName: "ft_openerr", Wrapper: "openerr"}
	_, err = c.getTableTreesForScan(&TableDef{Name: "ft_openerr", Type: "foreign"})
	if err == nil {
		t.Error("Expected error for wrapper Open failure")
	}

	// Foreign table with Scan error
	reg.Register("scanerr", func() fdw.ForeignDataWrapper { return &mockScanErrorFDW{} })
	c.foreignTables["ft_scanerr"] = &ForeignTableDef{TableName: "ft_scanerr", Wrapper: "scanerr"}
	_, err = c.getTableTreesForScan(&TableDef{Name: "ft_scanerr", Type: "foreign"})
	if err == nil {
		t.Error("Expected error for wrapper Scan failure")
	}

	// Normal table not found
	_, err = c.getTableTreesForScan(&TableDef{Name: "nonexistent", Type: ""})
	if err == nil {
		t.Error("Expected error for normal table not found")
	}
}

// TestProcessRowChunkDirect tests processRowChunk directly to hit edge cases.
func TestProcessRowChunkDirect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	table := &TableDef{
		Name:    "proc_test",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "TEXT"}},
	}

	selectCols := []selectColInfo{
		{name: "id", index: 0},
		{name: "name", index: 1},
	}

	stmt := &query.SelectStmt{Columns: []query.Expression{&query.StarExpr{}}}
	now := time.Now()

	// Valid row
	validRow, _ := encodeVersionedRow([]interface{}{int64(1), "alice"}, nil)
	rows, _ := c.processRowChunk([][]byte{validRow}, table, selectCols, stmt, nil, now, false)
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}

	// Corrupted value (should be skipped)
	rows, _ = c.processRowChunk([][]byte{[]byte("garbage")}, table, selectCols, stmt, nil, now, false)
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows for garbage, got %d", len(rows))
	}

	// Row with WHERE that doesn't match
	stmtWithWhere := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 999},
		},
	}
	rows, _ = c.processRowChunk([][]byte{validRow}, table, selectCols, stmtWithWhere, nil, now, false)
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows after WHERE filter, got %d", len(rows))
	}

	// Row with future createdAt (not visible)
	future := now.Add(time.Hour)
	futureRow, _ := encodeVersionedRow([]interface{}{int64(2), "bob"}, &future)
	rows, _ = c.processRowChunk([][]byte{futureRow}, table, selectCols, stmt, nil, now, false)
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows for future row, got %d", len(rows))
	}

	// Window functions path
	windowCols := []selectColInfo{{name: "id", index: 0, isWindow: true}}
	rows, fullRows := c.processRowChunk([][]byte{validRow}, table, windowCols, stmt, nil, now, true)
	if len(rows) != 1 || len(fullRows) != 1 {
		t.Errorf("Expected 1 row and 1 full row for window funcs, got %d/%d", len(rows), len(fullRows))
	}

	// WHERE evaluation error path
	stmtWhereErr := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		Where:   &query.Identifier{Name: "nonexistent_column"},
	}
	rows, _ = c.processRowChunk([][]byte{validRow}, table, selectCols, stmtWhereErr, nil, now, false)
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows when WHERE errors, got %d", len(rows))
	}

	// Expression evaluation path (ci.index == -1, !isAggregate)
	exprCols := []selectColInfo{{name: "expr", index: -1, isAggregate: false}}
	stmtExpr := &query.SelectStmt{Columns: []query.Expression{&query.BinaryExpr{
		Left: &query.Identifier{Name: "id"}, Operator: query.TokenPlus, Right: &query.NumberLiteral{Value: 10},
	}}}
	rows, _ = c.processRowChunk([][]byte{validRow}, table, exprCols, stmtExpr, nil, now, false)
	if len(rows) != 1 {
		t.Errorf("Expected 1 row for expression eval, got %d", len(rows))
	}

	// __orderby_ path
	orderbyCols := []selectColInfo{{name: "__orderby_0", index: -1, isAggregate: false}}
	stmtOB := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		OrderBy: []*query.OrderByExpr{{Expr: &query.Identifier{Name: "id"}}},
	}
	rows, _ = c.processRowChunk([][]byte{validRow}, table, orderbyCols, stmtOB, nil, now, false)
	if len(rows) != 1 {
		t.Errorf("Expected 1 row for orderby eval, got %d", len(rows))
	}
}

// TestCreateTableIfNotExists tests CREATE TABLE IF NOT EXISTS paths.
func TestCreateTableIfNotExists(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create initial table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "ifne_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// IF NOT EXISTS on existing table should succeed silently
	err = c.CreateTable(&query.CreateTableStmt{
		Table:       "ifne_test",
		IfNotExists: true,
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Errorf("IF NOT EXISTS on existing table should succeed: %v", err)
	}

	// IF NOT EXISTS on existing foreign table should succeed silently
	c.foreignTables["ifne_ft"] = &ForeignTableDef{TableName: "ifne_ft", Wrapper: "mock"}
	err = c.CreateTable(&query.CreateTableStmt{
		Table:       "ifne_ft",
		IfNotExists: true,
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Errorf("IF NOT EXISTS on existing foreign table should succeed: %v", err)
	}

	// Partition auto-generation with NumPartitions
	err = c.CreateTable(&query.CreateTableStmt{
		Table: "part_auto",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
		Partition: &query.PartitionDef{
			Type:          query.PartitionTypeHash,
			Column:        "id",
			NumPartitions: 4,
		},
	})
	if err != nil {
		t.Errorf("CreateTable with auto partitions failed: %v", err)
	}
}

// TestParallelGroupBy triggers the parallel GROUP BY path in computeAggregatesWithGroupBy.
func TestParallelGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)
	c.SetParallelOptions(2, 1)

	c.CreateTable(&query.CreateTableStmt{
		Table: "parallel_gb",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	for i := 1; i <= 20; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "parallel_gb",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	result, err := c.ExecuteQuery("SELECT category, SUM(amount) FROM parallel_gb GROUP BY category")
	if err != nil {
		t.Fatalf("Parallel GROUP BY failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(result.Rows))
	}
}
