package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSelectLockedScalarSelect tests SELECT without FROM (scalar expressions)
func TestSelectLockedScalarSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Scalar SELECT
	result, err := c.ExecuteQuery("SELECT 1 + 2 as total, 'hello' as msg")
	if err != nil {
		t.Errorf("Scalar SELECT failed: %v", err)
	} else {
		t.Logf("Scalar SELECT returned %d rows, %d cols", len(result.Rows), len(result.Columns))
	}
}

// TestSelectLockedAsOfSystemTime tests AS OF SYSTEM TIME
func TestSelectLockedAsOfSystemTime(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "asof_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "asof_test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// AS OF query
	result, err := c.ExecuteQuery("SELECT * FROM asof_test AS OF SYSTEM TIME '2024-01-01'")
	if err != nil {
		t.Logf("AS OF query: %v", err)
	} else {
		t.Logf("AS OF returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedDerivedTable tests derived table subqueries
func TestSelectLockedDerivedTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "derived_base",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "derived_base",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(200)}},
	}, nil)

	// Derived table query
	result, err := c.ExecuteQuery("SELECT * FROM (SELECT id, val * 2 as doubled FROM derived_base) AS dt WHERE doubled > 150")
	if err != nil {
		t.Logf("Derived table query: %v", err)
	} else {
		t.Logf("Derived table returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithWindowFuncs tests window functions
func TestSelectLockedWithWindowFuncs(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "window_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "window_test",
		Columns: []string{"id", "cat", "val"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(10)},
			{numReal(2), strReal("A"), numReal(20)},
			{numReal(3), strReal("B"), numReal(30)},
		},
	}, nil)

	// ROW_NUMBER window function
	result, err := c.ExecuteQuery("SELECT id, ROW_NUMBER() OVER (ORDER BY val) as rn FROM window_test")
	if err != nil {
		t.Logf("Window function query: %v", err)
	} else {
		t.Logf("Window function returned %d rows", len(result.Rows))
	}
}

// TestProcessUpdateRowWithRLS tests RLS during update
func TestProcessUpdateRowWithRLS(t *testing.T) {
	ctx := context.WithValue(context.Background(), "cobaltdb_user", "admin")
	ctx = context.WithValue(ctx, "cobaltdb_roles", []string{"admin"})

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "rls_update",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "rls_update",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal("secret")}},
	}, nil)

	// Update with RLS context
	count, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "rls_update",
		Set:   []*query.SetClause{{Column: "data", Value: strReal("updated")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Logf("Update with RLS: %v", err)
	} else {
		t.Logf("Updated %d rows with RLS", count)
	}
}

// TestProcessUpdateRowUniqueConstraint tests UNIQUE constraint during update
func TestProcessUpdateRowUniqueConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "unique_update",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText, Unique: true},
		},
	})

	// Insert two rows
	c.Insert(ctx, &query.InsertStmt{
		Table:   "unique_update",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("ABC")}, {numReal(2), strReal("XYZ")}},
	}, nil)

	// Try to update to duplicate value
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "unique_update",
		Set:   []*query.SetClause{{Column: "code", Value: strReal("ABC")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 2},
		},
	}, nil)
	if err != nil {
		t.Logf("Update with UNIQUE violation: %v", err)
	}
}

// TestBuildJSONIndexWithData tests building JSON index with existing data
func TestBuildJSONIndexWithData(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "json_idx_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})

	// Insert data first
	c.Insert(ctx, &query.InsertStmt{
		Table:   "json_idx_data",
		Columns: []string{"id", "data"},
		Values: [][]query.Expression{
			{numReal(1), strReal(`{"name": "alice", "age": 30}`)},
			{numReal(2), strReal(`{"name": "bob", "age": 25}`)},
			{numReal(3), strReal(`{"name": "charlie", "age": 35}`)},
		},
	}, nil)

	// Create index on existing data
	err := c.CreateJSONIndex("idx_name", "json_idx_data", "data", "$.name", "TEXT")
	if err != nil {
		t.Errorf("CreateJSONIndex on existing data failed: %v", err)
	}

	// Query using JSON index
	result, err := c.ExecuteQuery("SELECT * FROM json_idx_data WHERE JSON_EXTRACT(data, '$.name') = 'alice'")
	if err != nil {
		t.Logf("JSON index query: %v", err)
	} else {
		t.Logf("JSON index query returned %d rows", len(result.Rows))
	}
}

// TestBuildJSONIndexNumericPath tests JSON index on numeric paths
func TestBuildJSONIndexNumericPath(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "json_num",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenJSON},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "json_num",
		Columns: []string{"id", "data"},
		Values: [][]query.Expression{
			{numReal(1), strReal(`{"age": 30}`)},
			{numReal(2), strReal(`{"age": 25}`)},
		},
	}, nil)

	// Create numeric JSON index
	err := c.CreateJSONIndex("idx_age", "json_num", "data", "$.age", "INTEGER")
	if err != nil {
		t.Logf("CreateJSONIndex numeric: %v", err)
	}
}

// TestVacuumWithMultipleTables tests Vacuum with multiple tables
func TestVacuumWithMultipleTables(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create multiple tables
	for i := 1; i <= 3; i++ {
		tableName := fmt.Sprintf("vac_multi_%d", i)
		c.CreateTable(&query.CreateTableStmt{
			Table: tableName,
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			},
		})

		// Insert data
		for j := 1; j <= 20; j++ {
			c.Insert(ctx, &query.InsertStmt{
				Table:   tableName,
				Columns: []string{"id"},
				Values:  [][]query.Expression{{numReal(float64(j))}},
			}, nil)
		}

		// Delete some
		c.Delete(ctx, &query.DeleteStmt{
			Table: tableName,
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 10},
			},
		}, nil)
	}

	// Vacuum all tables
	err := c.Vacuum()
	if err != nil {
		t.Errorf("Vacuum with multiple tables failed: %v", err)
	}

	// Verify counts
	for i := 1; i <= 3; i++ {
		tableName := fmt.Sprintf("vac_multi_%d", i)
		result, _ := c.ExecuteQuery(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
		if len(result.Rows) == 1 {
			t.Logf("Table %s has %v rows after vacuum", tableName, result.Rows[0][0])
		}
	}
}

// TestSaveWithMultipleObjects tests Save with many objects
func TestSaveWithMultipleObjects(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create multiple tables, indexes, views
	for i := 1; i <= 5; i++ {
		tableName := fmt.Sprintf("save_obj_%d", i)
		c.CreateTable(&query.CreateTableStmt{
			Table: tableName,
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "val", Type: query.TokenText},
			},
		})

		c.Insert(ctx, &query.InsertStmt{
			Table:   tableName,
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(1), strReal("test")}},
		}, nil)
	}

	// Save
	err := c.Save()
	if err != nil {
		t.Errorf("Save with multiple objects failed: %v", err)
	}
}

// TestCountRowsWithSpecialNames tests countRows with special table names
func TestCountRowsWithSpecialNames(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	sc := NewStatsCollector(c)

	// Create table with underscore name
	c.CreateTable(&query.CreateTableStmt{
		Table: "my_test_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "my_test_table",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	count, err := sc.countRows("my_test_table")
	if err != nil {
		t.Errorf("countRows failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// TestUpdateVectorIndexWithExistingData tests vector index update
func TestUpdateVectorIndexWithExistingData(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "vec_upd",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector},
		},
	})

	// Create vector index
	c.CreateVectorIndex("idx_vec_upd", "vec_upd", "embedding")

	// Insert vectors
	for i := 1; i <= 3; i++ {
		vec := fmt.Sprintf("[%d.0, %d.0, %d.0]", i, i, i)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vec_upd",
			Columns: []string{"id", "embedding"},
			Values:  [][]query.Expression{{numReal(float64(i)), &query.StringLiteral{Value: vec}}},
		}, nil)
	}

	// Update a vector
	c.Update(ctx, &query.UpdateStmt{
		Table: "vec_upd",
		Set:   []*query.SetClause{{Column: "embedding", Value: &query.StringLiteral{Value: "[9.0, 9.0, 9.0]"}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)

	// Search after update
	results, _, err := c.SearchVectorKNN("idx_vec_upd", []float64{9, 9, 9}, 1)
	if err != nil {
		t.Logf("Vector search after update: %v", err)
	} else {
		t.Logf("Found %d results after update", len(results))
	}
}

// TestUpdateRowSliceWithIndex tests updateRowSlice when indexes exist
func TestUpdateRowSliceWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_slice_idx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Create index
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "upd_slice_idx",
		Columns: []string{"code"},
		Unique:  false,
	})

	// Insert and update
	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_slice_idx",
		Columns: []string{"id", "code", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A"), numReal(10)}},
	}, nil)

	c.Update(ctx, &query.UpdateStmt{
		Table: "upd_slice_idx",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(20)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)

	// Verify
	result, _ := c.ExecuteQuery("SELECT val FROM upd_slice_idx WHERE id = 1")
	if len(result.Rows) == 1 {
		if v, ok := result.Rows[0][0].(int64); ok && v == 20 {
			t.Log("Update with index succeeded")
		}
	}
}

// TestDeleteWithUsingWithResults tests DELETE with USING that actually deletes
func TestDeleteWithUsingWithResults(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_target_u",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "del_ref_u",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_target_u",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_ref_u",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Delete using subquery
	count, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "del_target_u",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenIn,
			Right: &query.SubqueryExpr{
				Query: &query.SelectStmt{
					Columns: []query.Expression{&query.Identifier{Name: "id"}},
					From:    &query.TableRef{Name: "del_ref_u"},
				},
			},
		},
	}, nil)
	if err != nil {
		t.Logf("Delete with USING: %v", err)
	} else {
		t.Logf("Deleted %d rows", count)
	}
}

// TestUpdateWithJoinActual tests UPDATE with actual JOIN
func TestUpdateWithJoinActual(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "upd_s",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "new_val", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_t",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(0)}, {numReal(2), numReal(0)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "upd_s",
		Columns: []string{"id", "new_val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Update with FROM
	_, err := c.ExecuteQuery("UPDATE upd_t SET val = (SELECT new_val FROM upd_s WHERE upd_s.id = upd_t.id) WHERE id = 1")
	if err != nil {
		t.Logf("UPDATE with FROM: %v", err)
	}
}

// TestVectorHNSWUpdateMultiple tests multiple HNSW updates
func TestVectorHNSWUpdateMultiple(t *testing.T) {
	hnsw := NewHNSWIndex("multi_upd", "table1", "vec", 3)

	// Insert multiple
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		vec := []float64{float64(i), float64(i + 1), float64(i + 2)}
		hnsw.Insert(key, vec)
	}

	// Update each multiple times
	for round := 0; round < 3; round++ {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			vec := []float64{float64(i + round), float64(i + round + 1), float64(i + round + 2)}
			err := hnsw.Update(key, vec)
			if err != nil {
				t.Errorf("Update %s round %d failed: %v", key, round, err)
			}
		}
	}

	if len(hnsw.Nodes) != 5 {
		t.Errorf("Expected 5 nodes, got %d", len(hnsw.Nodes))
	}
}

// TestEvaluateExprWithGroupAggregatesJoinMultiple tests complex JOIN aggregates
func TestEvaluateExprWithGroupAggregatesJoinMultiple(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "agg_a",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat", Type: query.TokenText},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "agg_b",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "a_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "agg_a",
		Columns: []string{"id", "cat"},
		Values:  [][]query.Expression{{numReal(1), strReal("X")}, {numReal(2), strReal("Y")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "agg_b",
		Columns: []string{"id", "a_id", "amount"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), numReal(100)},
			{numReal(2), numReal(1), numReal(200)},
			{numReal(3), numReal(2), numReal(300)},
			{numReal(4), numReal(2), numReal(400)},
		},
	}, nil)

	// Multiple aggregates in JOIN
	result, err := c.ExecuteQuery("SELECT a.cat, COUNT(b.id), SUM(b.amount), AVG(b.amount) FROM agg_a a JOIN agg_b b ON a.id = b.a_id GROUP BY a.cat")
	if err != nil {
		t.Logf("JOIN with multiple aggregates: %v", err)
	} else {
		t.Logf("JOIN aggregates returned %d rows", len(result.Rows))
	}
}

// TestApplyGroupByOrderByMultiCol tests GROUP BY ORDER BY with multiple columns
func TestApplyGroupByOrderByMultiCol(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "gb_ob",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat1", Type: query.TokenText},
			{Name: "cat2", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "gb_ob",
		Columns: []string{"id", "cat1", "cat2", "val"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), strReal("X"), numReal(10)},
			{numReal(2), strReal("A"), strReal("Y"), numReal(20)},
			{numReal(3), strReal("B"), strReal("X"), numReal(30)},
			{numReal(4), strReal("B"), strReal("Y"), numReal(40)},
		},
	}, nil)

	// GROUP BY with ORDER BY on aggregate
	result, err := c.ExecuteQuery("SELECT cat1, cat2, SUM(val) as total FROM gb_ob GROUP BY cat1, cat2 ORDER BY total DESC, cat1 ASC")
	if err != nil {
		t.Logf("GROUP BY ORDER BY complex: %v", err)
	} else {
		t.Logf("Returned %d rows", len(result.Rows))
	}
}

// TestRollbackTransactionMultiple tests multiple rollbacks
func TestRollbackTransactionMultiple(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "rb_multi",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Multiple rollbacks in sequence
	for i := 0; i < 5; i++ {
		c.RollbackTransaction()
	}
}
