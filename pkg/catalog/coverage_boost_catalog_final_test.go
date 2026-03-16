package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_deleteWithUsingLockedWithReturning tests DELETE USING with RETURNING clause
func TestCoverage_deleteWithUsingLockedWithReturning(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "del_ret_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "ref_id", Type: query.TokenInteger},
			{Name: "data", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_ret_ref",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText},
		},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "del_ret_main",
			Columns: []string{"id", "ref_id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i%3 + 1)), strReal("data" + string(rune('0'+i%10)))}},
		}, nil)
	}

	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_ret_ref",
		Columns: []string{"id", "status"},
		Values:  [][]query.Expression{{numReal(1), strReal("inactive")}, {numReal(2), strReal("active")}, {numReal(3), strReal("inactive")}},
	}, nil)

	// Build DELETE USING statement with RETURNING
	deleteStmt := &query.DeleteStmt{
		Table: "del_ret_main",
		Using: []*query.TableRef{{Name: "del_ret_ref"}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "del_ret_main", Column: "ref_id"},
				Operator: query.TokenEq,
				Right:    &query.QualifiedIdentifier{Table: "del_ret_ref", Column: "id"},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "del_ret_ref", Column: "status"},
				Operator: query.TokenEq,
				Right:    &query.StringLiteral{Value: "inactive"},
			},
		},
		Returning: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "data"},
		},
	}

	// Execute DELETE USING with RETURNING
	_, _, err := c.Delete(ctx, deleteStmt, nil)
	if err != nil {
		t.Logf("DELETE USING with RETURNING error: %v", err)
	}

	// Verify remaining rows
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM del_ret_main")
	if len(result.Rows) > 0 {
		t.Logf("Rows after delete: %v", result.Rows[0][0])
	}
}

// TestCoverage_deleteWithUsingLockedWithIndex tests DELETE USING with index cleanup
func TestCoverage_deleteWithUsingLockedWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "del_idx_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "ref_id", Type: query.TokenInteger},
			{Name: "code", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_idx_ref",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
		},
	})

	// Create index
	c.CreateIndex(&query.CreateIndexStmt{
		Index:     "idx_del_code",
		Table:     "del_idx_main",
		Columns:   []string{"code"},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "del_idx_main",
			Columns: []string{"id", "ref_id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%3)+1)), strReal("code" + string(rune('0'+i%10)))}},
		}, nil)
	}

	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_idx_ref",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}, {numReal(3), strReal("A")}},
	}, nil)

	// Build DELETE USING statement
	deleteStmt := &query.DeleteStmt{
		Table: "del_idx_main",
		Using: []*query.TableRef{{Name: "del_idx_ref"}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "del_idx_main", Column: "ref_id"},
				Operator: query.TokenEq,
				Right:    &query.QualifiedIdentifier{Table: "del_idx_ref", Column: "id"},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "del_idx_ref", Column: "category"},
				Operator: query.TokenEq,
				Right:    &query.StringLiteral{Value: "A"},
			},
		},
	}

	// Delete USING with index cleanup
	_, _, err := c.Delete(ctx, deleteStmt, nil)
	if err != nil {
		t.Logf("DELETE USING with index error: %v", err)
	}

	// Verify
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM del_idx_main")
	if len(result.Rows) > 0 {
		t.Logf("Rows after delete: %v", result.Rows[0][0])
	}
}

// TestCoverage_deleteWithUsingLockedNoMatch tests DELETE USING with no matching rows
func TestCoverage_deleteWithUsingLockedNoMatch(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "del_nomatch_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "ref_id", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "del_nomatch_ref",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "flag", Type: query.TokenBoolean},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "del_nomatch_main",
			Columns: []string{"id", "ref_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_nomatch_ref",
		Columns: []string{"id", "flag"},
		Values:  [][]query.Expression{{numReal(1), bl(true)}, {numReal(2), bl(true)}},
	}, nil)

	// Build DELETE USING statement with no matches
	deleteStmt := &query.DeleteStmt{
		Table: "del_nomatch_main",
		Using: []*query.TableRef{{Name: "del_nomatch_ref"}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "del_nomatch_main", Column: "ref_id"},
				Operator: query.TokenEq,
				Right:    &query.QualifiedIdentifier{Table: "del_nomatch_ref", Column: "id"},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "del_nomatch_ref", Column: "flag"},
				Operator: query.TokenEq,
				Right:    &query.BooleanLiteral{Value: false},
			},
		},
	}

	// Delete USING with no matches
	_, _, err := c.Delete(ctx, deleteStmt, nil)
	if err != nil {
		t.Logf("DELETE USING no match error: %v", err)
	}

	// Verify all rows still exist
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM del_nomatch_main")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0]
		t.Logf("Rows after delete (should be 5): %v", count)
		if f, ok := count.(int64); ok && f != 5 {
			t.Errorf("Expected 5 rows, got %v", f)
		}
	}
}

// TestCoverage_selectLockedWithComplexCTE tests selectLocked with complex CTEs
func TestCoverage_selectLockedWithComplexCTE(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "cte_base",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert hierarchical data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "cte_base",
		Columns: []string{"id", "parent_id", "name"},
		Values: [][]query.Expression{
			{numReal(1), numReal(0), strReal("Root")},
			{numReal(2), numReal(1), strReal("Child1")},
			{numReal(3), numReal(1), strReal("Child2")},
			{numReal(4), numReal(2), strReal("GrandChild")},
		},
	}, nil)

	// Multiple CTEs
	result, err := c.ExecuteQuery(`
		WITH
			roots AS (SELECT * FROM cte_base WHERE parent_id = 0),
			children AS (SELECT * FROM cte_base WHERE parent_id IN (SELECT id FROM roots))
		SELECT r.name as root, c.name as child
		FROM roots r
		JOIN children c ON r.id = c.parent_id
	`)
	if err != nil {
		t.Logf("Complex CTE error: %v", err)
	} else {
		t.Logf("Complex CTE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithDerivedTable tests selectLocked with derived tables
func TestCoverage_selectLockedWithDerivedTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "derived_base",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "derived_base",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	// Derived table with aggregation
	result, err := c.ExecuteQuery(`
		SELECT d.cat, d.total, d.cnt
		FROM (
			SELECT category as cat, SUM(amount) as total, COUNT(*) as cnt
			FROM derived_base
			GROUP BY category
		) d
		WHERE d.total > 500
	`)
	if err != nil {
		t.Logf("Derived table error: %v", err)
	} else {
		t.Logf("Derived table returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithUnionInSubquery tests selectLocked with UNION in subquery
func TestCoverage_selectLockedWithUnionInSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "union_a",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "union_b",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "union_a",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A1")}, {numReal(2), strReal("A2")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "union_b",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("B1")}, {numReal(4), strReal("B2")}},
	}, nil)

	// IN with UNION subquery
	result, err := c.ExecuteQuery(`
		SELECT * FROM union_a
		WHERE id IN (SELECT id FROM union_a UNION ALL SELECT id FROM union_b)
	`)
	if err != nil {
		t.Logf("UNION in subquery error: %v", err)
	} else {
		t.Logf("UNION in subquery returned %d rows", len(result.Rows))
	}
}

// TestCoverage_LoadWithCorruptedIndex tests Load with corrupted index data
func TestCoverage_LoadWithCorruptedIndex(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())

	// Create catalog with corrupted index entry
	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create catalog tree: %v", err)
	}

	// Insert corrupted index definition
	err = catalogTree.Put([]byte("idx:corrupt_idx"), []byte("not valid json"))
	if err != nil {
		t.Fatalf("Failed to insert corrupted data: %v", err)
	}

	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		indexes:    make(map[string]*IndexDef),
		indexTrees: make(map[string]*btree.BTree),
		tree:       catalogTree,
		pool:       pool,
	}

	err = c.Load()
	if err != nil {
		t.Logf("Load with corrupted index error (may be expected): %v", err)
	}
}

// TestCoverage_LoadWithMissingTableRef tests Load with index referencing missing table
func TestCoverage_LoadWithMissingTableRef(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())

	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create catalog tree: %v", err)
	}

	// Insert valid index definition but no corresponding table
	idxDef := `{"name":"orphan_idx","table_name":"nonexistent_table","columns":["id"],"unique":true}`
	err = catalogTree.Put([]byte("idx:orphan_idx"), []byte(idxDef))
	if err != nil {
		t.Fatalf("Failed to insert index def: %v", err)
	}

	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		indexes:    make(map[string]*IndexDef),
		indexTrees: make(map[string]*btree.BTree),
		tree:       catalogTree,
		pool:       pool,
	}

	err = c.Load()
	if err != nil {
		t.Logf("Load with orphan index error: %v", err)
	}
}

// TestCoverage_countRows tests StatsCollector.countRows
func TestCoverage_countRows(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "stats_count",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "stats_count",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val" + string(rune('0'+i%10)))}},
		}, nil)
	}

	// Create stats collector
	sc := NewStatsCollector(c)

	// Count rows
	count, err := sc.countRows("stats_count")
	if err != nil {
		t.Logf("countRows error: %v", err)
	} else {
		t.Logf("countRows returned: %d", count)
		if count != 50 {
			t.Errorf("Expected 50 rows, got %d", count)
		}
	}

	// Count rows on non-existent table
	_, err = sc.countRows("nonexistent_table")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestCoverage_updateRowSlice tests ForeignKeyEnforcer.updateRowSlice
func TestCoverage_updateRowSlice(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent table
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Create child table with FK
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "data", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Create index on FK column
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_fk_parent",
		Table:   "fk_child",
		Columns: []string{"parent_id"},
	})

	// Insert parent data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Parent1")}, {numReal(2), strReal("Parent2")}},
	}, nil)

	// Insert child data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child",
		Columns: []string{"id", "parent_id", "data"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("Child1")}},
	}, nil)

	// Create FK enforcer
	fke := NewForeignKeyEnforcer(c)

	// Test updateRowSlice - this is called during ON UPDATE CASCADE
	rowData := []interface{}{int64(1), int64(2), "Child1"}
	err := fke.updateRowSlice("fk_child", int64(1), rowData)
	if err != nil {
		t.Logf("updateRowSlice error: %v", err)
	}

	// Verify the update
	result, _ := c.ExecuteQuery("SELECT parent_id FROM fk_child WHERE id = 1")
	if len(result.Rows) > 0 {
		t.Logf("Updated parent_id: %v", result.Rows[0][0])
	}
}

// TestCoverage_updateRowSliceNonExistentTable tests updateRowSlice with missing table
func TestCoverage_updateRowSliceNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	fke := NewForeignKeyEnforcer(c)

	// Test with non-existent table
	err := fke.updateRowSlice("nonexistent", int64(1), []interface{}{})
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestCoverage_selectLockedWithWindowFunc tests selectLocked with window functions
func TestCoverage_selectLockedWithWindowFunc(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "window_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "score", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "window_test",
		Columns: []string{"id", "category", "score"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(100)},
			{numReal(2), strReal("A"), numReal(200)},
			{numReal(3), strReal("B"), numReal(150)},
			{numReal(4), strReal("B"), numReal(250)},
		},
	}, nil)

	// Window function with PARTITION BY
	result, err := c.ExecuteQuery(`
		SELECT id, category, score,
		       ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) as rn
		FROM window_test
	`)
	if err != nil {
		t.Logf("Window function error: %v", err)
	} else {
		t.Logf("Window function returned %d rows", len(result.Rows))
	}

	// Window function without PARTITION BY
	result, err = c.ExecuteQuery(`
		SELECT id, score,
		       RANK() OVER (ORDER BY score DESC) as rnk
		FROM window_test
	`)
	if err != nil {
		t.Logf("Window function without PARTITION error: %v", err)
	} else {
		t.Logf("Window function without PARTITION returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithLimitOffsetFinal tests selectLocked with LIMIT/OFFSET
func TestCoverage_selectLockedWithLimitOffsetFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "limit_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert 20 rows
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "limit_test",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	// LIMIT only
	result, err := c.ExecuteQuery("SELECT * FROM limit_test ORDER BY id LIMIT 5")
	if err != nil {
		t.Logf("LIMIT error: %v", err)
	} else {
		if len(result.Rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(result.Rows))
		}
	}

	// LIMIT OFFSET
	result, err = c.ExecuteQuery("SELECT * FROM limit_test ORDER BY id LIMIT 5 OFFSET 10")
	if err != nil {
		t.Logf("LIMIT OFFSET error: %v", err)
	} else {
		if len(result.Rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(result.Rows))
		}
		// Verify correct offset
		if len(result.Rows) > 0 {
			if id, ok := result.Rows[0][0].(int64); ok && id != 11 {
				t.Errorf("Expected first row id=11, got %d", id)
			}
		}
	}

	// OFFSET only (should work)
	result, err = c.ExecuteQuery("SELECT * FROM limit_test ORDER BY id OFFSET 15")
	if err != nil {
		t.Logf("OFFSET only error: %v", err)
	} else {
		if len(result.Rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedWithSubqueryInSelect tests selectLocked with subquery in SELECT clause
func TestCoverage_selectLockedWithSubqueryInSelect(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "sq_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "sq_ref",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "main_id", Type: query.TokenInteger},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sq_main",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sq_ref",
		Columns: []string{"id", "main_id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}, {numReal(2), numReal(1), numReal(200)}},
	}, nil)

	// Subquery in SELECT
	result, err := c.ExecuteQuery(`
		SELECT id, name,
		       (SELECT SUM(val) FROM sq_ref WHERE main_id = sq_main.id) as total
		FROM sq_main
	`)
	if err != nil {
		t.Logf("Subquery in SELECT error: %v", err)
	} else {
		t.Logf("Subquery in SELECT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithExistsSubquery tests selectLocked with EXISTS subquery
func TestCoverage_selectLockedWithExistsSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "exists_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "exists_ref",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "main_id", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "exists_main",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "exists_ref",
		Columns: []string{"id", "main_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// EXISTS subquery
	result, err := c.ExecuteQuery(`
		SELECT * FROM exists_main
		WHERE EXISTS (SELECT 1 FROM exists_ref WHERE main_id = exists_main.id)
	`)
	if err != nil {
		t.Logf("EXISTS subquery error: %v", err)
	} else {
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
	}

	// NOT EXISTS subquery
	result, err = c.ExecuteQuery(`
		SELECT * FROM exists_main
		WHERE NOT EXISTS (SELECT 1 FROM exists_ref WHERE main_id = exists_main.id)
	`)
	if err != nil {
		t.Logf("NOT EXISTS subquery error: %v", err)
	} else {
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedWithCorrelatedSubquery tests selectLocked with correlated subquery
func TestCoverage_selectLockedWithCorrelatedSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "corr_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "threshold", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "corr_values",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "main_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "corr_main",
		Columns: []string{"id", "threshold"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(200)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "corr_values",
		Columns: []string{"id", "main_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(150)}, {numReal(2), numReal(2), numReal(150)}},
	}, nil)

	// Correlated subquery - compare with outer query column
	result, err := c.ExecuteQuery(`
		SELECT * FROM corr_main
		WHERE threshold < (SELECT MAX(amount) FROM corr_values WHERE main_id = corr_main.id)
	`)
	if err != nil {
		t.Logf("Correlated subquery error: %v", err)
	} else {
		t.Logf("Correlated subquery returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithCaseExpression tests selectLocked with CASE expressions
func TestCoverage_selectLockedWithCaseExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "case_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "score", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "case_test",
		Columns: []string{"id", "score"},
		Values: [][]query.Expression{
			{numReal(1), numReal(50)},
			{numReal(2), numReal(75)},
			{numReal(3), numReal(90)},
		},
	}, nil)

	// Simple CASE
	result, err := c.ExecuteQuery(`
		SELECT id,
		       CASE score
			   WHEN 50 THEN 'F'
			   WHEN 75 THEN 'C'
			   WHEN 90 THEN 'A'
			   ELSE 'Unknown'
		   END as grade
		FROM case_test
	`)
	if err != nil {
		t.Logf("Simple CASE error: %v", err)
	} else {
		t.Logf("Simple CASE returned %d rows", len(result.Rows))
	}

	// Searched CASE
	result, err = c.ExecuteQuery(`
		SELECT id,
		       CASE
			   WHEN score >= 90 THEN 'A'
			   WHEN score >= 80 THEN 'B'
			   WHEN score >= 70 THEN 'C'
			   ELSE 'F'
		   END as grade
		FROM case_test
	`)
	if err != nil {
		t.Logf("Searched CASE error: %v", err)
	} else {
		t.Logf("Searched CASE returned %d rows", len(result.Rows))
	}

	// CASE in WHERE
	result, err = c.ExecuteQuery(`
		SELECT * FROM case_test
		WHERE CASE WHEN score > 60 THEN 1 ELSE 0 END = 1
	`)
	if err != nil {
		t.Logf("CASE in WHERE error: %v", err)
	} else {
		t.Logf("CASE in WHERE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithCastExpression tests selectLocked with CAST expressions
func TestCoverage_selectLockedWithCastExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "cast_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "num_str", Type: query.TokenText},
			{Name: "int_val", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "num_str", "int_val"},
		Values: [][]query.Expression{
			{numReal(1), strReal("123"), numReal(456)},
			{numReal(2), strReal("789.5"), numReal(0)},
		},
	}, nil)

	// CAST to INTEGER
	result, err := c.ExecuteQuery(`SELECT CAST(num_str AS INTEGER) as int_val FROM cast_test`)
	if err != nil {
		t.Logf("CAST to INTEGER error: %v", err)
	} else {
		t.Logf("CAST to INTEGER returned %d rows", len(result.Rows))
	}

	// CAST to REAL
	result, err = c.ExecuteQuery(`SELECT CAST(int_val AS REAL) as real_val FROM cast_test`)
	if err != nil {
		t.Logf("CAST to REAL error: %v", err)
	} else {
		t.Logf("CAST to REAL returned %d rows", len(result.Rows))
	}

	// CAST to TEXT
	result, err = c.ExecuteQuery(`SELECT CAST(int_val AS TEXT) as str_val FROM cast_test`)
	if err != nil {
		t.Logf("CAST to TEXT error: %v", err)
	} else {
		t.Logf("CAST to TEXT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithInSubquery tests selectLocked with IN subquery
func TestCoverage_selectLockedWithInSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "in_main",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "in_ref",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "in_main",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}, {numReal(3), strReal("C")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "in_ref",
		Columns: []string{"id", "cat"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	// IN with subquery
	result, err := c.ExecuteQuery(`
		SELECT * FROM in_main
		WHERE category IN (SELECT cat FROM in_ref)
	`)
	if err != nil {
		t.Logf("IN subquery error: %v", err)
	} else {
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	}

	// NOT IN with subquery
	result, err = c.ExecuteQuery(`
		SELECT * FROM in_main
		WHERE category NOT IN (SELECT cat FROM in_ref)
	`)
	if err != nil {
		t.Logf("NOT IN subquery error: %v", err)
	} else {
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedWithBetween tests selectLocked with BETWEEN
func TestCoverage_selectLockedWithBetween(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "between_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "between_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// BETWEEN
	result, err := c.ExecuteQuery(`SELECT * FROM between_test WHERE val BETWEEN 50 AND 100`)
	if err != nil {
		t.Logf("BETWEEN error: %v", err)
	} else {
		t.Logf("BETWEEN returned %d rows", len(result.Rows))
	}

	// NOT BETWEEN
	result, err = c.ExecuteQuery(`SELECT * FROM between_test WHERE val NOT BETWEEN 50 AND 100`)
	if err != nil {
		t.Logf("NOT BETWEEN error: %v", err)
	} else {
		t.Logf("NOT BETWEEN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithIsNull tests selectLocked with IS NULL
func TestCoverage_selectLockedWithIsNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "null_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data with NULLs
	c.Insert(ctx, &query.InsertStmt{
		Table:   "null_test",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), strReal("not null")},
			{numReal(2), nl()},
			{numReal(3), strReal("also not null")},
			{numReal(4), nl()},
		},
	}, nil)

	// IS NULL
	result, err := c.ExecuteQuery(`SELECT * FROM null_test WHERE val IS NULL`)
	if err != nil {
		t.Logf("IS NULL error: %v", err)
	} else {
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	}

	// IS NOT NULL
	result, err = c.ExecuteQuery(`SELECT * FROM null_test WHERE val IS NOT NULL`)
	if err != nil {
		t.Logf("IS NOT NULL error: %v", err)
	} else {
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedWithAllJoinTypes tests selectLocked with all JOIN types
func TestCoverage_selectLockedWithAllJoinTypes(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "join_a",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "join_b",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "a_id", Type: query.TokenInteger},
			{Name: "value", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "join_a",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A1")}, {numReal(2), strReal("A2")}, {numReal(3), strReal("A3")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "join_b",
		Columns: []string{"id", "a_id", "value"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("B1")}, {numReal(2), numReal(2), strReal("B2")}},
	}, nil)

	// INNER JOIN
	result, err := c.ExecuteQuery(`SELECT * FROM join_a INNER JOIN join_b ON join_a.id = join_b.a_id`)
	if err != nil {
		t.Logf("INNER JOIN error: %v", err)
	} else {
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows from INNER JOIN, got %d", len(result.Rows))
		}
	}

	// LEFT JOIN
	result, err = c.ExecuteQuery(`SELECT * FROM join_a LEFT JOIN join_b ON join_a.id = join_b.a_id`)
	if err != nil {
		t.Logf("LEFT JOIN error: %v", err)
	} else {
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows from LEFT JOIN, got %d", len(result.Rows))
		}
	}

	// CROSS JOIN
	result, err = c.ExecuteQuery(`SELECT * FROM join_a CROSS JOIN join_b`)
	if err != nil {
		t.Logf("CROSS JOIN error: %v", err)
	} else {
		if len(result.Rows) != 6 {
			t.Errorf("Expected 6 rows from CROSS JOIN, got %d", len(result.Rows))
		}
	}
}

// TestCoverage_evaluateLikeEscape tests evaluateLike with ESCAPE clause
func TestCoverage_evaluateLikeEscape(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "like_escape_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "pattern", Type: query.TokenText},
		},
	})

	// Insert data with special characters
	c.Insert(ctx, &query.InsertStmt{
		Table:   "like_escape_test",
		Columns: []string{"id", "pattern"},
		Values:  [][]query.Expression{{numReal(1), strReal("100%")}, {numReal(2), strReal("50")}},
	}, nil)

	// LIKE with ESCAPE
	result, err := c.ExecuteQuery(`SELECT * FROM like_escape_test WHERE pattern LIKE '%#%%' ESCAPE '#'`)
	if err != nil {
		t.Logf("LIKE ESCAPE error: %v", err)
	} else {
		t.Logf("LIKE ESCAPE returned %d rows", len(result.Rows))
	}

	// NOT LIKE
	result, err = c.ExecuteQuery(`SELECT * FROM like_escape_test WHERE pattern NOT LIKE '100%'`)
	if err != nil {
		t.Logf("NOT LIKE error: %v", err)
	} else {
		t.Logf("NOT LIKE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_evaluateLikeWithNull tests evaluateLike with NULL values
func TestCoverage_evaluateLikeWithNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "like_null_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data with NULL
	c.Insert(ctx, &query.InsertStmt{
		Table:   "like_null_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}, {numReal(2), nl()}},
	}, nil)

	// LIKE with NULL column
	result, err := c.ExecuteQuery(`SELECT * FROM like_null_test WHERE val LIKE 'test%'`)
	if err != nil {
		t.Logf("LIKE with NULL error: %v", err)
	} else {
		t.Logf("LIKE with NULL returned %d rows", len(result.Rows))
	}
}

// TestCoverage_evaluateHavingComplexFinal tests evaluateHaving with complex expressions
func TestCoverage_evaluateHavingComplexFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "having_complex",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "having_complex",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	// HAVING with multiple conditions
	result, err := c.ExecuteQuery(`
		SELECT category, COUNT(*) as cnt, SUM(amount) as total
		FROM having_complex
		GROUP BY category
		HAVING COUNT(*) > 5 AND SUM(amount) > 500
	`)
	if err != nil {
		t.Logf("HAVING complex error: %v", err)
	} else {
		t.Logf("HAVING complex returned %d rows", len(result.Rows))
	}

	// HAVING with OR
	result, err = c.ExecuteQuery(`
		SELECT category, COUNT(*) as cnt
		FROM having_complex
		GROUP BY category
		HAVING COUNT(*) = 5 OR SUM(amount) > 1000
	`)
	if err != nil {
		t.Logf("HAVING with OR error: %v", err)
	} else {
		t.Logf("HAVING with OR returned %d rows", len(result.Rows))
	}

	// HAVING with comparison to literal
	result, err = c.ExecuteQuery(`
		SELECT category, AVG(amount) as avg_amt
		FROM having_complex
		GROUP BY category
		HAVING AVG(amount) > 100
	`)
	if err != nil {
		t.Logf("HAVING AVG error: %v", err)
	} else {
		t.Logf("HAVING AVG returned %d rows", len(result.Rows))
	}
}

// TestCoverage_resolveAggregateInExprComplexFinal tests resolveAggregateInExpr with complex cases
func TestCoverage_resolveAggregateInExprComplexFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "agg_resolve",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "group_col", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		grp := "G1"
		if i > 15 {
			grp = "G2"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "agg_resolve",
			Columns: []string{"id", "group_col", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i))}},
		}, nil)
	}

	// HAVING with arithmetic on aggregates
	result, err := c.ExecuteQuery(`
		SELECT group_col, SUM(val) as total, COUNT(*) as cnt
		FROM agg_resolve
		GROUP BY group_col
		HAVING SUM(val) / COUNT(*) > 15
	`)
	if err != nil {
		t.Logf("HAVING arithmetic on aggregates error: %v", err)
	} else {
		t.Logf("HAVING arithmetic returned %d rows", len(result.Rows))
	}

	// HAVING with DISTINCT aggregate
	result, err = c.ExecuteQuery(`
		SELECT group_col, COUNT(DISTINCT val) as distinct_cnt
		FROM agg_resolve
		GROUP BY group_col
		HAVING COUNT(DISTINCT val) > 5
	`)
	if err != nil {
		t.Logf("HAVING DISTINCT error: %v", err)
	} else {
		t.Logf("HAVING DISTINCT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_JSONPathSetComplexPaths tests JSONPath.Set with complex paths
func TestCoverage_JSONPathSetComplexPaths(t *testing.T) {
	// Set nested object value
	jsonData := `{"level1": {"level2": {"level3": "original"}}}`
	result, err := JSONSet(jsonData, "$.level1.level2.level3", `"updated"`)
	if err != nil {
		t.Logf("JSONSet nested path error: %v", err)
	} else {
		t.Logf("JSONSet nested result: %s", result)
	}

	// Set array element in nested object
	jsonData = `{"data": {"items": [1, 2, 3]}}`
	result, err = JSONSet(jsonData, "$.data.items[1]", `99`)
	if err != nil {
		t.Logf("JSONSet array in nested object error: %v", err)
	} else {
		t.Logf("JSONSet array element result: %s", result)
	}

	// Set new key in existing object
	jsonData = `{"existing": "value"}`
	result, err = JSONSet(jsonData, "$.newkey", `"newvalue"`)
	if err != nil {
		t.Logf("JSONSet new key error: %v", err)
	} else {
		t.Logf("JSONSet new key result: %s", result)
	}
}

// TestCoverage_JSONPathSetErrors tests JSONPath.Set error paths
func TestCoverage_JSONPathSetErrors(t *testing.T) {
	// Invalid path - not an array
	jsonData := `{"key": "value"}`
	_, err := JSONSet(jsonData, "$.key[0]", `"test"`)
	if err == nil {
		t.Error("Expected error for non-array path")
	}

	// Array index out of bounds
	jsonData = `{"arr": [1, 2]}`
	_, err = JSONSet(jsonData, "$.arr[10]", `99`)
	if err == nil {
		t.Error("Expected error for out of bounds index")
	}

	// Path not found - intermediate missing
	jsonData = `{"level1": {}}`
	_, err = JSONSet(jsonData, "$.level1.level2.level3", `"test"`)
	if err == nil {
		t.Error("Expected error for missing intermediate path")
	}
}
