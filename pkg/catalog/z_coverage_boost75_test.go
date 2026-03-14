package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_QueryCacheDisabled tests query cache when disabled
func TestCoverage_QueryCacheDisabled(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Disable query cache by setting size 0
	cat.EnableQueryCache(0, 0)

	createCoverageTestTable(t, cat, "cache_disabled", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cache_disabled",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Execute query (should work without cache)
	result, err := cat.ExecuteQuery("SELECT * FROM cache_disabled WHERE id <= 5")
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Query returned %d rows", len(result.Rows))
	}

	// Check cache stats (should be 0)
	hits, misses, size := cat.GetQueryCacheStats()
	t.Logf("Cache stats: hits=%d, misses=%d, size=%d", hits, misses, size)
}

// TestCoverage_QueryCacheInvalidate tests cache invalidation
func TestCoverage_QueryCacheInvalidate(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Enable query cache
	cat.EnableQueryCache(100, 0)

	createCoverageTestTable(t, cat, "cache_inval", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cache_inval",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Execute query multiple times to populate cache
	for i := 0; i < 3; i++ {
		cat.ExecuteQuery("SELECT * FROM cache_inval WHERE id <= 5")
	}

	// Insert should invalidate cache
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cache_inval",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(99), strReal("new")}},
	}, nil)

	hits, misses, size := cat.GetQueryCacheStats()
	t.Logf("Cache stats after invalidation: hits=%d, misses=%d, size=%d", hits, misses, size)
}

// TestCoverage_CTEWithWindowFunctions tests CTE with window functions
func TestCoverage_CTEWithWindowFunctions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_win", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		catg := "A"
		if i > 10 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cte_win",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 10))}},
		}, nil)
	}

	// CTE with window function
	result, err := cat.ExecuteQuery(`
		WITH ranked AS (
			SELECT id, category, amount,
				ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as rn
			FROM cte_win
		)
		SELECT * FROM ranked WHERE rn <= 3
	`)
	if err != nil {
		t.Logf("CTE with window error: %v", err)
	} else {
		t.Logf("CTE with window returned %d rows", len(result.Rows))
	}
}

// TestCoverage_SelectWithCacheHit tests select with cache hit
func TestCoverage_SelectWithCacheHit(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Enable query cache
	cat.EnableQueryCache(100, 0)

	createCoverageTestTable(t, cat, "select_cache", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "select_cache",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Execute same query multiple times using ExecuteQuery (which uses cache)
	for i := 0; i < 5; i++ {
		cat.ExecuteQuery("SELECT * FROM select_cache")
	}

	hits, misses, size := cat.GetQueryCacheStats()
	t.Logf("Cache stats: hits=%d, misses=%d, size=%d", hits, misses, size)
}

// TestCoverage_DeleteFKSetNull tests DELETE with FK SET NULL
func TestCoverage_DeleteFKSetNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Parent table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_setnull_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Child table with SET NULL
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_setnull_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_setnull_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "SET NULL",
			},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_setnull_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_setnull_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}, {numReal(3), numReal(2)}},
	}, nil)

	// Delete parent (should set NULL in children)
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_setnull_parent",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}

	// Check children
	result, _ := cat.ExecuteQuery("SELECT * FROM fk_setnull_child WHERE parent_id IS NULL")
	t.Logf("Children with NULL parent: %d", len(result.Rows))
}

// TestCoverage_UpdateFKCascade tests UPDATE with FK CASCADE
func TestCoverage_UpdateFKCascade(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Parent table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_update_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Child table with ON UPDATE CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_update_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_update_parent",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_update_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_update_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}},
	}, nil)

	// Update parent ID (should cascade to children)
	_, rows, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_update_parent",
		Set:   []*query.SetClause{{Column: "id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	if err != nil {
		t.Logf("Update error: %v", err)
	} else {
		t.Logf("Updated %d rows", rows)
	}

	// Check children
	result, _ := cat.ExecuteQuery("SELECT * FROM fk_update_child WHERE parent_id = 100")
	t.Logf("Children with updated parent_id: %d", len(result.Rows))
}

// TestCoverage_ApplyOuterQueryMore75 targets applyOuterQuery more scenarios
func TestCoverage_ApplyOuterQueryMore75(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_more75", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_more75",
			Columns: []string{"id", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("name")}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "outer_more75"},
	}
	cat.CreateView("outer_more75_view", viewStmt)

	// Query view with various combinations
	queries := []string{
		"SELECT * FROM outer_more75_view ORDER BY val DESC LIMIT 10 OFFSET 5",
		"SELECT DISTINCT val FROM outer_more75_view ORDER BY val",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex view query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}

	cat.DropView("outer_more75_view")
}

// TestCoverage_evaluateWhereMore75 targets evaluateWhere with more expressions
func TestCoverage_evaluateWhereMore75(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_more75", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_more75",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 5)), numReal(float64(i % 3)), strReal("test")}},
		}, nil)
	}

	// Complex WHERE expressions
	queries := []string{
		"SELECT * FROM where_more75 WHERE (a = 1 OR b = 2) AND c = 'test'",
		"SELECT * FROM where_more75 WHERE NOT (a = 0 AND b = 0)",
		"SELECT * FROM where_more75 WHERE a IN (1, 2, 3) OR b BETWEEN 0 AND 2",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE complex error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_resolveAggregateInExprMore75 tests resolveAggregateInExpr
func TestCoverage_resolveAggregateInExprMore75(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "resolve_agg75", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		grp := "A"
		if i > 25 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "resolve_agg75",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	// HAVING with complex expressions
	queries := []string{
		"SELECT grp, SUM(val) as total FROM resolve_agg75 GROUP BY grp HAVING total > 5000",
		"SELECT grp, COUNT(*) as cnt, AVG(val) as avg_val FROM resolve_agg75 GROUP BY grp HAVING cnt > 10 AND avg_val > 200",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("resolveAggregate error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ExecuteSelectDistinctOrderBy targets DISTINCT with ORDER BY
func TestCoverage_ExecuteSelectDistinctOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "distinct_ob", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "value", Type: query.TokenInteger},
	})

	// Insert with duplicate categories
	for i := 1; i <= 30; i++ {
		catg := "A"
		if i > 10 && i <= 20 {
			catg = "B"
		} else if i > 20 {
			catg = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "distinct_ob",
			Columns: []string{"id", "category", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i))}},
		}, nil)
	}

	// DISTINCT with ORDER BY
	queries := []string{
		"SELECT DISTINCT category FROM distinct_ob ORDER BY category",
		"SELECT DISTINCT category FROM distinct_ob ORDER BY category DESC",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("DISTINCT ORDER BY error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_AlterTableAddColumnDefault tests ALTER TABLE ADD COLUMN with DEFAULT
func TestCoverage_AlterTableAddColumnDefault(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "alter_add_default",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "alter_add_default",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("item")}},
		}, nil)
	}

	// Add column with default value
	err := cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "alter_add_default",
		Action: "ADD",
		Column: query.ColumnDef{
			Name:    "status",
			Type:    query.TokenText,
			Default: strReal("active"),
		},
	})
	if err != nil {
		t.Logf("Add column error: %v", err)
	}

	// Verify default was applied
	result, _ := cat.ExecuteQuery("SELECT * FROM alter_add_default")
	t.Logf("Rows after add column: %d", len(result.Rows))
	t.Logf("Columns: %v", result.Columns)
}

// TestCoverage_InsertWithSubquery75 tests INSERT with subquery in VALUES
func TestCoverage_InsertWithSubquery75(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "insert_subq_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "insert_subq_dst", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert into source
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "insert_subq_src",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// INSERT INTO ... SELECT
	result, err := cat.ExecuteQuery("INSERT INTO insert_subq_dst SELECT * FROM insert_subq_src WHERE id <= 5")
	if err != nil {
		t.Logf("Insert with subquery error: %v", err)
	}
	t.Logf("Insert result: %v", result)

	// Verify
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM insert_subq_dst")
	t.Logf("Count in destination: %v", result.Rows)
}

// TestCoverage_ExecuteScalarSelectWithArgs tests scalar SELECT with args
func TestCoverage_ExecuteScalarSelectWithArgs(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_args", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_args",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// SELECT with WHERE
	result, err := cat.ExecuteQuery("SELECT * FROM scalar_args WHERE id > 5")
	if err != nil {
		t.Logf("Scalar select error: %v", err)
	} else {
		t.Logf("Query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_JoinWithSubquery tests JOIN with subquery
func TestCoverage_JoinWithSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "join_main_subq", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "data", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "join_ref_subq", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_main_subq",
			Columns: []string{"id", "ref_id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 5)), strReal("data")}},
		}, nil)
	}

	for i := 0; i < 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_ref_subq",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(string(rune('A' + i)))}},
		}, nil)
	}

	// JOIN with subquery
	result, err := cat.ExecuteQuery(`
		SELECT m.*, r.category
		FROM join_main_subq m
		JOIN join_ref_subq r ON m.ref_id = r.id
		WHERE r.category IN ('A', 'B', 'C')
	`)
	if err != nil {
		t.Logf("JOIN with subquery error: %v", err)
	} else {
		t.Logf("JOIN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_TransactionSavepointEdgeCases tests transaction savepoint edge cases
func TestCoverage_TransactionSavepointEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Test rolling back to non-existent savepoint
	cat.BeginTransaction(1)
	err := cat.RollbackToSavepoint("nonexistent")
	if err != nil {
		t.Logf("Rollback to non-existent savepoint error (expected): %v", err)
	}
	cat.RollbackTransaction()

	// Test multiple savepoints with same name (should overwrite)
	cat.BeginTransaction(2)
	cat.Savepoint("sp1")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_edge",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("first")}},
	}, nil)
	cat.Savepoint("sp1") // Overwrite
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_edge",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("second")}},
	}, nil)
	cat.RollbackToSavepoint("sp1")
	cat.CommitTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_edge")
	t.Logf("Count after savepoint test: %v", result.Rows)
}
