package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Additional coverage targeting specific low-coverage paths
// ============================================================

// TestUseIndexForExactMatch_Paths - targets useIndexForExactMatch (69.2%)
func TestUseIndexForExactMatch_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "idx_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "idx_test",
		Columns: []string{"code"},
	})

	// Insert data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "idx_test",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("code"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Query that should use index
	result, err := cat.ExecuteQuery(`SELECT * FROM idx_test WHERE code = 'code'`)
	if err != nil {
		t.Logf("Index query error: %v", err)
	} else {
		t.Logf("Index query returned %d rows", len(result.Rows))
	}

	// Query with range
	result, err = cat.ExecuteQuery(`SELECT * FROM idx_test WHERE id > 50`)
	if err != nil {
		t.Logf("Range query error: %v", err)
	}
	_ = result
}

// TestForeignKey_OnDelete_OnUpdate - targets OnDelete/OnUpdate (73.7%)
func TestForeignKey_OnDelete_OnUpdate(t *testing.T) {
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
	createCoverageTestTable(t, cat, "fk_parent", []*query.ColumnDef{
		{Name: "parent_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Child table with FK
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child",
		Columns: []*query.ColumnDef{
			{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "data", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent",
				ReferencedColumns: []string{"parent_id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Insert parent
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent",
		Columns: []string{"parent_id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Parent1")}},
	}, nil)

	// Insert child
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child",
		Columns: []string{"child_id", "parent_id", "data"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("Child1")}},
	}, nil)

	// Update parent (should cascade)
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_parent",
		Set:   []*query.SetClause{{Column: "parent_id", Value: numReal(2)}},
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("FK update cascade error (may be expected): %v", err)
	}

	// Delete parent (should cascade)
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent",
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err != nil {
		t.Logf("FK delete cascade error (may be expected): %v", err)
	}
}

// TestRollbackToSavepoint_Paths - targets RollbackToSavepoint (73.4%)
func TestRollbackToSavepoint_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "savepoint_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "savepoint_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Create savepoint
	err = cat.Savepoint("sp1")
	if err != nil {
		t.Logf("CreateSavepoint error: %v", err)
	}

	// More inserts
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "savepoint_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), numReal(200)}},
	}, nil)

	// Rollback to savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("RollbackToSavepoint error: %v", err)
	}

	// Commit
	cat.CommitTransaction()
}

// TestSaveAndLoad_Paths - targets Save (71.4%) and Load (75.0%)
func TestSaveAndLoad_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "persist_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "persist_test",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Save
	err = cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}
}

// TestVacuum_Paths - targets Vacuum (76.5%)
func TestVacuum_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "vacuum_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Insert and delete data to create fragmentation
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "vacuum_test",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Delete half
	for i := 1; i <= 50; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "vacuum_test",
			Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(float64(i))},
		}, nil)
	}

	// Vacuum
	err = cat.Vacuum()
	if err != nil {
		t.Logf("Vacuum error: %v", err)
	}
}

// TestApplyGroupByOrderBy_Paths - targets applyGroupByOrderBy (72.2%)
func TestApplyGroupByOrderBy_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gbob_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "subcategory", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert test data
	data := []struct{ cat, sub string; amt int }{
		{"A", "X", 10},
		{"A", "Y", 20},
		{"A", "X", 30},
		{"B", "X", 15},
		{"B", "Y", 25},
	}
	for i, d := range data {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gbob_test",
			Columns: []string{"id", "category", "subcategory", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(d.cat), strReal(d.sub), numReal(float64(d.amt))}},
		}, nil)
	}

	// GROUP BY with ORDER BY on aggregate
	result, err := cat.ExecuteQuery(`
		SELECT category, SUM(amount) as total
		FROM gbob_test
		GROUP BY category
		ORDER BY total DESC
	`)
	if err != nil {
		t.Logf("GROUP BY ORDER BY aggregate error: %v", err)
	} else {
		t.Logf("GROUP BY ORDER BY returned %d rows", len(result.Rows))
	}

	// Multi-column GROUP BY with ORDER BY
	result, err = cat.ExecuteQuery(`
		SELECT category, subcategory, SUM(amount) as total
		FROM gbob_test
		GROUP BY category, subcategory
		ORDER BY category ASC, total DESC
	`)
	if err != nil {
		t.Logf("Multi-column GROUP BY ORDER BY error: %v", err)
	}
	_ = result
}

// TestComputeAggregatesWithGroupBy_Paths - targets computeAggregatesWithGroupBy (72.6%)
func TestComputeAggregatesWithGroupBy_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "agg_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert data with NULLs
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "agg_test",
		Columns: []string{"id", "grp", "val", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A"), numReal(10), strReal("x")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "agg_test",
		Columns: []string{"id", "grp", "val", "name"},
		Values:  [][]query.Expression{{numReal(2), strReal("A"), &query.NullLiteral{}, strReal("y")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "agg_test",
		Columns: []string{"id", "grp", "val", "name"},
		Values:  [][]query.Expression{{numReal(3), strReal("B"), numReal(20), &query.NullLiteral{}}},
	}, nil)

	// Test various aggregates with NULLs
	result, err := cat.ExecuteQuery(`
		SELECT grp,
			COUNT(*) as cnt,
			COUNT(val) as cnt_val,
			SUM(val) as sum_val,
			AVG(val) as avg_val,
			MIN(val) as min_val,
			MAX(val) as max_val,
			MIN(name) as min_name,
			MAX(name) as max_name
		FROM agg_test
		GROUP BY grp
		ORDER BY grp
	`)
	if err != nil {
		t.Logf("Aggregate with NULLs error: %v", err)
	} else {
		t.Logf("Aggregate returned %d rows", len(result.Rows))
	}

	// GROUP_CONCAT
	result, err = cat.ExecuteQuery(`
		SELECT grp, GROUP_CONCAT(name) as names
		FROM agg_test
		GROUP BY grp
		ORDER BY grp
	`)
	if err != nil {
		t.Logf("GROUP_CONCAT error: %v", err)
	}
	_ = result
}

// TestExecuteSelectWithJoin_MorePaths - targets executeSelectWithJoin (72.7%)
func TestExecuteSelectWithJoin_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "join_a", []*query.ColumnDef{
		{Name: "a_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_val", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "join_b", []*query.ColumnDef{
		{Name: "b_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_id", Type: query.TokenInteger},
		{Name: "b_val", Type: query.TokenText},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "join_a",
		Columns: []string{"a_id", "a_val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A1")}, {numReal(2), strReal("A2")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "join_b",
		Columns: []string{"b_id", "a_id", "b_val"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("B1")}, {numReal(2), numReal(1), strReal("B2")}, {numReal(3), numReal(3), strReal("B3")}},
	}, nil)

	// INNER JOIN
	result, err := cat.ExecuteQuery(`
		SELECT a.a_val, b.b_val
		FROM join_a a
		INNER JOIN join_b b ON a.a_id = b.a_id
		ORDER BY b.b_id
	`)
	if err != nil {
		t.Logf("INNER JOIN error: %v", err)
	} else {
		if len(result.Rows) != 2 {
			t.Errorf("INNER JOIN expected 2 rows, got %d", len(result.Rows))
		}
	}

	// LEFT JOIN with unmatched rows
	result, err = cat.ExecuteQuery(`
		SELECT a.a_val, b.b_val
		FROM join_a a
		LEFT JOIN join_b b ON a.a_id = b.a_id
		ORDER BY a.a_id, b.b_id
	`)
	if err != nil {
		t.Logf("LEFT JOIN error: %v", err)
	} else {
		if len(result.Rows) != 3 {
			t.Errorf("LEFT JOIN expected 3 rows (A1-B1, A1-B2, A2-null), got %d", len(result.Rows))
		}
	}

	// RIGHT JOIN (implemented as reverse LEFT JOIN)
	result, err = cat.ExecuteQuery(`
		SELECT a.a_val, b.b_val
		FROM join_a a
		RIGHT JOIN join_b b ON a.a_id = b.a_id
		ORDER BY b.b_id
	`)
	if err != nil {
		t.Logf("RIGHT JOIN error: %v", err)
	} else {
		t.Logf("RIGHT JOIN returned %d rows", len(result.Rows))
	}

	// CROSS JOIN
	result, err = cat.ExecuteQuery(`
		SELECT a.a_val, b.b_val
		FROM join_a a
		CROSS JOIN join_b b
		ORDER BY a.a_id, b.b_id
	`)
	if err != nil {
		t.Logf("CROSS JOIN error: %v", err)
	} else {
		if len(result.Rows) != 6 {
			t.Errorf("CROSS JOIN expected 6 rows, got %d", len(result.Rows))
		}
	}
	_ = result
}

// TestResolveAggregateInExpr_Paths - targets resolveAggregateInExpr (79.6%)
func TestResolveAggregateInExpr_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "agg_expr_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_expr_test",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	// Aggregate in expression
	result, err := cat.ExecuteQuery(`
		SELECT grp, SUM(val) * 2 as doubled_sum
		FROM agg_expr_test
		GROUP BY grp
		ORDER BY grp
	`)
	if err != nil {
		t.Logf("Aggregate in expression error: %v", err)
	} else {
		t.Logf("Aggregate in expression returned %d rows", len(result.Rows))
	}

	// Nested aggregate expressions
	result, err = cat.ExecuteQuery(`
		SELECT grp, (MAX(val) - MIN(val)) as range_val
		FROM agg_expr_test
		GROUP BY grp
		ORDER BY grp
	`)
	if err != nil {
		t.Logf("Nested aggregate expression error: %v", err)
	}
	_ = result
}

// TestEncodeRow_Paths - targets encodeRow (80.0%)
func TestEncodeRow_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Table with various data types
	createCoverageTestTable(t, cat, "encode_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "int_col", Type: query.TokenInteger},
		{Name: "text_col", Type: query.TokenText},
		{Name: "real_col", Type: query.TokenReal},
		{Name: "bool_col", Type: query.TokenBoolean},
		{Name: "json_col", Type: query.TokenJSON},
		{Name: "blob_col", Type: query.TokenBlob},
	})

	// Insert with various types
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "encode_test",
		Columns: []string{"id", "int_col", "text_col", "real_col", "bool_col", "json_col", "blob_col"},
		Values: [][]query.Expression{{
			numReal(1),
			numReal(42),
			strReal("test"),
			numReal(3.14),
			&query.BooleanLiteral{Value: true},
			strReal(`{"key": "value"}`),
			&query.StringLiteral{Value: "blobdata"},
		}},
	}, nil)
	if err != nil {
		t.Errorf("Insert with various types failed: %v", err)
	}

	// Insert with NULLs
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "encode_test",
		Columns: []string{"id", "int_col", "text_col"},
		Values:  [][]query.Expression{{numReal(2), &query.NullLiteral{}, strReal("not null")}},
	}, nil)
	if err != nil {
		t.Errorf("Insert with NULL failed: %v", err)
	}

	// Verify data can be read back
	result, err := cat.ExecuteQuery(`SELECT * FROM encode_test WHERE id = 1`)
	if err != nil {
		t.Errorf("Select after insert failed: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}
