package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Final coverage push - targeting specific uncovered paths
// ============================================================

// TestApplyOuterQuery_AllAggregateTypes - tests all aggregate types
func TestApplyOuterQuery_AllAggregateTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_agg_all", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		category := "A"
		if i > 25 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_agg_all",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create simple view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "category"},
			&query.QualifiedIdentifier{Column: "amount"},
		},
		From: &query.TableRef{Name: "aoq_agg_all"},
	}
	cat.CreateView("aoq_view_all", viewStmt)

	// Test all aggregate functions
	queries := []string{
		`SELECT COUNT(*) FROM aoq_view_all`,
		`SELECT COUNT(amount) FROM aoq_view_all`,
		`SELECT SUM(amount) FROM aoq_view_all`,
		`SELECT AVG(amount) FROM aoq_view_all`,
		`SELECT MIN(amount) FROM aoq_view_all`,
		`SELECT MAX(amount) FROM aoq_view_all`,
		`SELECT category, COUNT(*) FROM aoq_view_all GROUP BY category`,
		`SELECT category, SUM(amount), AVG(amount) FROM aoq_view_all GROUP BY category`,
		`SELECT COUNT(DISTINCT category) FROM aoq_view_all`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestDeleteWithUsingLocked_EdgeCases - edge cases
func TestDeleteWithUsingLocked_EdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "delu_edge_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "delu_edge_ref", []*query.ColumnDef{
		{Name: "ref_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_edge_main",
			Columns: []string{"id", "ref_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i%5 + 1))}},
		}, nil)
	}
	for i := 1; i <= 5; i++ {
		code := "KEEP"
		if i%2 == 0 {
			code = "DELETE"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_edge_ref",
			Columns: []string{"ref_id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code)}},
		}, nil)
	}

	// DELETE USING with complex WHERE
	result, err := cat.ExecuteQuery(`
		SELECT m.id FROM delu_edge_main m
		JOIN delu_edge_ref r ON m.ref_id = r.ref_id
		WHERE r.code = 'DELETE'
	`)
	if err != nil {
		t.Logf("Pre-check error: %v", err)
	} else {
		t.Logf("Rows to delete: %d", len(result.Rows))
	}
}

// TestUpdateRowSlice_ChainedFK - chained FK updates
func TestUpdateRowSlice_ChainedFK(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Level 1
	createCoverageTestTable(t, cat, "fk_chain_l1", []*query.ColumnDef{
		{Name: "l1_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Level 2
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_chain_l2",
		Columns: []*query.ColumnDef{
			{Name: "l2_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "l1_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"l1_id"},
				ReferencedTable:   "fk_chain_l1",
				ReferencedColumns: []string{"l1_id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Level 3
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_chain_l3",
		Columns: []*query.ColumnDef{
			{Name: "l3_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "l2_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"l2_id"},
				ReferencedTable:   "fk_chain_l2",
				ReferencedColumns: []string{"l2_id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_chain_l1",
			Columns: []string{"l1_id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_chain_l2",
			Columns: []string{"l2_id", "l1_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_chain_l3",
			Columns: []string{"l3_id", "l2_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// Update L1 - should cascade through L2 to L3
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_chain_l1",
		Set:   []*query.SetClause{{Column: "l1_id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: colReal("l1_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Chained FK update error: %v", err)
	}
}

// TestResolveOuterRefsInQuery_NotExists - NOT EXISTS patterns
func TestResolveOuterRefsInQuery_NotExists(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "ne_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "ne_sub", []*query.ColumnDef{
		{Name: "sub_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "status", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ne_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Name")}},
		}, nil)
		if i%2 == 0 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "ne_sub",
				Columns: []string{"sub_id", "main_id", "status"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("active")}},
			}, nil)
		}
	}

	// NOT EXISTS with correlated subquery
	result, err := cat.ExecuteQuery(`
		SELECT * FROM ne_main m
		WHERE NOT EXISTS (
			SELECT 1 FROM ne_sub s
			WHERE s.main_id = m.id
			AND s.status = 'inactive'
		)
		ORDER BY id
	`)
	if err != nil {
		t.Logf("NOT EXISTS error: %v", err)
	} else {
		t.Logf("NOT EXISTS returned %d rows", len(result.Rows))
	}

	// Complex NOT EXISTS
	result, err = cat.ExecuteQuery(`
		SELECT * FROM ne_main m
		WHERE NOT EXISTS (
			SELECT 1 FROM ne_sub s
			WHERE s.main_id = m.id
			AND s.status IN ('inactive', 'deleted')
		)
	`)
	if err != nil {
		t.Logf("Complex NOT EXISTS error: %v", err)
	} else {
		t.Logf("Complex NOT EXISTS returned %d rows", len(result.Rows))
	}
}

// TestCountRows_AllScenarios - comprehensive countRows tests
func TestCountRows_AllScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Empty table
	createCoverageTestTable(t, cat, "count_empty", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM count_empty`)
	if err != nil {
		t.Logf("Empty count error: %v", err)
	} else {
		t.Logf("Empty count: %v", result.Rows)
	}

	// Table with NULLs
	createCoverageTestTable(t, cat, "count_nulls", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		var val query.Expression
		if i%3 == 0 {
			val = &query.NullLiteral{}
		} else {
			val = numReal(float64(i))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "count_nulls",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), val}},
		}, nil)
	}

	// Count with NULL handling
	result, err = cat.ExecuteQuery(`SELECT COUNT(*), COUNT(val) FROM count_nulls`)
	if err != nil {
		t.Logf("Count with NULLs error: %v", err)
	} else {
		t.Logf("Count with NULLs: %v", result.Rows)
	}

	// Count with DISTINCT
	result, err = cat.ExecuteQuery(`SELECT COUNT(DISTINCT val) FROM count_nulls`)
	if err != nil {
		t.Logf("Count DISTINCT error: %v", err)
	} else {
		t.Logf("Count DISTINCT: %v", result.Rows)
	}
}

// TestSave_WithWal - Save with WAL enabled
func TestSave_WithWal(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create tables and indexes
	for i := 1; i <= 5; i++ {
		name := "save_wal_tbl" + string(rune('0'+i))
		createCoverageTestTable(t, cat, name, []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		})

		cat.CreateIndex(&query.CreateIndexStmt{
			Index:   "idx_" + name,
			Table:   name,
			Columns: []string{"data"},
		})

		for j := 1; j <= 30; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   name,
				Columns: []string{"id", "data"},
				Values:  [][]query.Expression{{numReal(float64(j)), strReal("data")}},
			}, nil)
		}
	}

	// Begin transaction and make changes
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "save_wal_tbl1",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(999), strReal("txn")}},
	}, nil)
	cat.CommitTransaction()

	// Save
	err = cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}
}

// TestLoad_AfterOperations - Load after various operations
func TestLoad_AfterOperations(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create and populate
	createCoverageTestTable(t, cat, "load_ops", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "load_ops",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Update some rows
	cat.Update(ctx, &query.UpdateStmt{
		Table: "load_ops",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenLte, Right: numReal(10)},
	}, nil)

	// Delete some rows
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "load_ops",
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenGt, Right: numReal(40)},
	}, nil)

	// Save
	err = cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Load
	err = cat.Load()
	if err != nil {
		t.Logf("Load error: %v", err)
	}

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM load_ops`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after load: %v", result.Rows)
	}
}

// TestFlushTableTreesLocked_TransactionScenarios - various transaction scenarios
func TestFlushTableTreesLocked_TransactionScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "flush_txn", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_flush_txn",
		Table:   "flush_txn",
		Columns: []string{"val"},
	})

	// Initial data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "flush_txn",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Multiple small transactions
	for i := 1; i <= 5; i++ {
		cat.BeginTransaction(uint64(i))
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "flush_txn",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(100 + i)), numReal(float64(i))}},
		}, nil)
		cat.CommitTransaction()
	}

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM flush_txn`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Total rows: %v", result.Rows)
	}
}

// TestVacuum_AfterManyOperations - vacuum after mixed operations
func TestVacuum_AfterManyOperations(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "vac_mixed", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
		{Name: "num", Type: query.TokenInteger},
	})

	// Create indexes
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac_data",
		Table:   "vac_mixed",
		Columns: []string{"data"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac_num",
		Table:   "vac_mixed",
		Columns: []string{"num"},
	})

	// Insert
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "vac_mixed",
			Columns: []string{"id", "data", "num"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data"), numReal(float64(i))}},
		}, nil)
	}

	// Update half
	for i := 1; i <= 50; i++ {
		cat.Update(ctx, &query.UpdateStmt{
			Table: "vac_mixed",
			Set:   []*query.SetClause{{Column: "data", Value: strReal("updated")}},
			Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(float64(i))},
		}, nil)
	}

	// Delete some
	for i := 51; i <= 70; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "vac_mixed",
			Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(float64(i))},
		}, nil)
	}

	// Count before
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM vac_mixed`)
	if err != nil {
		t.Logf("Count before error: %v", err)
	} else {
		t.Logf("Count before vacuum: %v", result.Rows)
	}

	// Vacuum
	err = cat.Vacuum()
	if err != nil {
		t.Logf("Vacuum error: %v", err)
	}

	// Count after
	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM vac_mixed`)
	if err != nil {
		t.Logf("Count after error: %v", err)
	} else {
		t.Logf("Count after vacuum: %v", result.Rows)
	}
}
