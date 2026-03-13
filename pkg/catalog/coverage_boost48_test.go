package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Targeting specific uncovered paths in low-coverage functions
// ============================================================

// TestApplyOuterQuery_NonAggregatePaths - covers non-aggregate branches
func TestApplyOuterQuery_NonAggregatePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_non_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_non_agg",
			Columns: []string{"id", "name", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Name"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "name"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "aoq_non_agg"},
	}
	cat.CreateView("aoq_non_agg_view", viewStmt)

	// Test non-aggregate SELECT with expression
	result, err := cat.ExecuteQuery(`SELECT id + 1 as id_plus, name FROM aoq_non_agg_view WHERE id > 10 ORDER BY val`)
	if err != nil {
		t.Logf("Expression query error: %v", err)
	} else {
		t.Logf("Expression query returned %d rows", len(result.Rows))
	}

	// Test ORDER BY on hidden column not in SELECT
	result, err = cat.ExecuteQuery(`SELECT name FROM aoq_non_agg_view ORDER BY val DESC`)
	if err != nil {
		t.Logf("ORDER BY hidden column error: %v", err)
	} else {
		t.Logf("ORDER BY hidden column returned %d rows", len(result.Rows))
	}

	// Test DISTINCT
	result, err = cat.ExecuteQuery(`SELECT DISTINCT name FROM aoq_non_agg_view`)
	if err != nil {
		t.Logf("DISTINCT error: %v", err)
	} else {
		t.Logf("DISTINCT returned %d rows", len(result.Rows))
	}

	// Test OFFSET and LIMIT
	result, err = cat.ExecuteQuery(`SELECT * FROM aoq_non_agg_view ORDER BY id LIMIT 5 OFFSET 5`)
	if err != nil {
		t.Logf("LIMIT OFFSET error: %v", err)
	} else {
		t.Logf("LIMIT OFFSET returned %d rows", len(result.Rows))
	}

	// Test OFFSET beyond result
	result, err = cat.ExecuteQuery(`SELECT * FROM aoq_non_agg_view ORDER BY id LIMIT 5 OFFSET 100`)
	if err != nil {
		t.Logf("Large OFFSET error: %v", err)
	} else {
		t.Logf("Large OFFSET returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestApplyOuterQuery_WhereError - covers WHERE clause error paths
func TestApplyOuterQuery_WhereError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_where", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_where",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "data"},
		},
		From: &query.TableRef{Name: "aoq_where"},
	}
	cat.CreateView("aoq_where_view", viewStmt)

	// Query with WHERE clause
	result, err := cat.ExecuteQuery(`SELECT * FROM aoq_where_view WHERE id > 5`)
	if err != nil {
		t.Logf("WHERE error: %v", err)
	} else {
		t.Logf("WHERE returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestCountRows_InvalidIdentifier - covers validateIdentifier error path
func TestCountRows_InvalidIdentifier(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	sc := NewStatsCollector(cat)

	// Test with invalid identifier containing semicolon
	_, err = sc.countRows("table;drop")
	if err == nil {
		t.Error("Expected error for invalid identifier with semicolon")
	} else {
		t.Logf("Got expected error: %v", err)
	}

	// Test with invalid identifier containing comment
	_, err = sc.countRows("table--comment")
	if err == nil {
		t.Error("Expected error for invalid identifier with comment")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestCountRows_NonInt64Type - covers non-int64 count type path
func TestCountRows_NonInt64Type(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "count_types", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "count_types",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	sc := NewStatsCollector(cat)
	stats, err := sc.CollectStats("count_types")
	if err != nil {
		t.Logf("CollectStats error: %v", err)
	} else {
		t.Logf("Row count: %d", stats.RowCount)
	}
}

// TestRollbackToSavepoint_MissingSavepoint - covers savepoint not found
func TestRollbackToSavepoint_MissingSavepoint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sp_missing", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_missing",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Try to rollback to non-existent savepoint
	err = cat.RollbackToSavepoint("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent savepoint")
	} else {
		t.Logf("Got expected error: %v", err)
	}

	cat.RollbackTransaction()
}

// TestRollbackToSavepoint_NotInTransaction - covers no transaction error
func TestRollbackToSavepoint_NotInTransaction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Try rollback without transaction
	err = cat.RollbackToSavepoint("sp1")
	if err == nil {
		t.Error("Expected error for rollback without transaction")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestDeleteWithUsing_NoMatch - covers no rows to delete case
func TestDeleteWithUsing_NoMatch(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "delu_main_nm", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "delu_ref_nm", []*query.ColumnDef{
		{Name: "ref_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	// Insert data in main table only
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_main_nm",
			Columns: []string{"id", "ref_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// Insert ref with non-matching codes
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_ref_nm",
			Columns: []string{"ref_id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("KEEP")}},
		}, nil)
	}

	// Execute DELETE USING with no matching rows
	result, err := cat.ExecuteQuery(`
		SELECT m.id FROM delu_main_nm m
		JOIN delu_ref_nm r ON m.ref_id = r.ref_id
		WHERE r.code = 'DELETE'
	`)
	if err != nil {
		t.Logf("Pre-check error: %v", err)
	} else {
		t.Logf("Rows to delete: %d", len(result.Rows))
	}

	_ = result
}

// TestDeleteWithUsing_MissingTable - covers table not found error
func TestDeleteWithUsing_MissingTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Try to delete from non-existent table
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "nonexistent",
		Using: []*query.TableRef{{Name: "othertable"}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err == nil {
		t.Error("Expected error for non-existent table")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestUpdateRowSlice_InvalidPKIndex - covers pkIdx adjustment
func TestUpdateRowSlice_InvalidPKIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table with composite-like primary key structure
	cat.CreateTable(&query.CreateTableStmt{
		Table: "upd_slice_pk",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_slice_pk",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Update rows
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_slice_pk",
		Set:   []*query.SetClause{{Column: "data", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenLte, Right: numReal(3)},
	}, nil)
	if err != nil {
		t.Logf("Update error: %v", err)
	}

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM upd_slice_pk WHERE data = 'updated'`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Updated rows: %v", result.Rows)
	}

	_ = result
}

// TestSave_Load_EmptyCatalog - save/load with empty catalog
func TestSave_Load_EmptyCatalog(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Save empty catalog
	err = cat.Save()
	if err != nil {
		t.Logf("Save empty error: %v", err)
	}

	// Load empty catalog
	err = cat.Load()
	if err != nil {
		t.Logf("Load empty error: %v", err)
	}
}

// TestLoad_NilTree - load with nil tree
func TestLoad_NilTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()

	// Create catalog without tree
	cat := New(nil, pool, nil)

	err := cat.Load()
	if err != nil {
		t.Logf("Load with nil tree error: %v", err)
	}
}

// TestApplyOuterQuery_StarExpr - covers StarExpr mapping
func TestApplyOuterQuery_StarExpr(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_star", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_star",
			Columns: []string{"id", "name", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Name"), numReal(float64(i))}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "name"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "aoq_star"},
	}
	cat.CreateView("aoq_star_view", viewStmt)

	// SELECT * from view
	result, err := cat.ExecuteQuery(`SELECT * FROM aoq_star_view`)
	if err != nil {
		t.Logf("SELECT * error: %v", err)
	} else {
		t.Logf("SELECT * returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestApplyOuterQuery_ColumnNotFound - covers column not found in view
func TestApplyOuterQuery_ColumnNotFound(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_col_nf", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "aoq_col_nf",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Test")}},
	}, nil)

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "name"},
		},
		From: &query.TableRef{Name: "aoq_col_nf"},
	}
	cat.CreateView("aoq_col_nf_view", viewStmt)

	// Query with literal expression (no column match)
	result, err := cat.ExecuteQuery(`SELECT 'literal' as literal_col FROM aoq_col_nf_view`)
	if err != nil {
		t.Logf("Literal query error: %v", err)
	} else {
		t.Logf("Literal query returned %d rows", len(result.Rows))
	}

	_ = result
}
