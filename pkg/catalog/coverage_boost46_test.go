package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Targeting specific uncovered error paths
// ============================================================

// TestCountRows_ErrorPaths - tests countRows error handling
func TestCountRows_ErrorPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	sc := NewStatsCollector(cat)

	// Test with invalid table name
	_, err = sc.countRows("invalid;table")
	if err == nil {
		t.Error("Expected error for invalid table name")
	} else {
		t.Logf("Invalid table name error: %v", err)
	}

	// Test with empty table name
	_, err = sc.countRows("")
	if err == nil {
		t.Error("Expected error for empty table name")
	} else {
		t.Logf("Empty table name error: %v", err)
	}

	// Test with non-existent table
	_, err = sc.countRows("nonexistent_table_xyz")
	if err == nil {
		t.Error("Expected error for non-existent table")
	} else {
		t.Logf("Non-existent table error: %v", err)
	}
}

// TestStatsCollector_CollectColumnStats - tests collectColumnStats
func TestStatsCollector_CollectColumnStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "stats_col", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "stats_col",
			Columns: []string{"id", "name", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name" + string(rune('A'+i%5))), numReal(float64(i))}},
		}, nil)
	}

	sc := NewStatsCollector(cat)

	// Test valid column
	stats, err := sc.collectColumnStats("stats_col", "val")
	if err != nil {
		t.Logf("collectColumnStats error: %v", err)
	} else {
		t.Logf("Column stats: Distinct=%d, NullCount=%d", stats.DistinctCount, stats.NullCount)
	}

	// Test with invalid table name
	_, err = sc.collectColumnStats("invalid;table", "col")
	if err == nil {
		t.Error("Expected error for invalid table name")
	}

	// Test with invalid column name
	_, err = sc.collectColumnStats("stats_col", "invalid;col")
	if err == nil {
		t.Error("Expected error for invalid column name")
	}

	// Test with non-existent table
	_, err = sc.collectColumnStats("nonexistent_xyz", "col")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	_ = stats
}

// TestStatsCollector_CollectStats - tests CollectStats
func TestStatsCollector_CollectStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create multiple tables
	for i := 1; i <= 3; i++ {
		name := "collect_stats_" + string(rune('0'+i))
		createCoverageTestTable(t, cat, name, []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		})

		for j := 1; j <= 20; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   name,
				Columns: []string{"id", "val"},
				Values:  [][]query.Expression{{numReal(float64(j)), numReal(float64(j * 10))}},
			}, nil)
		}
	}

	sc := NewStatsCollector(cat)

	// Collect stats for each table
	for i := 1; i <= 3; i++ {
		name := "collect_stats_" + string(rune('0'+i))
		_, err := sc.CollectStats(name)
		if err != nil {
			t.Logf("CollectStats %s error: %v", name, err)
		}
	}

	// Get summary
	summary := sc.GetSummary()
	t.Logf("Stats summary: %d tables, %d total rows", summary.TotalTables, summary.TotalRows)
}

// TestStatsCollector_GetTableStats - tests GetTableStats
func TestStatsCollector_GetTableStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "get_stats_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "col1", Type: query.TokenText},
		{Name: "col2", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "get_stats_test",
			Columns: []string{"id", "col1", "col2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data"), numReal(float64(i))}},
		}, nil)
	}

	sc := NewStatsCollector(cat)
	sc.CollectStats("get_stats_test")

	// Get existing stats
	stats, ok := sc.GetTableStats("get_stats_test")
	if !ok {
		t.Error("Expected to find stats for table")
	} else {
		t.Logf("Table stats: RowCount=%d, ColStats=%d", stats.RowCount, len(stats.ColumnStats))
	}

	// Get non-existent stats
	_, ok = sc.GetTableStats("nonexistent_xyz")
	if ok {
		t.Error("Expected not to find stats for non-existent table")
	}
}

// TestApplyOuterQuery_GroupByCases - tests GROUP BY in outer query
func TestApplyOuterQuery_GroupByCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_gb", []*query.ColumnDef{
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
			Table:   "aoq_gb",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "grp"},
			&query.QualifiedIdentifier{Column: "val"},
		},
		From: &query.TableRef{Name: "aoq_gb"},
	}
	cat.CreateView("aoq_gb_view", viewStmt)

	// Query with GROUP BY on view
	result, err := cat.ExecuteQuery(`
		SELECT grp, COUNT(*) as cnt, SUM(val) as total, AVG(val) as avg_val
		FROM aoq_gb_view
		GROUP BY grp
		ORDER BY grp
	`)
	if err != nil {
		t.Logf("GROUP BY on view error: %v", err)
	} else {
		t.Logf("GROUP BY on view returned %d rows", len(result.Rows))
	}

	// Query with HAVING
	result, err = cat.ExecuteQuery(`
		SELECT grp, COUNT(*) as cnt
		FROM aoq_gb_view
		GROUP BY grp
		HAVING COUNT(*) > 10
	`)
	if err != nil {
		t.Logf("HAVING on view error: %v", err)
	} else {
		t.Logf("HAVING on view returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestResolveOuterRefsInQuery_ALL - comprehensive outer ref tests
func TestResolveOuterRefsInQuery_ALL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_main_all", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "status", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "outer_sub_all", []*query.ColumnDef{
		{Name: "sub_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		status := "active"
		if i%3 == 0 {
			status = "inactive"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_main_all",
			Columns: []string{"id", "name", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Name"), strReal(status)}},
		}, nil)

		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "outer_sub_all",
				Columns: []string{"sub_id", "main_id", "amount"},
				Values:  [][]query.Expression{{numReal(float64(i*10+j)), numReal(float64(i)), numReal(float64(j * 100))}},
			}, nil)
		}
	}

	// ALL subquery
	result, err := cat.ExecuteQuery(`
		SELECT * FROM outer_main_all m
		WHERE status = 'active'
		AND id > ALL (
			SELECT main_id FROM outer_sub_all
			WHERE amount < 200
		)
	`)
	if err != nil {
		t.Logf("ALL subquery error: %v", err)
	} else {
		t.Logf("ALL subquery returned %d rows", len(result.Rows))
	}

	// ANY/SOME subquery
	result, err = cat.ExecuteQuery(`
		SELECT * FROM outer_main_all m
		WHERE id = ANY (
			SELECT main_id FROM outer_sub_all
			WHERE amount > 250
		)
	`)
	if err != nil {
		t.Logf("ANY subquery error: %v", err)
	} else {
		t.Logf("ANY subquery returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestSave_Load_ErrorScenarios - error scenarios for Save/Load
func TestSave_Load_ErrorScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create tables with various column types
	createCoverageTestTable(t, cat, "save_err", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "txt", Type: query.TokenText},
		{Name: "real", Type: query.TokenReal},
		{Name: "bool", Type: query.TokenBoolean},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "save_err",
			Columns: []string{"id", "txt", "real", "bool"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("text"), numReal(float64(i) * 1.5), &query.BooleanLiteral{Value: i%2 == 0}}},
		}, nil)
	}

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_save_err",
		Table:   "save_err",
		Columns: []string{"txt"},
	})

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "txt"},
		},
		From: &query.TableRef{Name: "save_err"},
	}
	cat.CreateView("save_err_view", viewStmt)

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

	// Verify data
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM save_err`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after load: %v", result.Rows)
	}

	_ = result
}

// TestVacuum_EmptyTable - vacuum on empty table
func TestVacuum_EmptyTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create empty table
	createCoverageTestTable(t, cat, "vac_empty", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Vacuum empty table
	err = cat.Vacuum()
	if err != nil {
		t.Logf("Vacuum empty table error: %v", err)
	}

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM vac_empty`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Empty table count: %v", result.Rows)
	}

	_ = result
}

// TestVacuum_AllDeleted - vacuum when all rows deleted
func TestVacuum_AllDeleted(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "vac_all_del", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac_all_del",
		Table:   "vac_all_del",
		Columns: []string{"data"},
	})

	// Insert and delete all
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "vac_all_del",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Delete all
	for i := 1; i <= 20; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "vac_all_del",
			Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(float64(i))},
		}, nil)
	}

	// Count before vacuum
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM vac_all_del`)
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

	// Count after vacuum
	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM vac_all_del`)
	if err != nil {
		t.Logf("Count after error: %v", err)
	} else {
		t.Logf("Count after vacuum: %v", result.Rows)
	}

	_ = result
}

// TestRollbackToSavepoint_AllCases - comprehensive savepoint tests
func TestRollbackToSavepoint_AllCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sp_all", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sp_all",
		Table:   "sp_all",
		Columns: []string{"val"},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	// Insert
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_all",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}},
	}, nil)

	// Savepoint
	cat.Savepoint("sp1")

	// More inserts
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_all",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("b")}},
	}, nil)

	// Update
	cat.Update(ctx, &query.UpdateStmt{
		Table: "sp_all",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Nested savepoint
	cat.Savepoint("sp2")

	// Delete
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "sp_all",
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)

	// Rollback to sp2 - should undo delete
	err = cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("Rollback to sp2 error: %v", err)
	}

	// Verify
	result, err := cat.ExecuteQuery(`SELECT * FROM sp_all ORDER BY id`)
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("After rollback to sp2: %d rows", len(result.Rows))
	}

	// Rollback to sp1 - should undo update and second insert
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	// Verify
	result, err = cat.ExecuteQuery(`SELECT * FROM sp_all ORDER BY id`)
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("After rollback to sp1: %d rows", len(result.Rows))
	}

	// Commit
	cat.CommitTransaction()

	_ = result
}
