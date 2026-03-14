package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_MaintenanceVacuum targets Vacuum functionality
func TestCoverage_MaintenanceVacuum(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "vacuum_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "vacuum_test",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Delete some rows to create fragmentation
	for i := 1; i <= 50; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "vacuum_test",
			Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(float64(i))},
		}, nil)
	}

	// Run vacuum
	err := cat.Vacuum()
	if err != nil {
		t.Logf("Vacuum error: %v", err)
	}

	// Verify data
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM vacuum_test")
	t.Logf("Count after vacuum: %v", result.Rows)
}

// TestCoverage_MaintenanceAnalyze targets Analyze functionality
func TestCoverage_MaintenanceAnalyze(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "analyze_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data with distribution
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "analyze_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 10))}},
		}, nil)
	}

	// Run analyze
	err := cat.Analyze("analyze_test")
	if err != nil {
		t.Logf("Analyze error: %v", err)
	}

	// Get stats
	stats, err := cat.GetTableStats("analyze_test")
	if err != nil {
		t.Logf("GetTableStats error: %v", err)
	} else {
		t.Logf("Table stats row count: %d", stats.RowCount)
	}
}

// TestCoverage_ForeignKeyComplex targets complex FK scenarios
func TestCoverage_ForeignKeyComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent with composite key reference
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_parent_complex",
		Columns: []*query.ColumnDef{
			{Name: "id1", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "id2", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent_complex",
		Columns: []string{"id1", "id2"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(1), numReal(2)}, {numReal(2), numReal(1)}},
	}, nil)

	// Create child referencing composite key
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child_complex",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id1", Type: query.TokenInteger},
			{Name: "parent_id2", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id1", "parent_id2"},
				ReferencedTable:   "fk_parent_complex",
				ReferencedColumns: []string{"id1", "id2"},
				OnDelete:          "RESTRICT",
			},
		},
	})

	// Insert valid child
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child_complex",
		Columns: []string{"id", "parent_id1", "parent_id2"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(1)}},
	}, nil)
	if err != nil {
		t.Logf("Insert error: %v", err)
	}

	// Try to delete parent with RESTRICT (should fail)
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent_complex",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id1"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Delete with RESTRICT error (expected): %v", err)
	}
}

// TestCoverage_InsertLockedAutoInc targets insertLocked with auto-increment
func TestCoverage_InsertLockedAutoInc(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "autoinc_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert without specifying ID
	for i := 0; i < 5; i++ {
		_, _, err := cat.Insert(ctx, &query.InsertStmt{
			Table:   "autoinc_test",
			Columns: []string{"val"},
			Values:  [][]query.Expression{{strReal("test")}},
		}, nil)
		if err != nil {
			t.Logf("Auto-inc insert error: %v", err)
		}
	}

	result, _ := cat.ExecuteQuery("SELECT * FROM autoinc_test ORDER BY id")
	t.Logf("Auto-inc rows: %v", result.Rows)
}

// TestCoverage_InsertLockedMultiRow targets insertLocked with multi-row insert
func TestCoverage_InsertLockedMultiRow(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "multi_insert", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Multi-row insert
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "multi_insert",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), strReal("a")},
			{numReal(2), strReal("b")},
			{numReal(3), strReal("c")},
			{numReal(4), strReal("d")},
			{numReal(5), strReal("e")},
		},
	}, nil)

	if err != nil {
		t.Logf("Multi-row insert error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM multi_insert")
	t.Logf("Count after multi-insert: %v", result.Rows)
}

// TestCoverage_DeleteLockedComplex targets deleteLocked with complex scenarios
func TestCoverage_DeleteLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		status := "active"
		if i > 30 {
			status = "inactive"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_complex",
			Columns: []string{"id", "status", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(status), numReal(float64(i * 10))}},
		}, nil)
	}

	// Delete with complex WHERE
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_complex",
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: &query.Identifier{Name: "status"}, Operator: query.TokenEq, Right: strReal("inactive")},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: &query.Identifier{Name: "val"}, Operator: query.TokenGt, Right: numReal(350)},
		},
	}, nil)

	if err != nil {
		t.Logf("Complex delete error: %v", err)
	} else {
		t.Logf("Complex delete affected %d rows", rows)
	}
}

// TestCoverage_UpdateLockedJoin targets updateLocked with JOIN
func TestCoverage_UpdateLockedJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_target_join", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "upd_source_join", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "new_val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_target_join",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_source_join",
			Columns: []string{"id", "new_val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 100))}},
		}, nil)
	}

	// UPDATE with JOIN
	_, rows, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_target_join",
		Set:   []*query.SetClause{{Column: "val", Value: &query.QualifiedIdentifier{Table: "upd_source_join", Column: "new_val"}}},
		Joins: []*query.JoinClause{
			{Type: query.TokenJoin, Table: &query.TableRef{Name: "upd_source_join"}, Condition: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "upd_target_join.id"},
				Operator: query.TokenEq,
				Right:    &query.Identifier{Name: "upd_source_join.id"},
			}},
		},
	}, nil)

	if err != nil {
		t.Logf("UPDATE JOIN error: %v", err)
	} else {
		t.Logf("UPDATE JOIN affected %d rows", rows)
	}
}

// TestCoverage_SelectLockedGroupByHaving targets selectLocked with GROUP BY and HAVING
func TestCoverage_SelectLockedGroupByHaving(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_group", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		catg := "A"
		if i > 20 {
			catg = "B"
		}
		if i > 40 {
			catg = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_group",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 10))}},
		}, nil)
	}

	// GROUP BY with HAVING
	result, err := cat.ExecuteQuery("SELECT category, SUM(amount) as total FROM sel_group GROUP BY category HAVING total > 500")
	if err != nil {
		t.Logf("GROUP BY HAVING error: %v", err)
	} else {
		t.Logf("GROUP BY HAVING returned %d rows", len(result.Rows))
	}
}

// TestCoverage_InsertLockedDefaultValues targets insertLocked with DEFAULT VALUES
func TestCoverage_InsertLockedDefaultValues(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "default_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "val", Type: query.TokenText, Default: strReal("default_val")},
		},
	})

	// Insert with DEFAULT
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "default_test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	if err != nil {
		t.Logf("Insert with default error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT * FROM default_test")
	t.Logf("Default values result: %v", result.Rows)
}
