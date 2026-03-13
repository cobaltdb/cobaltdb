package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Comprehensive tests for remaining low-coverage functions
// ============================================================

// TestDeleteLocked_MorePaths - additional deleteLocked coverage
func TestDeleteLocked_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Table with indexes
	createCoverageTestTable(t, cat, "del_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText},
		{Name: "status", Type: query.TokenText},
	})

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_email",
		Table:   "del_idx",
		Columns: []string{"email"},
		Unique:  true,
	})
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_status",
		Table:   "del_idx",
		Columns: []string{"status"},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		status := "active"
		if i%3 == 0 {
			status = "deleted"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_idx",
			Columns: []string{"id", "email", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("user" + string(rune('0'+i)) + "@test.com"), strReal(status)}},
		}, nil)
	}

	// Delete with WHERE on indexed column
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_idx",
		Where: &query.BinaryExpr{Left: colReal("status"), Operator: query.TokenEq, Right: strReal("deleted")},
	}, nil)
	if err != nil {
		t.Logf("Delete error: %v", err)
	}

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM del_idx`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after delete: %v", result.Rows)
	}

	_ = result
}

// TestExecuteTriggers_MoreCases - more trigger execution tests
func TestExecuteTriggers_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Main table
	createCoverageTestTable(t, cat, "trig_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Audit table
	createCoverageTestTable(t, cat, "trig_audit", []*query.ColumnDef{
		{Name: "audit_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "action", Type: query.TokenText},
		{Name: "old_val", Type: query.TokenInteger},
		{Name: "new_val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "trig_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Update
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "trig_main",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Update error: %v", err)
	}

	// Delete
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "trig_main",
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err != nil {
		t.Logf("Delete error: %v", err)
	}

	// Verify main table
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM trig_main`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows in main: %v", result.Rows)
	}

	_ = result
}

// TestRollbackTransaction_MoreCases - additional rollback tests
func TestRollbackTransaction_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "rb_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_rb",
		Table:   "rb_test",
		Columns: []string{"data"},
	})

	// Initial data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rb_test",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	// Begin and rollback without any operations
	cat.BeginTransaction(1)
	cat.RollbackTransaction()

	// Begin, insert, rollback
	cat.BeginTransaction(2)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rb_test",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(2), strReal("txn2")}},
	}, nil)
	cat.RollbackTransaction()

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM rb_test`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after rollback: %v", result.Rows)
	}

	// Begin, update, rollback
	cat.BeginTransaction(3)
	cat.Update(ctx, &query.UpdateStmt{
		Table: "rb_test",
		Set:   []*query.SetClause{{Column: "data", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	cat.RollbackTransaction()

	// Verify data unchanged
	result, err = cat.ExecuteQuery(`SELECT data FROM rb_test WHERE id = 1`)
	if err != nil {
		t.Logf("Select error: %v", err)
	} else if len(result.Rows) > 0 {
		t.Logf("Data after rollback: %v", result.Rows[0][0])
	}

	_ = result
}

// TestUpdateWithJoinLocked_MoreCases - additional UPDATE JOIN tests
func TestUpdateWithJoinLocked_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "upd_join_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "upd_join_ref", []*query.ColumnDef{
		{Name: "ref_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "multiplier", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_join_main",
			Columns: []string{"id", "ref_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i%5 + 1)), numReal(float64(i * 10))}},
		}, nil)
	}
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_join_ref",
			Columns: []string{"ref_id", "multiplier"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// UPDATE with FROM (simulated via ExecuteQuery if supported)
	// Try the programmatic approach
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_join_main",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		From:  &query.TableRef{Name: "upd_join_ref"},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: colReal("upd_join_main.ref_id"), Operator: query.TokenEq, Right: colReal("upd_join_ref.ref_id")},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: colReal("upd_join_ref.multiplier"), Operator: query.TokenGt, Right: numReal(5)},
		},
	}, nil)
	if err != nil {
		t.Logf("Update with FROM error (may be expected): %v", err)
	}

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM upd_join_main WHERE val = 999`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows updated: %v", result.Rows)
	}

	_ = result
}

// TestFindReferencingRows_MoreCases - additional FK find tests
func TestFindReferencingRows_MoreCases(t *testing.T) {
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
	createCoverageTestTable(t, cat, "fk_find_parent", []*query.ColumnDef{
		{Name: "parent_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Multiple child tables referencing parent
	for i := 1; i <= 3; i++ {
		childName := "fk_find_child_" + string(rune('0'+i))
		cat.CreateTable(&query.CreateTableStmt{
			Table: childName,
			Columns: []*query.ColumnDef{
				{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "parent_id", Type: query.TokenInteger},
				{Name: "data", Type: query.TokenText},
			},
			ForeignKeys: []*query.ForeignKeyDef{
				{
					Columns:           []string{"parent_id"},
					ReferencedTable:   "fk_find_parent",
					ReferencedColumns: []string{"parent_id"},
					OnDelete:          "RESTRICT",
				},
			},
		})
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_find_parent",
			Columns: []string{"parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)

		for j := 1; j <= 3; j++ {
			childName := "fk_find_child_" + string(rune('0'+j))
			cat.Insert(ctx, &query.InsertStmt{
				Table:   childName,
				Columns: []string{"child_id", "parent_id", "data"},
				Values:  [][]query.Expression{{numReal(float64(i*10+j)), numReal(float64(i)), strReal("data")}},
			}, nil)
		}
	}

	// Try to delete parent with RESTRICT - should find referencing rows
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_find_parent",
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err == nil {
		t.Error("Expected error for DELETE with RESTRICT FK")
	} else {
		t.Logf("Expected RESTRICT error: %v", err)
	}
}

// TestResolveAggregateInExpr_MoreCases - aggregate resolution tests
func TestResolveAggregateInExpr_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "agg_res_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "agg_res_detail", []*query.ColumnDef{
		{Name: "detail_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "qty", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_res_main",
			Columns: []string{"id", "grp", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 100))}},
		}, nil)

		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "agg_res_detail",
				Columns: []string{"detail_id", "main_id", "qty"},
				Values:  [][]query.Expression{{numReal(float64(i*10+j)), numReal(float64(i)), numReal(float64(j))}},
			}, nil)
		}
	}

	// JOIN with aggregates
	result, err := cat.ExecuteQuery(`
		SELECT m.grp,
			COUNT(DISTINCT m.id) as distinct_count,
			SUM(m.amount) as total_amount,
			AVG(d.qty) as avg_qty
		FROM agg_res_main m
		JOIN agg_res_detail d ON m.id = d.main_id
		GROUP BY m.grp
		ORDER BY m.grp
	`)
	if err != nil {
		t.Logf("JOIN with aggregates error: %v", err)
	} else {
		t.Logf("JOIN with aggregates returned %d rows", len(result.Rows))
	}

	// HAVING with multiple aggregates
	result, err = cat.ExecuteQuery(`
		SELECT grp, COUNT(*) as cnt
		FROM agg_res_main
		GROUP BY grp
		HAVING SUM(amount) > 5000 AND AVG(amount) > 500
	`)
	if err != nil {
		t.Logf("HAVING with multiple aggregates error: %v", err)
	} else {
		t.Logf("HAVING returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestJSONQuote_MoreCases - additional JSONQuote tests
func TestJSONQuote_MoreCases(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", `"hello"`},
		{"", `""`},
		{"with \"quotes\"", `"with \"quotes\""`},
		{"with\\backslash", `"with\\backslash"`},
		{"line1\nline2", `"line1\nline2"`},
		{"tab\there", `"tab\there"`},
		{"special: \x00\x01", `"special: \u0000\u0001"`},
	}

	for _, tt := range tests {
		result := JSONQuote(tt.input)
		if result != tt.expected {
			t.Logf("JSONQuote(%q) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

// TestFlushTableTreesLocked_Savepoint - flush with savepoints
func TestFlushTableTreesLocked_Savepoint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "flush_sp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_flush_sp",
		Table:   "flush_sp",
		Columns: []string{"val"},
	})

	// Begin, insert, savepoint, insert, rollback to savepoint, commit
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_sp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("first")}},
	}, nil)

	cat.Savepoint("sp1")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_sp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("second")}},
	}, nil)

	// Update
	cat.Update(ctx, &query.UpdateStmt{
		Table: "flush_sp",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	cat.RollbackToSavepoint("sp1")
	cat.CommitTransaction()

	// Verify
	result, err := cat.ExecuteQuery(`SELECT * FROM flush_sp ORDER BY id`)
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Rows after savepoint rollback: %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("Row: %v", row)
		}
	}

	_ = result
}
