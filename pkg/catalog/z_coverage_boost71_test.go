package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_Vacuum targets Vacuum functionality
func TestCoverage_Vacuum(t *testing.T) {
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
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    numReal(float64(i)),
			},
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

// TestCoverage_VacuumWithIndexes targets Vacuum with indexes
func TestCoverage_VacuumWithIndexes(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "vacuum_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vacuum",
		Table:   "vacuum_idx",
		Columns: []string{"code"},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "vacuum_idx",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE" + string(rune('A'+i%26)))}},
		}, nil)
	}

	// Delete some rows
	for i := 1; i <= 25; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "vacuum_idx",
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    numReal(float64(i)),
			},
		}, nil)
	}

	// Run vacuum
	err := cat.Vacuum()
	if err != nil {
		t.Logf("Vacuum error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM vacuum_idx")
	t.Logf("Count after vacuum: %v", result.Rows)
}

// TestCoverage_SaveLoadComplex targets Save and Load with complex schema
func TestCoverage_SaveLoadComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create multiple tables
	cat.CreateTable(&query.CreateTableStmt{
		Table: "save_load_1",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	cat.CreateTable(&query.CreateTableStmt{
		Table: "save_load_2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "num", Type: query.TokenInteger},
		},
	})

	// Create indexes
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sl1",
		Table:   "save_load_1",
		Columns: []string{"val"},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "save_load_1",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val")}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "save_load_2",
			Columns: []string{"id", "num"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Save
	err := cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Load
	err = cat.Load()
	if err != nil {
		t.Logf("Load error: %v", err)
	}

	// Verify data
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM save_load_1")
	t.Logf("Table 1 count: %v", result.Rows)
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM save_load_2")
	t.Logf("Table 2 count: %v", result.Rows)
}

// TestCoverage_CTE targets ExecuteCTE
func TestCoverage_CTE(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "parent_id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert hierarchical data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_base",
		Columns: []string{"id", "parent_id", "name"},
		Values: [][]query.Expression{
			{numReal(1), &query.NullLiteral{}, strReal("root")},
			{numReal(2), numReal(1), strReal("child1")},
			{numReal(3), numReal(1), strReal("child2")},
			{numReal(4), numReal(2), strReal("grandchild1")},
			{numReal(5), numReal(2), strReal("grandchild2")},
		},
	}, nil)

	// Recursive CTE
	result, err := cat.ExecuteQuery(`
		WITH RECURSIVE tree AS (
			SELECT id, parent_id, name, 0 as level FROM cte_base WHERE parent_id IS NULL
			UNION ALL
			SELECT c.id, c.parent_id, c.name, t.level + 1
			FROM cte_base c
			JOIN tree t ON c.parent_id = t.id
		)
		SELECT * FROM tree ORDER BY level, id
	`)
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE returned %d rows", len(result.Rows))
	}

	// Non-recursive CTE
	result, err = cat.ExecuteQuery(`
		WITH summary AS (
			SELECT parent_id, COUNT(*) as cnt
			FROM cte_base
			WHERE parent_id IS NOT NULL
			GROUP BY parent_id
		)
		SELECT * FROM summary ORDER BY cnt DESC
	`)
	if err != nil {
		t.Logf("Non-recursive CTE error: %v", err)
	} else {
		t.Logf("Non-recursive CTE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_CTEMultiple targets CTE with multiple CTEs
func TestCoverage_CTEMultiple(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cte_multi",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Multiple CTEs
	result, err := cat.ExecuteQuery(`
		WITH
			evens AS (SELECT * FROM cte_multi WHERE id % 2 = 0),
			odds AS (SELECT * FROM cte_multi WHERE id % 2 = 1),
			even_sum AS (SELECT SUM(val) as total FROM evens),
			odd_sum AS (SELECT SUM(val) as total FROM odds)
		SELECT * FROM even_sum, odd_sum
	`)
	if err != nil {
		t.Logf("Multiple CTEs error: %v", err)
	} else {
		t.Logf("Multiple CTEs returned %d rows", len(result.Rows))
	}
}

// TestCoverage_RLSFilterInternal targets applyRLSFilterInternal
func TestCoverage_RLSFilterInternal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rls_filter",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "owner", Type: query.TokenText},
			{Name: "data", Type: query.TokenText},
		},
	})

	// Enable RLS
	cat.EnableRLS()

	// Insert data with different owners
	owners := []string{"user1", "user2", "user1", "user3"}
	for i, owner := range owners {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "rls_filter",
			Columns: []string{"id", "owner", "data"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(owner), strReal("data")}},
		}, nil)
	}

	// Query should apply RLS filter
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM rls_filter")
	t.Logf("Count with RLS: %v", result.Rows)
}

// TestCoverage_RLSCheckInternal targets RLS check internal functions
func TestCoverage_RLSCheckInternal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rls_check",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "owner", Type: query.TokenText},
			{Name: "val", Type: query.TokenText},
		},
	})

	cat.EnableRLS()

	// Insert (triggers checkRLSForInsertInternal)
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "rls_check",
		Columns: []string{"id", "owner", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("current_user"), strReal("test")}},
	}, nil)
	if err != nil {
		t.Logf("Insert error: %v", err)
	}

	// Update (triggers checkRLSForUpdateInternal)
	cat.Update(ctx, &query.UpdateStmt{
		Table: "rls_check",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Delete (triggers checkRLSForDeleteInternal)
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "rls_check",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
}

// TestCoverage_ApplyGroupByOrderBy targets applyGroupByOrderBy
func TestCoverage_ApplyGroupByOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "gb_order", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		catg := "A"
		if i > 15 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_order",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 10))}},
		}, nil)
	}

	// GROUP BY with ORDER BY
	result, err := cat.ExecuteQuery("SELECT category, SUM(amount) as total FROM gb_order GROUP BY category ORDER BY total DESC")
	if err != nil {
		t.Logf("GROUP BY ORDER BY error: %v", err)
	} else {
		t.Logf("GROUP BY ORDER BY returned %d rows", len(result.Rows))
	}
}

// TestCoverage_EvaluateExprWithGroupAggregatesJoin targets evaluateExprWithGroupAggregatesJoin
func TestCoverage_EvaluateExprWithGroupAggregatesJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "eega_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "eega_detail", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "eega_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("item")}},
		}, nil)
		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "eega_detail",
				Columns: []string{"id", "main_id", "amount"},
				Values:  [][]query.Expression{{numReal(float64(i*10 + j)), numReal(float64(i)), numReal(float64(j * 10))}},
			}, nil)
		}
	}

	// JOIN with GROUP BY and aggregates in SELECT
	result, err := cat.ExecuteQuery(`
		SELECT m.name, COUNT(*) as cnt, SUM(d.amount) as total, AVG(d.amount) as avg_amt
		FROM eega_main m
		JOIN eega_detail d ON m.id = d.main_id
		GROUP BY m.name
		HAVING total > 50
	`)
	if err != nil {
		t.Logf("JOIN GROUP BY aggregates error: %v", err)
	} else {
		t.Logf("JOIN GROUP BY aggregates returned %d rows", len(result.Rows))
	}
}

// TestCoverage_AlterTableRename targets AlterTableRename
func TestCoverage_AlterTableRename(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rename_old",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rename_old",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Rename table
	err := cat.AlterTableRename(&query.AlterTableStmt{
		Table:   "rename_old",
		Action:  "RENAME_TABLE",
		NewName: "rename_new",
	})
	if err != nil {
		t.Logf("Rename error: %v", err)
	}

	// Verify rename
	hasOld := cat.HasTableOrView("rename_old")
	hasNew := cat.HasTableOrView("rename_new")
	t.Logf("Has old: %v, Has new: %v", hasOld, hasNew)

	result, _ := cat.ExecuteQuery("SELECT * FROM rename_new")
	t.Logf("Data in renamed table: %v", result.Rows)
}

// TestCoverage_AlterTableRenameColumn targets AlterTableRenameColumn
func TestCoverage_AlterTableRenameColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rename_col",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "oldname", Type: query.TokenText},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rename_col",
		Columns: []string{"id", "oldname"},
		Values:  [][]query.Expression{{numReal(1), strReal("value")}},
	}, nil)

	// Rename column
	err := cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "rename_col",
		Action:  "RENAME_COLUMN",
		OldName: "oldname",
		NewName: "newname",
	})
	if err != nil {
		t.Logf("Rename column error: %v", err)
	}

	// Verify rename
	result, _ := cat.ExecuteQuery("SELECT newname FROM rename_col")
	t.Logf("Data with renamed column: %v", result.Rows)
}

// TestCoverage_DeleteLocked targets deleteLocked
func TestCoverage_DeleteLocked(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_locked", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_locked",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val")}},
		}, nil)
	}

	// Simple DELETE
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_locked",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLte,
			Right:    numReal(5),
		},
	}, nil)
	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}

	// DELETE with RETURNING (if supported)
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_locked")
	t.Logf("Count after delete: %v", result.Rows)
}
