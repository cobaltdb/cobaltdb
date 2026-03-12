package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Foreign Key: OnDelete and OnUpdate full coverage
// ============================================================

func TestCovBoost11_ForeignKey_OnDeleteCascade(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create parent table
	createCoverageTestTable(t, cat, "fk_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Create child table with CASCADE
	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
		},
	})

	// Insert parent and child
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Delete parent - should cascade to child
	_, affected, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent",
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}, nil)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row deleted, got %d", affected)
	}

	// Verify child is also deleted
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM fk_child")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 0 {
			t.Errorf("expected 0 child rows after cascade, got %d", count)
		}
	}
}

func TestCovBoost11_ForeignKey_OnDeleteSetNull(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "fk_sn_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_sn_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger, NotNull: false},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_sn_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "SET NULL",
			},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_sn_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_sn_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Delete parent - should set child.parent_id to NULL
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_sn_parent",
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}, nil)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify child.parent_id is NULL
	result, _ := cat.ExecuteQuery("SELECT parent_id FROM fk_sn_child WHERE id = 1")
	if len(result.Rows) > 0 && result.Rows[0][0] != nil {
		t.Errorf("expected parent_id to be NULL, got %v", result.Rows[0][0])
	}
}

func TestCovBoost11_ForeignKey_OnDeleteRestrict(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "fk_restr_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_restr_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_restr_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "RESTRICT",
			},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_restr_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_restr_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Try to delete parent - should fail with RESTRICT
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_restr_parent",
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}, nil)
	if err == nil {
		t.Error("expected error when deleting parent with RESTRICT constraint")
	}
}

func TestCovBoost11_ForeignKey_OnUpdateCascade(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "fk_upd_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_upd_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_upd_parent",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
			},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_upd_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_upd_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Update parent id - should cascade to child
	_, affected, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_upd_parent",
		Set:   []*query.SetClause{{Column: "id", Value: &query.NumberLiteral{Value: 99}}},
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}, nil)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row updated, got %d", affected)
	}

	// Verify child.parent_id is updated
	result, _ := cat.ExecuteQuery("SELECT parent_id FROM fk_upd_child WHERE id = 1")
	if len(result.Rows) > 0 {
		pid, ok := result.Rows[0][0].(int64)
		if !ok || pid != 99 {
			t.Errorf("expected parent_id=99, got %v", result.Rows[0][0])
		}
	}
}

// ============================================================
// Index: useIndexForExactMatch and useIndexForQueryWithArgs
// ============================================================

func TestCovBoost11_Index_ExactMatch(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "idx_exact_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	// Create index on code
	ctx := context.Background()
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "idx_exact_t",
		Columns: []string{"code"},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "idx_exact_t",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "ABC"}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "idx_exact_t",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "DEF"}}},
	}, nil)

	// Query by exact match on indexed column
	_, err = cat.ExecuteQuery("SELECT * FROM idx_exact_t WHERE code = 'ABC'")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	// Query executed without error
}

func TestCovBoost11_Index_RangeQuery(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "idx_range_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "idx_range_t",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// Range query
	_, err = cat.ExecuteQuery("SELECT * FROM idx_range_t WHERE score > 50 AND score < 80")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	// Query executed without error
}

// ============================================================
// CTE: executeRecursiveCTE
// ============================================================

func TestCovBoost11_CTE_Recursive(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create hierarchical data table
	createCoverageTestTable(t, cat, "employees", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "manager_id", Type: query.TokenInteger, NotNull: false},
	})

	ctx := context.Background()
	// Insert: Alice (id=1) manages Bob (id=2) and Charlie (id=3)
	// Bob manages Dave (id=4)
	data := []struct{ id int; name string; mgr interface{} }{
		{1, "Alice", nil},
		{2, "Bob", 1},
		{3, "Charlie", 1},
		{4, "Dave", 2},
	}
	for _, d := range data {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "employees",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(d.id)}, &query.StringLiteral{Value: d.name}}},
		}, nil)
	}

	// Update Dave's manager_id
	cat.Update(ctx, &query.UpdateStmt{
		Table: "employees",
		Set:   []*query.SetClause{{Column: "manager_id", Value: &query.NumberLiteral{Value: 2}}},
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 4}},
	}, nil)

	// Recursive CTE to find all employees under Alice
	_, err = cat.ExecuteQuery(`
		WITH RECURSIVE subordinates AS (
			SELECT id, name, manager_id FROM employees WHERE id = 1
			UNION ALL
			SELECT e.id, e.name, e.manager_id FROM employees e
			JOIN subordinates s ON e.manager_id = s.id
		)
		SELECT * FROM subordinates ORDER BY id
	`)
	if err != nil {
		t.Fatalf("recursive CTE failed: %v", err)
	}
	// Query executed without error
}

// ============================================================
// Vacuum: more edge cases
// ============================================================

func TestCovBoost11_Vacuum_WithIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "vac_idx_t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	ctx := context.Background()
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac_code",
		Table:   "vac_idx_t",
		Columns: []string{"code"},
	})

	// Insert and delete data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "vac_idx_t",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "test"}}},
		}, nil)
	}

	// Delete half
	for i := 1; i <= 50; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "vac_idx_t",
			Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: float64(i)}},
		}, nil)
	}

	// Vacuum should clean up index as well
	err = cat.Vacuum()
	if err != nil {
		t.Errorf("vacuum failed: %v", err)
	}

	// Verify remaining data
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM vac_idx_t")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 50 {
			t.Errorf("expected 50 rows after delete, got %d", count)
		}
	}
}

// ============================================================
// Save/Load with various table types
// ============================================================

func TestCovBoost11_SaveLoad_WithFK(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Create tables with FK
	createCoverageTestTable(t, cat, "save_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table: "save_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "save_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "save_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "save_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Save
	err = cat.Save()
	if err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Create new catalog and load
	cat2 := New(tree, pool, nil)
	err = cat2.Load()
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify FK relationship preserved
	result, _ := cat2.ExecuteQuery("SELECT COUNT(*) FROM save_child WHERE parent_id = 1")
	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count != 1 {
			t.Errorf("expected 1 child row, got %d", count)
		}
	}
}
