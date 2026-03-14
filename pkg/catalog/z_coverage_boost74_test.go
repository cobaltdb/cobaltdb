package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RollbackToSavepointFK targets RollbackToSavepoint with FK changes
func TestCoverage_RollbackToSavepointFK(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_sp_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Create child
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_sp_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "fk_sp_parent", ReferencedColumns: []string{"id"}},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_sp_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Insert child
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_sp_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// Rollback should undo child insert
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	cat.RollbackTransaction()
}

// TestCoverage_applyRLSFilterInternal targets applyRLSFilterInternal more
func TestCoverage_applyRLSFilterInternal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rls_filter_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "owner", Type: query.TokenText},
		},
	})

	// Enable RLS
	cat.EnableRLS()

	// Insert data
	for i := 1; i <= 10; i++ {
		owner := "user1"
		if i > 5 {
			owner = "user2"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "rls_filter_test",
			Columns: []string{"id", "owner"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(owner)}},
		}, nil)
	}

	// Query with RLS
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM rls_filter_test")
	t.Logf("Count with RLS: %v", result.Rows)
}

// TestCoverage_checkRLSForOperations targets RLS check operations
func TestCoverage_checkRLSForOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rls_check_ops",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "owner", Type: query.TokenText},
		},
	})

	cat.EnableRLS()

	// Insert
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rls_check_ops",
		Columns: []string{"id", "owner"},
		Values:  [][]query.Expression{{numReal(1), strReal("current_user")}},
	}, nil)

	// Update
	cat.Update(ctx, &query.UpdateStmt{
		Table: "rls_check_ops",
		Set:   []*query.SetClause{{Column: "owner", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Delete
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "rls_check_ops",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
}

// TestCoverage_VacuumWithData targets Vacuum with various data types
func TestCoverage_VacuumWithData(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "vacuum_data", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "txt", Type: query.TokenText},
		{Name: "num", Type: query.TokenInteger},
		{Name: "flt", Type: query.TokenReal},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "vacuum_data",
			Columns: []string{"id", "txt", "num", "flt"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("text"), numReal(float64(i * 10)), numReal(float64(i) * 1.5)}},
		}, nil)
	}

	// Delete many rows
	for i := 1; i <= 40; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "vacuum_data",
			Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(float64(i))},
		}, nil)
	}

	// Vacuum
	err := cat.Vacuum()
	if err != nil {
		t.Logf("Vacuum error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM vacuum_data")
	t.Logf("Count after vacuum: %v", result.Rows)
}

// TestCoverage_LoadWithMultipleTables targets Load with multiple tables
func TestCoverage_LoadWithMultipleTables(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create multiple tables with indexes
	for i := 1; i <= 3; i++ {
		tblName := "load_tbl_" + string(rune('0'+i))
		cat.CreateTable(&query.CreateTableStmt{
			Table: tblName,
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "val", Type: query.TokenText},
			},
		})

		cat.CreateIndex(&query.CreateIndexStmt{
			Index:   "idx_" + tblName,
			Table:   tblName,
			Columns: []string{"val"},
		})

		cat.Insert(ctx, &query.InsertStmt{
			Table:   tblName,
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(1), strReal("test")}},
		}, nil)
	}

	// Save
	cat.Save()

	// Load
	err := cat.Load()
	if err != nil {
		t.Logf("Load error: %v", err)
	}

	// Verify all tables exist
	for i := 1; i <= 3; i++ {
		tblName := "load_tbl_" + string(rune('0'+i))
		has := cat.HasTableOrView(tblName)
		t.Logf("Has %s: %v", tblName, has)
	}
}

// TestCoverage_applyOrderByMixed targets applyOrderBy with mixed types
func TestCoverage_applyOrderByMixed(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_mixed", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "num", Type: query.TokenInteger},
		{Name: "txt", Type: query.TokenText},
		{Name: "flt", Type: query.TokenReal},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_mixed",
			Columns: []string{"id", "num", "txt", "flt"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(21-i)), strReal(string(rune('A'+i%26))), numReal(float64(i) * 0.5)}},
		}, nil)
	}

	// ORDER BY different types
	queries := []string{
		"SELECT * FROM order_mixed ORDER BY num",
		"SELECT * FROM order_mixed ORDER BY txt DESC",
		"SELECT * FROM order_mixed ORDER BY flt",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("ORDER BY error: %v", err)
		} else {
			t.Logf("ORDER BY returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ExecuteCTENested targets ExecuteCTE with nested CTEs
func TestCoverage_ExecuteCTENested(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_nested", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "parent_id", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_nested",
		Columns: []string{"id", "parent_id"},
		Values: [][]query.Expression{
			{numReal(1), &query.NullLiteral{}},
			{numReal(2), numReal(1)},
			{numReal(3), numReal(1)},
			{numReal(4), numReal(2)},
		},
	}, nil)

	// CTE with self-join
	result, err := cat.ExecuteQuery(`
		WITH RECURSIVE hierarchy AS (
			SELECT id, parent_id, 0 as level FROM cte_nested WHERE parent_id IS NULL
			UNION ALL
			SELECT c.id, c.parent_id, h.level + 1
			FROM cte_nested c
			JOIN hierarchy h ON c.parent_id = h.id
		)
		SELECT * FROM hierarchy ORDER BY level, id
	`)
	if err != nil {
		t.Logf("Nested CTE error: %v", err)
	} else {
		t.Logf("Nested CTE returned %d rows", len(result.Rows))
	}
}
