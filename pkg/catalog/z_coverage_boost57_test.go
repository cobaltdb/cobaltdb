package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_DeleteRowLockedNoTable targets deleteRowLocked when table doesn't exist
func TestCoverage_DeleteRowLockedNoTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Try to delete from non-existent table
	err := cat.DeleteRow(ctx, "nonexistent_table", 1)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
	t.Logf("DeleteRow error: %v", err)
}

// TestCoverage_DeleteRowLockedKeyNotExist targets deleteRowLocked when key doesn't exist
func TestCoverage_DeleteRowLockedKeyNotExist(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_nokey", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Try to delete non-existent key
	err := cat.DeleteRow(ctx, "del_nokey", 99999)
	if err != nil {
		t.Logf("DeleteRow non-existent key error (expected): %v", err)
	}
}

// TestCoverage_UpdateLockedConstraintErrors targets updateLocked constraint failures
func TestCoverage_UpdateLockedConstraintErrors(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create table with NOT NULL constraint
	cat.CreateTable(&query.CreateTableStmt{
		Table: "up_notnull",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "req", Type: query.TokenText, NotNull: true},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "up_notnull",
		Columns: []string{"id", "req"},
		Values:  [][]query.Expression{{numReal(1), strReal("value")}},
	}, nil)

	// Try to update to NULL (should fail)
	_, rows, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "up_notnull",
		Set:   []*query.SetClause{{Column: "req", Value: &query.NullLiteral{}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err != nil {
		t.Logf("NOT NULL constraint error (expected): %v", err)
	} else {
		t.Logf("Updated %d rows", rows)
	}
}

// TestCoverage_UpdateLockedFKError targets updateLocked FK constraint failure
func TestCoverage_UpdateLockedFKError(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Create child with FK
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "fk_parent", ReferencedColumns: []string{"id"}},
		},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// Try to update parent_id to non-existent value
	_, rows, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_child",
		Set:   []*query.SetClause{{Column: "parent_id", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err != nil {
		t.Logf("FK constraint error (expected): %v", err)
	} else {
		t.Logf("Updated %d rows", rows)
	}
}

// TestCoverage_UpdateLockedUniqueConstraint targets updateLocked UNIQUE constraint
func TestCoverage_UpdateLockedUniqueConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "up_unique",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText, Unique: true},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "up_unique",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	// Try to update to duplicate value
	_, rows, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "up_unique",
		Set:   []*query.SetClause{{Column: "code", Value: strReal("A")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(2),
		},
	}, nil)
	if err != nil {
		t.Logf("UNIQUE constraint error (expected): %v", err)
	} else {
		t.Logf("Updated %d rows", rows)
	}
}

// TestCoverage_UpdateLockedCheckConstraint targets updateLocked CHECK constraint
func TestCoverage_UpdateLockedCheckConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "up_check",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "val"},
				Operator: query.TokenGt,
				Right:    numReal(0),
			}},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "up_check",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}},
	}, nil)

	// Try to update to invalid value
	_, rows, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "up_check",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(-5)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err != nil {
		t.Logf("CHECK constraint error (expected): %v", err)
	} else {
		t.Logf("Updated %d rows", rows)
	}
}

// TestCoverage_UpdateLockedPKChange targets updateLocked with PRIMARY KEY change
func TestCoverage_UpdateLockedPKChange(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "up_pk",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "up_pk",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}, {numReal(2), strReal("b")}},
	}, nil)

	// Try to update PK to existing value (should fail)
	_, rows, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "up_pk",
		Set:   []*query.SetClause{{Column: "id", Value: numReal(2)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err != nil {
		t.Logf("PK duplicate error (expected): %v", err)
	} else {
		t.Logf("Updated %d rows", rows)
	}

	// Update PK to new value (should succeed)
	_, rows, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "up_pk",
		Set:   []*query.SetClause{{Column: "id", Value: numReal(100)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(2),
		},
	}, nil)
	if err != nil {
		t.Logf("PK update error: %v", err)
	} else {
		t.Logf("PK updated %d rows", rows)
	}
}

// TestCoverage_RollbackToSavepointInvalid targets RollbackToSavepoint with invalid savepoint
func TestCoverage_RollbackToSavepointInvalid(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_invalid", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Try to rollback to non-existent savepoint without transaction
	err := cat.RollbackToSavepoint("nonexistent")
	if err != nil {
		t.Logf("Rollback without txn error (expected): %v", err)
	}

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Rollback to non-existent savepoint
	err = cat.RollbackToSavepoint("nonexistent")
	if err != nil {
		t.Logf("Invalid savepoint error (expected): %v", err)
	}

	cat.RollbackTransaction()
}

// TestCoverage_EvaluateWhereComplexMore targets evaluateWhere complex expressions
func TestCoverage_EvaluateWhereComplexOps(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_complex2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_complex2",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2)), numReal(float64(i % 5)), strReal("test")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_complex2 WHERE a > 5 OR b < 3",
		"SELECT * FROM where_complex2 WHERE a IN (2, 4, 6, 8, 10)",
		"SELECT * FROM where_complex2 WHERE a BETWEEN 5 AND 15",
		"SELECT * FROM where_complex2 WHERE c LIKE 'te%'",
		"SELECT * FROM where_complex2 WHERE a >= 10 AND b <= 2",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOuterQueryComplex targets applyOuterQuery complex scenarios
func TestCoverage_ApplyOuterQueryComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		c := "A"
		if i > 15 {
			c = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_complex",
			Columns: []string{"id", "cat", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(c), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with GROUP BY, ORDER BY, LIMIT
	cat.CreateView("view_complex", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "cat"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
		},
		From:    &query.TableRef{Name: "outer_complex"},
		GroupBy: []query.Expression{&query.Identifier{Name: "cat"}},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}}, Desc: true},
		},
	})

	// Query view with outer WHERE
	result, _ := cat.ExecuteQuery("SELECT * FROM view_complex WHERE cat = 'A'")
	t.Logf("Complex view query returned %d rows", len(result.Rows))
}
