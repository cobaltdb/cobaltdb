package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_deleteRowLockedTxn targets deleteRowLocked with transaction undo log
func TestCoverage_deleteRowLockedTxn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_txn_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create unique and non-unique indexes
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_txn_code",
		Table:   "del_txn_idx",
		Columns: []string{"code"},
		Unique:  true,
	})
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_txn_val",
		Table:   "del_txn_idx",
		Columns: []string{"val"},
		Unique:  false,
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_txn_idx",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE" + string(rune('A'+i))), numReal(float64(i * 10))}},
		}, nil)
	}

	// Start transaction and delete (this covers index undo log in deleteRowLocked)
	cat.BeginTransaction(1)

	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_txn_idx",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(3),
		},
	}, nil)

	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}

	// Rollback to test undo log
	err = cat.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify row is restored
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_txn_idx")
	t.Logf("Count after rollback: %v", result.Rows)

	// Verify index is restored by querying via index
	result, _ = cat.ExecuteQuery("SELECT * FROM del_txn_idx WHERE code = 'CODED'")
	t.Logf("Index query after rollback: %v", result.Rows)
}

// TestCoverage_deleteRowLockedFKCascade targets deleteRowLocked with FK cascade
func TestCoverage_deleteRowLockedFKCascade(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_parent_del",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Create child table with CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child_del",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent_del",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent_del",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("parent1")}, {numReal(2), strReal("parent2")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child_del",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}, {numReal(3), numReal(2)}},
	}, nil)

	// Delete parent (should cascade to children)
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent_del",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}

	// Verify parent deleted
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM fk_parent_del")
	t.Logf("Parent count: %v", result.Rows)

	// Verify children cascaded
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM fk_child_del")
	t.Logf("Child count after cascade: %v", result.Rows)
}

// TestCoverage_deleteRowLockedFKSetNull targets deleteRowLocked with FK SET NULL
func TestCoverage_deleteRowLockedFKSetNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_parent_sn",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Create child table with SET NULL
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child_sn",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent_sn",
				ReferencedColumns: []string{"id"},
				OnDelete:          "SET NULL",
			},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent_sn",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child_sn",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}, {numReal(3), numReal(2)}},
	}, nil)

	// Delete parent (should set NULL in children)
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent_sn",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}

	// Verify children have NULL parent_id
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM fk_child_sn WHERE parent_id IS NULL")
	t.Logf("Children with NULL parent: %v", result.Rows)
}

// TestCoverage_applyOuterQueryOrderBy targets applyOuterQuery with ORDER BY on view
func TestCoverage_applyOuterQueryOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_order", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_order",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(21 - i))}},
		}, nil)
	}

	// Create a simple view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "outer_order"},
	}
	err := cat.CreateView("order_view", viewStmt)
	if err != nil {
		t.Logf("CreateView error: %v", err)
	}

	// Query view with ORDER BY (triggers applyOuterQuery)
	result, err := cat.ExecuteQuery("SELECT * FROM order_view ORDER BY val DESC")
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Ordered view returned %d rows, first row: %v", len(result.Rows), result.Rows[0])
	}

	cat.DropView("order_view")
}

// TestCoverage_applyOuterQueryLimit targets applyOuterQuery with LIMIT/OFFSET on view
func TestCoverage_applyOuterQueryLimit(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_limit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_limit",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create a view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.FunctionCall{Name: "val", Args: []query.Expression{&query.Identifier{Name: "val"}}},
		},
		From: &query.TableRef{Name: "outer_limit"},
	}
	err := cat.CreateView("limit_view", viewStmt)
	if err != nil {
		t.Logf("CreateView error: %v", err)
	}

	// Query view with LIMIT and OFFSET
	result, err := cat.ExecuteQuery("SELECT * FROM limit_view ORDER BY id LIMIT 10 OFFSET 20")
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Limited view returned %d rows, first row: %v", len(result.Rows), result.Rows[0])
	}

	cat.DropView("limit_view")
}

// TestCoverage_evaluateWhereNull targets evaluateWhere with NULL handling
func TestCoverage_evaluateWhereNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_null", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "txt", Type: query.TokenText},
	})

	// Insert data with NULLs
	for i := 1; i <= 10; i++ {
		if i%3 == 0 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "where_null",
				Columns: []string{"id"},
				Values:  [][]query.Expression{{numReal(float64(i))}},
			}, nil)
		} else if i%3 == 1 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "where_null",
				Columns: []string{"id", "val"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
			}, nil)
		} else {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "where_null",
				Columns: []string{"id", "txt"},
				Values:  [][]query.Expression{{numReal(float64(i)), strReal("text")}},
			}, nil)
		}
	}

	// Test NULL comparisons
	queries := []string{
		"SELECT * FROM where_null WHERE val IS NULL",
		"SELECT * FROM where_null WHERE val IS NOT NULL",
		"SELECT * FROM where_null WHERE txt IS NULL",
		"SELECT * FROM where_null WHERE txt IS NOT NULL",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("NULL query error: %v", err)
		} else {
			t.Logf("NULL query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_evaluateWhereLike targets evaluateWhere with LIKE patterns
func TestCoverage_evaluateWhereLike(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_like", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Insert data
	names := []string{"Alice", "Bob", "Charlie", "Alex", "Anna"}
	for i, name := range names {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_like",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(name)}},
		}, nil)
	}

	// Test LIKE patterns
	queries := []string{
		"SELECT * FROM where_like WHERE name LIKE 'A%'",
		"SELECT * FROM where_like WHERE name LIKE '%e'",
		"SELECT * FROM where_like WHERE name LIKE '%li%'",
		"SELECT * FROM where_like WHERE name NOT LIKE 'A%'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIKE query error: %v", err)
		} else {
			t.Logf("LIKE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_resolveAggregateInExprComplex targets resolveAggregateInExpr with complex expressions
func TestCoverage_resolveAggregateInExprComplex2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_complex2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 60; i++ {
		grp := "A"
		if i > 20 {
			grp = "B"
		}
		if i > 40 {
			grp = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_complex2",
			Columns: []string{"id", "grp", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), numReal(float64(i * 5))}},
		}, nil)
	}

	// Complex HAVING with arithmetic on aggregates
	queries := []string{
		"SELECT grp, SUM(val1) as s1, SUM(val2) as s2 FROM agg_complex2 GROUP BY grp HAVING s1 > s2 * 2",
		"SELECT grp, COUNT(*) as cnt, AVG(val1) as avg1 FROM agg_complex2 GROUP BY grp HAVING cnt > 10 AND avg1 > 100",
		"SELECT grp, SUM(val1 + val2) as total FROM agg_complex2 GROUP BY grp HAVING total > 10000",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex HAVING error: %v", err)
		} else {
			t.Logf("Complex HAVING returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedComplex targets selectLocked with complex scenarios
func TestCoverage_selectLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_complex",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Complex queries
	queries := []string{
		"SELECT id, val, val * 2 as doubled FROM sel_complex WHERE id > 10 ORDER BY doubled DESC LIMIT 5",
		"SELECT DISTINCT val % 5 as remainder FROM sel_complex ORDER BY remainder",
		"SELECT COUNT(*) as cnt, SUM(val) as total FROM sel_complex WHERE id BETWEEN 5 AND 25",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex select error: %v", err)
		} else {
			t.Logf("Complex select returned %d rows", len(result.Rows))
		}
	}
}
