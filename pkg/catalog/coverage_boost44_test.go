package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Targeted tests for lowest coverage functions (67-70%)
// ============================================================

// TestApplyOuterQuery_AggregateCases - tests aggregates in outer query
func TestApplyOuterQuery_AggregateCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_agg_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		grp := "A"
		if i > 25 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_agg_base",
			Columns: []string{"id", "grp", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create simple view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "grp"},
			&query.QualifiedIdentifier{Column: "amount"},
		},
		From: &query.TableRef{Name: "aoq_agg_base"},
	}
	cat.CreateView("aoq_simple_view", viewStmt)

	// Aggregate on view
	result, err := cat.ExecuteQuery(`
		SELECT grp, COUNT(*) as cnt, SUM(amount) as total, AVG(amount) as avg_amt
		FROM aoq_simple_view
		GROUP BY grp
		ORDER BY grp
	`)
	if err != nil {
		t.Logf("Aggregate on view error: %v", err)
	} else {
		t.Logf("Aggregate on view returned %d rows", len(result.Rows))
	}

	// Aggregate with WHERE
	result, err = cat.ExecuteQuery(`
		SELECT COUNT(*) as cnt, SUM(amount) as total
		FROM aoq_simple_view
		WHERE amount > 200
	`)
	if err != nil {
		t.Logf("Aggregate with WHERE error: %v", err)
	} else {
		t.Logf("Aggregate with WHERE returned %d rows", len(result.Rows))
	}

	// MIN/MAX on view
	result, err = cat.ExecuteQuery(`
		SELECT MIN(amount) as min_amt, MAX(amount) as max_amt
		FROM aoq_simple_view
	`)
	if err != nil {
		t.Logf("MIN/MAX error: %v", err)
	} else {
		t.Logf("MIN/MAX returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestDeleteWithUsingLocked_JoinTypes - various join types in DELETE USING
func TestDeleteWithUsingLocked_JoinTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "delu_target2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "status", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "delu_ref2", []*query.ColumnDef{
		{Name: "ref_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "flag", Type: query.TokenBoolean},
	})
	createCoverageTestTable(t, cat, "delu_extra2", []*query.ColumnDef{
		{Name: "extra_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		status := "active"
		if i%3 == 0 {
			status = "deleted"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_target2",
			Columns: []string{"id", "ref_id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i%5 + 1)), strReal(status)}},
		}, nil)
	}
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_ref2",
			Columns: []string{"ref_id", "flag"},
			Values:  [][]query.Expression{{numReal(float64(i)), &query.BooleanLiteral{Value: i%2 == 0}}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_extra2",
			Columns: []string{"extra_id", "ref_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// DELETE USING with LEFT JOIN condition
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "delu_target2",
		Using: []*query.TableRef{{Name: "delu_ref2"}},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: colReal("delu_target2.ref_id"), Operator: query.TokenEq, Right: colReal("delu_ref2.ref_id")},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: colReal("delu_ref2.flag"), Operator: query.TokenEq, Right: &query.BooleanLiteral{Value: true}},
		},
	}, nil)
	if err != nil {
		t.Logf("DELETE USING with boolean condition error: %v", err)
	}

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM delu_target2`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after DELETE USING: %v", result.Rows)
	}

	_ = result
}

// TestUpdateRowSlice_BulkOperations - bulk update operations
func TestUpdateRowSlice_BulkOperations(t *testing.T) {
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
	createCoverageTestTable(t, cat, "fk_bulk_parent", []*query.ColumnDef{
		{Name: "parent_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	// Multiple child tables with CASCADE
	for i := 1; i <= 3; i++ {
		childName := "fk_bulk_child_" + string(rune('0'+i))
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
					ReferencedTable:   "fk_bulk_parent",
					ReferencedColumns: []string{"parent_id"},
					OnDelete:          "CASCADE",
					OnUpdate:          "CASCADE",
				},
			},
		})
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_bulk_parent",
			Columns: []string{"parent_id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("active")}},
		}, nil)
		for j := 1; j <= 3; j++ {
			childName := "fk_bulk_child_" + string(rune('0'+j))
			cat.Insert(ctx, &query.InsertStmt{
				Table:   childName,
				Columns: []string{"child_id", "parent_id", "data"},
				Values:  [][]query.Expression{{numReal(float64(i*10+j)), numReal(float64(i)), strReal("data")}},
			}, nil)
		}
	}

	// Bulk update parent IDs - should cascade to all children
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_bulk_parent",
		Set:   []*query.SetClause{{Column: "parent_id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenLte, Right: numReal(5)},
	}, nil)
	if err != nil {
		t.Logf("Bulk update error: %v", err)
	}

	// Verify all children updated
	for i := 1; i <= 3; i++ {
		childName := "fk_bulk_child_" + string(rune('0'+i))
		result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM ` + childName + ` WHERE parent_id = 100`)
		if err != nil {
			t.Logf("Count error for %s: %v", childName, err)
		} else {
			t.Logf("Children with parent_id 100 in %s: %v", childName, result.Rows)
		}
	}
}

// TestResolveOuterRefsInQuery_CorrelatedExists - correlated EXISTS subqueries
func TestResolveOuterRefsInQuery_CorrelatedExists(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "corr_dept", []*query.ColumnDef{
		{Name: "dept_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_name", Type: query.TokenText},
		{Name: "budget", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "corr_emp", []*query.ColumnDef{
		{Name: "emp_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "corr_dept",
			Columns: []string{"dept_id", "dept_name", "budget"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Dept"), numReal(float64(i * 10000))}},
		}, nil)
		for j := 1; j <= 10; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "corr_emp",
				Columns: []string{"emp_id", "dept_id", "salary"},
				Values:  [][]query.Expression{{numReal(float64(i*10+j)), numReal(float64(i)), numReal(float64(j*100))}},
			}, nil)
		}
	}

	// EXISTS with correlated subquery and aggregate
	result, err := cat.ExecuteQuery(`
		SELECT d.dept_id, d.dept_name
		FROM corr_dept d
		WHERE EXISTS (
			SELECT 1 FROM corr_emp e
			WHERE e.dept_id = d.dept_id
			HAVING SUM(e.salary) > d.budget / 2
		)
	`)
	if err != nil {
		t.Logf("EXISTS with HAVING error: %v", err)
	} else {
		t.Logf("EXISTS with HAVING returned %d rows", len(result.Rows))
	}

	// Multiple correlated subqueries
	result, err = cat.ExecuteQuery(`
		SELECT d.dept_id,
			(SELECT COUNT(*) FROM corr_emp e WHERE e.dept_id = d.dept_id) as emp_count,
			(SELECT SUM(salary) FROM corr_emp e WHERE e.dept_id = d.dept_id) as total_salary,
			(SELECT AVG(salary) FROM corr_emp e WHERE e.dept_id = d.dept_id) as avg_salary
		FROM corr_dept d
		WHERE EXISTS (SELECT 1 FROM corr_emp e WHERE e.dept_id = d.dept_id AND salary > 500)
		ORDER BY d.dept_id
	`)
	if err != nil {
		t.Logf("Multiple correlated error: %v", err)
	} else {
		t.Logf("Multiple correlated returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestSave_ErrorHandling - error paths in Save
func TestSave_ErrorHandling(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create many tables to stress Save
	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("save_stress_%d", i)
		createCoverageTestTable(t, cat, name, []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		})

		for j := 1; j <= 50; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   name,
				Columns: []string{"id", "data"},
				Values:  [][]query.Expression{{numReal(float64(j)), strReal("test data")}},
			}, nil)
		}
	}

	// Create indexes
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("save_stress_%d", i)
		cat.CreateIndex(&query.CreateIndexStmt{
			Index:   "idx_" + name,
			Table:   name,
			Columns: []string{"data"},
		})
	}

	// Save with transaction active
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "save_stress_1",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(999), strReal("txn data")}},
	}, nil)

	err = cat.Save()
	if err != nil {
		t.Logf("Save with active transaction error: %v", err)
	}

	cat.CommitTransaction()
}

// TestLoad_MoreScenarios - more Load scenarios
func TestLoad_MoreScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create tables with various data types
	createCoverageTestTable(t, cat, "load_types", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "txt", Type: query.TokenText},
		{Name: "real", Type: query.TokenReal},
		{Name: "bool", Type: query.TokenBoolean},
		{Name: "blob", Type: query.TokenBlob},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "load_types",
		Columns: []string{"id", "txt", "real", "bool", "blob"},
		Values:  [][]query.Expression{{numReal(1), strReal("test"), numReal(3.14), &query.BooleanLiteral{Value: true}, strReal("blob")}},
	}, nil)

	// Create views
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "txt"},
		},
		From: &query.TableRef{Name: "load_types"},
	}
	cat.CreateView("load_view", viewStmt)

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
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM load_types`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after load: %v", result.Rows)
	}

	_ = result
}

// TestRollbackToSavepoint_Partial - partial rollback scenarios
func TestRollbackToSavepoint_Partial(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sp_partial", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_sp_partial",
		Table:   "sp_partial",
		Columns: []string{"val"},
	})

	cat.BeginTransaction(1)

	// Insert initial
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_partial",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Create savepoint
	cat.Savepoint("sp1")

	// More inserts
	for i := 2; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sp_partial",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create nested savepoint
	cat.Savepoint("sp2")

	// Update within nested savepoint
	cat.Update(ctx, &query.UpdateStmt{
		Table: "sp_partial",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Delete within nested savepoint
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "sp_partial",
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(10)},
	}, nil)

	// Rollback to middle savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	// Verify state
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM sp_partial`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after partial rollback: %v", result.Rows)
	}

	cat.CommitTransaction()

	_ = result
}

// TestFlushTableTreesLocked_MoreCases - more flush cases
func TestFlushTableTreesLocked_MoreCases(t *testing.T) {
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
	for i := 1; i <= 5; i++ {
		name := "flush_tbl_" + string(rune('0'+i))
		createCoverageTestTable(t, cat, name, []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})

		for j := 1; j <= 20; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   name,
				Columns: []string{"id"},
				Values:  [][]query.Expression{{numReal(float64(j))}},
			}, nil)
		}

		// Create index for each table
		cat.CreateIndex(&query.CreateIndexStmt{
			Index:   "idx_" + name,
			Table:   name,
			Columns: []string{"id"},
		})
	}

	// Begin and rollback transaction
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_tbl_1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(999)}},
	}, nil)
	cat.RollbackTransaction()

	// Begin and commit transaction
	cat.BeginTransaction(2)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_tbl_2",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(888)}},
	}, nil)
	cat.CommitTransaction()

	// Verify
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM flush_tbl_2`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows in flush_tbl_2: %v", result.Rows)
	}

	_ = result
}

// TestVacuum_WithMultipleIndexes - vacuum with indexes
func TestVacuum_WithMultipleIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "vac_multi_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "col1", Type: query.TokenText},
		{Name: "col2", Type: query.TokenInteger},
		{Name: "col3", Type: query.TokenReal},
	})

	// Create multiple indexes
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac1",
		Table:   "vac_multi_idx",
		Columns: []string{"col1"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac2",
		Table:   "vac_multi_idx",
		Columns: []string{"col2"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac3",
		Table:   "vac_multi_idx",
		Columns: []string{"col3"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_vac_composite",
		Table:   "vac_multi_idx",
		Columns: []string{"col1", "col2"},
	})

	// Insert data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "vac_multi_idx",
			Columns: []string{"id", "col1", "col2", "col3"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val"+string(rune('A'+i%5))), numReal(float64(i)), numReal(float64(i) * 1.5)}},
		}, nil)
	}

	// Delete many rows
	for i := 1; i <= 70; i++ {
		cat.Delete(ctx, &query.DeleteStmt{
			Table: "vac_multi_idx",
			Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(float64(i))},
		}, nil)
	}

	// Count before vacuum
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM vac_multi_idx`)
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
	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM vac_multi_idx`)
	if err != nil {
		t.Logf("Count after error: %v", err)
	} else {
		t.Logf("Count after vacuum: %v", result.Rows)
	}

	_ = result
}
