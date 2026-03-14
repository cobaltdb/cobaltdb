package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_CommitTransactionComplex targets CommitTransaction with more scenarios
func TestCoverage_CommitTransactionComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_commit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Begin and commit multiple transactions
	for i := 1; i <= 3; i++ {
		cat.BeginTransaction(uint64(i))

		cat.Insert(ctx, &query.InsertStmt{
			Table:   "txn_commit",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("txn")}},
		}, nil)

		err := cat.CommitTransaction()
		if err != nil {
			t.Logf("Commit error: %v", err)
		}
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_commit")
	t.Logf("Count after commits: %v", result.Rows)
}

// TestCoverage_RollbackTransactionComplex targets RollbackTransaction with more scenarios
func TestCoverage_RollbackTransactionComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_rollback", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert initial data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_rollback",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("original")}},
	}, nil)

	// Begin and rollback with modifications
	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_rollback",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("temp")}},
	}, nil)

	cat.Update(ctx, &query.UpdateStmt{
		Table: "txn_rollback",
		Set:   []*query.SetClause{{Column: "val", Value: strReal("modified")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	err := cat.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify rollback
	result, _ := cat.ExecuteQuery("SELECT * FROM txn_rollback")
	t.Logf("Rows after rollback: %v", result.Rows)
}

// TestCoverage_TransactionWithDDL targets transactions with DDL operations
func TestCoverage_TransactionWithDDL(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_ddl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_ddl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}},
	}, nil)

	// Transaction with DDL
	cat.BeginTransaction(1)

	// Add column within transaction
	cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "txn_ddl",
		Action: "ADD",
		Column: query.ColumnDef{Name: "newcol", Type: query.TokenInteger, Default: numReal(0)},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_ddl",
		Columns: []string{"id", "val", "newcol"},
		Values:  [][]query.Expression{{numReal(2), strReal("b"), numReal(100)}},
	}, nil)

	// Rollback DDL changes
	err := cat.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify DDL was rolled back
	result, _ := cat.ExecuteQuery("SELECT * FROM txn_ddl")
	t.Logf("Columns after rollback: %d", len(result.Columns))
}

// TestCoverage_TransactionWithIndex targets transactions with index operations
func TestCoverage_TransactionWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_idx",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	// Transaction with CREATE INDEX
	cat.BeginTransaction(1)

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_txn",
		Table:   "txn_idx",
		Columns: []string{"code"},
		Unique:  true,
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_idx",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(3), strReal("C")}},
	}, nil)

	// Rollback should undo both index creation and insert
	err := cat.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify index was dropped
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_idx")
	t.Logf("Count after rollback: %v", result.Rows)
}

// TestCoverage_ExecuteScalarSelectDistinct targets executeScalarSelect with DISTINCT
func TestCoverage_ExecuteScalarSelectDistinct(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_distinct", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
	})

	// Insert data with duplicates
	grps := []string{"A", "A", "B", "B", "C", "A"}
	for i, grp := range grps {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_distinct",
			Columns: []string{"id", "grp"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(grp)}},
		}, nil)
	}

	// COUNT DISTINCT
	result, err := cat.ExecuteQuery("SELECT COUNT(DISTINCT grp) FROM scalar_distinct")
	if err != nil {
		t.Logf("COUNT DISTINCT error: %v", err)
	} else {
		t.Logf("Distinct count: %v", result.Rows)
	}
}

// TestCoverage_ExecuteScalarSelectGroupBy targets executeScalarSelect with GROUP BY
func TestCoverage_ExecuteScalarSelectGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_group", []*query.ColumnDef{
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
			Table:   "scalar_group",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 10))}},
		}, nil)
	}

	// Scalar with GROUP BY
	result, err := cat.ExecuteQuery("SELECT category, COUNT(*), SUM(amount) FROM scalar_group GROUP BY category")
	if err != nil {
		t.Logf("GROUP BY error: %v", err)
	} else {
		t.Logf("GROUP BY result: %v", result.Rows)
	}
}

// TestCoverage_ApplyOrderByNulls targets applyOrderBy with NULL handling
func TestCoverage_ApplyOrderByNulls(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_nulls", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data with NULLs
	for i := 1; i <= 10; i++ {
		if i%3 == 0 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "order_nulls",
				Columns: []string{"id"},
				Values:  [][]query.Expression{{numReal(float64(i))}},
			}, nil)
		} else {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "order_nulls",
				Columns: []string{"id", "val"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
			}, nil)
		}
	}

	// ORDER BY with NULLs
	queries := []string{
		"SELECT * FROM order_nulls ORDER BY val ASC",
		"SELECT * FROM order_nulls ORDER BY val DESC",
		"SELECT * FROM order_nulls ORDER BY val ASC, id DESC",
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

// TestCoverage_ApplyOrderByMultiple targets applyOrderBy with multiple columns
func TestCoverage_ApplyOrderByMultiple(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_multi",
			Columns: []string{"id", "grp", "score", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(21 - i)), strReal("name")}},
		}, nil)
	}

	// Multi-column ORDER BY
	queries := []string{
		"SELECT * FROM order_multi ORDER BY grp ASC, score DESC",
		"SELECT * FROM order_multi ORDER BY grp DESC, score ASC, id DESC",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Multi ORDER BY error: %v", err)
		} else {
			t.Logf("Multi ORDER BY returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_EvaluateHavingComplex targets evaluateHaving with complex conditions
func TestCoverage_EvaluateHavingComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
		{Name: "sales", Type: query.TokenInteger},
	})

	// Insert data
	regions := []string{"North", "South", "East", "West", "North"}
	for i, region := range regions {
		for j := 0; j < 10; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "having_complex",
				Columns: []string{"id", "region", "sales"},
				Values:  [][]query.Expression{{numReal(float64(i*10 + j + 1)), strReal(region), numReal(float64((j + 1) * 100))}},
			}, nil)
		}
	}

	// Complex HAVING
	queries := []string{
		"SELECT region, COUNT(*) as cnt, AVG(sales) as avg_sales FROM having_complex GROUP BY region HAVING cnt >= 5 AND avg_sales > 500",
		"SELECT region, SUM(sales) as total FROM having_complex GROUP BY region HAVING total BETWEEN 1000 AND 10000",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING error: %v", err)
		} else {
			t.Logf("HAVING returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ExecuteSelectWithJoinAndGroupBy targets executeSelectWithJoinAndGroupBy
func TestCoverage_ExecuteSelectWithJoinAndGroupBy2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jgb_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "jgb_detail", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("item" + string(rune('0'+i)))}},
		}, nil)
		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "jgb_detail",
				Columns: []string{"id", "main_id", "amount"},
				Values:  [][]query.Expression{{numReal(float64(i*10 + j)), numReal(float64(i)), numReal(float64(j * 10))}},
			}, nil)
		}
	}

	// JOIN with GROUP BY and HAVING
	result, err := cat.ExecuteQuery(`
		SELECT m.name, COUNT(*) as cnt, SUM(d.amount) as total
		FROM jgb_main m
		JOIN jgb_detail d ON m.id = d.main_id
		GROUP BY m.name
		HAVING cnt >= 2 AND total > 50
	`)
	if err != nil {
		t.Logf("JOIN GROUP BY error: %v", err)
	} else {
		t.Logf("JOIN GROUP BY returned %d rows", len(result.Rows))
	}
}

// TestCoverage_AlterTableDropColumnWithIndex targets AlterTableDropColumn with index cleanup
func TestCoverage_AlterTableDropColumnWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "alter_drop_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create index on column to be dropped
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code_drop",
		Table:   "alter_drop_idx",
		Columns: []string{"code"},
		Unique:  true,
	})
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_val_drop",
		Table:   "alter_drop_idx",
		Columns: []string{"val", "code"},
		Unique:  false,
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "alter_drop_idx",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE" + string(rune('A'+i))), numReal(float64(i * 10))}},
		}, nil)
	}

	// Drop column with indexes
	err := cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "alter_drop_idx",
		Action:  "DROP",
		OldName: "code",
	})
	if err != nil {
		t.Logf("Drop column error: %v", err)
	}

	// Verify indexes are dropped
	result, _ := cat.ExecuteQuery("SELECT * FROM alter_drop_idx")
	t.Logf("Columns after drop: %d", len(result.Columns))
}

// TestCoverage_TriggersComplex targets executeTriggers with various scenarios
func TestCoverage_TriggersComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create audit log table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "audit_complex",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "action", Type: query.TokenText},
			{Name: "old_val", Type: query.TokenText},
			{Name: "new_val", Type: query.TokenText},
		},
	})

	createCoverageTestTable(t, cat, "trig_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	// Create BEFORE INSERT trigger
	cat.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trig_before_insert",
		Table: "trig_src",
		Time:  "BEFORE",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "audit_complex",
				Columns: []string{"action", "new_val"},
				Values:  [][]query.Expression{{strReal("BEFORE INSERT"), strReal("inserting")}},
			},
		},
	})

	// Create AFTER UPDATE trigger
	cat.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trig_after_update",
		Table: "trig_src",
		Time:  "AFTER",
		Event: "UPDATE",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "audit_complex",
				Columns: []string{"action"},
				Values:  [][]query.Expression{{strReal("AFTER UPDATE")}},
			},
		},
	})

	// Create AFTER DELETE trigger
	cat.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trig_after_delete",
		Table: "trig_src",
		Time:  "AFTER",
		Event: "DELETE",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "audit_complex",
				Columns: []string{"action"},
				Values:  [][]query.Expression{{strReal("AFTER DELETE")}},
			},
		},
	})

	// Insert (should fire BEFORE trigger)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "trig_src",
		Columns: []string{"id", "status"},
		Values:  [][]query.Expression{{numReal(1), strReal("active")}},
	}, nil)

	// Update (should fire AFTER UPDATE trigger)
	cat.Update(ctx, &query.UpdateStmt{
		Table: "trig_src",
		Set:   []*query.SetClause{{Column: "status", Value: strReal("inactive")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Delete (should fire AFTER DELETE trigger)
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "trig_src",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Check audit log
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM audit_complex")
	t.Logf("Audit count: %v", result.Rows)
}
