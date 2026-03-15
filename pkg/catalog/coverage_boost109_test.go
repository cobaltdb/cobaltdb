package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_updateLockedColumnNotFound tests updateLocked with non-existent column
func TestCoverage_updateLockedColumnNotFound(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "update_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert a row
	c.Insert(ctx, &query.InsertStmt{
		Table:   "update_test",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Try to update non-existent column
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "update_test",
		Set: []*query.SetClause{
			{Column: "nonexistent_column", Value: strReal("value")},
		},
	}, nil)
	if err == nil {
		t.Error("Should fail with non-existent column")
	} else {
		t.Logf("Expected error: %v", err)
	}
}

// TestCoverage_updateLockedUniqueConstraint tests updateLocked UNIQUE constraint violation
func TestCoverage_updateLockedUniqueConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with UNIQUE column
	c.CreateTable(&query.CreateTableStmt{
		Table: "unique_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText, Unique: true},
		},
	})

	// Insert two rows
	c.Insert(ctx, &query.InsertStmt{
		Table:   "unique_test",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("ABC")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "unique_test",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(2), strReal("XYZ")}},
	}, nil)

	// Try to update row 2's code to "ABC" which already exists
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "unique_test",
		Set: []*query.SetClause{
			{Column: "code", Value: strReal("ABC")},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(2),
		},
	}, nil)
	if err == nil {
		t.Error("Should fail with UNIQUE constraint violation")
	} else {
		t.Logf("Expected error: %v", err)
	}
}

// TestCoverage_updateLockedNotNullConstraint tests updateLocked NOT NULL constraint
func TestCoverage_updateLockedNotNullConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with NOT NULL column
	c.CreateTable(&query.CreateTableStmt{
		Table: "notnull_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "required", Type: query.TokenText, NotNull: true},
		},
	})

	// Insert a row
	c.Insert(ctx, &query.InsertStmt{
		Table:   "notnull_test",
		Columns: []string{"id", "required"},
		Values:  [][]query.Expression{{numReal(1), strReal("value")}},
	}, nil)

	// Try to update required column to NULL
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "notnull_test",
		Set: []*query.SetClause{
			{Column: "required", Value: &query.NullLiteral{}},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err == nil {
		t.Error("Should fail with NOT NULL constraint violation")
	} else {
		t.Logf("Expected error: %v", err)
	}
}

// TestCoverage_updateLockedCheckConstraint tests updateLocked CHECK constraint
func TestCoverage_updateLockedCheckConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with CHECK constraint
	c.CreateTable(&query.CreateTableStmt{
		Table: "check_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "amount", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "amount"},
				Operator: query.TokenGte,
				Right:    numReal(0),
			}},
		},
	})

	// Insert a row
	c.Insert(ctx, &query.InsertStmt{
		Table:   "check_test",
		Columns: []string{"id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Try to update amount to negative value
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "check_test",
		Set: []*query.SetClause{
			{Column: "amount", Value: numReal(-50)},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err == nil {
		t.Error("Should fail with CHECK constraint violation")
	} else {
		t.Logf("Expected error: %v", err)
	}
}

// TestCoverage_deleteLockedWithWhereError tests deleteLocked with WHERE error
func TestCoverage_deleteLockedWithWhereError(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "delete_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	})

	// Insert a row
	c.Insert(ctx, &query.InsertStmt{
		Table:   "delete_test",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Try delete with invalid WHERE (subquery that doesn't work)
	// This tests the error path in WHERE evaluation
	_, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "delete_test",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "nonexistent_col"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	// Should either succeed (no match) or fail gracefully
	if err != nil {
		t.Logf("Delete with invalid WHERE returned: %v", err)
	}
}

// TestCoverage_deleteWithUsingNoMatches tests deleteWithUsingLocked with no matching rows
func TestCoverage_deleteWithUsingNoMatches(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "customer_id", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "old_customers",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText},
		},
	})

	// Insert data - no matching customers
	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "old_customers",
		Columns: []string{"id", "status"},
		Values:  [][]query.Expression{{numReal(200), strReal("inactive")}},
	}, nil)

	// Delete using WHERE that won't match
	result, err := c.ExecuteQuery("DELETE FROM orders USING old_customers WHERE orders.customer_id = old_customers.id AND old_customers.status = 'inactive'")
	if err != nil {
		t.Logf("DELETE USING error: %v", err)
	} else {
		t.Logf("DELETE USING result: %+v", result)
	}
}

// TestCoverage_updateWithJoinLocked tests updateWithJoinLocked
func TestCoverage_updateWithJoinLocked(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "price", Type: query.TokenInteger},
			{Name: "category_id", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "categories",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "discount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "price", "category_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(100), numReal(10)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "categories",
		Columns: []string{"id", "discount"},
		Values:  [][]query.Expression{{numReal(10), numReal(10)}},
	}, nil)

	// UPDATE with FROM/JOIN
	result, err := c.ExecuteQuery("UPDATE products SET price = price - 10 FROM categories WHERE products.category_id = categories.id")
	if err != nil {
		t.Logf("UPDATE FROM error: %v", err)
	} else {
		t.Logf("UPDATE FROM result: %+v", result)
	}
}

// TestCoverage_executeSelectWithJoinCrossJoin tests CROSS JOIN
func TestCoverage_executeSelectWithJoinCrossJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "colors",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "sizes",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "colors",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Red")}, {numReal(2), strReal("Blue")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sizes",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Small")}, {numReal(2), strReal("Large")}},
	}, nil)

	// Test CROSS JOIN
	result, err := c.ExecuteQuery("SELECT colors.name, sizes.name FROM colors CROSS JOIN sizes")
	if err != nil {
		t.Logf("CROSS JOIN error: %v", err)
	} else {
		t.Logf("CROSS JOIN returned %d rows", len(result.Rows))
		if len(result.Rows) != 4 {
			t.Errorf("Expected 4 rows for CROSS JOIN, got %d", len(result.Rows))
		}
	}
}

// TestCoverage_executeSelectWithJoinRightJoin tests RIGHT JOIN
func TestCoverage_executeSelectWithJoinRightJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept_id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "departments",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data - employee with no dept, dept with no employees
	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id", "name"},
		Values:  [][]query.Expression{{numReal(1), numReal(10), strReal("Alice")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "departments",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(10), strReal("Sales")}, {numReal(20), strReal("IT")}},
	}, nil)

	// Test RIGHT JOIN
	result, err := c.ExecuteQuery("SELECT employees.name, departments.name FROM employees RIGHT JOIN departments ON employees.dept_id = departments.id")
	if err != nil {
		t.Logf("RIGHT JOIN error: %v", err)
	} else {
		t.Logf("RIGHT JOIN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_executeSelectWithJoinCTE tests JOIN with CTE
func TestCoverage_executeSelectWithJoinCTE(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "main_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "main_data",
		Columns: []string{"id", "value"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(200)}},
	}, nil)

	// Test JOIN with CTE
	result, err := c.ExecuteQuery(`
		WITH cte AS (SELECT id, value * 2 as doubled FROM main_data)
		SELECT m.id, c.doubled
		FROM main_data m
		JOIN cte c ON m.id = c.id
	`)
	if err != nil {
		t.Logf("JOIN with CTE error: %v", err)
	} else {
		t.Logf("JOIN with CTE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByDottedCol tests complex JOIN+GROUP BY with dotted column
func TestCoverage_executeSelectWithJoinAndGroupByDottedCol(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "customer_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "customers",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "region", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{numReal(1), strReal("North")}, {numReal(2), strReal("South")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id", "amount"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), numReal(100)},
			{numReal(2), numReal(1), numReal(200)},
			{numReal(3), numReal(2), numReal(150)},
		},
	}, nil)

	// Test JOIN with GROUP BY and dotted column reference
	result, err := c.ExecuteQuery(`
		SELECT customers.region, SUM(orders.amount) as total
		FROM orders
		JOIN customers ON orders.customer_id = customers.id
		GROUP BY customers.region
	`)
	if err != nil {
		t.Logf("JOIN+GROUP BY with dotted ref error: %v", err)
	} else {
		t.Logf("JOIN+GROUP BY returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("Row: %v", row)
		}
	}
}

// TestCoverage_executeScalarSelectSubquery tests executeScalarSelect with subquery
func TestCoverage_executeScalarSelectSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "data",
		Columns: []string{"id", "value"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Test scalar select
	result, err := c.ExecuteQuery("SELECT COUNT(*) FROM data")
	if err != nil {
		t.Fatalf("Scalar SELECT error: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestCoverage_updateLockedFKConstraint tests updateLocked with FK constraint
func TestCoverage_updateLockedFKConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent table
	c.CreateTable(&query.CreateTableStmt{
		Table: "parents",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Create child table with FK inline
	c.CreateTable(&query.CreateTableStmt{
		Table: "children",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "parents",
				ReferencedColumns: []string{"id"},
				OnDelete:          "RESTRICT",
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "parents",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "children",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// Update parent id (should cascade)
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "parents",
		Set: []*query.SetClause{
			{Column: "id", Value: numReal(10)},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err != nil {
		t.Logf("UPDATE with CASCADE error: %v", err)
	}
}
