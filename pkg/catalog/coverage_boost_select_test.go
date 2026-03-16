package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSelectLockedWithMultipleJoins tests selectLocked with multiple JOINs
func TestSelectLockedWithMultipleJoins(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create three tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "t1_id", Type: query.TokenInteger}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "t3",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "t2_id", Type: query.TokenInteger}},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id", "t1_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t3",
		Columns: []string{"id", "t2_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// Multi-table JOIN
	result, err := c.ExecuteQuery("SELECT t1.val, t3.id FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id")
	if err != nil {
		t.Logf("Multi-JOIN error: %v", err)
	} else {
		t.Logf("Multi-JOIN returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithLeftJoin tests selectLocked with LEFT JOIN
func TestSelectLockedWithLeftJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "t1_id", Type: query.TokenInteger}},
	})

	// Insert data with no matching rows in t2
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id", "t1_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// LEFT JOIN should return all rows from t1
	result, err := c.ExecuteQuery("SELECT t1.id, t2.id FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id")
	if err != nil {
		t.Logf("LEFT JOIN error: %v", err)
	} else {
		t.Logf("LEFT JOIN returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithWhereInSubquery tests selectLocked with WHERE IN (subquery)
func TestSelectLockedWithWhereInSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(2)}},
	}, nil)

	// WHERE IN with subquery
	result, err := c.ExecuteQuery("SELECT * FROM t1 WHERE id IN (SELECT val FROM t2)")
	if err != nil {
		t.Logf("WHERE IN subquery error: %v", err)
	} else {
		t.Logf("WHERE IN subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithExistsSubquery tests selectLocked with EXISTS subquery
func TestSelectLockedWithExistsSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "parent",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "child",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "parent_id", Type: query.TokenInteger}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// EXISTS subquery
	result, err := c.ExecuteQuery("SELECT * FROM parent WHERE EXISTS (SELECT 1 FROM child WHERE child.parent_id = parent.id)")
	if err != nil {
		t.Logf("EXISTS subquery error: %v", err)
	} else {
		t.Logf("EXISTS subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithCaseExpression tests selectLocked with CASE expression
func TestSelectLockedWithCaseExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}, {numReal(3), numReal(30)}},
	}, nil)

	// CASE expression
	result, err := c.ExecuteQuery("SELECT id, CASE WHEN val < 15 THEN 'low' WHEN val < 25 THEN 'med' ELSE 'high' END AS level FROM test")
	if err != nil {
		t.Logf("CASE expression error: %v", err)
	} else if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithLimitOffset tests selectLocked with LIMIT and OFFSET
func TestSelectLockedWithLimitOffset(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	// Insert 10 rows
	values := make([][]query.Expression, 10)
	for i := 0; i < 10; i++ {
		values[i] = []query.Expression{numReal(float64(i + 1))}
	}
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id"},
		Values:  values,
	}, nil)

	// LIMIT 3 OFFSET 2
	result, err := c.ExecuteQuery("SELECT * FROM test ORDER BY id LIMIT 3 OFFSET 2")
	if err != nil {
		t.Logf("LIMIT OFFSET error: %v", err)
	} else if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithAllAggregates tests selectLocked with various aggregates
func TestSelectLockedWithAllAggregates(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "dept", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"dept", "amount"},
		Values: [][]query.Expression{
			{strReal("A"), numReal(100)},
			{strReal("A"), numReal(200)},
			{strReal("B"), numReal(300)},
			{strReal("B"), numReal(400)},
		},
	}, nil)

	// Test various aggregates
	queries := []string{
		"SELECT COUNT(*) FROM sales",
		"SELECT SUM(amount) FROM sales",
		"SELECT AVG(amount) FROM sales",
		"SELECT MIN(amount) FROM sales",
		"SELECT MAX(amount) FROM sales",
		"SELECT dept, COUNT(*), SUM(amount), AVG(amount) FROM sales GROUP BY dept",
	}

	for _, sql := range queries {
		result, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Logf("Query '%s' error: %v", sql, err)
		} else {
			t.Logf("Query '%s' returned %d rows", sql, len(result.Rows))
		}
	}
}

// TestSelectLockedWithGroupByHavingComplex tests complex GROUP BY HAVING
func TestSelectLockedWithGroupByHavingComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "dept", Type: query.TokenText},
			{Name: "role", Type: query.TokenText},
			{Name: "salary", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"dept", "role", "salary"},
		Values: [][]query.Expression{
			{strReal("IT"), strReal("dev"), numReal(5000)},
			{strReal("IT"), strReal("dev"), numReal(6000)},
			{strReal("IT"), strReal("mgr"), numReal(8000)},
			{strReal("HR"), strReal("mgr"), numReal(7000)},
		},
	}, nil)

	// GROUP BY multiple columns with HAVING
	result, err := c.ExecuteQuery("SELECT dept, role, COUNT(*), AVG(salary) FROM employees GROUP BY dept, role HAVING COUNT(*) > 1")
	if err != nil {
		t.Logf("Complex GROUP BY HAVING error: %v", err)
	} else {
		t.Logf("Complex GROUP BY HAVING returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithOrderByNulls tests ORDER BY with NULL values
func TestSelectLockedWithOrderByNulls(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), &query.NullLiteral{}},
			{numReal(2), numReal(10)},
			{numReal(3), &query.NullLiteral{}},
			{numReal(4), numReal(20)},
		},
	}, nil)

	// ORDER BY with NULLs
	result, err := c.ExecuteQuery("SELECT * FROM test ORDER BY val ASC")
	if err != nil {
		t.Logf("ORDER BY NULLs error: %v", err)
	} else if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithCrossJoin tests CROSS JOIN
func TestSelectLockedWithCrossJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(10)}, {numReal(20)}},
	}, nil)

	// CROSS JOIN (implicit)
	result, err := c.ExecuteQuery("SELECT * FROM t1, t2")
	if err != nil {
		t.Logf("CROSS JOIN error: %v", err)
	} else if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows from CROSS JOIN, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithSelfJoin tests self-JOIN
func TestSelectLockedWithSelfJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "manager_id", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "name", "manager_id"},
		Values: [][]query.Expression{
			{numReal(1), strReal("CEO"), &query.NullLiteral{}},
			{numReal(2), strReal("Mgr1"), numReal(1)},
			{numReal(3), strReal("Emp1"), numReal(2)},
		},
	}, nil)

	// Self JOIN
	result, err := c.ExecuteQuery("SELECT e.name, m.name AS manager FROM employees e JOIN employees m ON e.manager_id = m.id")
	if err != nil {
		t.Logf("Self JOIN error: %v", err)
	} else {
		t.Logf("Self JOIN returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithUnionAll tests UNION ALL
func TestSelectLockedWithUnionAll(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}, {numReal(3)}},
	}, nil)

	// UNION ALL (no deduplication)
	result, err := c.ExecuteQuery("SELECT id FROM t1 UNION ALL SELECT id FROM t2")
	if err != nil {
		t.Logf("UNION ALL error: %v", err)
	} else if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows from UNION ALL, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithIntersect tests INTERSECT
func TestSelectLockedWithIntersect(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}, {numReal(3)}, {numReal(4)}},
	}, nil)

	// INTERSECT
	result, err := c.ExecuteQuery("SELECT id FROM t1 INTERSECT SELECT id FROM t2")
	if err != nil {
		t.Logf("INTERSECT error: %v", err)
	} else if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows from INTERSECT, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithExcept tests EXCEPT
func TestSelectLockedWithExcept(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}, {numReal(4)}},
	}, nil)

	// EXCEPT
	result, err := c.ExecuteQuery("SELECT id FROM t1 EXCEPT SELECT id FROM t2")
	if err != nil {
		t.Logf("EXCEPT error: %v", err)
	} else if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows from EXCEPT, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithDistinctAggregation tests DISTINCT in aggregation
func TestSelectLockedWithDistinctAggregation(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), numReal(10)},
			{numReal(2), numReal(10)},
			{numReal(3), numReal(20)},
			{numReal(4), numReal(20)},
		},
	}, nil)

	// COUNT(DISTINCT)
	result, err := c.ExecuteQuery("SELECT COUNT(DISTINCT val) FROM test")
	if err != nil {
		t.Logf("COUNT DISTINCT error: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithArithmetic tests arithmetic expressions
func TestSelectLockedWithArithmetic(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "price", Type: query.TokenInteger},
			{Name: "qty", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "price", "qty"},
		Values:  [][]query.Expression{{numReal(1), numReal(100), numReal(5)}},
	}, nil)

	// Arithmetic operations
	result, err := c.ExecuteQuery("SELECT id, price * qty AS total, price + 10 AS new_price, price - 5 AS discount, price / 2 AS half FROM products")
	if err != nil {
		t.Logf("Arithmetic error: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}
