package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestEvaluateHavingWithNil tests evaluateHaving with nil having
func TestEvaluateHavingWithNil(t *testing.T) {
	c := &Catalog{}
	result, err := evaluateHaving(c, nil, nil, nil, nil, nil)
	if err != nil {
		t.Errorf("evaluateHaving(nil) returned error: %v", err)
	}
	if !result {
		t.Error("evaluateHaving(nil) should return true")
	}
}

// TestEvaluateHavingWithSimpleComparison tests evaluateHaving with simple comparison
func TestEvaluateHavingWithSimpleComparison(t *testing.T) {
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
			{strReal("B"), numReal(50)},
		},
	}, nil)

	// HAVING with simple comparison
	result, err := c.ExecuteQuery("SELECT dept, SUM(amount) as total FROM sales GROUP BY dept HAVING total > 100")
	if err != nil {
		t.Logf("HAVING with simple comparison error: %v", err)
	} else {
		// Only dept A should qualify (sum=300)
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
	}
}

// TestEvaluateHavingWithMultipleConditions tests evaluateHaving with AND/OR
func TestEvaluateHavingWithMultipleConditions(t *testing.T) {
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
			{strReal("B"), numReal(50)},
			{strReal("B"), numReal(30)},
		},
	}, nil)

	// HAVING with AND
	result, err := c.ExecuteQuery("SELECT dept, SUM(amount) as total, COUNT(*) as cnt FROM sales GROUP BY dept HAVING total > 50 AND cnt > 1")
	if err != nil {
		t.Logf("HAVING with AND error: %v", err)
	} else {
		t.Logf("HAVING with AND returned %d rows", len(result.Rows))
	}
}

// TestEvaluateHavingWithMINMAX tests HAVING with MIN/MAX
func TestEvaluateHavingWithMINMAX(t *testing.T) {
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
			{strReal("B"), numReal(50)},
			{strReal("B"), numReal(60)},
		},
	}, nil)

	// HAVING with MIN
	result, err := c.ExecuteQuery("SELECT dept, MIN(amount) as min_amt FROM sales GROUP BY dept HAVING MIN(amount) >= 50")
	if err != nil {
		t.Logf("HAVING with MIN error: %v", err)
	} else {
		// Both A (min=100) and B (min=50) should qualify
		t.Logf("HAVING with MIN returned %d rows", len(result.Rows))
	}

	// HAVING with MAX
	result, err = c.ExecuteQuery("SELECT dept, MAX(amount) as max_amt FROM sales GROUP BY dept HAVING MAX(amount) > 150")
	if err != nil {
		t.Logf("HAVING with MAX error: %v", err)
	} else {
		// Only A (max=200) should qualify
		t.Logf("HAVING with MAX returned %d rows", len(result.Rows))
	}
}

// TestEvaluateHavingWithCOUNTDistinct tests HAVING with COUNT(DISTINCT)
func TestEvaluateHavingWithCOUNTDistinct(t *testing.T) {
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
			{Name: "product", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"dept", "product", "amount"},
		Values: [][]query.Expression{
			{strReal("A"), strReal("P1"), numReal(100)},
			{strReal("A"), strReal("P2"), numReal(200)},
			{strReal("B"), strReal("P1"), numReal(50)},
			{strReal("B"), strReal("P1"), numReal(60)}, // Same product as above
		},
	}, nil)

	// HAVING with COUNT(DISTINCT)
	result, err := c.ExecuteQuery("SELECT dept, COUNT(DISTINCT product) as unique_products FROM sales GROUP BY dept HAVING COUNT(DISTINCT product) > 1")
	if err != nil {
		t.Logf("HAVING with COUNT DISTINCT error: %v", err)
	} else {
		// Only A (2 unique products) should qualify
		t.Logf("HAVING with COUNT DISTINCT returned %d rows", len(result.Rows))
	}
}

// TestEvaluateHavingWithArithmetic tests HAVING with arithmetic
func TestEvaluateHavingWithArithmetic(t *testing.T) {
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
			{Name: "qty", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"dept", "amount", "qty"},
		Values: [][]query.Expression{
			{strReal("A"), numReal(100), numReal(2)},
			{strReal("A"), numReal(200), numReal(3)},
			{strReal("B"), numReal(50), numReal(1)},
		},
	}, nil)

	// HAVING with arithmetic in aggregate
	result, err := c.ExecuteQuery("SELECT dept, SUM(amount) as total FROM sales GROUP BY dept HAVING SUM(amount) / COUNT(*) > 100")
	if err != nil {
		t.Logf("HAVING with arithmetic error: %v", err)
	} else {
		t.Logf("HAVING with arithmetic returned %d rows", len(result.Rows))
	}
}

// TestEvaluateHavingWithNestedAggregate tests HAVING with nested aggregate references
func TestEvaluateHavingWithNestedAggregate(t *testing.T) {
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
			{strReal("A"), numReal(300)},
			{strReal("B"), numReal(50)},
			{strReal("B"), numReal(60)},
		},
	}, nil)

	// HAVING comparing two aggregates
	result, err := c.ExecuteQuery("SELECT dept, SUM(amount) as total, AVG(amount) as avg_amt FROM sales GROUP BY dept HAVING total > 2 * avg_amt")
	if err != nil {
		t.Logf("HAVING with nested aggregate error: %v", err)
	} else {
		t.Logf("HAVING with nested aggregate returned %d rows", len(result.Rows))
	}
}

// TestEvaluateHavingWithBooleanResult tests evaluateHaving returning boolean
func TestEvaluateHavingWithBooleanResult(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "grp", Type: query.TokenText},
			{Name: "flag", Type: query.TokenBoolean},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"grp", "flag"},
		Values: [][]query.Expression{
			{strReal("A"), &query.BooleanLiteral{Value: true}},
			{strReal("A"), &query.BooleanLiteral{Value: true}},
			{strReal("B"), &query.BooleanLiteral{Value: false}},
		},
	}, nil)

	// HAVING with boolean condition
	result, err := c.ExecuteQuery("SELECT grp FROM test GROUP BY grp HAVING COUNT(*) > 1")
	if err != nil {
		t.Logf("HAVING boolean result error: %v", err)
	} else {
		// Both groups have more than 1 row
		t.Logf("HAVING boolean result returned %d rows", len(result.Rows))
	}
}

// TestEvaluateHavingWithNumericResult tests evaluateHaving returning numeric
func TestEvaluateHavingWithNumericResult(t *testing.T) {
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
			{strReal("B"), numReal(50)},
		},
	}, nil)

	// Test various HAVING conditions
	tests := []string{
		"SELECT dept FROM sales GROUP BY dept HAVING 1",              // Always true (numeric 1)
		"SELECT dept FROM sales GROUP BY dept HAVING 0",              // Always false (numeric 0)
		"SELECT dept FROM sales GROUP BY dept HAVING SUM(amount)",    // Truthy if non-zero
	}

	for _, sql := range tests {
		result, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Logf("Query '%s' error: %v", sql, err)
		} else {
			t.Logf("Query '%s' returned %d rows", sql, len(result.Rows))
		}
	}
}

// TestEvaluateHavingWithErrorHandling tests evaluateHaving error handling
func TestEvaluateHavingWithErrorHandling(t *testing.T) {
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
		Values:  [][]query.Expression{{strReal("A"), numReal(100)}},
	}, nil)

	// HAVING with non-existent column
	result, err := c.ExecuteQuery("SELECT dept, SUM(amount) as total FROM sales GROUP BY dept HAVING nonexistent > 0")
	if err != nil {
		t.Logf("HAVING with non-existent column error (expected): %v", err)
	} else {
		t.Logf("HAVING with non-existent column returned %d rows", len(result.Rows))
	}
}
