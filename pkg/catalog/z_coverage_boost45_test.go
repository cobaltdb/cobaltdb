package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_InsertLockedExtended targets insertLocked more deeply
func TestCoverage_InsertLockedExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	// Single insert
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_ext",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(1), numReal(100), strReal("test")}},
	}, nil)

	// Multi-row insert
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_ext",
		Columns: []string{"id", "a", "b"},
		Values: [][]query.Expression{
			{numReal(2), numReal(200), strReal("a")},
			{numReal(3), numReal(300), strReal("b")},
			{numReal(4), numReal(400), strReal("c")},
		},
	}, nil)

	// Insert with transaction
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_ext",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(5), numReal(500), strReal("txn")}},
	}, nil)
	cat.CommitTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_ext")
	t.Logf("Count after inserts: %v", result.Rows)
}

// TestCoverage_DeleteRowLockedExtended targets deleteRowLocked more deeply
func TestCoverage_DeleteRowLockedExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_row_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_row_ext",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Delete with simple condition
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row_ext",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(5),
		},
	}, nil)

	// Delete with range condition
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row_ext",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    numReal(15),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_row_ext")
	t.Logf("Count after deletes: %v", result.Rows)
}

// TestCoverage_UpdateLockedExtended targets updateLocked more deeply
func TestCoverage_UpdateLockedExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_ext",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("orig")}},
		}, nil)
	}

	// Update single row
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_ext",
		Set:   []*query.SetClause{{Column: "a", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	// Update with condition
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_ext",
		Set:   []*query.SetClause{{Column: "b", Value: strReal("updated")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    numReal(10),
		},
	}, nil)

	// Update multiple columns
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_ext",
		Set: []*query.SetClause{
			{Column: "a", Value: numReal(0)},
			{Column: "b", Value: strReal("zero")},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(2),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_ext WHERE b = 'updated'")
	t.Logf("Count after updates: %v", result.Rows)
}

// TestCoverage_DeleteLockedExtended targets deleteLocked more deeply
func TestCoverage_DeleteLockedExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_lock_ext", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 25; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_lock_ext",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	// Delete in transaction
	cat.BeginTransaction(1)
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_lock_ext",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGt,
			Right:    numReal(20),
		},
	}, nil)
	cat.CommitTransaction()

	// Delete without transaction
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_lock_ext",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLt,
			Right:    numReal(5),
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_lock_ext")
	t.Logf("Count after deletes: %v", result.Rows)
}

// TestCoverage_JoinWithGroupAggregate targets evaluateExprWithGroupAggregatesJoin
func TestCoverage_JoinWithGroupAggregate(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jga_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cust_id", Type: query.TokenInteger},
		{Name: "amt", Type: query.TokenInteger},
		{Name: "qty", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jga_custs", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jga_custs",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{numReal(1), strReal("East")}, {numReal(2), strReal("West")}},
	}, nil)

	for i := 1; i <= 40; i++ {
		custID := 1
		if i > 20 {
			custID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jga_orders",
			Columns: []string{"id", "cust_id", "amt", "qty"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(custID)), numReal(float64(i * 50)), numReal(float64(i % 10 + 1))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, SUM(o.amt) as total, AVG(o.amt) as avg_amt FROM jga_orders o JOIN jga_custs c ON o.cust_id = c.id GROUP BY c.region HAVING total > 20000",
		"SELECT c.region, SUM(o.amt * o.qty) as weighted FROM jga_orders o JOIN jga_custs c ON o.cust_id = c.id GROUP BY c.region",
		"SELECT c.region, COUNT(*) as cnt, SUM(o.amt) as total FROM jga_orders o JOIN jga_custs c ON o.cust_id = c.id GROUP BY c.region HAVING cnt > 10 AND total > 10000",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN GROUP AGG error: %v", err)
		} else {
			t.Logf("JOIN GROUP AGG returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_EvaluateLikeMore targets evaluateLike
func TestCoverage_EvaluateLikeMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "like_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "txt", Type: query.TokenText},
	})

	values := []string{"hello", "world", "hello world", "HELLO", "test", "testing", "best"}
	for i, v := range values {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "like_test",
			Columns: []string{"id", "txt"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(v)}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM like_test WHERE txt LIKE 'hello%'",
		"SELECT * FROM like_test WHERE txt LIKE '%world'",
		"SELECT * FROM like_test WHERE txt LIKE '%ell%'",
		"SELECT * FROM like_test WHERE txt LIKE 'test%'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIKE error: %v", err)
		} else {
			t.Logf("LIKE returned %d rows", len(result.Rows))
		}
	}
}
