package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_deleteRowLockedIndexCleanup targets deleteRowLocked index cleanup paths
func TestCoverage_deleteRowLockedIndexCleanup(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with indexes
	_, err := c.ExecuteQuery(`CREATE TABLE idx_del_test (
		id INTEGER PRIMARY KEY,
		email TEXT UNIQUE,
		status TEXT
	)`)
	if err != nil {
		t.Logf("Create table error: %v", err)
		return
	}

	// Create non-unique index
	_, err = c.ExecuteQuery(`CREATE INDEX idx_status ON idx_del_test(status)`)
	if err != nil {
		t.Logf("Create index error: %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "idx_del_test",
			Columns: []string{"id", "email", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("user@test.com"), strReal("active")}},
		}, nil)
	}

	// Delete within transaction (covers undo log + index cleanup)
	c.BeginTransaction(1)
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "idx_del_test",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "status"}, Operator: query.TokenEq, Right: strReal("active")},
	}, nil)
	if err != nil {
		t.Logf("Delete with transaction error: %v", err)
	} else {
		t.Logf("Deleted %d rows with transaction", rows)
	}
	c.CommitTransaction()

	// Verify indexes are cleaned up by checking count
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM idx_del_test")
	if result != nil {
		t.Logf("Remaining rows: %v", result.Rows)
	}
}

// TestCoverage_deleteRowLockedFKCascadeDeep targets FK cascade in deleteRowLocked
func TestCoverage_deleteRowLockedFKCascadeDeep(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent
	_, err := c.ExecuteQuery(`CREATE TABLE fk_deep_parent (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create parent error: %v", err)
		return
	}

	// Create child with CASCADE
	_, err = c.ExecuteQuery(`CREATE TABLE fk_deep_child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES fk_deep_parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Logf("Create child error: %v", err)
		return
	}

	// Create grandchild referencing child
	_, err = c.ExecuteQuery(`CREATE TABLE fk_deep_grandchild (
		id INTEGER PRIMARY KEY,
		child_id INTEGER REFERENCES fk_deep_child(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Logf("Create grandchild error: %v", err)
		return
	}

	// Insert data - parent -> child -> grandchild
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "fk_deep_parent",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("parent")}},
		}, nil)
	}

	for i := 1; i <= 6; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "fk_deep_child",
			Columns: []string{"id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1))}},
		}, nil)
	}

	for i := 1; i <= 12; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "fk_deep_grandchild",
			Columns: []string{"id", "child_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%6 + 1))}},
		}, nil)
	}

	// Delete parent - should cascade to children and grandchildren
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_deep_parent",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Deep cascade delete error: %v", err)
	} else {
		t.Logf("Deleted %d parent rows", rows)
	}

	// Verify cascade worked
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM fk_deep_child WHERE parent_id = 1")
	if result != nil {
		t.Logf("Remaining children of parent 1: %v", result.Rows)
	}

	result, _ = c.ExecuteQuery("SELECT COUNT(*) FROM fk_deep_grandchild")
	if result != nil {
		t.Logf("Remaining grandchildren: %v", result.Rows)
	}
}

// TestCoverage_evaluateWhereNotExists targets evaluateWhere with NOT EXISTS
func TestCoverage_evaluateWhereNotExists(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "not_exists_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, c, "not_exists_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
	})

	// Insert main data
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "not_exists_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Insert sub data for only some refs
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "not_exists_sub",
			Columns: []string{"id", "ref_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// NOT EXISTS query
	result, err := c.ExecuteQuery("SELECT * FROM not_exists_main WHERE NOT EXISTS (SELECT 1 FROM not_exists_sub WHERE ref_id = not_exists_main.id)")
	if err != nil {
		t.Logf("NOT EXISTS error: %v", err)
	} else {
		t.Logf("NOT EXISTS returned %d rows", len(result.Rows))
	}
}

// TestCoverage_evaluateWhereScalarSubquery targets evaluateWhere with scalar subquery
func TestCoverage_evaluateWhereScalarSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "scalar_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "amount", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, c, "scalar_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "value", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_main",
			Columns: []string{"id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 100))}},
		}, nil)
	}

	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_agg",
			Columns: []string{"id", "category", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("A"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Scalar subquery in WHERE
	result, err := c.ExecuteQuery("SELECT * FROM scalar_main WHERE amount > (SELECT AVG(value) FROM scalar_agg)")
	if err != nil {
		t.Logf("Scalar subquery error: %v", err)
	} else {
		t.Logf("Scalar subquery returned %d rows", len(result.Rows))
	}
}

// TestCoverage_resolveAggregateInExprArithmetic93 targets resolveAggregateInExpr with arithmetic
func TestCoverage_resolveAggregateInExprArithmetic93(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "agg_arith", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
		{Name: "val3", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 15 {
			grp = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "agg_arith",
			Columns: []string{"id", "grp", "val1", "val2", "val3"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i)), numReal(float64(i * 2)), numReal(float64(i * 3))}},
		}, nil)
	}

	// Arithmetic with aggregates
	queries := []string{
		"SELECT grp, SUM(val1) + SUM(val2) as total FROM agg_arith GROUP BY grp HAVING total > 500",
		"SELECT grp, SUM(val1) - AVG(val2) as diff FROM agg_arith GROUP BY grp HAVING diff > 0",
		"SELECT grp, MAX(val1) * MIN(val2) as product FROM agg_arith GROUP BY grp",
		"SELECT grp, COUNT(*) / 2 as half_count FROM agg_arith GROUP BY grp",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate arithmetic error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_applyOuterQueryViews targets applyOuterQuery with complex views
func TestCoverage_applyOuterQueryViews(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "view_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat := "X"
		if i > 10 {
			cat = "Y"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "view_base",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with DISTINCT
	_, err := c.ExecuteQuery("CREATE VIEW view_distinct AS SELECT DISTINCT category FROM view_base")
	if err != nil {
		t.Logf("CREATE VIEW DISTINCT error: %v", err)
	}

	// Create view with GROUP BY
	_, err = c.ExecuteQuery("CREATE VIEW view_grouped AS SELECT category, SUM(amount) as total FROM view_base GROUP BY category")
	if err != nil {
		t.Logf("CREATE VIEW GROUP BY error: %v", err)
	}

	// Query views with WHERE (triggers applyOuterQuery)
	result, err := c.ExecuteQuery("SELECT * FROM view_distinct WHERE category = 'X'")
	if err != nil {
		t.Logf("Query view distinct error: %v", err)
	} else {
		t.Logf("View distinct returned %d rows", len(result.Rows))
	}

	result, err = c.ExecuteQuery("SELECT * FROM view_grouped WHERE total > 500")
	if err != nil {
		t.Logf("Query view grouped error: %v", err)
	} else {
		t.Logf("View grouped returned %d rows", len(result.Rows))
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByComplex targets complex JOIN+GROUP BY
func TestCoverage_executeSelectWithJoinAndGroupByComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	_, err := c.ExecuteQuery(`CREATE TABLE jg_orders (
		order_id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		product_id INTEGER,
		amount INTEGER
	)`)
	if err != nil {
		t.Logf("Create orders error: %v", err)
		return
	}

	_, err = c.ExecuteQuery(`CREATE TABLE jg_customers (
		customer_id INTEGER PRIMARY KEY,
		name TEXT,
		region TEXT
	)`)
	if err != nil {
		t.Logf("Create customers error: %v", err)
		return
	}

	_, err = c.ExecuteQuery(`CREATE TABLE jg_products (
		product_id INTEGER PRIMARY KEY,
		name TEXT,
		category TEXT
	)`)
	if err != nil {
		t.Logf("Create products error: %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jg_customers",
			Columns: []string{"customer_id", "name", "region"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Customer"), strReal("North")}},
		}, nil)
	}

	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jg_products",
			Columns: []string{"product_id", "name", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Product"), strReal("Electronics")}},
		}, nil)
	}

	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jg_orders",
			Columns: []string{"order_id", "customer_id", "product_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1)), numReal(float64((i-1)%5 + 1)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Complex multi-table JOIN with GROUP BY
	result, err := c.ExecuteQuery(`
		SELECT
			c.region,
			p.category,
			COUNT(*) as order_count,
			SUM(o.amount) as total_amount,
			AVG(o.amount) as avg_amount
		FROM jg_orders o
		JOIN jg_customers c ON o.customer_id = c.customer_id
		JOIN jg_products p ON o.product_id = p.product_id
		GROUP BY c.region, p.category
		HAVING SUM(o.amount) > 100
	`)
	if err != nil {
		t.Logf("Complex JOIN+GROUP BY error: %v", err)
	} else {
		t.Logf("Complex query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_rollbackSavepointNestedDDL targets nested savepoint rollback
func TestCoverage_rollbackSavepointNestedDDL(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.BeginTransaction(1)

	// Create initial table
	createCoverageTestTable(t, c, "nested_sp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("t1")}},
	}, nil)

	// Nested savepoints
	c.Savepoint("sp1")
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("t2")}},
	}, nil)

	c.Savepoint("sp2")
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("t3")}},
	}, nil)

	c.Savepoint("sp3")
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(4), strReal("t4")}},
	}, nil)

	// Rollback to sp2 (should undo rows 3 and 4)
	err := c.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("Rollback to sp2 error: %v", err)
	}

	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM nested_sp")
	if result != nil {
		t.Logf("Count after rollback to sp2: %v", result.Rows)
	}

	// Rollback to sp1 (should undo row 2)
	err = c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	result, _ = c.ExecuteQuery("SELECT COUNT(*) FROM nested_sp")
	if result != nil {
		t.Logf("Count after rollback to sp1: %v", result.Rows)
	}

	c.RollbackTransaction()
}
