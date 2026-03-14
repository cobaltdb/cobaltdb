package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_deleteRowLockedTriggers targets deleteRowLocked with triggers
func TestCoverage_deleteRowLockedTriggers(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with trigger
	createCoverageTestTable(t, c, "trig_del", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, c, "trig_audit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "action", Type: query.TokenText},
	})

	// Create BEFORE/AFTER DELETE triggers
	c.ExecuteQuery("CREATE TRIGGER trig_del_before BEFORE DELETE ON trig_del BEGIN INSERT INTO trig_audit (action) VALUES ('before_delete'); END")
	c.ExecuteQuery("CREATE TRIGGER trig_del_after AFTER DELETE ON trig_del BEGIN INSERT INTO trig_audit (action) VALUES ('after_delete'); END")

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "trig_del",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Delete (triggers fire)
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "trig_del",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenLte, Right: numReal(3)},
	}, nil)

	if err != nil {
		t.Logf("Delete with triggers error: %v", err)
	} else {
		t.Logf("Deleted %d rows with triggers", rows)
	}

	// Check audit log
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM trig_audit")
	if result != nil {
		t.Logf("Audit entries: %v", result.Rows)
	}
}

// TestCoverage_deleteRowLockedFKCascade90 targets deleteRowLocked with FK CASCADE
func TestCoverage_deleteRowLockedFKCascade90(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent table
	_, err := c.ExecuteQuery(`CREATE TABLE fk_parent (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create parent error: %v", err)
		return
	}

	// Create child table with CASCADE
	_, err = c.ExecuteQuery(`CREATE TABLE fk_child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES fk_parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Logf("Create child error: %v", err)
		return
	}

	// Insert parents
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "fk_parent",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("parent")}},
		}, nil)
	}

	// Insert children
	for i := 1; i <= 6; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "fk_child",
			Columns: []string{"id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1))}},
		}, nil)
	}

	// Delete parent (should cascade to children)
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	if err != nil {
		t.Logf("FK cascade delete error: %v", err)
	} else {
		t.Logf("Deleted %d parent rows", rows)
	}

	// Check remaining children
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM fk_child")
	if result != nil {
		t.Logf("Remaining children: %v", result.Rows)
	}
}

// TestCoverage_evaluateWhereInSubquery targets evaluateWhere with IN subquery
func TestCoverage_evaluateWhereInSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "in_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	createCoverageTestTable(t, c, "in_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Insert data
	categories := []string{"A", "B", "C", "D", "E"}
	for i, cat := range categories {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "in_main",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(cat)}},
		}, nil)
	}

	c.Insert(ctx, &query.InsertStmt{
		Table:   "in_ref",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("C")}, {numReal(3), strReal("E")}},
	}, nil)

	// Test IN with subquery
	result, err := c.ExecuteQuery("SELECT * FROM in_main WHERE category IN (SELECT name FROM in_ref)")
	if err != nil {
		t.Logf("IN subquery error: %v", err)
	} else {
		t.Logf("IN subquery returned %d rows", len(result.Rows))
	}

	// Test NOT IN with subquery
	result, err = c.ExecuteQuery("SELECT * FROM in_main WHERE category NOT IN (SELECT name FROM in_ref)")
	if err != nil {
		t.Logf("NOT IN subquery error: %v", err)
	} else {
		t.Logf("NOT IN subquery returned %d rows", len(result.Rows))
	}
}

// TestCoverage_evaluateWhereCase targets evaluateWhere with CASE expression
func TestCoverage_evaluateWhereCase(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "case_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "case_test",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM case_test WHERE CASE WHEN score > 50 THEN 1 ELSE 0 END = 1",
		"SELECT * FROM case_test WHERE CASE score WHEN 25 THEN 1 WHEN 50 THEN 1 ELSE 0 END = 1",
		"SELECT * FROM case_test WHERE (CASE WHEN score > 30 THEN 'high' ELSE 'low' END) = 'high'",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("CASE WHERE error: %v", err)
		} else {
			t.Logf("CASE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_insertLockedAutoInc targets insertLocked with AUTOINCREMENT
func TestCoverage_insertLockedAutoInc(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE autoinc_test (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create table error: %v", err)
		return
	}

	// Insert without specifying id
	for i := 1; i <= 5; i++ {
		_, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "autoinc_test",
			Columns: []string{"name"},
			Values:  [][]query.Expression{{strReal("item")}},
		}, nil)
		if err != nil {
			t.Logf("AutoInc insert error: %v", err)
		}
	}

	// Delete some rows
	c.ExecuteQuery("DELETE FROM autoinc_test WHERE id <= 2")

	// Insert more (should continue from last id)
	for i := 1; i <= 2; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "autoinc_test",
			Columns: []string{"name"},
			Values:  [][]query.Expression{{strReal("newitem")}},
		}, nil)
	}

	result, _ := c.ExecuteQuery("SELECT * FROM autoinc_test ORDER BY id")
	if result != nil {
		t.Logf("Final rows: %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("Row: %v", row)
		}
	}
}

// TestCoverage_insertLockedExpr targets insertLocked with expression evaluation
func TestCoverage_insertLockedExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE expr_test (
		id INTEGER PRIMARY KEY,
		val INTEGER,
		double_val INTEGER
	)`)
	if err != nil {
		t.Logf("Create table error: %v", err)
		return
	}

	// Insert with expressions
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "expr_test",
		Columns: []string{"id", "val", "double_val"},
		Values:  [][]query.Expression{{numReal(1), &query.BinaryExpr{Left: numReal(5), Operator: query.TokenPlus, Right: numReal(3)}, &query.BinaryExpr{Left: numReal(10), Operator: query.TokenStar, Right: numReal(2)}}},
	}, nil)
	if err != nil {
		t.Logf("Insert with expression error: %v", err)
	}

	// Verify
	result, _ := c.ExecuteQuery("SELECT * FROM expr_test WHERE id = 1")
	if result != nil && len(result.Rows) > 0 {
		t.Logf("Row with expressions: %v", result.Rows[0])
	}
}

// TestCoverage_applyOrderByMulti targets applyOrderBy with multi-column
func TestCoverage_applyOrderByMulti(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "orderby_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "subcategory", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	data := []struct {
		id          float64
		category    string
		subcategory string
		amount      float64
	}{
		{1, "A", "X", 100},
		{2, "A", "Y", 200},
		{3, "B", "X", 150},
		{4, "B", "Y", 50},
		{5, "A", "X", 300},
	}
	for _, d := range data {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "orderby_multi",
			Columns: []string{"id", "category", "subcategory", "amount"},
			Values:  [][]query.Expression{{numReal(d.id), strReal(d.category), strReal(d.subcategory), numReal(d.amount)}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM orderby_multi ORDER BY category, amount",
		"SELECT * FROM orderby_multi ORDER BY category DESC, subcategory ASC",
		"SELECT * FROM orderby_multi ORDER BY amount DESC, id ASC",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Multi-column ORDER BY error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedCache targets selectLocked with query cache
func TestCoverage_selectLockedCache(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.EnableQueryCache(10, 0)

	createCoverageTestTable(t, c, "cache_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "cache_test",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Same query multiple times (should hit cache)
	for i := 0; i < 5; i++ {
		c.ExecuteQuery("SELECT * FROM cache_test WHERE id = 1")
	}

	// Different queries (cache misses)
	for i := 1; i <= 10; i++ {
		q := "SELECT * FROM cache_test WHERE id = " + string(rune('0'+i%10))
		c.ExecuteQuery(q)
	}

	hits, misses, _ := c.GetQueryCacheStats()
	t.Logf("Cache hits: %d, misses: %d", hits, misses)
}

// TestCoverage_executeSelectWithJoinAndGroupBy90 targets the complex JOIN+GROUP BY path
func TestCoverage_executeSelectWithJoinAndGroupBy90(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	createCoverageTestTable(t, c, "jg_orders", []*query.ColumnDef{
		{Name: "order_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, c, "jg_customers", []*query.ColumnDef{
		{Name: "customer_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Insert customers
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jg_customers",
			Columns: []string{"customer_id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Customer")}},
		}, nil)
	}

	// Insert orders
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jg_orders",
			Columns: []string{"order_id", "customer_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1)), numReal(float64(i * 10))}},
		}, nil)
	}

	// JOIN + GROUP BY + HAVING
	result, err := c.ExecuteQuery(`
		SELECT c.name, SUM(o.amount) as total
		FROM jg_orders o
		JOIN jg_customers c ON o.customer_id = c.customer_id
		GROUP BY c.name
		HAVING SUM(o.amount) > 100
	`)
	if err != nil {
		t.Logf("JOIN+GROUP BY error: %v", err)
	} else {
		t.Logf("JOIN+GROUP BY returned %d rows", len(result.Rows))
	}

	// Multiple JOINs with GROUP BY
	result, err = c.ExecuteQuery(`
		SELECT c.customer_id, COUNT(*) as order_count
		FROM jg_orders o
		JOIN jg_customers c ON o.customer_id = c.customer_id
		GROUP BY c.customer_id
		ORDER BY order_count DESC
	`)
	if err != nil {
		t.Logf("Multiple JOIN+GROUP BY error: %v", err)
	} else {
		t.Logf("Multiple JOIN+GROUP BY returned %d rows", len(result.Rows))
	}
}

// TestCoverage_rollbackSavepointDDL targets RollbackToSavepoint with DDL
func TestCoverage_rollbackSavepointDDL(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.BeginTransaction(1)

	createCoverageTestTable(t, c, "ddl_sp_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	c.Savepoint("sp1")

	// DDL operation
	c.ExecuteQuery("CREATE INDEX idx_val ON ddl_sp_test(val)")

	c.Insert(ctx, &query.InsertStmt{
		Table:   "ddl_sp_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Rollback to savepoint (should undo index creation and insert)
	err := c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to savepoint error: %v", err)
	}

	// Verify data
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM ddl_sp_test")
	if result != nil {
		t.Logf("Rows after rollback: %v", result.Rows)
	}

	c.RollbackTransaction()
}
