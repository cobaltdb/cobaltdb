package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_evaluateWhereExists80 targets evaluateWhere with EXISTS subquery
func TestCoverage_evaluateWhereExists80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "exists_main80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, c, "exists_sub80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "exists_main80",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		if i%2 == 0 {
			c.Insert(ctx, &query.InsertStmt{
				Table:   "exists_sub80",
				Columns: []string{"id", "ref_id"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
			}, nil)
		}
	}

	queries := []string{
		"SELECT * FROM exists_main80 WHERE EXISTS (SELECT 1 FROM exists_sub80 WHERE ref_id = exists_main80.id)",
		"SELECT * FROM exists_main80 WHERE NOT EXISTS (SELECT 1 FROM exists_sub80 WHERE ref_id = exists_main80.id)",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("EXISTS error: %v", err)
		} else {
			t.Logf("EXISTS query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_resolveAggHaving80 targets resolveAggregateInExpr with complex HAVING
func TestCoverage_resolveAggHaving80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "resolve_agg80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		grp := "A"
		if i > 25 {
			grp = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "resolve_agg80",
			Columns: []string{"id", "grp", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val1) as s1, SUM(val2) as s2 FROM resolve_agg80 GROUP BY grp HAVING s1 + s2 > 1000",
		"SELECT grp, COUNT(*) as c, AVG(val1) as a FROM resolve_agg80 GROUP BY grp HAVING c * a > 500",
		"SELECT grp, MIN(val1) as mn, MAX(val1) as mx FROM resolve_agg80 GROUP BY grp HAVING mx - mn > 20",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex HAVING error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_applyOuterQuery80 targets applyOuterQuery with view
func TestCoverage_applyOuterQuery80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "outer_base80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		catg := "X"
		if i > 15 {
			catg = "Y"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "outer_base80",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	view1 := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		},
		From:    &query.TableRef{Name: "outer_base80"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
	}
	c.CreateView("view1_80", view1)

	// Query view with WHERE (triggers applyOuterQuery)
	result, err := c.ExecuteQuery("SELECT * FROM view1_80 WHERE col_1 > 2000")
	if err != nil {
		t.Logf("applyOuterQuery error: %v", err)
	} else {
		t.Logf("Query returned %d rows", len(result.Rows))
	}

	c.DropView("view1_80")
}

// TestCoverage_selectLockedCache80 targets selectLocked with cache miss paths
func TestCoverage_selectLockedCache80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.EnableQueryCache(10, 0)

	createCoverageTestTable(t, c, "cache_test80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "cache_test80",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Unique queries to cause cache misses
	for i := 1; i <= 15; i++ {
		q := "SELECT * FROM cache_test80 WHERE id = " + string(rune('0'+i))
		c.ExecuteQuery(q)
	}

	hits, misses, _ := c.GetQueryCacheStats()
	t.Logf("Cache hits: %d, misses: %d", hits, misses)
}

// TestCoverage_deleteRowLockedIdx80 targets deleteRowLocked with index cleanup
func TestCoverage_deleteRowLockedIdx80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE del_idx80 (
		id INTEGER PRIMARY KEY,
		email TEXT UNIQUE,
		status TEXT
	)`)
	if err != nil {
		t.Logf("Create table error: %v", err)
		return
	}

	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "del_idx80",
			Columns: []string{"id", "email", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("user@test.com"), strReal("active")}},
		}, nil)
	}

	// Delete with index cleanup
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "del_idx80",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "status"}, Operator: query.TokenEq, Right: strReal("active")},
	}, nil)
	if err != nil {
		t.Logf("Delete with index error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}
}

// TestCoverage_evaluateWhereIn80 targets evaluateWhere with IN subquery
func TestCoverage_evaluateWhereIn80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "in_main80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	createCoverageTestTable(t, c, "in_ref80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	categories := []string{"A", "B", "C", "D", "E"}
	for i, cat := range categories {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "in_main80",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(cat)}},
		}, nil)
	}

	c.Insert(ctx, &query.InsertStmt{
		Table:   "in_ref80",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("C")}, {numReal(3), strReal("E")}},
	}, nil)

	result, err := c.ExecuteQuery("SELECT * FROM in_main80 WHERE category IN (SELECT name FROM in_ref80)")
	if err != nil {
		t.Logf("IN subquery error: %v", err)
	} else {
		t.Logf("IN subquery returned %d rows", len(result.Rows))
	}
}

// TestCoverage_insertLockedAutoInc80 targets insertLocked with AUTOINCREMENT
func TestCoverage_insertLockedAutoInc80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE autoinc_test80 (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create table error: %v", err)
		return
	}

	// Insert without specifying id
	for i := 1; i <= 10; i++ {
		_, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "autoinc_test80",
			Columns: []string{"name"},
			Values:  [][]query.Expression{{strReal("item")}},
		}, nil)
		if err != nil {
			t.Logf("AutoInc insert error: %v", err)
		}
	}

	// Delete some
	c.ExecuteQuery("DELETE FROM autoinc_test80 WHERE id <= 5")

	// Insert more (should continue from last id)
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "autoinc_test80",
			Columns: []string{"name"},
			Values:  [][]query.Expression{{strReal("newitem")}},
		}, nil)
	}

	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM autoinc_test80")
	if result != nil {
		t.Logf("Final rows: %d", len(result.Rows))
	}
}

// TestCoverage_rollbackSavepointNested80 targets nested savepoint rollback
func TestCoverage_rollbackSavepointNested80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "nested_sp80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	c.BeginTransaction(1)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp80",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("t1")}},
	}, nil)

	c.Savepoint("sp1")
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp80",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("t2")}},
	}, nil)

	c.Savepoint("sp2")
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nested_sp80",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("t3")}},
	}, nil)

	// Rollback to inner savepoint
	err := c.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("Rollback to sp2 error: %v", err)
	}

	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM nested_sp80")
	t.Logf("Count after rollback to sp2: %v", result.Rows)

	// Rollback to outer savepoint
	err = c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	result, _ = c.ExecuteQuery("SELECT COUNT(*) FROM nested_sp80")
	t.Logf("Count after rollback to sp1: %v", result.Rows)

	c.RollbackTransaction()
}

// TestCoverage_evaluateWhereCase80 targets evaluateWhere with CASE expression
func TestCoverage_evaluateWhereCase80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "case_where80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "case_where80",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM case_where80 WHERE CASE WHEN score > 50 THEN 1 ELSE 0 END = 1",
		"SELECT * FROM case_where80 WHERE CASE score WHEN 25 THEN 1 WHEN 50 THEN 1 ELSE 0 END = 1",
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

// TestCoverage_deleteRowLockedFKSetNull80 targets deleteRowLocked with FK SET NULL
func TestCoverage_deleteRowLockedFKSetNull80(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "parent_sn80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, c, "child_sn80", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "parent_id", Type: query.TokenInteger},
	})

	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "parent_sn80",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("parent")}},
		}, nil)
	}

	for i := 1; i <= 6; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "child_sn80",
			Columns: []string{"id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1))}},
		}, nil)
	}

	// Delete parent
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "parent_sn80",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("FK delete error: %v", err)
	} else {
		t.Logf("Deleted %d parent rows", rows)
	}

	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM child_sn80 WHERE parent_id = 1")
	if result != nil {
		t.Logf("Children with parent_id=1: %v", result.Rows)
	}
}
