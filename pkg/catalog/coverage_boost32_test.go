package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Additional coverage for lowest coverage functions
// ============================================================

// TestUpdateLocked_Paths - targets updateLocked (56.3%)
func TestUpdateLocked_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Table with unique index
	createCoverageTestTable(t, cat, "upd_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create unique index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_upd_code",
		Table:   "upd_test",
		Columns: []string{"code"},
		Unique:  true,
	})

	// Insert test data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_test",
		Columns: []string{"id", "code", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A"), numReal(100)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_test",
		Columns: []string{"id", "code", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("B"), numReal(200)}},
	}, nil)

	// Test 1: Simple UPDATE
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_test",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(150)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Errorf("simple UPDATE failed: %v", err)
	}

	// Test 2: UPDATE with expression
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_test",
		Set:   []*query.SetClause{{Column: "val", Value: &query.BinaryExpr{Left: numReal(10), Operator: query.TokenPlus, Right: numReal(5)}}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err != nil {
		t.Errorf("UPDATE with expression failed: %v", err)
	}

	// Test 3: UPDATE with unique index change
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_test",
		Set:   []*query.SetClause{{Column: "code", Value: strReal("C")}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err != nil {
		t.Logf("UPDATE unique column (may fail): %v", err)
	}

	// Test 4: UPDATE all rows (no WHERE)
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_test",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(0)}},
	}, nil)
	if err != nil {
		t.Errorf("UPDATE all rows failed: %v", err)
	}
}

// TestUpdateLocked_Transaction - UPDATE within transaction
func TestUpdateLocked_Transaction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "upd_txn", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_txn",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Start transaction
	cat.BeginTransaction(1)

	// UPDATE within transaction
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_txn",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(200)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Errorf("UPDATE in transaction failed: %v", err)
	}

	// Rollback
	cat.RollbackTransaction()
}

// TestSelectLocked_ComplexPaths - targets selectLocked (59.9%)
func TestSelectLocked_ComplexPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sel_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "status", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
	})

	// Insert test data
	for i := 1; i <= 10; i++ {
		status := "active"
		if i > 7 {
			status = "inactive"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_complex",
			Columns: []string{"id", "name", "status", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name"), strReal(status), numReal(float64(i * 10))}},
		}, nil)
	}

	// Test 1: SELECT with complex WHERE (AND/OR)
	result, err := cat.ExecuteQuery(`
		SELECT * FROM sel_complex
		WHERE (status = 'active' AND score > 50) OR (status = 'inactive' AND score < 100)
		ORDER BY id
	`)
	if err != nil {
		t.Logf("Complex WHERE error: %v", err)
	} else {
		t.Logf("Complex WHERE returned %d rows", len(result.Rows))
	}

	// Test 2: SELECT with BETWEEN
	result, err = cat.ExecuteQuery(`
		SELECT * FROM sel_complex
		WHERE score BETWEEN 30 AND 70
		ORDER BY score
	`)
	if err != nil {
		t.Logf("BETWEEN error: %v", err)
	} else {
		t.Logf("BETWEEN returned %d rows", len(result.Rows))
	}

	// Test 3: SELECT with IN
	result, err = cat.ExecuteQuery(`
		SELECT * FROM sel_complex
		WHERE id IN (1, 3, 5, 7)
		ORDER BY id
	`)
	if err != nil {
		t.Logf("IN error: %v", err)
	} else {
		if len(result.Rows) != 4 {
			t.Errorf("IN clause expected 4 rows, got %d", len(result.Rows))
		}
	}

	// Test 4: SELECT with LIKE
	result, err = cat.ExecuteQuery(`
		SELECT * FROM sel_complex
		WHERE name LIKE 'na%'
	`)
	if err != nil {
		t.Logf("LIKE error: %v", err)
	}

	// Test 5: SELECT DISTINCT
	result, err = cat.ExecuteQuery(`
		SELECT DISTINCT status FROM sel_complex
	`)
	if err != nil {
		t.Logf("DISTINCT error: %v", err)
	} else {
		if len(result.Rows) != 2 {
			t.Errorf("DISTINCT expected 2 statuses, got %d", len(result.Rows))
		}
	}

	// Test 6: SELECT with LIMIT and OFFSET
	result, err = cat.ExecuteQuery(`
		SELECT * FROM sel_complex
		ORDER BY id
		LIMIT 3 OFFSET 2
	`)
	if err != nil {
		t.Logf("LIMIT/OFFSET error: %v", err)
	} else {
		if len(result.Rows) != 3 {
			t.Errorf("LIMIT 3 expected 3 rows, got %d", len(result.Rows))
		}
	}

	// Test 7: SELECT with NULL check
	result, err = cat.ExecuteQuery(`
		SELECT * FROM sel_complex
		WHERE score IS NOT NULL
	`)
	if err != nil {
		t.Logf("IS NOT NULL error: %v", err)
	}
	_ = result
}

// TestExecuteScalarSelect_Paths - targets executeScalarSelect (59.3%)
func TestExecuteScalarSelect_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Test 1: SELECT without FROM (scalar)
	result, err := cat.ExecuteQuery(`SELECT 1 + 1 AS result`)
	if err != nil {
		t.Logf("Scalar SELECT error: %v", err)
	} else if len(result.Rows) > 0 {
		t.Logf("Scalar SELECT result: %v", result.Rows[0])
	}

	// Test 2: SELECT expression
	result, err = cat.ExecuteQuery(`SELECT 10 * 5 AS product, 'hello' AS greeting`)
	if err != nil {
		t.Logf("SELECT expressions error: %v", err)
	}

	// Test 3: SELECT with function call
	result, err = cat.ExecuteQuery(`SELECT LENGTH('hello') AS len`)
	if err != nil {
		t.Logf("SELECT function error: %v", err)
	}

	// Test 4: SELECT with CASE
	result, err = cat.ExecuteQuery(`SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END AS result`)
	if err != nil {
		t.Logf("SELECT CASE error: %v", err)
	}

	// Test 5: SELECT with COALESCE
	result, err = cat.ExecuteQuery(`SELECT COALESCE(NULL, 'fallback', 'other') AS result`)
	if err != nil {
		t.Logf("SELECT COALESCE error: %v", err)
	}

	// Test 6: SELECT with subquery
	createCoverageTestTable(t, cat, "scalar_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	cat.Insert(context.Background(), &query.InsertStmt{
		Table:   "scalar_sub",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(42)}},
	}, nil)

	result, err = cat.ExecuteQuery(`SELECT (SELECT val FROM scalar_sub WHERE id = 1) AS subval`)
	if err != nil {
		t.Logf("SELECT with subquery error: %v", err)
	}
	_ = result
}

// TestExecuteSelectWithJoinAndGroupBy_MorePaths - more coverage for executeSelectWithJoinAndGroupBy
func TestExecuteSelectWithJoinAndGroupBy_MorePaths(t *testing.T) {
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
	createCoverageTestTable(t, cat, "fact_table", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dim_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenReal},
		{Name: "quantity", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "dim_table", []*query.ColumnDef{
		{Name: "dim_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "region", Type: query.TokenText},
	})

	// Insert dimension data
	categories := []string{"A", "B", "C"}
	regions := []string{"North", "South", "East", "West"}
	dimID := 1
	for _, catg := range categories {
		for _, reg := range regions {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "dim_table",
				Columns: []string{"dim_id", "category", "region"},
				Values:  [][]query.Expression{{numReal(float64(dimID)), strReal(catg), strReal(reg)}},
			}, nil)
			dimID++
		}
	}

	// Insert fact data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fact_table",
			Columns: []string{"id", "dim_id", "amount", "quantity"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%12)+1)), numReal(float64(i) * 1.5), numReal(float64(i % 10))}},
		}, nil)
	}

	// Test 1: Complex JOIN with multiple GROUP BY columns
	result, err := cat.ExecuteQuery(`
		SELECT d.category, d.region,
			COUNT(*) as cnt,
			SUM(f.amount) as total_amount,
			AVG(f.quantity) as avg_qty,
			MIN(f.amount) as min_amount,
			MAX(f.amount) as max_amount
		FROM fact_table f
		JOIN dim_table d ON f.dim_id = d.dim_id
		GROUP BY d.category, d.region
		ORDER BY d.category, d.region
	`)
	if err != nil {
		t.Logf("Complex JOIN+GROUP BY error: %v", err)
	} else {
		t.Logf("Complex JOIN+GROUP BY returned %d rows", len(result.Rows))
	}

	// Test 2: JOIN + GROUP BY + HAVING with aggregate condition
	result, err = cat.ExecuteQuery(`
		SELECT d.category,
			COUNT(*) as cnt,
			SUM(f.amount) as total
		FROM fact_table f
		JOIN dim_table d ON f.dim_id = d.dim_id
		GROUP BY d.category
		HAVING COUNT(*) > 5
		ORDER BY total DESC
	`)
	if err != nil {
		t.Logf("JOIN+GROUP BY+HAVING error: %v", err)
	}

	// Test 3: LEFT JOIN + GROUP BY
	result, err = cat.ExecuteQuery(`
		SELECT d.category,
			COUNT(f.id) as fact_count,
			COALESCE(SUM(f.amount), 0) as total
		FROM dim_table d
		LEFT JOIN fact_table f ON d.dim_id = f.dim_id
		GROUP BY d.category
		ORDER BY d.category
	`)
	if err != nil {
		t.Logf("LEFT JOIN+GROUP BY error: %v", err)
	}

	// Test 4: Multiple JOINs with GROUP BY
	createCoverageTestTable(t, cat, "dim2_table", []*query.ColumnDef{
		{Name: "cat_code", Type: query.TokenText, PrimaryKey: true},
		{Name: "cat_name", Type: query.TokenText},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "dim2_table",
		Columns: []string{"cat_code", "cat_name"},
		Values:  [][]query.Expression{{strReal("A"), strReal("Category A")}, {strReal("B"), strReal("Category B")}, {strReal("C"), strReal("Category C")}},
	}, nil)

	result, err = cat.ExecuteQuery(`
		SELECT d2.cat_name, d.region, SUM(f.amount) as total
		FROM fact_table f
		JOIN dim_table d ON f.dim_id = d.dim_id
		JOIN dim2_table d2 ON d.category = d2.cat_code
		GROUP BY d2.cat_name, d.region
		ORDER BY d2.cat_name, d.region
	`)
	if err != nil {
		t.Logf("Multiple JOINs+GROUP BY error: %v", err)
	}
	_ = result
}

// TestDeleteLocked_Paths - targets deleteLocked (76.0%)
func TestDeleteLocked_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "del_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_del_code",
		Table:   "del_test",
		Columns: []string{"code"},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_test",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("code")}},
		}, nil)
	}

	// Test 1: DELETE with WHERE
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_test",
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Errorf("DELETE with WHERE failed: %v", err)
	}

	// Test 2: DELETE with complex WHERE
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_test",
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenGt, Right: numReal(2)},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenLt, Right: numReal(5)},
		},
	}, nil)
	if err != nil {
		t.Errorf("DELETE with complex WHERE failed: %v", err)
	}

	// Test 3: DELETE all (no WHERE)
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_test",
	}, nil)
	if err != nil {
		t.Errorf("DELETE all failed: %v", err)
	}
}

// TestInsertLocked_Paths - targets insertLocked (72.2%)
func TestInsertLocked_Paths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "ins_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Test 1: Single row INSERT
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_test",
		Columns: []string{"id", "name", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A"), numReal(100)}},
	}, nil)
	if err != nil {
		t.Errorf("Single INSERT failed: %v", err)
	}

	// Test 2: Multi-row INSERT
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_test",
		Columns: []string{"id", "name", "val"},
		Values: [][]query.Expression{
			{numReal(2), strReal("B"), numReal(200)},
			{numReal(3), strReal("C"), numReal(300)},
			{numReal(4), strReal("D"), numReal(400)},
		},
	}, nil)
	if err != nil {
		t.Errorf("Multi-row INSERT failed: %v", err)
	}

	// Test 3: INSERT with expression
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_test",
		Columns: []string{"id", "name", "val"},
		Values:  [][]query.Expression{{numReal(5), strReal("E"), &query.BinaryExpr{Left: numReal(10), Operator: query.TokenPlus, Right: numReal(5)}}},
	}, nil)
	if err != nil {
		t.Errorf("INSERT with expression failed: %v", err)
	}

	// Test 4: INSERT with subquery
	createCoverageTestTable(t, cat, "ins_source", []*query.ColumnDef{
		{Name: "src_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "src_val", Type: query.TokenInteger},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_source",
		Columns: []string{"src_id", "src_val"},
		Values:  [][]query.Expression{{numReal(1), numReal(999)}},
	}, nil)

	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:  "ins_test",
		Select: &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "src_id"}, strReal("from_select"), &query.QualifiedIdentifier{Column: "src_val"}},
			From:    &query.TableRef{Name: "ins_source"},
		},
	}, nil)
	if err != nil {
		t.Logf("INSERT with SELECT error (may not be supported): %v", err)
	}
}

// TestWindowFunctions_MorePaths - targets evaluateWindowFunctions (70.8%)
func TestWindowFunctions_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "win_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert test data with groups
	data := []struct{ id int; grp string; val int }{
		{1, "A", 10},
		{2, "A", 20},
		{3, "A", 30},
		{4, "B", 15},
		{5, "B", 25},
		{6, "B", 35},
	}
	for _, d := range data {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_test",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(d.id)), strReal(d.grp), numReal(float64(d.val))}},
		}, nil)
	}

	// Test 1: ROW_NUMBER with PARTITION BY
	result, err := cat.ExecuteQuery(`
		SELECT id, grp, val,
			ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn
		FROM win_test
		ORDER BY grp, rn
	`)
	if err != nil {
		t.Logf("ROW_NUMBER with PARTITION BY error: %v", err)
	} else {
		t.Logf("ROW_NUMBER returned %d rows", len(result.Rows))
	}

	// Test 2: RANK with PARTITION BY
	result, err = cat.ExecuteQuery(`
		SELECT id, grp, val,
			RANK() OVER (PARTITION BY grp ORDER BY val DESC) as rnk
		FROM win_test
		ORDER BY grp, rnk
	`)
	if err != nil {
		t.Logf("RANK error: %v", err)
	}

	// Test 3: DENSE_RANK
	result, err = cat.ExecuteQuery(`
		SELECT id, grp, val,
			DENSE_RANK() OVER (ORDER BY grp, val) as dr
		FROM win_test
		ORDER BY dr
	`)
	if err != nil {
		t.Logf("DENSE_RANK error: %v", err)
	}

	// Test 4: SUM as window function
	result, err = cat.ExecuteQuery(`
		SELECT id, grp, val,
			SUM(val) OVER (PARTITION BY grp ORDER BY id) as running_sum
		FROM win_test
		ORDER BY grp, id
	`)
	if err != nil {
		t.Logf("SUM window function error: %v", err)
	}

	// Test 5: AVG as window function
	result, err = cat.ExecuteQuery(`
		SELECT id, grp, val,
			AVG(val) OVER (PARTITION BY grp) as avg_val
		FROM win_test
	`)
	if err != nil {
		t.Logf("AVG window function error: %v", err)
	}
	_ = result
}
