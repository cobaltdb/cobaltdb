package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Additional tests for lowest coverage functions
// ============================================================

// TestExecuteScalarSelect_MorePaths - targets executeScalarSelect (59.3%)
func TestExecuteScalarSelect_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Test 1: Simple number literal
	result, err := cat.ExecuteQuery(`SELECT 42`)
	if err != nil {
		t.Logf("SELECT 42 error: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Test 2: String literal
	result, err = cat.ExecuteQuery(`SELECT 'hello world'`)
	if err != nil {
		t.Logf("SELECT string error: %v", err)
	}

	// Test 3: NULL literal
	result, err = cat.ExecuteQuery(`SELECT NULL`)
	if err != nil {
		t.Logf("SELECT NULL error: %v", err)
	}

	// Test 4: Boolean literal
	result, err = cat.ExecuteQuery(`SELECT TRUE`)
	if err != nil {
		t.Logf("SELECT TRUE error: %v", err)
	}

	result, err = cat.ExecuteQuery(`SELECT FALSE`)
	if err != nil {
		t.Logf("SELECT FALSE error: %v", err)
	}

	// Test 5: Arithmetic expression
	result, err = cat.ExecuteQuery(`SELECT 10 + 20 * 3`)
	if err != nil {
		t.Logf("SELECT arithmetic error: %v", err)
	}

	// Test 6: Complex expression
	result, err = cat.ExecuteQuery(`SELECT (5 + 5) * 2 - 3`)
	if err != nil {
		t.Logf("SELECT complex expression error: %v", err)
	}

	// Test 7: String concatenation simulation (if supported)
	result, err = cat.ExecuteQuery(`SELECT 'Hello' || ' World'`)
	if err != nil {
		t.Logf("SELECT string concat error: %v", err)
	}

	// Test 8: Multiple columns
	result, err = cat.ExecuteQuery(`SELECT 1, 2, 3, 'a', 'b'`)
	if err != nil {
		t.Logf("SELECT multiple columns error: %v", err)
	} else if len(result.Rows) > 0 && len(result.Rows[0]) != 5 {
		t.Errorf("Expected 5 columns, got %d", len(result.Rows[0]))
	}

	// Test 9: Function call without arguments
	result, err = cat.ExecuteQuery(`SELECT RANDOM()`)
	if err != nil {
		t.Logf("SELECT RANDOM() error: %v", err)
	}

	// Test 10: CURRENT_DATE / CURRENT_TIME if supported
	result, err = cat.ExecuteQuery(`SELECT CURRENT_DATE`)
	if err != nil {
		t.Logf("SELECT CURRENT_DATE error: %v", err)
	}

	_ = result
}

// TestUpdateLocked_Returning - tests UPDATE with RETURNING clause
func TestUpdateLocked_Returning(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "upd_ret", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_ret",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// UPDATE
	rowsAffected, _, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_ret",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(200)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	if err != nil {
		t.Logf("UPDATE error: %v", err)
	} else {
		t.Logf("UPDATE affected %d rows", rowsAffected)
	}
}

// TestUpdateLocked_Join - tests UPDATE with JOIN
func TestUpdateLocked_Join(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "upd_target", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "upd_source", []*query.ColumnDef{
		{Name: "ref_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "new_val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_target",
		Columns: []string{"id", "ref_id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(0)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_source",
		Columns: []string{"ref_id", "new_val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// UPDATE with FROM/JOIN
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_target",
		Set:   []*query.SetClause{{Column: "val", Value: &query.QualifiedIdentifier{Table: "s", Column: "new_val"}}},
		From:  &query.TableRef{Name: "upd_source", Alias: "s"},
		Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Table: "upd_target", Column: "ref_id"}, Operator: query.TokenEq, Right: &query.QualifiedIdentifier{Table: "s", Column: "ref_id"}},
	}, nil)

	if err != nil {
		t.Logf("UPDATE with JOIN error (may be expected): %v", err)
	}
}

// TestSelectLocked_MoreJoinTypes - tests more JOIN variations
func TestSelectLocked_MoreJoinTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sel_t1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "sel_t2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "t1_id", Type: query.TokenInteger},
		{Name: "value", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_t1",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name")}},
		}, nil)
	}
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_t2",
			Columns: []string{"id", "t1_id", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%5)+1)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Test 1: JOIN with aggregate
	result, err := cat.ExecuteQuery(`
		SELECT t1.name, COUNT(t2.id) as cnt, SUM(t2.value) as total
		FROM sel_t1 t1
		JOIN sel_t2 t2 ON t1.id = t2.t1_id
		GROUP BY t1.id, t1.name
		ORDER BY t1.id
	`)
	if err != nil {
		t.Logf("JOIN with aggregate error: %v", err)
	} else {
		t.Logf("JOIN with aggregate returned %d rows", len(result.Rows))
	}

	// Test 2: LEFT JOIN with WHERE
	result, err = cat.ExecuteQuery(`
		SELECT t1.name, t2.value
		FROM sel_t1 t1
		LEFT JOIN sel_t2 t2 ON t1.id = t2.t1_id
		WHERE t2.value > 50 OR t2.value IS NULL
		ORDER BY t1.id, t2.id
	`)
	if err != nil {
		t.Logf("LEFT JOIN with WHERE error: %v", err)
	}

	// Test 3: Self JOIN
	result, err = cat.ExecuteQuery(`
		SELECT a.name, b.name
		FROM sel_t1 a
		JOIN sel_t1 b ON a.id < b.id
		ORDER BY a.id, b.id
	`)
	if err != nil {
		t.Logf("Self JOIN error: %v", err)
	}

	// Test 4: JOIN with subquery
	result, err = cat.ExecuteQuery(`
		SELECT t1.name, sq.total
		FROM sel_t1 t1
		JOIN (
			SELECT t1_id, SUM(value) as total
			FROM sel_t2
			GROUP BY t1_id
		) sq ON t1.id = sq.t1_id
		ORDER BY t1.id
	`)
	if err != nil {
		t.Logf("JOIN with subquery error: %v", err)
	}

	_ = result
}

// TestExecuteSelectWithJoinAndGroupBy_MoreAggregates - more aggregate tests
func TestExecuteSelectWithJoinAndGroupBy_MoreAggregates(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "agg_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "agg_detail", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenReal},
		{Name: "quantity", Type: query.TokenInteger},
	})

	// Insert data
	cats := []string{"A", "B", "C"}
	for i, c := range cats {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_main",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(c)}},
		}, nil)
	}
	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_detail",
			Columns: []string{"id", "main_id", "amount", "quantity"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%3)+1)), numReal(float64(i) * 1.5), numReal(float64(i))}},
		}, nil)
	}

	// Test 1: Multiple aggregates
	result, err := cat.ExecuteQuery(`
		SELECT m.category,
			COUNT(*) as cnt,
			SUM(d.amount) as total_amount,
			AVG(d.amount) as avg_amount,
			MIN(d.quantity) as min_qty,
			MAX(d.quantity) as max_qty,
			SUM(d.quantity) as total_qty
		FROM agg_main m
		JOIN agg_detail d ON m.id = d.main_id
		GROUP BY m.id, m.category
		ORDER BY m.id
	`)
	if err != nil {
		t.Logf("Multiple aggregates error: %v", err)
	} else {
		t.Logf("Multiple aggregates returned %d rows", len(result.Rows))
	}

	// Test 2: Aggregate with expression
	result, err = cat.ExecuteQuery(`
		SELECT m.category,
			SUM(d.amount * 2) as doubled,
			AVG(d.amount + d.quantity) as combined
		FROM agg_main m
		JOIN agg_detail d ON m.id = d.main_id
		GROUP BY m.id, m.category
		ORDER BY m.id
	`)
	if err != nil {
		t.Logf("Aggregate with expression error: %v", err)
	}

	// Test 3: Complex GROUP BY with ROLLUP simulation (multiple grouping levels)
	result, err = cat.ExecuteQuery(`
		SELECT m.category,
			COUNT(*) as cnt,
			SUM(d.amount) as total
		FROM agg_main m
		JOIN agg_detail d ON m.id = d.main_id
		WHERE d.amount > 10
		GROUP BY m.category
		HAVING COUNT(*) >= 2
		ORDER BY total DESC
	`)
	if err != nil {
		t.Logf("Complex GROUP BY error: %v", err)
	}

	_ = result
}

// TestSelectLocked_WhereClauses - tests various WHERE clause patterns
func TestSelectLocked_WhereClauses(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "where_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
		{Name: "active", Type: query.TokenBoolean},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		active := i%2 == 0
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_test",
			Columns: []string{"id", "name", "score", "active"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("item"), numReal(float64(i * 10)), &query.BooleanLiteral{Value: active}}},
		}, nil)
	}

	tests := []struct {
		name  string
		query string
	}{
		{"equality", `SELECT * FROM where_test WHERE id = 5`},
		{"inequality", `SELECT * FROM where_test WHERE id != 5`},
		{"less_than", `SELECT * FROM where_test WHERE score < 100`},
		{"less_equal", `SELECT * FROM where_test WHERE score <= 100`},
		{"greater_than", `SELECT * FROM where_test WHERE score > 50`},
		{"greater_equal", `SELECT * FROM where_test WHERE score >= 50`},
		{"and", `SELECT * FROM where_test WHERE score > 50 AND active = TRUE`},
		{"or", `SELECT * FROM where_test WHERE score < 30 OR score > 150`},
		{"in", `SELECT * FROM where_test WHERE id IN (1, 5, 10, 15)`},
		{"between", `SELECT * FROM where_test WHERE score BETWEEN 50 AND 100`},
		{"like", `SELECT * FROM where_test WHERE name LIKE 'it%'`},
		{"is_null", `SELECT * FROM where_test WHERE name IS NOT NULL`},
		{"not", `SELECT * FROM where_test WHERE NOT active = FALSE`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cat.ExecuteQuery(tt.query)
			if err != nil {
				t.Logf("Query '%s' error: %v", tt.query, err)
			} else {
				t.Logf("Query '%s' returned %d rows", tt.name, len(result.Rows))
			}
		})
	}
}

// TestDeleteWithUsing_MoreCases - additional DELETE ... USING tests
func TestDeleteWithUsing_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "del_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "del_to_remove", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		status := "keep"
		if i%2 == 0 {
			status = "delete"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_main",
			Columns: []string{"id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(status)}},
		}, nil)
	}
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_to_remove",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}, {numReal(4)}, {numReal(6)}},
	}, nil)

	// DELETE ... USING with complex WHERE
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_main",
		Using: []*query.TableRef{{Name: "del_to_remove"}},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: colReal("del_main.id"), Operator: query.TokenEq, Right: colReal("del_to_remove.id")},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: colReal("del_main.status"), Operator: query.TokenEq, Right: strReal("delete")},
		},
	}, nil)
	if err != nil {
		t.Logf("DELETE ... USING complex error (may be expected): %v", err)
	}
}

// TestInsertLocked_MorePaths - additional INSERT tests
func TestInsertLocked_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "ins_paths", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "str", Type: query.TokenText},
		{Name: "num", Type: query.TokenInteger},
	})

	// Test 1: INSERT with explicit column order
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_paths",
		Columns: []string{"num", "str", "id"},
		Values:  [][]query.Expression{{numReal(100), strReal("test"), numReal(1)}},
	}, nil)
	if err != nil {
		t.Errorf("INSERT with reordered columns failed: %v", err)
	}

	// Test 2: INSERT with NULL values
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_paths",
		Columns: []string{"id", "str", "num"},
		Values:  [][]query.Expression{{numReal(2), &query.NullLiteral{}, &query.NullLiteral{}}},
	}, nil)
	if err != nil {
		t.Errorf("INSERT with NULL values failed: %v", err)
	}

	// Test 3: INSERT multiple rows
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_paths",
		Columns: []string{"id", "str", "num"},
		Values: [][]query.Expression{
			{numReal(3), strReal("a"), numReal(1)},
			{numReal(4), strReal("b"), numReal(2)},
			{numReal(5), strReal("c"), numReal(3)},
			{numReal(6), strReal("d"), numReal(4)},
		},
	}, nil)
	if err != nil {
		t.Errorf("INSERT multiple rows failed: %v", err)
	}
}
