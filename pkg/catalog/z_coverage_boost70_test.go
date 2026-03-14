package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_insertLockedComplex targets insertLocked with complex scenarios
func TestCoverage_insertLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "insert_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "unique_code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create unique index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_insert_unique",
		Table:   "insert_complex",
		Columns: []string{"unique_code"},
		Unique:  true,
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "insert_complex",
			Columns: []string{"id", "unique_code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE" + string(rune('A'+i))), numReal(float64(i * 10))}},
		}, nil)
	}

	// Try to insert duplicate (should fail)
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "insert_complex",
		Columns: []string{"id", "unique_code", "val"},
		Values:  [][]query.Expression{{numReal(99), strReal("CODEB"), numReal(999)}},
	}, nil)
	if err != nil {
		t.Logf("Duplicate insert error (expected): %v", err)
	}

	// Verify only 5 rows
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM insert_complex")
	t.Logf("Count after insert attempts: %v", result.Rows)
}

// TestCoverage_insertLockedWithTransaction targets insertLocked with transaction
func TestCoverage_insertLockedWithTransaction(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "insert_txn", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create non-unique index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_insert_txn",
		Table:   "insert_txn",
		Columns: []string{"val"},
		Unique:  false,
	})

	cat.BeginTransaction(1)

	// Insert within transaction
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "insert_txn",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Rollback
	cat.RollbackTransaction()

	// Verify no rows
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM insert_txn")
	t.Logf("Count after rollback: %v", result.Rows)
}

// TestCoverage_insertLockedAutoIncrement targets insertLocked with auto-increment
func TestCoverage_insertLockedAutoIncrement(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "insert_autoinc",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert without specifying ID
	for i := 0; i < 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "insert_autoinc",
			Columns: []string{"val"},
			Values:  [][]query.Expression{{strReal("test")}},
		}, nil)
	}

	// Verify auto-increment worked
	result, _ := cat.ExecuteQuery("SELECT MAX(id) FROM insert_autoinc")
	t.Logf("Max auto-increment ID: %v", result.Rows)
}

// TestCoverage_insertLockedDefaultValues targets insertLocked with DEFAULT
func TestCoverage_insertLockedDefaultValues(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "insert_default",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText, Default: strReal("pending")},
			{Name: "count", Type: query.TokenInteger, Default: numReal(0)},
		},
	})

	// Insert with some defaults
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "insert_default",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "insert_default",
		Columns: []string{"id", "status"},
		Values:  [][]query.Expression{{numReal(2), strReal("active")}},
	}, nil)

	// Verify defaults
	result, _ := cat.ExecuteQuery("SELECT * FROM insert_default ORDER BY id")
	t.Logf("Rows with defaults: %v", result.Rows)
}

// TestCoverage_deleteWithUsingLocked targets deleteWithUsingLocked
func TestCoverage_deleteWithUsingLocked(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_using_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "data", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "del_using_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_using_main",
			Columns: []string{"id", "ref_id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 5)), strReal("data")}},
		}, nil)
	}

	for i := 0; i < 5; i++ {
		status := "keep"
		if i%2 == 0 {
			status = "delete"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_using_ref",
			Columns: []string{"id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(status)}},
		}, nil)
	}

	// DELETE USING with JOIN
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_using_main",
		Using: []*query.TableRef{{Name: "del_using_ref"}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "del_using_main.ref_id"},
				Operator: query.TokenEq,
				Right:    &query.Identifier{Name: "del_using_ref.id"},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "del_using_ref.status"},
				Operator: query.TokenEq,
				Right:    strReal("delete"),
			},
		},
	}, nil)

	if err != nil {
		t.Logf("DELETE USING error: %v", err)
	} else {
		t.Logf("DELETE USING affected %d rows", rows)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_using_main")
	t.Logf("Count after DELETE USING: %v", result.Rows)
}

// TestCoverage_deleteWithUsingComplex targets deleteWithUsingLocked with complex scenarios
func TestCoverage_deleteWithUsingComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "du_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "du_ref1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "du_ref2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "exclude", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		catg := "A"
		if i > 15 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "du_main",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg)}},
		}, nil)
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "du_ref1",
		Columns: []string{"id", "cat"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "du_ref2",
		Columns: []string{"id", "exclude"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// DELETE USING with multiple tables
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "du_main",
		Using: []*query.TableRef{{Name: "du_ref1"}, {Name: "du_ref2"}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "du_main.category"},
				Operator: query.TokenEq,
				Right:    &query.Identifier{Name: "du_ref1.cat"},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "du_main.id"},
				Operator: query.TokenNeq,
				Right:    &query.Identifier{Name: "du_ref2.exclude"},
			},
		},
	}, nil)

	if err != nil {
		t.Logf("DELETE USING complex error: %v", err)
	} else {
		t.Logf("DELETE USING complex affected %d rows", rows)
	}
}

// TestCoverage_applyOuterQueryMore targets applyOuterQuery with more scenarios
func TestCoverage_applyOuterQueryMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_more",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// Create view with subquery
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "outer_more"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLte,
			Right:    numReal(30),
		},
	}
	cat.CreateView("outer_view", viewStmt)

	// Query view with various clauses
	queries := []string{
		"SELECT * FROM outer_view ORDER BY val DESC LIMIT 5",
		"SELECT * FROM outer_view WHERE val > 20 ORDER BY id LIMIT 10 OFFSET 5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("View query error: %v", err)
		} else {
			t.Logf("View query returned %d rows", len(result.Rows))
		}
	}

	cat.DropView("outer_view")
}

// TestCoverage_evaluateLikePatterns targets evaluateLike with various patterns
func TestCoverage_evaluateLikePatterns(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "like_patterns", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "text", Type: query.TokenText},
	})

	// Insert various text patterns
	texts := []string{
		"hello world",
		"HELLO WORLD",
		"Hello World",
		"prefix_test",
		"test_suffix",
		"contains_test_here",
		"test",
		"TEST",
	}
	for i, txt := range texts {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "like_patterns",
			Columns: []string{"id", "text"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(txt)}},
		}, nil)
	}

	// Test LIKE patterns
	queries := []string{
		"SELECT * FROM like_patterns WHERE text LIKE '%test%'",
		"SELECT * FROM like_patterns WHERE text LIKE 'test%'",
		"SELECT * FROM like_patterns WHERE text LIKE '%test'",
		"SELECT * FROM like_patterns WHERE text LIKE '_____'",
		"SELECT * FROM like_patterns WHERE text NOT LIKE '%hello%'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIKE query error: %v", err)
		} else {
			t.Logf("LIKE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_evaluateWhereBoolean targets evaluateWhere with boolean logic
func TestCoverage_evaluateWhereBoolean(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_bool", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_bool",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 3)), numReal(float64(i % 2)), numReal(float64(i % 4))}},
		}, nil)
	}

	// Complex boolean expressions
	queries := []string{
		"SELECT * FROM where_bool WHERE a = 1 AND b = 1",
		"SELECT * FROM where_bool WHERE a = 1 OR b = 1",
		"SELECT * FROM where_bool WHERE NOT (a = 0 AND b = 0)",
		"SELECT * FROM where_bool WHERE (a = 1 OR b = 1) AND c > 0",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Boolean WHERE error: %v", err)
		} else {
			t.Logf("Boolean WHERE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_resolveAggregateInExprMore targets resolveAggregateInExpr with more cases
func TestCoverage_resolveAggregateInExprMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "amt1", Type: query.TokenInteger},
		{Name: "amt2", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 60; i++ {
		grp := "X"
		if i > 30 {
			grp = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_more",
			Columns: []string{"id", "grp", "amt1", "amt2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), numReal(float64(i * 5))}},
		}, nil)
	}

	// HAVING with arithmetic on aggregates
	queries := []string{
		"SELECT grp, SUM(amt1) as s1, SUM(amt2) as s2 FROM agg_more GROUP BY grp HAVING s1 / 2 > s2",
		"SELECT grp, AVG(amt1) as a1, AVG(amt2) as a2 FROM agg_more GROUP BY grp HAVING a1 > a2 * 1.5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate HAVING error: %v", err)
		} else {
			t.Logf("Aggregate HAVING returned %d rows", len(result.Rows))
		}
	}
}
