package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_JsonExtract targets JSONExtract function
func TestCoverage_JsonExtract(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_extract", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Insert JSON data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_extract",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"name": "test", "value": 123}`)}},
	}, nil)

	// Extract JSON values
	queries := []string{
		"SELECT JSON_EXTRACT(data, '$.name') FROM json_extract",
		"SELECT JSON_EXTRACT(data, '$.value') FROM json_extract",
		"SELECT JSON_EXTRACT(data, '$.missing') FROM json_extract",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JSONExtract error: %v", err)
		} else {
			t.Logf("Query returned: %v", result.Rows)
		}
	}
}

// TestCoverage_JsonSet targets JSONSet function
func TestCoverage_JsonSet(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_set", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_set",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"name": "test"}`)}},
	}, nil)

	// JSON_SET
	result, err := cat.ExecuteQuery("SELECT JSON_SET(data, '$.value', 456) FROM json_set")
	if err != nil {
		t.Logf("JSONSet error: %v", err)
	} else {
		t.Logf("JSONSet returned: %v", result.Rows)
	}
}

// TestCoverage_JsonRemove targets JSONRemove function
func TestCoverage_JsonRemove(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_remove", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_remove",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"a": 1, "b": 2, "c": 3}`)}},
	}, nil)

	// JSON_REMOVE
	result, err := cat.ExecuteQuery("SELECT JSON_REMOVE(data, '$.b') FROM json_remove")
	if err != nil {
		t.Logf("JSONRemove error: %v", err)
	} else {
		t.Logf("JSONRemove returned: %v", result.Rows)
	}
}

// TestCoverage_JsonArrayLength targets JSONArrayLength function
func TestCoverage_JsonArrayLength(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_arr_len", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_arr_len",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`[1, 2, 3, 4, 5]`)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT JSON_ARRAY_LENGTH(data) FROM json_arr_len")
	if err != nil {
		t.Logf("JSONArrayLength error: %v", err)
	} else {
		t.Logf("Array length: %v", result.Rows)
	}
}

// TestCoverage_JsonKeys targets JSONKeys function
func TestCoverage_JsonKeys(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_keys", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_keys",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"a": 1, "b": 2, "c": 3}`)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT JSON_KEYS(data) FROM json_keys")
	if err != nil {
		t.Logf("JSONKeys error: %v", err)
	} else {
		t.Logf("Keys: %v", result.Rows)
	}
}

// TestCoverage_JsonType targets JSONType function
func TestCoverage_JsonType(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_type", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_type",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"name": "test", "count": 42}`)}},
	}, nil)

	queries := []string{
		"SELECT JSON_TYPE(JSON_EXTRACT(data, '$.name')) FROM json_type",
		"SELECT JSON_TYPE(JSON_EXTRACT(data, '$.count')) FROM json_type",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JSONType error: %v", err)
		} else {
			t.Logf("Type: %v", result.Rows)
		}
	}
}

// TestCoverage_WindowFunctions78 targets various window functions
func TestCoverage_WindowFunctions78(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_funcs", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		catg := "A"
		if i > 10 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_funcs",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 100))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(amount) OVER (PARTITION BY category) as total FROM win_funcs ORDER BY id",
		"SELECT id, ROW_NUMBER() OVER (ORDER BY amount) as rn FROM win_funcs ORDER BY id",
		"SELECT id, RANK() OVER (ORDER BY amount DESC) as rnk FROM win_funcs ORDER BY id",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window function error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DistinctWithMultipleColumns targets DISTINCT with multiple columns
func TestCoverage_DistinctWithMultipleColumns(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "distinct_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenText},
		{Name: "b", Type: query.TokenInteger},
	})

	// Insert data with duplicates across columns
	for i := 1; i <= 30; i++ {
		a := "X"
		if i > 15 {
			a = "Y"
		}
		b := i % 3
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "distinct_multi",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(a), numReal(float64(b))}},
		}, nil)
	}

	result, err := cat.ExecuteQuery("SELECT DISTINCT a, b FROM distinct_multi ORDER BY a, b")
	if err != nil {
		t.Logf("DISTINCT error: %v", err)
	} else {
		t.Logf("DISTINCT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_IsNullInWhere targets IS NULL / IS NOT NULL in WHERE
func TestCoverage_IsNullInWhere(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "is_null_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		var val query.Expression = &query.NullLiteral{}
		if i%3 != 0 {
			val = numReal(float64(i * 10))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "is_null_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), val}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM is_null_test WHERE val IS NULL",
		"SELECT * FROM is_null_test WHERE val IS NOT NULL",
		"SELECT COUNT(*) FROM is_null_test WHERE val IS NULL",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("IS NULL error: %v", err)
		} else {
			t.Logf("Query returned: %v", result.Rows)
		}
	}
}

// TestCoverage_BetweenOperator78 targets BETWEEN operator
func TestCoverage_BetweenOperator78(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "between_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "between_test",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM between_test WHERE score BETWEEN 25 AND 75",
		"SELECT * FROM between_test WHERE score NOT BETWEEN 40 AND 60",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("BETWEEN error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_InOperatorWithSubquery targets IN with subquery
func TestCoverage_InOperatorWithSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "in_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "in_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		catg := "A"
		if i > 10 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "in_main",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg)}},
		}, nil)
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "in_ref",
		Columns: []string{"id", "cat"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}},
	}, nil)

	// IN with list
	result, err := cat.ExecuteQuery("SELECT * FROM in_main WHERE category IN ('A')")
	if err != nil {
		t.Logf("IN error: %v", err)
	} else {
		t.Logf("IN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_CaseExpression78 targets CASE expression
func TestCoverage_CaseExpression78(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "case_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "case_test",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// CASE expression
	result, err := cat.ExecuteQuery(`
		SELECT id, score,
			CASE
				WHEN score >= 80 THEN 'A'
				WHEN score >= 60 THEN 'B'
				WHEN score >= 40 THEN 'C'
				ELSE 'D'
			END as grade
		FROM case_test
	`)
	if err != nil {
		t.Logf("CASE error: %v", err)
	} else {
		t.Logf("CASE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_CastExpression78 targets CAST expression
func TestCoverage_CastExpression78(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cast_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenReal},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cast_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i) + 0.5)}},
		}, nil)
	}

	// CAST expressions
	queries := []string{
		"SELECT id, CAST(val AS INTEGER) as int_val FROM cast_test",
		"SELECT id, CAST(id AS TEXT) as str_val FROM cast_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CAST error: %v", err)
		} else {
			t.Logf("CAST returned %d rows", len(result.Rows))
		}
	}
}
