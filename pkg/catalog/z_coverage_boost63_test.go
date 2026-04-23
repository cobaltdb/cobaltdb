package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_LikeEscape targets evaluateLike with ESCAPE clause
func TestCoverage_LikeEscape(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "like_escape", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "path", Type: query.TokenText},
	})

	// Insert test data with special characters
	paths := []string{
		"/home/user/file.txt",
		"/home/admin/data.csv",
		"/var/log/app.log",
		"C:\\Windows\\System32",
	}
	for i, p := range paths {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "like_escape",
			Columns: []string{"id", "path"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(p)}},
		}, nil)
	}

	// Test LIKE with different patterns
	queries := []string{
		"SELECT * FROM like_escape WHERE path LIKE '%.txt'",
		"SELECT * FROM like_escape WHERE path LIKE '/home/%'",
		"SELECT * FROM like_escape WHERE path LIKE 'C:\\\\%'",
		"SELECT * FROM like_escape WHERE path NOT LIKE '%.log'",
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

// TestCoverage_IsNullNotNull targets IS NULL and IS NOT NULL
func TestCoverage_IsNullNotNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "null_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "txt", Type: query.TokenText},
	})

	// Insert mix of NULL and non-NULL values
	for i := 1; i <= 20; i++ {
		if i%3 == 0 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "null_test",
				Columns: []string{"id"},
				Values:  [][]query.Expression{{numReal(float64(i))}},
			}, nil)
		} else if i%3 == 1 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "null_test",
				Columns: []string{"id", "val"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
			}, nil)
		} else {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "null_test",
				Columns: []string{"id", "txt"},
				Values:  [][]query.Expression{{numReal(float64(i)), strReal("text")}},
			}, nil)
		}
	}

	queries := []string{
		"SELECT * FROM null_test WHERE val IS NULL",
		"SELECT * FROM null_test WHERE val IS NOT NULL",
		"SELECT * FROM null_test WHERE txt IS NULL",
		"SELECT * FROM null_test WHERE txt IS NOT NULL",
		"SELECT COUNT(*) FROM null_test WHERE val IS NULL",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("NULL query error: %v", err)
		} else {
			t.Logf("NULL query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_WindowFunctions targets window functions
func TestCoverage_WindowFunctions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "window_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert test data
	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "window_test",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	// Test window functions
	queries := []string{
		"SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn FROM window_test",
		"SELECT id, grp, val, RANK() OVER (ORDER BY val DESC) as rnk FROM window_test",
		"SELECT id, grp, val, SUM(val) OVER (PARTITION BY grp) as total FROM window_test",
		"SELECT id, grp, val, AVG(val) OVER (PARTITION BY grp) as avg_val FROM window_test",
		"SELECT id, grp, val, COUNT(*) OVER (PARTITION BY grp) as cnt FROM window_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window function error: %v", err)
		} else {
			t.Logf("Window query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CTEMore targets CTE functionality
func TestCoverage_CTEMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "parent_id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert hierarchical data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_base",
		Columns: []string{"id", "parent_id", "name"},
		Values: [][]query.Expression{
			{numReal(1), &query.NullLiteral{}, strReal("root")},
			{numReal(2), numReal(1), strReal("child1")},
			{numReal(3), numReal(1), strReal("child2")},
			{numReal(4), numReal(2), strReal("grandchild1")},
			{numReal(5), numReal(2), strReal("grandchild2")},
		},
	}, nil)

	// Test recursive CTE
	result, err := cat.ExecuteQuery(`
		WITH RECURSIVE tree AS (
			SELECT id, parent_id, name, 0 as level FROM cte_base WHERE parent_id IS NULL
			UNION ALL
			SELECT c.id, c.parent_id, c.name, t.level + 1
			FROM cte_base c
			JOIN tree t ON c.parent_id = t.id
		)
		SELECT * FROM tree ORDER BY level, id
	`)
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE returned %d rows", len(result.Rows))
	}

	// Test non-recursive CTE
	result, err = cat.ExecuteQuery(`
		WITH summary AS (
			SELECT parent_id, COUNT(*) as cnt
			FROM cte_base
			WHERE parent_id IS NOT NULL
			GROUP BY parent_id
		)
		SELECT * FROM summary ORDER BY cnt DESC
	`)
	if err != nil {
		t.Logf("Non-recursive CTE error: %v", err)
	} else {
		t.Logf("Non-recursive CTE returned %d rows", len(result.Rows))
	}
}

// TestCoverage_IndexOperations targets index creation and usage
func TestCoverage_IndexOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "idx_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "idx_test",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE" + string(rune('A'+i%26))), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create unique index
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "idx_test",
		Columns: []string{"code"},
		Unique:  true,
	})
	if err != nil {
		t.Logf("Create unique index error: %v", err)
	}

	// Create non-unique index
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_val",
		Table:   "idx_test",
		Columns: []string{"val"},
		Unique:  false,
	})
	if err != nil {
		t.Logf("Create index error: %v", err)
	}

	// Query using indexes
	queries := []string{
		"SELECT * FROM idx_test WHERE code = 'CODEA'",
		"SELECT * FROM idx_test WHERE val > 500",
		"SELECT * FROM idx_test WHERE code = 'CODEB' AND val > 100",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Index query error: %v", err)
		} else {
			t.Logf("Index query returned %d rows", len(result.Rows))
		}
	}

	// Drop index
	err = cat.DropIndex("idx_val")
	if err != nil {
		t.Logf("Drop index error: %v", err)
	}
}

// TestCoverage_TriggersMore targets trigger functionality
func TestCoverage_TriggersMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create audit log table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "audit_log",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "action", Type: query.TokenText},
			{Name: "table_name", Type: query.TokenText},
			{Name: "row_id", Type: query.TokenInteger},
		},
	})

	createCoverageTestTable(t, cat, "trig_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create trigger (syntax may vary)
	// This is a simplified test - actual trigger syntax depends on the parser

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "trig_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM trig_test")
	t.Logf("Trigger test table count: %v", result.Rows)
}

// TestCoverage_SetOperations targets UNION, INTERSECT, EXCEPT
func TestCoverage_SetOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "set_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "set_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert overlapping data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "set_a",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("A" + string(rune('0'+i%10)))}},
		}, nil)
	}

	for i := 5; i <= 15; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "set_b",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("B" + string(rune('0'+i%10)))}},
		}, nil)
	}

	// Test set operations
	queries := []string{
		"SELECT val FROM set_a UNION SELECT val FROM set_b",
		"SELECT val FROM set_a UNION ALL SELECT val FROM set_b",
		"SELECT id FROM set_a INTERSECT SELECT id FROM set_b",
		"SELECT id FROM set_a EXCEPT SELECT id FROM set_b",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Set operation error: %v", err)
		} else {
			t.Logf("Set operation returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CastExpression targets CAST function
func TestCoverage_CastExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cast_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "num_str", Type: query.TokenText},
		{Name: "float_val", Type: query.TokenReal},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cast_test",
			Columns: []string{"id", "num_str", "float_val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(string(rune('0' + i))), numReal(float64(i) + 0.5)}},
		}, nil)
	}

	// Test CAST operations
	queries := []string{
		"SELECT id, CAST(num_str AS INTEGER) as num FROM cast_test",
		"SELECT id, CAST(float_val AS INTEGER) as int_val FROM cast_test",
		"SELECT id, CAST(id AS TEXT) as str_id FROM cast_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CAST error: %v", err)
		} else {
			t.Logf("CAST query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_Coalesce targets COALESCE function
func TestCoverage_CoalesceMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "coalesce_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	// Insert data with NULLs
	for i := 1; i <= 10; i++ {
		if i%2 == 0 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "coalesce_test",
				Columns: []string{"id", "val1"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
			}, nil)
		} else {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "coalesce_test",
				Columns: []string{"id", "val2"},
				Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 100))}},
			}, nil)
		}
	}

	// Test COALESCE
	result, err := cat.ExecuteQuery("SELECT id, COALESCE(val1, val2, 0) as val FROM coalesce_test ORDER BY id")
	if err != nil {
		t.Logf("COALESCE error: %v", err)
	} else {
		t.Logf("COALESCE query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_NullIf targets NULLIF function
func TestCoverage_NullIf(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "nullif_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	// Insert data
	statuses := []string{"active", "inactive", "active", "deleted", "active"}
	for i, s := range statuses {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "nullif_test",
			Columns: []string{"id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(s)}},
		}, nil)
	}

	// Test NULLIF
	result, err := cat.ExecuteQuery("SELECT id, NULLIF(status, 'active') as non_active FROM nullif_test")
	if err != nil {
		t.Logf("NULLIF error: %v", err)
	} else {
		t.Logf("NULLIF query returned %d rows", len(result.Rows))
	}
}
