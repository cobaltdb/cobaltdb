package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_InsertLockedFKCheck targets insertLocked FK constraint checking
func TestCoverage_InsertLockedFKCheck(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent
	cat.CreateTable(&query.CreateTableStmt{
		Table: "ins_fk_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_fk_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Create child with FK
	cat.CreateTable(&query.CreateTableStmt{
		Table: "ins_fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "ins_fk_parent", ReferencedColumns: []string{"id"}},
		},
	})

	// Insert with valid FK
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_fk_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)
	if err != nil {
		t.Logf("Valid FK insert error: %v", err)
	}

	// Insert with invalid FK
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_fk_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(2), numReal(999)}},
	}, nil)
	if err != nil {
		t.Logf("Invalid FK insert error (expected): %v", err)
	}

	// Insert with NULL FK (should be allowed)
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_fk_child",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(3)}},
	}, nil)
	if err != nil {
		t.Logf("NULL FK insert error: %v", err)
	}
}

// TestCoverage_InsertLockedUniqueCheck targets insertLocked UNIQUE constraint
func TestCoverage_InsertLockedUniqueCheck(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "ins_unique",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText, Unique: true},
		},
	})

	// Insert valid
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_unique",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("ABC")}},
	}, nil)
	if err != nil {
		t.Logf("Insert error: %v", err)
	}

	// Insert duplicate UNIQUE value
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_unique",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(2), strReal("ABC")}},
	}, nil)
	if err != nil {
		t.Logf("Duplicate UNIQUE error (expected): %v", err)
	}
}

// TestCoverage_InsertLockedNotNullCheck targets insertLocked NOT NULL constraint
func TestCoverage_InsertLockedNotNullCheck(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "ins_notnull",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "req", Type: query.TokenText, NotNull: true},
		},
	})

	// Insert NULL into NOT NULL column
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_notnull",
		Columns: []string{"id", "req"},
		Values:  [][]query.Expression{{numReal(1), &query.NullLiteral{}}},
	}, nil)
	if err != nil {
		t.Logf("NOT NULL error (expected): %v", err)
	}
}

// TestCoverage_InsertLockedCheckConstraint targets insertLocked CHECK constraint
func TestCoverage_InsertLockedCheckConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "ins_check_ins",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "val"},
				Operator: query.TokenGt,
				Right:    numReal(0),
			}},
		},
	})

	// Insert valid value
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_check_ins",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}},
	}, nil)
	if err != nil {
		t.Logf("Insert error: %v", err)
	}

	// Insert failing CHECK constraint
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_check_ins",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), numReal(-5)}},
	}, nil)
	if err != nil {
		t.Logf("CHECK constraint error (expected): %v", err)
	}
}

// TestCoverage_SelectLockedGroupByError targets selectLocked GROUP BY errors
func TestCoverage_SelectLockedGroupByError(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_gb_err", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_gb_err",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("A"), numReal(float64(i * 10))}},
		}, nil)
	}

	// GROUP BY with column not in SELECT
	result, err := cat.ExecuteQuery("SELECT id FROM sel_gb_err GROUP BY grp")
	if err != nil {
		t.Logf("GROUP BY error (expected): %v", err)
	} else {
		t.Logf("GROUP BY returned %d rows", len(result.Rows))
	}
}

// TestCoverage_SelectLockedOrderByNulls targets ORDER BY with NULL handling
func TestCoverage_SelectLockedOrderByNulls(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_ob_null", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert with NULLs
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sel_ob_null",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sel_ob_null",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), numReal(100)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sel_ob_null",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(3)}},
	}, nil)

	// ORDER BY with NULLs
	result, err := cat.ExecuteQuery("SELECT * FROM sel_ob_null ORDER BY val")
	if err != nil {
		t.Logf("ORDER BY NULL error: %v", err)
	} else {
		t.Logf("ORDER BY NULL returned %d rows", len(result.Rows))
	}
}

// TestCoverage_SelectLockedDistinct targets DISTINCT handling
func TestCoverage_SelectLockedDistinct(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_distinct", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
	})

	// Insert duplicate categories
	for i := 1; i <= 20; i++ {
		c := "A"
		if i > 10 {
			c = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_distinct",
			Columns: []string{"id", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(c)}},
		}, nil)
	}

	// SELECT DISTINCT
	result, err := cat.ExecuteQuery("SELECT DISTINCT cat FROM sel_distinct")
	if err != nil {
		t.Logf("DISTINCT error: %v", err)
	} else {
		t.Logf("DISTINCT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_SelectLockedLimitOffset targets LIMIT/OFFSET
func TestCoverage_SelectLockedLimitOffset(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_limit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_limit",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM sel_limit LIMIT 10",
		"SELECT * FROM sel_limit LIMIT 10 OFFSET 10",
		"SELECT * FROM sel_limit OFFSET 90",
		"SELECT * FROM sel_limit LIMIT 0",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIMIT/OFFSET error: %v", err)
		} else {
			t.Logf("Query '%s' returned %d rows", q, len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOuterQueryAlias targets applyOuterQuery with table alias
func TestCoverage_ApplyOuterQueryAlias(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_alias", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_alias",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with alias
	cat.CreateView("view_alias", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "outer_alias", Alias: "oa"},
	})

	// Query view with outer filter using alias
	result, err := cat.ExecuteQuery("SELECT * FROM view_alias WHERE val > 50")
	if err != nil {
		t.Logf("Alias query error: %v", err)
	} else {
		t.Logf("Alias query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ApplyOuterQuerySubquery targets applyOuterQuery with subquery
func TestCoverage_ApplyOuterQuerySubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_sub_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "outer_sub_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_sub_main",
			Columns: []string{"id", "ref"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_sub_ref",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("code")}},
		}, nil)
	}

	// Query with subquery in WHERE
	result, err := cat.ExecuteQuery("SELECT * FROM outer_sub_main WHERE ref IN (SELECT id FROM outer_sub_ref WHERE code = 'code')")
	if err != nil {
		t.Logf("Subquery error: %v", err)
	} else {
		t.Logf("Subquery returned %d rows", len(result.Rows))
	}
}
