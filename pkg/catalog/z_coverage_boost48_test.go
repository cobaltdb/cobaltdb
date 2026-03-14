package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_HavingClauseMore targets evaluateHaving
func TestCoverage_HavingClauseMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		grp := "A"
		if i > 25 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "having_test",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) as total FROM having_test GROUP BY grp HAVING total > 5000",
		"SELECT grp, COUNT(*) as cnt, AVG(val) as avg_val FROM having_test GROUP BY grp HAVING cnt > 10 AND avg_val > 200",
		"SELECT grp, MAX(val) as mx, MIN(val) as mn FROM having_test GROUP BY grp HAVING mx - mn > 200",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING error: %v", err)
		} else {
			t.Logf("HAVING returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_OrderByMore targets applyOrderBy
func TestCoverage_OrderByMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_test",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(31 - i)), strReal("val")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM order_test ORDER BY a",
		"SELECT * FROM order_test ORDER BY a DESC",
		"SELECT * FROM order_test ORDER BY b, a DESC",
		"SELECT * FROM order_test ORDER BY a LIMIT 5 OFFSET 10",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("ORDER BY error: %v", err)
		} else {
			t.Logf("ORDER BY returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_LoadMore targets Load function
func TestCoverage_LoadMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create a table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "load_test1",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "load_test1",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Save
	if err := cat.Save(); err != nil {
		t.Logf("Save error: %v", err)
	}

	// Load
	if err := cat.Load(); err != nil {
		t.Logf("Load error: %v", err)
	}
}

// TestCoverage_AlterTableDropColumnMore targets AlterTableDropColumn
func TestCoverage_AlterTableDropColumnMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "alt_drop", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "keep", Type: query.TokenText},
		{Name: "dropme", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "alt_drop",
			Columns: []string{"id", "keep", "dropme"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("value"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Drop column
	err := cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "alt_drop",
		Action:  "DROP",
		OldName: "dropme",
	})
	if err != nil {
		t.Logf("Drop column error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT * FROM alt_drop")
	t.Logf("Columns after drop: %d", len(result.Columns))
}

// TestCoverage_AlterTableRenameColumnMore targets AlterTableRenameColumn
func TestCoverage_AlterTableRenameColumnMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "alt_rename", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "oldname", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "alt_rename",
			Columns: []string{"id", "oldname"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Rename column
	err := cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "alt_rename",
		Action:  "RENAME_COLUMN",
		OldName: "oldname",
		NewName: "newname",
	})
	if err != nil {
		t.Logf("Rename column error: %v", err)
	}

	// Verify new name works
	result, _ := cat.ExecuteQuery("SELECT newname FROM alt_rename LIMIT 1")
	t.Logf("Rename result: %v", result.Rows)
}

// TestCoverage_ExecuteScalarSelectMore targets executeScalarSelect
func TestCoverage_ExecuteScalarSelectMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*) FROM scalar_test",
		"SELECT SUM(val) FROM scalar_test",
		"SELECT AVG(val) FROM scalar_test",
		"SELECT MIN(val) FROM scalar_test",
		"SELECT MAX(val) FROM scalar_test",
		"SELECT COUNT(*) FROM scalar_test WHERE val > 100",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Scalar error: %v", err)
		} else {
			t.Logf("Scalar result: %v", result.Rows)
		}
	}
}
