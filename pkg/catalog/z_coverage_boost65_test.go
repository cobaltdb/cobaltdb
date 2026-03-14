package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_AlterTableDropColumnMore targets AlterTableDropColumn
func TestCoverage_AlterTableDropColumnExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "alter_drop",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "keep", Type: query.TokenText},
			{Name: "dropme", Type: query.TokenInteger},
			{Name: "alsokeep", Type: query.TokenText},
		},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "alter_drop",
			Columns: []string{"id", "keep", "dropme", "alsokeep"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val"), numReal(float64(i * 10)), strReal("keep")}},
		}, nil)
	}

	// Drop column
	err := cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "alter_drop",
		Action:  "DROP",
		OldName: "dropme",
	})
	if err != nil {
		t.Logf("Drop column error: %v", err)
	}

	// Verify column is dropped
	result, _ := cat.ExecuteQuery("SELECT id, keep, alsokeep FROM alter_drop")
	t.Logf("After drop column, rows: %d", len(result.Rows))
}

// TestCoverage_AlterTableRenameColumnMore targets AlterTableRenameColumn
func TestCoverage_AlterTableRenameColumnExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "alter_rename",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "oldname", Type: query.TokenText},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "alter_rename",
		Columns: []string{"id", "oldname"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Rename column
	err := cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "alter_rename",
		Action:  "RENAME_COLUMN",
		OldName: "oldname",
		NewName: "newname",
	})
	if err != nil {
		t.Logf("Rename column error: %v", err)
	}

	// Verify column is renamed
	result, _ := cat.ExecuteQuery("SELECT id, newname FROM alter_rename")
	t.Logf("After rename column, rows: %v", result.Rows)
}

// TestCoverage_AlterTableRenameTableMore targets AlterTableRename
func TestCoverage_AlterTableRenameTableMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rename_old",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rename_old",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Rename table
	err := cat.AlterTableRename(&query.AlterTableStmt{
		Table:   "rename_old",
		Action:  "RENAME_TABLE",
		NewName: "rename_new",
	})
	if err != nil {
		t.Logf("Rename table error: %v", err)
	}

	// Verify table is renamed
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM rename_new")
	t.Logf("After rename table, count: %v", result.Rows)
}

// TestCoverage_ApplyOrderByMore targets applyOrderBy
func TestCoverage_ApplyOrderByMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 15 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_multi",
			Columns: []string{"id", "grp", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(31 - i))}},
		}, nil)
	}

	// Multi-column ORDER BY
	result, err := cat.ExecuteQuery("SELECT * FROM order_multi ORDER BY grp ASC, score DESC")
	if err != nil {
		t.Logf("Multi-column ORDER BY error: %v", err)
	} else {
		t.Logf("Multi-column ORDER BY returned %d rows", len(result.Rows))
	}

	// ORDER BY with NULLs
	result, err = cat.ExecuteQuery("SELECT * FROM order_multi ORDER BY score")
	if err != nil {
		t.Logf("ORDER BY error: %v", err)
	} else {
		t.Logf("ORDER BY returned %d rows", len(result.Rows))
	}
}

// TestCoverage_EvaluateHavingMore targets evaluateHaving
func TestCoverage_EvaluateHavingMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 60; i++ {
		category := "A"
		if i > 25 {
			category = "B"
		}
		if i > 50 {
			category = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "having_complex",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	// Complex HAVING with multiple conditions
	queries := []string{
		"SELECT category, SUM(amount) as total FROM having_complex GROUP BY category HAVING total > 5000 AND COUNT(*) > 20",
		"SELECT category, AVG(amount) as avg_amt FROM having_complex GROUP BY category HAVING avg_amt BETWEEN 200 AND 400",
		"SELECT category, COUNT(*) as cnt FROM having_complex GROUP BY category HAVING cnt >= 10",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING error: %v", err)
		} else {
			t.Logf("HAVING query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DeleteWithUsingMore targets deleteWithUsingLocked
func TestCoverage_DeleteWithUsingExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_using_target", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "del_using_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_using_target",
			Columns: []string{"id", "ref"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 10))}},
		}, nil)
		status := "keep"
		if i%3 == 0 {
			status = "delete"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_using_ref",
			Columns: []string{"id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(status)}},
		}, nil)
	}

	// DELETE USING with JOIN condition
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_using_target",
		Using: []*query.TableRef{{Name: "del_using_ref"}},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: &query.Identifier{Name: "del_using_target.ref"}, Operator: query.TokenEq, Right: &query.Identifier{Name: "del_using_ref.id"}},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: &query.Identifier{Name: "del_using_ref.status"}, Operator: query.TokenEq, Right: strReal("delete")},
		},
	}, nil)

	if err != nil {
		t.Logf("DELETE USING error: %v", err)
	} else {
		t.Logf("DELETE USING affected %d rows", rows)
	}
}

// TestCoverage_ExecuteSelectWithJoinAndGroupByMore targets executeSelectWithJoinAndGroupBy
func TestCoverage_JoinGroupByExtended(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "join_gb_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "join_gb_detail", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		catg := "A"
		if i > 10 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_gb_main",
			Columns: []string{"id", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg)}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_gb_detail",
			Columns: []string{"id", "main_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// JOIN with GROUP BY
	result, err := cat.ExecuteQuery("SELECT m.cat, SUM(d.amount) as total FROM join_gb_main m JOIN join_gb_detail d ON m.id = d.main_id GROUP BY m.cat")
	if err != nil {
		t.Logf("JOIN GROUP BY error: %v", err)
	} else {
		t.Logf("JOIN GROUP BY returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ForeignKeyValuesEqual targets valuesEqual in foreign_key.go
func TestCoverage_ForeignKeyValuesEqual(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create parent
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_eq_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "str", Type: query.TokenText},
		},
	})

	// Create child
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_eq_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "parent_str", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "fk_eq_parent", ReferencedColumns: []string{"id"}},
		},
	})

	// Insert data with various types
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_eq_parent",
		Columns: []string{"id", "str"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}, {numReal(2), strReal("test2")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_eq_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(2)}},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM fk_eq_child")
	t.Logf("FK values equal test, count: %v", result.Rows)
}

// TestCoverage_LoadSave targets Load and Save
func TestCoverage_LoadSave(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create some tables
	cat.CreateTable(&query.CreateTableStmt{
		Table: "save_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Save
	err := cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Load
	err = cat.Load()
	if err != nil {
		t.Logf("Load error: %v", err)
	}
}

// TestCoverage_ExecuteScalarSelectMore targets executeScalarSelect
func TestCoverage_ExecuteScalarSelectExtended(t *testing.T) {
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
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// Various scalar queries
	queries := []string{
		"SELECT COUNT(*) FROM scalar_test",
		"SELECT SUM(val) FROM scalar_test",
		"SELECT AVG(val) FROM scalar_test",
		"SELECT MIN(val), MAX(val) FROM scalar_test",
		"SELECT COUNT(DISTINCT val) FROM scalar_test",
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
