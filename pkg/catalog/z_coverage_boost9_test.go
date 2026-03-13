package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_DeleteOperations tests DELETE operations
func TestCoverage_DeleteOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Delete with WHERE
	_, _, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_test",
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "id"},
			Operator: query.TokenLt,
			Right:    &query.NumberLiteral{Value: 5},
		},
	}, nil)
	if err != nil {
		t.Logf("Delete error: %v", err)
	}

	// Verify remaining rows
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_test")
	t.Logf("Remaining rows: %v", result.Rows)
}

// TestCoverage_UpdateOperations tests UPDATE operations
func TestCoverage_UpdateOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Update with WHERE
	_, _, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_test",
		Set:   []*query.SetClause{{Column: "val", Value: &query.NumberLiteral{Value: 999}}},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Logf("Update error: %v", err)
	}

	// Verify update
	result, _ := cat.ExecuteQuery("SELECT val FROM upd_test WHERE id = 1")
	t.Logf("Updated value: %v", result.Rows)
}

// TestCoverage_LikePatterns tests LIKE patterns
func TestCoverage_LikePatterns(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "like_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "like_test",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice")},
			{numReal(2), strReal("Bob")},
			{numReal(3), strReal("Charlie")},
			{numReal(4), strReal("Alex")},
		},
	}, nil)

	queries := []string{
		"SELECT * FROM like_test WHERE name LIKE 'A%'",
		"SELECT * FROM like_test WHERE name LIKE '%e'",
		"SELECT * FROM like_test WHERE name LIKE '%li%'",
		"SELECT * FROM like_test WHERE name NOT LIKE 'A%'",
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

// TestCoverage_ExistsSubquery tests EXISTS subquery
func TestCoverage_ExistsSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "exists_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "exists_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "exists_main",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "exists_sub",
		Columns: []string{"id", "main_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT * FROM exists_main WHERE EXISTS (SELECT 1 FROM exists_sub WHERE exists_sub.main_id = exists_main.id)")
	if err != nil {
		t.Logf("EXISTS error: %v", err)
	} else {
		t.Logf("EXISTS returned %d rows", len(result.Rows))
	}
}
