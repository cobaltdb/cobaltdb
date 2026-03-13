package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestUseIndexForExactMatch_MoreCases - tests more index usage scenarios
func TestUseIndexForExactMatch_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table with unique index
	createCoverageTestTable(t, cat, "idx_unique_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText},
		{Name: "code", Type: query.TokenText},
	})

	// Create unique index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_email",
		Table:   "idx_unique_test",
		Columns: []string{"email"},
		Unique:  true,
	})

	// Create non-unique index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "idx_unique_test",
		Columns: []string{"code"},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		email := fmt.Sprintf("user%d@example.com", i)
		code := fmt.Sprintf("CODE%d", i%10)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "idx_unique_test",
			Columns: []string{"id", "email", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(email), strReal(code)}},
		}, nil)
	}

	// Query using unique index
	result, err := cat.ExecuteQuery(`SELECT * FROM idx_unique_test WHERE email = 'user5@example.com'`)
	if err != nil {
		t.Logf("Unique index query error: %v", err)
	} else {
		t.Logf("Unique index query returned %d rows", len(result.Rows))
	}

	// Query using non-unique index
	result, err = cat.ExecuteQuery(`SELECT * FROM idx_unique_test WHERE code = 'CODE5'`)
	if err != nil {
		t.Logf("Non-unique index query error: %v", err)
	} else {
		t.Logf("Non-unique index query returned %d rows", len(result.Rows))
	}
	_ = result
}

// TestResolveOuterRefsInQuery_MoreCases - tests outer reference resolution
func TestResolveOuterRefsInQuery_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_ref_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "outer_ref_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "data", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_ref_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_ref_sub",
			Columns: []string{"id", "main_id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Correlated subquery
	result, err := cat.ExecuteQuery(`
		SELECT * FROM outer_ref_main m
		WHERE EXISTS (SELECT 1 FROM outer_ref_sub s WHERE s.main_id = m.id)
	`)
	if err != nil {
		t.Logf("Correlated subquery error: %v", err)
	} else {
		t.Logf("Correlated subquery returned %d rows", len(result.Rows))
	}

	// Subquery with outer reference in SELECT
	result, err = cat.ExecuteQuery(`
		SELECT m.id, (SELECT COUNT(*) FROM outer_ref_sub s WHERE s.main_id = m.id) as sub_count
		FROM outer_ref_main m
	`)
	if err != nil {
		t.Logf("Subquery in SELECT error: %v", err)
	}
	_ = result
}

// TestApplyOuterQuery_MoreCases - tests applyOuterQuery function
func TestApplyOuterQuery_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "view_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "view_base",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i))}},
		}, nil)
	}

	// Create a complex view with GROUP BY
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "grp"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
		},
		From:    &query.TableRef{Name: "view_base"},
		GroupBy: []query.Expression{&query.QualifiedIdentifier{Column: "grp"}},
	}
	cat.CreateView("complex_agg_view", viewStmt)

	// Query view with ORDER BY on aggregate
	result, err := cat.ExecuteQuery(`
		SELECT * FROM complex_agg_view
		ORDER BY 2 DESC
	`)
	if err != nil {
		t.Logf("View with ORDER BY error: %v", err)
	} else {
		t.Logf("View query returned %d rows", len(result.Rows))
	}

	// Query view with LIMIT
	result, err = cat.ExecuteQuery(`
		SELECT * FROM complex_agg_view
		LIMIT 1
	`)
	if err != nil {
		t.Logf("View with LIMIT error: %v", err)
	}
	_ = result
}
