package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Deep coverage for applyGroupByOrderBy branches
// ============================================================

// TestApplyGroupByOrderBy_PositionalMore - positional ORDER BY (ORDER BY 1, 2)
func TestApplyGroupByOrderBy_PositionalMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_pos", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		category := "B"
		if i <= 10 {
			category = "A"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_pos",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64((21-i) * 10))}},
		}, nil)
	}

	// Positional ORDER BY
	queries := []string{
		`SELECT category, COUNT(*) FROM gb_pos GROUP BY category ORDER BY 1`,
		`SELECT category, COUNT(*) FROM gb_pos GROUP BY category ORDER BY 2`,
		`SELECT category, SUM(amount) FROM gb_pos GROUP BY category ORDER BY 2 DESC`,
		`SELECT category, COUNT(*), SUM(amount) FROM gb_pos GROUP BY category ORDER BY 1, 2`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestApplyGroupByOrderBy_StringComparison - string comparison in ORDER BY
func TestApplyGroupByOrderBy_StringComparison(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_str", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	names := []string{"Charlie", "Alice", "Bob", "Diana", "Eve"}
	for i := 1; i <= 20; i++ {
		name := names[i%5]
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_str",
			Columns: []string{"id", "name", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(name), numReal(float64(i * 10))}},
		}, nil)
	}

	// String ORDER BY
	queries := []string{
		`SELECT name, COUNT(*) FROM gb_str GROUP BY name ORDER BY name`,
		`SELECT name, COUNT(*) FROM gb_str GROUP BY name ORDER BY name ASC`,
		`SELECT name, COUNT(*) FROM gb_str GROUP BY name ORDER BY name DESC`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestApplyGroupByOrderBy_StarExpr - ORDER BY with COUNT(*)
func TestApplyGroupByOrderBy_StarExpr(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_star", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		if i > 20 {
			grp = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_star",
			Columns: []string{"id", "grp"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp)}},
		}, nil)
	}

	// ORDER BY COUNT(*)
	queries := []string{
		`SELECT grp, COUNT(*) FROM gb_star GROUP BY grp ORDER BY COUNT(*)`,
		`SELECT grp, COUNT(*) FROM gb_star GROUP BY grp ORDER BY COUNT(*) DESC`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestApplyGroupByOrderBy_QualifiedIdentifier - ORDER BY with qualified identifiers
func TestApplyGroupByOrderBy_QualifiedIdentifier(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_qual", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		category := "A"
		if i > 10 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_qual",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	// These might use qualified identifiers internally
	queries := []string{
		`SELECT category, SUM(amount) as total FROM gb_qual GROUP BY category ORDER BY total`,
		`SELECT category, COUNT(*) as cnt FROM gb_qual GROUP BY category ORDER BY cnt DESC`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestApplyGroupByOrderBy_Nulls - NULL handling in ORDER BY
func TestApplyGroupByOrderBy_Nulls(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_null_ob", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 15; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		if i > 10 {
			grp = "C"
		}
		var val query.Expression
		if i%3 == 0 {
			val = &query.NullLiteral{}
		} else {
			val = numReal(float64(i * 10))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_null_ob",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), val}},
		}, nil)
	}

	// ORDER BY with NULLs
	queries := []string{
		`SELECT grp, SUM(val) FROM gb_null_ob GROUP BY grp ORDER BY SUM(val)`,
		`SELECT grp, SUM(val) FROM gb_null_ob GROUP BY grp ORDER BY SUM(val) DESC`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestComputeAggregatesWithGroupBy_NonAggregate - non-aggregate column handling
func TestComputeAggregatesWithGroupBy_NonAggregate(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "gb_non_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
		{Name: "extra", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_non_agg",
			Columns: []string{"id", "grp", "val", "extra"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i)), strReal("extra")}},
		}, nil)
	}

	// Non-aggregate columns alongside aggregates
	queries := []string{
		`SELECT grp, COUNT(*), extra FROM gb_non_agg GROUP BY grp`,
		`SELECT grp, val, SUM(val) FROM gb_non_agg GROUP BY grp`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestComputeViewAggregate_ExtraCases - additional view aggregate coverage
func TestComputeViewAggregate_ExtraCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "view_agg_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert with some NULL values
	for i := 1; i <= 10; i++ {
		var val query.Expression
		if i%4 == 0 {
			val = &query.NullLiteral{}
		} else {
			val = numReal(float64(i * 10))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "view_agg_more",
			Columns: []string{"id", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), val, strReal("name")}},
		}, nil)
	}

	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "val"},
			&query.QualifiedIdentifier{Column: "name"},
		},
		From: &query.TableRef{Name: "view_agg_more"},
	}
	cat.CreateView("view_agg_more_view", viewStmt)

	queries := []string{
		`SELECT COUNT(val) FROM view_agg_more_view`,
		`SELECT COUNT(name) FROM view_agg_more_view`,
		`SELECT MIN(val) FROM view_agg_more_view`,
		`SELECT MAX(val) FROM view_agg_more_view`,
		`SELECT SUM(val) FROM view_agg_more_view`,
		`SELECT AVG(val) FROM view_agg_more_view`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d result: %v", i, result.Rows)
		}
	}
}

// TestToInt_MoreCases - toInt coverage
func TestToInt_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "toint_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ival", Type: query.TokenInteger},
		{Name: "rval", Type: query.TokenReal},
		{Name: "tval", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "toint_test",
		Columns: []string{"id", "ival", "rval", "tval"},
		Values:  [][]query.Expression{{numReal(1), numReal(100), numReal(3.14), strReal("42")}},
	}, nil)

	// These should exercise toInt
	queries := []string{
		`SELECT ival FROM toint_test LIMIT 1`,
		`SELECT rval FROM toint_test LIMIT 1`,
		`SELECT CAST(tval AS INTEGER) FROM toint_test`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d result: %v", i, result.Rows)
		}
	}
}
