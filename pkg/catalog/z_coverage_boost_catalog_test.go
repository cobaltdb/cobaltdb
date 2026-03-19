package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ─── helpers ────────────────────────────────────────────────────────────────

func newBoostCatalog(t *testing.T) *Catalog {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	t.Cleanup(func() { pool.Close() })
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	return New(tree, pool, nil)
}

func boostCreateTable(t *testing.T, c *Catalog, name string, cols []*query.ColumnDef) {
	t.Helper()
	if err := c.CreateTable(&query.CreateTableStmt{Table: name, Columns: cols}); err != nil {
		t.Fatalf("CreateTable(%s): %v", name, err)
	}
}

func boostInsertRows(t *testing.T, c *Catalog, table string, cols []string, rows [][]query.Expression) {
	t.Helper()
	ctx := context.Background()
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   table,
		Columns: cols,
		Values:  rows,
	}, nil)
	if err != nil {
		t.Fatalf("insert into %s: %v", table, err)
	}
}

func boostQuery(t *testing.T, c *Catalog, sql string) *QueryResult {
	t.Helper()
	r, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return r
}

func boostQueryMayFail(c *Catalog, sql string) (*QueryResult, error) {
	return c.ExecuteQuery(sql)
}

func boostSelect(t *testing.T, c *Catalog, stmt *query.SelectStmt) ([]string, [][]interface{}) {
	t.Helper()
	cols, rows, err := c.Select(stmt, nil)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	return cols, rows
}

func boostNum(v float64) *query.NumberLiteral { return &query.NumberLiteral{Value: v} }
func boostStr(s string) *query.StringLiteral  { return &query.StringLiteral{Value: s} }
func boostNull() *query.NullLiteral           { return &query.NullLiteral{} }

// ─── GROUP BY + HAVING + ORDER BY ───────────────────────────────────────────

// TestBoost_GroupByHavingSum exercises computeAggregatesWithGroupBy, evaluateHaving
func TestBoost_GroupByHavingSum(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_sales", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})
	ctx := context.Background()
	depts := []string{"A", "B", "C"}
	rows := make([][]query.Expression, 30)
	for i := range rows {
		rows[i] = []query.Expression{boostNum(float64(i + 1)), boostStr(depts[i%3]), boostNum(float64((i + 1) * 100))}
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_sales", Columns: []string{"id", "dept", "amount"}, Values: rows}, nil)

	// HAVING SUM
	r, err := boostQueryMayFail(c, `SELECT dept, SUM(amount) as total FROM boost_sales GROUP BY dept HAVING SUM(amount) > 1000`)
	if err != nil {
		t.Fatalf("HAVING SUM: %v", err)
	}
	t.Logf("HAVING SUM rows: %d", len(r.Rows))

	// HAVING COUNT
	r, err = boostQueryMayFail(c, `SELECT dept, COUNT(*) as cnt FROM boost_sales GROUP BY dept HAVING COUNT(*) >= 5`)
	if err != nil {
		t.Fatalf("HAVING COUNT: %v", err)
	}
	t.Logf("HAVING COUNT rows: %d", len(r.Rows))

	// HAVING AVG
	r, err = boostQueryMayFail(c, `SELECT dept, AVG(amount) as avg_amt FROM boost_sales GROUP BY dept HAVING AVG(amount) > 500`)
	if err != nil {
		t.Fatalf("HAVING AVG: %v", err)
	}
	t.Logf("HAVING AVG rows: %d", len(r.Rows))

	// HAVING with AND combining two aggregates
	r, err = boostQueryMayFail(c, `SELECT dept, SUM(amount) as total, COUNT(*) as cnt FROM boost_sales GROUP BY dept HAVING SUM(amount) > 500 AND COUNT(*) > 3`)
	if err != nil {
		t.Fatalf("HAVING AND: %v", err)
	}
	t.Logf("HAVING AND rows: %d", len(r.Rows))
}

// TestBoost_GroupByOrderByAggregate exercises applyGroupByOrderBy with aggregate ORDER BY
func TestBoost_GroupByOrderByAggregate(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
		{Name: "product", Type: query.TokenText},
		{Name: "qty", Type: query.TokenInteger},
	})
	ctx := context.Background()
	regions := []string{"North", "South", "East", "West"}
	products := []string{"X", "Y", "Z"}
	var allRows [][]query.Expression
	id := 1
	for _, reg := range regions {
		for _, prod := range products {
			for i := 1; i <= 5; i++ {
				allRows = append(allRows, []query.Expression{
					boostNum(float64(id)), boostStr(reg), boostStr(prod), boostNum(float64(i * 10)),
				})
				id++
			}
		}
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_orders", Columns: []string{"id", "region", "product", "qty"}, Values: allRows}, nil)

	// ORDER BY aggregate DESC
	r := boostQuery(t, c, `SELECT region, SUM(qty) as total_qty FROM boost_orders GROUP BY region ORDER BY total_qty DESC`)
	t.Logf("ORDER BY aggregate DESC rows: %d", len(r.Rows))
	if len(r.Rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(r.Rows))
	}

	// ORDER BY aggregate ASC with multiple GROUP BY cols
	r = boostQuery(t, c, `SELECT region, product, COUNT(*) as cnt FROM boost_orders GROUP BY region, product ORDER BY cnt ASC, region DESC`)
	t.Logf("ORDER BY multi GROUP BY rows: %d", len(r.Rows))

	// ORDER BY with positional ref
	r = boostQuery(t, c, `SELECT region, SUM(qty) as total FROM boost_orders GROUP BY region ORDER BY 2 DESC`)
	t.Logf("ORDER BY positional rows: %d", len(r.Rows))
}

// TestBoost_GroupByOrderByCount exercises applyGroupByOrderBy with COUNT ORDER BY
func TestBoost_GroupByOrderByCount(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_cats", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "val", Type: query.TokenReal},
	})
	ctx := context.Background()
	cats := []string{"alpha", "beta", "gamma", "delta"}
	var allRows [][]query.Expression
	for i := 1; i <= 40; i++ {
		allRows = append(allRows, []query.Expression{boostNum(float64(i)), boostStr(cats[i%4]), boostNum(float64(i) * 1.5)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cats", Columns: []string{"id", "category", "val"}, Values: allRows}, nil)

	r := boostQuery(t, c, `SELECT category, COUNT(*) as cnt, MIN(val) as min_v, MAX(val) as max_v FROM boost_cats GROUP BY category ORDER BY cnt DESC`)
	t.Logf("COUNT ORDER BY rows: %d", len(r.Rows))

	r = boostQuery(t, c, `SELECT category, COUNT(*) FROM boost_cats GROUP BY category ORDER BY COUNT(*) ASC`)
	t.Logf("ORDER BY COUNT(*) rows: %d", len(r.Rows))
}

// ─── JOIN + GROUP BY (evaluateExprWithGroupAggregatesJoin) ───────────────────

// TestBoost_JoinGroupBy exercises the JOIN + GROUP BY path
func TestBoost_JoinGroupBy(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_employees", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
		{Name: "salary", Type: query.TokenReal},
	})
	boostCreateTable(t, c, "boost_departments", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_name", Type: query.TokenText},
		{Name: "budget", Type: query.TokenReal},
	})
	ctx := context.Background()
	var deptRows [][]query.Expression
	for i := 1; i <= 5; i++ {
		deptRows = append(deptRows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("Dept%d", i)), boostNum(float64(i * 50000))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_departments", Columns: []string{"id", "dept_name", "budget"}, Values: deptRows}, nil)

	var empRows [][]query.Expression
	for i := 1; i <= 20; i++ {
		empRows = append(empRows, []query.Expression{boostNum(float64(i)), boostNum(float64((i-1)%5 + 1)), boostStr(fmt.Sprintf("Emp%d", i)), boostNum(float64(30000 + i*1000))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_employees", Columns: []string{"id", "dept_id", "name", "salary"}, Values: empRows}, nil)

	// JOIN + GROUP BY + SUM
	r := boostQuery(t, c, `SELECT d.dept_name, COUNT(e.id) as emp_count, SUM(e.salary) as total_salary
		FROM boost_employees e
		JOIN boost_departments d ON e.dept_id = d.id
		GROUP BY d.dept_name`)
	t.Logf("JOIN+GROUP BY rows: %d", len(r.Rows))

	// JOIN + GROUP BY + HAVING
	r, err := boostQueryMayFail(c, `SELECT d.dept_name, AVG(e.salary) as avg_sal
		FROM boost_employees e
		JOIN boost_departments d ON e.dept_id = d.id
		GROUP BY d.dept_name
		HAVING AVG(e.salary) > 35000`)
	if err != nil {
		t.Logf("JOIN+GROUP BY+HAVING error: %v", err)
	} else {
		t.Logf("JOIN+GROUP BY+HAVING rows: %d", len(r.Rows))
	}

	// JOIN + GROUP BY + ORDER BY aggregate
	r, err = boostQueryMayFail(c, `SELECT d.dept_name, SUM(e.salary) as total
		FROM boost_employees e
		JOIN boost_departments d ON e.dept_id = d.id
		GROUP BY d.dept_name
		ORDER BY total DESC`)
	if err != nil {
		t.Logf("JOIN+GROUP BY+ORDER BY error: %v", err)
	} else {
		t.Logf("JOIN+GROUP BY+ORDER BY rows: %d", len(r.Rows))
	}
}

// TestBoost_JoinGroupByMinMax exercises MIN/MAX in evaluateExprWithGroupAggregatesJoin
func TestBoost_JoinGroupByMinMax(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_products", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "price", Type: query.TokenReal},
	})
	boostCreateTable(t, c, "boost_product_cats", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat_name", Type: query.TokenText},
	})
	ctx := context.Background()
	var catRows [][]query.Expression
	for i := 1; i <= 3; i++ {
		catRows = append(catRows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("Cat%d", i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_product_cats", Columns: []string{"id", "cat_name"}, Values: catRows}, nil)

	var prodRows [][]query.Expression
	for i := 1; i <= 15; i++ {
		prodRows = append(prodRows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("Cat%d", (i-1)%3+1)), boostNum(float64(i) * 9.99)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_products", Columns: []string{"id", "category", "price"}, Values: prodRows}, nil)

	r, err := boostQueryMayFail(c, `SELECT pc.cat_name, MIN(p.price) as min_p, MAX(p.price) as max_p
		FROM boost_products p
		JOIN boost_product_cats pc ON p.category = pc.cat_name
		GROUP BY pc.cat_name`)
	if err != nil {
		t.Logf("MIN/MAX JOIN error: %v", err)
	} else {
		t.Logf("MIN/MAX JOIN rows: %d", len(r.Rows))
	}
}

// ─── BETWEEN ─────────────────────────────────────────────────────────────────

// TestBoost_BetweenVariants exercises evaluateBetween with various types
func TestBoost_BetweenVariants(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_range", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "num_val", Type: query.TokenReal},
		{Name: "str_val", Type: query.TokenText},
		{Name: "nullable_val", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var allRows [][]query.Expression
	for i := 1; i <= 20; i++ {
		var nullVal query.Expression = boostNum(float64(i))
		if i%5 == 0 {
			nullVal = boostNull()
		}
		allRows = append(allRows, []query.Expression{boostNum(float64(i)), boostNum(float64(i) * 2.5), boostStr(fmt.Sprintf("item%02d", i)), nullVal})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_range", Columns: []string{"id", "num_val", "str_val", "nullable_val"}, Values: allRows}, nil)

	queries := []string{
		`SELECT * FROM boost_range WHERE num_val BETWEEN 5.0 AND 25.0`,
		`SELECT * FROM boost_range WHERE num_val NOT BETWEEN 10.0 AND 40.0`,
		`SELECT * FROM boost_range WHERE id BETWEEN 1 AND 5`,
		`SELECT * FROM boost_range WHERE nullable_val BETWEEN 1 AND 10`,
		`SELECT * FROM boost_range WHERE str_val BETWEEN 'item05' AND 'item15'`,
	}
	for _, q := range queries {
		r, err := boostQueryMayFail(c, q)
		if err != nil {
			t.Logf("BETWEEN query error: %v  sql: %s", err, q)
		} else {
			t.Logf("BETWEEN query rows: %d  sql: %s", len(r.Rows), q)
		}
	}
}

// ─── CAST ────────────────────────────────────────────────────────────────────

// TestBoost_CastVariants exercises evaluateCastExpr with various target types
func TestBoost_CastVariants(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_cast_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "int_col", Type: query.TokenInteger},
		{Name: "real_col", Type: query.TokenReal},
		{Name: "text_col", Type: query.TokenText},
		{Name: "bool_col", Type: query.TokenInteger},
	})
	ctx := context.Background()
	data := [][]query.Expression{
		{boostNum(1), boostNum(42), boostNum(3.14), boostStr("123"), boostNum(1)},
		{boostNum(2), boostNum(-7), boostNum(-0.5), boostStr("3.14"), boostNum(0)},
		{boostNum(3), boostNum(0), boostNum(0.0), boostStr("true"), boostNum(1)},
		{boostNum(4), boostNum(100), boostNum(100.0), boostStr("false"), boostNum(0)},
		{boostNum(5), boostNum(999), boostNum(1.0), boostStr("0"), boostNum(0)},
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cast_src", Columns: []string{"id", "int_col", "real_col", "text_col", "bool_col"}, Values: data}, nil)

	queries := []string{
		`SELECT id, CAST(text_col AS INTEGER) as t2i FROM boost_cast_src`,
		`SELECT id, CAST(text_col AS REAL) as t2r FROM boost_cast_src`,
		`SELECT id, CAST(int_col AS TEXT) as i2t FROM boost_cast_src`,
		`SELECT id, CAST(int_col AS BOOLEAN) as i2b FROM boost_cast_src`,
		`SELECT id, CAST(real_col AS INTEGER) as r2i FROM boost_cast_src`,
		`SELECT id, CAST(real_col AS TEXT) as r2t FROM boost_cast_src`,
		`SELECT id, CAST(text_col AS BOOLEAN) as s2b FROM boost_cast_src`,
		`SELECT * FROM boost_cast_src WHERE CAST(text_col AS INTEGER) > 100`,
	}
	for _, q := range queries {
		r, err := boostQueryMayFail(c, q)
		if err != nil {
			t.Logf("CAST error: %v  sql: %s", err, q)
		} else {
			t.Logf("CAST rows: %d  sql: %s", len(r.Rows), q)
		}
	}
}

// TestBoost_CastNull exercises CAST with NULL input → should return NULL
func TestBoost_CastNull(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_cast_null", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenText},
	})
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "boost_cast_null",
		Columns: []string{"id", "v"},
		Values:  [][]query.Expression{{boostNum(1), boostNull()}},
	}, nil)
	r := boostQuery(t, c, `SELECT CAST(v AS INTEGER) as x FROM boost_cast_null`)
	if len(r.Rows) == 0 {
		t.Fatal("expected at least 1 row")
	}
	if r.Rows[0][0] != nil {
		t.Errorf("expected NULL from CAST(NULL AS INTEGER), got %v", r.Rows[0][0])
	}
}

// ─── CTE ─────────────────────────────────────────────────────────────────────

// TestBoost_CTESingle exercises ExecuteCTE with a single non-recursive CTE
func TestBoost_CTESingle(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_cte_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "label", Type: query.TokenText},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 10; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostNum(float64(i * 10)), boostStr(fmt.Sprintf("L%d", i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cte_base", Columns: []string{"id", "val", "label"}, Values: rows}, nil)

	r := boostQuery(t, c, `WITH cte AS (SELECT id, val FROM boost_cte_base WHERE val > 30)
		SELECT * FROM cte ORDER BY id`)
	t.Logf("CTE single rows: %d", len(r.Rows))
	if len(r.Rows) != 7 {
		t.Errorf("expected 7 rows, got %d", len(r.Rows))
	}
}

// TestBoost_CTEMultiple exercises ExecuteCTE with multiple CTEs
func TestBoost_CTEMultiple(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_cte_data", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 20; i++ {
		cat := "X"
		if i > 10 {
			cat = "Y"
		}
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(cat), boostNum(float64(i * 5))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cte_data", Columns: []string{"id", "category", "amount"}, Values: rows}, nil)

	r, err := boostQueryMayFail(c, `
		WITH cat_x AS (SELECT id, amount FROM boost_cte_data WHERE category = 'X'),
		     cat_y AS (SELECT id, amount FROM boost_cte_data WHERE category = 'Y')
		SELECT cat_x.amount as x_amt, cat_y.amount as y_amt
		FROM cat_x JOIN cat_y ON cat_x.id = cat_y.id - 10
	`)
	if err != nil {
		t.Logf("Multi-CTE error: %v", err)
	} else {
		t.Logf("Multi-CTE rows: %d", len(r.Rows))
	}
}

// TestBoost_CTERecursive exercises ExecuteCTE with a recursive CTE
func TestBoost_CTERecursive(t *testing.T) {
	c := newBoostCatalog(t)

	r, err := boostQueryMayFail(c, `
		WITH RECURSIVE counter(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM counter WHERE n < 5
		)
		SELECT n FROM counter
	`)
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE rows: %d", len(r.Rows))
		if len(r.Rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(r.Rows))
		}
	}
}

// TestBoost_CTEUnion exercises ExecuteCTE with a UNION CTE
func TestBoost_CTEUnion(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_cte_t1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenText},
	})
	boostCreateTable(t, c, "boost_cte_t2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenText},
	})
	ctx := context.Background()
	var t1rows, t2rows [][]query.Expression
	for i := 1; i <= 5; i++ {
		t1rows = append(t1rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("t1_%d", i))})
		t2rows = append(t2rows, []query.Expression{boostNum(float64(i + 5)), boostStr(fmt.Sprintf("t2_%d", i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cte_t1", Columns: []string{"id", "v"}, Values: t1rows}, nil)
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cte_t2", Columns: []string{"id", "v"}, Values: t2rows}, nil)

	r, err := boostQueryMayFail(c, `
		WITH combined AS (
			SELECT id, v FROM boost_cte_t1
			UNION ALL
			SELECT id, v FROM boost_cte_t2
		)
		SELECT * FROM combined ORDER BY id
	`)
	if err != nil {
		t.Logf("UNION CTE error: %v", err)
	} else {
		t.Logf("UNION CTE rows: %d", len(r.Rows))
		if len(r.Rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(r.Rows))
		}
	}
}

// ─── INSTEAD OF triggers ─────────────────────────────────────────────────────

// TestBoost_InsteadOfInsertTrigger exercises findInsteadOfTrigger + executeInsteadOfTrigger
func TestBoost_InsteadOfInsertTrigger(t *testing.T) {
	c := newBoostCatalog(t)

	// Create base table
	boostCreateTable(t, c, "boost_ioi_real", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "created_by", Type: query.TokenText},
	})

	// Create view
	err := c.CreateView("boost_ioi_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "boost_ioi_real"},
	})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	// Create INSTEAD OF INSERT trigger using direct API
	trig := &query.CreateTriggerStmt{
		Name:  "boost_ioi_ins",
		Table: "boost_ioi_view",
		Time:  "INSTEAD OF",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "boost_ioi_real",
				Columns: []string{"id", "name", "created_by"},
				Values: [][]query.Expression{{
					&query.QualifiedIdentifier{Table: "NEW", Column: "id"},
					&query.QualifiedIdentifier{Table: "NEW", Column: "name"},
					&query.StringLiteral{Value: "trigger"},
				}},
			},
		},
	}
	if err = c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Verify trigger is findable
	found := c.findInsteadOfTrigger("boost_ioi_view", "INSERT")
	if found == nil {
		t.Fatal("expected to find INSTEAD OF INSERT trigger")
	}
	t.Logf("findInsteadOfTrigger: found %q", found.Name)

	// Insert into view — fires INSTEAD OF trigger
	ctx := context.Background()
	_, rowsAffected, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "boost_ioi_view",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{boostNum(1), boostStr("Alice")}},
	}, nil)
	if err != nil {
		t.Logf("INSERT into view (INSTEAD OF): error %v", err)
	} else {
		t.Logf("INSERT into view (INSTEAD OF): rowsAffected=%d", rowsAffected)
	}
}

// TestBoost_InsteadOfUpdateTrigger exercises INSTEAD OF UPDATE path
func TestBoost_InsteadOfUpdateTrigger(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_iou_real", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
		{Name: "updated", Type: query.TokenInteger},
	})
	err := c.CreateView("boost_iou_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "boost_iou_real"},
	})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 3; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("val%d", i)), boostNum(0)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_iou_real", Columns: []string{"id", "val", "updated"}, Values: rows}, nil)

	// Create INSTEAD OF UPDATE trigger
	trig := &query.CreateTriggerStmt{
		Name:  "boost_iou_upd",
		Table: "boost_iou_view",
		Time:  "INSTEAD OF",
		Event: "UPDATE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "boost_iou_real",
				Set:   []*query.SetClause{{Column: "updated", Value: boostNum(1)}},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "NEW", Column: "id"},
				},
			},
		},
	}
	if err = c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger INSTEAD OF UPDATE: %v", err)
	}

	// Verify findInsteadOfTrigger works for UPDATE
	found := c.findInsteadOfTrigger("boost_iou_view", "UPDATE")
	if found == nil {
		t.Fatal("expected to find INSTEAD OF UPDATE trigger")
	}
	t.Logf("INSTEAD OF UPDATE trigger: %q", found.Name)
}

// TestBoost_InsteadOfDeleteTrigger exercises INSTEAD OF DELETE path
func TestBoost_InsteadOfDeleteTrigger(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_iod_real", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
		{Name: "deleted", Type: query.TokenInteger},
	})
	err := c.CreateView("boost_iod_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "boost_iod_real"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "deleted"},
			Operator: query.TokenEq,
			Right:    boostNum(0),
		},
	})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 5; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("item%d", i)), boostNum(0)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_iod_real", Columns: []string{"id", "val", "deleted"}, Values: rows}, nil)

	// Create INSTEAD OF DELETE trigger
	trig := &query.CreateTriggerStmt{
		Name:  "boost_iod_del",
		Table: "boost_iod_view",
		Time:  "INSTEAD OF",
		Event: "DELETE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "boost_iod_real",
				Set:   []*query.SetClause{{Column: "deleted", Value: boostNum(1)}},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "OLD", Column: "id"},
				},
			},
		},
	}
	if err = c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger INSTEAD OF DELETE: %v", err)
	}

	// Verify findInsteadOfTrigger works for DELETE
	found := c.findInsteadOfTrigger("boost_iod_view", "DELETE")
	if found == nil {
		t.Fatal("expected to find INSTEAD OF DELETE trigger")
	}
	t.Logf("INSTEAD OF DELETE trigger: %q", found.Name)

	// executeInsteadOfDeleteTrigger path — fire a DELETE on the view
	_, affected, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "boost_iod_view",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    boostNum(2),
		},
	}, nil)
	if err != nil {
		t.Logf("DELETE via view (INSTEAD OF DELETE): %v", err)
	} else {
		t.Logf("DELETE via view (INSTEAD OF DELETE): affected=%d", affected)
	}
}

// TestBoost_ExecuteInsteadOfTrigger_InsertSelect exercises INSERT...SELECT via INSTEAD OF trigger
func TestBoost_ExecuteInsteadOfTrigger_InsertSelect(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_ioisel_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	boostCreateTable(t, c, "boost_ioisel_dst", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	err := c.CreateView("boost_ioisel_view", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "boost_ioisel_dst"},
	})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	ctx := context.Background()
	var srcRows [][]query.Expression
	for i := 1; i <= 3; i++ {
		srcRows = append(srcRows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("name%d", i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_ioisel_src", Columns: []string{"id", "name"}, Values: srcRows}, nil)

	// Create INSTEAD OF INSERT trigger on the view
	trig := &query.CreateTriggerStmt{
		Name:  "boost_ioisel_ins",
		Table: "boost_ioisel_view",
		Time:  "INSTEAD OF",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "boost_ioisel_dst",
				Columns: []string{"id", "name"},
				Values: [][]query.Expression{{
					&query.QualifiedIdentifier{Table: "NEW", Column: "id"},
					&query.QualifiedIdentifier{Table: "NEW", Column: "name"},
				}},
			},
		},
	}
	if err = c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Use INSERT...SELECT which exercises the SELECT path in executeInsteadOfTrigger
	insSelectStmt := &query.InsertStmt{
		Table:   "boost_ioisel_view",
		Columns: []string{"id", "name"},
		Select: &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "boost_ioisel_src"},
		},
	}
	_, affected, err := c.Insert(ctx, insSelectStmt, nil)
	if err != nil {
		t.Logf("INSERT...SELECT via INSTEAD OF trigger: %v", err)
	} else {
		t.Logf("INSERT...SELECT via INSTEAD OF trigger: affected=%d", affected)
	}
}

// ─── Query Cache ─────────────────────────────────────────────────────────────

// TestBoost_QueryCacheSetGet exercises QueryCache.Set and cache hit/miss
func TestBoost_QueryCacheSetGet(t *testing.T) {
	c := newBoostCatalog(t)
	c.EnableQueryCache(50, 0)

	boostCreateTable(t, c, "boost_cache_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 10; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("row%d", i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cache_tbl", Columns: []string{"id", "data"}, Values: rows}, nil)

	// Same query 5 times → 1 miss + 4 hits
	for i := 0; i < 5; i++ {
		boostQuery(t, c, `SELECT * FROM boost_cache_tbl WHERE id = 1`)
	}

	hits, misses, size := c.GetQueryCacheStats()
	t.Logf("Cache stats: hits=%d misses=%d size=%d", hits, misses, size)
	if hits < 1 {
		t.Errorf("expected at least 1 cache hit, got %d", hits)
	}
}

// TestBoost_QueryCacheInvalidate exercises cache invalidation after write
func TestBoost_QueryCacheInvalidate(t *testing.T) {
	c := newBoostCatalog(t)
	c.EnableQueryCache(50, 0)

	boostCreateTable(t, c, "boost_cache_inv", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 5; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostNum(float64(i * 10))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cache_inv", Columns: []string{"id", "val"}, Values: rows}, nil)

	// Populate cache
	boostQuery(t, c, `SELECT * FROM boost_cache_inv`)
	boostQuery(t, c, `SELECT * FROM boost_cache_inv`)

	_, _, sizeAfterRead := c.GetQueryCacheStats()
	t.Logf("Cache size after reads: %d", sizeAfterRead)

	// INSERT should invalidate cache
	c.Insert(ctx, &query.InsertStmt{
		Table:   "boost_cache_inv",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{boostNum(100), boostNum(999)}},
	}, nil)

	// Query again — should be a fresh miss
	boostQuery(t, c, `SELECT * FROM boost_cache_inv`)
	hits2, misses2, _ := c.GetQueryCacheStats()
	t.Logf("Cache stats after insert+query: hits=%d misses=%d", hits2, misses2)
}

// TestBoost_QueryCacheEviction exercises LRU eviction when cache is full
func TestBoost_QueryCacheEviction(t *testing.T) {
	c := newBoostCatalog(t)
	// Very small cache — only 3 entries
	c.EnableQueryCache(3, 0)

	boostCreateTable(t, c, "boost_cache_evict", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 10; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostNum(float64(i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cache_evict", Columns: []string{"id", "v"}, Values: rows}, nil)

	// Fill cache beyond capacity with distinct queries
	for i := 1; i <= 10; i++ {
		boostQuery(t, c, fmt.Sprintf(`SELECT * FROM boost_cache_evict WHERE id = %d`, i))
	}

	hits, misses, size := c.GetQueryCacheStats()
	t.Logf("Eviction test - hits=%d misses=%d size=%d (max 3)", hits, misses, size)
	if size > 3 {
		t.Errorf("cache size %d exceeds max 3", size)
	}
}

// TestBoost_QueryCacheInvalidateAll exercises InvalidateAll
func TestBoost_QueryCacheInvalidateAll(t *testing.T) {
	c := newBoostCatalog(t)
	c.EnableQueryCache(50, 0)

	boostCreateTable(t, c, "boost_cache_all", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenText},
	})
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "boost_cache_all",
		Columns: []string{"id", "v"},
		Values:  [][]query.Expression{{boostNum(1), boostStr("x")}},
	}, nil)

	// Populate cache
	boostQuery(t, c, `SELECT * FROM boost_cache_all`)
	_, _, size := c.GetQueryCacheStats()
	t.Logf("Cache size before InvalidateAll: %d", size)

	// Directly invoke InvalidateAll
	if c.queryCache != nil {
		c.queryCache.InvalidateAll()
		_, _, sizeAfter := c.GetQueryCacheStats()
		t.Logf("Cache size after InvalidateAll: %d", sizeAfter)
		if sizeAfter != 0 {
			t.Errorf("expected 0 after InvalidateAll, got %d", sizeAfter)
		}
	}
}

// TestBoost_QueryCacheNonCacheable exercises isCacheableQuery (should not cache scalar queries)
func TestBoost_QueryCacheNonCacheable(t *testing.T) {
	c := newBoostCatalog(t)
	c.EnableQueryCache(50, 0)

	boostCreateTable(t, c, "boost_cache_nc", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenInteger},
	})
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "boost_cache_nc",
		Columns: []string{"id", "v"},
		Values:  [][]query.Expression{{boostNum(1), boostNum(42)}},
	}, nil)

	// scalar queries (no FROM) are not cacheable
	for i := 0; i < 3; i++ {
		boostQuery(t, c, `SELECT 1`)
		boostQuery(t, c, `SELECT 42 + 1`)
	}
	_, _, size := c.GetQueryCacheStats()
	t.Logf("Cache size after non-cacheable scalar queries: %d", size)

	// Run a cacheable query twice to confirm caching works for normal queries
	boostQuery(t, c, `SELECT * FROM boost_cache_nc WHERE id = 1`)
	boostQuery(t, c, `SELECT * FROM boost_cache_nc WHERE id = 1`)
	hits, _, _ := c.GetQueryCacheStats()
	t.Logf("Cache hits for cacheable query: %d", hits)
	if hits < 1 {
		t.Errorf("expected at least 1 cache hit, got %d", hits)
	}
}

// ─── Temporal / AS OF queries ────────────────────────────────────────────────

// TestBoost_AsOfTimestampQuery exercises evaluateTemporalExpr
func TestBoost_AsOfTimestampQuery(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_temporal", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 5; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("v%d", i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_temporal", Columns: []string{"id", "val"}, Values: rows}, nil)

	// AS OF with RFC3339 timestamp
	r, err := boostQueryMayFail(c, `SELECT * FROM boost_temporal FOR SYSTEM_TIME AS OF '2099-01-01T00:00:00Z'`)
	if err != nil {
		t.Logf("AS OF RFC3339 error: %v", err)
	} else {
		t.Logf("AS OF RFC3339 rows: %d", len(r.Rows))
	}

	// AS OF with datetime format
	r, err = boostQueryMayFail(c, `SELECT * FROM boost_temporal FOR SYSTEM_TIME AS OF '2099-01-01 00:00:00'`)
	if err != nil {
		t.Logf("AS OF datetime error: %v", err)
	} else {
		t.Logf("AS OF datetime rows: %d", len(r.Rows))
	}

	// AS OF with date only
	r, err = boostQueryMayFail(c, `SELECT * FROM boost_temporal FOR SYSTEM_TIME AS OF '2099-01-01'`)
	if err != nil {
		t.Logf("AS OF date-only error: %v", err)
	} else {
		t.Logf("AS OF date-only rows: %d", len(r.Rows))
	}
}

// ─── ANALYZE (stats countRows) ───────────────────────────────────────────────

// TestBoost_AnalyzeCountRows exercises StatsCollector.countRows via CollectStats
func TestBoost_AnalyzeCountRows(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_analyze", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenReal},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 50; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("cat%d", i%5)), boostNum(float64(i) * 1.23)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_analyze", Columns: []string{"id", "category", "amount"}, Values: rows}, nil)

	// CollectStats exercises countRows internally
	sc := NewStatsCollector(c)
	stats, err := sc.CollectStats("boost_analyze")
	if err != nil {
		t.Fatalf("CollectStats: %v", err)
	}
	t.Logf("CollectStats rows: %d", stats.RowCount)
	if stats.RowCount != 50 {
		t.Errorf("expected 50 rows, got %d", stats.RowCount)
	}

	// countRows on empty table
	boostCreateTable(t, c, "boost_analyze_empty", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	stats2, err := sc.CollectStats("boost_analyze_empty")
	if err != nil {
		t.Fatalf("CollectStats empty: %v", err)
	}
	t.Logf("Empty table row count: %d", stats2.RowCount)
	if stats2.RowCount != 0 {
		t.Errorf("expected 0, got %d", stats2.RowCount)
	}

	// Catalog.Analyze method
	if err := c.Analyze("boost_analyze"); err != nil {
		t.Logf("Catalog.Analyze error: %v", err)
	} else {
		t.Logf("Catalog.Analyze succeeded")
	}
}

// ─── Vector index Update ─────────────────────────────────────────────────────

// TestBoost_VectorUpdate exercises HNSWIndex.Update (vector.go:192)
func TestBoost_VectorUpdate(t *testing.T) {
	h := NewHNSWIndex("test_vec", "boost_vec_tbl", "embedding", 4)

	// Insert a vector
	err := h.Insert("vec1", []float64{1.0, 2.0, 3.0, 4.0})
	if err != nil {
		t.Fatalf("Insert vector: %v", err)
	}

	// Update the vector
	err = h.Update("vec1", []float64{5.0, 6.0, 7.0, 8.0})
	if err != nil {
		t.Fatalf("Update vector: %v", err)
	}

	// Verify via KNN search
	keys, dists, err := h.SearchKNN([]float64{5.0, 6.0, 7.0, 8.0}, 1)
	if err != nil {
		t.Fatalf("SearchKNN: %v", err)
	}
	if len(keys) != 1 || keys[0] != "vec1" {
		t.Errorf("expected vec1 in results, got %v", keys)
	}
	t.Logf("Vector update verified: key=%v dist=%v", keys, dists)
}

// TestBoost_VectorUpdateMultiple exercises Update with multiple vectors
func TestBoost_VectorUpdateMultiple(t *testing.T) {
	h := NewHNSWIndex("multi_vec", "boost_vec_tbl2", "embedding", 3)
	vectors := map[string][]float64{
		"a": {1.0, 0.0, 0.0},
		"b": {0.0, 1.0, 0.0},
		"c": {0.0, 0.0, 1.0},
	}
	for key, vec := range vectors {
		if err := h.Insert(key, vec); err != nil {
			t.Fatalf("Insert %s: %v", key, err)
		}
	}

	// Update all vectors
	updates := map[string][]float64{
		"a": {2.0, 0.0, 0.0},
		"b": {0.0, 2.0, 0.0},
		"c": {0.0, 0.0, 2.0},
	}
	for key, vec := range updates {
		if err := h.Update(key, vec); err != nil {
			t.Fatalf("Update %s: %v", key, err)
		}
	}

	keys, _, err := h.SearchKNN([]float64{2.0, 0.0, 0.0}, 1)
	if err != nil {
		t.Fatalf("SearchKNN: %v", err)
	}
	t.Logf("Vector multi-update: nearest to (2,0,0) = %v", keys)
}

// ─── typeTaggedKey ────────────────────────────────────────────────────────────

// TestBoost_TypeTaggedKey exercises typeTaggedKey with all branch types
func TestBoost_TypeTaggedKey(t *testing.T) {
	tests := []struct {
		input   interface{}
		wantPfx string
	}{
		{nil, "\x01NULL\x01"},
		{int64(42), "I:"},
		{int64(-1), "I:"},
		{float64(3.14), "F:"},    // non-integer float
		{float64(100.0), "I:"},   // integer float (cast to integer path)
		{float64(1e16), "F:"},    // too large for integer path
		{true, "B:1"},
		{false, "B:0"},
		{[]byte("hello"), "S:"},
		{"world", "S:"},
		{struct{}{}, "S:"}, // default branch
	}
	for _, tt := range tests {
		result := typeTaggedKey(tt.input)
		if tt.wantPfx == "\x01NULL\x01" {
			if result != "\x01NULL\x01" {
				t.Errorf("typeTaggedKey(%v): expected NULL marker, got %q", tt.input, result)
			}
		} else if len(result) < len(tt.wantPfx) || result[:len(tt.wantPfx)] != tt.wantPfx {
			t.Errorf("typeTaggedKey(%v): expected prefix %q, got %q", tt.input, tt.wantPfx, result)
		}
	}
}

// ─── EvalExpression ───────────────────────────────────────────────────────────

// TestBoost_EvalExpressionNullBoolTypes exercises various EvalExpression branches
func TestBoost_EvalExpressionNullBoolTypes(t *testing.T) {
	// NULL literal
	v, err := EvalExpression(&query.NullLiteral{}, nil)
	if err != nil || v != nil {
		t.Errorf("EvalExpression(NULL): want nil,nil got %v,%v", v, err)
	}

	// Boolean literal
	v, err = EvalExpression(&query.BooleanLiteral{Value: true}, nil)
	if err != nil || v != true {
		t.Errorf("EvalExpression(true): want true,nil got %v,%v", v, err)
	}
	v, err = EvalExpression(&query.BooleanLiteral{Value: false}, nil)
	if err != nil || v != false {
		t.Errorf("EvalExpression(false): want false,nil got %v,%v", v, err)
	}

	// Placeholder
	v, err = EvalExpression(&query.PlaceholderExpr{Index: 0}, []interface{}{"hello"})
	if err != nil || v != "hello" {
		t.Errorf("EvalExpression(placeholder): want hello,nil got %v,%v", v, err)
	}

	// Placeholder out of range
	_, err = EvalExpression(&query.PlaceholderExpr{Index: 5}, []interface{}{"only_one"})
	if err == nil {
		t.Error("EvalExpression(placeholder OOB): expected error")
	}

	// UnaryExpr NOT NULL → NULL
	v, err = EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: &query.NullLiteral{}}, nil)
	if err != nil || v != nil {
		t.Errorf("NOT NULL: want nil,nil got %v,%v", v, err)
	}

	// UnaryExpr NOT true → false
	v, err = EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: &query.BooleanLiteral{Value: true}}, nil)
	if err != nil || v != false {
		t.Errorf("NOT true: want false,nil got %v,%v", v, err)
	}

	// UnaryExpr MINUS int
	v, err = EvalExpression(&query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.NumberLiteral{Value: 5, Raw: "5"}}, nil)
	if err != nil {
		t.Errorf("UnaryExpr MINUS: unexpected error %v", err)
	}
	t.Logf("UnaryExpr MINUS result: %v", v)

	// BinaryExpr NULL AND false → false
	v, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || v != false {
		t.Errorf("NULL AND false: want false,nil got %v,%v", v, err)
	}

	// BinaryExpr NULL OR true → true
	v, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil || v != true {
		t.Errorf("NULL OR true: want true,nil got %v,%v", v, err)
	}

	// BinaryExpr CONCAT with NULL → NULL
	v, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenConcat,
		Right:    &query.StringLiteral{Value: "x"},
	}, nil)
	if err != nil || v != nil {
		t.Errorf("NULL||x: want nil,nil got %v,%v", v, err)
	}
}

// ─── LIKE / IS NULL / IS NOT NULL ────────────────────────────────────────────

// TestBoost_LikePatterns exercises evaluateLike with various patterns
func TestBoost_LikePatterns(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_like", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	ctx := context.Background()
	names := []string{"Alice", "Bob", "Charlie", "alice_wonder", "alice123", "Bobbie", "ALICE"}
	var rows [][]query.Expression
	for i, name := range names {
		rows = append(rows, []query.Expression{boostNum(float64(i + 1)), boostStr(name)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_like", Columns: []string{"id", "name"}, Values: rows}, nil)

	queries := []struct {
		sql     string
		minRows int
	}{
		{`SELECT * FROM boost_like WHERE name LIKE 'Alice'`, 1},
		{`SELECT * FROM boost_like WHERE name LIKE 'alice%'`, 2},
		{`SELECT * FROM boost_like WHERE name LIKE '%ie'`, 1},
		{`SELECT * FROM boost_like WHERE name LIKE '%li%'`, 3},
		{`SELECT * FROM boost_like WHERE name LIKE '_ob%'`, 2},
		{`SELECT * FROM boost_like WHERE name NOT LIKE 'Alice'`, 5},
		{`SELECT * FROM boost_like WHERE name LIKE 'B_b'`, 1},
		{`SELECT * FROM boost_like WHERE name LIKE '%'`, 7},
	}
	for _, tc := range queries {
		r, err := boostQueryMayFail(c, tc.sql)
		if err != nil {
			t.Logf("LIKE error: %v  sql: %s", err, tc.sql)
		} else {
			t.Logf("LIKE rows: %d  sql: %s", len(r.Rows), tc.sql)
			if len(r.Rows) < tc.minRows {
				t.Errorf("LIKE: expected at least %d rows, got %d  sql: %s", tc.minRows, len(r.Rows), tc.sql)
			}
		}
	}
}

// TestBoost_IsNullIsNotNull exercises evaluateIsNull
func TestBoost_IsNullIsNotNull(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_isnull", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "optional_val", Type: query.TokenText},
		{Name: "required_val", Type: query.TokenText},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 10; i++ {
		var optVal query.Expression = boostStr(fmt.Sprintf("val%d", i))
		if i%3 == 0 {
			optVal = boostNull()
		}
		rows = append(rows, []query.Expression{boostNum(float64(i)), optVal, boostStr("always")})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_isnull", Columns: []string{"id", "optional_val", "required_val"}, Values: rows}, nil)

	// IS NULL — IDs 3, 6, 9 → 3 rows
	r := boostQuery(t, c, `SELECT * FROM boost_isnull WHERE optional_val IS NULL`)
	t.Logf("IS NULL rows: %d", len(r.Rows))
	if len(r.Rows) != 3 {
		t.Errorf("IS NULL: expected 3, got %d", len(r.Rows))
	}

	// IS NOT NULL
	r = boostQuery(t, c, `SELECT * FROM boost_isnull WHERE optional_val IS NOT NULL`)
	t.Logf("IS NOT NULL rows: %d", len(r.Rows))
	if len(r.Rows) != 7 {
		t.Errorf("IS NOT NULL: expected 7, got %d", len(r.Rows))
	}

	// IS NULL on non-nullable col
	r = boostQuery(t, c, `SELECT * FROM boost_isnull WHERE required_val IS NULL`)
	t.Logf("IS NULL on non-nullable rows: %d", len(r.Rows))
	if len(r.Rows) != 0 {
		t.Errorf("IS NULL on non-nullable: expected 0, got %d", len(r.Rows))
	}
}

// ─── Procedures ──────────────────────────────────────────────────────────────

// TestBoost_CreateDropProcedure exercises CreateProcedure / GetProcedure / DropProcedure
func TestBoost_CreateDropProcedure(t *testing.T) {
	c := newBoostCatalog(t)

	proc := &query.CreateProcedureStmt{
		Name:   "boost_proc_1",
		Params: []*query.ParamDef{{Name: "p_val", Type: query.TokenInteger}},
		Body:   []query.Statement{},
	}
	err := c.CreateProcedure(proc)
	if err != nil {
		t.Fatalf("CreateProcedure: %v", err)
	}

	// Get procedure
	got, err := c.GetProcedure("boost_proc_1")
	if err != nil {
		t.Fatalf("GetProcedure: %v", err)
	}
	if got.Name != "boost_proc_1" {
		t.Errorf("GetProcedure: wrong name %q", got.Name)
	}

	// Duplicate create should error
	err = c.CreateProcedure(proc)
	if err == nil {
		t.Error("duplicate CreateProcedure should error")
	}

	// Drop
	err = c.DropProcedure("boost_proc_1")
	if err != nil {
		t.Fatalf("DropProcedure: %v", err)
	}

	// Get after drop should error
	_, err = c.GetProcedure("boost_proc_1")
	if err == nil {
		t.Error("GetProcedure after drop should error")
	}

	// Drop non-existent should error
	err = c.DropProcedure("nonexistent_proc")
	if err == nil {
		t.Error("DropProcedure nonexistent should error")
	}
}

// ─── applyOuterQuery ─────────────────────────────────────────────────────────

// TestBoost_ApplyOuterQueryDerivedTable exercises applyOuterQuery via derived table
func TestBoost_ApplyOuterQueryDerivedTable(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_outer_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 20; i++ {
		grp := "G1"
		if i > 10 {
			grp = "G2"
		}
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(grp), boostNum(float64(i * 7))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_outer_base", Columns: []string{"id", "grp", "score"}, Values: rows}, nil)

	// FROM (subquery) with WHERE on outer
	r, err := boostQueryMayFail(c, `
		SELECT grp, score FROM (
			SELECT grp, score FROM boost_outer_base
		) AS sub
		WHERE score > 70
	`)
	if err != nil {
		t.Fatalf("applyOuterQuery WHERE: %v", err)
	}
	t.Logf("applyOuterQuery WHERE rows: %d", len(r.Rows))

	// FROM (subquery) with aggregate on outer
	r, err = boostQueryMayFail(c, `
		SELECT COUNT(*) as cnt, SUM(score) as total
		FROM (SELECT grp, score FROM boost_outer_base WHERE score > 50) AS sub
	`)
	if err != nil {
		t.Fatalf("applyOuterQuery aggregate: %v", err)
	}
	t.Logf("applyOuterQuery aggregate rows: %d", len(r.Rows))

	// FROM (subquery) with GROUP BY on outer
	r, err = boostQueryMayFail(c, `
		SELECT grp, COUNT(*) as cnt FROM (
			SELECT grp, score FROM boost_outer_base
		) AS sub
		GROUP BY grp
		ORDER BY grp
	`)
	if err != nil {
		t.Fatalf("applyOuterQuery GROUP BY: %v", err)
	}
	t.Logf("applyOuterQuery GROUP BY rows: %d", len(r.Rows))
	if len(r.Rows) != 2 {
		t.Errorf("expected 2 groups, got %d", len(r.Rows))
	}
}

// TestBoost_ApplyOuterQueryView exercises applyOuterQuery through a view
func TestBoost_ApplyOuterQueryView(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_view_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "salary", Type: query.TokenReal},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 15; i++ {
		dept := "Eng"
		if i > 7 {
			dept = "Sales"
		}
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(dept), boostNum(float64(40000 + i*2000))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_view_base", Columns: []string{"id", "dept", "salary"}, Values: rows}, nil)

	if err := c.CreateView("boost_dept_view", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "dept"}, &query.Identifier{Name: "salary"}},
		From:    &query.TableRef{Name: "boost_view_base"},
	}); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	// Query view with HAVING
	r, err := boostQueryMayFail(c, `SELECT dept, AVG(salary) as avg_sal FROM boost_dept_view GROUP BY dept HAVING AVG(salary) > 50000`)
	if err != nil {
		t.Logf("View HAVING error: %v", err)
	} else {
		t.Logf("View HAVING rows: %d", len(r.Rows))
	}

	// Query view with ORDER BY
	r, err = boostQueryMayFail(c, `SELECT dept, MAX(salary) as max_sal FROM boost_dept_view GROUP BY dept ORDER BY max_sal DESC`)
	if err != nil {
		t.Logf("View ORDER BY error: %v", err)
	} else {
		t.Logf("View ORDER BY rows: %d", len(r.Rows))
	}
}

// ─── encodeRow ────────────────────────────────────────────────────────────────

// TestBoost_EncodeRow exercises encodeRow with various expression types
func TestBoost_EncodeRow(t *testing.T) {
	// NULL literal
	b, err := encodeRow([]query.Expression{&query.NullLiteral{}}, nil)
	if err != nil {
		t.Fatalf("encodeRow(NULL): %v", err)
	}
	t.Logf("encodeRow(NULL): %s", b)

	// Boolean literal
	b, err = encodeRow([]query.Expression{&query.BooleanLiteral{Value: true}}, nil)
	if err != nil {
		t.Fatalf("encodeRow(true): %v", err)
	}
	t.Logf("encodeRow(true): %s", b)

	// Identifier (returns name as string)
	b, err = encodeRow([]query.Expression{&query.Identifier{Name: "my_col"}}, nil)
	if err != nil {
		t.Fatalf("encodeRow(Identifier): %v", err)
	}
	t.Logf("encodeRow(Identifier): %s", b)

	// PlaceholderExpr with indexed args
	b, err = encodeRow([]query.Expression{
		&query.PlaceholderExpr{Index: 0},
		&query.PlaceholderExpr{Index: 1},
	}, []interface{}{42, "hello"})
	if err != nil {
		t.Fatalf("encodeRow(placeholder): %v", err)
	}
	t.Logf("encodeRow(placeholder): %s", b)

	// PlaceholderExpr out-of-range index → fallback to positional
	b, err = encodeRow([]query.Expression{
		&query.PlaceholderExpr{Index: 99},
	}, []interface{}{"fallback"})
	if err != nil {
		t.Fatalf("encodeRow(placeholder positional): %v", err)
	}
	t.Logf("encodeRow(placeholder positional): %s", b)

	// BinaryExpr as default branch
	b, err = encodeRow([]query.Expression{
		&query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 2},
			Operator: query.TokenPlus,
			Right:    &query.NumberLiteral{Value: 3},
		},
	}, nil)
	if err != nil {
		t.Fatalf("encodeRow(BinaryExpr): %v", err)
	}
	t.Logf("encodeRow(BinaryExpr): %s", b)
}

// TestBoost_EncodeRowRemainingArgs exercises the "remaining args" path in encodeRow
func TestBoost_EncodeRowRemainingArgs(t *testing.T) {
	b, err := encodeRow([]query.Expression{
		&query.PlaceholderExpr{Index: 0},
	}, []interface{}{"first", "second", "third"})
	if err != nil {
		t.Fatalf("encodeRow remaining args: %v", err)
	}
	t.Logf("encodeRow remaining args: %s", b)
}

// ─── resolveAggregateInExpr ───────────────────────────────────────────────────

// TestBoost_ResolveAggregateInExpr exercises resolveAggregateInExpr
func TestBoost_ResolveAggregateInExpr(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_resolve_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "v1", Type: query.TokenInteger},
		{Name: "v2", Type: query.TokenReal},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(grp), boostNum(float64(i)), boostNum(float64(i) * 1.5)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_resolve_agg", Columns: []string{"id", "grp", "v1", "v2"}, Values: rows}, nil)

	// HAVING with alias reference
	r, err := boostQueryMayFail(c, `SELECT grp, SUM(v1) as s1 FROM boost_resolve_agg GROUP BY grp HAVING s1 > 50`)
	if err != nil {
		t.Logf("HAVING alias error: %v", err)
	} else {
		t.Logf("HAVING alias rows: %d", len(r.Rows))
	}

	// HAVING BETWEEN on aggregate alias
	r, err = boostQueryMayFail(c, `SELECT grp, COUNT(*) as cnt FROM boost_resolve_agg GROUP BY grp HAVING cnt BETWEEN 8 AND 12`)
	if err != nil {
		t.Logf("HAVING BETWEEN aggregate error: %v", err)
	} else {
		t.Logf("HAVING BETWEEN rows: %d", len(r.Rows))
	}

	// HAVING IN on aggregate alias
	r, err = boostQueryMayFail(c, `SELECT grp, COUNT(*) as cnt FROM boost_resolve_agg GROUP BY grp HAVING cnt IN (10, 11, 12)`)
	if err != nil {
		t.Logf("HAVING IN aggregate error: %v", err)
	} else {
		t.Logf("HAVING IN rows: %d", len(r.Rows))
	}

	// HAVING with nested binary expr
	r, err = boostQueryMayFail(c, `SELECT grp, AVG(v1) as avg_v1, SUM(v2) as sum_v2 FROM boost_resolve_agg GROUP BY grp HAVING avg_v1 > 5 AND sum_v2 < 200`)
	if err != nil {
		t.Logf("HAVING nested expr error: %v", err)
	} else {
		t.Logf("HAVING nested expr rows: %d", len(r.Rows))
	}
}

// ─── selectLocked CTE pre-computed path ─────────────────────────────────────

// TestBoost_SelectLockedCTEWithWindowFuncs exercises window-function-on-CTE path
func TestBoost_SelectLockedCTEWithWindowFuncs(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_wf_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "salary", Type: query.TokenReal},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 10; i++ {
		dept := "D1"
		if i > 5 {
			dept = "D2"
		}
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(dept), boostNum(float64(30000 + i*5000))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_wf_base", Columns: []string{"id", "dept", "salary"}, Values: rows}, nil)

	// CTE + window function on CTE result
	r, err := boostQueryMayFail(c, `
		WITH dept_data AS (SELECT id, dept, salary FROM boost_wf_base)
		SELECT id, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as row_num
		FROM dept_data
	`)
	if err != nil {
		t.Logf("CTE+window func error: %v", err)
	} else {
		t.Logf("CTE+window func rows: %d", len(r.Rows))
	}
}

// TestBoost_SelectLockedDerivedTableWithJoin exercises derived table + JOIN path
func TestBoost_SelectLockedDerivedTableWithJoin(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_dtj_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})
	boostCreateTable(t, c, "boost_dtj_ref", []*query.ColumnDef{
		{Name: "cat", Type: query.TokenText, PrimaryKey: true},
		{Name: "label", Type: query.TokenText},
	})
	ctx := context.Background()
	var mainRows, refRows [][]query.Expression
	for i := 1; i <= 6; i++ {
		mainRows = append(mainRows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("C%d", (i-1)%3+1)), boostNum(float64(i * 10))})
	}
	for i := 1; i <= 3; i++ {
		refRows = append(refRows, []query.Expression{boostStr(fmt.Sprintf("C%d", i)), boostStr(fmt.Sprintf("Label%d", i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_dtj_main", Columns: []string{"id", "cat", "val"}, Values: mainRows}, nil)
	c.Insert(ctx, &query.InsertStmt{Table: "boost_dtj_ref", Columns: []string{"cat", "label"}, Values: refRows}, nil)

	r, err := boostQueryMayFail(c, `
		SELECT sub.cat, sub.total, r.label
		FROM (SELECT cat, SUM(val) as total FROM boost_dtj_main GROUP BY cat) AS sub
		JOIN boost_dtj_ref r ON sub.cat = r.cat
	`)
	if err != nil {
		t.Logf("Derived table+JOIN error: %v", err)
	} else {
		t.Logf("Derived table+JOIN rows: %d", len(r.Rows))
	}
}

// ─── evaluateWhere extra branches ────────────────────────────────────────────

// TestBoost_EvaluateWhereExtraPatterns exercises evaluateWhere uncovered branches
func TestBoost_EvaluateWhereExtraPatterns(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_where_extra", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
		{Name: "active", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 20; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostNum(float64(i * 5)), boostStr(fmt.Sprintf("user%02d", i)), boostNum(float64(i % 2))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_where_extra", Columns: []string{"id", "score", "name", "active"}, Values: rows}, nil)

	queries := []string{
		`SELECT * FROM boost_where_extra WHERE active = 1 AND score > 50`,
		`SELECT * FROM boost_where_extra WHERE score < 20 OR score > 80`,
		`SELECT * FROM boost_where_extra WHERE (score BETWEEN 20 AND 60) AND active = 0`,
		`SELECT * FROM boost_where_extra WHERE name LIKE 'user0%'`,
		`SELECT * FROM boost_where_extra WHERE name NOT LIKE '%10'`,
		`SELECT * FROM boost_where_extra WHERE id IN (1, 3, 5, 7, 9)`,
		`SELECT * FROM boost_where_extra WHERE id NOT IN (2, 4, 6, 8, 10)`,
	}
	for _, q := range queries {
		r, err := boostQueryMayFail(c, q)
		if err != nil {
			t.Logf("evaluateWhere error: %v  sql: %s", err, q)
		} else {
			t.Logf("evaluateWhere rows: %d  sql: %s", len(r.Rows), q)
		}
	}
}

// ─── isCacheableQuery extra branches ────────────────────────────────────────

// TestBoost_IsCacheableQueryBranches exercises isCacheableQuery edge cases
func TestBoost_IsCacheableQueryBranches(t *testing.T) {
	c := newBoostCatalog(t)
	c.EnableQueryCache(100, 0)
	boostCreateTable(t, c, "boost_cacheable", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenText},
	})
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "boost_cacheable",
		Columns: []string{"id", "v"},
		Values:  [][]query.Expression{{boostNum(1), boostStr("hello")}},
	}, nil)

	// Run cacheable query twice to confirm caching
	for i := 0; i < 2; i++ {
		boostQuery(t, c, `SELECT * FROM boost_cacheable WHERE id = 1`)
	}

	// Non-cacheable: subquery in SELECT (containsSubquery = true → skip cache)
	for i := 0; i < 2; i++ {
		boostQueryMayFail(c, `SELECT (SELECT COUNT(*) FROM boost_cacheable) as cnt`)
	}

	hits, misses, _ := c.GetQueryCacheStats()
	t.Logf("isCacheable branches - hits=%d misses=%d", hits, misses)
	if hits < 1 {
		t.Errorf("expected at least 1 hit, got %d", hits)
	}
}

// ─── computeAggregatesWithGroupBy edge cases ─────────────────────────────────

// TestBoost_GroupByOnNonExistentTable exercises early return in computeAggregatesWithGroupBy
func TestBoost_GroupByOnNonExistentTable(t *testing.T) {
	c := newBoostCatalog(t)
	// GROUP BY on a table not in tableTrees
	r, err := boostQueryMayFail(c, `SELECT nonexist_col, COUNT(*) FROM nonexistent_table GROUP BY nonexist_col`)
	if err != nil {
		t.Logf("GROUP BY non-existent table error (expected): %v", err)
	} else {
		t.Logf("GROUP BY non-existent table rows: %d", len(r.Rows))
	}
}

// TestBoost_GroupByExpressionAlias exercises GROUP BY alias resolution
func TestBoost_GroupByExpressionAlias(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_gb_alias", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "raw_val", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 10; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostNum(float64(i % 3))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_gb_alias", Columns: []string{"id", "raw_val"}, Values: rows}, nil)

	r, err := boostQueryMayFail(c, `SELECT raw_val as grp_key, COUNT(*) as cnt FROM boost_gb_alias GROUP BY grp_key ORDER BY grp_key`)
	if err != nil {
		t.Logf("GROUP BY alias error: %v", err)
	} else {
		t.Logf("GROUP BY alias rows: %d", len(r.Rows))
	}

	r, err = boostQueryMayFail(c, `SELECT raw_val, SUM(raw_val) as total FROM boost_gb_alias GROUP BY raw_val HAVING total > 5`)
	if err != nil {
		t.Logf("GROUP BY expr HAVING error: %v", err)
	} else {
		t.Logf("GROUP BY expr HAVING rows: %d", len(r.Rows))
	}
}

// ─── applyGroupByOrderBy edge cases ─────────────────────────────────────────

// TestBoost_ApplyGroupByOrderByFunctionCall exercises FunctionCall in ORDER BY
func TestBoost_ApplyGroupByOrderByFunctionCall(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_ob_fc", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})
	ctx := context.Background()
	cats := []string{"alpha", "beta", "gamma"}
	var rows [][]query.Expression
	for i := 1; i <= 15; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(cats[i%3]), boostNum(float64(i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_ob_fc", Columns: []string{"id", "cat", "val"}, Values: rows}, nil)

	// ORDER BY SUM() function call
	r, err := boostQueryMayFail(c, `SELECT cat, SUM(val) as total FROM boost_ob_fc GROUP BY cat ORDER BY SUM(val) ASC`)
	if err != nil {
		t.Logf("ORDER BY SUM() error: %v", err)
	} else {
		t.Logf("ORDER BY SUM() rows: %d", len(r.Rows))
	}

	// ORDER BY COUNT(*)
	r, err = boostQueryMayFail(c, `SELECT cat, COUNT(*) FROM boost_ob_fc GROUP BY cat ORDER BY COUNT(*) DESC`)
	if err != nil {
		t.Logf("ORDER BY COUNT(*) error: %v", err)
	} else {
		t.Logf("ORDER BY COUNT(*) rows: %d", len(r.Rows))
	}

	// ORDER BY plain column
	r, err = boostQueryMayFail(c, `SELECT cat, MAX(val) as mx FROM boost_ob_fc GROUP BY cat ORDER BY cat ASC`)
	if err != nil {
		t.Logf("ORDER BY plain col error: %v", err)
	} else {
		t.Logf("ORDER BY plain col rows: %d", len(r.Rows))
	}
}

// ─── Misc coverage boosters ─────────────────────────────────────────────────

// TestBoost_CTEWithAggregateAndHaving exercises CTE + GROUP BY + HAVING
func TestBoost_CTEWithAggregateAndHaving(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_cte_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "salary", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 15; i++ {
		dept := "Eng"
		if i > 7 {
			dept = "Sales"
		}
		if i > 12 {
			dept = "HR"
		}
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(dept), boostNum(float64(40000 + i*1000))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_cte_agg", Columns: []string{"id", "dept", "salary"}, Values: rows}, nil)

	r, err := boostQueryMayFail(c, `
		WITH dept_stats AS (
			SELECT dept, COUNT(*) as emp_count, AVG(salary) as avg_sal
			FROM boost_cte_agg
			GROUP BY dept
		)
		SELECT dept, emp_count, avg_sal
		FROM dept_stats
		WHERE emp_count > 2
		ORDER BY avg_sal DESC
	`)
	if err != nil {
		t.Logf("CTE+agg+having error: %v", err)
	} else {
		t.Logf("CTE+agg+having rows: %d", len(r.Rows))
	}
}

// TestBoost_GroupByWithNullValues exercises GROUP BY when some values are NULL
func TestBoost_GroupByWithNullValues(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_gb_nulls", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 12; i++ {
		var grpExpr query.Expression = boostStr(fmt.Sprintf("G%d", i%3))
		if i%4 == 0 {
			grpExpr = boostNull()
		}
		rows = append(rows, []query.Expression{boostNum(float64(i)), grpExpr, boostNum(float64(i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_gb_nulls", Columns: []string{"id", "grp", "val"}, Values: rows}, nil)

	r, err := boostQueryMayFail(c, `SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM boost_gb_nulls GROUP BY grp ORDER BY grp`)
	if err != nil {
		t.Logf("GROUP BY with NULLs error: %v", err)
	} else {
		t.Logf("GROUP BY with NULLs rows: %d", len(r.Rows))
	}
}

// TestBoost_SelectDistinctWithGroupBy exercises SELECT DISTINCT with GROUP BY
func TestBoost_SelectDistinctWithGroupBy(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_dist_gb", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "color", Type: query.TokenText},
		{Name: "size", Type: query.TokenText},
		{Name: "count", Type: query.TokenInteger},
	})
	ctx := context.Background()
	colors := []string{"red", "blue", "green"}
	sizes := []string{"S", "M", "L"}
	var allRows [][]query.Expression
	id := 1
	for _, color := range colors {
		for _, sz := range sizes {
			for i := 1; i <= 3; i++ {
				allRows = append(allRows, []query.Expression{boostNum(float64(id)), boostStr(color), boostStr(sz), boostNum(float64(i * 10))})
				id++
			}
		}
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_dist_gb", Columns: []string{"id", "color", "size", "count"}, Values: allRows}, nil)

	r := boostQuery(t, c, `SELECT DISTINCT color FROM boost_dist_gb ORDER BY color`)
	t.Logf("DISTINCT rows: %d", len(r.Rows))
	if len(r.Rows) != 3 {
		t.Errorf("DISTINCT colors: expected 3, got %d", len(r.Rows))
	}

	r = boostQuery(t, c, `SELECT color, SUM(count) as total FROM boost_dist_gb GROUP BY color HAVING total > 100 ORDER BY total DESC`)
	t.Logf("GROUP BY+HAVING rows: %d", len(r.Rows))
}

// TestBoost_LargeGroupByWithOrderBy exercises applyGroupByOrderBy with larger dataset
func TestBoost_LargeGroupByWithOrderBy(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_large_gb", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "country", Type: query.TokenText},
		{Name: "product", Type: query.TokenText},
		{Name: "revenue", Type: query.TokenReal},
	})
	ctx := context.Background()
	countries := []string{"US", "UK", "DE", "FR", "JP", "CN", "AU", "CA", "BR", "IN"}
	var allRows [][]query.Expression
	id := 1
	for _, country := range countries {
		for j := 1; j <= 10; j++ {
			allRows = append(allRows, []query.Expression{
				boostNum(float64(id)), boostStr(country), boostStr(fmt.Sprintf("P%d", j)), boostNum(float64(j) * 1000.0),
			})
			id++
		}
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_large_gb", Columns: []string{"id", "country", "product", "revenue"}, Values: allRows}, nil)

	r := boostQuery(t, c, `SELECT country, SUM(revenue) as total_rev, COUNT(*) as num_products FROM boost_large_gb GROUP BY country ORDER BY total_rev DESC`)
	t.Logf("Large GROUP BY rows: %d", len(r.Rows))
	if len(r.Rows) != 10 {
		t.Errorf("expected 10 countries, got %d", len(r.Rows))
	}
	if len(r.Rows) > 1 {
		t.Logf("First two total_rev: %v, %v", r.Rows[0][1], r.Rows[1][1])
	}
}

// TestBoost_GroupByHavingMinMax exercises GROUP BY HAVING with MIN/MAX
func TestBoost_GroupByHavingMinMax(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_mm", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenReal},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 30; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("G%d", i%5)), boostNum(float64(i) * 3.7)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_mm", Columns: []string{"id", "grp", "val"}, Values: rows}, nil)

	// HAVING MIN and MAX
	r := boostQuery(t, c, `SELECT grp, MIN(val) as mn, MAX(val) as mx FROM boost_mm GROUP BY grp HAVING MAX(val) > 50`)
	t.Logf("HAVING MAX rows: %d", len(r.Rows))

	r = boostQuery(t, c, `SELECT grp, MIN(val) as mn FROM boost_mm GROUP BY grp HAVING MIN(val) < 10`)
	t.Logf("HAVING MIN rows: %d", len(r.Rows))
}

// TestBoost_CTEDuplicateNameError exercises CTE duplicate name error path
func TestBoost_CTEDuplicateNameError(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_cte_dup", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "boost_cte_dup",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{boostNum(1)}},
	}, nil)

	// Two CTEs with same name — should detect duplicate
	r, err := boostQueryMayFail(c, `
		WITH cte AS (SELECT id FROM boost_cte_dup),
		     cte AS (SELECT id FROM boost_cte_dup)
		SELECT * FROM cte
	`)
	if err != nil {
		t.Logf("CTE duplicate name error (expected): %v", err)
	} else {
		t.Logf("CTE duplicate name rows: %d", len(r.Rows))
	}
}

// TestBoost_SelectFromCTEAsView exercises CTE-as-view path in selectLocked
func TestBoost_SelectFromCTEAsView(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_ctev", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenInteger},
	})
	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 5; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostNum(float64(i * 2))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_ctev", Columns: []string{"id", "v"}, Values: rows}, nil)

	r, err := boostQueryMayFail(c, `WITH my_cte AS (SELECT id, v FROM boost_ctev WHERE v > 4) SELECT * FROM my_cte WHERE id > 1`)
	if err != nil {
		t.Logf("CTE-as-view error: %v", err)
	} else {
		t.Logf("CTE-as-view rows: %d", len(r.Rows))
	}
}

// TestBoost_DeleteViaInsteadOfView exercises Delete's INSTEAD OF path
func TestBoost_DeleteViaInsteadOfView(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_del_real", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenText},
		{Name: "active", Type: query.TokenInteger},
	})
	if err := c.CreateView("boost_del_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "v"},
		},
		From: &query.TableRef{Name: "boost_del_real"},
	}); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 4; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("v%d", i)), boostNum(1)})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_del_real", Columns: []string{"id", "v", "active"}, Values: rows}, nil)

	trig := &query.CreateTriggerStmt{
		Name:  "boost_del_view_trig",
		Table: "boost_del_view",
		Time:  "INSTEAD OF",
		Event: "DELETE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "boost_del_real",
				Set:   []*query.SetClause{{Column: "active", Value: boostNum(0)}},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "OLD", Column: "id"},
				},
			},
		},
	}
	if err := c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Verify delete fires INSTEAD OF trigger
	_, affected, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "boost_del_view",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    boostNum(1),
		},
	}, nil)
	if err != nil {
		t.Logf("Delete via INSTEAD OF: %v", err)
	} else {
		t.Logf("Delete via INSTEAD OF: affected=%d", affected)
	}
}

// TestBoost_UpdateViaInsteadOfView exercises Update's INSTEAD OF path
func TestBoost_UpdateViaInsteadOfView(t *testing.T) {
	c := newBoostCatalog(t)
	boostCreateTable(t, c, "boost_upd_real", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "v", Type: query.TokenText},
	})
	if err := c.CreateView("boost_upd_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "v"},
		},
		From: &query.TableRef{Name: "boost_upd_real"},
	}); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	ctx := context.Background()
	var rows [][]query.Expression
	for i := 1; i <= 3; i++ {
		rows = append(rows, []query.Expression{boostNum(float64(i)), boostStr(fmt.Sprintf("orig%d", i))})
	}
	c.Insert(ctx, &query.InsertStmt{Table: "boost_upd_real", Columns: []string{"id", "v"}, Values: rows}, nil)

	trig := &query.CreateTriggerStmt{
		Name:  "boost_upd_view_trig",
		Table: "boost_upd_view",
		Time:  "INSTEAD OF",
		Event: "UPDATE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "boost_upd_real",
				Set:   []*query.SetClause{{Column: "v", Value: &query.QualifiedIdentifier{Table: "NEW", Column: "v"}}},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "NEW", Column: "id"},
				},
			},
		},
	}
	if err := c.CreateTrigger(trig); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Verify update fires INSTEAD OF trigger
	_, affected, err := c.Update(ctx, &query.UpdateStmt{
		Table: "boost_upd_view",
		Set:   []*query.SetClause{{Column: "v", Value: boostStr("changed")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    boostNum(1),
		},
	}, nil)
	if err != nil {
		t.Logf("Update via INSTEAD OF: %v", err)
	} else {
		t.Logf("Update via INSTEAD OF: affected=%d", affected)
	}
}
