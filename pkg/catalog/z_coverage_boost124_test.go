package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newCat124() *Catalog {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	return New(tree, pool, nil)
}

func exec124(c *Catalog, sql string) {
	_, _ = c.ExecuteQuery(sql)
}

func query124(c *Catalog, sql string) [][]interface{} {
	r, _ := c.ExecuteQuery(sql)
	if r == nil {
		return nil
	}
	return r.Rows
}

// ─── JOIN + GROUP BY (evaluateExprWithGroupAggregatesJoin) ──────────────────

func TestB124_JoinGroupByAggregates(t *testing.T) {
	c := newCat124()
	exec124(c, "CREATE TABLE b124_orders (id INTEGER PRIMARY KEY, cid INTEGER, amount INTEGER)")
	exec124(c, "CREATE TABLE b124_customers (id INTEGER PRIMARY KEY, name TEXT)")
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table: "b124_customers", Columns: []string{"id", "name"},
			Values: [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: "c"}}},
		}, nil)
	}
	amounts := []int{100, 200, 150, 300, 250, 50}
	cids := []int{1, 2, 1, 3, 2, 3}
	for i, a := range amounts {
		c.Insert(ctx, &query.InsertStmt{
			Table: "b124_orders", Columns: []string{"id", "cid", "amount"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i + 1)},
				&query.NumberLiteral{Value: float64(cids[i])},
				&query.NumberLiteral{Value: float64(a)},
			}},
		}, nil)
	}

	// JOIN + GROUP BY + SUM → exercises evaluateExprWithGroupAggregatesJoin
	rows := query124(c, `SELECT b124_customers.name, SUM(b124_orders.amount) FROM b124_orders JOIN b124_customers ON b124_orders.cid = b124_customers.id GROUP BY b124_customers.name`)
	t.Logf("JOIN GROUP BY SUM rows: %v", rows)

	// JOIN + GROUP BY + COUNT
	rows = query124(c, `SELECT b124_customers.name, COUNT(*) FROM b124_orders JOIN b124_customers ON b124_orders.cid = b124_customers.id GROUP BY b124_customers.name`)
	t.Logf("JOIN GROUP BY COUNT rows: %v", rows)

	// JOIN + GROUP BY + AVG
	rows = query124(c, `SELECT b124_customers.name, AVG(b124_orders.amount) FROM b124_orders JOIN b124_customers ON b124_orders.cid = b124_customers.id GROUP BY b124_customers.name`)
	t.Logf("JOIN GROUP BY AVG rows: %v", rows)

	// JOIN + GROUP BY + MIN/MAX
	rows = query124(c, `SELECT b124_customers.name, MIN(b124_orders.amount), MAX(b124_orders.amount) FROM b124_orders JOIN b124_customers ON b124_orders.cid = b124_customers.id GROUP BY b124_customers.name`)
	t.Logf("JOIN GROUP BY MIN/MAX rows: %v", rows)

	// JOIN + GROUP BY + HAVING
	rows = query124(c, `SELECT b124_customers.name, SUM(b124_orders.amount) FROM b124_orders JOIN b124_customers ON b124_orders.cid = b124_customers.id GROUP BY b124_customers.name HAVING SUM(b124_orders.amount) > 200`)
	t.Logf("JOIN GROUP BY HAVING rows: %v", rows)

	// JOIN + GROUP BY + ORDER BY aggregate
	rows = query124(c, `SELECT b124_customers.name, SUM(b124_orders.amount) FROM b124_orders JOIN b124_customers ON b124_orders.cid = b124_customers.id GROUP BY b124_customers.name ORDER BY SUM(b124_orders.amount) DESC`)
	t.Logf("JOIN GROUP BY ORDER BY DESC rows: %v", rows)
}

// ─── applyGroupByOrderBy branches ───────────────────────────────────────────

func TestB124_GroupByOrderByBranches(t *testing.T) {
	c := newCat124()
	exec124(c, "CREATE TABLE b124_data (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	ctx := context.Background()
	entries := []struct{ cat string; val int }{
		{"X", 10}, {"Y", 30}, {"X", 20}, {"Z", 5}, {"Y", 15}, {"Z", 25},
	}
	for i, e := range entries {
		c.Insert(ctx, &query.InsertStmt{
			Table: "b124_data", Columns: []string{"id", "cat", "val"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i + 1)},
				&query.StringLiteral{Value: e.cat},
				&query.NumberLiteral{Value: float64(e.val)},
			}},
		}, nil)
	}

	// ORDER BY aggregate name (FunctionCall path in applyGroupByOrderBy)
	rows := query124(c, `SELECT cat, SUM(val) FROM b124_data GROUP BY cat ORDER BY SUM(val) ASC`)
	t.Logf("ORDER BY SUM ASC: %v", rows)

	// ORDER BY aggregate DESC
	rows = query124(c, `SELECT cat, MAX(val) FROM b124_data GROUP BY cat ORDER BY MAX(val) DESC`)
	t.Logf("ORDER BY MAX DESC: %v", rows)

	// ORDER BY positional reference (NumberLiteral path)
	rows = query124(c, `SELECT cat, COUNT(*) FROM b124_data GROUP BY cat ORDER BY 2 DESC`)
	t.Logf("ORDER BY positional: %v", rows)

	// ORDER BY positional ASC with NULLs
	exec124(c, "CREATE TABLE b124_nulldata (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.Insert(ctx, &query.InsertStmt{
		Table: "b124_nulldata", Columns: []string{"id", "cat", "val"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "A"}, &query.NullLiteral{}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "B"}, &query.NumberLiteral{Value: 42}},
		},
	}, nil)
	rows = query124(c, `SELECT cat, SUM(val) FROM b124_nulldata GROUP BY cat ORDER BY 2 ASC`)
	t.Logf("ORDER BY positional with NULL: %v", rows)

	// ORDER BY qualified identifier (QualifiedIdentifier path)
	rows = query124(c, `SELECT b124_data.cat, SUM(b124_data.val) FROM b124_data GROUP BY b124_data.cat ORDER BY b124_data.cat`)
	t.Logf("ORDER BY qualified: %v", rows)
}

// ─── BETWEEN branches (NULL values) ─────────────────────────────────────────

func TestB124_BetweenNullBranches(t *testing.T) {
	c := newCat124()
	exec124(c, "CREATE TABLE b124_between (id INTEGER PRIMARY KEY, v INTEGER)")
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table: "b124_between", Columns: []string{"id", "v"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.NullLiteral{}},
			{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 5}},
			{&query.NumberLiteral{Value: 3}, &query.NumberLiteral{Value: 15}},
		},
	}, nil)

	// NULL value → BETWEEN returns NULL → filtered out by WHERE
	rows := query124(c, `SELECT id FROM b124_between WHERE v BETWEEN 1 AND 10`)
	t.Logf("BETWEEN with NULL in data: %v", rows)
	if len(rows) != 1 {
		t.Errorf("expected 1 row (only id=2), got %d", len(rows))
	}

	// NOT BETWEEN
	rows = query124(c, `SELECT id FROM b124_between WHERE v NOT BETWEEN 1 AND 10`)
	t.Logf("NOT BETWEEN: %v", rows)

	// BETWEEN strings
	exec124(c, "CREATE TABLE b124_strbetween (id INTEGER PRIMARY KEY, s TEXT)")
	c.Insert(ctx, &query.InsertStmt{
		Table: "b124_strbetween", Columns: []string{"id", "s"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "apple"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "mango"}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "zebra"}},
		},
	}, nil)
	rows = query124(c, `SELECT id FROM b124_strbetween WHERE s BETWEEN 'banana' AND 'orange'`)
	t.Logf("BETWEEN strings: %v", rows)
}

// ─── CAST branches ───────────────────────────────────────────────────────────

func TestB124_CastBranches(t *testing.T) {
	c := newCat124()
	exec124(c, "CREATE TABLE b124_cast (id INTEGER PRIMARY KEY, s TEXT, n INTEGER, r REAL)")
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table: "b124_cast", Columns: []string{"id", "s", "n", "r"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "42"}, &query.NumberLiteral{Value: 100}, &query.NumberLiteral{Value: 3.14}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "abc"}, &query.NumberLiteral{Value: 0}, &query.NumberLiteral{Value: 0}},
			{&query.NumberLiteral{Value: 3}, &query.NullLiteral{}, &query.NullLiteral{}, &query.NullLiteral{}},
		},
	}, nil)

	rows := query124(c, `SELECT CAST(s AS INTEGER) FROM b124_cast`)
	t.Logf("CAST TEXT→INTEGER: %v", rows)

	rows = query124(c, `SELECT CAST(n AS TEXT) FROM b124_cast`)
	t.Logf("CAST INTEGER→TEXT: %v", rows)

	rows = query124(c, `SELECT CAST(r AS INTEGER) FROM b124_cast`)
	t.Logf("CAST REAL→INTEGER: %v", rows)

	rows = query124(c, `SELECT CAST(n AS REAL) FROM b124_cast`)
	t.Logf("CAST INTEGER→REAL: %v", rows)

	rows = query124(c, `SELECT CAST(s AS BOOLEAN) FROM b124_cast`)
	t.Logf("CAST TEXT→BOOLEAN: %v", rows)

	rows = query124(c, `SELECT CAST(n AS BOOLEAN) FROM b124_cast`)
	t.Logf("CAST INTEGER→BOOLEAN: %v", rows)

	// CAST NULL
	rows = query124(c, `SELECT CAST(NULL AS INTEGER) FROM b124_cast WHERE id = 1`)
	t.Logf("CAST NULL→INTEGER: %v", rows)
}

// ─── vector.Update and vector.max ────────────────────────────────────────────

func TestB124_VectorUpdateAndMax(t *testing.T) {
	h := NewHNSWIndex("b124_vec", "tbl", "col", 3)

	// Insert multiple
	vecs := map[string][]float64{
		"v1": {1, 0, 0},
		"v2": {0, 1, 0},
		"v3": {0, 0, 1},
	}
	for k, v := range vecs {
		if err := h.Insert(k, v); err != nil {
			t.Fatalf("Insert %s: %v", k, err)
		}
	}

	// Update — covers Update (66.7%)
	if err := h.Update("v1", []float64{0.9, 0.1, 0}); err != nil {
		t.Fatalf("Update v1: %v", err)
	}
	// Update non-existent key (should not panic)
	_ = h.Update("nonexistent", []float64{1, 1, 1})

	// SearchKNN covers max() helper inside selectNeighborsByKey
	keys, dists, err := h.SearchKNN([]float64{1, 0, 0}, 3)
	t.Logf("SearchKNN: keys=%v dists=%v err=%v", keys, dists, err)

	// SearchRange — also exercises max() in selectNeighborsByKey
	rkeys, _, err2 := h.SearchRange([]float64{1, 0, 0}, 2.0)
	t.Logf("SearchRange: keys=%v err=%v", rkeys, err2)

	// cosineSimilarity path via separate index with dimension > 1
	h2 := NewHNSWIndex("b124_cos", "tbl", "col", 2)
	h2.Insert("a", []float64{1, 0})
	h2.Insert("b", []float64{0, 1})
	h2.Insert("c", []float64{0.7, 0.7})
	h2.SearchKNN([]float64{1, 0}, 2)

	// innerProduct path (cosine similarity uses normalized dot product)
	h3 := NewHNSWIndex("b124_ip", "tbl", "col", 4)
	h3.Insert("x", []float64{1, 2, 3, 4})
	h3.Insert("y", []float64{4, 3, 2, 1})
	h3.SearchKNN([]float64{2, 2, 2, 2}, 2)

	// removeString path — covered internally by Update
	t.Logf("Vector update and max coverage done")
}

// ─── countRows via direct catalog API ────────────────────────────────────────

func TestB124_CountRowsPath(t *testing.T) {
	c := newCat124()
	ctx := context.Background()
	c.CreateTable(&query.CreateTableStmt{
		Table: "b124_cntrows",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "v", Type: query.TokenText},
		},
	})
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table: "b124_cntrows", Columns: []string{"id", "v"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "x"},
			}},
		}, nil)
	}

	sc := NewStatsCollector(c)
	count, err := sc.countRows("b124_cntrows")
	if err != nil {
		t.Fatalf("countRows: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5, got %d", count)
	}

	// float64 path: CollectStats returns row count as float64 in some paths
	stats, err := sc.CollectStats("b124_cntrows")
	if err != nil {
		t.Logf("CollectStats: %v", err)
	} else {
		t.Logf("CollectStats row count: %d", stats.RowCount)
	}
}

// ─── JSONQuote ────────────────────────────────────────────────────────────────

func TestB124_JSONQuote(t *testing.T) {
	c := newCat124()
	exec124(c, "CREATE TABLE b124_jq (id INTEGER PRIMARY KEY, v TEXT)")
	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table: "b124_jq", Columns: []string{"id", "v"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: `hello "world"`}},
			{&query.NumberLiteral{Value: 2}, &query.NullLiteral{}},
		},
	}, nil)

	// JSON_QUOTE via SQL
	rows := query124(c, `SELECT JSON_QUOTE(v) FROM b124_jq WHERE id = 1`)
	t.Logf("JSON_QUOTE string: %v", rows)

	rows = query124(c, `SELECT JSON_QUOTE(v) FROM b124_jq WHERE id = 2`)
	t.Logf("JSON_QUOTE NULL: %v", rows)

	// Direct call through JSONQuote function
	t.Logf("JSONQuote string: %v", JSONQuote("test string"))
	t.Logf("JSONQuote empty: %v", JSONQuote(""))
	t.Logf("JSONQuote with quotes: %v", JSONQuote(`say "hello"`))
}

// ─── selectLocked edge cases ─────────────────────────────────────────────────

func TestB124_SelectLockedEdgeCases(t *testing.T) {
	c := newCat124()
	exec124(c, "CREATE TABLE b124_sel (id INTEGER PRIMARY KEY, v INTEGER, s TEXT)")
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table: "b124_sel", Columns: []string{"id", "v", "s"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.NumberLiteral{Value: float64(i * 10)},
				&query.StringLiteral{Value: []string{"a", "b", "c", "a", "b"}[i-1]},
			}},
		}, nil)
	}

	// IS NULL / IS NOT NULL
	rows := query124(c, `SELECT id FROM b124_sel WHERE v IS NOT NULL`)
	t.Logf("IS NOT NULL: %v", rows)

	// LIKE patterns
	rows = query124(c, `SELECT id FROM b124_sel WHERE s LIKE 'a%'`)
	t.Logf("LIKE 'a%%': %v", rows)
	rows = query124(c, `SELECT id FROM b124_sel WHERE s LIKE '%b'`)
	t.Logf("LIKE '%%b': %v", rows)
	rows = query124(c, `SELECT id FROM b124_sel WHERE s NOT LIKE 'c%'`)
	t.Logf("NOT LIKE: %v", rows)

	// IN with multiple values
	rows = query124(c, `SELECT id FROM b124_sel WHERE s IN ('a', 'c')`)
	t.Logf("IN: %v", rows)

	// NOT IN
	rows = query124(c, `SELECT id FROM b124_sel WHERE s NOT IN ('a')`)
	t.Logf("NOT IN: %v", rows)

	// Derived table (subquery in FROM)
	rows = query124(c, `SELECT sub.id FROM (SELECT id, v FROM b124_sel WHERE v > 20) AS sub ORDER BY sub.id`)
	t.Logf("Derived table: %v", rows)

	// DISTINCT
	rows = query124(c, `SELECT DISTINCT s FROM b124_sel ORDER BY s`)
	t.Logf("DISTINCT: %v", rows)

	// LIMIT + OFFSET
	rows = query124(c, `SELECT id FROM b124_sel ORDER BY id LIMIT 2 OFFSET 1`)
	t.Logf("LIMIT OFFSET: %v", rows)

	// Scalar subquery
	rows = query124(c, `SELECT id FROM b124_sel WHERE v = (SELECT MAX(v) FROM b124_sel)`)
	t.Logf("Scalar subquery: %v", rows)

	// EXISTS subquery
	rows = query124(c, `SELECT id FROM b124_sel WHERE EXISTS (SELECT 1 FROM b124_sel WHERE s = 'c')`)
	t.Logf("EXISTS: %v", rows)
}

// ─── applyOuterQuery branches ─────────────────────────────────────────────────

func TestB124_ApplyOuterQueryBranches(t *testing.T) {
	c := newCat124()
	exec124(c, "CREATE TABLE b124_outer (id INTEGER PRIMARY KEY, dept TEXT, sal INTEGER)")
	ctx := context.Background()
	data := []struct{ dept string; sal int }{
		{"eng", 100}, {"eng", 200}, {"hr", 150}, {"hr", 80}, {"mg", 300},
	}
	for i, d := range data {
		c.Insert(ctx, &query.InsertStmt{
			Table: "b124_outer", Columns: []string{"id", "dept", "sal"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i + 1)},
				&query.StringLiteral{Value: d.dept},
				&query.NumberLiteral{Value: float64(d.sal)},
			}},
		}, nil)
	}

	// Derived table + outer WHERE
	rows := query124(c, `SELECT dept, total FROM (SELECT dept, SUM(sal) AS total FROM b124_outer GROUP BY dept) AS sub WHERE total > 200`)
	t.Logf("Outer WHERE on derived: %v", rows)

	// Derived table + outer ORDER BY
	rows = query124(c, `SELECT dept, total FROM (SELECT dept, SUM(sal) AS total FROM b124_outer GROUP BY dept) AS sub ORDER BY total DESC`)
	t.Logf("Outer ORDER BY on derived: %v", rows)

	// Derived table + outer LIMIT
	rows = query124(c, `SELECT dept FROM (SELECT dept, SUM(sal) AS total FROM b124_outer GROUP BY dept ORDER BY total DESC) AS sub LIMIT 2`)
	t.Logf("Outer LIMIT on derived: %v", rows)

	// Derived table + outer aggregate
	rows = query124(c, `SELECT COUNT(*) FROM (SELECT dept FROM b124_outer WHERE sal > 100) AS sub`)
	t.Logf("Outer COUNT on derived: %v", rows)
}

// ─── resolveAggregateInExpr branches ─────────────────────────────────────────

func TestB124_ResolveAggregateInExprBranches(t *testing.T) {
	c := newCat124()
	exec124(c, "CREATE TABLE b124_ragg (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)")
	ctx := context.Background()
	for i := 1; i <= 6; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table: "b124_ragg", Columns: []string{"id", "g", "v"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: []string{"x", "y", "x", "y", "x", "y"}[i-1]},
				&query.NumberLiteral{Value: float64(i * 5)},
			}},
		}, nil)
	}

	// HAVING with complex expression (exercises resolveAggregateInExpr for BinaryExpr)
	rows := query124(c, `SELECT g, SUM(v) FROM b124_ragg GROUP BY g HAVING SUM(v) > 20 AND COUNT(*) >= 2`)
	t.Logf("HAVING AND: %v", rows)

	// HAVING with OR
	rows = query124(c, `SELECT g, COUNT(*) FROM b124_ragg GROUP BY g HAVING COUNT(*) > 2 OR SUM(v) > 50`)
	t.Logf("HAVING OR: %v", rows)

	// ORDER BY expression aggregate (SUM * 2)
	rows = query124(c, `SELECT g, SUM(v) FROM b124_ragg GROUP BY g ORDER BY SUM(v) DESC`)
	t.Logf("ORDER BY SUM: %v", rows)

	// SELECT aggregate expression (AVG + COUNT)
	rows = query124(c, `SELECT g, AVG(v), COUNT(*), MIN(v), MAX(v) FROM b124_ragg GROUP BY g`)
	t.Logf("Multi aggregate: %v", rows)
}

// ─── ExecuteQuery DDL coverage ───────────────────────────────────────────────

func TestB124_ExecuteQueryDDL(t *testing.T) {
	c := newCat124()

	// CREATE TABLE
	r, err := c.ExecuteQuery("CREATE TABLE b124_ddl (id INTEGER PRIMARY KEY, v TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_ = r

	// INSERT
	r, err = c.ExecuteQuery("INSERT INTO b124_ddl VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	_ = r

	// SELECT
	r, err = c.ExecuteQuery("SELECT * FROM b124_ddl")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(r.Rows))
	}

	// UPDATE
	r, err = c.ExecuteQuery("UPDATE b124_ddl SET v = 'world' WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	_ = r

	// DELETE
	r, err = c.ExecuteQuery("DELETE FROM b124_ddl WHERE id = 1")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	_ = r

	// CREATE INDEX
	c.ExecuteQuery("CREATE TABLE b124_idxtbl (id INTEGER PRIMARY KEY, v TEXT)")
	r, err = c.ExecuteQuery("CREATE INDEX b124_idx ON b124_idxtbl (v)")
	if err != nil {
		t.Logf("CREATE INDEX: %v", err)
	}
	_ = r

	// DROP TABLE
	r, err = c.ExecuteQuery("DROP TABLE b124_ddl")
	if err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}
	_ = r

	// Unsupported type
	_, err = c.ExecuteQuery("SHOW TABLES")
	if err == nil {
		t.Log("SHOW TABLES: no error (supported)")
	} else {
		t.Logf("SHOW TABLES error (expected for unsupported): %v", err)
	}

	// Parse error
	_, err = c.ExecuteQuery("NOT VALID SQL @@@@")
	if err == nil {
		t.Error("expected parse error")
	}
}
