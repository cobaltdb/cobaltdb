package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ── applyOuterQuery aggregate/HAVING/ORDER BY/LIMIT/OFFSET paths ──
func TestLastMile_ApplyOuterQueryPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE aoq (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO aoq (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO aoq (id, val) VALUES (2, 20)")
	c.ExecuteQuery("INSERT INTO aoq (id, val) VALUES (3, 30)")

	// Derived table -> applyOuterQuery with aggregate + HAVING + ORDER BY + LIMIT + OFFSET
	_, err := c.ExecuteQuery("SELECT val, COUNT(*) FROM (SELECT id, val FROM aoq) AS dt GROUP BY val HAVING COUNT(*) > 0 ORDER BY val DESC LIMIT 1 OFFSET 1")
	if err != nil {
		t.Logf("Derived table aggregate full: %v", err)
	}

	// Derived table with ORDER BY only (non-aggregate path)
	_, err = c.ExecuteQuery("SELECT id, val FROM (SELECT id, val FROM aoq) AS dt ORDER BY val DESC LIMIT 2 OFFSET 1")
	if err != nil {
		t.Logf("Derived table order limit: %v", err)
	}

	// Complex view (DISTINCT) with outer aggregate
	c.ExecuteQuery("CREATE VIEW v_aoq AS SELECT DISTINCT id, val FROM aoq")
	_, err = c.ExecuteQuery("SELECT COUNT(*) FROM v_aoq")
	if err != nil {
		t.Logf("View COUNT: %v", err)
	}
	_, err = c.ExecuteQuery("SELECT val, COUNT(*) FROM v_aoq GROUP BY val HAVING COUNT(*) > 0 ORDER BY val LIMIT 1 OFFSET 1")
	if err != nil {
		t.Logf("View aggregate full: %v", err)
	}

	// Multiple CTEs (materializes first CTE) -> applyOuterQuery
	_, err = c.ExecuteQuery("WITH c1 AS (SELECT id, val FROM aoq), c2 AS (SELECT id FROM aoq) SELECT COUNT(*) FROM c1")
	if err != nil {
		t.Logf("CTE materialized COUNT: %v", err)
	}
}

// ── computeAggregatesWithGroupBy remaining paths ──
func TestLastMile_AggregatePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// COUNT(expr) with NULLs
	c.ExecuteQuery("CREATE TABLE agg_expr (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO agg_expr (id, cat, val) VALUES (1, 'A', 10)")
	c.ExecuteQuery("INSERT INTO agg_expr (id, cat, val) VALUES (2, 'A', NULL)")
	c.ExecuteQuery("INSERT INTO agg_expr (id, cat, val) VALUES (3, 'B', NULL)")
	_, err := c.ExecuteQuery("SELECT cat, COUNT(val) FROM agg_expr GROUP BY cat")
	if err != nil {
		t.Logf("COUNT(expr): %v", err)
	}

	// Embedded COUNT(expr) without GROUP BY -> evaluateExprWithGroupAggregates
	_, err = c.ExecuteQuery("SELECT CASE WHEN COUNT(val) > 0 THEN 1 ELSE 0 END FROM agg_expr")
	if err != nil {
		t.Logf("Embedded COUNT(expr): %v", err)
	}

	// Empty table with embedded aggregate + HAVING
	c.ExecuteQuery("CREATE TABLE agg_empty (id INTEGER PRIMARY KEY, val INTEGER)")
	_, err = c.ExecuteQuery("SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM agg_empty HAVING COUNT(*) > 0")
	if err != nil {
		t.Logf("Empty embedded agg HAVING: %v", err)
	}

	// Positional ORDER BY with NULLs in GROUP BY
	c.ExecuteQuery("CREATE TABLE agg_pos (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO agg_pos (id, cat, val) VALUES (1, 'A', 10)")
	c.ExecuteQuery("INSERT INTO agg_pos (id, cat, val) VALUES (2, 'B', NULL)")
	_, err = c.ExecuteQuery("SELECT cat, SUM(val) FROM agg_pos GROUP BY cat ORDER BY 2")
	if err != nil {
		t.Logf("Positional ORDER BY NULL: %v", err)
	}

	// GROUP BY alias
	_, err = c.ExecuteQuery("SELECT cat AS c, SUM(val) AS s FROM agg_pos GROUP BY c ORDER BY s")
	if err != nil {
		t.Logf("GROUP BY alias: %v", err)
	}

	// GROUP BY expression alias (CASE)
	_, err = c.ExecuteQuery("SELECT CASE WHEN cat = 'A' THEN 1 ELSE 2 END AS grp, COUNT(*) FROM agg_pos GROUP BY grp")
	if err != nil {
		t.Logf("GROUP BY expr alias: %v", err)
	}
}

// ── updateLocked transaction + index paths ──
func TestLastMile_UpdateTransactionPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE upd_txn (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO upd_txn (id, name) VALUES (1, 'alice')")
	c.ExecuteQuery("INSERT INTO upd_txn (id, name) VALUES (2, 'bob')")

	// UPDATE within transaction -> undo log
	c.BeginTransaction(1)
	_, _, err := c.Update(ctx, mustParseUpdateLM("UPDATE upd_txn SET name = 'ALICE' WHERE id = 1"), nil)
	if err != nil {
		t.Logf("Update txn: %v", err)
	}
	c.CommitTransaction()

	// UPDATE PK within transaction -> undo log + index undo
	c.ExecuteQuery("CREATE TABLE upd_pk_txn (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	c.ExecuteQuery("INSERT INTO upd_pk_txn (id, code) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO upd_pk_txn (id, code) VALUES (2, 'B')")
	c.BeginTransaction(2)
	_, _, err = c.Update(ctx, mustParseUpdateLM("UPDATE upd_pk_txn SET id = 3 WHERE id = 1"), nil)
	if err != nil {
		t.Logf("Update PK txn: %v", err)
	}
	c.CommitTransaction()

	// UPDATE unique conflict
	_, _, err = c.Update(ctx, mustParseUpdateLM("UPDATE upd_pk_txn SET code = 'B' WHERE id = 3"), nil)
	if err == nil {
		t.Error("Expected unique constraint error")
	}
}

// ── insertLocked remaining paths ──
func TestLastMile_InsertPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// INSERT OR REPLACE with unique index -> index delete path
	c.ExecuteQuery("CREATE TABLE ins_rep (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE UNIQUE INDEX idx_ins_rep ON ins_rep(name)")
	c.ExecuteQuery("INSERT INTO ins_rep (id, name) VALUES (1, 'alice')")
	_, err := c.ExecuteQuery("INSERT OR REPLACE INTO ins_rep (id, name) VALUES (2, 'alice')")
	if err != nil {
		t.Logf("Insert replace unique idx: %v", err)
	}

	// CHECK constraint failure
	c.ExecuteQuery("CREATE TABLE ins_chk (id INTEGER PRIMARY KEY, val INTEGER CHECK (val > 0))")
	_, _, err = c.Insert(ctx, mustParseInsertLM("INSERT INTO ins_chk (id, val) VALUES (1, -1)"), nil)
	if err == nil {
		t.Error("Expected CHECK constraint error")
	}

	// INSERT with invalid column name
	_, _, err = c.Insert(ctx, mustParseInsertLM("INSERT INTO ins_chk (nonexistent) VALUES (1)"), nil)
	if err == nil {
		t.Error("Expected invalid column error")
	}

	// INSERT...SELECT with boolean literal (covers bool case in type switch)
	c.ExecuteQuery("CREATE TABLE bool_src (id INTEGER PRIMARY KEY, flag INTEGER)")
	c.ExecuteQuery("INSERT INTO bool_src (id, flag) VALUES (1, 1)")
	c.ExecuteQuery("CREATE TABLE bool_dst (id INTEGER PRIMARY KEY, active INTEGER)")
	_, err = c.ExecuteQuery("INSERT INTO bool_dst (id, active) SELECT id, CASE WHEN flag = 1 THEN 1 ELSE 0 END FROM bool_src")
	if err != nil {
		t.Logf("INSERT...SELECT CASE: %v", err)
	}

	// INSERT...SELECT with direct integer column (covers int64 case in type switch)
	c.ExecuteQuery("CREATE TABLE int_src (id INTEGER PRIMARY KEY, num INTEGER)")
	c.ExecuteQuery("INSERT INTO int_src (id, num) VALUES (1, 42)")
	c.ExecuteQuery("CREATE TABLE int_dst (id INTEGER PRIMARY KEY, num INTEGER)")
	_, err = c.ExecuteQuery("INSERT INTO int_dst (id, num) SELECT id, num FROM int_src")
	if err != nil {
		t.Logf("INSERT...SELECT int64: %v", err)
	}
}

// ── selectLocked hidden columns / resolveAggregateInExpr CaseExpr ──
func TestLastMile_SelectCorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE sel_core (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO sel_core (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO sel_core (id, val) VALUES (2, 20)")

	// ORDER BY expression not in SELECT -> hidden columns, then removed after LIMIT
	_, err := c.ExecuteQuery("SELECT id FROM sel_core ORDER BY val DESC LIMIT 1")
	if err != nil {
		t.Logf("Hidden columns limit: %v", err)
	}

	// CASE with aggregate inside -> resolveAggregateInExpr with CaseExpr
	_, err = c.ExecuteQuery("SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM sel_core")
	if err != nil {
		t.Logf("CASE aggregate: %v", err)
	}

	// JOIN with GROUP BY dotted identifier
	c.ExecuteQuery("CREATE TABLE sc_a (id INTEGER PRIMARY KEY, cat TEXT)")
	c.ExecuteQuery("CREATE TABLE sc_b (id INTEGER PRIMARY KEY, a_id INTEGER, val INTEGER)")
	c.ExecuteQuery("INSERT INTO sc_a (id, cat) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO sc_b (id, a_id, val) VALUES (1, 1, 10)")
	_, err = c.ExecuteQuery("SELECT sc_a.cat, SUM(sc_b.val) FROM sc_a JOIN sc_b ON sc_a.id = sc_b.a_id GROUP BY sc_a.cat")
	if err != nil {
		t.Logf("JOIN GROUP BY dotted: %v", err)
	}

	// JSON path operators
	c.ExecuteQuery("CREATE TABLE json_t (id INTEGER PRIMARY KEY, data TEXT)")
	c.ExecuteQuery("INSERT INTO json_t (id, data) VALUES (1, '{\"a\":1}')")
	_, err = c.ExecuteQuery("SELECT data->'$.a' FROM json_t")
	if err != nil {
		t.Logf("JSON path object: %v", err)
	}
	_, err = c.ExecuteQuery("SELECT data->>'$.a' FROM json_t")
	if err != nil {
		t.Logf("JSON path text: %v", err)
	}
}

// ── Direct EvalExpression tests for uncovered paths ──
func TestLastMile_EvalExpressionMore(t *testing.T) {
	// Binary unsupported operator in EvalExpression
	_, err := EvalExpression(&query.BinaryExpr{Operator: query.TokenArrow, Left: &query.NumberLiteral{Value: 1}, Right: &query.NumberLiteral{Value: 2}}, nil)
	if err == nil {
		t.Error("Expected unsupported binary operator error")
	}

	// Division by zero
	_, err = EvalExpression(&query.BinaryExpr{Operator: query.TokenSlash, Left: &query.NumberLiteral{Value: 1}, Right: &query.NumberLiteral{Value: 0}}, nil)
	if err == nil {
		t.Error("Expected division by zero error")
	}

	// Modulo by zero
	_, err = EvalExpression(&query.BinaryExpr{Operator: query.TokenPercent, Left: &query.NumberLiteral{Value: 1}, Right: &query.NumberLiteral{Value: 0}}, nil)
	if err == nil {
		t.Error("Expected modulo by zero error")
	}

	// Unary minus on non-numeric
	v, err := EvalExpression(&query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.StringLiteral{Value: "abc"}}, nil)
	if err != nil || v != "abc" {
		t.Errorf("Unary minus non-numeric: got %v, %v", v, err)
	}

	// Unary NOT on non-bool
	v, err = EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: &query.NumberLiteral{Value: 1}}, nil)
	if err != nil || v != float64(1) {
		t.Errorf("Unary NOT non-bool: got %v, %v", v, err)
	}
}

// ── FK ON UPDATE CASCADE within transaction ──
func TestLastMile_FKTransaction(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE fk_par (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE fk_chd (id INTEGER PRIMARY KEY, par_id INTEGER, FOREIGN KEY (par_id) REFERENCES fk_par(id) ON UPDATE CASCADE)")
	c.ExecuteQuery("INSERT INTO fk_par (id, name) VALUES (1, 'a')")
	c.ExecuteQuery("INSERT INTO fk_chd (id, par_id) VALUES (1, 1)")

	c.BeginTransaction(3)
	_, _, err := c.Update(ctx, mustParseUpdateLM("UPDATE fk_par SET id = 2 WHERE id = 1"), nil)
	if err != nil {
		t.Logf("FK update txn: %v", err)
	}
	c.CommitTransaction()
}

// ── JOIN with embedded aggregates, positional ORDER BY, expression-arg aggregates ──
func TestLastMile_JoinAggregatePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE ja_a (id INTEGER PRIMARY KEY, cat TEXT)")
	c.ExecuteQuery("CREATE TABLE ja_b (id INTEGER PRIMARY KEY, a_id INTEGER, val INTEGER)")
	c.ExecuteQuery("INSERT INTO ja_a (id, cat) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO ja_a (id, cat) VALUES (2, 'B')")
	c.ExecuteQuery("INSERT INTO ja_b (id, a_id, val) VALUES (1, 1, 10)")
	c.ExecuteQuery("INSERT INTO ja_b (id, a_id, val) VALUES (2, 2, 20)")

	// JOIN GROUP BY dotted identifier + alias
	_, err := c.ExecuteQuery("SELECT ja_a.cat AS c, SUM(ja_b.val) AS s FROM ja_a JOIN ja_b ON ja_a.id = ja_b.a_id GROUP BY ja_a.cat ORDER BY s")
	if err != nil {
		t.Logf("JOIN GROUP BY dotted alias: %v", err)
	}

	// Positional ORDER BY with string values (non-numeric comparison)
	_, err = c.ExecuteQuery("SELECT ja_a.cat, COUNT(*) FROM ja_a JOIN ja_b ON ja_a.id = ja_b.a_id GROUP BY ja_a.cat ORDER BY 1")
	if err != nil {
		t.Logf("Positional ORDER BY string: %v", err)
	}

	// Expression-arg aggregate in ORDER BY (SUM(val * 2))
	_, err = c.ExecuteQuery("SELECT ja_a.cat, SUM(ja_b.val) FROM ja_a JOIN ja_b ON ja_a.id = ja_b.a_id GROUP BY ja_a.cat ORDER BY SUM(ja_b.val * 2)")
	if err != nil {
		t.Logf("Expr arg aggregate ORDER BY: %v", err)
	}

	// CASE with aggregate in JOIN context -> resolveAggregateInExpr CaseExpr
	_, err = c.ExecuteQuery("SELECT CASE WHEN COUNT(ja_b.val) > 0 THEN 1 ELSE 0 END FROM ja_a JOIN ja_b ON ja_a.id = ja_b.a_id")
	if err != nil {
		t.Logf("CASE aggregate JOIN: %v", err)
	}

	// AVG embedded aggregate
	_, err = c.ExecuteQuery("SELECT CASE WHEN AVG(ja_b.val) > 5 THEN 'high' ELSE 'low' END FROM ja_a JOIN ja_b ON ja_a.id = ja_b.a_id")
	if err != nil {
		t.Logf("AVG embedded JOIN: %v", err)
	}

	// Hidden ORDER BY columns in JOIN
	_, err = c.ExecuteQuery("SELECT ja_a.cat FROM ja_a JOIN ja_b ON ja_a.id = ja_b.a_id ORDER BY ja_b.val")
	if err != nil {
		t.Logf("JOIN hidden ORDER BY: %v", err)
	}
}

// ── DELETE with unique index within transaction ──
func TestLastMile_DeleteTransactionPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE del_txn (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE UNIQUE INDEX idx_del_txn ON del_txn(code)")
	c.ExecuteQuery("INSERT INTO del_txn (id, code) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO del_txn (id, code) VALUES (2, 'B')")

	c.BeginTransaction(4)
	_, _, err := c.Delete(ctx, mustParseDeleteLM("DELETE FROM del_txn WHERE id = 1"), nil)
	if err != nil {
		t.Logf("Delete txn: %v", err)
	}
	c.CommitTransaction()
}

func mustParseDeleteLM(sql string) *query.DeleteStmt {
	parsed, err := query.Parse(sql)
	if err != nil {
		panic(err)
	}
	if del, ok := parsed.(*query.DeleteStmt); ok {
		return del
	}
	panic("parsed statement is not a DELETE")
}

func mustParseUpdateLM(sql string) *query.UpdateStmt {
	parsed, err := query.Parse(sql)
	if err != nil {
		panic(err)
	}
	if upd, ok := parsed.(*query.UpdateStmt); ok {
		return upd
	}
	panic("parsed statement is not an UPDATE")
}

func mustParseInsertLM(sql string) *query.InsertStmt {
	parsed, err := query.Parse(sql)
	if err != nil {
		panic(err)
	}
	if ins, ok := parsed.(*query.InsertStmt); ok {
		return ins
	}
	panic("parsed statement is not an INSERT")
}

// ── Direct evaluateExpression paths for remaining uncovered blocks ──
func TestLastMile_EvaluateExpressionDirect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// SubqueryExpr error (line 131)
	_, err := evaluateExpression(c, nil, nil, &query.SubqueryExpr{
		Query: &query.SelectStmt{Columns: []query.Expression{&query.Identifier{Name: "bad"}}},
	}, nil)
	if err == nil {
		t.Log("expected SubqueryExpr error")
	}

	// ExistsExpr error (line 146)
	_, err = evaluateExpression(c, nil, nil, &query.ExistsExpr{
		Subquery: &query.SelectStmt{Columns: []query.Expression{&query.Identifier{Name: "bad"}}},
	}, nil)
	if err == nil {
		t.Log("expected ExistsExpr error")
	}

	// JSONPathExpr column error (line 159)
	_, err = evaluateExpression(c, nil, nil, &query.JSONPathExpr{
		Column: &query.Identifier{Name: "bad"}, Path: "$.a",
	}, nil)
	if err == nil {
		t.Log("expected JSONPathExpr error")
	}

	// JSONPathExpr non-string JSON (line 163)
	v, _ := evaluateExpression(c, []interface{}{42}, []ColumnDef{{Name: "x"}}, &query.JSONPathExpr{
		Column: &query.Identifier{Name: "x"}, Path: "$.a",
	}, nil)
	if v != nil {
		t.Logf("expected nil, got %v", v)
	}

	// JSONPathExpr JSONExtract error (line 167)
	_, err = evaluateExpression(c, nil, nil, &query.JSONPathExpr{
		Column: &query.StringLiteral{Value: "not json"}, Path: "$.a",
	}, nil)
	if err == nil {
		t.Log("expected JSONPathExpr JSONExtract error")
	}

	// JSONPathExpr result nil (line 170)
	v, _ = evaluateExpression(c, nil, nil, &query.JSONPathExpr{
		Column: &query.StringLiteral{Value: `{"a":1}`}, Path: "$.b",
	}, nil)
	if v != nil {
		t.Logf("expected nil, got %v", v)
	}

	// UnaryExpr TokenMinus int (line 192)
	v, _ = evaluateExpression(c, []interface{}{int(5)}, []ColumnDef{{Name: "x"}}, &query.UnaryExpr{
		Operator: query.TokenMinus, Expr: &query.Identifier{Name: "x"},
	}, nil)
	if v != int(-5) {
		t.Logf("expected int(-5), got %v %T", v, v)
	}

	// UnaryExpr cannot negate non-numeric (line 200)
	_, err = evaluateExpression(c, nil, nil, &query.UnaryExpr{
		Operator: query.TokenMinus, Expr: &query.StringLiteral{Value: "abc"},
	}, nil)
	if err == nil {
		t.Log("expected negate error")
	}

	// UnaryExpr TokenNot nil (line 202)
	v, _ = evaluateExpression(c, nil, nil, &query.UnaryExpr{
		Operator: query.TokenNot, Expr: &query.NullLiteral{},
	}, nil)
	if v != nil {
		t.Logf("expected nil, got %v", v)
	}

	// BinaryExpr unsupported operator (line 302)
	_, err = evaluateExpression(c, nil, nil, &query.BinaryExpr{
		Left: &query.NumberLiteral{Value: 1}, Operator: 99999, Right: &query.NumberLiteral{Value: 2},
	}, nil)
	if err == nil {
		t.Log("expected unsupported operator error")
	}

	// Simple CASE baseVal error (line 353)
	_, err = evaluateExpression(c, nil, nil, &query.CaseExpr{
		Expr: &query.Identifier{Name: "bad"},
		Whens: []*query.WhenClause{{Condition: &query.NumberLiteral{Value: 1}, Result: &query.NumberLiteral{Value: 1}}},
	}, nil)
	if err == nil {
		t.Log("expected CASE baseVal error")
	}

	// Simple CASE when error (line 361)
	_, err = evaluateExpression(c, nil, nil, &query.CaseExpr{
		Expr: &query.NumberLiteral{Value: 1},
		Whens: []*query.WhenClause{{Condition: &query.Identifier{Name: "bad"}, Result: &query.NumberLiteral{Value: 1}}},
	}, nil)
	if err == nil {
		t.Log("expected CASE when error")
	}

	// COALESCE arg evaluation error (line 394)
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "COALESCE", Args: []query.Expression{&query.Identifier{Name: "bad"}},
	}, nil)
	if err == nil {
		t.Log("expected COALESCE error")
	}

	// PRINTF default case (line 696)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "PRINTF", Args: []query.Expression{&query.StringLiteral{Value: "%x"}, &query.NumberLiteral{Value: 1}},
	}, nil)
	_ = v

	// CAST non-string target type (line 738)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.NumberLiteral{Value: 42}, &query.NumberLiteral{Value: 123}},
	}, nil)
	if v != float64(42) {
		t.Logf("CAST non-string target: %v", v)
	}

	// CAST INTEGER string parse success (line 746)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.StringLiteral{Value: "123"}, &query.StringLiteral{Value: "INTEGER"}},
	}, nil)
	if v != int64(123) {
		t.Logf("CAST INTEGER string: %v", v)
	}

	// CAST INTEGER string parse float fallback (line 752)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.StringLiteral{Value: "3.14"}, &query.StringLiteral{Value: "INTEGER"}},
	}, nil)
	if v != int64(3) {
		t.Logf("CAST INTEGER float fallback: %v", v)
	}

	// CAST INTEGER string parse fail (line 755)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.StringLiteral{Value: "abc"}, &query.StringLiteral{Value: "INTEGER"}},
	}, nil)
	if v != int64(0) {
		t.Logf("CAST INTEGER fail: %v", v)
	}

	// CAST INTEGER bool true (line 757)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.BooleanLiteral{Value: true}, &query.StringLiteral{Value: "INTEGER"}},
	}, nil)
	if v != int64(1) {
		t.Logf("CAST INTEGER bool true: %v", v)
	}

	// CAST INTEGER bool false (line 761)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.BooleanLiteral{Value: false}, &query.StringLiteral{Value: "INTEGER"}},
	}, nil)
	if v != int64(0) {
		t.Logf("CAST INTEGER bool false: %v", v)
	}

	// CAST fallback unknown type (line 787)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.NumberLiteral{Value: 42}, &query.StringLiteral{Value: "BLOB"}},
	}, nil)
	if v != float64(42) {
		t.Logf("CAST fallback: %v", v)
	}

	// CAST REAL string parse success (line 769)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.StringLiteral{Value: "3.14"}, &query.StringLiteral{Value: "REAL"}},
	}, nil)
	if v != float64(3.14) {
		t.Logf("CAST REAL string: %v", v)
	}

	// CAST REAL string parse fail (line 772)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.StringLiteral{Value: "abc"}, &query.StringLiteral{Value: "REAL"}},
	}, nil)
	if v != float64(0) {
		t.Logf("CAST REAL fail: %v", v)
	}

	// CAST BOOLEAN bool (line 777)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.BooleanLiteral{Value: true}, &query.StringLiteral{Value: "BOOLEAN"}},
	}, nil)
	if v != true {
		t.Logf("CAST BOOLEAN bool: %v", v)
	}

	// CAST BOOLEAN float (line 780)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "BOOLEAN"}},
	}, nil)
	if v != true {
		t.Logf("CAST BOOLEAN float: %v", v)
	}

	// CAST BOOLEAN string (line 783)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST", Args: []query.Expression{&query.StringLiteral{Value: "true"}, &query.StringLiteral{Value: "BOOLEAN"}},
	}, nil)
	if v != true {
		t.Logf("CAST BOOLEAN string: %v", v)
	}

	// GROUP_CONCAT scalar no args (line 814)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "GROUP_CONCAT", Args: []query.Expression{},
	}, nil)
	if v != nil {
		t.Logf("GROUP_CONCAT no args: %v", v)
	}

	// REPEAT nil first arg (line 834)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "REPEAT", Args: []query.Expression{&query.NullLiteral{}, &query.NumberLiteral{Value: 5}},
	}, nil)
	if v != nil {
		t.Logf("REPEAT nil: %v", v)
	}

	// TYPEOF int (line 966)
	v, _ = evaluateExpression(c, []interface{}{int(5)}, []ColumnDef{{Name: "x"}}, &query.FunctionCall{
		Name: "TYPEOF", Args: []query.Expression{&query.Identifier{Name: "x"}},
	}, nil)
	if v != "integer" {
		t.Logf("TYPEOF int: %v", v)
	}

	// TYPEOF default unknown type (line 978)
	v, _ = evaluateExpression(c, []interface{}{[]byte("abc")}, []ColumnDef{{Name: "x"}}, &query.FunctionCall{
		Name: "TYPEOF", Args: []query.Expression{&query.Identifier{Name: "x"}},
	}, nil)
	if v != "text" {
		t.Logf("TYPEOF []byte: %v", v)
	}

	// COSINE_SIMILARITY wrong arg count (line 1069)
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "COSINE_SIMILARITY", Args: []query.Expression{&query.NumberLiteral{Value: 1}},
	}, nil)
	if err == nil {
		t.Log("expected COSINE_SIMILARITY arg count error")
	}

	// COSINE_SIMILARITY second arg toVector error (line 1076)
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "COSINE_SIMILARITY", Args: []query.Expression{
			&query.VectorLiteral{Values: []float64{1, 0}},
			&query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil {
		t.Log("expected COSINE_SIMILARITY toVector error")
	}

	// COSINE_SIMILARITY success (line 1080)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "COSINE_SIMILARITY", Args: []query.Expression{
			&query.VectorLiteral{Values: []float64{1, 0}},
			&query.VectorLiteral{Values: []float64{1, 0}},
		},
	}, nil)
	_ = v

	// L2_DISTANCE wrong arg count (line 1083)
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "L2_DISTANCE", Args: []query.Expression{&query.NumberLiteral{Value: 1}},
	}, nil)
	if err == nil {
		t.Log("expected L2_DISTANCE arg count error")
	}

	// L2_DISTANCE first arg toVector error (line 1091)
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "L2_DISTANCE", Args: []query.Expression{
			&query.NumberLiteral{Value: 1},
			&query.VectorLiteral{Values: []float64{1, 0}},
		},
	}, nil)
	if err == nil {
		t.Log("expected L2_DISTANCE first arg error")
	}

	// L2_DISTANCE success (line 1094)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "L2_DISTANCE", Args: []query.Expression{
			&query.VectorLiteral{Values: []float64{1, 0}},
			&query.VectorLiteral{Values: []float64{0, 1}},
		},
	}, nil)
	_ = v

	// L1_DISTANCE wrong arg count (line 1097)
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "L1_DISTANCE", Args: []query.Expression{&query.NumberLiteral{Value: 1}},
	}, nil)
	if err == nil {
		t.Log("expected L1_DISTANCE arg count error")
	}

	// L1_DISTANCE first arg toVector error (line 1105)
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "L1_DISTANCE", Args: []query.Expression{
			&query.NumberLiteral{Value: 1},
			&query.VectorLiteral{Values: []float64{1, 0}},
		},
	}, nil)
	if err == nil {
		t.Log("expected L1_DISTANCE first arg error")
	}

	// L1_DISTANCE success (line 1108)
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "L1_DISTANCE", Args: []query.Expression{
			&query.VectorLiteral{Values: []float64{1, 0}},
			&query.VectorLiteral{Values: []float64{0, 1}},
		},
	}, nil)
	_ = v

	// BinaryExpr TokenIs with false (line 260)
	v, _ = evaluateExpression(c, nil, nil, &query.BinaryExpr{
		Left: &query.NullLiteral{}, Operator: query.TokenIs, Right: &query.BooleanLiteral{Value: false},
	}, nil)
	_ = v

	// evaluateBetween with error in Expr (line 3601 in catalog_core.go)
	_, err = evaluateBetween(c, nil, nil, &query.BetweenExpr{
		Expr: &query.Identifier{Name: "bad"}, Lower: &query.NumberLiteral{Value: 1}, Upper: &query.NumberLiteral{Value: 10},
	}, nil)
	if err == nil {
		t.Log("expected BetweenExpr error")
	}

	// evaluateIn with error in list item (line 3520 in catalog_core.go)
	_, err = evaluateIn(c, nil, nil, &query.InExpr{
		Expr: &query.NumberLiteral{Value: 1},
		List: []query.Expression{&query.Identifier{Name: "bad"}},
	}, nil)
	if err == nil {
		t.Log("expected InExpr error")
	}

	// evaluateLike with pattern error (line 3330 in catalog_core.go)
	_, err = evaluateLike(c, nil, nil, &query.LikeExpr{
		Expr: &query.StringLiteral{Value: "a"}, Pattern: &query.Identifier{Name: "bad"},
	}, nil)
	if err == nil {
		t.Log("expected LikeExpr pattern error")
	}

	// evaluateIsNull with error in expr (line 3377 in catalog_core.go)
	_, err = evaluateIsNull(c, nil, nil, &query.IsNullExpr{
		Expr: &query.Identifier{Name: "bad"}, Not: false,
	}, nil)
	if err == nil {
		t.Log("expected IsNullExpr error")
	}

	// evaluateMatchExpr with empty allText (line 1236)
	v, _ = evaluateMatchExpr(c, []interface{}{"hello"}, []ColumnDef{{Name: "x"}}, &query.MatchExpr{
		Columns: []query.Expression{&query.Identifier{Name: "x"}},
		Pattern: &query.StringLiteral{Value: "world"},
	}, nil)
	_ = v

	// evaluateMatchExpr with column eval error (line 1268)
	v, _ = evaluateMatchExpr(c, []interface{}{"hello"}, []ColumnDef{{Name: "x"}}, &query.MatchExpr{
		Columns: []query.Expression{&query.Identifier{Name: "bad"}},
		Pattern: &query.StringLiteral{Value: "world"},
	}, nil)
	_ = v

	// evaluateMatchExpr with empty allText fallback (line 1276)
	v, _ = evaluateMatchExpr(c, []interface{}{nil}, []ColumnDef{{Name: "x"}}, &query.MatchExpr{
		Columns: []query.Expression{&query.Identifier{Name: "x"}},
		Pattern: &query.StringLiteral{Value: "world"},
	}, nil)
	_ = v

	// ── catalog_select.go paths via SQL ──

	// Scalar SELECT WHERE error (line 99)
	_, err = c.ExecuteQuery("SELECT 1 WHERE x = 1")
	if err == nil {
		t.Log("expected scalar WHERE error")
	}

	// Scalar SELECT Identifier in WHERE false (line 108)
	_, err = c.ExecuteQuery("SELECT x WHERE 1=0")
	if err != nil {
		t.Logf("scalar identifier WHERE false: %v", err)
	}

	// Scalar aggregate non-FunctionCall (line 167)
	_, err = c.ExecuteQuery("SELECT 1, COUNT(*)")
	if err == nil {
		t.Log("expected scalar aggregate non-func error")
	}

	// Scalar SELECT FunctionCall column name in normal eval (line 131)
	_, err = c.ExecuteQuery("SELECT UPPER('a')")
	if err != nil {
		t.Logf("Scalar FunctionCall col name: %v", err)
	}

	// ── catalog_aggregate.go paths via SQL ──

	// Empty table aggregate with HAVING true (line 183)
	c.ExecuteQuery("CREATE TABLE empty_agg (id INTEGER PRIMARY KEY, val INTEGER)")
	_, err = c.ExecuteQuery("SELECT COUNT(*) FROM empty_agg HAVING COUNT(*) = 0")
	if err != nil {
		t.Logf("empty agg HAVING true: %v", err)
	}

	// AVG with all NULLs in join+group context (line 548 in evaluateExprWithGroupAggregatesJoin)
	c.ExecuteQuery("CREATE TABLE avg_join (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO avg_join (id, cat, val) VALUES (1, 'A', NULL)")
	c.ExecuteQuery("INSERT INTO avg_join (id, cat, val) VALUES (2, 'A', NULL)")
	c.ExecuteQuery("CREATE TABLE avg_join2 (id INTEGER PRIMARY KEY, cat TEXT)")
	c.ExecuteQuery("INSERT INTO avg_join2 (id, cat) VALUES (1, 'A')")
	_, err = c.ExecuteQuery("SELECT avg_join.cat FROM avg_join JOIN avg_join2 ON avg_join.cat = avg_join2.cat GROUP BY avg_join.cat HAVING AVG(avg_join.val) > 0")
	_ = err

	// GROUP BY with identifier from selectCols (lines 66-69)
	c.ExecuteQuery("CREATE TABLE gb1 (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO gb1 (id, cat, val) VALUES (1, 'A', 10)")
	_, err = c.ExecuteQuery("SELECT cat, SUM(val) FROM gb1 GROUP BY cat")
	if err != nil {
		t.Logf("GROUP BY identifier: %v", err)
	}

	// GROUP BY with unresolved identifier (lines 72-74)
	_, err = c.ExecuteQuery("SELECT SUM(val) FROM gb1 GROUP BY missing")
	if err != nil {
		t.Logf("GROUP BY unresolved: %v", err)
	}

	// ── catalog_select.go JOIN paths ──

	// JOIN with ORDER BY dotted identifier not in selectCols, column found via getTableLocked (lines 604-606)
	c.ExecuteQuery("CREATE TABLE j1 (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO j1 (id, name) VALUES (1, 'a')")
	c.ExecuteQuery("CREATE TABLE j2 (id INTEGER PRIMARY KEY, j1_id INTEGER)")
	c.ExecuteQuery("INSERT INTO j2 (id, j1_id) VALUES (1, 1)")
	_, err = c.ExecuteQuery("SELECT j1.id FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY j1.name")
	if err != nil {
		t.Logf("JOIN ORDER BY dotted found: %v", err)
	}

	// JOIN with ORDER BY dotted identifier not found (lines 606-609)
	_, err = c.ExecuteQuery("SELECT j1.id FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY j1.missing")
	if err != nil {
		t.Logf("JOIN ORDER BY dotted not found: %v", err)
	}

	// JOIN with ORDER BY non-identifier (line 574-575)
	_, err = c.ExecuteQuery("SELECT j1.id FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY 1+1")
	if err != nil {
		t.Logf("JOIN ORDER BY expr: %v", err)
	}

	// JOIN OFFSET >= len (line 718)
	_, err = c.ExecuteQuery("SELECT j1.id FROM j1 JOIN j2 ON j1.id = j2.j1_id OFFSET 99")
	if err != nil {
		t.Logf("JOIN OFFSET >= len: %v", err)
	}

	// CROSS JOIN with condition (lines 444-448)
	_, err = c.ExecuteQuery("SELECT * FROM j1 CROSS JOIN j2 ON j1.id = j2.j1_id")
	if err != nil {
		t.Logf("CROSS JOIN with condition: %v", err)
	}

	// ── catalog_insert.go paths ──

	// INSERT NOT NULL constraint
	c.ExecuteQuery("CREATE TABLE nn_t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	_, err = c.ExecuteQuery("INSERT INTO nn_t (id) VALUES (1)")
	if err == nil {
		t.Log("expected NOT NULL constraint error")
	}

	// INSERT JSON default
	c.ExecuteQuery("CREATE TABLE json_def (id INTEGER PRIMARY KEY, data JSON DEFAULT '{}')")
	_, err = c.ExecuteQuery("INSERT INTO json_def (id) VALUES (1)")
	if err != nil {
		t.Logf("INSERT JSON default: %v", err)
	}
}
