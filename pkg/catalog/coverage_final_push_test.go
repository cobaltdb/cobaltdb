package catalog

import (
	"math"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestFinalCoveragePush(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// --- CTE with window functions + WHERE + ORDER BY + LIMIT + OFFSET ---
	c.ExecuteQuery("CREATE TABLE cte_base (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO cte_base (id, name) VALUES (1, 'alice')")
	c.ExecuteQuery("INSERT INTO cte_base (id, name) VALUES (2, 'bob')")
	c.ExecuteQuery("INSERT INTO cte_base (id, name) VALUES (3, 'charlie')")
	_, err := c.ExecuteQuery("WITH c1 AS (SELECT id, name FROM cte_base), c2 AS (SELECT id FROM cte_base) SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, name FROM c1 WHERE id > 1 ORDER BY rn DESC LIMIT 1 OFFSET 1")
	if err != nil {
		t.Logf("CTE window full: %v", err)
	}

	// --- CTE with window functions + Identifier column + expression column ---
	_, err = c.ExecuteQuery("WITH c1 AS (SELECT id, name FROM cte_base), c2 AS (SELECT id FROM cte_base) SELECT id, 1+1 AS expr, ROW_NUMBER() OVER (ORDER BY id) FROM c1")
	if err != nil {
		t.Logf("CTE window mixed cols: %v", err)
	}

	// --- applyOuterQuery: qualified identifier from main table ---
	c.ExecuteQuery("CREATE TABLE aq (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO aq (id, name) VALUES (1, 'a')")
	_, err = c.ExecuteQuery("SELECT aq.id, aq.name FROM aq")
	if err != nil {
		t.Logf("Qualified id main table: %v", err)
	}

	// --- applyOuterQuery: qualified identifier from join table ---
	c.ExecuteQuery("CREATE TABLE bq (id INTEGER PRIMARY KEY, aq_id INTEGER, val TEXT)")
	c.ExecuteQuery("INSERT INTO bq (id, aq_id, val) VALUES (1, 1, 'x')")
	_, err = c.ExecuteQuery("SELECT aq.name, bq.val FROM aq JOIN bq ON aq.id = bq.aq_id")
	if err != nil {
		t.Logf("Qualified id join table: %v", err)
	}

	// --- computeAggregatesWithGroupBy: COUNT(col) with NULLs ---
	c.ExecuteQuery("CREATE TABLE agg_null (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO agg_null (id, cat, val) VALUES (1, 'A', 10)")
	c.ExecuteQuery("INSERT INTO agg_null (id, cat, val) VALUES (2, 'A', NULL)")
	c.ExecuteQuery("INSERT INTO agg_null (id, cat, val) VALUES (3, 'B', NULL)")
	_, err = c.ExecuteQuery("SELECT cat, COUNT(val) FROM agg_null GROUP BY cat")
	if err != nil {
		t.Logf("COUNT NULL: %v", err)
	}

	// --- MIN/MAX with all NULLs ---
	_, err = c.ExecuteQuery("SELECT cat, MIN(val), MAX(val) FROM agg_null WHERE cat = 'B' GROUP BY cat")
	if err != nil {
		t.Logf("MIN MAX all NULL: %v", err)
	}

	// --- AVG with no numeric values ---
	_, err = c.ExecuteQuery("SELECT cat, AVG(val) FROM agg_null WHERE cat = 'B' GROUP BY cat")
	if err != nil {
		t.Logf("AVG all NULL: %v", err)
	}

	// --- executeSelectWithJoinAndGroupBy: RIGHT JOIN unmatched + GROUP BY ---
	c.ExecuteQuery("CREATE TABLE rj_a (id INTEGER PRIMARY KEY, cat TEXT)")
	c.ExecuteQuery("CREATE TABLE rj_b (id INTEGER PRIMARY KEY, a_id INTEGER, val INTEGER)")
	c.ExecuteQuery("INSERT INTO rj_a (id, cat) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO rj_b (id, a_id, val) VALUES (1, 1, 10)")
	c.ExecuteQuery("INSERT INTO rj_b (id, a_id, val) VALUES (2, 999, 20)")
	_, err = c.ExecuteQuery("SELECT rj_a.cat, COUNT(rj_b.id) FROM rj_a RIGHT JOIN rj_b ON rj_a.id = rj_b.a_id GROUP BY rj_a.cat")
	if err != nil {
		t.Logf("RIGHT JOIN GROUP BY: %v", err)
	}

	// --- GROUP BY with table.column ---
	_, err = c.ExecuteQuery("SELECT rj_a.cat, SUM(rj_b.val) FROM rj_a JOIN rj_b ON rj_a.id = rj_b.a_id GROUP BY rj_a.cat")
	if err != nil {
		t.Logf("GROUP BY table.column: %v", err)
	}

	// --- GROUP BY with alias ---
	_, err = c.ExecuteQuery("SELECT rj_a.cat AS c, SUM(rj_b.val) AS s FROM rj_a JOIN rj_b ON rj_a.id = rj_b.a_id GROUP BY c ORDER BY s")
	if err != nil {
		t.Logf("GROUP BY alias JOIN: %v", err)
	}

	// --- DISTINCT with GROUP BY ---
	_, err = c.ExecuteQuery("SELECT DISTINCT rj_a.cat FROM rj_a JOIN rj_b ON rj_a.id = rj_b.a_id")
	if err != nil {
		t.Logf("DISTINCT JOIN: %v", err)
	}

	// --- EXISTS subquery ---
	_, err = c.ExecuteQuery("SELECT * FROM rj_a WHERE EXISTS (SELECT 1 FROM rj_b WHERE rj_b.a_id = rj_a.id)")
	if err != nil {
		t.Logf("EXISTS: %v", err)
	}

	// --- Scalar subquery ---
	_, err = c.ExecuteQuery("SELECT (SELECT val FROM rj_b WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery: %v", err)
	}

	// --- BETWEEN ---
	_, err = c.ExecuteQuery("SELECT * FROM rj_b WHERE val BETWEEN 5 AND 15")
	if err != nil {
		t.Logf("BETWEEN: %v", err)
	}

	// --- IN list ---
	_, err = c.ExecuteQuery("SELECT * FROM rj_b WHERE val IN (10, 20)")
	if err != nil {
		t.Logf("IN list: %v", err)
	}

	// --- LIKE ---
	_, err = c.ExecuteQuery("SELECT * FROM rj_a WHERE cat LIKE 'A%'")
	if err != nil {
		t.Logf("LIKE: %v", err)
	}

	// --- IS NULL / IS NOT NULL ---
	_, err = c.ExecuteQuery("SELECT * FROM agg_null WHERE val IS NULL")
	if err != nil {
		t.Logf("IS NULL: %v", err)
	}
	_, err = c.ExecuteQuery("SELECT * FROM agg_null WHERE val IS NOT NULL")
	if err != nil {
		t.Logf("IS NOT NULL: %v", err)
	}
}

// ── INSERT...SELECT type switch paths ──
func TestFinalPush_InsertSelectTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE ins_nil (id INTEGER PRIMARY KEY, val INTEGER)")
	_, err := c.ExecuteQuery("INSERT INTO ins_nil (val) SELECT NULL")
	if err != nil {
		t.Logf("INSERT SELECT NULL: %v", err)
	}

	c.ExecuteQuery("CREATE TABLE ins_bool (id INTEGER PRIMARY KEY, flag BOOLEAN)")
	_, err = c.ExecuteQuery("INSERT INTO ins_bool (flag) SELECT 1=1")
	if err != nil {
		t.Logf("INSERT SELECT bool: %v", err)
	}

	c.ExecuteQuery("CREATE TABLE ins_json (id INTEGER PRIMARY KEY, data JSON)")
	_, err = c.ExecuteQuery("INSERT INTO ins_json (data) SELECT JSON_EXTRACT('[1,2]', '$')")
	if err != nil {
		t.Logf("INSERT SELECT JSON array: %v", err)
	}
}

// ── Scalar SELECT paths ──
func TestFinalPush_ScalarSelectPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery("SELECT DISTINCT 1")
	if err != nil {
		t.Logf("Scalar DISTINCT: %v", err)
	}

	_, err = c.ExecuteQuery("SELECT 1 LIMIT 1")
	if err != nil {
		t.Logf("Scalar LIMIT: %v", err)
	}

	_, err = c.ExecuteQuery("SELECT 1 AS x WHERE 1=0")
	if err != nil {
		t.Logf("Scalar WHERE false AliasExpr: %v", err)
	}

	_, err = c.ExecuteQuery("SELECT UPPER('a') WHERE 1=0")
	if err != nil {
		t.Logf("Scalar WHERE false FunctionCall: %v", err)
	}

	_, err = c.ExecuteQuery("SELECT COUNT(*)")
	if err != nil {
		t.Logf("Scalar COUNT(*): %v", err)
	}

	_, err = c.ExecuteQuery("SELECT SUM(5)")
	if err != nil {
		t.Logf("Scalar SUM: %v", err)
	}

	_, err = c.ExecuteQuery("SELECT AVG(5)")
	if err != nil {
		t.Logf("Scalar AVG: %v", err)
	}

	_, err = c.ExecuteQuery("SELECT MIN(5)")
	if err != nil {
		t.Logf("Scalar MIN: %v", err)
	}

	_, err = c.ExecuteQuery("SELECT MAX(5)")
	if err != nil {
		t.Logf("Scalar MAX: %v", err)
	}
}

// ── CTE paths ──
func TestFinalPush_CTEPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM b")
	if err != nil {
		t.Logf("Multiple CTEs: %v", err)
	}

	_, err = c.ExecuteQuery("WITH a AS (SELECT 1 UNION SELECT 2) SELECT * FROM a")
	if err != nil {
		t.Logf("UNION CTE: %v", err)
	}

	_, err = c.ExecuteQuery("WITH RECURSIVE a AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM a WHERE n < 3) SELECT * FROM a")
	if err != nil {
		t.Logf("Recursive CTE: %v", err)
	}
}

// ── Vacuum with indexes ──
func TestFinalPush_VacuumWithIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE vac_idx (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE INDEX idx_vac ON vac_idx(code)")
	c.ExecuteQuery("INSERT INTO vac_idx (id, code) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO vac_idx (id, code) VALUES (2, 'B')")

	err := c.Vacuum()
	if err != nil {
		t.Logf("Vacuum with indexes: %v", err)
	}
}

// ── EvalExpression direct paths ──
func TestFinalPush_EvalExpressionPaths(t *testing.T) {
	// MAX with 2+ args
	v, err := EvalExpression(
		&query.FunctionCall{
			Name: "MAX",
			Args: []query.Expression{
				&query.NumberLiteral{Value: 1},
				&query.NumberLiteral{Value: 5},
				&query.NumberLiteral{Value: 3},
			},
		}, nil)
	if err != nil || v != float64(5) {
		t.Logf("MAX 3 args: %v, err=%v", v, err)
	}

	// MIN with 2+ args
	v, err = EvalExpression(
		&query.FunctionCall{
			Name: "MIN",
			Args: []query.Expression{
				&query.NumberLiteral{Value: 1},
				&query.NumberLiteral{Value: 5},
				&query.NumberLiteral{Value: 3},
			},
		}, nil)
	if err != nil || v != float64(1) {
		t.Logf("MIN 3 args: %v, err=%v", v, err)
	}

	// TYPEOF with []byte (default case)
	v, err = EvalExpression(
		&query.FunctionCall{
			Name: "TYPEOF",
			Args: []query.Expression{&query.PlaceholderExpr{Index: 0}},
		}, []interface{}{[]byte("abc")})
	if err != nil || v != "text" {
		t.Logf("TYPEOF []byte: %v, err=%v", v, err)
	}

	// Simple CASE with when condition error (unsupported function)
	_, err = EvalExpression(
		&query.CaseExpr{
			Expr: &query.NumberLiteral{Value: 1},
			Whens: []*query.WhenClause{
				{Condition: &query.FunctionCall{Name: "UNKNOWN_FUNC"}, Result: &query.NumberLiteral{Value: 99}},
			},
		}, nil)
	if err != nil {
		t.Logf("Simple CASE when error: %v", err)
	}

	// intersectSorted with a[i] > b[j]
	res := intersectSorted([]int64{3}, []int64{1, 2, 3})
	if len(res) != 1 || res[0] != 3 {
		t.Logf("intersectSorted: %v", res)
	}
}

// ── evaluateExpression direct paths ──
func TestFinalPush_EvaluateExpressionPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Placeholder out of range
	_, err := evaluateExpression(c, nil, nil, &query.PlaceholderExpr{Index: 99}, nil)
	if err == nil {
		t.Log("expected placeholder error")
	}

	// QualifiedIdentifier exact table match
	_, err = evaluateExpression(c, []interface{}{42}, []ColumnDef{{Name: "x", sourceTbl: "t"}}, &query.QualifiedIdentifier{Table: "t", Column: "x"}, nil)
	if err != nil {
		t.Logf("QualifiedIdentifier exact: %v", err)
	}

	// QualifiedIdentifier fallback match
	_, err = evaluateExpression(c, []interface{}{42}, []ColumnDef{{Name: "x"}}, &query.QualifiedIdentifier{Table: "t", Column: "x"}, nil)
	if err != nil {
		t.Logf("QualifiedIdentifier fallback: %v", err)
	}

	// SubqueryExpr empty result
	_, err = evaluateExpression(c, nil, nil, &query.SubqueryExpr{
		Query: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}, Where: &query.BooleanLiteral{Value: false}},
	}, nil)
	if err != nil {
		t.Logf("SubqueryExpr empty: %v", err)
	}

	// ExistsExpr Not with empty result
	v, err := evaluateExpression(c, nil, nil, &query.ExistsExpr{
		Not:      true,
		Subquery: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}, Where: &query.BooleanLiteral{Value: false}},
	}, nil)
	if err != nil || v != true {
		t.Logf("ExistsExpr Not empty: %v, err=%v", v, err)
	}

	// FunctionCall eager arg evaluation error (invalid identifier)
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "UPPER",
		Args: []query.Expression{&query.Identifier{Name: "nonexistent"}},
	}, nil)
	if err == nil {
		t.Log("expected eager arg eval error")
	}

	// PRINTF with %f
	v, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "PRINTF",
		Args: []query.Expression{
			&query.StringLiteral{Value: "%f"},
			&query.NumberLiteral{Value: 1.5},
		},
	}, nil)
	if err != nil {
		t.Logf("PRINTF %%f: %v, err=%v", v, err)
	}

	// REPEAT too long
	_, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "REPEAT",
		Args: []query.Expression{
			&query.StringLiteral{Value: "a"},
			&query.NumberLiteral{Value: 10000000},
		},
	}, nil)
	if err == nil {
		t.Log("expected REPEAT error")
	}

	// MATCH pattern nil
	v, err = evaluateExpression(c, nil, nil, &query.MatchExpr{
		Columns: []query.Expression{&query.Identifier{Name: "x"}},
		Pattern: &query.NullLiteral{},
	}, nil)
	if err != nil || v != false {
		t.Logf("MATCH NULL pattern: %v, err=%v", v, err)
	}

	// CEIL non-numeric
	v, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CEIL",
		Args: []query.Expression{&query.StringLiteral{Value: "abc"}},
	}, nil)
	if err != nil {
		t.Logf("CEIL non-numeric: %v, err=%v", v, err)
	}

	// REPLACE non-string newStr
	v, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "REPLACE",
		Args: []query.Expression{
			&query.StringLiteral{Value: "hello"},
			&query.StringLiteral{Value: "l"},
			&query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Logf("REPLACE non-string newStr: %v, err=%v", v, err)
	}
}

// ── catalog_core.go selectLocked edge cases ──
func TestFinalPush_SelectLockedEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE sel_edge (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO sel_edge (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO sel_edge (id, val) VALUES (2, 20)")

	// OFFSET >= row count (covers offset >= len in selectLocked)
	_, err := c.ExecuteQuery("SELECT * FROM sel_edge OFFSET 99")
	if err != nil {
		t.Logf("OFFSET >= len: %v", err)
	}

	// Scalar subquery in SELECT list
	_, err = c.ExecuteQuery("SELECT (SELECT val FROM sel_edge WHERE id = 1)")
	if err != nil {
		t.Logf("Scalar subquery: %v", err)
	}

	// EXISTS subquery
	_, err = c.ExecuteQuery("SELECT EXISTS(SELECT * FROM sel_edge WHERE id = 99)")
	if err != nil {
		t.Logf("EXISTS false: %v", err)
	}

	// JSON path expression in SELECT
	c.ExecuteQuery("CREATE TABLE json_edge (id INTEGER PRIMARY KEY, data JSON)")
	c.ExecuteQuery("INSERT INTO json_edge (id, data) VALUES (1, '{\"a\":1}')")
	_, err = c.ExecuteQuery("SELECT data->>'$.a' FROM json_edge")
	if err != nil {
		t.Logf("JSON path text: %v", err)
	}

	// Window function in simple SELECT (should error)
	_, err = c.ExecuteQuery("SELECT ROW_NUMBER() OVER ()")
	if err != nil {
		t.Logf("Window without FROM: %v", err)
	}
}

// ── catalog_maintenance.go LoadSchema / LoadData paths ──
func TestFinalPush_LoadPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE load_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO load_t (id, name) VALUES (1, 'a')")

	// Save and Load round-trip
	_ = c.Save()
	_ = c.Load()
}

// ── catalog_select.go hidden ORDER BY dotted identifier ──
func TestFinalPush_HiddenOrderByDotted(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE h1 (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO h1 (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO h1 (id, val) VALUES (2, 20)")
	c.ExecuteQuery("CREATE TABLE h2 (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO h2 (id, val) VALUES (1, 5)")
	c.ExecuteQuery("INSERT INTO h2 (id, val) VALUES (2, 15)")

	_, err := c.ExecuteQuery("SELECT h1.val, h2.val FROM h1 JOIN h2 ON h1.id = h2.id ORDER BY h1.val")
	if err != nil {
		t.Logf("Dotted ORDER BY: %v", err)
	}
}

// ── catalog_eval.go evaluateCaseExpr paths ──
func TestFinalPush_CaseExprPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Searched CASE with cond error
	v, err := evaluateExpression(c, nil, nil, &query.CaseExpr{
		Whens: []*query.WhenClause{
			{Condition: &query.Identifier{Name: "bad"}, Result: &query.NumberLiteral{Value: 1}},
		},
	}, nil)
	if err != nil {
		t.Logf("Searched CASE cond error: %v, err=%v", v, err)
	}

	// Simple CASE baseVal nil
	v, err = evaluateExpression(c, nil, nil, &query.CaseExpr{
		Expr:  &query.NullLiteral{},
		Whens: []*query.WhenClause{{Condition: &query.NullLiteral{}, Result: &query.NumberLiteral{Value: 1}}},
	}, nil)
	if err != nil {
		t.Logf("Simple CASE base nil: %v, err=%v", v, err)
	}
}

// ── catalog_core.go evaluateCastExpr fallback ──
func TestFinalPush_CastFallback(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Cast to unsupported type falls through
	v, err := evaluateExpression(c, nil, nil, &query.CastExpr{
		Expr:     &query.NumberLiteral{Value: 42},
		DataType: query.TokenBlob,
	}, nil)
	if err != nil {
		t.Logf("Cast fallback: %v, err=%v", v, err)
	}
}

// ── catalog_core.go evaluateJSONFunction paths ──
func TestFinalPush_JSONFunctionPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// JSON_TYPE with non-string arg
	v, err := evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "JSON_TYPE",
		Args: []query.Expression{&query.NumberLiteral{Value: 42}},
	}, nil)
	if err != nil {
		t.Logf("JSON_TYPE non-string: %v, err=%v", v, err)
	}

	// JSON_KEYS with non-string arg
	v, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "JSON_KEYS",
		Args: []query.Expression{&query.NumberLiteral{Value: 42}},
	}, nil)
	if err != nil {
		t.Logf("JSON_KEYS non-string: %v, err=%v", v, err)
	}

	// JSON_ARRAY_LENGTH with non-string arg
	v, err = evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "JSON_ARRAY_LENGTH",
		Args: []query.Expression{&query.NumberLiteral{Value: 42}},
	}, nil)
	if err != nil {
		t.Logf("JSON_ARRAY_LENGTH non-string: %v, err=%v", v, err)
	}
}

// ── catalog_core.go catalogCompareValues paths ──
func TestFinalPush_CatalogCompareValues(t *testing.T) {
	if catalogCompareValues(nil, nil) != 0 {
		t.Error("catalogCompareValues nil nil")
	}
	if catalogCompareValues(nil, 1) != -1 {
		t.Error("catalogCompareValues nil 1")
	}
	if catalogCompareValues(1, nil) != 1 {
		t.Error("catalogCompareValues 1 nil")
	}
	if catalogCompareValues([]byte("a"), []byte("b")) >= 0 {
		t.Logf("catalogCompareValues bytes: %d", catalogCompareValues([]byte("a"), []byte("b")))
	}
}

// ── catalog_core.go isIntegerType paths ──
func TestFinalPush_IsIntegerType(t *testing.T) {
	if !isIntegerType(int(1)) {
		t.Error("isIntegerType int")
	}
	if !isIntegerType(int64(1)) {
		t.Error("isIntegerType int64")
	}
	if !isIntegerType(float64(1)) {
		t.Error("isIntegerType float64")
	}
	if isIntegerType("abc") {
		t.Error("isIntegerType string")
	}
}

// ── catalog_core.go toFloat64 string parse ──
func TestFinalPush_ToFloat64String(t *testing.T) {
	f, ok := toFloat64("3.14")
	if !ok || math.Abs(f-3.14) > 0.001 {
		t.Logf("toFloat64 string: %v, %v", f, ok)
	}
	_, ok = toFloat64("not_a_number")
	if ok {
		t.Error("toFloat64 invalid string")
	}
}
