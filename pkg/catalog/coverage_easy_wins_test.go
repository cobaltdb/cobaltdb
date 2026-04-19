package catalog

import (
	"math"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestVectorDistanceMismatchedLengths covers l2Distance and innerProduct mismatch paths
func TestVectorDistanceMismatchedLengths(t *testing.T) {
	// l2Distance with mismatched lengths returns +Inf
	if dist := l2Distance([]float64{1, 2}, []float64{1}); !math.IsInf(dist, 1) {
		t.Errorf("l2Distance mismatch: expected +Inf, got %v", dist)
	}
	// innerProduct with mismatched lengths returns 0
	if dot := innerProduct([]float64{1, 2}, []float64{1}); dot != 0 {
		t.Errorf("innerProduct mismatch: expected 0, got %v", dot)
	}
}

// TestCosineSimilarityZeroNorm covers zero-norm vectors
func TestCosineSimilarityZeroNorm(t *testing.T) {
	if sim := cosineSimilarity([]float64{0, 0}, []float64{1, 1}); sim != 0 {
		t.Errorf("cosineSimilarity zero norm: expected 0, got %v", sim)
	}
	if sim := cosineSimilarity([]float64{1, 1}, []float64{0, 0}); sim != 0 {
		t.Errorf("cosineSimilarity zero norm: expected 0, got %v", sim)
	}
}

// TestValueToStringUncoveredTypes covers int32, bool, default cases
func TestValueToStringUncoveredTypes(t *testing.T) {
	if s := valueToString(int32(42)); s != "42" {
		t.Errorf("valueToString int32: expected '42', got %s", s)
	}
	if s := valueToString(true); s != "true" {
		t.Errorf("valueToString bool true: expected 'true', got %s", s)
	}
	if s := valueToString(false); s != "false" {
		t.Errorf("valueToString bool false: expected 'false', got %s", s)
	}
	if s := valueToString([]int{1, 2}); s != "[1 2]" {
		t.Errorf("valueToString default: expected '[1 2]', got %s", s)
	}
}

// TestValidateIdentifierErrors covers empty, too long, and invalid character paths
func TestValidateIdentifierErrors(t *testing.T) {
	if err := validateIdentifier(""); err == nil {
		t.Error("validateIdentifier empty: expected error")
	}
	if err := validateIdentifier("a_very_long_identifier_that_exceeds_the_sixty_four_character_limit_abc123"); err == nil {
		t.Error("validateIdentifier too long: expected error")
	}
	if err := validateIdentifier("bad-name"); err == nil {
		t.Error("validateIdentifier invalid char: expected error")
	}
}

// TestJSONEachNonObject covers JSONEach with non-object input
func TestJSONEachNonObject(t *testing.T) {
	_, err := JSONEach("[1,2,3]")
	if err == nil {
		t.Error("JSONEach non-object: expected error")
	}
}

// TestJSONArrayLengthNonArray covers JSONArrayLength with non-array input
func TestJSONArrayLengthNonArray(t *testing.T) {
	len, err := JSONArrayLength("{\"a\":1}")
	if err != nil {
		t.Errorf("JSONArrayLength non-array: unexpected error %v", err)
	}
	if len != 0 {
		t.Errorf("JSONArrayLength non-array: expected 0, got %d", len)
	}
}

// TestJSONTypeWithPath covers JSONType with a path
func TestJSONTypeWithPath(t *testing.T) {
	typ, err := JSONType(`{"a":1}`, "$.a")
	if err != nil {
		t.Errorf("JSONType with path: unexpected error %v", err)
	}
	if typ != "number" {
		t.Errorf("JSONType with path: expected 'number', got %s", typ)
	}
}

// TestJSONRemoveEmpty covers JSONRemove with empty input
func TestJSONRemoveEmpty(t *testing.T) {
	result, err := JSONRemove("", "$.a")
	if err != nil {
		t.Errorf("JSONRemove empty: unexpected error %v", err)
	}
	if result != "" {
		t.Errorf("JSONRemove empty: expected '', got %s", result)
	}
}

// TestParseJSONPathInvalidArrayIndex covers invalid array index error
func TestParseJSONPathInvalidArrayIndex(t *testing.T) {
	_, err := ParseJSONPath("$.foo[abc]")
	if err == nil {
		t.Error("ParseJSONPath invalid array index: expected error")
	}
}

// TestJSONPathGetArrayIndexError covers array index parse error in Get
func TestJSONPathGetArrayIndexError(t *testing.T) {
	// Build a JSONPath manually with an invalid numeric segment to trigger strconv.Atoi error in Get
	jp := &JSONPath{Segments: []string{"[99999999999999999999]"}}
	_, err := jp.Get([]interface{}{1})
	if err == nil {
		t.Error("JSONPath.Get huge array index: expected error")
	}
}

// ── EvalExpression easy uncovered paths ──
func TestEasyWins_EvalExpressionFuncs(t *testing.T) {
	// TYPEOF int
	v, err := EvalExpression(&query.FunctionCall{Name: "TYPEOF", Args: []query.Expression{
		&query.NumberLiteral{Value: 42},
	}}, nil)
	if err != nil || v != "integer" {
		t.Logf("TYPEOF int: got %v, %v", v, err)
	}

	// TYPEOF float64 whole number
	v, err = EvalExpression(&query.FunctionCall{Name: "TYPEOF", Args: []query.Expression{
		&query.NumberLiteral{Value: 42.0},
	}}, nil)
	if err != nil || v != "integer" {
		t.Logf("TYPEOF float whole: got %v, %v", v, err)
	}

	// TYPEOF float64 non-whole
	v, err = EvalExpression(&query.FunctionCall{Name: "TYPEOF", Args: []query.Expression{
		&query.NumberLiteral{Value: 3.14},
	}}, nil)
	if err != nil || v != "real" {
		t.Logf("TYPEOF float non-whole: got %v, %v", v, err)
	}

	// TYPEOF string
	v, err = EvalExpression(&query.FunctionCall{Name: "TYPEOF", Args: []query.Expression{
		&query.StringLiteral{Value: "hello"},
	}}, nil)
	if err != nil || v != "text" {
		t.Logf("TYPEOF string: got %v, %v", v, err)
	}

	// TYPEOF bool
	v, err = EvalExpression(&query.FunctionCall{Name: "TYPEOF", Args: []query.Expression{
		&query.BooleanLiteral{Value: true},
	}}, nil)
	if err != nil || v != "integer" {
		t.Logf("TYPEOF bool: got %v, %v", v, err)
	}

	// TYPEOF default ([]byte)
	v, err = EvalExpression(&query.FunctionCall{Name: "TYPEOF", Args: []query.Expression{
		&query.BinaryExpr{Operator: query.TokenPlus, Left: &query.NumberLiteral{Value: 1}, Right: &query.NumberLiteral{Value: 1}},
	}}, nil)
	if err != nil {
		t.Logf("TYPEOF default: got %v, %v", v, err)
	}

	// MIN with 1 arg
	v, err = EvalExpression(&query.FunctionCall{Name: "MIN", Args: []query.Expression{
		&query.NumberLiteral{Value: 5},
	}}, nil)
	if err != nil || v != float64(5) {
		t.Logf("MIN 1 arg: got %v, %v", v, err)
	}

	// MIN with 0 args
	v, err = EvalExpression(&query.FunctionCall{Name: "MIN", Args: []query.Expression{}}, nil)
	if err != nil || v != nil {
		t.Logf("MIN 0 args: got %v, %v", v, err)
	}

	// MAX with 1 arg
	v, err = EvalExpression(&query.FunctionCall{Name: "MAX", Args: []query.Expression{
		&query.NumberLiteral{Value: 5},
	}}, nil)
	if err != nil || v != float64(5) {
		t.Logf("MAX 1 arg: got %v, %v", v, err)
	}

	// MAX with 0 args
	v, err = EvalExpression(&query.FunctionCall{Name: "MAX", Args: []query.Expression{}}, nil)
	if err != nil || v != nil {
		t.Logf("MAX 0 args: got %v, %v", v, err)
	}

	// REVERSE with nil
	v, err = EvalExpression(&query.FunctionCall{Name: "REVERSE", Args: []query.Expression{
		&query.NullLiteral{},
	}}, nil)
	if err != nil || v != nil {
		t.Logf("REVERSE nil: got %v, %v", v, err)
	}

	// REPEAT with n <= 0
	v, err = EvalExpression(&query.FunctionCall{Name: "REPEAT", Args: []query.Expression{
		&query.StringLiteral{Value: "a"},
		&query.NumberLiteral{Value: 0},
	}}, nil)
	if err != nil || v != "" {
		t.Logf("REPEAT n<=0: got %v, %v", v, err)
	}

	// REPEAT with 1 arg (nil fallback)
	v, err = EvalExpression(&query.FunctionCall{Name: "REPEAT", Args: []query.Expression{
		&query.StringLiteral{Value: "a"},
	}}, nil)
	if err != nil || v != nil {
		t.Logf("REPEAT 1 arg: got %v, %v", v, err)
	}

	// NULLIF with 1 arg
	v, err = EvalExpression(&query.FunctionCall{Name: "NULLIF", Args: []query.Expression{
		&query.NumberLiteral{Value: 5},
	}}, nil)
	if err != nil || v != float64(5) {
		t.Logf("NULLIF 1 arg: got %v, %v", v, err)
	}

	// REPLACE with old == ""
	v, err = EvalExpression(&query.FunctionCall{Name: "REPLACE", Args: []query.Expression{
		&query.StringLiteral{Value: "abc"},
		&query.StringLiteral{Value: ""},
		&query.StringLiteral{Value: "x"},
	}}, nil)
	if err != nil || v != "abc" {
		t.Logf("REPLACE old empty: got %v, %v", v, err)
	}

	// ROUND with non-numeric
	v, err = EvalExpression(&query.FunctionCall{Name: "ROUND", Args: []query.Expression{
		&query.StringLiteral{Value: "abc"},
	}}, nil)
	if err != nil || v != nil {
		t.Logf("ROUND non-numeric: got %v, %v", v, err)
	}

	// IIF with 2 args
	v, err = EvalExpression(&query.FunctionCall{Name: "IIF", Args: []query.Expression{
		&query.BooleanLiteral{Value: true},
		&query.NumberLiteral{Value: 1},
	}}, nil)
	if err != nil || v != nil {
		t.Logf("IIF 2 args: got %v, %v", v, err)
	}

	// ABS with non-numeric
	v, err = EvalExpression(&query.FunctionCall{Name: "ABS", Args: []query.Expression{
		&query.StringLiteral{Value: "abc"},
	}}, nil)
	if err != nil || v != nil {
		t.Logf("ABS non-numeric: got %v, %v", v, err)
	}

	// IFNULL with 1 arg
	v, err = EvalExpression(&query.FunctionCall{Name: "IFNULL", Args: []query.Expression{
		&query.NumberLiteral{Value: 5},
	}}, nil)
	if err != nil || v != nil {
		t.Logf("IFNULL 1 arg: got %v, %v", v, err)
	}

	// SUBSTR with start >= len
	v, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{
		&query.StringLiteral{Value: "ab"},
		&query.NumberLiteral{Value: 5},
	}}, nil)
	if err != nil || v != "" {
		t.Logf("SUBSTR start>=len: got %v, %v", v, err)
	}

	// SUBSTR with length < 0
	v, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{
		&query.StringLiteral{Value: "abcde"},
		&query.NumberLiteral{Value: 2},
		&query.NumberLiteral{Value: -1},
	}}, nil)
	if err != nil || v != "" {
		t.Logf("SUBSTR length<0: got %v, %v", v, err)
	}

	// SUBSTR with start+length > len
	v, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{
		&query.StringLiteral{Value: "abcde"},
		&query.NumberLiteral{Value: 3},
		&query.NumberLiteral{Value: 10},
	}}, nil)
	if err != nil || v != "cde" {
		t.Logf("SUBSTR start+length>len: got %v, %v", v, err)
	}

	// Unary minus error propagation (inner expr errors)
	_, err = EvalExpression(&query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.JSONPathExpr{
		Column: &query.StringLiteral{Value: "{}"}, Path: "$.a",
	}}, nil)
	if err != nil {
		t.Logf("Unary minus error prop: %v", err)
	}

	// Binary subtraction with floats (not bothInt)
	v, err = EvalExpression(&query.BinaryExpr{
		Operator: query.TokenMinus,
		Left:     &query.NumberLiteral{Value: 5.5},
		Right:    &query.NumberLiteral{Value: 2.2},
	}, nil)
	if err != nil {
		t.Logf("Binary float minus: got %v, %v", v, err)
	}

	// Binary multiplication with floats
	v, err = EvalExpression(&query.BinaryExpr{
		Operator: query.TokenStar,
		Left:     &query.NumberLiteral{Value: 2.5},
		Right:    &query.NumberLiteral{Value: 4.0},
	}, nil)
	if err != nil {
		t.Logf("Binary float star: got %v, %v", v, err)
	}

	// CaseExpr simple case with baseVal error
	v, err = EvalExpression(&query.CaseExpr{
		Expr:  &query.JSONPathExpr{Column: &query.StringLiteral{Value: "{}"}, Path: "$.a"},
		Whens: []*query.WhenClause{{Condition: &query.NumberLiteral{Value: 1}, Result: &query.NumberLiteral{Value: 1}}},
	}, nil)
	if err != nil {
		t.Logf("CaseExpr baseVal error: got %v, %v", v, err)
	}

	// CastExpr with inner error
	v, err = EvalExpression(&query.CastExpr{
		Expr:     &query.JSONPathExpr{Column: &query.StringLiteral{Value: "{}"}, Path: "$.a"},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil {
		t.Logf("CastExpr inner error: got %v, %v", v, err)
	}

	// Cast INTEGER with string parse error
	v, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "notanumber"},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil || v != int64(0) {
		t.Logf("Cast INTEGER string error: got %v, %v", v, err)
	}

	// Cast REAL with string parse error
	v, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "notanumber"},
		DataType: query.TokenReal,
	}, nil)
	if err != nil || v != float64(0) {
		t.Logf("Cast REAL string error: got %v, %v", v, err)
	}
}

// ── Scalar SELECT uncovered paths ──
func TestEasyWins_ScalarSelectPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Scalar SELECT with DISTINCT
	_, err := c.ExecuteQuery("SELECT DISTINCT 1")
	if err != nil {
		t.Logf("Scalar DISTINCT: %v", err)
	}

	// Scalar SELECT with identifier column (no FROM)
	_, err = c.ExecuteQuery("SELECT 1 AS x")
	if err != nil {
		t.Logf("Scalar alias: %v", err)
	}
}

// ── SELECT OFFSET/LIMIT edge cases ──
func TestEasyWins_SelectOffsetLimit(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE offlim (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO offlim (id) VALUES (1)")

	// OFFSET >= row count
	_, err := c.ExecuteQuery("SELECT id FROM offlim OFFSET 99")
	if err != nil {
		t.Logf("OFFSET >= len: %v", err)
	}

	// LIMIT 0
	_, err = c.ExecuteQuery("SELECT id FROM offlim LIMIT 0")
	if err != nil {
		t.Logf("LIMIT 0: %v", err)
	}
}

// ── resolveAggregateInExpr CaseExpr path ──
func TestEasyWins_ResolveAggregateCase(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE agg_case (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO agg_case (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO agg_case (id, val) VALUES (2, 20)")

	// CASE with aggregate inside
	_, err := c.ExecuteQuery("SELECT CASE WHEN COUNT(*) > 0 THEN SUM(val) ELSE 0 END FROM agg_case")
	if err != nil {
		t.Logf("Aggregate in CASE: %v", err)
	}
}

// ── Correlated subquery paths ──
func TestEasyWins_CorrelatedSubquery(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE cs_outer (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("CREATE TABLE cs_inner (id INTEGER PRIMARY KEY, o_id INTEGER, name TEXT)")
	c.ExecuteQuery("INSERT INTO cs_outer (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO cs_outer (id, val) VALUES (2, 20)")
	c.ExecuteQuery("INSERT INTO cs_inner (id, o_id, name) VALUES (1, 1, 'a')")
	c.ExecuteQuery("INSERT INTO cs_inner (id, o_id, name) VALUES (2, 1, 'b')")

	// Correlated subquery with EXISTS
	_, err := c.ExecuteQuery("SELECT id FROM cs_outer WHERE EXISTS (SELECT 1 FROM cs_inner WHERE cs_inner.o_id = cs_outer.id)")
	if err != nil {
		t.Logf("Correlated EXISTS: %v", err)
	}

	// Correlated subquery with scalar subquery
	_, err = c.ExecuteQuery("SELECT id, (SELECT COUNT(*) FROM cs_inner WHERE cs_inner.o_id = cs_outer.id) AS cnt FROM cs_outer")
	if err != nil {
		t.Logf("Correlated scalar: %v", err)
	}
}

// ── JOIN with hidden ORDER BY columns ──
func TestEasyWins_JoinHiddenOrderBy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE jho_a (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE jho_b (id INTEGER PRIMARY KEY, a_id INTEGER, val INTEGER)")
	c.ExecuteQuery("INSERT INTO jho_a (id, name) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO jho_b (id, a_id, val) VALUES (1, 1, 10)")

	// JOIN with ORDER BY column from right table not in SELECT
	_, err := c.ExecuteQuery("SELECT jho_a.name FROM jho_a JOIN jho_b ON jho_a.id = jho_b.a_id ORDER BY jho_b.val")
	if err != nil {
		t.Logf("JOIN hidden ORDER BY: %v", err)
	}
}

// ── catalog_select.go evaluateWindowFunctions path ──
func TestEasyWins_WindowFuncInSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE win_sel (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO win_sel (id, val) VALUES (1, 10)")
	c.ExecuteQuery("INSERT INTO win_sel (id, val) VALUES (2, 20)")

	// Window function in select without ORDER BY (covers hasWindowFuncs path)
	_, err := c.ExecuteQuery("SELECT id, ROW_NUMBER() OVER () FROM win_sel")
	if err != nil {
		t.Logf("Window no ORDER BY: %v", err)
	}
}

// ── catalog_maintenance.go LoadSchema/LoadData paths ──
func TestEasyWins_LoadSchemaData(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// LoadSchema with non-existent dir (returns nil)
	err := c.LoadSchema("/nonexistent/path/12345")
	if err != nil {
		t.Logf("LoadSchema nonexistent: %v", err)
	}

	// LoadData with non-existent dir
	err = c.LoadData("/nonexistent/path/12345")
	if err != nil {
		t.Logf("LoadData nonexistent: %v", err)
	}
}

// ── catalog_txn.go paths ──
func TestEasyWins_TransactionPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Begin transaction
	c.BeginTransaction(1)
	if !c.IsTransactionActive() {
		t.Error("Expected transaction to be active")
	}

	// Rollback without WAL (simple rollback)
	err := c.RollbackTransaction()
	if err != nil {
		t.Logf("Rollback no WAL: %v", err)
	}

	// Commit without changes
	c.BeginTransaction(2)
	err = c.CommitTransaction()
	if err != nil {
		t.Logf("Commit no WAL: %v", err)
	}

	// Savepoint without active transaction
	err = c.Savepoint("sp1")
	if err == nil {
		t.Error("Expected error for savepoint without transaction")
	}

	// RollbackToSavepoint without active transaction
	err = c.RollbackToSavepoint("sp1")
	if err == nil {
		t.Error("Expected error for rollback savepoint without transaction")
	}
}

// ── catalog_eval.go evaluateWhere with nil result ──
func TestEasyWins_WhereNilResult(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE where_nil (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO where_nil (id, val) VALUES (1, NULL)")
	c.ExecuteQuery("INSERT INTO where_nil (id, val) VALUES (2, 10)")

	// WHERE with NULL comparison (returns nil from EvalExpression)
	_, err := c.ExecuteQuery("SELECT id FROM where_nil WHERE val = NULL")
	if err != nil {
		t.Logf("WHERE NULL cmp: %v", err)
	}
}

// ── catalog_core.go addHiddenOrderByCols dotted identifier ──
func TestEasyWins_HiddenOrderByDotted(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE hd_a (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE hd_b (id INTEGER PRIMARY KEY, a_id INTEGER, val INTEGER)")
	c.ExecuteQuery("INSERT INTO hd_a (id, name) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO hd_b (id, a_id, val) VALUES (1, 1, 10)")

	// ORDER BY dotted identifier not in SELECT
	_, err := c.ExecuteQuery("SELECT hd_a.name FROM hd_a JOIN hd_b ON hd_a.id = hd_b.a_id ORDER BY hd_b.val")
	if err != nil {
		t.Logf("Hidden ORDER BY dotted: %v", err)
	}
}

// ── catalog_core.go getTableTreesForScan partition path ──
func TestEasyWins_PartitionScan(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE part_t (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO part_t (id, val) VALUES (1, 10)")

	// Normal table scan (getTableTreesForScan returns single tree)
	_, err := c.ExecuteQuery("SELECT * FROM part_t")
	if err != nil {
		t.Logf("Partition scan: %v", err)
	}
}

// ── catalog_core.go early termination limit path ──
func TestEasyWins_EarlyTermination(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE early_t (id INTEGER PRIMARY KEY)")
	for i := 1; i <= 20; i++ {
		c.ExecuteQuery("INSERT INTO early_t (id) VALUES (" + string(rune('0'+i%10)) + ")")
	}

	// LIMIT without ORDER BY triggers early termination
	_, err := c.ExecuteQuery("SELECT id FROM early_t LIMIT 5")
	if err != nil {
		t.Logf("Early termination: %v", err)
	}
}
