package catalog

// Regression tests for SQL-semantics fixes:
//  1. Lazy (short-circuit) evaluation of CASE / COALESCE / IFNULL / IIF guards
//  2. String-vs-string comparison must not coerce numeric-looking strings
//  3. CASE WHEN with numeric (non-bool) conditions
//  4. SUM accumulates in int64 while inputs are integers
//  5. Window PARTITION BY keys are collision-safe
//  6. LAG/LEAD default expression evaluated per out-of-range row
//  7. LIKE is rune-aware and non-exponential
//  8. CAST expression and CAST function share one implementation

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newSemanticsTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

func semExec(t *testing.T, c *Catalog, sql string) {
	t.Helper()
	stmt, err := query.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	switch s := stmt.(type) {
	case *query.CreateTableStmt:
		if err := c.CreateTable(s); err != nil {
			t.Fatalf("create table %q: %v", sql, err)
		}
	case *query.InsertStmt:
		if _, _, err := c.Insert(context.Background(), s, nil); err != nil {
			t.Fatalf("insert %q: %v", sql, err)
		}
	default:
		t.Fatalf("unsupported statement %T for %q", stmt, sql)
	}
}

func semQuery(t *testing.T, c *Catalog, sql string) [][]interface{} {
	t.Helper()
	stmt, err := query.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	sel, ok := stmt.(*query.SelectStmt)
	if !ok {
		t.Fatalf("statement %q is %T, not SELECT", sql, stmt)
	}
	_, rows, err := c.Select(sel, nil)
	if err != nil {
		t.Fatalf("select %q: %v", sql, err)
	}
	return rows
}

// semNum converts a query result cell to float64 for tolerant numeric checks.
func semNum(t *testing.T, v interface{}) float64 {
	t.Helper()
	f, ok := toFloat64(v)
	if !ok {
		t.Fatalf("value %v (%T) is not numeric", v, v)
	}
	return f
}

func newGuardTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	c := newSemanticsTestCatalog(t)
	semExec(t, c, "CREATE TABLE guard (a INTEGER, b INTEGER)")
	semExec(t, c, "INSERT INTO guard VALUES (5, 0)")
	semExec(t, c, "INSERT INTO guard VALUES (6, 2)")
	return c
}

// Fix 1: COALESCE must not evaluate later arguments once a non-NULL argument
// is found; a guarded division by zero must never run.
func TestLazyCoalesceSkipsGuardedError(t *testing.T) {
	c := newGuardTestCatalog(t)

	rows := semQuery(t, c, "SELECT COALESCE(a, 1/0) FROM guard ORDER BY b")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if got := semNum(t, rows[0][0]); got != 5 {
		t.Errorf("COALESCE(a, 1/0) with a=5: got %v, want 5", rows[0][0])
	}
	if got := semNum(t, rows[1][0]); got != 6 {
		t.Errorf("COALESCE(a, 1/0) with a=6: got %v, want 6", rows[1][0])
	}

	// IFNULL shares the same short-circuit.
	rows = semQuery(t, c, "SELECT IFNULL(a, 1/0) FROM guard ORDER BY b")
	if got := semNum(t, rows[0][0]); got != 5 {
		t.Errorf("IFNULL(a, 1/0) with a=5: got %v, want 5", rows[0][0])
	}

	// The fallback still evaluates when needed.
	rows = semQuery(t, c, "SELECT COALESCE(NULL, 7) FROM guard ORDER BY b")
	if got := semNum(t, rows[0][0]); got != 7 {
		t.Errorf("COALESCE(NULL, 7): got %v, want 7", rows[0][0])
	}
}

// Fix 1: searched CASE must not evaluate THEN branches whose condition is
// false, nor the ELSE branch when a WHEN matches.
func TestLazySearchedCaseGuardsDivision(t *testing.T) {
	c := newGuardTestCatalog(t)

	rows := semQuery(t, c, "SELECT CASE WHEN b <> 0 THEN a / b ELSE 0 END FROM guard ORDER BY b")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] == nil {
		t.Fatalf("CASE guard with b=0 returned NULL; guarded a/b was evaluated eagerly")
	}
	if got := semNum(t, rows[0][0]); got != 0 {
		t.Errorf("CASE with b=0: got %v, want 0", rows[0][0])
	}
	if got := semNum(t, rows[1][0]); got != 3 {
		t.Errorf("CASE with b=2: got %v, want 3", rows[1][0])
	}
}

// Fix 1: simple CASE is lazy too, and compares the base value per SQL rules.
func TestLazySimpleCaseGuardsDivision(t *testing.T) {
	c := newGuardTestCatalog(t)

	rows := semQuery(t, c, "SELECT CASE b WHEN 0 THEN -1 ELSE a / b END FROM guard ORDER BY b")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if got := semNum(t, rows[0][0]); got != -1 {
		t.Errorf("simple CASE with b=0: got %v, want -1", rows[0][0])
	}
	if got := semNum(t, rows[1][0]); got != 3 {
		t.Errorf("simple CASE with b=2: got %v, want 3", rows[1][0])
	}

	// A NULL base value matches no WHEN (comparison is UNKNOWN) and falls
	// through to ELSE.
	rows = semQuery(t, c, "SELECT CASE NULL WHEN NULL THEN 'matched' ELSE 'else' END FROM guard ORDER BY b")
	if got, _ := toString(rows[0][0]); got != "else" {
		t.Errorf("CASE NULL WHEN NULL: got %v, want 'else'", rows[0][0])
	}
}

// Fix 1: IIF evaluates only the selected branch.
func TestLazyIIFGuardsDivision(t *testing.T) {
	c := newGuardTestCatalog(t)

	rows := semQuery(t, c, "SELECT IIF(b <> 0, a / b, 0) FROM guard ORDER BY b")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] == nil {
		t.Fatalf("IIF with b=0 returned NULL; guarded a/b was evaluated eagerly")
	}
	if got := semNum(t, rows[0][0]); got != 0 {
		t.Errorf("IIF with b=0: got %v, want 0", rows[0][0])
	}
	if got := semNum(t, rows[1][0]); got != 3 {
		t.Errorf("IIF with b=2: got %v, want 3", rows[1][0])
	}
}

// Fix 3: CASE WHEN with a numeric condition (not strictly bool true).
func TestCaseNumericCondition(t *testing.T) {
	c := newGuardTestCatalog(t)

	rows := semQuery(t, c, "SELECT CASE WHEN 1 THEN 'yes' ELSE 'no' END FROM guard ORDER BY b")
	if got, _ := toString(rows[0][0]); got != "yes" {
		t.Errorf("CASE WHEN 1: got %v, want 'yes'", rows[0][0])
	}
	rows = semQuery(t, c, "SELECT CASE WHEN 0 THEN 'yes' ELSE 'no' END FROM guard ORDER BY b")
	if got, _ := toString(rows[0][0]); got != "no" {
		t.Errorf("CASE WHEN 0: got %v, want 'no'", rows[0][0])
	}
}

// Fix 3 (unit): EvalCase accepts truthy non-bool conditions and performs
// simple-CASE comparison when a base value is supplied.
func TestEvalCaseTruthiness(t *testing.T) {
	ctx := NewEvalContext(nil, nil, nil, nil)

	got, err := ctx.EvalCase(nil, [][2]interface{}{{int64(1), "yes"}}, "no")
	if err != nil || got != "yes" {
		t.Errorf("EvalCase(nil, [1->yes], no) = %v, %v; want yes", got, err)
	}
	got, err = ctx.EvalCase(nil, [][2]interface{}{{int64(0), "yes"}}, "no")
	if err != nil || got != "no" {
		t.Errorf("EvalCase(nil, [0->yes], no) = %v, %v; want no", got, err)
	}
	// Simple CASE: base value compared against WHEN values.
	got, err = ctx.EvalCase(int64(2), [][2]interface{}{{int64(1), "one"}, {int64(2), "two"}}, "other")
	if err != nil || got != "two" {
		t.Errorf("EvalCase(2, ...) = %v, %v; want two", got, err)
	}
}

// Fix 2: two strings always compare as strings; numeric coercion applies only
// to mixed string/number pairs.
func TestCompareValuesStringSemantics(t *testing.T) {
	if compareValues("0123", "123") == 0 {
		t.Error("'0123' must not equal '123'")
	}
	if compareValues("1e3", "1000") == 0 {
		t.Error("'1e3' must not equal '1000'")
	}
	if compareValues("9", "10") <= 0 {
		t.Error("'9' must sort after '10' lexicographically")
	}
	if compareValues("abc", "abd") >= 0 {
		t.Error("'abc' must sort before 'abd'")
	}
	// Mixed string/number pairs keep numeric coercion.
	if compareValues("123", int64(123)) != 0 {
		t.Error("'123' should equal 123 via mixed coercion")
	}
	if compareValues(int64(9), "10") >= 0 {
		t.Error("9 should be less than '10' via mixed coercion")
	}
}

func TestTextColumnComparisonAndOrdering(t *testing.T) {
	c := newSemanticsTestCatalog(t)
	semExec(t, c, "CREATE TABLE zips (zip TEXT)")
	semExec(t, c, "INSERT INTO zips VALUES ('01234')")
	semExec(t, c, "INSERT INTO zips VALUES ('1234')")

	rows := semQuery(t, c, "SELECT zip FROM zips WHERE zip = '1234'")
	if len(rows) != 1 {
		t.Fatalf("WHERE zip = '1234' matched %d rows, want 1", len(rows))
	}
	if got, _ := toString(rows[0][0]); got != "1234" {
		t.Errorf("matched %q, want '1234'", got)
	}

	semExec(t, c, "CREATE TABLE strs (s TEXT)")
	for _, v := range []string{"9", "10", "1000", "1e3"} {
		semExec(t, c, "INSERT INTO strs VALUES ('"+v+"')")
	}
	rows = semQuery(t, c, "SELECT s FROM strs ORDER BY s")
	want := []string{"10", "1000", "1e3", "9"}
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(rows), len(want))
	}
	for i, w := range want {
		if got, _ := toString(rows[i][0]); got != w {
			t.Errorf("ORDER BY s row %d: got %q, want %q", i, got, w)
		}
	}
}

// Fix 4 (unit): SUM keeps int64 precision for integer inputs and falls back
// to float64 on overflow or float inputs.
func TestSumAccumulatorPrecision(t *testing.T) {
	got := reduceBasicAggregate("SUM", []interface{}{int64(9007199254740993), int64(1)}, 2, false, false)
	if v, ok := got.(int64); !ok || v != 9007199254740994 {
		t.Errorf("SUM(9007199254740993, 1) = %v (%T), want int64 9007199254740994", got, got)
	}

	// NULLs are ignored; all-NULL input yields NULL.
	got = reduceBasicAggregate("SUM", []interface{}{nil, int64(2), nil}, 3, false, false)
	if v, ok := got.(int64); !ok || v != 2 {
		t.Errorf("SUM(NULL, 2, NULL) = %v (%T), want int64 2", got, got)
	}
	if got := reduceBasicAggregate("SUM", []interface{}{nil, nil}, 2, false, false); got != nil {
		t.Errorf("SUM of NULLs = %v, want nil", got)
	}

	// Float input switches to float64 accumulation.
	got = reduceBasicAggregate("SUM", []interface{}{int64(1), 2.5}, 2, false, false)
	if v, ok := got.(float64); !ok || v != 3.5 {
		t.Errorf("SUM(1, 2.5) = %v (%T), want float64 3.5", got, got)
	}

	// Overflow falls back to float64 rather than wrapping.
	got = reduceBasicAggregate("SUM", []interface{}{int64(math.MaxInt64), int64(math.MaxInt64)}, 2, false, false)
	if v, ok := got.(float64); !ok || v <= 0 {
		t.Errorf("overflowing SUM = %v (%T), want positive float64", got, got)
	}
}

// Fix 4: window SUM keeps int64 precision too.
func TestWindowSumInt64Precision(t *testing.T) {
	c := newSemanticsTestCatalog(t)
	semExec(t, c, "CREATE TABLE big (id INTEGER, v INTEGER)")
	semExec(t, c, "INSERT INTO big VALUES (1, 9007199254740993)")
	semExec(t, c, "INSERT INTO big VALUES (2, 1)")

	rows := semQuery(t, c, "SELECT SUM(v) OVER () FROM big")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	for i, row := range rows {
		v, ok := row[0].(int64)
		if !ok || v != 9007199254740994 {
			t.Errorf("row %d: SUM(v) OVER () = %v (%T), want int64 9007199254740994", i, row[0], row[0])
		}
	}

	// Running (ORDER BY) window sum.
	rows = semQuery(t, c, "SELECT SUM(v) OVER (ORDER BY id) FROM big")
	if v, ok := rows[1][0].(int64); !ok || v != 9007199254740994 {
		t.Errorf("running SUM last row = %v (%T), want int64 9007199254740994", rows[1][0], rows[1][0])
	}
}

// Fix 5: PARTITION BY keys must not collide when concatenated values are
// ambiguous: ('x|y','z') and ('x','y|z') are different partitions.
func TestWindowPartitionKeyCollision(t *testing.T) {
	c := newSemanticsTestCatalog(t)
	semExec(t, c, "CREATE TABLE pt (a TEXT, b TEXT)")
	semExec(t, c, "INSERT INTO pt VALUES ('x|y', 'z')")
	semExec(t, c, "INSERT INTO pt VALUES ('x', 'y|z')")

	rows := semQuery(t, c, "SELECT COUNT(*) OVER (PARTITION BY a, b) FROM pt")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	for i, row := range rows {
		if got := semNum(t, row[0]); got != 1 {
			t.Errorf("row %d: COUNT(*) OVER (PARTITION BY a, b) = %v, want 1 (partition key collision)", i, row[0])
		}
	}
}

// Fix 6: the LAG/LEAD default expression is evaluated against each
// out-of-range row, not once against the partition's first row.
func TestLagLeadDefaultEvaluatedPerRow(t *testing.T) {
	c := newSemanticsTestCatalog(t)
	semExec(t, c, "CREATE TABLE emp (id INTEGER, sal INTEGER)")
	semExec(t, c, "INSERT INTO emp VALUES (1, 100)")
	semExec(t, c, "INSERT INTO emp VALUES (2, 200)")
	semExec(t, c, "INSERT INTO emp VALUES (3, 300)")

	rows := semQuery(t, c, "SELECT id, LEAD(sal, 1, id * 100) OVER (ORDER BY id) FROM emp")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	byID := map[int64]float64{}
	for _, row := range rows {
		byID[int64(semNum(t, row[0]))] = semNum(t, row[1])
	}
	if byID[1] != 200 || byID[2] != 300 {
		t.Errorf("LEAD in-range values wrong: %v", byID)
	}
	// Last row is out of range: default id*100 must use ITS id (3*100=300),
	// not the first row's (1*100=100).
	if byID[3] != 300 {
		t.Errorf("LEAD default for id=3: got %v, want 300 (its own id*100)", byID[3])
	}

	rows = semQuery(t, c, "SELECT id, LAG(sal, 1, id * 100) OVER (ORDER BY id) FROM emp")
	byID = map[int64]float64{}
	for _, row := range rows {
		byID[int64(semNum(t, row[0]))] = semNum(t, row[1])
	}
	if byID[1] != 100 {
		t.Errorf("LAG default for id=1: got %v, want 100", byID[1])
	}
	if byID[2] != 100 || byID[3] != 200 {
		t.Errorf("LAG in-range values wrong: %v", byID)
	}
}

// Fix 7: LIKE `_` matches one character (not one byte), the matcher is
// linear-ish (no exponential backtracking), and escape handling is preserved.
// LIKE remains case-sensitive: that is this engine's established semantics.
func TestMatchLikeSimpleRunesAndPerformance(t *testing.T) {
	if !matchLikeSimple("é", "_") {
		t.Error("'é' LIKE '_' should match (rune, not byte)")
	}
	if !matchLikeSimple("héllo", "h_llo") {
		t.Error("'héllo' LIKE 'h_llo' should match")
	}
	if !matchLikeSimple("日本語", "___") {
		t.Error("'日本語' LIKE '___' should match")
	}
	if matchLikeSimple("日本語", "__") {
		t.Error("'日本語' LIKE '__' should not match")
	}
	if !matchLikeSimple("abc", "a%c") || matchLikeSimple("abd", "a%c") {
		t.Error("basic %% matching broken")
	}
	if !matchLikeSimple("", "") || matchLikeSimple("x", "") {
		t.Error("empty pattern must match only empty string")
	}
	if !matchLikeSimple("anything", "%%%") {
		t.Error("all-%% pattern must match anything")
	}
	// Escape handling.
	if !matchLikeSimple("50%", "50\\%", '\\') {
		t.Error("escaped %% should match literal %%")
	}
	if matchLikeSimple("50x", "50\\%", '\\') {
		t.Error("escaped %% must not act as wildcard")
	}
	if !matchLikeSimple("a_b", "a\\_b", '\\') || matchLikeSimple("axb", "a\\_b", '\\') {
		t.Error("escaped _ should match only literal _")
	}
	// Case sensitivity is preserved (documented engine behavior).
	if matchLikeSimple("ABC", "abc") {
		t.Error("LIKE is case-sensitive in this engine")
	}

	// Pathological pattern must complete quickly (previously exponential).
	s := strings.Repeat("a", 4000) + "!"
	pattern := strings.Repeat("%a", 25) + "%b"
	start := time.Now()
	if matchLikeSimple(s, pattern) {
		t.Error("pathological pattern should not match")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("pathological LIKE took %v; matcher is super-linear", elapsed)
	}
}

func TestLikeUnicodeUnderscoreQuery(t *testing.T) {
	c := newSemanticsTestCatalog(t)
	semExec(t, c, "CREATE TABLE uni (name TEXT)")
	semExec(t, c, "INSERT INTO uni VALUES ('é')")
	semExec(t, c, "INSERT INTO uni VALUES ('ab')")

	rows := semQuery(t, c, "SELECT name FROM uni WHERE name LIKE '_'")
	if len(rows) != 1 {
		t.Fatalf("LIKE '_' matched %d rows, want 1", len(rows))
	}
	if got, _ := toString(rows[0][0]); got != "é" {
		t.Errorf("LIKE '_' matched %q, want 'é'", got)
	}
}

// Fix 8: CAST as expression node uses applyCast (single implementation).
func TestCastExprUnified(t *testing.T) {
	c := newGuardTestCatalog(t)

	rows := semQuery(t, c, "SELECT CAST('3.7' AS INTEGER) FROM guard ORDER BY b")
	if v, ok := rows[0][0].(int64); !ok || v != 3 {
		t.Errorf("CAST('3.7' AS INTEGER) = %v (%T), want int64 3", rows[0][0], rows[0][0])
	}

	// Direct expression-node path.
	expr := &query.CastExpr{Expr: &query.StringLiteral{Value: "3.7"}, DataType: query.TokenInteger}
	got, err := evaluateExpression(c, nil, nil, expr, nil)
	if err != nil {
		t.Fatalf("CastExpr eval: %v", err)
	}
	if v, ok := got.(int64); !ok || v != 3 {
		t.Errorf("CastExpr '3.7'->INTEGER = %v (%T), want int64 3", got, got)
	}
	// And it agrees with the CAST(...) function-call form.
	fnGot, err := evaluateExpression(c, nil, nil, &query.FunctionCall{
		Name: "CAST",
		Args: []query.Expression{&query.StringLiteral{Value: "3.7"}, &query.StringLiteral{Value: "INTEGER"}},
	}, nil)
	if err != nil {
		t.Fatalf("CAST function eval: %v", err)
	}
	if fnGot != got {
		t.Errorf("CAST expr (%v) and CAST function (%v) diverge", got, fnGot)
	}
}

// Fix 9 (live-path divergences): value-expression arithmetic keeps int64
// precision and LENGTH returns int64.
func TestValueExpressionIntegerPrecision(t *testing.T) {
	got, err := evalBinaryExprValue(int64(9007199254740993), int64(1), query.TokenPlus)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := got.(int64); !ok || v != 9007199254740994 {
		t.Errorf("9007199254740993 + 1 = %v (%T), want int64 9007199254740994", got, got)
	}

	got, err = evalFunctionCallValue("LENGTH", []interface{}{"abcd"})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := got.(int64); !ok || v != 4 {
		t.Errorf("LENGTH('abcd') = %v (%T), want int64 4", got, got)
	}
}
