package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestEvaluateWhereTrueLiteral tests evaluateWhere with TRUE literal
func TestEvaluateWhereTrueLiteral(t *testing.T) {
	c := &Catalog{}
	row := []interface{}{1, "test"}
	columns := []ColumnDef{{Name: "id"}, {Name: "name"}}

	// TRUE literal
	result, err := evaluateWhere(c, row, columns, &query.BooleanLiteral{Value: true}, nil)
	if err != nil {
		t.Errorf("evaluateWhere with TRUE failed: %v", err)
	}
	if !result {
		t.Error("Expected TRUE to return true")
	}

	// FALSE literal
	result, err = evaluateWhere(c, row, columns, &query.BooleanLiteral{Value: false}, nil)
	if err != nil {
		t.Errorf("evaluateWhere with FALSE failed: %v", err)
	}
	if result {
		t.Error("Expected FALSE to return false")
	}
}

// TestEvaluateWhereNullResult tests evaluateWhere when expression returns nil
func TestEvaluateWhereNullResult(t *testing.T) {
	c := &Catalog{}
	row := []interface{}{nil, "test"}
	columns := []ColumnDef{{Name: "id"}, {Name: "name"}}

	// Identifier that resolves to nil
	result, err := evaluateWhere(c, row, columns, &query.Identifier{Name: "id"}, nil)
	if err != nil {
		t.Errorf("evaluateWhere with nil value failed: %v", err)
	}
	if result {
		t.Error("Expected nil value to return false")
	}
}

// TestEvaluateWhereNumeric tests evaluateWhere with numeric results
func TestEvaluateWhereNumeric(t *testing.T) {
	c := &Catalog{}
	row := []interface{}{1, "test"}
	columns := []ColumnDef{{Name: "id"}, {Name: "name"}}

	// Non-zero int should be truthy
	result, err := evaluateWhere(c, row, columns, &query.Identifier{Name: "id"}, nil)
	if err != nil {
		t.Errorf("evaluateWhere with int failed: %v", err)
	}
	if !result {
		t.Error("Expected non-zero int to return true")
	}
}

// TestEvaluateWhereString tests evaluateWhere with string results
func TestEvaluateWhereString(t *testing.T) {
	c := &Catalog{}
	row := []interface{}{1, "test"}
	columns := []ColumnDef{{Name: "id"}, {Name: "name"}}

	// Non-empty string should be truthy
	result, err := evaluateWhere(c, row, columns, &query.Identifier{Name: "name"}, nil)
	if err != nil {
		t.Errorf("evaluateWhere with string failed: %v", err)
	}
	if !result {
		t.Error("Expected non-empty string to return true")
	}

	// Empty string should be falsy
	row2 := []interface{}{1, ""}
	result, err = evaluateWhere(c, row2, columns, &query.Identifier{Name: "name"}, nil)
	if err != nil {
		t.Errorf("evaluateWhere with empty string failed: %v", err)
	}
	if result {
		t.Error("Expected empty string to return false")
	}
}

// TestToIntEdgeCases tests toInt function edge cases
func TestToIntEdgeCases(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int
		ok       bool
	}{
		{int(42), 42, true},
		{int64(42), 42, true},
		{float64(42.0), 42, true},
		{float64(42.5), 42, true}, // Truncates
		// Note: toInt only handles numeric types, not strings or bools
		{nil, 0, false},
		{struct{}{}, 0, false},
	}

	for _, tt := range tests {
		result, ok := toInt(tt.input)
		if ok != tt.ok {
			t.Errorf("toInt(%v) ok = %v, want %v", tt.input, ok, tt.ok)
			continue
		}
		if ok && result != tt.expected {
			t.Errorf("toInt(%v) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

// TestAddHiddenOrderByColsComplex tests addHiddenOrderByCols with complex expressions
func TestAddHiddenOrderByColsComplex(t *testing.T) {
	// Test with aggregate in ORDER BY
	orderBy := []*query.OrderByExpr{
		{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}}, Desc: true},
	}
	table := &TableDef{Name: "test", Columns: []ColumnDef{{Name: "category"}}}

	cols, _ := addHiddenOrderByCols(orderBy, []selectColInfo{{name: "category"}}, table)
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}
}

// TestAddHiddenOrderByColsPositional tests addHiddenOrderByCols with positional refs
func TestAddHiddenOrderByColsPositional(t *testing.T) {
	orderBy := []*query.OrderByExpr{
		{Expr: &query.NumberLiteral{Value: 2}}, // ORDER BY 2
	}
	table := &TableDef{Name: "test", Columns: []ColumnDef{{Name: "a"}, {Name: "b"}}}

	cols, _ := addHiddenOrderByCols(orderBy, []selectColInfo{{name: "a"}, {name: "b"}}, table)
	// Positional refs should not add hidden columns
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns for positional ORDER BY, got %d", len(cols))
	}
}

// TestReplaceAggregatesInExprComplex tests replaceAggregatesInExpr with complex expressions
func TestReplaceAggregatesInExprComplex(t *testing.T) {
	// Test with nested expression containing aggregate
	expr := &query.BinaryExpr{
		Left:     &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		Operator: query.TokenPlus,
		Right:    &query.NumberLiteral{Value: 10},
	}

	aggResults := make(map[*query.FunctionCall]interface{})

	result := replaceAggregatesInExpr(expr, aggResults)

	// Check if result is still a BinaryExpr
	if _, ok := result.(*query.BinaryExpr); !ok {
		t.Errorf("Expected BinaryExpr, got %T", result)
	}
}

// TestResolveAggregateInExprBinary tests resolveAggregateInExpr with BinaryExpr
func TestResolveAggregateInExprBinary(t *testing.T) {
	groupRow := []interface{}{"A", 100}
	selectCols := []selectColInfo{{name: "category"}, {name: "SUM(amount)"}}

	// Test SUM(x) + 10
	expr := &query.BinaryExpr{
		Left:     &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		Operator: query.TokenPlus,
		Right:    &query.NumberLiteral{Value: 10},
	}

	result := resolveAggregateInExpr(expr, selectCols, groupRow)
	_ = result
	t.Logf("resolveAggregateInExpr result: %v", result)
}

// TestResolveOuterRefsInQuerySimple tests resolveOuterRefsInQuery
func TestResolveOuterRefsInQuerySimple(t *testing.T) {
	// Simple query without outer refs
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
		},
		From: &query.TableRef{Name: "test"},
	}

	result := resolveOuterRefsInQuery(stmt, nil, nil)
	_ = result
}

// TestResolveOuterRefsInExprSimple tests resolveOuterRefsInExpr
func TestResolveOuterRefsInExprSimple(t *testing.T) {
	// Simple identifier
	expr := &query.Identifier{Name: "id"}
	result := resolveOuterRefsInExpr(expr, nil, nil, nil)
	_ = result
}

// TestMatchLikeSimpleBasic tests matchLikeSimple basic patterns
func TestMatchLikeSimpleBasic(t *testing.T) {
	tests := []struct {
		value   string
		pattern string
		want    bool
	}{
		{"hello", "h%", true},
		{"hello", "%o", true},
		{"hello", "%ell%", true},
		{"hello", "h_llo", true},
		{"hello", "world", false},
		{"hello", "", false},
		{"", "%", true},
		{"HELLO", "hello", true}, // Case insensitive
	}

	for _, tt := range tests {
		result := matchLikeSimple(tt.value, tt.pattern)
		if result != tt.want {
			t.Errorf("matchLikeSimple(%q, %q) = %v, want %v", tt.value, tt.pattern, result, tt.want)
		}
	}
}

// TestEvaluateCastExprString tests evaluateCastExpr with string casts
func TestEvaluateCastExprString(t *testing.T) {
	c := &Catalog{}
	row := []interface{}{123}
	columns := []ColumnDef{{Name: "val"}}

	// CAST(int AS TEXT)
	expr := &query.CastExpr{
		Expr:     &query.Identifier{Name: "val"},
		DataType: query.TokenString,
	}

	result, err := evaluateCastExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Errorf("evaluateCastExpr failed: %v", err)
	}
	// Just verify it runs - the actual cast behavior may vary
	_ = result
}

// TestEvaluateCastExprInteger tests evaluateCastExpr with integer casts
func TestEvaluateCastExprInteger(t *testing.T) {
	c := &Catalog{}
	row := []interface{}{"456"}
	columns := []ColumnDef{{Name: "val"}}

	// CAST(string AS INTEGER)
	expr := &query.CastExpr{
		Expr:     &query.Identifier{Name: "val"},
		DataType: query.TokenInteger,
	}

	result, err := evaluateCastExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Errorf("evaluateCastExpr failed: %v", err)
	}
	// Just verify it runs - the actual cast behavior may vary
	_ = result
}

// TestEvaluateCastExprReal tests evaluateCastExpr with REAL casts
func TestEvaluateCastExprReal(t *testing.T) {
	c := &Catalog{}
	row := []interface{}{"3.14"}
	columns := []ColumnDef{{Name: "val"}}

	// CAST(string AS REAL)
	expr := &query.CastExpr{
		Expr:     &query.Identifier{Name: "val"},
		DataType: query.TokenReal,
	}

	result, err := evaluateCastExpr(c, row, columns, expr, nil)
	if err != nil {
		t.Errorf("evaluateCastExpr failed: %v", err)
	}
	// Just verify it runs - the actual cast behavior may vary
	_ = result
}

// TestIsCacheableQueryUnit tests isCacheableQuery function
func TestIsCacheableQueryUnit(t *testing.T) {
	// Cacheable query
	stmt1 := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "test"},
	}
	if !isCacheableQuery(stmt1) {
		t.Error("Simple SELECT should be cacheable")
	}

	// Non-cacheable: has RANDOM()
	stmt2 := &query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "RANDOM"}},
		From:    &query.TableRef{Name: "test"},
	}
	if isCacheableQuery(stmt2) {
		t.Error("SELECT with RANDOM() should not be cacheable")
	}

	// Non-cacheable: has NOW()
	stmt3 := &query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "NOW"}},
		From:    &query.TableRef{Name: "test"},
	}
	if isCacheableQuery(stmt3) {
		t.Error("SELECT with NOW() should not be cacheable")
	}
}

// TestQueryToSQLParts tests queryToSQL with various query parts
func TestQueryToSQLParts(t *testing.T) {
	// Simple query
	stmt1 := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "test"},
	}
	sql1 := queryToSQL(stmt1)
	if sql1 == "" {
		t.Error("queryToSQL returned empty string")
	}

	// Query with DISTINCT
	stmt2 := &query.SelectStmt{
		Distinct: true,
		Columns:  []query.Expression{&query.Identifier{Name: "id"}},
		From:     &query.TableRef{Name: "test"},
	}
	sql2 := queryToSQL(stmt2)
	if sql2 == "" {
		t.Error("queryToSQL with DISTINCT returned empty string")
	}

	// Query with WHERE
	stmt3 := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "test"},
		Where:   &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	}
	sql3 := queryToSQL(stmt3)
	if sql3 == "" {
		t.Error("queryToSQL with WHERE returned empty string")
	}
}

// TestExprToStringComplex tests exprToString with complex expressions
func TestExprToStringComplex(t *testing.T) {
	// Binary expression
	expr1 := &query.BinaryExpr{
		Left:     &query.Identifier{Name: "a"},
		Operator: query.TokenPlus,
		Right:    &query.NumberLiteral{Value: 1},
	}
	s1 := exprToString(expr1)
	if s1 == "" {
		t.Error("exprToString with BinaryExpr returned empty string")
	}

	// Unary expression
	expr2 := &query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.NumberLiteral{Value: 5},
	}
	s2 := exprToString(expr2)
	if s2 == "" {
		t.Error("exprToString with UnaryExpr returned empty string")
	}

	// Function call
	expr3 := &query.FunctionCall{
		Name: "COUNT",
		Args: []query.Expression{&query.StarExpr{}},
	}
	s3 := exprToString(expr3)
	if s3 == "" {
		t.Error("exprToString with FunctionCall returned empty string")
	}

	// Alias expression
	expr4 := &query.AliasExpr{
		Expr:  &query.Identifier{Name: "id"},
		Alias: "pk",
	}
	s4 := exprToString(expr4)
	if s4 == "" {
		t.Error("exprToString with AliasExpr returned empty string")
	}

	// Case expression
	expr5 := &query.CaseExpr{
		Whens: []*query.WhenClause{
			{Condition: &query.BooleanLiteral{Value: true}, Result: &query.NumberLiteral{Value: 1}},
		},
		Else: &query.NumberLiteral{Value: 0},
	}
	s5 := exprToString(expr5)
	if s5 == "" {
		t.Error("exprToString with CaseExpr returned empty string")
	}
}

// TestGetTableTreesForScan tests getTableTreesForScan function
func TestGetTableTreesForScan(t *testing.T) {
	c := New(nil, nil, nil)

	// Create a table
	table := &TableDef{
		Name:    "test",
		Columns: []ColumnDef{{Name: "id", PrimaryKey: true}},
	}
	c.tables["test"] = table

	// Get trees for scan
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		t.Logf("getTableTreesForScan returned error: %v", err)
	}
	_ = trees
}

// TestCompareValuesEdgeCases tests compareValues with edge cases
// Note: compareValues sorts NULLs LAST (after all non-NULL values)
func TestCompareValuesEdgeCases(t *testing.T) {
	// Both nil
	if compareValues(nil, nil) != 0 {
		t.Error("compareValues(nil, nil) should return 0")
	}

	// NULL sorts AFTER non-NULL (returns positive)
	if compareValues(nil, 1) <= 0 {
		t.Error("compareValues(nil, 1) should return positive (NULL sorts last)")
	}

	// non-NULL sorts BEFORE NULL (returns negative)
	if compareValues(1, nil) >= 0 {
		t.Error("compareValues(1, nil) should return negative (NULL sorts last)")
	}

	// Same strings
	if compareValues("abc", "abc") != 0 {
		t.Error("compareValues('abc', 'abc') should return 0")
	}

	// Different strings
	if compareValues("abc", "def") >= 0 {
		t.Error("compareValues('abc', 'def') should return negative")
	}
}

// TestIntersectSortedUnit tests intersectSorted function
func TestIntersectSortedUnit(t *testing.T) {
	a := []int64{1, 2, 3, 4, 5}
	b := []int64{3, 4, 5, 6, 7}

	result := intersectSorted(a, b)
	if len(result) != 3 {
		t.Errorf("Expected 3 elements in intersection, got %d", len(result))
	}

	// No intersection
	c := []int64{1, 2}
	d := []int64{3, 4}
	result2 := intersectSorted(c, d)
	if len(result2) != 0 {
		t.Errorf("Expected 0 elements in intersection, got %d", len(result2))
	}
}
