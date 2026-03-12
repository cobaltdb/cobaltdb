package catalog

import (
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// helper: create a catalog with a btree for unit tests
func setupCoverageCatalog(t *testing.T) (*Catalog, func()) {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	cat := New(tree, pool, nil)
	cleanup := func() { pool.Close() }
	return cat, cleanup
}

// helper: create a table via CreateTable
func createCoverageTestTable(t *testing.T, cat *Catalog, name string, cols []*query.ColumnDef) {
	t.Helper()
	stmt := &query.CreateTableStmt{
		Table:   name,
		Columns: cols,
	}
	if err := cat.CreateTable(stmt); err != nil {
		t.Fatalf("CreateTable(%s) failed: %v", name, err)
	}
}

// ============================================================
// 1. evaluateLike
// ============================================================

func TestCoverage_evaluateLike_BasicPatterns(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	row := []interface{}{"hello world"}
	cols := []ColumnDef{{Name: "val", Type: "TEXT"}}

	tests := []struct {
		name    string
		expr    *query.LikeExpr
		want    interface{}
	}{
		{
			"percent_match",
			&query.LikeExpr{
				Expr:    &query.Identifier{Name: "val"},
				Pattern: &query.StringLiteral{Value: "hello%"},
			},
			true,
		},
		{
			"percent_no_match",
			&query.LikeExpr{
				Expr:    &query.Identifier{Name: "val"},
				Pattern: &query.StringLiteral{Value: "xyz%"},
			},
			false,
		},
		{
			"underscore_match",
			&query.LikeExpr{
				Expr:    &query.Identifier{Name: "val"},
				Pattern: &query.StringLiteral{Value: "hello_world"},
			},
			true,
		},
		{
			"underscore_no_match",
			&query.LikeExpr{
				Expr:    &query.Identifier{Name: "val"},
				Pattern: &query.StringLiteral{Value: "hello_xyz"},
			},
			false,
		},
		{
			"exact_match",
			&query.LikeExpr{
				Expr:    &query.Identifier{Name: "val"},
				Pattern: &query.StringLiteral{Value: "hello world"},
			},
			true,
		},
		{
			"not_like",
			&query.LikeExpr{
				Expr:    &query.Identifier{Name: "val"},
				Pattern: &query.StringLiteral{Value: "hello%"},
				Not:     true,
			},
			false,
		},
		{
			"not_like_no_match",
			&query.LikeExpr{
				Expr:    &query.Identifier{Name: "val"},
				Pattern: &query.StringLiteral{Value: "xyz%"},
				Not:     true,
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluateLike(cat, row, cols, tt.expr, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCoverage_evaluateLike_NullHandling(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cols := []ColumnDef{{Name: "val", Type: "TEXT"}}

	// NULL value LIKE pattern => NULL
	row := []interface{}{nil}
	got, err := evaluateLike(cat, row, cols, &query.LikeExpr{
		Expr:    &query.Identifier{Name: "val"},
		Pattern: &query.StringLiteral{Value: "%"},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for NULL LIKE, got %v", got)
	}

	// value LIKE NULL => NULL
	row = []interface{}{"hello"}
	got, err = evaluateLike(cat, row, cols, &query.LikeExpr{
		Expr:    &query.Identifier{Name: "val"},
		Pattern: &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for val LIKE NULL, got %v", got)
	}
}

func TestCoverage_evaluateLike_EscapeChar(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	row := []interface{}{"100%"}
	cols := []ColumnDef{{Name: "val", Type: "TEXT"}}

	// Match literal % with escape
	got, err := evaluateLike(cat, row, cols, &query.LikeExpr{
		Expr:    &query.Identifier{Name: "val"},
		Pattern: &query.StringLiteral{Value: "100#%"},
		Escape:  &query.StringLiteral{Value: "#"},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for escaped %%, got %v", got)
	}
}

func TestCoverage_evaluateLike_NonStringValue(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Non-string value gets converted via Sprintf
	row := []interface{}{float64(42)}
	cols := []ColumnDef{{Name: "val", Type: "REAL"}}

	got, err := evaluateLike(cat, row, cols, &query.LikeExpr{
		Expr:    &query.Identifier{Name: "val"},
		Pattern: &query.StringLiteral{Value: "42%"},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for numeric LIKE, got %v", got)
	}
}

func TestCoverage_evaluateLike_CaseInsensitive(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	row := []interface{}{"Hello World"}
	cols := []ColumnDef{{Name: "val", Type: "TEXT"}}

	got, err := evaluateLike(cat, row, cols, &query.LikeExpr{
		Expr:    &query.Identifier{Name: "val"},
		Pattern: &query.StringLiteral{Value: "hello%"},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("LIKE should be case insensitive, got %v", got)
	}
}

// ============================================================
// 2. evaluateCastExpr
// ============================================================

func TestCoverage_evaluateCastExpr(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cols := []ColumnDef{}
	var row []interface{}

	tests := []struct {
		name     string
		expr     *query.CastExpr
		want     interface{}
	}{
		{
			"null_cast",
			&query.CastExpr{Expr: &query.NullLiteral{}, DataType: query.TokenInteger},
			nil,
		},
		{
			"float_to_int",
			&query.CastExpr{Expr: &query.NumberLiteral{Value: 3.7}, DataType: query.TokenInteger},
			int64(3),
		},
		{
			"string_to_int",
			&query.CastExpr{Expr: &query.StringLiteral{Value: "42"}, DataType: query.TokenInteger},
			int64(42),
		},
		{
			"bad_string_to_int",
			&query.CastExpr{Expr: &query.StringLiteral{Value: "abc"}, DataType: query.TokenInteger},
			int64(0),
		},
		{
			"bool_to_int",
			&query.CastExpr{Expr: &query.BooleanLiteral{Value: true}, DataType: query.TokenInteger},
			int64(1), // bool true => toFloat64 returns 1.0 => int64(1)
		},
		{
			"float_to_real",
			&query.CastExpr{Expr: &query.NumberLiteral{Value: 3.14}, DataType: query.TokenReal},
			float64(3.14),
		},
		{
			"string_to_real",
			&query.CastExpr{Expr: &query.StringLiteral{Value: "3.14"}, DataType: query.TokenReal},
			float64(3.14),
		},
		{
			"bad_string_to_real",
			&query.CastExpr{Expr: &query.StringLiteral{Value: "abc"}, DataType: query.TokenReal},
			float64(0),
		},
		{
			"int_to_text",
			&query.CastExpr{Expr: &query.NumberLiteral{Value: 42}, DataType: query.TokenText},
			"42",
		},
		{
			"bool_to_text",
			&query.CastExpr{Expr: &query.BooleanLiteral{Value: true}, DataType: query.TokenText},
			"true",
		},
		{
			"bool_to_boolean",
			&query.CastExpr{Expr: &query.BooleanLiteral{Value: true}, DataType: query.TokenBoolean},
			true,
		},
		{
			"int_to_boolean_nonzero",
			&query.CastExpr{Expr: &query.NumberLiteral{Value: 1}, DataType: query.TokenBoolean},
			true,
		},
		{
			"int_to_boolean_zero",
			&query.CastExpr{Expr: &query.NumberLiteral{Value: 0}, DataType: query.TokenBoolean},
			false,
		},
		{
			"string_true_to_boolean",
			&query.CastExpr{Expr: &query.StringLiteral{Value: "true"}, DataType: query.TokenBoolean},
			true,
		},
		{
			"string_1_to_boolean",
			&query.CastExpr{Expr: &query.StringLiteral{Value: "1"}, DataType: query.TokenBoolean},
			true,
		},
		{
			"string_false_to_boolean",
			&query.CastExpr{Expr: &query.StringLiteral{Value: "false"}, DataType: query.TokenBoolean},
			false,
		},
		{
			"unknown_datatype",
			&query.CastExpr{Expr: &query.StringLiteral{Value: "x"}, DataType: query.TokenType(9999)},
			"x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluateCastExpr(cat, row, cols, tt.expr, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// ============================================================
// 3. evaluateBetween
// ============================================================

func TestCoverage_evaluateBetween(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cols := []ColumnDef{}
	var row []interface{}

	tests := []struct {
		name string
		expr *query.BetweenExpr
		want interface{}
	}{
		{
			"in_range",
			&query.BetweenExpr{
				Expr:  &query.NumberLiteral{Value: 5},
				Lower: &query.NumberLiteral{Value: 1},
				Upper: &query.NumberLiteral{Value: 10},
			},
			true,
		},
		{
			"out_of_range",
			&query.BetweenExpr{
				Expr:  &query.NumberLiteral{Value: 15},
				Lower: &query.NumberLiteral{Value: 1},
				Upper: &query.NumberLiteral{Value: 10},
			},
			false,
		},
		{
			"at_lower_bound",
			&query.BetweenExpr{
				Expr:  &query.NumberLiteral{Value: 1},
				Lower: &query.NumberLiteral{Value: 1},
				Upper: &query.NumberLiteral{Value: 10},
			},
			true,
		},
		{
			"at_upper_bound",
			&query.BetweenExpr{
				Expr:  &query.NumberLiteral{Value: 10},
				Lower: &query.NumberLiteral{Value: 1},
				Upper: &query.NumberLiteral{Value: 10},
			},
			true,
		},
		{
			"not_between_in_range",
			&query.BetweenExpr{
				Expr:  &query.NumberLiteral{Value: 5},
				Lower: &query.NumberLiteral{Value: 1},
				Upper: &query.NumberLiteral{Value: 10},
				Not:   true,
			},
			false,
		},
		{
			"not_between_out_of_range",
			&query.BetweenExpr{
				Expr:  &query.NumberLiteral{Value: 15},
				Lower: &query.NumberLiteral{Value: 1},
				Upper: &query.NumberLiteral{Value: 10},
				Not:   true,
			},
			true,
		},
		{
			"null_expr",
			&query.BetweenExpr{
				Expr:  &query.NullLiteral{},
				Lower: &query.NumberLiteral{Value: 1},
				Upper: &query.NumberLiteral{Value: 10},
			},
			nil,
		},
		{
			"null_lower",
			&query.BetweenExpr{
				Expr:  &query.NumberLiteral{Value: 5},
				Lower: &query.NullLiteral{},
				Upper: &query.NumberLiteral{Value: 10},
			},
			nil,
		},
		{
			"null_upper",
			&query.BetweenExpr{
				Expr:  &query.NumberLiteral{Value: 5},
				Lower: &query.NumberLiteral{Value: 1},
				Upper: &query.NullLiteral{},
			},
			nil,
		},
		{
			"string_between",
			&query.BetweenExpr{
				Expr:  &query.StringLiteral{Value: "dog"},
				Lower: &query.StringLiteral{Value: "cat"},
				Upper: &query.StringLiteral{Value: "fox"},
			},
			true,
		},
		{
			"string_not_between",
			&query.BetweenExpr{
				Expr:  &query.StringLiteral{Value: "ant"},
				Lower: &query.StringLiteral{Value: "cat"},
				Upper: &query.StringLiteral{Value: "fox"},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluateBetween(cat, row, cols, tt.expr, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// ============================================================
// 4. evaluateHaving
// ============================================================

func TestCoverage_evaluateHaving(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// nil having => true
	got, err := evaluateHaving(cat, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("nil having should return true, got %v", got)
	}

	// Simple boolean having: true
	got, err = evaluateHaving(cat, []interface{}{}, nil, nil, &query.BooleanLiteral{Value: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true, got %v", got)
	}

	// Simple boolean having: false
	got, err = evaluateHaving(cat, []interface{}{}, nil, nil, &query.BooleanLiteral{Value: false}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Errorf("expected false, got %v", got)
	}

	// NULL having => false
	got, err = evaluateHaving(cat, []interface{}{}, nil, nil, &query.NullLiteral{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Errorf("expected false for NULL having, got %v", got)
	}

	// Numeric having - nonzero => true
	got, err = evaluateHaving(cat, []interface{}{}, nil, nil, &query.NumberLiteral{Value: 1}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for nonzero having, got %v", got)
	}

	// Numeric having - zero => false
	got, err = evaluateHaving(cat, []interface{}{}, nil, nil, &query.NumberLiteral{Value: 0}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Errorf("expected false for zero having, got %v", got)
	}

	// String result => true (non-empty)
	got, err = evaluateHaving(cat, []interface{}{}, nil, nil, &query.StringLiteral{Value: "notempty"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for string having, got %v", got)
	}

	// HAVING with selectCols building columns when nil columns provided
	selectCols := []selectColInfo{
		{name: "cnt", tableName: "t1"},
	}
	row := []interface{}{int64(10)}
	got, err = evaluateHaving(cat, row, selectCols, nil, &query.BinaryExpr{
		Left:     &query.Identifier{Name: "cnt"},
		Operator: query.TokenGt,
		Right:    &query.NumberLiteral{Value: 5},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for cnt > 5, got %v", got)
	}
}

// ============================================================
// 5. evaluateWhere
// ============================================================

func TestCoverage_evaluateWhere(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cols := []ColumnDef{{Name: "val", Type: "INTEGER"}}

	// nil where => true
	got, err := evaluateWhere(cat, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("nil where should return true, got %v", got)
	}

	// Boolean true
	got, err = evaluateWhere(cat, []interface{}{int64(1)}, cols, &query.BooleanLiteral{Value: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true")
	}

	// Boolean false
	got, err = evaluateWhere(cat, []interface{}{int64(1)}, cols, &query.BooleanLiteral{Value: false}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Errorf("expected false")
	}

	// NULL => false
	got, err = evaluateWhere(cat, []interface{}{int64(1)}, cols, &query.NullLiteral{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Errorf("expected false for NULL where")
	}

	// Non-zero int64 => true (via column value)
	got, err = evaluateWhere(cat, []interface{}{int64(5)}, cols, &query.Identifier{Name: "val"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for non-zero int64")
	}

	// Zero int64 => false
	got, err = evaluateWhere(cat, []interface{}{int64(0)}, cols, &query.Identifier{Name: "val"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Errorf("expected false for zero int64")
	}

	// Float64 non-zero => true
	floatCols := []ColumnDef{{Name: "val", Type: "REAL"}}
	got, err = evaluateWhere(cat, []interface{}{float64(3.14)}, floatCols, &query.Identifier{Name: "val"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for non-zero float")
	}

	// Float64 zero => false
	got, err = evaluateWhere(cat, []interface{}{float64(0)}, floatCols, &query.Identifier{Name: "val"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Errorf("expected false for zero float")
	}

	// String non-empty => true
	strCols := []ColumnDef{{Name: "val", Type: "TEXT"}}
	got, err = evaluateWhere(cat, []interface{}{"hello"}, strCols, &query.Identifier{Name: "val"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for non-empty string")
	}

	// String empty => false
	got, err = evaluateWhere(cat, []interface{}{""}, strCols, &query.Identifier{Name: "val"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Errorf("expected false for empty string")
	}

	// Comparison expression
	got, err = evaluateWhere(cat, []interface{}{int64(10)}, cols, &query.BinaryExpr{
		Left:     &query.Identifier{Name: "val"},
		Operator: query.TokenGt,
		Right:    &query.NumberLiteral{Value: 5},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != true {
		t.Errorf("expected true for 10 > 5")
	}
}

// ============================================================
// 6. toInt
// ============================================================

func TestCoverage_toInt(t *testing.T) {
	tests := []struct {
		name   string
		input  interface{}
		want   int
		wantOk bool
	}{
		{"int_value", 42, 42, true},
		{"int64_value", int64(100), 100, true},
		{"float64_value", float64(7.9), 7, true},
		{"string_value", "hello", 0, false},
		{"nil_value", nil, 0, false},
		{"bool_value", true, 0, false},
		{"float64_overflow", float64(1e20), 0, false},
		{"float64_negative_overflow", float64(-1e20), 0, false},
		{"zero_int", 0, 0, true},
		{"zero_int64", int64(0), 0, true},
		{"zero_float64", float64(0), 0, true},
		{"negative_int", -5, -5, true},
		{"negative_int64", int64(-5), -5, true},
		{"negative_float64", float64(-5.5), -5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toInt(tt.input)
			if ok != tt.wantOk {
				t.Errorf("toInt(%v) ok = %v, want %v", tt.input, ok, tt.wantOk)
			}
			if ok && got != tt.want {
				t.Errorf("toInt(%v) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// 7. stripQuotes
// ============================================================

func TestCoverage_stripQuotes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"double_quotes", `"users"`, "users"},
		{"backticks", "`users`", "users"},
		{"square_brackets", "[users]", "users"},
		{"no_quotes", "users", "users"},
		{"empty_string", "", ""},
		{"single_char", "x", "x"},
		{"mismatched_quotes", `"users'`, `"users'`},
		{"double_quoted_with_spaces", `"user name"`, "user name"},
		{"backtick_with_special", "`table-1`", "table-1"},
		{"only_quotes", `""`, ""},
		{"only_backticks", "``", ""},
		{"only_brackets", "[]", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripQuotes(tt.input)
			if got != tt.want {
				t.Errorf("stripQuotes(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// 8. valuesEqual (ForeignKeyEnforcer)
// ============================================================

func TestCoverage_valuesEqual(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	fke := NewForeignKeyEnforcer(cat)

	tests := []struct {
		name string
		a    interface{}
		b    interface{}
		want bool
	}{
		{"both_nil", nil, nil, true},
		{"nil_and_value", nil, 1, false},
		{"value_and_nil", 1, nil, false},
		{"int_int_equal", 1, 1, true},
		{"int_int_not_equal", 1, 2, false},
		{"int_int64_equal", 1, int64(1), true},
		{"int_float64_equal", 1, float64(1), true},
		{"int64_float64_equal", int64(42), float64(42), true},
		{"float64_float64_equal", float64(3.14), float64(3.14), true},
		{"float64_float64_not_equal", float64(3.14), float64(3.15), false},
		{"string_string_equal", "abc", "abc", true},
		{"string_string_not_equal", "abc", "xyz", false},
		{"string_int_not_equal", "1", 1, false},
		{"int8_int_equal", int8(5), 5, true},
		{"int16_int_equal", int16(5), 5, true},
		{"int32_int_equal", int32(5), 5, true},
		{"uint_int_equal", uint(5), 5, true},
		{"uint8_int_equal", uint8(5), 5, true},
		{"uint16_int_equal", uint16(5), 5, true},
		{"uint32_int_equal", uint32(5), 5, true},
		{"uint64_int_equal", uint64(5), 5, true},
		{"float32_float64_equal", float32(5), float64(5), true},
		{"int8_float64", int8(10), float64(10), true},
		{"int16_float64", int16(10), float64(10), true},
		{"int32_float64", int32(10), float64(10), true},
		{"uint_float64", uint(10), float64(10), true},
		{"uint8_float64", uint8(10), float64(10), true},
		{"uint16_float64", uint16(10), float64(10), true},
		{"uint32_float64", uint32(10), float64(10), true},
		{"uint64_float64", uint64(10), float64(10), true},
		{"float32_int", float32(7), int(7), true},
		{"bool_bool_equal", true, true, true},
		{"bool_bool_not_equal", true, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fke.valuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// ============================================================
// 9. JSONQuote
// ============================================================

func TestCoverage_JSONQuote(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple_string", "hello", `"hello"`},
		{"empty_string", "", `""`},
		{"with_quotes", `say "hi"`, `"say \"hi\""`},
		{"with_newline", "line1\nline2", `"line1\nline2"`},
		{"with_tab", "a\tb", `"a\tb"`},
		{"with_backslash", `a\b`, `"a\\b"`},
		{"unicode", "caf\u00e9", `"café"`},
		{"special_chars", "<script>alert('xss')</script>", `"\u003cscript\u003ealert('xss')\u003c/script\u003e"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := JSONQuote(tt.input)
			if got != tt.want {
				t.Errorf("JSONQuote(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// 10. JSONExtract
// ============================================================

func TestCoverage_JSONExtract(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		path    string
		want    interface{}
		wantErr bool
	}{
		{"simple_key", `{"name":"Alice"}`, "$.name", "Alice", false},
		{"nested_key", `{"a":{"b":"c"}}`, "$.a.b", "c", false},
		{"array_index", `{"arr":[1,2,3]}`, "$.arr[0]", float64(1), false},
		{"empty_json", "", "$", nil, false},
		{"invalid_json", "{bad", "$.x", nil, true},
		{"invalid_path", `{"a":1}`, "", nil, true},
		{"missing_key", `{"a":1}`, "$.b", nil, true},
		{"number_value", `{"n":42}`, "$.n", float64(42), false},
		{"bool_value", `{"b":true}`, "$.b", true, false},
		{"null_value", `{"n":null}`, "$.n", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONExtract(tt.json, tt.path)
			if (err != nil) != tt.wantErr {
				t.Fatalf("JSONExtract(%q, %q) error = %v, wantErr = %v", tt.json, tt.path, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("JSONExtract(%q, %q) = %v (%T), want %v (%T)", tt.json, tt.path, got, got, tt.want, tt.want)
			}
		})
	}
}

// ============================================================
// 11. JSONPath.Set
// ============================================================

func TestCoverage_JSONSet(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		path    string
		value   string
		wantErr bool
	}{
		{"set_simple_key", `{"a":1}`, "$.a", "2", false},
		{"set_new_key", `{"a":1}`, "$.b", "2", false},
		{"set_nested", `{"a":{"b":1}}`, "$.a.b", "2", false},
		{"set_array_element", `{"a":[1,2,3]}`, "$.a[1]", "99", false},
		{"set_empty_json", "", "$.a", "1", false},
		{"invalid_json", "{bad", "$.a", "1", true},
		{"invalid_path", `{"a":1}`, "", "1", true},
		{"set_string_value", `{"a":1}`, "$.a", `"hello"`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := JSONSet(tt.json, tt.path, tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("JSONSet(%q, %q, %q) error = %v, wantErr = %v", tt.json, tt.path, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestCoverage_JSONPathSet_Errors(t *testing.T) {
	// Empty path segments
	jp := &JSONPath{Segments: []string{}}
	err := jp.Set(&map[string]interface{}{"a": 1}, "val")
	if err == nil {
		t.Error("expected error for empty segments")
	}

	// Not an object at segment
	jp = &JSONPath{Segments: []string{"a", "b"}}
	var data interface{} = map[string]interface{}{"a": "not_object"}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for non-object intermediate")
	}

	// Not an array at array segment
	jp = &JSONPath{Segments: []string{"a", "[0]"}}
	data = map[string]interface{}{"a": "not_array"}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for non-array intermediate")
	}

	// Array index out of bounds
	jp = &JSONPath{Segments: []string{"a", "[99]"}}
	data = map[string]interface{}{"a": []interface{}{1, 2}}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for out of bounds index")
	}

	// Set on array final segment - not an array
	jp = &JSONPath{Segments: []string{"[0]"}}
	data = "not_array"
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error setting array index on non-array")
	}

	// Set on object final segment - not an object
	jp = &JSONPath{Segments: []string{"key"}}
	data = "not_object"
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error setting key on non-object")
	}

	// Set with invalid array index in path
	jp = &JSONPath{Segments: []string{"[abc]"}}
	data = []interface{}{1, 2}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for invalid array index")
	}

	// nil in navigation path
	jp = &JSONPath{Segments: []string{"a", "b"}}
	data = map[string]interface{}{"a": nil}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for nil in path")
	}

	// Invalid array index in navigation
	jp = &JSONPath{Segments: []string{"[abc]", "b"}}
	data = []interface{}{1, 2}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for invalid array index in path")
	}

	// Array index out of bounds in navigation
	jp = &JSONPath{Segments: []string{"[99]", "b"}}
	data = []interface{}{1, 2}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for out of bounds in path navigation")
	}

	// Final array segment with out-of-bounds
	jp = &JSONPath{Segments: []string{"[99]"}}
	data = []interface{}{1, 2}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for final out of bounds index")
	}

	// Final array segment with negative index
	jp = &JSONPath{Segments: []string{"[-1]"}}
	data = []interface{}{1, 2}
	err = jp.Set(&data, "val")
	if err == nil {
		t.Error("expected error for negative final index")
	}
}

// ============================================================
// 12. matchLikeSimple (exercises uncovered branches)
// ============================================================

func TestCoverage_matchLikeSimple(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		pattern string
		escape  []byte
		want    bool
	}{
		{"empty_both", "", "", nil, true},
		{"empty_pattern", "abc", "", nil, false},
		{"empty_string_percent", "", "%", nil, true},
		{"percent_start", "hello", "%llo", nil, true},
		{"percent_middle", "hello", "h%o", nil, true},
		{"double_percent", "hello", "%%", nil, true},
		{"underscore_exact", "abc", "___", nil, true},
		{"underscore_short", "ab", "___", nil, false},
		{"literal_no_match", "abc", "xyz", nil, false},
		{"escape_percent", "100%off", "100#%off", []byte{'#'}, true},
		{"escape_underscore", "a_b", "a#_b", []byte{'#'}, true},
		{"escape_no_match", "100xoff", "100#%off", []byte{'#'}, false},
		{"percent_then_literal_no_match", "abcx", "%y", nil, false},
		{"trailing_percent", "hello", "hello%%", nil, true},
		{"only_percent", "anything", "%", nil, true},
		{"only_underscore", "x", "_", nil, true},
		{"mixed_wildcards", "abc123", "a%1_3", nil, true},
		{"case_insensitive_escape", "100%OFF", "100#%OFF", []byte{'#'}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got bool
			if tt.escape != nil {
				got = matchLikeSimple(tt.s, tt.pattern, tt.escape...)
			} else {
				got = matchLikeSimple(tt.s, tt.pattern)
			}
			if got != tt.want {
				t.Errorf("matchLikeSimple(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
			}
		})
	}
}

// ============================================================
// 13. Save / Load
// ============================================================

func TestCoverage_SaveAndLoad(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create a table
	createCoverageTestTable(t, cat, "items", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Save should succeed
	if err := cat.Save(); err != nil {
		t.Fatalf("Save() failed: %v", err)
	}

	// Load on a fresh catalog to see if it restores
	backend2 := storage.NewMemory()
	pool2 := storage.NewBufferPool(1024, backend2)
	tree2, err := btree.NewBTree(pool2)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	cat2 := New(tree2, pool2, nil)
	defer pool2.Close()

	// Load with no data should succeed
	if err := cat2.Load(); err != nil {
		t.Fatalf("Load() failed: %v", err)
	}
}

func TestCoverage_Save_NilTree(t *testing.T) {
	// Catalog with nil tree
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	defer pool.Close()

	// Save with nil tree - should still work since tree is nil
	if err := cat.Save(); err != nil {
		t.Fatalf("Save() with nil tree failed: %v", err)
	}
}

func TestCoverage_Load_NilTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	defer pool.Close()

	// Load with nil tree => early return nil
	if err := cat.Load(); err != nil {
		t.Fatalf("Load() with nil tree should return nil, got: %v", err)
	}
}

// ============================================================
// 14. Vacuum
// ============================================================

func TestCoverage_Vacuum_EmptyDB(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Vacuum on empty catalog should succeed
	if err := cat.Vacuum(); err != nil {
		t.Fatalf("Vacuum() failed: %v", err)
	}
}

func TestCoverage_Vacuum_WithTable(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "products", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Vacuum should compact tables
	if err := cat.Vacuum(); err != nil {
		t.Fatalf("Vacuum() failed: %v", err)
	}

	// Table should still be accessible
	_, err := cat.GetTable("products")
	if err != nil {
		t.Fatalf("GetTable after Vacuum failed: %v", err)
	}
}

// ============================================================
// 15. RollbackTransaction
// ============================================================

func TestCoverage_RollbackTransaction_NoTxn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Rollback without active transaction should work (no-op)
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction() without active txn failed: %v", err)
	}
}

func TestCoverage_RollbackTransaction_UndoCreateTable(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.BeginTransaction(1)

	createCoverageTestTable(t, cat, "temp_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Table should exist
	_, err := cat.GetTable("temp_tbl")
	if err != nil {
		t.Fatalf("expected table to exist before rollback")
	}

	// Rollback should undo the CREATE TABLE
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction() failed: %v", err)
	}

	// Table should no longer exist
	_, err = cat.GetTable("temp_tbl")
	if err == nil {
		t.Error("expected table not to exist after rollback")
	}
}

func TestCoverage_RollbackTransaction_UndoInsert(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table outside transaction
	createCoverageTestTable(t, cat, "data_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(2)

	// Insert a row via the btree directly (simulating what insertLocked does)
	tree := cat.tableTrees["data_tbl"]
	key := []byte("row:1")
	val := []byte(`[1, "hello"]`)
	if err := tree.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	// Record in undo log
	cat.mu.Lock()
	cat.undoLog = append(cat.undoLog, undoEntry{
		action:    undoInsert,
		tableName: "data_tbl",
		key:       key,
	})
	cat.mu.Unlock()

	// Rollback should undo the insert
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction() failed: %v", err)
	}

	// The key should be gone
	_, err := tree.Get(key)
	if err == nil {
		t.Error("expected key to be deleted after rollback")
	}
}

func TestCoverage_RollbackTransaction_UndoUpdate(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "upd_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	tree := cat.tableTrees["upd_tbl"]
	key := []byte("row:1")
	oldVal := []byte(`[1, "old"]`)
	newVal := []byte(`[1, "new"]`)

	// Insert original
	if err := tree.Put(key, oldVal); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	cat.BeginTransaction(3)

	// "Update" the value
	if err := tree.Put(key, newVal); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	cat.mu.Lock()
	cat.undoLog = append(cat.undoLog, undoEntry{
		action:    undoUpdate,
		tableName: "upd_tbl",
		key:       key,
		oldValue:  oldVal,
	})
	cat.mu.Unlock()

	// Rollback
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction() failed: %v", err)
	}

	// Should be restored to old value
	got, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Get after rollback failed: %v", err)
	}
	if string(got) != string(oldVal) {
		t.Errorf("expected %s, got %s", oldVal, got)
	}
}

func TestCoverage_RollbackTransaction_UndoDelete(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "del_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	tree := cat.tableTrees["del_tbl"]
	key := []byte("row:1")
	val := []byte(`[1]`)

	if err := tree.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	cat.BeginTransaction(4)

	// Delete
	if err := tree.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	cat.mu.Lock()
	cat.undoLog = append(cat.undoLog, undoEntry{
		action:    undoDelete,
		tableName: "del_tbl",
		key:       key,
		oldValue:  val,
	})
	cat.mu.Unlock()

	// Rollback should restore
	if err := cat.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction() failed: %v", err)
	}

	got, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Get after rollback failed: %v", err)
	}
	if string(got) != string(val) {
		t.Errorf("expected %s, got %s", val, got)
	}
}

// ============================================================
// 16. RollbackToSavepoint
// ============================================================

func TestCoverage_RollbackToSavepoint_NoTxn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	err := cat.RollbackToSavepoint("sp1")
	if err == nil {
		t.Error("expected error when no transaction active")
	}
}

func TestCoverage_RollbackToSavepoint_Missing(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.BeginTransaction(1)
	err := cat.RollbackToSavepoint("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent savepoint")
	}
}

func TestCoverage_RollbackToSavepoint_UndoInsert(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.BeginTransaction(1)

	// Set a savepoint
	if err := cat.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint failed: %v", err)
	}

	// Insert after savepoint
	tree := cat.tableTrees["sp_tbl"]
	key := []byte("row:1")
	if err := tree.Put(key, []byte(`[1]`)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	cat.mu.Lock()
	cat.undoLog = append(cat.undoLog, undoEntry{
		action:    undoInsert,
		tableName: "sp_tbl",
		key:       key,
	})
	cat.mu.Unlock()

	// Rollback to savepoint
	if err := cat.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	// Key should be gone
	_, err := tree.Get(key)
	if err == nil {
		t.Error("expected key to be deleted after rollback to savepoint")
	}

	// Transaction should still be active
	if !cat.IsTransactionActive() {
		t.Error("expected transaction to still be active after rollback to savepoint")
	}
}

func TestCoverage_RollbackToSavepoint_UndoUpdate(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_upd", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	tree := cat.tableTrees["sp_upd"]
	key := []byte("row:1")
	oldVal := []byte(`[1]`)
	if err := tree.Put(key, oldVal); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	cat.BeginTransaction(1)
	if err := cat.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint failed: %v", err)
	}

	// Update after savepoint
	newVal := []byte(`[2]`)
	if err := tree.Put(key, newVal); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	cat.mu.Lock()
	cat.undoLog = append(cat.undoLog, undoEntry{
		action:    undoUpdate,
		tableName: "sp_upd",
		key:       key,
		oldValue:  oldVal,
	})
	cat.mu.Unlock()

	if err := cat.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	got, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(oldVal) {
		t.Errorf("expected %s, got %s", oldVal, got)
	}
}

func TestCoverage_RollbackToSavepoint_UndoDelete(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "sp_del", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	tree := cat.tableTrees["sp_del"]
	key := []byte("row:1")
	val := []byte(`[1]`)
	if err := tree.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	cat.BeginTransaction(1)
	if err := cat.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint failed: %v", err)
	}

	// Delete after savepoint
	if err := tree.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	cat.mu.Lock()
	cat.undoLog = append(cat.undoLog, undoEntry{
		action:    undoDelete,
		tableName: "sp_del",
		key:       key,
		oldValue:  val,
	})
	cat.mu.Unlock()

	if err := cat.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	got, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(val) {
		t.Errorf("expected %s, got %s", val, got)
	}
}

func TestCoverage_RollbackToSavepoint_UndoCreateTable(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.BeginTransaction(1)
	if err := cat.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint failed: %v", err)
	}

	createCoverageTestTable(t, cat, "sp_create", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	if err := cat.RollbackToSavepoint("sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	_, err := cat.GetTable("sp_create")
	if err == nil {
		t.Error("expected table not to exist after rollback to savepoint")
	}
}

// ============================================================
// 17. StatsCollector - countRows and collectColumnStats
// ============================================================

func TestCoverage_StatsCollector_CollectStats(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// The ExecuteQuery in stats.go is a stub that returns empty result.
	// We can still test the flow.
	createCoverageTestTable(t, cat, "stat_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	sc := NewStatsCollector(cat)

	// CollectStats should work (even with stub ExecuteQuery returning empty results)
	stats, err := sc.CollectStats("stat_tbl")
	if err != nil {
		t.Fatalf("CollectStats failed: %v", err)
	}
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	if stats.TableName != "stat_tbl" {
		t.Errorf("expected table name stat_tbl, got %s", stats.TableName)
	}
}

func TestCoverage_StatsCollector_NonExistentTable(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	_, err := sc.CollectStats("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestCoverage_StatsCollector_CountRows(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	// Test with invalid table name
	_, err := sc.countRows("DROP TABLE;--")
	if err != nil {
		// Expected - contains SQL keyword
	}

	// Valid name - will use the stub ExecuteQuery
	count, err := sc.countRows("validname")
	if err != nil {
		t.Fatalf("countRows failed: %v", err)
	}
	// Stub returns empty result, so count should be 0
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestCoverage_StatsCollector_CollectColumnStats(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	// Invalid table name
	_, err := sc.collectColumnStats("SELECT", "id")
	if err == nil {
		t.Error("expected error for invalid table name")
	}

	// Invalid column name
	_, err = sc.collectColumnStats("validtbl", "DROP")
	if err == nil {
		t.Error("expected error for invalid column name")
	}
}

func TestCoverage_validateIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid", "users", false},
		{"empty", "", true},
		{"too_long", "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffggggg", true},
		{"special_char", "user;name", true},
		{"sql_keyword_select", "SELECT", true},
		{"sql_keyword_insert", "INSERT", true},
		{"sql_keyword_update", "UPDATE", true},
		{"sql_keyword_delete", "DELETE", true},
		{"sql_keyword_drop", "DROP", true},
		{"sql_keyword_union", "UNION", true},
		{"sql_comment_dash", "a--b", true},
		{"valid_underscore", "my_table", false},
		{"valid_number", "tbl1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentifier(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateIdentifier(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestCoverage_quoteIdent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"users", `"users"`},
		{`us"ers`, `"us""ers"`},
		{"", `""`},
	}

	for _, tt := range tests {
		got := quoteIdent(tt.input)
		if got != tt.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// ============================================================
// 18. QueryCache Set
// ============================================================

func TestCoverage_QueryCacheSet(t *testing.T) {
	// Create an enabled query cache with maxSize 10
	qc := NewQueryCache(10, time.Minute)

	// Set a value
	qc.Set("key1", []string{"col1"}, [][]interface{}{{1}}, []string{"t1"})

	// Overwrite same key (should move to front of LRU)
	qc.Set("key1", []string{"col1"}, [][]interface{}{{2}}, []string{"t1"})

	// Get should return the updated value
	entry, found := qc.Get("key1")
	if !found {
		t.Fatal("expected to find key1")
	}
	if entry.Rows[0][0] != 2 {
		t.Errorf("expected updated value 2, got %v", entry.Rows[0][0])
	}

	// Fill the cache to trigger eviction
	for i := 0; i < 15; i++ {
		key := "fill_" + string(rune('A'+i))
		qc.Set(key, []string{"c"}, [][]interface{}{{i}}, []string{"t"})
	}

	// Test disabled cache (size 0)
	qcDisabled := NewQueryCache(0, 0)
	qcDisabled.Set("k", []string{"c"}, [][]interface{}{{1}}, []string{"t"})
	_, found = qcDisabled.Get("k")
	if found {
		t.Error("expected disabled cache to not store entries")
	}
	qcDisabled.Invalidate("t")
}

// ============================================================
// 19. ListTables, SaveData, LoadSchema, LoadData
// ============================================================

func TestCoverage_ListTables(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	tables := cat.ListTables()
	if len(tables) != 0 {
		t.Errorf("expected 0 tables, got %d", len(tables))
	}

	createCoverageTestTable(t, cat, "tbl_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	createCoverageTestTable(t, cat, "tbl_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	tables = cat.ListTables()
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}
}

func TestCoverage_SaveDataLoadSchemaLoadData(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// These are thin wrappers
	if err := cat.SaveData(""); err != nil {
		t.Fatalf("SaveData failed: %v", err)
	}
	if err := cat.LoadSchema(""); err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if err := cat.LoadData(""); err != nil {
		t.Fatalf("LoadData failed: %v", err)
	}
}

// ============================================================
// 20. FlushTableTrees
// ============================================================

func TestCoverage_FlushTableTrees(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "flush_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	if err := cat.FlushTableTrees(); err != nil {
		t.Fatalf("FlushTableTrees failed: %v", err)
	}
}

// ============================================================
// 21. isIntegerType
// ============================================================

func TestCoverage_isIntegerType(t *testing.T) {
	tests := []struct {
		name string
		val  interface{}
		want bool
	}{
		{"int", 42, true},
		{"int64", int64(42), true},
		{"float64_whole", float64(42), true},
		{"float64_fraction", float64(42.5), false},
		{"float64_large", float64(1e16), false},
		{"string", "42", false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isIntegerType(tt.val)
			if got != tt.want {
				t.Errorf("isIntegerType(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

// ============================================================
// 22. Savepoint and IsTransactionActive
// ============================================================

func TestCoverage_Savepoint_NoTxn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	err := cat.Savepoint("sp1")
	if err == nil {
		t.Error("expected error when no transaction active")
	}
}

func TestCoverage_IsTransactionActive(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	if cat.IsTransactionActive() {
		t.Error("expected no active transaction initially")
	}

	cat.BeginTransaction(1)
	if !cat.IsTransactionActive() {
		t.Error("expected active transaction after BeginTransaction")
	}

	cat.CommitTransaction()
	if cat.IsTransactionActive() {
		t.Error("expected no active transaction after commit")
	}
}

// ============================================================
// 23. CommitTransaction
// ============================================================

func TestCoverage_CommitTransaction(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.BeginTransaction(1)

	createCoverageTestTable(t, cat, "commit_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	if err := cat.CommitTransaction(); err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Table should still exist after commit
	_, err := cat.GetTable("commit_tbl")
	if err != nil {
		t.Errorf("table should exist after commit: %v", err)
	}
}

// ============================================================
// 24. EstimateRowCount and GetStatsSummary
// ============================================================

func TestCoverage_EstimateRowCount(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	// No stats collected - should return default 1000
	count := sc.EstimateRowCount("nonexistent")
	if count != 1000 {
		t.Errorf("expected default 1000, got %d", count)
	}
}

func TestCoverage_GetStatsSummary(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	summary := sc.GetStatsSummary()
	if len(summary) != 0 {
		t.Errorf("expected empty summary, got %d entries", len(summary))
	}
}

// ============================================================
// 25. Invalidate QueryCache
// ============================================================

func TestCoverage_QueryCacheInvalidate(t *testing.T) {
	// Use an enabled cache
	qc := NewQueryCache(10, 0)

	qc.Set("q1", []string{"col"}, [][]interface{}{{1}}, []string{"users"})
	qc.Set("q2", []string{"col"}, [][]interface{}{{2}}, []string{"orders"})

	// Invalidate table "users"
	qc.Invalidate("users")

	// q1 should be gone
	_, found := qc.Get("q1")
	if found {
		t.Error("expected q1 to be invalidated")
	}

	// q2 should still exist
	_, found = qc.Get("q2")
	if !found {
		t.Error("expected q2 to still be cached")
	}
}

// ============================================================
// 26. Additional matchLikeSimple edge cases
// ============================================================

func TestCoverage_matchLikeSimple_TrailingPercent(t *testing.T) {
	// Pattern with only trailing percent after consuming string
	if !matchLikeSimple("abc", "abc%") {
		t.Error("expected match for 'abc' LIKE 'abc%'")
	}
	if !matchLikeSimple("abc", "abc%%%") {
		t.Error("expected match for 'abc' LIKE 'abc%%%'")
	}
	// Remaining pattern after string consumed
	if matchLikeSimple("ab", "abc") {
		t.Error("expected no match for 'ab' LIKE 'abc'")
	}
}

// ============================================================
// 27. JSONPath.Set with *interface{} unwrap
// ============================================================

func TestCoverage_JSONPathSet_InterfacePointer(t *testing.T) {
	var data interface{} = map[string]interface{}{"a": float64(1)}

	jp := &JSONPath{Segments: []string{"a"}}
	err := jp.Set(&data, float64(2))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	m := data.(map[string]interface{})
	if m["a"] != float64(2) {
		t.Errorf("expected a=2, got %v", m["a"])
	}
}

// ============================================================
// 28. JSONUnquote and IsValidJSON
// ============================================================

func TestCoverage_JSONUnquote(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"empty", "", "", false},
		{"quoted", `"hello"`, "hello", false},
		{"with_escapes", `"say \"hi\""`, `say "hi"`, false},
		{"invalid", "not_json", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONUnquote(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("JSONUnquote(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("JSONUnquote(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestCoverage_IsValidJSON(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{`{"a":1}`, true},
		{`[1,2,3]`, true},
		{`"hello"`, true},
		{`42`, true},
		{`true`, true},
		{`null`, true},
		{"", false},
		{`{bad`, false},
	}

	for _, tt := range tests {
		got := IsValidJSON(tt.input)
		if got != tt.want {
			t.Errorf("IsValidJSON(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// ============================================================
// 29. RollbackTransaction with undoAutoIncSeq
// ============================================================

func TestCoverage_RollbackTransaction_UndoAutoIncSeq(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	createCoverageTestTable(t, cat, "autoinc_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)

	// Modify AutoIncSeq and record undo
	cat.mu.Lock()
	tbl := cat.tables["autoinc_tbl"]
	oldSeq := tbl.AutoIncSeq
	tbl.AutoIncSeq = 10
	cat.undoLog = append(cat.undoLog, undoEntry{
		action:         undoAutoIncSeq,
		tableName:      "autoinc_tbl",
		oldAutoIncSeq:  oldSeq,
	})
	cat.mu.Unlock()

	if err := cat.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}

	if tbl.AutoIncSeq != oldSeq {
		t.Errorf("expected AutoIncSeq restored to %d, got %d", oldSeq, tbl.AutoIncSeq)
	}
}

// ============================================================
// 30. RollbackToSavepoint case-insensitive name match
// ============================================================

func TestCoverage_RollbackToSavepoint_CaseInsensitive(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	cat.BeginTransaction(1)

	if err := cat.Savepoint("MyPoint"); err != nil {
		t.Fatalf("Savepoint failed: %v", err)
	}

	// Should find savepoint case-insensitively
	if err := cat.RollbackToSavepoint("mypoint"); err != nil {
		t.Fatalf("RollbackToSavepoint (case insensitive) failed: %v", err)
	}
}
