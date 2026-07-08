package catalog

import (
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ---------------------------------------------------------------------------
// applyInterval / addMonthsClamped / intervalUnitHasTime / applyIntervalToValue
// ---------------------------------------------------------------------------

func TestApplyInterval_Microsecond(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 45, 123456, time.UTC)
	iv := query.IntervalValue{N: 500, Unit: "MICROSECOND"}
	got := applyInterval(base, iv, 1)
	want := base.Add(500 * time.Microsecond)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+500us) = %v, want %v", got, want)
	}
	// negative sign
	got2 := applyInterval(base, iv, -1)
	want2 := base.Add(-500 * time.Microsecond)
	if !got2.Equal(want2) {
		t.Errorf("applyInterval(-500us) = %v, want %v", got2, want2)
	}
}

func TestApplyInterval_Second(t *testing.T) {
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 30, Unit: "SECOND"}
	got := applyInterval(base, iv, 1)
	want := base.Add(30 * time.Second)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+30s) = %v, want %v", got, want)
	}
}

func TestApplyInterval_Minute(t *testing.T) {
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 15, Unit: "MINUTE"}
	got := applyInterval(base, iv, 1)
	want := base.Add(15 * time.Minute)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+15m) = %v, want %v", got, want)
	}
}

func TestApplyInterval_Hour(t *testing.T) {
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 3, Unit: "HOUR"}
	got := applyInterval(base, iv, 1)
	want := base.Add(3 * time.Hour)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+3h) = %v, want %v", got, want)
	}
}

func TestApplyInterval_Day(t *testing.T) {
	base := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 10, Unit: "DAY"}
	got := applyInterval(base, iv, 1)
	want := base.AddDate(0, 0, 10)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+10d) = %v, want %v", got, want)
	}
}

func TestApplyInterval_Week(t *testing.T) {
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 2, Unit: "WEEK"}
	got := applyInterval(base, iv, 1)
	want := base.AddDate(0, 0, 14)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+2w) = %v, want %v", got, want)
	}
}

func TestApplyInterval_Month(t *testing.T) {
	base := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 1, Unit: "MONTH"}
	got := applyInterval(base, iv, 1)
	// Jan 31 + 1 month clamped to Feb 29 (2024 is leap)
	want := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+1 month from Jan 31) = %v, want %v", got, want)
	}
}

func TestApplyInterval_Quarter(t *testing.T) {
	base := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 1, Unit: "QUARTER"}
	got := applyInterval(base, iv, 1)
	want := base.AddDate(0, 3, 0)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+1 quarter) = %v, want %v", got, want)
	}
}

func TestApplyInterval_Year(t *testing.T) {
	base := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 2, Unit: "YEAR"}
	got := applyInterval(base, iv, 1)
	want := base.AddDate(2, 0, 0)
	if !got.Equal(want) {
		t.Errorf("applyInterval(+2 years) = %v, want %v", got, want)
	}
}

func TestApplyInterval_UnknownUnit(t *testing.T) {
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	iv := query.IntervalValue{N: 5, Unit: "FORTNIGHT"}
	got := applyInterval(base, iv, 1)
	if !got.Equal(base) {
		t.Errorf("applyInterval(unknown unit) = %v, want original %v", got, base)
	}
}

func TestAddMonthsClamped_Normal(t *testing.T) {
	// Standard case — no clamping needed
	base := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	got := addMonthsClamped(base, 2) // May 15
	want := time.Date(2024, 5, 15, 10, 30, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("addMonthsClamped(Mar 15, +2) = %v, want %v", got, want)
	}
}

func TestAddMonthsClamped_EndOfMonth(t *testing.T) {
	// Jan 31 + 1 month -> Feb 28/29 (clamped)
	base := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	got := addMonthsClamped(base, 1) // Feb 29 (leap year)
	want := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("addMonthsClamped(Jan 31, +1 in leap) = %v, want %v", got, want)
	}

	// Non-leap year: Mar 31 + 1 month -> Apr 30
	base2 := time.Date(2023, 3, 31, 0, 0, 0, 0, time.UTC)
	got2 := addMonthsClamped(base2, 1)
	want2 := time.Date(2023, 4, 30, 0, 0, 0, 0, time.UTC)
	if !got2.Equal(want2) {
		t.Errorf("addMonthsClamped(Mar 31, +1 in non-leap) = %v, want %v", got2, want2)
	}
}

func TestAddMonthsClamped_WrapYear(t *testing.T) {
	base := time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC)
	got := addMonthsClamped(base, 4) // Feb 28/29 next year
	want := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("addMonthsClamped(Oct 31, +4) = %v, want %v", got, want)
	}
}

func TestAddMonthsClamped_Negative(t *testing.T) {
	// Mar 31 - 1 month -> Feb 28/29 (clamped)
	base := time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC)
	got := addMonthsClamped(base, -1)
	want := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC) // leap year
	if !got.Equal(want) {
		t.Errorf("addMonthsClamped(Mar 31, -1 in leap) = %v, want %v", got, want)
	}
}

func TestAddMonthsClamped_NegativeWraps(t *testing.T) {
	// Jan 15 - 2 months -> Nov 15 previous year
	base := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	got := addMonthsClamped(base, -2)
	want := time.Date(2023, 11, 15, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("addMonthsClamped(Jan 15, -2) = %v, want %v", got, want)
	}
}

func TestAddMonthsClamped_ZeroMonths(t *testing.T) {
	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	got := addMonthsClamped(base, 0)
	if !got.Equal(base) {
		t.Errorf("addMonthsClamped(+0) = %v, want %v", got, base)
	}
}

func TestIntervalUnitHasTime(t *testing.T) {
	tests := []struct {
		unit string
		want bool
	}{
		{"MICROSECOND", true},
		{"SECOND", true},
		{"MINUTE", true},
		{"HOUR", true},
		{"DAY", false},
		{"WEEK", false},
		{"MONTH", false},
		{"QUARTER", false},
		{"YEAR", false},
		{"UNKNOWN", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.unit, func(t *testing.T) {
			got := intervalUnitHasTime(tt.unit)
			if got != tt.want {
				t.Errorf("intervalUnitHasTime(%q) = %v, want %v", tt.unit, got, tt.want)
			}
		})
	}
}

func TestApplyIntervalToValue_DatePlusInterval(t *testing.T) {
	// date-only + INTERVAL DAY -> date-only result
	iv := query.IntervalValue{N: 5, Unit: "DAY"}
	got, err := applyIntervalToValue("2024-01-15", iv, query.TokenPlus)
	if err != nil {
		t.Fatalf("applyIntervalToValue error: %v", err)
	}
	if got != "2024-01-20" {
		t.Errorf("applyIntervalToValue = %v, want %v", got, "2024-01-20")
	}
}

func TestApplyIntervalToValue_DateMinusInterval(t *testing.T) {
	iv := query.IntervalValue{N: 1, Unit: "MONTH"}
	got, err := applyIntervalToValue("2024-03-31", iv, query.TokenMinus)
	if err != nil {
		t.Fatalf("applyIntervalToValue error: %v", err)
	}
	if got != "2024-02-29" {
		t.Errorf("applyIntervalToValue = %v, want %v", got, "2024-02-29")
	}
}

func TestApplyIntervalToValue_DateTimePlusTimeUnit(t *testing.T) {
	// datetime + INTERVAL HOUR -> datetime result (with time component)
	iv := query.IntervalValue{N: 3, Unit: "HOUR"}
	got, err := applyIntervalToValue("2024-01-15 10:30:00", iv, query.TokenPlus)
	if err != nil {
		t.Fatalf("applyIntervalToValue error: %v", err)
	}
	want := "2024-01-15 13:30:00"
	if got != want {
		t.Errorf("applyIntervalToValue = %v, want %v", got, want)
	}
}

func TestApplyIntervalToValue_DateOnlyWithTimeUnit(t *testing.T) {
	// date-only + time unit becomes datetime per MySQL semantics
	iv := query.IntervalValue{N: 12, Unit: "HOUR"}
	got, err := applyIntervalToValue("2024-01-15", iv, query.TokenPlus)
	if err != nil {
		t.Fatalf("applyIntervalToValue error: %v", err)
	}
	want := "2024-01-15 12:00:00"
	if got != want {
		t.Errorf("applyIntervalToValue = %v, want %v", got, want)
	}
}

func TestApplyIntervalToValue_NullOnBadDate(t *testing.T) {
	iv := query.IntervalValue{N: 1, Unit: "DAY"}
	got, err := applyIntervalToValue("not-a-date", iv, query.TokenPlus)
	if err != nil {
		t.Fatalf("applyIntervalToValue error: %v", err)
	}
	if got != nil {
		t.Errorf("applyIntervalToValue(bad date) = %v, want nil", got)
	}
}

func TestApplyIntervalToValue_NullOnNilDate(t *testing.T) {
	iv := query.IntervalValue{N: 1, Unit: "DAY"}
	got, err := applyIntervalToValue(nil, iv, query.TokenPlus)
	if err != nil {
		t.Fatalf("applyIntervalToValue error: %v", err)
	}
	if got != nil {
		t.Errorf("applyIntervalToValue(nil) = %v, want nil", got)
	}
}

// ---------------------------------------------------------------------------
// systemVariableValue
// ---------------------------------------------------------------------------

func TestSystemVariableValue(t *testing.T) {
	tests := []struct {
		name string
		want interface{}
	}{
		{"@@version", "5.7.0-CobaltDB"},
		{"@@version_comment", "CobaltDB"},
		{"@@autocommit", int64(1)},
		{"@@sql_mode", ""},
		{"@@max_allowed_packet", int64(67108864)},
		{"@@character_set_client", "utf8mb4"},
		{"@@character_set_connection", "utf8mb4"},
		{"@@character_set_results", "utf8mb4"},
		{"@@character_set_server", "utf8mb4"},
		{"@@character_set_database", "utf8mb4"},
		{"@@collation_connection", "utf8mb4_general_ci"},
		{"@@collation_server", "utf8mb4_general_ci"},
		{"@@collation_database", "utf8mb4_general_ci"},
		{"@@time_zone", "SYSTEM"},
		{"@@tx_isolation", "READ-COMMITTED"},
		{"@@transaction_isolation", "READ-COMMITTED"},
		{"@@lower_case_table_names", int64(0)},
		{"@@wait_timeout", int64(28800)},
		{"@@interactive_timeout", int64(28800)},
		// @ prefix (user variable vs system variable)
		{"@version", "5.7.0-CobaltDB"},
		{"@@nonexistent_var", nil},
		{"@nonexistent", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := systemVariableValue(tt.name)
			if got != tt.want {
				t.Errorf("systemVariableValue(%q) = %v (%T), want %v (%T)", tt.name, got, got, tt.want, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// evalRegexpLikeValue
// ---------------------------------------------------------------------------

func TestEvalRegexpLikeValue_Match(t *testing.T) {
	got, err := evalRegexpLikeValue([]interface{}{"hello world", "hello"})
	if err != nil {
		t.Fatalf("evalRegexpLikeValue error: %v", err)
	}
	if got != true {
		t.Errorf("evalRegexpLikeValue = %v, want true", got)
	}
}

func TestEvalRegexpLikeValue_NoMatch(t *testing.T) {
	got, err := evalRegexpLikeValue([]interface{}{"hello world", "^bye"})
	if err != nil {
		t.Fatalf("evalRegexpLikeValue error: %v", err)
	}
	if got != false {
		t.Errorf("evalRegexpLikeValue = %v, want false", got)
	}
}

func TestEvalRegexpLikeValue_RegexSyntax(t *testing.T) {
	got, err := evalRegexpLikeValue([]interface{}{"abc123", "[0-9]+"})
	if err != nil {
		t.Fatalf("evalRegexpLikeValue error: %v", err)
	}
	if got != true {
		t.Errorf("evalRegexpLikeValue = %v, want true", got)
	}
}

func TestEvalRegexpLikeValue_NilInput(t *testing.T) {
	got, err := evalRegexpLikeValue([]interface{}{nil, "pattern"})
	if err != nil {
		t.Fatalf("evalRegexpLikeValue error: %v", err)
	}
	if got != nil {
		t.Errorf("evalRegexpLikeValue(nil str) = %v, want nil", got)
	}
}

func TestEvalRegexpLikeValue_NilPattern(t *testing.T) {
	got, err := evalRegexpLikeValue([]interface{}{"hello", nil})
	if err != nil {
		t.Fatalf("evalRegexpLikeValue error: %v", err)
	}
	if got != nil {
		t.Errorf("evalRegexpLikeValue(nil pattern) = %v, want nil", got)
	}
}

func TestEvalRegexpLikeValue_TooFewArgs(t *testing.T) {
	_, err := evalRegexpLikeValue([]interface{}{"only-one"})
	if err == nil {
		t.Fatal("evalRegexpLikeValue with 1 arg should error")
	}
}

func TestEvalRegexpLikeValue_InvalidPattern(t *testing.T) {
	_, err := evalRegexpLikeValue([]interface{}{"hello", "[invalid"})
	if err == nil {
		t.Fatal("evalRegexpLikeValue with bad pattern should error")
	}
}

// ---------------------------------------------------------------------------
// EvalJSONPath
// ---------------------------------------------------------------------------

func TestEvalJSONPath_Valid(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONPath(`{"a": 42}`, "$.a", false)
	if err != nil {
		t.Fatalf("EvalJSONPath error: %v", err)
	}
	if got != float64(42) {
		t.Errorf("EvalJSONPath = %v (%T), want float64(42)", got, got)
	}
}

func TestEvalJSONPath_AsText(t *testing.T) {
	ctx := &EvalContext{}
	// When asText=true, a string result is returned as-is; a non-string is
	// stringified.
	got, err := ctx.EvalJSONPath(`{"name": "alice"}`, "$.name", true)
	if err != nil {
		t.Fatalf("EvalJSONPath error: %v", err)
	}
	if got != "alice" {
		t.Errorf("EvalJSONPath(asText) = %v, want %q", got, "alice")
	}
}

func TestEvalJSONPath_AsTextNumeric(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONPath(`{"val": 42}`, "$.val", true)
	if err != nil {
		t.Fatalf("EvalJSONPath error: %v", err)
	}
	if got != "42" {
		t.Errorf("EvalJSONPath(asText num) = %v, want %q", got, "42")
	}
}

func TestEvalJSONPath_NullResult(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONPath(`{"a": 1}`, "$.nonexistent", false)
	if err != nil {
		t.Fatalf("EvalJSONPath error: %v", err)
	}
	if got != nil {
		t.Errorf("EvalJSONPath(missing) = %v, want nil", got)
	}
}

func TestEvalJSONPath_NonStringJSON(t *testing.T) {
	ctx := &EvalContext{}
	// If jsonVal can't be converted to a string, returns nil
	got, err := ctx.EvalJSONPath(int64(42), "$.a", false)
	if err != nil {
		t.Fatalf("EvalJSONPath error: %v", err)
	}
	if got != nil {
		t.Errorf("EvalJSONPath(non-string) = %v, want nil", got)
	}
}

func TestEvalJSONPath_NilJSON(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONPath(nil, "$.a", false)
	if err != nil {
		t.Fatalf("EvalJSONPath error: %v", err)
	}
	if got != nil {
		t.Errorf("EvalJSONPath(nil) = %v, want nil", got)
	}
}

// ---------------------------------------------------------------------------
// EvalJSONContains
// ---------------------------------------------------------------------------

func TestEvalJSONContains_StringSubstring(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONContains(`{"a":1, "b":2}`, `"b"`)
	if err != nil {
		t.Fatalf("EvalJSONContains error: %v", err)
	}
	if got != true {
		t.Errorf("EvalJSONContains = %v, want true", got)
	}
}

func TestEvalJSONContains_NotContained(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONContains(`{"a":1}`, `"z"`)
	if err != nil {
		t.Fatalf("EvalJSONContains error: %v", err)
	}
	if got != false {
		t.Errorf("EvalJSONContains = %v, want false", got)
	}
}

func TestEvalJSONContains_NilJSON(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONContains(nil, `"a"`)
	if err != nil {
		t.Fatalf("EvalJSONContains error: %v", err)
	}
	if got != false {
		t.Errorf("EvalJSONContains(nil json) = %v, want false", got)
	}
}

func TestEvalJSONContains_NilVal(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONContains(`{"a":1}`, nil)
	if err != nil {
		t.Fatalf("EvalJSONContains error: %v", err)
	}
	if got != false {
		t.Errorf("EvalJSONContains(nil val) = %v, want false", got)
	}
}

func TestEvalJSONContains_NonStringJSON(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalJSONContains(int64(42), `"a"`)
	if err != nil {
		t.Fatalf("EvalJSONContains error: %v", err)
	}
	if got != false {
		t.Errorf("EvalJSONContains(non-string) = %v, want false", got)
	}
}

// ---------------------------------------------------------------------------
// EvalMatch
// ---------------------------------------------------------------------------

func TestEvalMatch_NoMatchingFTSIndex(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	ctx := NewEvalContext(c, []interface{}{"hello world"}, []ColumnDef{{Name: "content", Type: "TEXT"}}, nil)
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.StringLiteral{Value: "hello"},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := ctx.EvalMatch(expr, nil)
	if err != nil {
		t.Fatalf("EvalMatch error: %v", err)
	}
	if got != true {
		t.Errorf("EvalMatch = %v, want true (substring fallback)", got)
	}
}

func TestEvalMatch_NoMatch(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	ctx := NewEvalContext(c, []interface{}{"hello world"}, []ColumnDef{{Name: "content", Type: "TEXT"}}, nil)
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.StringLiteral{Value: "xyzzy"},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := ctx.EvalMatch(expr, nil)
	if err != nil {
		t.Fatalf("EvalMatch error: %v", err)
	}
	if got != false {
		t.Errorf("EvalMatch = %v, want false", got)
	}
}

func TestEvalMatch_NilPattern(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	ctx := NewEvalContext(c, []interface{}{"hello world"}, []ColumnDef{{Name: "content", Type: "TEXT"}}, nil)
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.NullLiteral{},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := ctx.EvalMatch(expr, nil)
	if err != nil {
		t.Fatalf("EvalMatch error: %v", err)
	}
	if got != false {
		t.Errorf("EvalMatch(nil pattern) = %v, want false", got)
	}
}

func TestEvalMatch_WithFTSIndex(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	c.mu.Lock()
	c.ftsIndexes["test_fts"] = &FTSIndexDef{
		Name:      "test_fts",
		TableName: "test",
		Columns:   []string{"title", "body"},
		Index:     map[string][]int64{},
	}
	c.mu.Unlock()

	row := []interface{}{"hello world", "this is the body content"}
	columns := []ColumnDef{
		{Name: "title", Type: "TEXT"},
		{Name: "body", Type: "TEXT"},
	}

	ctx := NewEvalContext(c, row, columns, nil)
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "title"},
			&query.Identifier{Name: "body"},
		},
		Pattern: &query.StringLiteral{Value: "hello content"},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := ctx.EvalMatch(expr, nil)
	if err != nil {
		t.Fatalf("EvalMatch with FTS index error: %v", err)
	}
	if got != true {
		t.Errorf("EvalMatch with FTS = %v, want true", got)
	}
}

func TestEvalMatch_FTSIndexColumnMismatch(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	c.mu.Lock()
	c.ftsIndexes["test_fts"] = &FTSIndexDef{
		Name:      "test_fts",
		TableName: "test",
		Columns:   []string{"title", "body"},
		Index:     map[string][]int64{},
	}
	c.mu.Unlock()

	row := []interface{}{"hello world", "other text"}
	columns := []ColumnDef{
		{Name: "title", Type: "TEXT"},
		{Name: "other_col", Type: "TEXT"},
	}

	ctx := NewEvalContext(c, row, columns, nil)
	// The MATCH only mentions "title" (different column count than FTS index)
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "title"},
		},
		Pattern: &query.StringLiteral{Value: "hello"},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := ctx.EvalMatch(expr, nil)
	if err != nil {
		t.Fatalf("EvalMatch error: %v", err)
	}
	// Falls through to substring search: "hello" is in "hello world"
	if got != true {
		t.Errorf("EvalMatch (column mismatch fallback) = %v, want true", got)
	}
}

// ---------------------------------------------------------------------------
// EvalStar
// ---------------------------------------------------------------------------

func TestEvalStar(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalStar("")
	if err == nil {
		t.Fatal("EvalStar should return an error")
	}
	if got != nil {
		t.Errorf("EvalStar = %v, want nil", got)
	}
	if !strings.Contains(err.Error(), "star expression") {
		t.Errorf("EvalStar error = %v, want 'star expression'", err)
	}
}

func TestEvalStar_WithTable(t *testing.T) {
	ctx := &EvalContext{}
	got, err := ctx.EvalStar("t1")
	if err == nil {
		t.Fatal("EvalStar should return an error even with table qualifier")
	}
	if got != nil {
		t.Errorf("EvalStar = %v, want nil", got)
	}
	if !strings.Contains(err.Error(), "star expression") {
		t.Errorf("EvalStar error = %v, want 'star expression'", err)
	}
}

// ---------------------------------------------------------------------------
// EvalWindow
// ---------------------------------------------------------------------------

func TestEvalWindow_NilWindowValues(t *testing.T) {
	ctx := &EvalContext{}
	w := &query.WindowExpr{
		Function: "ROW_NUMBER",
	}
	got, err := ctx.EvalWindow(w)
	if err != nil {
		t.Fatalf("EvalWindow error: %v", err)
	}
	if got != nil {
		t.Errorf("EvalWindow (no windowValues) = %v, want nil", got)
	}
}

func TestEvalWindow_FoundInMap(t *testing.T) {
	w := &query.WindowExpr{
		Function: "ROW_NUMBER",
	}
	ctx := &EvalContext{
		windowValues: map[*query.WindowExpr]interface{}{
			w: int64(3),
		},
	}
	got, err := ctx.EvalWindow(w)
	if err != nil {
		t.Fatalf("EvalWindow error: %v", err)
	}
	if got != int64(3) {
		t.Errorf("EvalWindow = %v, want int64(3)", got)
	}
}

func TestEvalWindow_NotFoundInMap(t *testing.T) {
	w1 := &query.WindowExpr{Function: "ROW_NUMBER"}
	w2 := &query.WindowExpr{Function: "RANK"}
	ctx := &EvalContext{
		windowValues: map[*query.WindowExpr]interface{}{
			w1: int64(1),
		},
	}
	got, err := ctx.EvalWindow(w2)
	if err != nil {
		t.Fatalf("EvalWindow error: %v", err)
	}
	if got != nil {
		t.Errorf("EvalWindow (not in map) = %v, want nil", got)
	}
}

// ---------------------------------------------------------------------------
// toStringS
// ---------------------------------------------------------------------------

func TestToStringS_String(t *testing.T) {
	got := toStringS("hello")
	if got != "hello" {
		t.Errorf("toStringS(string) = %q, want %q", got, "hello")
	}
}

func TestToStringS_Int(t *testing.T) {
	// toString only handles string/*string/StringBox; non-string types return ""
	got := toStringS(int64(42))
	if got != "" {
		t.Errorf("toStringS(int64) = %q, want %q", got, "")
	}
}

func TestToStringS_Float(t *testing.T) {
	got := toStringS(3.14)
	if got != "" {
		t.Errorf("toStringS(float64) = %q, want %q", got, "")
	}
}

func TestToStringS_Bool(t *testing.T) {
	got := toStringS(true)
	if got != "" {
		t.Errorf("toStringS(bool) = %q, want %q", got, "")
	}
}

func TestToStringS_StringBox(t *testing.T) {
	s := "hello"
	sb := StringBox{ptr: &s}
	got := toStringS(sb)
	if got != "hello" {
		t.Errorf("toStringS(StringBox) = %q, want %q", got, "hello")
	}
}

func TestToStringS_Nil(t *testing.T) {
	got := toStringS(nil)
	if got != "" {
		t.Errorf("toStringS(nil) = %q, want %q", got, "")
	}
}

// ---------------------------------------------------------------------------
// bitwiseOp
// ---------------------------------------------------------------------------

func TestBitwiseOp_And(t *testing.T) {
	got, err := bitwiseOp(int64(6), int64(4), query.TokenBitAnd)
	if err != nil {
		t.Fatalf("bitwiseOp AND error: %v", err)
	}
	if got != int64(4) { // 6 & 4 = 4
		t.Errorf("bitwiseOp AND = %v, want 4", got)
	}
}

func TestBitwiseOp_Or(t *testing.T) {
	got, err := bitwiseOp(int64(2), int64(4), query.TokenBitOr)
	if err != nil {
		t.Fatalf("bitwiseOp OR error: %v", err)
	}
	if got != int64(6) {
		t.Errorf("bitwiseOp OR = %v, want 6", got)
	}
}

func TestBitwiseOp_Xor(t *testing.T) {
	got, err := bitwiseOp(int64(6), int64(4), query.TokenBitXor)
	if err != nil {
		t.Fatalf("bitwiseOp XOR error: %v", err)
	}
	if got != int64(2) { // 6 ^ 4 = 2
		t.Errorf("bitwiseOp XOR = %v, want 2", got)
	}
}

func TestBitwiseOp_ShiftLeft(t *testing.T) {
	got, err := bitwiseOp(int64(3), int64(2), query.TokenShiftLeft)
	if err != nil {
		t.Fatalf("bitwiseOp ShiftLeft error: %v", err)
	}
	if got != int64(12) { // 3 << 2 = 12
		t.Errorf("bitwiseOp ShiftLeft = %v, want 12", got)
	}
}

func TestBitwiseOp_ShiftRight(t *testing.T) {
	got, err := bitwiseOp(int64(16), int64(2), query.TokenShiftRight)
	if err != nil {
		t.Fatalf("bitwiseOp ShiftRight error: %v", err)
	}
	if got != int64(4) { // 16 >> 2 = 4
		t.Errorf("bitwiseOp ShiftRight = %v, want 4", got)
	}
}

func TestBitwiseOp_NonNumericLeft(t *testing.T) {
	_, err := bitwiseOp("abc", int64(4), query.TokenBitAnd)
	if err == nil {
		t.Fatal("bitwiseOp with non-numeric left should error")
	}
}

func TestBitwiseOp_NonNumericRight(t *testing.T) {
	_, err := bitwiseOp(int64(6), "abc", query.TokenBitAnd)
	if err == nil {
		t.Fatal("bitwiseOp with non-numeric right should error")
	}
}

func TestBitwiseOp_NegativeShiftLeft(t *testing.T) {
	_, err := bitwiseOp(int64(8), int64(-1), query.TokenShiftLeft)
	if err == nil {
		t.Fatal("bitwiseOp with negative shift should error")
	}
}

func TestBitwiseOp_NegativeShiftRight(t *testing.T) {
	_, err := bitwiseOp(int64(8), int64(-1), query.TokenShiftRight)
	if err == nil {
		t.Fatal("bitwiseOp with negative shift should error")
	}
}

func TestBitwiseOp_FloatOperands(t *testing.T) {
	// Float operands that can be converted to int64
	got, err := bitwiseOp(float64(6.0), float64(4.0), query.TokenBitAnd)
	if err != nil {
		t.Fatalf("bitwiseOp float AND error: %v", err)
	}
	if got != int64(4) { // 6 & 4 = 4
		t.Errorf("bitwiseOp float AND = %v, want 4", got)
	}
}

// ---------------------------------------------------------------------------
// evaluateMatchExprLocked
// ---------------------------------------------------------------------------

func TestEvaluateMatchExprLocked_NoFTSIndex(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	row := []interface{}{"hello world"}
	columns := []ColumnDef{{Name: "content", Type: "TEXT"}}
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.StringLiteral{Value: "hello"},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := evaluateMatchExprLocked(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateMatchExprLocked error: %v", err)
	}
	if got != true {
		t.Errorf("evaluateMatchExprLocked = %v, want true", got)
	}
}

func TestEvaluateMatchExprLocked_AllWordsRequired(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	row := []interface{}{"hello world from test"}
	columns := []ColumnDef{{Name: "content", Type: "TEXT"}}
	// All words must be present (AND logic)
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.StringLiteral{Value: "hello test"},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := evaluateMatchExprLocked(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateMatchExprLocked error: %v", err)
	}
	if got != true {
		t.Errorf("evaluateMatchExprLocked (all words) = %v, want true", got)
	}

	// Missing one word
	expr2 := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.StringLiteral{Value: "hello missing"},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got2, err := evaluateMatchExprLocked(c, row, columns, expr2, nil)
	if err != nil {
		t.Fatalf("evaluateMatchExprLocked error: %v", err)
	}
	if got2 != false {
		t.Errorf("evaluateMatchExprLocked (missing word) = %v, want false", got2)
	}
}

func TestEvaluateMatchExprLocked_NilRowValues(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	row := []interface{}{nil}
	columns := []ColumnDef{{Name: "content", Type: "TEXT"}}
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.StringLiteral{Value: "hello"},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := evaluateMatchExprLocked(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateMatchExprLocked error: %v", err)
	}
	if got != false {
		t.Errorf("evaluateMatchExprLocked (nil row) = %v, want false", got)
	}
}

func TestEvaluateMatchExprLocked_EmptyPattern(t *testing.T) {
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()

	row := []interface{}{"hello world"}
	columns := []ColumnDef{{Name: "content", Type: "TEXT"}}
	expr := &query.MatchExpr{
		Columns: []query.Expression{
			&query.Identifier{Name: "content"},
		},
		Pattern: &query.StringLiteral{Value: ""},
		Mode:    "NATURAL LANGUAGE MODE",
	}
	got, err := evaluateMatchExprLocked(c, row, columns, expr, nil)
	if err != nil {
		t.Fatalf("evaluateMatchExprLocked error: %v", err)
	}
	// Empty search pattern tokenizes to zero words => no match
	if got != false {
		t.Errorf("evaluateMatchExprLocked (empty pattern) = %v, want false", got)
	}
}

// ---------------------------------------------------------------------------
// evalStringSubstringIndex (from catalog_eval_string.go)
// ---------------------------------------------------------------------------

func TestEvalStringSubstringIndex_PositiveCount(t *testing.T) {
	got := evalStringSubstringIndex([]interface{}{"a.b.c.d", ".", 2})
	if got.err != nil {
		t.Fatalf("evalStringSubstringIndex error: %v", got.err)
	}
	if got.val != "a.b" {
		t.Errorf("evalStringSubstringIndex = %q, want %q", got.val, "a.b")
	}
}

func TestEvalStringSubstringIndex_NegativeCount(t *testing.T) {
	got := evalStringSubstringIndex([]interface{}{"a.b.c.d", ".", -2})
	if got.err != nil {
		t.Fatalf("evalStringSubstringIndex error: %v", got.err)
	}
	if got.val != "c.d" {
		t.Errorf("evalStringSubstringIndex = %q, want %q", got.val, "c.d")
	}
}

func TestEvalStringSubstringIndex_CountExceedsParts(t *testing.T) {
	got := evalStringSubstringIndex([]interface{}{"a.b.c.d", ".", 10})
	if got.err != nil {
		t.Fatalf("evalStringSubstringIndex error: %v", got.err)
	}
	if got.val != "a.b.c.d" {
		t.Errorf("evalStringSubstringIndex (count exceeds) = %q, want %q", got.val, "a.b.c.d")
	}
}

func TestEvalStringSubstringIndex_NegativeCountExceeds(t *testing.T) {
	got := evalStringSubstringIndex([]interface{}{"a.b.c.d", ".", -10})
	if got.err != nil {
		t.Fatalf("evalStringSubstringIndex error: %v", got.err)
	}
	if got.val != "a.b.c.d" {
		t.Errorf("evalStringSubstringIndex (neg count exceeds) = %q, want %q", got.val, "a.b.c.d")
	}
}

func TestEvalStringSubstringIndex_EmptyDelim(t *testing.T) {
	got := evalStringSubstringIndex([]interface{}{"hello", "", 2})
	if got.err != nil {
		t.Fatalf("evalStringSubstringIndex error: %v", got.err)
	}
	if got.val != "" {
		t.Errorf("evalStringSubstringIndex (empty delim) = %q, want empty", got.val)
	}
}

func TestEvalStringSubstringIndex_ZeroCount(t *testing.T) {
	got := evalStringSubstringIndex([]interface{}{"a.b.c", ".", 0})
	if got.err != nil {
		t.Fatalf("evalStringSubstringIndex error: %v", got.err)
	}
	if got.val != "" {
		t.Errorf("evalStringSubstringIndex (zero count) = %q, want empty", got.val)
	}
}

func TestEvalStringSubstringIndex_TooFewArgs(t *testing.T) {
	got := evalStringSubstringIndex([]interface{}{"a.b.c", "."})
	if got.err == nil {
		t.Fatal("evalStringSubstringIndex with 2 args should error")
	}
}

func TestEvalStringSubstringIndex_NilArg(t *testing.T) {
	got := evalStringSubstringIndex([]interface{}{nil, ".", 2})
	if got.err != nil {
		t.Fatalf("evalStringSubstringIndex error: %v", got.err)
	}
	if got.val != nil {
		t.Errorf("evalStringSubstringIndex (nil str) = %v, want nil", got.val)
	}
}

// ---------------------------------------------------------------------------
// evalStringAscii (from catalog_eval_string.go)
// ---------------------------------------------------------------------------

func TestEvalStringAscii_Normal(t *testing.T) {
	got := evalStringAscii([]interface{}{"hello"})
	if got.err != nil {
		t.Fatalf("evalStringAscii error: %v", got.err)
	}
	if got.val != float64('h') {
		t.Errorf("evalStringAscii = %v, want %v", got.val, float64('h'))
	}
}

func TestEvalStringAscii_Empty(t *testing.T) {
	got := evalStringAscii([]interface{}{""})
	if got.err != nil {
		t.Fatalf("evalStringAscii error: %v", got.err)
	}
	if got.val != float64(0) {
		t.Errorf("evalStringAscii (empty) = %v, want float64(0)", got.val)
	}
}

func TestEvalStringAscii_Nil(t *testing.T) {
	got := evalStringAscii([]interface{}{nil})
	if got.err != nil {
		t.Fatalf("evalStringAscii error: %v", got.err)
	}
	if got.val != nil {
		t.Errorf("evalStringAscii (nil) = %v, want nil", got.val)
	}
}

func TestEvalStringAscii_ZeroByte(t *testing.T) {
	got := evalStringAscii([]interface{}{"\x00"})
	if got.err != nil {
		t.Fatalf("evalStringAscii error: %v", got.err)
	}
	if got.val != float64(0) {
		t.Errorf("evalStringAscii (null byte) = %v, want float64(0)", got.val)
	}
}

func TestEvalStringAscii_NonStringArg(t *testing.T) {
	// Non-string arg gets converted via ValueToStringKey
	got := evalStringAscii([]interface{}{int64(65)})
	if got.err != nil {
		t.Fatalf("evalStringAscii error: %v", got.err)
	}
	if got.val != float64('6') { // string representation "65" -> '6' (ASCII 54)
		t.Errorf("evalStringAscii(int64) = %v, want %v", got.val, float64('6'))
	}
}

func TestEvalStringAscii_TooFewArgs(t *testing.T) {
	got := evalStringAscii([]interface{}{})
	if got.err == nil {
		t.Fatal("evalStringAscii with 0 args should error")
	}
}
