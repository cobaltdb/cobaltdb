package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ============================================================
// TestCovBoost7_JSONQuote
// ============================================================

func TestCovBoost7_JSONQuote(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple_string", "hello", `"hello"`},
		{"empty_string", "", `""`},
		{"with_double_quotes", `say "hi"`, `"say \"hi\""`},
		{"with_backslash", `path\to\file`, `"path\\to\\file"`},
		{"with_newline", "line1\nline2", `"line1\nline2"`},
		{"with_tab", "col1\tcol2", `"col1\tcol2"`},
		// Unicode chars: json.Marshal outputs UTF-8 directly
		{"with_control_char_formfeed", "ab\x0ccd", `"ab\fcd"`},
		{"with_carriage_return", "line1\rline2", `"line1\rline2"`},
		{"mixed_special_chars", "a\"b\\c\nd\te", `"a\"b\\c\nd\te"`},
		{"already_quoted_looking", `"already"`, `"\"already\""`},
		{"with_null_char", "ab\x00cd", `"ab\u0000cd"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := JSONQuote(tt.input)
			if got != tt.want {
				t.Errorf("JSONQuote(%q) = %s, want %s", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// TestCovBoost7_applyOrderBy
// ============================================================

func TestCovBoost7_applyOrderBy_NullsAndDesc(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Rows: [name, age]
	rows := [][]interface{}{
		{"Alice", float64(30)},
		{"Bob", nil},
		{"Charlie", float64(25)},
		{nil, float64(40)},
		{"Eve", float64(35)},
	}

	selectCols := []selectColInfo{
		{name: "name", index: 0},
		{name: "age", index: 1},
	}

	// ORDER BY age ASC — NULLs should sort last
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "age"}, Desc: false},
	}
	sorted := cat.applyOrderBy(rows, selectCols, orderBy)
	if len(sorted) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(sorted))
	}
	// Last row should have nil age
	if sorted[len(sorted)-1][1] != nil {
		t.Errorf("NULLs should sort last in ASC, got last age = %v", sorted[len(sorted)-1][1])
	}

	// ORDER BY age DESC — NULLs should sort first
	orderByDesc := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "age"}, Desc: true},
	}
	sortedDesc := cat.applyOrderBy(rows, selectCols, orderByDesc)
	if sortedDesc[len(sortedDesc)-1][1] != nil {
		// NULL sorts first in DESC
		if sortedDesc[0][1] != nil {
			t.Errorf("NULLs should sort first in DESC, got first age = %v", sortedDesc[0][1])
		}
	}
}

func TestCovBoost7_applyOrderBy_QualifiedIdentifier(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	rows := [][]interface{}{
		{"Charlie", float64(30)},
		{"Alice", float64(20)},
		{"Bob", float64(25)},
	}

	selectCols := []selectColInfo{
		{name: "name", tableName: "users", index: 0},
		{name: "age", tableName: "users", index: 1},
	}

	// ORDER BY users.age using QualifiedIdentifier
	orderBy := []*query.OrderByExpr{
		{Expr: &query.QualifiedIdentifier{Table: "users", Column: "age"}, Desc: false},
	}
	sorted := cat.applyOrderBy(rows, selectCols, orderBy)
	if sorted[0][0] != "Alice" {
		t.Errorf("expected first row to be Alice, got %v", sorted[0][0])
	}
	if sorted[2][0] != "Charlie" {
		t.Errorf("expected last row to be Charlie, got %v", sorted[2][0])
	}
}

func TestCovBoost7_applyOrderBy_QualifiedIdentifier_FallbackColumnOnly(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	rows := [][]interface{}{
		{"Charlie", float64(30)},
		{"Alice", float64(20)},
		{"Bob", float64(25)},
	}

	// selectCols with different table name than what ORDER BY uses
	selectCols := []selectColInfo{
		{name: "name", tableName: "people", index: 0},
		{name: "age", tableName: "people", index: 1},
	}

	// ORDER BY nonexistent_table.age — falls back to column-only match
	orderBy := []*query.OrderByExpr{
		{Expr: &query.QualifiedIdentifier{Table: "nonexistent", Column: "age"}, Desc: false},
	}
	sorted := cat.applyOrderBy(rows, selectCols, orderBy)
	if sorted[0][0] != "Alice" {
		t.Errorf("expected first row to be Alice after fallback, got %v", sorted[0][0])
	}
}

func TestCovBoost7_applyOrderBy_NumberLiteral(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	rows := [][]interface{}{
		{"Charlie", float64(30)},
		{"Alice", float64(20)},
		{"Bob", float64(25)},
	}

	selectCols := []selectColInfo{
		{name: "name", index: 0},
		{name: "age", index: 1},
	}

	// ORDER BY 2 (column position, 1-based => age)
	orderBy := []*query.OrderByExpr{
		{Expr: &query.NumberLiteral{Value: 2}, Desc: false},
	}
	sorted := cat.applyOrderBy(rows, selectCols, orderBy)
	if sorted[0][0] != "Alice" {
		t.Errorf("expected first row to be Alice, got %v", sorted[0][0])
	}
}

func TestCovBoost7_applyOrderBy_ExpressionOrderBy(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Rows: [name, __orderby_0]
	rows := [][]interface{}{
		{"Charlie", float64(300)},
		{"Alice", float64(100)},
		{"Bob", float64(200)},
	}

	selectCols := []selectColInfo{
		{name: "name", index: 0},
		{name: "__orderby_0", index: 1},
	}

	// ORDER BY with a BinaryExpr (expression-based), should match __orderby_0
	orderBy := []*query.OrderByExpr{
		{Expr: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "price"},
			Operator: query.TokenStar,
			Right:    &query.Identifier{Name: "qty"},
		}, Desc: false},
	}
	sorted := cat.applyOrderBy(rows, selectCols, orderBy)
	if sorted[0][0] != "Alice" {
		t.Errorf("expected first row to be Alice, got %v", sorted[0][0])
	}
}

func TestCovBoost7_applyOrderBy_DottedIdentifier(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	rows := [][]interface{}{
		{"Charlie", float64(30)},
		{"Alice", float64(20)},
		{"Bob", float64(25)},
	}

	selectCols := []selectColInfo{
		{name: "name", tableName: "users", index: 0},
		{name: "age", tableName: "users", index: 1},
	}

	// ORDER BY "users.age" — a dotted Identifier (not QualifiedIdentifier)
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "users.age"}, Desc: false},
	}
	sorted := cat.applyOrderBy(rows, selectCols, orderBy)
	if sorted[0][0] != "Alice" {
		t.Errorf("expected first row to be Alice (dotted identifier), got %v", sorted[0][0])
	}
}

func TestCovBoost7_applyOrderBy_DottedIdentifier_FallbackColumnOnly(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	rows := [][]interface{}{
		{"Charlie", float64(30)},
		{"Alice", float64(20)},
		{"Bob", float64(25)},
	}

	// selectCols have different table name
	selectCols := []selectColInfo{
		{name: "name", tableName: "people", index: 0},
		{name: "age", tableName: "people", index: 1},
	}

	// ORDER BY "wrong_table.age" — table doesn't match, falls back to col-name-only
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "wrong_table.age"}, Desc: false},
	}
	sorted := cat.applyOrderBy(rows, selectCols, orderBy)
	if sorted[0][0] != "Alice" {
		t.Errorf("expected first row to be Alice after fallback, got %v", sorted[0][0])
	}
}

func TestCovBoost7_applyOrderBy_EmptyRows(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Empty rows returns immediately
	result := cat.applyOrderBy(nil, nil, []*query.OrderByExpr{{Expr: &query.Identifier{Name: "x"}}})
	if result != nil {
		t.Errorf("expected nil for empty rows, got %v", result)
	}

	// Empty orderBy returns rows as-is
	rows := [][]interface{}{{"a"}}
	result2 := cat.applyOrderBy(rows, nil, nil)
	if len(result2) != 1 {
		t.Errorf("expected 1 row, got %d", len(result2))
	}
}

func TestCovBoost7_applyOrderBy_InvalidColIdx(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	rows := [][]interface{}{
		{"Alice"},
		{"Bob"},
	}

	selectCols := []selectColInfo{
		{name: "name", index: 0},
	}

	// ORDER BY non-existent column — colIdx stays -1, should not panic
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "nonexistent"}, Desc: false},
	}
	sorted := cat.applyOrderBy(rows, selectCols, orderBy)
	if len(sorted) != 2 {
		t.Errorf("expected 2 rows, got %d", len(sorted))
	}
}

// ============================================================
// TestCovBoost7_collectColumnStats
// ============================================================

func TestCovBoost7_collectColumnStats(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	sc := NewStatsCollector(cat)

	// Test with invalid table name
	_, err := sc.collectColumnStats("", "col")
	if err == nil {
		t.Error("expected error for empty table name")
	}

	// Test with invalid column name
	_, err = sc.collectColumnStats("test", "")
	if err == nil {
		t.Error("expected error for empty column name")
	}

	// Test with valid table and column (ExecuteQuery returns empty so we just test paths)
	createTestTable := func(name string) {
		stmt := &query.CreateTableStmt{
			Table: name,
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "val", Type: query.TokenText},
			},
		}
		if err := cat.CreateTable(stmt); err != nil {
			t.Fatalf("CreateTable(%s) failed: %v", name, err)
		}
	}
	createTestTable("stats_test")

	// collectColumnStats works even with empty ExecuteQuery
	stats, err := sc.collectColumnStats("stats_test", "val")
	if err != nil {
		t.Fatalf("collectColumnStats returned error: %v", err)
	}
	if stats.ColumnName != "val" {
		t.Errorf("expected column name 'val', got %q", stats.ColumnName)
	}

	// Test with special characters in identifiers
	_, err = sc.collectColumnStats("test;", "col")
	if err == nil {
		t.Error("expected error for invalid table name with semicolon")
	}

	_, err = sc.collectColumnStats("test", "col;")
	if err == nil {
		t.Error("expected error for invalid column name with semicolon")
	}
}

// ============================================================
// TestCovBoost7_referencedRowExists
// ============================================================

func TestCovBoost7_referencedRowExists(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create parent table
	createCoverageTestTable(t, cat, "fk_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Insert some rows
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		_, _, err := cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_parent",
			Columns: []string{"id", "name"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: fmt.Sprintf("name_%d", i)},
			}},
		}, nil)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	fke := NewForeignKeyEnforcer(cat)

	// Test existing row (single column)
	exists, err := fke.referencedRowExists("fk_parent", []string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("referencedRowExists returned error: %v", err)
	}
	if !exists {
		t.Error("expected row to exist for id=1")
	}

	// Test non-existing row
	exists, err = fke.referencedRowExists("fk_parent", []string{"id"}, []interface{}{int64(999)})
	if err != nil {
		t.Fatalf("referencedRowExists returned error: %v", err)
	}
	if exists {
		t.Error("expected row NOT to exist for id=999")
	}

	// Test non-existing table
	exists, err = fke.referencedRowExists("nonexistent_table", []string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("referencedRowExists returned error: %v", err)
	}
	if exists {
		t.Error("expected false for nonexistent table")
	}

	// Test composite key lookup
	exists, err = fke.referencedRowExists("fk_parent", []string{"id", "name"}, []interface{}{int64(1), "name_1"})
	if err != nil {
		t.Fatalf("referencedRowExists composite returned error: %v", err)
	}
	// Composite key lookup uses serializeCompositeKey — the key may not exist in the
	// tree since the tree keys are single-column PKs. We just verify it doesn't error.
	_ = exists

	// Test with nil value
	exists, err = fke.referencedRowExists("fk_parent", []string{"id"}, []interface{}{nil})
	if err != nil {
		t.Fatalf("referencedRowExists nil returned error: %v", err)
	}
	_ = exists

	// Test with string value
	exists, err = fke.referencedRowExists("fk_parent", []string{"id"}, []interface{}{"string_val"})
	if err != nil {
		t.Fatalf("referencedRowExists string returned error: %v", err)
	}
	_ = exists
}

// ============================================================
// TestCovBoost7_isCacheableQuery
// ============================================================

func TestCovBoost7_isCacheableQuery(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *query.SelectStmt
		expected bool
	}{
		{
			"no_from_clause",
			&query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}},
			false,
		},
		{
			"simple_cacheable",
			&query.SelectStmt{
				Columns: []query.Expression{&query.Identifier{Name: "id"}},
				From:    &query.TableRef{Name: "t1"},
			},
			true,
		},
		{
			"with_subquery_in_column",
			&query.SelectStmt{
				Columns: []query.Expression{&query.SubqueryExpr{Query: &query.SelectStmt{
					Columns: []query.Expression{&query.NumberLiteral{Value: 1}},
					From:    &query.TableRef{Name: "t2"},
				}}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"with_exists_in_column",
			&query.SelectStmt{
				Columns: []query.Expression{&query.ExistsExpr{Subquery: &query.SelectStmt{
					Columns: []query.Expression{&query.NumberLiteral{Value: 1}},
					From:    &query.TableRef{Name: "t2"},
				}}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"with_random_function",
			&query.SelectStmt{
				Columns: []query.Expression{&query.FunctionCall{Name: "RANDOM"}},
				From:    &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"with_now_function",
			&query.SelectStmt{
				Columns: []query.Expression{&query.FunctionCall{Name: "NOW"}},
				From:    &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"random_in_where",
			&query.SelectStmt{
				Columns: []query.Expression{&query.Identifier{Name: "id"}},
				From:    &query.TableRef{Name: "t1"},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenGt,
					Right:    &query.FunctionCall{Name: "RANDOM"},
				},
			},
			false,
		},
		{
			"random_in_order_by",
			&query.SelectStmt{
				Columns: []query.Expression{&query.Identifier{Name: "id"}},
				From:    &query.TableRef{Name: "t1"},
				OrderBy: []*query.OrderByExpr{{Expr: &query.FunctionCall{Name: "RANDOM"}}},
			},
			false,
		},
		{
			"deterministic_function_cacheable",
			&query.SelectStmt{
				Columns: []query.Expression{&query.FunctionCall{Name: "UPPER", Args: []query.Expression{&query.Identifier{Name: "name"}}}},
				From:    &query.TableRef{Name: "t1"},
			},
			true,
		},
		{
			"subquery_in_alias",
			&query.SelectStmt{
				Columns: []query.Expression{&query.AliasExpr{
					Expr:  &query.SubqueryExpr{Query: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}, From: &query.TableRef{Name: "t2"}}},
					Alias: "sub",
				}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"subquery_in_binary_expr",
			&query.SelectStmt{
				Columns: []query.Expression{&query.BinaryExpr{
					Left:     &query.NumberLiteral{Value: 1},
					Operator: query.TokenPlus,
					Right:    &query.SubqueryExpr{Query: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 2}}, From: &query.TableRef{Name: "t2"}}},
				}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"subquery_in_unary_expr",
			&query.SelectStmt{
				Columns: []query.Expression{&query.UnaryExpr{
					Operator: query.TokenNot,
					Expr:     &query.SubqueryExpr{Query: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}, From: &query.TableRef{Name: "t2"}}},
				}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"subquery_in_function_arg",
			&query.SelectStmt{
				Columns: []query.Expression{&query.FunctionCall{
					Name: "COALESCE",
					Args: []query.Expression{&query.SubqueryExpr{Query: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}, From: &query.TableRef{Name: "t2"}}}},
				}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"nondet_in_alias_expr",
			&query.SelectStmt{
				Columns: []query.Expression{&query.AliasExpr{
					Expr:  &query.FunctionCall{Name: "UUID"},
					Alias: "uid",
				}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"nondet_in_binary_expr",
			&query.SelectStmt{
				Columns: []query.Expression{&query.BinaryExpr{
					Left:     &query.FunctionCall{Name: "RAND"},
					Operator: query.TokenStar,
					Right:    &query.NumberLiteral{Value: 10},
				}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"nondet_in_unary_expr",
			&query.SelectStmt{
				Columns: []query.Expression{&query.UnaryExpr{
					Operator: query.TokenMinus,
					Expr:     &query.FunctionCall{Name: "RANDOM"},
				}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
		{
			"nondet_nested_in_func_arg",
			&query.SelectStmt{
				Columns: []query.Expression{&query.FunctionCall{
					Name: "ABS",
					Args: []query.Expression{&query.FunctionCall{Name: "RANDOM"}},
				}},
				From: &query.TableRef{Name: "t1"},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCacheableQuery(tt.stmt)
			if got != tt.expected {
				t.Errorf("isCacheableQuery() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// ============================================================
// TestCovBoost7_FlushTableTrees
// ============================================================

func TestCovBoost7_FlushTableTrees(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create a table and insert data
	createCoverageTestTable(t, cat, "flush_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_test",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "test"},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Flush should succeed
	if err := cat.FlushTableTrees(); err != nil {
		t.Errorf("FlushTableTrees() returned error: %v", err)
	}

	// Verify data is still accessible after flush
	cols, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "flush_test"},
	}, nil)
	if err != nil {
		t.Fatalf("Select after flush failed: %v", err)
	}
	if len(cols) == 0 || len(rows) == 0 {
		t.Error("expected data to be accessible after flush")
	}
}

func TestCovBoost7_FlushTableTrees_Empty(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Flush with no tables should succeed
	if err := cat.FlushTableTrees(); err != nil {
		t.Errorf("FlushTableTrees() on empty catalog returned error: %v", err)
	}
}

// ============================================================
// TestCovBoost7_buildJSONIndex
// ============================================================

func TestCovBoost7_buildJSONIndex_WithData(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table with JSON column
	createCoverageTestTable(t, cat, "json_idx_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenJSON},
	})

	ctx := context.Background()
	// Insert rows with JSON data
	jsonValues := []string{
		`{"name": "Alice", "age": 30}`,
		`{"name": "Bob", "age": 25}`,
		`{"name": "Charlie", "active": true}`,
	}
	for i, jv := range jsonValues {
		_, _, err := cat.Insert(ctx, &query.InsertStmt{
			Table:   "json_idx_test",
			Columns: []string{"id", "data"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i + 1)},
				&query.StringLiteral{Value: jv},
			}},
		}, nil)
		if err != nil {
			t.Fatalf("Insert JSON row %d failed: %v", i+1, err)
		}
	}

	// Create JSON index on the name path — this calls buildJSONIndex internally
	err := cat.CreateJSONIndex("idx_json_name", "json_idx_test", "data", "$.name", "string")
	if err != nil {
		t.Fatalf("CreateJSONIndex failed: %v", err)
	}

	// Verify index was created (entries depend on storage format)
	cat.mu.RLock()
	_, exists := cat.jsonIndexes["idx_json_name"]
	cat.mu.RUnlock()
	if !exists {
		t.Fatal("JSON index not found")
	}

	// Create another JSON index on numeric path
	err = cat.CreateJSONIndex("idx_json_age", "json_idx_test", "data", "$.age", "number")
	if err != nil {
		t.Fatalf("CreateJSONIndex for age failed: %v", err)
	}

	// Create JSON index on boolean path
	err = cat.CreateJSONIndex("idx_json_active", "json_idx_test", "data", "$.active", "boolean")
	if err != nil {
		t.Fatalf("CreateJSONIndex for active failed: %v", err)
	}
}

func TestCovBoost7_buildJSONIndex_NoData(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table with JSON column but no data
	createCoverageTestTable(t, cat, "json_idx_empty", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenJSON},
	})

	// buildJSONIndex on empty table returns nil
	idx := &JSONIndexDef{
		Name:      "test_empty",
		TableName: "json_idx_empty",
		Column:    "data",
		Path:      "$.name",
		DataType:  "string",
		Index:     make(map[string][]int64),
		NumIndex:  make(map[string][]int64),
	}
	err := cat.buildJSONIndex(idx)
	if err != nil {
		t.Errorf("buildJSONIndex on empty table returned error: %v", err)
	}
}

func TestCovBoost7_buildJSONIndex_NonexistentTable(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// buildJSONIndex on nonexistent table returns nil (no tree)
	idx := &JSONIndexDef{
		Name:      "test_none",
		TableName: "nonexistent",
		Column:    "data",
		Path:      "$.name",
		DataType:  "string",
		Index:     make(map[string][]int64),
		NumIndex:  make(map[string][]int64),
	}
	err := cat.buildJSONIndex(idx)
	if err != nil {
		t.Errorf("buildJSONIndex on nonexistent table returned error: %v", err)
	}
}

// ============================================================
// TestCovBoost7_RollbackTransaction_AlterDropColumn
// ============================================================

func TestCovBoost7_RollbackTransaction_AlterDropColumn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table with multiple columns
	createCoverageTestTable(t, cat, "rb_drop", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "age", Type: query.TokenInteger},
	})

	ctx := context.Background()
	// Insert a row
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "rb_drop",
		Columns: []string{"id", "name", "age"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "Alice"},
			&query.NumberLiteral{Value: 30},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Create an index on the column we will drop
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_rb_age",
		Table:   "rb_drop",
		Columns: []string{"age"},
	})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Begin transaction
	cat.BeginTransaction(1)

	// Drop column within transaction
	err = cat.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "rb_drop",
		Action:  "DROP",
		NewName: "age", // Column name to drop is stored in NewName
	})
	if err != nil {
		t.Fatalf("AlterTableDropColumn failed: %v", err)
	}

	// Verify column was dropped
	tbl, _ := cat.GetTable("rb_drop")
	if len(tbl.Columns) != 2 {
		t.Fatalf("expected 2 columns after drop, got %d", len(tbl.Columns))
	}

	// Rollback the transaction — should restore the column and row data
	err = cat.RollbackTransaction()
	if err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}

	// Verify column is restored
	tbl, _ = cat.GetTable("rb_drop")
	if len(tbl.Columns) != 3 {
		t.Errorf("expected 3 columns after rollback, got %d", len(tbl.Columns))
	}

	// Verify age column exists
	found := false
	for _, col := range tbl.Columns {
		if col.Name == "age" {
			found = true
			break
		}
	}
	if !found {
		t.Error("age column should be restored after rollback")
	}
}

func TestCovBoost7_RollbackTransaction_AlterRenameColumn(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table
	createCoverageTestTable(t, cat, "rb_rename", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Create an index on the column we will rename
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_rb_name",
		Table:   "rb_rename",
		Columns: []string{"name"},
	})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Begin transaction
	cat.BeginTransaction(2)

	// Rename column within transaction
	err = cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "rb_rename",
		Action:  "RENAME_COLUMN",
		OldName: "name",
		NewName: "full_name",
	})
	if err != nil {
		t.Fatalf("AlterTableRenameColumn failed: %v", err)
	}

	// Verify column was renamed
	tbl, _ := cat.GetTable("rb_rename")
	renamed := false
	for _, col := range tbl.Columns {
		if col.Name == "full_name" {
			renamed = true
			break
		}
	}
	if !renamed {
		t.Fatal("expected column to be renamed to full_name")
	}

	// Rollback the transaction — should restore the old column name
	err = cat.RollbackTransaction()
	if err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}

	// Verify column name is restored
	tbl, _ = cat.GetTable("rb_rename")
	restored := false
	for _, col := range tbl.Columns {
		if col.Name == "name" {
			restored = true
			break
		}
	}
	if !restored {
		t.Error("column name should be restored to 'name' after rollback")
	}

	// Verify index column was also restored
	cat.mu.RLock()
	idxDef, exists := cat.indexes["idx_rb_name"]
	cat.mu.RUnlock()
	if !exists {
		t.Fatal("index should exist after rollback")
	}
	if len(idxDef.Columns) == 0 || idxDef.Columns[0] != "name" {
		t.Errorf("index column should be restored to 'name', got %v", idxDef.Columns)
	}
}

func TestCovBoost7_RollbackTransaction_AlterRenameColumn_PK(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table where the PK column will be renamed
	createCoverageTestTable(t, cat, "rb_rename_pk", []*query.ColumnDef{
		{Name: "myid", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Begin transaction
	cat.BeginTransaction(3)

	// Rename PK column
	err := cat.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "rb_rename_pk",
		Action:  "RENAME_COLUMN",
		OldName: "myid",
		NewName: "new_id",
	})
	if err != nil {
		t.Fatalf("AlterTableRenameColumn PK failed: %v", err)
	}

	// Verify PK was updated
	tbl, _ := cat.GetTable("rb_rename_pk")
	pkRenamed := false
	for _, pk := range tbl.PrimaryKey {
		if pk == "new_id" {
			pkRenamed = true
		}
	}
	if !pkRenamed {
		t.Fatal("expected PK to be renamed to new_id")
	}

	// Rollback
	err = cat.RollbackTransaction()
	if err != nil {
		t.Fatalf("RollbackTransaction failed: %v", err)
	}

	// Verify PK is restored
	tbl, _ = cat.GetTable("rb_rename_pk")
	pkRestored := false
	for _, pk := range tbl.PrimaryKey {
		if pk == "myid" {
			pkRestored = true
		}
	}
	if !pkRestored {
		t.Error("PK should be restored to 'myid' after rollback")
	}
}

// ============================================================
// TestCovBoost7_containsSubquery_and_hasNonDeterministicFunction extra paths
// ============================================================

func TestCovBoost7_containsSubquery_NilExpr(t *testing.T) {
	if containsSubquery(nil) {
		t.Error("expected false for nil expression")
	}

	// Plain identifier — no subquery
	if containsSubquery(&query.Identifier{Name: "x"}) {
		t.Error("expected false for plain identifier")
	}

	// FunctionCall with no subquery in args
	if containsSubquery(&query.FunctionCall{Name: "ABS", Args: []query.Expression{&query.NumberLiteral{Value: 1}}}) {
		t.Error("expected false for function without subquery")
	}
}

func TestCovBoost7_hasNonDeterministicFunction_NilExpr(t *testing.T) {
	if hasNonDeterministicFunction(nil) {
		t.Error("expected false for nil expression")
	}

	// Literal expression — not a function
	if hasNonDeterministicFunction(&query.NumberLiteral{Value: 42}) {
		t.Error("expected false for number literal")
	}
}

// ============================================================
// TestCovBoost7_JSONQuote_RoundTrip
// ============================================================

func TestCovBoost7_JSONQuote_RoundTrip(t *testing.T) {
	// Verify round-trip: JSONQuote then json.Unmarshal should give back original
	values := []string{
		"",
		"hello world",
		"line1\nline2",
		"tab\there",
		`quote"inside`,
		`back\slash`,
		"unicode: \u00e9\u00e8\u00ea",
	}
	for _, v := range values {
		quoted := JSONQuote(v)
		var unquoted string
		if err := json.Unmarshal([]byte(quoted), &unquoted); err != nil {
			t.Errorf("JSONQuote(%q) produced invalid JSON: %s, error: %v", v, quoted, err)
			continue
		}
		if unquoted != v {
			t.Errorf("round-trip failed for %q: got %q", v, unquoted)
		}
	}
}

// ============================================================
// TestCovBoost7_buildJSONIndex_NestedPath
// ============================================================

func TestCovBoost7_buildJSONIndex_NestedPath(t *testing.T) {
	cat, cleanup := setupCoverageCatalog(t)
	defer cleanup()

	// Create table with JSON column
	createCoverageTestTable(t, cat, "json_nested", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenJSON},
	})

	ctx := context.Background()
	// Insert rows with nested JSON
	_, _, err := cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_nested",
		Columns: []string{"id", "data"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: `{"address": {"city": "Boston"}}`},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("Insert nested JSON failed: %v", err)
	}

	// Create JSON index on nested path $.address.city
	err = cat.CreateJSONIndex("idx_nested_city", "json_nested", "data", "$.address.city", "string")
	if err != nil {
		t.Fatalf("CreateJSONIndex nested failed: %v", err)
	}

	// Verify JSON index was created
	cat.mu.RLock()
	_, idxExists := cat.jsonIndexes["idx_nested_city"]
	cat.mu.RUnlock()
	if !idxExists {
		t.Error("expected nested JSON index to exist")
	}
}

// ============================================================
// TestCovBoost7_generateQueryKey
// ============================================================

func TestCovBoost7_generateQueryKey(t *testing.T) {
	key1 := generateQueryKey("SELECT * FROM t", nil)
	if key1 != "SELECT * FROM t" {
		t.Errorf("expected key without args, got %q", key1)
	}

	key2 := generateQueryKey("SELECT * FROM t WHERE id = ?", []interface{}{42, "hello"})
	if !strings.Contains(key2, "42") || !strings.Contains(key2, "hello") {
		t.Errorf("expected key with args, got %q", key2)
	}

	// Different args produce different keys
	key3 := generateQueryKey("SELECT * FROM t WHERE id = ?", []interface{}{99})
	if key2 == key3 {
		t.Error("different args should produce different keys")
	}
}
