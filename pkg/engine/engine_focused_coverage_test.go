package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ---------------------------------------------------------------------------
// Tier 1:  Table-driven unit tests of standalone / leaf-level functions
// ---------------------------------------------------------------------------

// ---- valueToLiteralExpr ----

func TestValueToLiteralExpr(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		wantType string
	}{
		{"nil", nil, "null"},
		{"int", 42, "number"},
		{"int64", int64(99), "number"},
		{"float64", float64(3.14), "number"},
		{"bool true", true, "boolean"},
		{"bool false", false, "boolean"},
		{"string", "hello", "string"},
		{"default ([]byte)", []byte("raw"), "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valueToLiteralExpr(tt.input)
			if got == nil {
				t.Fatal("expected non-nil expression")
			}
			switch tt.wantType {
			case "null":
				if _, ok := got.(*query.NullLiteral); !ok {
					t.Errorf("expected *query.NullLiteral, got %T", got)
				}
			case "number":
				if _, ok := got.(*query.NumberLiteral); !ok {
					t.Errorf("expected *query.NumberLiteral, got %T", got)
				}
			case "boolean":
				if _, ok := got.(*query.BooleanLiteral); !ok {
					t.Errorf("expected *query.BooleanLiteral, got %T", got)
				}
			case "string":
				if _, ok := got.(*query.StringLiteral); !ok {
					t.Errorf("expected *query.StringLiteral, got %T", got)
				}
			}
		})
	}
}

// ---- upsertValuesColumnName ----

func TestUpsertValuesColumnName(t *testing.T) {
	tests := []struct {
		name     string
		expr     query.Expression
		wantName string
		wantOK   bool
	}{
		{name: "Identifier", expr: &query.Identifier{Name: "col1"}, wantName: "col1", wantOK: true},
		{name: "QualifiedIdentifier", expr: &query.QualifiedIdentifier{Table: "t", Column: "col2"}, wantName: "col2", wantOK: true},
		{name: "ColumnRef with name", expr: &query.ColumnRef{Column: "col3"}, wantName: "col3", wantOK: true},
		{name: "ColumnRef empty", expr: &query.ColumnRef{Column: ""}, wantName: "", wantOK: false},
		{name: "default (NumberLiteral)", expr: &query.NumberLiteral{Value: 1}, wantName: "", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, ok := upsertValuesColumnName(tt.expr)
			if name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
			if ok != tt.wantOK {
				t.Errorf("ok = %v, want %v", ok, tt.wantOK)
			}
		})
	}
}

// ---- isAggregateExpr ----

func TestIsAggregateExpr(t *testing.T) {
	tests := []struct {
		name string
		expr query.Expression
		want bool
	}{
		{"COUNT", &query.FunctionCall{Name: "COUNT"}, true},
		{"SUM", &query.FunctionCall{Name: "SUM"}, true},
		{"AVG", &query.FunctionCall{Name: "AVG"}, true},
		{"MIN", &query.FunctionCall{Name: "MIN"}, true},
		{"MAX", &query.FunctionCall{Name: "MAX"}, true},
		{"GROUP_CONCAT", &query.FunctionCall{Name: "GROUP_CONCAT"}, true},
		{"JSON_ARRAYAGG", &query.FunctionCall{Name: "JSON_ARRAYAGG"}, true},
		{"JSON_OBJECTAGG", &query.FunctionCall{Name: "JSON_OBJECTAGG"}, true},
		{"count lowercase", &query.FunctionCall{Name: "count"}, true},
		{"Sum mixed", &query.FunctionCall{Name: "Sum"}, true},
		{"LENGTH", &query.FunctionCall{Name: "LENGTH"}, false},
		{"SUBSTR", &query.FunctionCall{Name: "SUBSTR"}, false},
		{"COALESCE", &query.FunctionCall{Name: "COALESCE"}, false},
		{"Identifier", &query.Identifier{Name: "x"}, false},
		{"NumberLiteral", &query.NumberLiteral{Value: 1}, false},
		{"BinaryExpr", &query.BinaryExpr{Left: &query.NumberLiteral{Value: 1}, Operator: query.TokenPlus, Right: &query.NumberLiteral{Value: 2}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isAggregateExpr(tt.expr)
			if got != tt.want {
				t.Errorf("isAggregateExpr() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---- estimateLimitRows ----

func TestEstimateLimitRows(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name      string
		limit     query.Expression
		inputRows int64
		want      int64
	}{
		{name: "nil limit", limit: nil, inputRows: 100, want: 100},
		{name: "limit less than inputRows", limit: &query.NumberLiteral{Value: 10}, inputRows: 100, want: 10},
		{name: "limit equal to inputRows", limit: &query.NumberLiteral{Value: 100}, inputRows: 100, want: 100},
		{name: "limit greater than inputRows", limit: &query.NumberLiteral{Value: 200}, inputRows: 100, want: 100},
		{name: "non-literal limit", limit: &query.Identifier{Name: "x"}, inputRows: 50, want: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := db.estimateLimitRows(tt.limit, tt.inputRows)
			if got != tt.want {
				t.Errorf("estimateLimitRows() = %d, want %d", got, tt.want)
			}
		})
	}
}

// ---- releaseHandedOffSlot ----

func TestReleaseHandedOffSlot(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.connLimit = 5

	t.Run("with waiter", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		db.connWaitMu.Lock()
		db.connWaiters = append(db.connWaiters, ch)
		db.connWaitMu.Unlock()

		db.releaseHandedOffSlot()

		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("waiter was not signaled")
		}
		db.connWaitMu.Lock()
		if len(db.connWaiters) != 0 {
			t.Errorf("expected 0 waiters, got %d", len(db.connWaiters))
		}
		db.connWaitMu.Unlock()
	})

	t.Run("no waiter decrements connCount", func(t *testing.T) {
		db.connWaitMu.Lock()
		db.connWaiters = nil
		db.connWaitMu.Unlock()
		before := db.connCount.Load()
		db.releaseHandedOffSlot()
		after := db.connCount.Load()
		if after != before-1 {
			t.Errorf("connCount = %d, want %d", after, before-1)
		}
	})
}

// ---- TableSelfForeignKeyRefs ----

func TestTableSelfForeignKeyRefs(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name TEXT,
		manager_id INTEGER REFERENCES employees(id)
	)`)
	if err != nil {
		t.Fatal(err)
	}

	refs := db.TableSelfForeignKeyRefs("employees")
	if len(refs) == 0 {
		t.Fatal("expected at least one self-referencing FK ref")
	}
	found := false
	for _, ref := range refs {
		if len(ref.Columns) > 0 && ref.Columns[0] == "manager_id" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected self-FK on manager_id, got refs: %+v", refs)
	}

	refs2 := db.TableSelfForeignKeyRefs("nonexistent")
	if refs2 != nil {
		t.Errorf("expected nil for non-existent table, got %+v", refs2)
	}
}

// ---------------------------------------------------------------------------
// Tier 2:  Integration tests
// ---------------------------------------------------------------------------

// ---- executeExplainQuery (via EXPLAIN SQL) ----

func TestExecuteExplainQuery(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("EXPLAIN SELECT", func(t *testing.T) {
		rows, err := db.Query(ctx, "EXPLAIN SELECT * FROM items WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN SELECT failed: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count == 0 {
			t.Error("expected at least one plan row")
		}
	})

	t.Run("EXPLAIN INSERT", func(t *testing.T) {
		rows, err := db.Query(ctx, "EXPLAIN INSERT INTO items (id, name) VALUES (1, 'test')")
		if err != nil {
			t.Fatalf("EXPLAIN INSERT failed: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count == 0 {
			t.Error("expected at least one plan row for INSERT")
		}
	})

	t.Run("EXPLAIN UPDATE", func(t *testing.T) {
		rows, err := db.Query(ctx, "EXPLAIN UPDATE items SET name = 'updated' WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN UPDATE failed: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count == 0 {
			t.Error("expected at least one plan row for UPDATE")
		}
	})

	t.Run("EXPLAIN DELETE", func(t *testing.T) {
		rows, err := db.Query(ctx, "EXPLAIN DELETE FROM items WHERE id = 1")
		if err != nil {
			t.Fatalf("EXPLAIN DELETE failed: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count == 0 {
			t.Error("expected at least one plan row for DELETE")
		}
	})

	t.Run("EXPLAIN unsupported (CREATE TABLE)", func(t *testing.T) {
		rows, err := db.Query(ctx, "EXPLAIN CREATE TABLE other (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("EXPLAIN CREATE TABLE failed: %v", err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var plan string
		if err := rows.Scan(&plan); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if !strings.Contains(plan, "EXPLAIN not supported") {
			t.Errorf("expected 'EXPLAIN not supported' message, got %q", plan)
		}
	})
}

// ---- substituteParamsInExpr ----

func TestSubstituteParamsInExpr(t *testing.T) {
	paramMap := map[string]interface{}{
		"p_string": "hello",
		"p_int":    42,
		"p_int64":  int64(99),
		"p_float":  3.14,
		"p_bool":   true,
		"p_nil":    nil,
		"p_bytes":  []byte("raw"),
	}

	tests := []struct {
		name            string
		expr            query.Expression
		wantSubstituted bool
	}{
		{name: "nil expr", expr: nil},
		{name: "Identifier->string", expr: &query.Identifier{Name: "p_string"}, wantSubstituted: true},
		{name: "Identifier->int", expr: &query.Identifier{Name: "p_int"}, wantSubstituted: true},
		{name: "Identifier->int64", expr: &query.Identifier{Name: "p_int64"}, wantSubstituted: true},
		{name: "Identifier->float64", expr: &query.Identifier{Name: "p_float"}, wantSubstituted: true},
		{name: "Identifier->bool", expr: &query.Identifier{Name: "p_bool"}, wantSubstituted: true},
		{name: "Identifier->nil", expr: &query.Identifier{Name: "p_nil"}, wantSubstituted: true},
		{name: "Identifier->bytes (default)", expr: &query.Identifier{Name: "p_bytes"}, wantSubstituted: true},
		{name: "Identifier not in map", expr: &query.Identifier{Name: "unknown"}},
		{name: "BinaryExpr", expr: &query.BinaryExpr{Left: &query.Identifier{Name: "p_int"}, Operator: query.TokenPlus, Right: &query.Identifier{Name: "p_int64"}}, wantSubstituted: true},
		{name: "UnaryExpr", expr: &query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.Identifier{Name: "p_int"}}, wantSubstituted: true},
		{name: "FunctionCall", expr: &query.FunctionCall{Name: "CONCAT", Args: []query.Expression{&query.Identifier{Name: "p_string"}}}, wantSubstituted: true},
		{name: "CaseExpr", expr: &query.CaseExpr{Expr: &query.Identifier{Name: "p_int"}, Whens: []*query.WhenClause{{Condition: &query.NumberLiteral{Value: 1}, Result: &query.Identifier{Name: "p_string"}}}, Else: &query.Identifier{Name: "p_string"}}, wantSubstituted: true},
		{name: "BetweenExpr", expr: &query.BetweenExpr{Expr: &query.Identifier{Name: "p_int"}, Lower: &query.NumberLiteral{Value: 1}, Upper: &query.Identifier{Name: "p_int64"}}, wantSubstituted: true},
		{name: "InExpr", expr: &query.InExpr{Expr: &query.Identifier{Name: "p_int"}, List: []query.Expression{&query.Identifier{Name: "p_int"}, &query.Identifier{Name: "p_int64"}}}, wantSubstituted: true},
		{name: "IsNullExpr", expr: &query.IsNullExpr{Expr: &query.Identifier{Name: "p_string"}, Not: true}, wantSubstituted: true},
		{name: "CastExpr", expr: &query.CastExpr{Expr: &query.Identifier{Name: "p_string"}, DataType: query.TokenText}, wantSubstituted: true},
		{name: "LikeExpr", expr: &query.LikeExpr{Expr: &query.Identifier{Name: "p_string"}, Pattern: &query.StringLiteral{Value: "%test%"}}, wantSubstituted: true},
		{name: "AliasExpr", expr: &query.AliasExpr{Expr: &query.Identifier{Name: "p_int"}, Alias: "val"}, wantSubstituted: true},
		{name: "default NumberLiteral", expr: &query.NumberLiteral{Value: 42}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := substituteParamsInExpr(tt.expr, paramMap)
			if tt.expr == nil {
				if got != nil {
					t.Error("expected nil result for nil input")
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil result")
			}
			if tt.wantSubstituted {
				if id, ok := tt.expr.(*query.Identifier); ok {
					if _, inMap := paramMap[id.Name]; inMap {
						if _, stillID := got.(*query.Identifier); stillID {
							t.Errorf("Identifier %q should have been substituted but got %T", id.Name, got)
						}
					}
				}
			}
		})
	}
}

// ---- substituteParamsInStatement ----

func TestSubstituteParamsInStatement(t *testing.T) {
	paramMap := map[string]interface{}{
		"p_name": "test",
		"p_id":   42,
	}

	t.Run("InsertStmt", func(t *testing.T) {
		stmt := &query.InsertStmt{
			Table: "t",
			Values: [][]query.Expression{
				{&query.Identifier{Name: "p_id"}, &query.Identifier{Name: "p_name"}},
			},
		}
		got := substituteParamsInStatement(stmt, paramMap)
		ins, ok := got.(*query.InsertStmt)
		if !ok {
			t.Fatalf("expected *query.InsertStmt, got %T", got)
		}
		if _, stillID := ins.Values[0][0].(*query.Identifier); stillID {
			t.Error("p_id should have been substituted in values")
		}
	})

	t.Run("UpdateStmt with WHERE", func(t *testing.T) {
		stmt := &query.UpdateStmt{
			Table: "t",
			Set:   []*query.SetClause{{Column: "name", Value: &query.Identifier{Name: "p_name"}}},
			Where: &query.Identifier{Name: "p_id"},
		}
		got := substituteParamsInStatement(stmt, paramMap)
		upd, ok := got.(*query.UpdateStmt)
		if !ok {
			t.Fatalf("expected *query.UpdateStmt, got %T", got)
		}
		if _, stillID := upd.Set[0].Value.(*query.Identifier); stillID {
			t.Error("p_name should have been substituted in SET")
		}
		if _, stillID := upd.Where.(*query.Identifier); stillID {
			t.Error("p_id should have been substituted in WHERE")
		}
	})

	t.Run("UpdateStmt without WHERE", func(t *testing.T) {
		stmt := &query.UpdateStmt{
			Table: "t",
			Set:   []*query.SetClause{{Column: "name", Value: &query.Identifier{Name: "p_name"}}},
		}
		got := substituteParamsInStatement(stmt, paramMap)
		upd, ok := got.(*query.UpdateStmt)
		if !ok {
			t.Fatalf("expected *query.UpdateStmt, got %T", got)
		}
		if upd.Where != nil {
			t.Error("expected nil WHERE for UpdateStmt without Where")
		}
	})

	t.Run("DeleteStmt with WHERE", func(t *testing.T) {
		stmt := &query.DeleteStmt{Table: "t", Where: &query.Identifier{Name: "p_id"}}
		got := substituteParamsInStatement(stmt, paramMap)
		del, ok := got.(*query.DeleteStmt)
		if !ok {
			t.Fatalf("expected *query.DeleteStmt, got %T", got)
		}
		if _, stillID := del.Where.(*query.Identifier); stillID {
			t.Error("p_id should have been substituted in DeleteStmt WHERE")
		}
	})

	t.Run("DeleteStmt without WHERE", func(t *testing.T) {
		stmt := &query.DeleteStmt{Table: "t"}
		got := substituteParamsInStatement(stmt, paramMap)
		del, ok := got.(*query.DeleteStmt)
		if !ok {
			t.Fatalf("expected *query.DeleteStmt, got %T", got)
		}
		if del.Where != nil {
			t.Error("expected nil WHERE for DeleteStmt without Where")
		}
	})

	t.Run("SelectStmt", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "p_name"}},
			Where:   &query.Identifier{Name: "p_id"},
			Limit:   &query.Identifier{Name: "p_id"},
		}
		got := substituteParamsInStatement(stmt, paramMap)
		sel, ok := got.(*query.SelectStmt)
		if !ok {
			t.Fatalf("expected *query.SelectStmt, got %T", got)
		}
		if _, stillID := sel.Columns[0].(*query.Identifier); stillID {
			t.Error("p_name should have been substituted in Columns")
		}
		if _, stillID := sel.Where.(*query.Identifier); stillID {
			t.Error("p_id should have been substituted in WHERE")
		}
	})

	t.Run("default unknown statement", func(t *testing.T) {
		stmt := &query.CreateTableStmt{Table: "test"}
		got := substituteParamsInStatement(stmt, paramMap)
		if got != stmt {
			t.Error("expected same statement back for unknown type")
		}
	})
}

// ---- substituteUpsertValuesExpr ----

func TestSubstituteUpsertValuesExpr(t *testing.T) {
	colPos := map[string]int{"a": 0, "b": 1, "c": 2}
	row := []query.Expression{
		&query.NumberLiteral{Value: 10},
		&query.StringLiteral{Value: "hello"},
		&query.NullLiteral{},
	}

	t.Run("nil expr", func(t *testing.T) {
		got, err := substituteUpsertValuesExpr(nil, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Error("expected nil for nil expr")
		}
	})

	t.Run("VALUES() resolves to column", func(t *testing.T) {
		expr := &query.FunctionCall{
			Name: "VALUES",
			Args: []query.Expression{&query.Identifier{Name: "a"}},
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.NumberLiteral); !ok {
			t.Errorf("expected *query.NumberLiteral, got %T", got)
		}
	})

	t.Run("VALUES() error -- wrong arg count", func(t *testing.T) {
		expr := &query.FunctionCall{Name: "VALUES", Args: nil}
		_, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err == nil {
			t.Error("expected error for VALUES() without args")
		}
	})

	t.Run("VALUES() error -- not a column", func(t *testing.T) {
		expr := &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.NumberLiteral{Value: 1}}}
		_, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err == nil {
			t.Error("expected error for VALUES() with non-column arg")
		}
	})

	t.Run("VALUES() error -- column not present", func(t *testing.T) {
		expr := &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "missing"}}}
		_, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err == nil {
			t.Error("expected error for VALUES() with missing column")
		}
	})

	t.Run("non-VALUES FunctionCall recursion", func(t *testing.T) {
		expr := &query.FunctionCall{
			Name: "COALESCE",
			Args: []query.Expression{
				&query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
			},
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fc, ok := got.(*query.FunctionCall)
		if !ok {
			t.Fatalf("expected *query.FunctionCall, got %T", got)
		}
		if _, ok := fc.Args[0].(*query.NumberLiteral); !ok {
			t.Errorf("expected NumberLiteral for resolved VALUES(), got %T", fc.Args[0])
		}
	})

	t.Run("BinaryExpr substitution", func(t *testing.T) {
		expr := &query.BinaryExpr{
			Left:     &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
			Operator: query.TokenPlus,
			Right:    &query.NumberLiteral{Value: 5},
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		bin, ok := got.(*query.BinaryExpr)
		if !ok {
			t.Fatalf("expected *query.BinaryExpr, got %T", got)
		}
		if _, ok := bin.Left.(*query.NumberLiteral); !ok {
			t.Errorf("expected NumberLiteral for left, got %T", bin.Left)
		}
	})

	t.Run("UnaryExpr substitution", func(t *testing.T) {
		expr := &query.UnaryExpr{
			Operator: query.TokenMinus,
			Expr:     &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.UnaryExpr); !ok {
			t.Fatalf("expected *query.UnaryExpr, got %T", got)
		}
	})

	t.Run("CaseExpr substitution", func(t *testing.T) {
		expr := &query.CaseExpr{
			Expr: &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
			Whens: []*query.WhenClause{
				{Condition: &query.NumberLiteral{Value: 1}, Result: &query.NumberLiteral{Value: 100}},
			},
			Else: &query.NumberLiteral{Value: 0},
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.CaseExpr); !ok {
			t.Fatalf("expected *query.CaseExpr, got %T", got)
		}
	})

	t.Run("BetweenExpr substitution", func(t *testing.T) {
		expr := &query.BetweenExpr{
			Expr:  &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
			Lower: &query.NumberLiteral{Value: 0},
			Upper: &query.NumberLiteral{Value: 100},
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.BetweenExpr); !ok {
			t.Fatalf("expected *query.BetweenExpr, got %T", got)
		}
	})

	t.Run("InExpr substitution", func(t *testing.T) {
		expr := &query.InExpr{
			Expr: &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
			List: []query.Expression{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 2}},
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.InExpr); !ok {
			t.Fatalf("expected *query.InExpr, got %T", got)
		}
	})

	t.Run("LikeExpr substitution", func(t *testing.T) {
		expr := &query.LikeExpr{
			Expr:    &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "b"}}},
			Pattern: &query.StringLiteral{Value: "%test%"},
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.LikeExpr); !ok {
			t.Fatalf("expected *query.LikeExpr, got %T", got)
		}
	})

	t.Run("IsNullExpr substitution", func(t *testing.T) {
		expr := &query.IsNullExpr{
			Expr: &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
			Not:  false,
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.IsNullExpr); !ok {
			t.Fatalf("expected *query.IsNullExpr, got %T", got)
		}
	})

	t.Run("CastExpr substitution", func(t *testing.T) {
		expr := &query.CastExpr{
			Expr:     &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
			DataType: query.TokenText,
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.CastExpr); !ok {
			t.Fatalf("expected *query.CastExpr, got %T", got)
		}
	})

	t.Run("AliasExpr substitution", func(t *testing.T) {
		expr := &query.AliasExpr{
			Expr:  &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "a"}}},
			Alias: "val_a",
		}
		got, err := substituteUpsertValuesExpr(expr, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got.(*query.AliasExpr); !ok {
			t.Fatalf("expected *query.AliasExpr, got %T", got)
		}
	})
}

// ---- DeleteBackup ----

func TestDeleteBackup(t *testing.T) {
	t.Run("closed DB returns ErrDatabaseClosed", func(t *testing.T) {
		db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
		if err != nil {
			t.Fatal(err)
		}
		db.Close()

		err = db.DeleteBackup("some-id")
		if err != ErrDatabaseClosed {
			t.Errorf("expected ErrDatabaseClosed, got %v", err)
		}
	})

	t.Run("in-memory DB has no backup manager", func(t *testing.T) {
		db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		err = db.DeleteBackup("some-id")
		if err == nil {
			t.Fatal("expected error for missing backup manager")
		}
		if !strings.Contains(err.Error(), "backup manager not initialized") {
			t.Errorf("expected 'backup manager not initialized', got %v", err)
		}
	})

	t.Run("disk DB with backup dir can initialise backup manager", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "cobaltdb-backup-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		dbPath := filepath.Join(dir, "testdb")
		db, err := Open(dbPath, &Options{
			CoreStorage: CoreStorage{
				InMemory:   false,
				CacheSize:  1024,
				WALEnabled: BoolPtr(true),
			},
			Backup: BackupConfig{
				Dir: filepath.Join(dir, "backups"),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Attempt to delete a non-existent backup -- the backup manager exists
		// so we get the manager-level error rather than "not initialized".
		err = db.DeleteBackup("nonexistent")
		if err == nil {
			t.Fatal("expected an error deleting non-existent backup")
		}
		// The backup manager is initialized, the error comes from the manager.
		if strings.Contains(err.Error(), "backup manager not initialized") {
			t.Errorf("expected manager-level error, got 'not initialized': %v", err)
		}
	})
}

// ---- hasAggregates helper (for completeness) ----

func TestHasAggregates(t *testing.T) {
	tests := []struct {
		name string
		cols []query.Expression
		want bool
	}{
		{"no aggregates", []query.Expression{&query.Identifier{Name: "x"}}, false},
		{"COUNT aggregate", []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.Identifier{Name: "x"}}}}, true},
		{"multiple non-aggregates", []query.Expression{&query.Identifier{Name: "a"}, &query.NumberLiteral{Value: 1}}, false},
		{"mixed", []query.Expression{&query.Identifier{Name: "a"}, &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "b"}}}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasAggregates(tt.cols)
			if got != tt.want {
				t.Errorf("hasAggregates() = %v, want %v", got, tt.want)
			}
		})
	}
}
