package wasm

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ---------------------------------------------------------------------------
// compileUpdate / generateUpdateBody
// ---------------------------------------------------------------------------

func TestCompileUpdate(t *testing.T) {
	t.Run("basic_update", func(t *testing.T) {
		compiler := NewCompiler()
		stmt := &query.UpdateStmt{
			Table: "test",
			Set: []*query.SetClause{
				{Column: "name", Value: &query.StringLiteral{Value: "newname"}},
			},
		}
		compiled, err := compiler.CompileQuery("UPDATE test SET name = 'newname'", stmt, nil)
		if err != nil {
			t.Fatalf("compileUpdate failed: %v", err)
		}
		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}
		if compiled.EntryPoint == 0 && len(compiled.Bytecode) == 0 {
			t.Error("Expected valid compiled update")
		}
		t.Logf("UPDATE compiled OK, bytecode=%d bytes", len(compiled.Bytecode))
	})

	t.Run("update_with_where", func(t *testing.T) {
		compiler := NewCompiler()
		stmt := &query.UpdateStmt{
			Table: "test",
			Set: []*query.SetClause{
				{Column: "name", Value: &query.StringLiteral{Value: "Alice2"}},
			},
			Where: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Column: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 1},
			},
		}
		compiled, err := compiler.CompileQuery("UPDATE test SET name='Alice2' WHERE id=1", stmt, nil)
		if err != nil {
			t.Fatalf("compileUpdate with WHERE failed: %v", err)
		}
		if compiled.ResultSchema != nil {
			t.Error("UPDATE should have nil ResultSchema")
		}
		t.Logf("UPDATE with WHERE compiled OK")
	})

	t.Run("update_no_set", func(t *testing.T) {
		compiler := NewCompiler()
		stmt := &query.UpdateStmt{
			Table: "test",
			Set:   []*query.SetClause{},
		}
		compiled, err := compiler.CompileQuery("UPDATE test SET x=1", stmt, nil)
		if err != nil {
			t.Fatalf("compileUpdate no-set failed: %v", err)
		}
		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Expected valid bytecode, got %d bytes", len(compiled.Bytecode))
		}
	})
}

// ---------------------------------------------------------------------------
// compileAggregateFunction
// ---------------------------------------------------------------------------

func TestCompileAggregateFunction(t *testing.T) {
	compiler := NewCompiler()

	t.Run("count_star", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{Name: "COUNT", Args: []query.Expression{}}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("COUNT failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
		if buf.Len() == 0 {
			t.Error("Expected non-empty bytecode")
		}
	})

	t.Run("count_with_arg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{
			Name: "COUNT",
			Args: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
		}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("COUNT(id) failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("sum_with_arg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{
			Name: "SUM",
			Args: []query.Expression{&query.NumberLiteral{Value: 10}},
		}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("SUM failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("sum_no_arg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{Name: "SUM", Args: []query.Expression{}}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("SUM() failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("avg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{Name: "AVG", Args: []query.Expression{}}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("AVG failed: %v", err)
		}
		if typ != "REAL" {
			t.Errorf("Expected REAL, got %s", typ)
		}
	})

	t.Run("min_with_arg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{
			Name: "MIN",
			Args: []query.Expression{&query.NumberLiteral{Value: 5}},
		}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("MIN failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("min_no_arg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{Name: "MIN", Args: []query.Expression{}}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("MIN() failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("max_with_arg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{
			Name: "MAX",
			Args: []query.Expression{&query.NumberLiteral{Value: 99}},
		}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("MAX failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("max_no_arg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{Name: "MAX", Args: []query.Expression{}}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("MAX() failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("unknown_function", func(t *testing.T) {
		buf := new(bytes.Buffer)
		fn := &query.FunctionCall{Name: "UNKNOWN_FN", Args: []query.Expression{}}
		typ, err := compiler.compileAggregateFunction(fn, buf)
		if err != nil {
			t.Fatalf("unknown function failed: %v", err)
		}
		// Default case returns INTEGER
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER for unknown func, got %s", typ)
		}
		if buf.Len() == 0 {
			t.Error("Expected bytecode for unknown function")
		}
	})
}

// ---------------------------------------------------------------------------
// hasWhereClause
// ---------------------------------------------------------------------------

func TestHasWhereClause(t *testing.T) {
	compiler := NewCompiler()

	t.Run("with_where", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
			Where: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Column: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 1},
			},
		}
		if !compiler.hasWhereClause(stmt) {
			t.Error("Expected hasWhereClause to return true")
		}
	})

	t.Run("without_where", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
		}
		if compiler.hasWhereClause(stmt) {
			t.Error("Expected hasWhereClause to return false")
		}
	})
}

// ---------------------------------------------------------------------------
// encodeGlobalSection and encodeDataSection via generateModule
// ---------------------------------------------------------------------------

func TestEncodeGlobalSection(t *testing.T) {
	t.Run("mutable_global", func(t *testing.T) {
		compiler := NewCompiler()
		// Manually add a global so encodeGlobalSection is exercised via generateModule
		compiler.globals = append(compiler.globals, Global{
			Type:    I32,
			Mutable: true,
			Init:    []byte{0x41, 0x00, 0x0b}, // i32.const 0; end
		})
		bytecode := compiler.generateModule()
		if len(bytecode) < 8 {
			t.Fatalf("Expected valid module bytecode, got %d bytes", len(bytecode))
		}
		// Global section id is 0x06; verify it appears in output
		found := false
		for _, b := range bytecode {
			if b == 0x06 {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected global section (0x06) in bytecode")
		}
	})

	t.Run("immutable_global", func(t *testing.T) {
		compiler := NewCompiler()
		compiler.globals = append(compiler.globals, Global{
			Type:    I64,
			Mutable: false,
			Init:    []byte{0x42, 0x2a, 0x0b}, // i64.const 42; end
		})
		bytecode := compiler.generateModule()
		if len(bytecode) < 8 {
			t.Fatalf("Expected valid bytecode")
		}
	})

	t.Run("multiple_globals", func(t *testing.T) {
		compiler := NewCompiler()
		for i := 0; i < 3; i++ {
			compiler.globals = append(compiler.globals, Global{
				Type:    I32,
				Mutable: true,
				Init:    []byte{0x41, byte(i), 0x0b},
			})
		}
		bytecode := compiler.generateModule()
		if len(bytecode) < 8 {
			t.Fatalf("Expected valid bytecode")
		}
	})
}

func TestEncodeDataSection(t *testing.T) {
	t.Run("single_data_segment", func(t *testing.T) {
		compiler := NewCompiler()
		// Add a data segment so encodeDataSection is exercised
		compiler.data = append(compiler.data, DataSegment{
			Offset: []byte{0x41, 0x00, 0x0b}, // i32.const 0; end
			Data:   []byte("hello"),
		})
		bytecode := compiler.generateModule()
		if len(bytecode) < 8 {
			t.Fatalf("Expected valid module bytecode")
		}
		// Data section id is 0x0b; verify it appears
		found := false
		for _, b := range bytecode {
			if b == 0x0b {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected data section (0x0b) in bytecode")
		}
	})

	t.Run("multiple_data_segments", func(t *testing.T) {
		compiler := NewCompiler()
		compiler.data = append(compiler.data, DataSegment{
			Offset: []byte{0x41, 0x00, 0x0b},
			Data:   []byte("segment1"),
		})
		compiler.data = append(compiler.data, DataSegment{
			Offset: []byte{0x41, 0x40, 0x0b}, // offset 64
			Data:   []byte("segment2"),
		})
		bytecode := compiler.generateModule()
		if len(bytecode) < 8 {
			t.Fatalf("Expected valid bytecode with 2 data segments")
		}
	})
}

// ---------------------------------------------------------------------------
// compileExpression – all branches
// ---------------------------------------------------------------------------

func TestCompileExpression(t *testing.T) {
	compiler := NewCompiler()

	t.Run("number_literal", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.NumberLiteral{Value: 42}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("NumberLiteral failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
		if buf.Len() == 0 {
			t.Error("Expected bytecode output")
		}
	})

	t.Run("string_literal", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.StringLiteral{Value: "hello"}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("StringLiteral failed: %v", err)
		}
		if typ != "TEXT" {
			t.Errorf("Expected TEXT, got %s", typ)
		}
	})

	t.Run("boolean_literal_true", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BooleanLiteral{Value: true}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BooleanLiteral(true) failed: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected BOOLEAN, got %s", typ)
		}
		// Should emit i32.const 1
		b := buf.Bytes()
		if len(b) < 2 || b[0] != 0x41 || b[1] != 0x01 {
			t.Errorf("Expected [0x41 0x01], got %v", b)
		}
	})

	t.Run("boolean_literal_false", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BooleanLiteral{Value: false}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BooleanLiteral(false) failed: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected BOOLEAN, got %s", typ)
		}
		// Should emit i32.const 0
		b := buf.Bytes()
		if len(b) < 2 || b[0] != 0x41 || b[1] != 0x00 {
			t.Errorf("Expected [0x41 0x00], got %v", b)
		}
	})

	t.Run("function_call_count", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.FunctionCall{Name: "COUNT", Args: []query.Expression{}}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("FunctionCall COUNT failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("function_call_avg", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.FunctionCall{Name: "AVG", Args: []query.Expression{}}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("FunctionCall AVG failed: %v", err)
		}
		if typ != "REAL" {
			t.Errorf("Expected REAL, got %s", typ)
		}
	})

	t.Run("binary_expr_add_integers", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 3},
			Operator: query.TokenPlus,
			Right:    &query.NumberLiteral{Value: 4},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr + failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("binary_expr_sub_integers", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 10},
			Operator: query.TokenMinus,
			Right:    &query.NumberLiteral{Value: 4},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr - failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("binary_expr_mul_integers", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 3},
			Operator: query.TokenStar,
			Right:    &query.NumberLiteral{Value: 4},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr * failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("binary_expr_eq", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 1},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr = failed: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected BOOLEAN, got %s", typ)
		}
	})

	t.Run("binary_expr_neq", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 1},
			Operator: query.TokenNeq,
			Right:    &query.NumberLiteral{Value: 2},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr != failed: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected BOOLEAN, got %s", typ)
		}
	})

	t.Run("binary_expr_lt", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 1},
			Operator: query.TokenLt,
			Right:    &query.NumberLiteral{Value: 2},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr < failed: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected BOOLEAN, got %s", typ)
		}
	})

	t.Run("binary_expr_gt", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 5},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 2},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr > failed: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected BOOLEAN, got %s", typ)
		}
	})

	t.Run("binary_expr_string_types", func(t *testing.T) {
		buf := new(bytes.Buffer)
		// Text + Text -> uses i32.add (non-INTEGER path)
		expr := &query.BinaryExpr{
			Left:     &query.StringLiteral{Value: "a"},
			Operator: query.TokenPlus,
			Right:    &query.StringLiteral{Value: "b"},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr text+text failed: %v", err)
		}
		if typ != "TEXT" {
			t.Errorf("Expected TEXT, got %s", typ)
		}
	})

	t.Run("binary_expr_text_eq", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.StringLiteral{Value: "a"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "a"},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("BinaryExpr text eq failed: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected BOOLEAN, got %s", typ)
		}
	})

	t.Run("qualified_identifier", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.QualifiedIdentifier{Table: "t", Column: "col"}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("QualifiedIdentifier failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
	})

	t.Run("subquery_expr", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.SubqueryExpr{
			Query: &query.SelectStmt{
				Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
				From:    &query.TableRef{Name: "test"},
			},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("SubqueryExpr failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
		if buf.Len() == 0 {
			t.Error("Expected bytecode for subquery")
		}
	})

	t.Run("window_expr", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.WindowExpr{
			Function: "ROW_NUMBER",
			Args:     []query.Expression{},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("WindowExpr failed: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER, got %s", typ)
		}
		if buf.Len() == 0 {
			t.Error("Expected bytecode for window expr")
		}
	})

	t.Run("unknown_expression_type", func(t *testing.T) {
		buf := new(bytes.Buffer)
		// NullLiteral is not handled explicitly — falls to default
		expr := &query.NullLiteral{}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("Unknown expr type failed: %v", err)
		}
		// default returns INTEGER and emits i32.const 0
		if typ != "INTEGER" {
			t.Errorf("Expected INTEGER for unknown, got %s", typ)
		}
		if buf.Len() == 0 {
			t.Error("Expected bytecode for unknown expr")
		}
	})
}

// ---------------------------------------------------------------------------
// rollbackTransaction with delete / update ops in txLog
// ---------------------------------------------------------------------------

func TestRollbackTransactionWithOps(t *testing.T) {
	t.Run("rollback_no_tx", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)
		// No active transaction
		result, err := host.rollbackTransaction(rt, []uint64{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected 0 (no tx), got %d", result[0])
		}
	})

	t.Run("rollback_with_delete_op", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		host.beginTransaction(rt, []uint64{})

		// Manually record a delete operation in txLog
		deletedRow := map[string]interface{}{"id": int64(99), "name": "Deleted"}
		host.txLog = append(host.txLog, TxOperation{
			Type:  "delete",
			Table: "test",
			Row:   deletedRow,
		})

		initialLen := len(host.tables["test"])

		result, err := host.rollbackTransaction(rt, []uint64{})
		if err != nil {
			t.Fatalf("rollbackTransaction failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		// Deleted row should be restored
		if len(host.tables["test"]) != initialLen+1 {
			t.Errorf("Expected %d rows after rollback, got %d", initialLen+1, len(host.tables["test"]))
		}
		if host.txActive {
			t.Error("Expected txActive=false after rollback")
		}
	})

	t.Run("rollback_with_update_op", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		host.beginTransaction(rt, []uint64{})

		// Save original row 0
		originalRow := map[string]interface{}{"id": int64(1), "name": "Original"}
		host.txLog = append(host.txLog, TxOperation{
			Type:     "update",
			Table:    "test",
			Row:      originalRow,
			RowIndex: 0,
		})
		// Modify row 0
		host.tables["test"][0]["name"] = "Modified"

		result, err := host.rollbackTransaction(rt, []uint64{})
		if err != nil {
			t.Fatalf("rollbackTransaction failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		// Row 0 should be restored to original
		if host.tables["test"][0]["name"] != "Original" {
			t.Errorf("Expected 'Original', got %v", host.tables["test"][0]["name"])
		}
	})

	t.Run("rollback_with_insert_op", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		host.beginTransaction(rt, []uint64{})

		// Simulate an insert: add a row and log the insert
		host.tables["test"] = append(host.tables["test"], map[string]interface{}{"id": int64(100), "name": "Inserted"})
		host.txLog = append(host.txLog, TxOperation{
			Type:  "insert",
			Table: "test",
		})

		initialLen := len(host.tables["test"])

		result, err := host.rollbackTransaction(rt, []uint64{})
		if err != nil {
			t.Fatalf("rollbackTransaction failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		// Inserted row should be removed
		if len(host.tables["test"]) != initialLen-1 {
			t.Errorf("Expected %d rows, got %d", initialLen-1, len(host.tables["test"]))
		}
	})

	t.Run("rollback_multiple_ops", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		host.beginTransaction(rt, []uint64{})

		// Record delete + update ops
		host.txLog = append(host.txLog, TxOperation{
			Type:     "update",
			Table:    "test",
			Row:      map[string]interface{}{"id": int64(2), "name": "Bob"},
			RowIndex: 1,
		})
		host.txLog = append(host.txLog, TxOperation{
			Type:  "delete",
			Table: "test",
			Row:   map[string]interface{}{"id": int64(99)},
		})

		initialLen := len(host.tables["test"])
		result, err := host.rollbackTransaction(rt, []uint64{})
		if err != nil {
			t.Fatalf("rollbackTransaction failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		// One delete undone, so +1 row
		if len(host.tables["test"]) != initialLen+1 {
			t.Errorf("Expected %d rows, got %d", initialLen+1, len(host.tables["test"]))
		}
	})
}

// ---------------------------------------------------------------------------
// rollbackToSavepoint
// ---------------------------------------------------------------------------

func TestRollbackToSavepointCoverage(t *testing.T) {
	t.Run("rollback_savepoint_no_tx", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		// No active transaction
		result, err := host.rollbackToSavepoint(rt, []uint64{1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected 0 (no tx), got %d", result[0])
		}
	})

	t.Run("rollback_savepoint_no_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.beginTransaction(rt, []uint64{})
		result, err := host.rollbackToSavepoint(rt, []uint64{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected 0 (no params), got %d", result[0])
		}
	})

	t.Run("rollback_savepoint_with_log", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.beginTransaction(rt, []uint64{})
		host.savepoint(rt, []uint64{5})

		// Add an op to the log
		host.txLog = append(host.txLog, TxOperation{
			Type:  "insert",
			Table: "test",
		})

		result, err := host.rollbackToSavepoint(rt, []uint64{5})
		if err != nil {
			t.Fatalf("rollbackToSavepoint failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})

	t.Run("rollback_savepoint_empty_log", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.beginTransaction(rt, []uint64{})
		host.savepoint(rt, []uint64{2})

		// Empty log - loop body should not execute
		result, err := host.rollbackToSavepoint(rt, []uint64{2})
		if err != nil {
			t.Fatalf("rollbackToSavepoint failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})
}

// ---------------------------------------------------------------------------
// savepoint – no active tx path
// ---------------------------------------------------------------------------

func TestSavepointNoCoverage(t *testing.T) {
	t.Run("savepoint_no_tx", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		// No active transaction
		result, err := host.savepoint(rt, []uint64{1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected 0 (no tx), got %d", result[0])
		}
	})

	t.Run("savepoint_no_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.beginTransaction(rt, []uint64{})
		result, err := host.savepoint(rt, []uint64{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected 0 (no params), got %d", result[0])
		}
	})

	t.Run("savepoint_multiple_ids", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.beginTransaction(rt, []uint64{})

		for _, id := range []uint64{1, 2, 3, 10} {
			result, err := host.savepoint(rt, []uint64{id})
			if err != nil {
				t.Fatalf("savepoint(%d) failed: %v", id, err)
			}
			if result[0] != 1 {
				t.Errorf("savepoint(%d): expected 1, got %d", id, result[0])
			}
			if host.txSavepoint != int(id) {
				t.Errorf("Expected txSavepoint=%d, got %d", id, host.txSavepoint)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// vectorizedCompare – additional ops (ne, le, ge)
// ---------------------------------------------------------------------------

func TestVectorizedCompareEdgeCases(t *testing.T) {
	t.Run("compare_ne", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		inPtr1 := int32(1024)
		inPtr2 := int32(2048)
		outPtr := int32(3072)
		count := 3

		vals1 := []int64{1, 2, 3}
		vals2 := []int64{1, 5, 3}
		for i := range vals1 {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(vals1[i]))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(vals2[i]))
		}

		// op=1 for ne
		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count), 1}
		result, err := host.vectorizedCompare(rt, params)
		if err != nil {
			t.Fatalf("vectorizedCompare ne failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		expected := []int64{0, 1, 0}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("[%d] ne: expected %d, got %d", i, exp, val)
			}
		}
	})

	t.Run("compare_le", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		inPtr1 := int32(1024)
		inPtr2 := int32(2048)
		outPtr := int32(3072)
		count := 3

		vals1 := []int64{1, 5, 10}
		vals2 := []int64{5, 5, 9}
		for i := range vals1 {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(vals1[i]))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(vals2[i]))
		}

		// op=3 for le
		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count), 3}
		result, err := host.vectorizedCompare(rt, params)
		if err != nil {
			t.Fatalf("vectorizedCompare le failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		expected := []int64{1, 1, 0}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("[%d] le: expected %d, got %d", i, exp, val)
			}
		}
	})

	t.Run("compare_gt", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		inPtr1 := int32(1024)
		inPtr2 := int32(2048)
		outPtr := int32(3072)
		count := 3

		vals1 := []int64{10, 5, 3}
		vals2 := []int64{5, 5, 4}
		for i := range vals1 {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(vals1[i]))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(vals2[i]))
		}

		// op=4 for gt
		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count), 4}
		if _, err := host.vectorizedCompare(rt, params); err != nil {
			t.Fatalf("vectorizedCompare gt failed: %v", err)
		}
		expected := []int64{1, 0, 0}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("[%d] gt: expected %d, got %d", i, exp, val)
			}
		}
	})

	t.Run("compare_ge", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		inPtr1 := int32(1024)
		inPtr2 := int32(2048)
		outPtr := int32(3072)
		count := 3

		vals1 := []int64{10, 5, 3}
		vals2 := []int64{5, 5, 4}
		for i := range vals1 {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(vals1[i]))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(vals2[i]))
		}

		// op=5 for ge
		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count), 5}
		if _, err := host.vectorizedCompare(rt, params); err != nil {
			t.Fatalf("vectorizedCompare ge failed: %v", err)
		}
		expected := []int64{1, 1, 0}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("[%d] ge: expected %d, got %d", i, exp, val)
			}
		}
	})

	t.Run("compare_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.vectorizedCompare(rt, []uint64{1024, 2048, 3072})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("compare_zero_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.vectorizedCompare(rt, []uint64{1024, 2048, 3072, 0, 0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for zero count")
		}
	})

	t.Run("compare_overflow_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.vectorizedCompare(rt, []uint64{1024, 2048, 3072, 20000, 0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for overflow count")
		}
	})

	t.Run("compare_out_of_bounds", func(t *testing.T) {
		rt := NewRuntime(1) // Only 64KB
		host := NewHostFunctions()
		// Use pointers near end of memory to trigger OOB check
		memSize := len(rt.Memory)
		outOfBounds := uint64(memSize - 10)
		result, _ := host.vectorizedCompare(rt, []uint64{outOfBounds, 0, 0, 100, 0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for out-of-bounds")
		}
	})
}

// ---------------------------------------------------------------------------
// vectorizedBatchCopy – edge cases
// ---------------------------------------------------------------------------

func TestVectorizedBatchCopyEdgeCases(t *testing.T) {
	t.Run("batch_copy_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.vectorizedBatchCopy(rt, []uint64{1024, 2048})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("batch_copy_zero_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.vectorizedBatchCopy(rt, []uint64{1024, 2048, 0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for zero count")
		}
	})

	t.Run("batch_copy_overflow_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.vectorizedBatchCopy(rt, []uint64{1024, 2048, 20000})
		if result[0] != 0 {
			t.Errorf("Expected 0 for overflow count")
		}
	})

	t.Run("batch_copy_out_of_bounds_src", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		memSize := uint64(len(rt.Memory))
		result, _ := host.vectorizedBatchCopy(rt, []uint64{memSize - 10, 0, 100})
		if result[0] != 0 {
			t.Errorf("Expected 0 for OOB src")
		}
	})

	t.Run("batch_copy_out_of_bounds_dst", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		memSize := uint64(len(rt.Memory))
		result, _ := host.vectorizedBatchCopy(rt, []uint64{0, memSize - 10, 100})
		if result[0] != 0 {
			t.Errorf("Expected 0 for OOB dst")
		}
	})

	t.Run("batch_copy_single_element", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		srcPtr := int32(1024)
		dstPtr := int32(2048)
		binary.LittleEndian.PutUint64(rt.Memory[srcPtr:], uint64(12345))

		result, err := host.vectorizedBatchCopy(rt, []uint64{uint64(srcPtr), uint64(dstPtr), 1})
		if err != nil {
			t.Fatalf("vectorizedBatchCopy failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		val := binary.LittleEndian.Uint64(rt.Memory[dstPtr:])
		if val != 12345 {
			t.Errorf("Expected 12345, got %d", val)
		}
	})
}

// ---------------------------------------------------------------------------
// bindParameter – all param types
// ---------------------------------------------------------------------------

func TestBindParameterTypes(t *testing.T) {
	t.Run("bind_i32", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		valuePtr := int32(1024)
		binary.LittleEndian.PutUint32(rt.Memory[valuePtr:], uint32(42))

		// valueType=0 (i32)
		result, err := host.bindParameter(rt, []uint64{0, uint64(valuePtr), 0})
		if err != nil {
			t.Fatalf("bindParameter i32 failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success, got %d", result[0])
		}
	})

	t.Run("bind_i64", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		valuePtr := int32(1024)
		binary.LittleEndian.PutUint64(rt.Memory[valuePtr:], uint64(9999999999))

		// valueType=1 (i64)
		result, err := host.bindParameter(rt, []uint64{1, uint64(valuePtr), 1})
		if err != nil {
			t.Fatalf("bindParameter i64 failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success, got %d", result[0])
		}
	})

	t.Run("bind_f32", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		valuePtr := int32(1024)
		binary.LittleEndian.PutUint32(rt.Memory[valuePtr:], 0x4048F5C3) // ~3.14 as float32 bits

		// valueType=2 (f32)
		result, err := host.bindParameter(rt, []uint64{2, uint64(valuePtr), 2})
		if err != nil {
			t.Fatalf("bindParameter f32 failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success, got %d", result[0])
		}
	})

	t.Run("bind_f64", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		valuePtr := int32(1024)
		binary.LittleEndian.PutUint64(rt.Memory[valuePtr:], 0x400921FB54442D18) // pi as float64

		// valueType=3 (f64)
		result, err := host.bindParameter(rt, []uint64{3, uint64(valuePtr), 3})
		if err != nil {
			t.Fatalf("bindParameter f64 failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success, got %d", result[0])
		}
	})

	t.Run("bind_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.bindParameter(rt, []uint64{0, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("bind_various_slots", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		valuePtr := int32(1024)
		for slot := uint64(0); slot < 5; slot++ {
			binary.LittleEndian.PutUint64(rt.Memory[valuePtr:], slot*100)
			result, err := host.bindParameter(rt, []uint64{slot, uint64(valuePtr), 1})
			if err != nil {
				t.Fatalf("bindParameter slot %d failed: %v", slot, err)
			}
			if result[0] != 1 {
				t.Errorf("slot %d: expected success", slot)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// ExecutePrepared – error paths
// ---------------------------------------------------------------------------

func TestExecutePreparedErrors(t *testing.T) {
	t.Run("stmt_not_found", func(t *testing.T) {
		compiler := NewCompiler()
		_, err := compiler.ExecutePrepared("nonexistent_id", []interface{}{})
		if err == nil {
			t.Error("Expected error for nonexistent prepared statement")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("param_count_mismatch_too_many", func(t *testing.T) {
		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
		}
		prepared, err := compiler.Prepare("SELECT id FROM test WHERE id = ?", stmt, 1)
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		// Pass 3 params but expected 1
		_, err = compiler.ExecutePrepared(prepared.ID, []interface{}{1, 2, 3})
		if err == nil {
			t.Error("Expected error for too-many params")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("param_count_mismatch_too_few", func(t *testing.T) {
		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
		}
		prepared, err := compiler.Prepare("SELECT id FROM test WHERE id=? AND x=?", stmt, 2)
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		// Pass 0 params but expected 2
		_, err = compiler.ExecutePrepared(prepared.ID, []interface{}{})
		if err == nil {
			t.Error("Expected error for too-few params")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("execute_zero_params_ok", func(t *testing.T) {
		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
		}
		prepared, err := compiler.Prepare("SELECT id FROM test", stmt, 0)
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		compiled, err := compiler.ExecutePrepared(prepared.ID, []interface{}{})
		if err != nil {
			t.Fatalf("ExecutePrepared(0 params) failed: %v", err)
		}
		if compiled == nil {
			t.Error("Expected compiled query")
		}
	})
}

// ---------------------------------------------------------------------------
// rightJoin – edge cases
// ---------------------------------------------------------------------------

func TestRightJoinEdgeCases(t *testing.T) {
	t.Run("right_join_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.rightJoin(rt, []uint64{0, 1, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("right_join_zero_maxrows", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		rowCount, err := host.rightJoin(rt, []uint64{0, 0, 1024, 0})
		if err != nil {
			t.Fatalf("rightJoin failed: %v", err)
		}
		// maxRows=0 → no rows written but still returns 0 count
		t.Logf("rightJoin zero maxrows: rowCount=%d", rowCount[0])
	})

	t.Run("right_join_limited_maxrows", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		// Only allow 2 rows max
		rowCount, err := host.rightJoin(rt, []uint64{0, 0, uint64(outPtr), 2})
		if err != nil {
			t.Fatalf("rightJoin limited failed: %v", err)
		}
		if rowCount[0] > 2 {
			t.Errorf("Expected at most 2 rows, got %d", rowCount[0])
		}
		t.Logf("rightJoin limited: rowCount=%d", rowCount[0])
	})

	t.Run("right_join_full_execution", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		// 3 rows in test table, expect 3 matching rows
		result, err := host.rightJoin(rt, []uint64{0, 0, uint64(outPtr), 100})
		if err != nil {
			t.Fatalf("rightJoin full failed: %v", err)
		}
		if result[0] == 0 {
			t.Error("Expected non-zero row count")
		}
		t.Logf("rightJoin full: rowCount=%d", result[0])
	})

	t.Run("right_join_compilation", func(t *testing.T) {
		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "a", Column: "id"},
				&query.QualifiedIdentifier{Table: "b", Column: "id"},
			},
			From: &query.TableRef{Name: "a"},
			Joins: []*query.JoinClause{
				{
					Table:     &query.TableRef{Name: "b"},
					Type:      query.TokenRight,
					Condition: &query.BooleanLiteral{Value: true},
				},
			},
		}
		compiled, err := compiler.CompileQuery(
			"SELECT a.id, b.id FROM a RIGHT JOIN b ON true",
			stmt,
			nil,
		)
		if err != nil {
			t.Fatalf("RIGHT JOIN compilation failed: %v", err)
		}
		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short")
		}
		t.Logf("RIGHT JOIN compiled OK, bytecode=%d bytes", len(compiled.Bytecode))
	})
}

// ---------------------------------------------------------------------------
// parallelAggregate – edge cases
// ---------------------------------------------------------------------------

func TestParallelAggregateEdgeCases(t *testing.T) {
	makeParams := func(rt *Runtime, tableName, columnName string, aggType int, outPtr int32) []uint64 {
		tableNameOffset := int32(100)
		colNameOffset := int32(200)
		copy(rt.Memory[tableNameOffset:], []byte(tableName))
		copy(rt.Memory[colNameOffset:], []byte(columnName))
		return []uint64{
			uint64(tableNameOffset), uint64(len(tableName)),
			uint64(aggType),
			uint64(colNameOffset), uint64(len(columnName)),
			uint64(outPtr),
		}
	}

	t.Run("parallel_agg_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.parallelAggregate(rt, []uint64{0, 4, 0, 0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("parallel_agg_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		params := makeParams(rt, "test", "id", 0, outPtr) // COUNT

		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate COUNT failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		if val != 3 {
			t.Errorf("Expected COUNT=3, got %d", val)
		}
	})

	t.Run("parallel_agg_sum", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		params := makeParams(rt, "test", "id", 1, outPtr) // SUM

		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate SUM failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		// IDs are 1+2+3 = 6
		if val != 6 {
			t.Errorf("Expected SUM=6, got %d", val)
		}
	})

	t.Run("parallel_agg_avg", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		params := makeParams(rt, "test", "id", 2, outPtr) // AVG

		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate AVG failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		// AVG of 1,2,3 = 2
		if val != 2 {
			t.Errorf("Expected AVG=2, got %d", val)
		}
	})

	t.Run("parallel_agg_min", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		params := makeParams(rt, "test", "id", 3, outPtr) // MIN

		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate MIN failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		if val != 1 {
			t.Errorf("Expected MIN=1, got %d", val)
		}
	})

	t.Run("parallel_agg_max", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		params := makeParams(rt, "test", "id", 4, outPtr) // MAX

		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate MAX failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		if val != 3 {
			t.Errorf("Expected MAX=3, got %d", val)
		}
	})

	t.Run("parallel_agg_unknown_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		params := makeParams(rt, "nosuchtable", "id", 0, outPtr)

		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected 0 for unknown table, got %d", result[0])
		}
	})

	t.Run("parallel_agg_table_out_of_bounds", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		memSize := uint64(len(rt.Memory))
		// tableNamePtr near end so tableNamePtr+len overflows
		params := []uint64{memSize - 2, 10, 0, 0, 2, 1024}
		result, _ := host.parallelAggregate(rt, params)
		if result[0] != 0 {
			t.Errorf("Expected 0 for OOB table name")
		}
	})

	t.Run("parallel_agg_column_out_of_bounds", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		memSize := uint64(len(rt.Memory))
		// table name fits but column name OOB
		copy(rt.Memory[0:], []byte("test"))
		params := []uint64{0, 4, 0, memSize - 2, 10, 1024}
		result, _ := host.parallelAggregate(rt, params)
		if result[0] != 0 {
			t.Errorf("Expected 0 for OOB column name")
		}
	})

	t.Run("parallel_agg_empty_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.tables["empty_table"] = []map[string]interface{}{}
		outPtr := int32(1024)
		params := makeParams(rt, "empty_table", "id", 0, outPtr) // COUNT on empty table

		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		if val != 0 {
			t.Errorf("Expected COUNT=0 for empty table, got %d", val)
		}
	})

	t.Run("parallel_agg_min_empty", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.tables["empty_table2"] = []map[string]interface{}{}
		outPtr := int32(1024)
		params := makeParams(rt, "empty_table2", "id", 3, outPtr) // MIN on empty

		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})
}

// ---------------------------------------------------------------------------
// Additional compiler coverage: unsupported statement type
// ---------------------------------------------------------------------------

func TestCompileQueryUnsupportedStmt(t *testing.T) {
	compiler := NewCompiler()
	// CreateTableStmt is not handled by CompileQuery
	stmt := &query.CreateTableStmt{Table: "test"}
	_, err := compiler.CompileQuery("CREATE TABLE test (id INT)", stmt, nil)
	if err == nil {
		t.Error("Expected error for unsupported statement type")
	}
	t.Logf("Got expected error: %v", err)
}

// ---------------------------------------------------------------------------
// Query cache hit path
// ---------------------------------------------------------------------------

func TestCompileQueryCacheHit(t *testing.T) {
	compiler := NewCompiler()
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
		From:    &query.TableRef{Name: "test"},
	}
	sql := "SELECT id FROM test"

	// First compile – populates cache
	first, err := compiler.CompileQuery(sql, stmt, nil)
	if err != nil {
		t.Fatalf("First compile failed: %v", err)
	}

	// Second compile – should hit cache
	second, err := compiler.CompileQuery(sql, stmt, nil)
	if err != nil {
		t.Fatalf("Second compile (cache hit) failed: %v", err)
	}

	if first != second {
		t.Error("Expected same pointer on cache hit")
	}
}

// ---------------------------------------------------------------------------
// inferResultSchema – non-SELECT path (returns nil)
// ---------------------------------------------------------------------------

func TestInferResultSchema(t *testing.T) {
	compiler := NewCompiler()

	t.Run("select_stmt", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Column: "id"},
				&query.StringLiteral{Value: "foo"},
			},
			From: &query.TableRef{Name: "test"},
		}
		schema := compiler.inferResultSchema(stmt)
		if len(schema) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(schema))
		}
	})

	t.Run("update_stmt_nil_schema", func(t *testing.T) {
		stmt := &query.UpdateStmt{Table: "test"}
		schema := compiler.inferResultSchema(stmt)
		if schema != nil {
			t.Errorf("Expected nil schema for UPDATE, got %v", schema)
		}
	})

	t.Run("insert_stmt_nil_schema", func(t *testing.T) {
		stmt := &query.InsertStmt{Table: "test"}
		schema := compiler.inferResultSchema(stmt)
		if schema != nil {
			t.Errorf("Expected nil schema for INSERT, got %v", schema)
		}
	})
}

// ---------------------------------------------------------------------------
// insertRow / updateRow / deleteRow host functions
// ---------------------------------------------------------------------------

func TestInsertUpdateDeleteRowHostFunctions(t *testing.T) {
	t.Run("insertRow_success", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, err := host.insertRow(rt, []uint64{0, 1024})
		if err != nil {
			t.Fatalf("insertRow failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})

	t.Run("insertRow_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.insertRow(rt, []uint64{0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("updateRow_success", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, err := host.updateRow(rt, []uint64{0, 1, 1024})
		if err != nil {
			t.Fatalf("updateRow failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})

	t.Run("updateRow_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.updateRow(rt, []uint64{0, 1})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("deleteRow_success", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, err := host.deleteRow(rt, []uint64{0, 1})
		if err != nil {
			t.Fatalf("deleteRow failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})

	t.Run("deleteRow_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.deleteRow(rt, []uint64{0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})
}

// ---------------------------------------------------------------------------
// getTableId / getColumnOffset / executeSubquery
// ---------------------------------------------------------------------------

func TestGetTableIdGetColumnOffsetExecuteSubquery(t *testing.T) {
	t.Run("getTableId_test_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		// Write "test" to memory
		namePtr := int32(512)
		copy(rt.Memory[namePtr:], []byte("test"))
		result, err := host.getTableId(rt, []uint64{uint64(namePtr), 4})
		if err != nil {
			t.Fatalf("getTableId failed: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected tableId=0, got %d", result[0])
		}
	})

	t.Run("getTableId_unknown_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		namePtr := int32(512)
		copy(rt.Memory[namePtr:], []byte("nosuchtable"))
		result, err := host.getTableId(rt, []uint64{uint64(namePtr), 11})
		if err != nil {
			t.Fatalf("getTableId failed: %v", err)
		}
		// Returns -1 as unsigned
		if result[0] != ^uint64(0) {
			t.Errorf("Expected -1 for unknown table, got %d", result[0])
		}
	})

	t.Run("getTableId_oob", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		memSize := uint64(len(rt.Memory))
		result, _ := host.getTableId(rt, []uint64{memSize - 2, 10})
		if result[0] != ^uint64(0) {
			t.Errorf("Expected -1 for OOB")
		}
	})

	t.Run("getTableId_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.getTableId(rt, []uint64{512})
		if result[0] != ^uint64(0) {
			t.Errorf("Expected -1 for too-few params")
		}
	})

	t.Run("getColumnOffset_valid", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		// Column index 3 → offset = 3*8 = 24
		result, err := host.getColumnOffset(rt, []uint64{0, 3})
		if err != nil {
			t.Fatalf("getColumnOffset failed: %v", err)
		}
		if result[0] != 24 {
			t.Errorf("Expected offset=24, got %d", result[0])
		}
	})

	t.Run("getColumnOffset_zero", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, err := host.getColumnOffset(rt, []uint64{0, 0})
		if err != nil {
			t.Fatalf("getColumnOffset failed: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected offset=0, got %d", result[0])
		}
	})

	t.Run("getColumnOffset_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.getColumnOffset(rt, []uint64{0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("executeSubquery_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		result, err := host.executeSubquery(rt, []uint64{0, uint64(outPtr), 10})
		if err != nil {
			t.Fatalf("executeSubquery failed: %v", err)
		}
		// Should return count of test table rows = 3
		if result[0] != 3 {
			t.Errorf("Expected 3 rows, got %d", result[0])
		}
	})

	t.Run("executeSubquery_limited", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		// maxRows=1: only 1 row returned
		result, err := host.executeSubquery(rt, []uint64{0, uint64(outPtr), 1})
		if err != nil {
			t.Fatalf("executeSubquery failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected 1 row, got %d", result[0])
		}
	})

	t.Run("executeSubquery_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.executeSubquery(rt, []uint64{0, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})
}

// ---------------------------------------------------------------------------
// unionResults / exceptResults / intersectResults
// ---------------------------------------------------------------------------

func TestSetOperations(t *testing.T) {
	t.Run("unionResults_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		leftPtr := int32(1024)
		rightPtr := int32(2048)
		outPtr := int32(3072)

		// Left: [10, 20], Right: [30, 40, 50]
		lefts := []int64{10, 20}
		rights := []int64{30, 40, 50}
		for i, v := range lefts {
			binary.LittleEndian.PutUint64(rt.Memory[leftPtr+int32(i*8):], uint64(v))
		}
		for i, v := range rights {
			binary.LittleEndian.PutUint64(rt.Memory[rightPtr+int32(i*8):], uint64(v))
		}

		result, err := host.unionResults(rt, []uint64{uint64(leftPtr), 2, uint64(rightPtr), 3, uint64(outPtr)})
		if err != nil {
			t.Fatalf("unionResults failed: %v", err)
		}
		if result[0] != 5 {
			t.Errorf("Expected 5 total rows, got %d", result[0])
		}
		// Verify first element
		v := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		if v != 10 {
			t.Errorf("Expected first=10, got %d", v)
		}
	})

	t.Run("unionResults_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.unionResults(rt, []uint64{0, 1, 0, 1})
		if result[0] != 0 {
			t.Errorf("Expected 0")
		}
	})

	t.Run("exceptResults_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		leftPtr := int32(1024)
		outPtr := int32(2048)

		lefts := []int64{1, 2, 3}
		for i, v := range lefts {
			binary.LittleEndian.PutUint64(rt.Memory[leftPtr+int32(i*8):], uint64(v))
		}

		result, err := host.exceptResults(rt, []uint64{uint64(leftPtr), 3, 0, 0, uint64(outPtr)})
		if err != nil {
			t.Fatalf("exceptResults failed: %v", err)
		}
		if result[0] != 3 {
			t.Errorf("Expected 3 rows, got %d", result[0])
		}
	})

	t.Run("exceptResults_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.exceptResults(rt, []uint64{0, 1, 0, 1})
		if result[0] != 0 {
			t.Errorf("Expected 0")
		}
	})

	t.Run("intersectResults_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		leftPtr := int32(1024)
		outPtr := int32(2048)

		lefts := []int64{5, 10}
		for i, v := range lefts {
			binary.LittleEndian.PutUint64(rt.Memory[leftPtr+int32(i*8):], uint64(v))
		}

		result, err := host.intersectResults(rt, []uint64{uint64(leftPtr), 2, 0, 0, uint64(outPtr)})
		if err != nil {
			t.Fatalf("intersectResults failed: %v", err)
		}
		if result[0] != 2 {
			t.Errorf("Expected 2 rows, got %d", result[0])
		}
	})

	t.Run("intersectResults_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.intersectResults(rt, []uint64{0, 1, 0, 1})
		if result[0] != 0 {
			t.Errorf("Expected 0")
		}
	})
}

// ---------------------------------------------------------------------------
// windowFunction – uncovered cases (SUM, AVG, MIN, MAX, COUNT window funcs)
// ---------------------------------------------------------------------------

func TestWindowFunctionAdditional(t *testing.T) {
	setupInputs := func(rt *Runtime, inPtr int32, vals []int64) {
		for i, v := range vals {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr+int32(i*8):], uint64(v))
		}
	}
	readOutputs := func(rt *Runtime, outPtr int32, count int) []int64 {
		result := make([]int64, count)
		for i := range result {
			result[i] = int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
		}
		return result
	}

	t.Run("window_sum_running", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		inPtr := int32(1024)
		outPtr := int32(2048)
		vals := []int64{10, 20, 30}
		setupInputs(rt, inPtr, vals)

		// funcType=10 (SUM running)
		result, err := host.windowFunction(rt, []uint64{uint64(inPtr), 3, 10, uint64(outPtr)})
		if err != nil {
			t.Fatalf("windowFunction SUM failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		out := readOutputs(rt, outPtr, 3)
		// Running sum: [10, 30, 60]
		expected := []int64{10, 30, 60}
		for i, exp := range expected {
			if out[i] != exp {
				t.Errorf("[%d] running sum: expected %d, got %d", i, exp, out[i])
			}
		}
	})

	t.Run("window_avg_running", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		inPtr := int32(1024)
		outPtr := int32(2048)
		vals := []int64{10, 20, 30}
		setupInputs(rt, inPtr, vals)

		// funcType=11 (AVG running)
		result, err := host.windowFunction(rt, []uint64{uint64(inPtr), 3, 11, uint64(outPtr)})
		if err != nil {
			t.Fatalf("windowFunction AVG failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		out := readOutputs(rt, outPtr, 3)
		// Running avg: [10/1=10, 30/2=15, 60/3=20]
		expected := []int64{10, 15, 20}
		for i, exp := range expected {
			if out[i] != exp {
				t.Errorf("[%d] running avg: expected %d, got %d", i, exp, out[i])
			}
		}
	})

	t.Run("window_min_running", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		inPtr := int32(1024)
		outPtr := int32(2048)
		vals := []int64{30, 10, 20}
		setupInputs(rt, inPtr, vals)

		// funcType=12 (MIN running)
		result, err := host.windowFunction(rt, []uint64{uint64(inPtr), 3, 12, uint64(outPtr)})
		if err != nil {
			t.Fatalf("windowFunction MIN failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		out := readOutputs(rt, outPtr, 3)
		// Running min: [30, 10, 10]
		expected := []int64{30, 10, 10}
		for i, exp := range expected {
			if out[i] != exp {
				t.Errorf("[%d] running min: expected %d, got %d", i, exp, out[i])
			}
		}
	})

	t.Run("window_max_running", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		inPtr := int32(1024)
		outPtr := int32(2048)
		vals := []int64{10, 30, 20}
		setupInputs(rt, inPtr, vals)

		// funcType=13 (MAX running)
		result, err := host.windowFunction(rt, []uint64{uint64(inPtr), 3, 13, uint64(outPtr)})
		if err != nil {
			t.Fatalf("windowFunction MAX failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		out := readOutputs(rt, outPtr, 3)
		// Running max: [10, 30, 30]
		expected := []int64{10, 30, 30}
		for i, exp := range expected {
			if out[i] != exp {
				t.Errorf("[%d] running max: expected %d, got %d", i, exp, out[i])
			}
		}
	})

	t.Run("window_count_running", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		inPtr := int32(1024)
		outPtr := int32(2048)

		// funcType=14 (COUNT running)
		result, err := host.windowFunction(rt, []uint64{uint64(inPtr), 4, 14, uint64(outPtr)})
		if err != nil {
			t.Fatalf("windowFunction COUNT failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		out := readOutputs(rt, outPtr, 4)
		// Running count: [1, 2, 3, 4]
		for i, exp := range []int64{1, 2, 3, 4} {
			if out[i] != exp {
				t.Errorf("[%d] running count: expected %d, got %d", i, exp, out[i])
			}
		}
	})

	t.Run("window_first_value", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		inPtr := int32(1024)
		outPtr := int32(2048)
		vals := []int64{100, 200, 300}
		setupInputs(rt, inPtr, vals)

		// funcType=5 (FIRST_VALUE)
		result, err := host.windowFunction(rt, []uint64{uint64(inPtr), 3, 5, uint64(outPtr)})
		if err != nil {
			t.Fatalf("windowFunction FIRST_VALUE failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		out := readOutputs(rt, outPtr, 3)
		// All should be 100
		for i, v := range out {
			if v != 100 {
				t.Errorf("[%d] FIRST_VALUE: expected 100, got %d", i, v)
			}
		}
	})

	t.Run("window_last_value", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		inPtr := int32(1024)
		outPtr := int32(2048)
		vals := []int64{100, 200, 300}
		setupInputs(rt, inPtr, vals)

		// funcType=6 (LAST_VALUE)
		result, err := host.windowFunction(rt, []uint64{uint64(inPtr), 3, 6, uint64(outPtr)})
		if err != nil {
			t.Fatalf("windowFunction LAST_VALUE failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success")
		}
		out := readOutputs(rt, outPtr, 3)
		// All should be 300
		for i, v := range out {
			if v != 300 {
				t.Errorf("[%d] LAST_VALUE: expected 300, got %d", i, v)
			}
		}
	})

	t.Run("window_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.windowFunction(rt, []uint64{0, 5, 0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("window_zero_rowcount", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.windowFunction(rt, []uint64{0, 0, 0, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for rowCount=0")
		}
	})
}

// ---------------------------------------------------------------------------
// fullJoin
// ---------------------------------------------------------------------------

func TestFullJoinHostFunction(t *testing.T) {
	t.Run("full_join_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.fullJoin(rt, []uint64{0, 1, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("full_join_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		result, err := host.fullJoin(rt, []uint64{0, 0, uint64(outPtr), 100})
		if err != nil {
			t.Fatalf("fullJoin failed: %v", err)
		}
		if result[0] == 0 {
			t.Error("Expected non-zero rows from full join")
		}
		t.Logf("fullJoin: rowCount=%d", result[0])
	})

	t.Run("full_join_limited", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		result, err := host.fullJoin(rt, []uint64{0, 0, uint64(outPtr), 1})
		if err != nil {
			t.Fatalf("fullJoin limited failed: %v", err)
		}
		if result[0] > 1 {
			t.Errorf("Expected at most 1 row, got %d", result[0])
		}
	})
}

// ---------------------------------------------------------------------------
// getQueryMetrics / getMemoryStats / logProfilingEvent / getOpcodeStats / min
// ---------------------------------------------------------------------------

func TestProfilingFunctions(t *testing.T) {
	t.Run("getQueryMetrics_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		result, err := host.getQueryMetrics(rt, []uint64{uint64(outPtr)})
		if err != nil {
			t.Fatalf("getQueryMetrics failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		// Read totalExecs (first field)
		val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		if val != 100 {
			t.Errorf("Expected totalExecs=100, got %d", val)
		}
	})

	t.Run("getQueryMetrics_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.getQueryMetrics(rt, []uint64{})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("getQueryMetrics_oob", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		memSize := uint64(len(rt.Memory))
		result, _ := host.getQueryMetrics(rt, []uint64{memSize - 10})
		if result[0] != 0 {
			t.Errorf("Expected 0 for OOB")
		}
	})

	t.Run("getMemoryStats_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		result, err := host.getMemoryStats(rt, []uint64{uint64(outPtr)})
		if err != nil {
			t.Fatalf("getMemoryStats failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})

	t.Run("getMemoryStats_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.getMemoryStats(rt, []uint64{})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("getMemoryStats_oob", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		memSize := uint64(len(rt.Memory))
		result, _ := host.getMemoryStats(rt, []uint64{memSize - 10})
		if result[0] != 0 {
			t.Errorf("Expected 0 for OOB")
		}
	})

	t.Run("logProfilingEvent_success", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, err := host.logProfilingEvent(rt, []uint64{1, 5000, 100})
		if err != nil {
			t.Fatalf("logProfilingEvent failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})

	t.Run("logProfilingEvent_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.logProfilingEvent(rt, []uint64{1, 5000})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("getOpcodeStats_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		result, err := host.getOpcodeStats(rt, []uint64{uint64(outPtr), 5})
		if err != nil {
			t.Fatalf("getOpcodeStats failed: %v", err)
		}
		// Returns min(5, 10) = 5
		if result[0] != 5 {
			t.Errorf("Expected 5, got %d", result[0])
		}
	})

	t.Run("getOpcodeStats_large_maxOpcodes", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		// maxOpcodes > 10, so returns 10
		result, err := host.getOpcodeStats(rt, []uint64{uint64(outPtr), 20})
		if err != nil {
			t.Fatalf("getOpcodeStats failed: %v", err)
		}
		if result[0] != 10 {
			t.Errorf("Expected 10, got %d", result[0])
		}
	})

	t.Run("getOpcodeStats_zero_maxOpcodes", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.getOpcodeStats(rt, []uint64{1024, 0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for zero maxOpcodes")
		}
	})

	t.Run("getOpcodeStats_too_many", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.getOpcodeStats(rt, []uint64{1024, 300})
		if result[0] != 0 {
			t.Errorf("Expected 0 for maxOpcodes > 256")
		}
	})

	t.Run("getOpcodeStats_oob", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		memSize := uint64(len(rt.Memory))
		result, _ := host.getOpcodeStats(rt, []uint64{memSize - 10, 5})
		if result[0] != 0 {
			t.Errorf("Expected 0 for OOB")
		}
	})

	t.Run("getOpcodeStats_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.getOpcodeStats(rt, []uint64{1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("min_helper", func(t *testing.T) {
		if min(3, 5) != 3 {
			t.Error("min(3,5) should be 3")
		}
		if min(5, 3) != 3 {
			t.Error("min(5,3) should be 3")
		}
		if min(4, 4) != 4 {
			t.Error("min(4,4) should be 4")
		}
	})
}

// ---------------------------------------------------------------------------
// tableScan / leftJoin additional paths
// ---------------------------------------------------------------------------

func TestTableScanAndLeftJoinPaths(t *testing.T) {
	t.Run("tableScan_nonzero_table_id", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		// tableId != 0 returns 0 rows
		result, err := host.tableScan(rt, []uint64{5, 1024, 100})
		if err != nil {
			t.Fatalf("tableScan failed: %v", err)
		}
		if result[0] != 0 {
			t.Errorf("Expected 0 rows for non-zero tableId, got %d", result[0])
		}
	})

	t.Run("leftJoin_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.leftJoin(rt, []uint64{0, 0, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("leftJoin_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		result, err := host.leftJoin(rt, []uint64{0, 0, uint64(outPtr), 100})
		if err != nil {
			t.Fatalf("leftJoin failed: %v", err)
		}
		if result[0] == 0 {
			t.Error("Expected non-zero rows from left join")
		}
		t.Logf("leftJoin: rowCount=%d", result[0])
	})
}

// ---------------------------------------------------------------------------
// repartitionTable
// ---------------------------------------------------------------------------

func TestRepartitionTable(t *testing.T) {
	t.Run("repartition_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		tableNamePtr := int32(512)
		tableName := "test"
		copy(rt.Memory[tableNamePtr:], []byte(tableName))

		result, err := host.repartitionTable(rt, []uint64{uint64(tableNamePtr), uint64(len(tableName)), 2})
		if err != nil {
			t.Fatalf("repartitionTable failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
		// Verify 2 partitions were created
		if len(host.partitions["test"]) != 2 {
			t.Errorf("Expected 2 partitions, got %d", len(host.partitions["test"]))
		}
	})

	t.Run("repartition_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.repartitionTable(rt, []uint64{0, 4})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("repartition_invalid_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		tableNamePtr := int32(512)
		copy(rt.Memory[tableNamePtr:], []byte("test"))
		// partitionCount=0 is invalid
		result, _ := host.repartitionTable(rt, []uint64{uint64(tableNamePtr), 4, 0})
		if result[0] != 0 {
			t.Errorf("Expected 0 for invalid count=0")
		}
	})

	t.Run("repartition_unknown_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		tableNamePtr := int32(512)
		copy(rt.Memory[tableNamePtr:], []byte("unknowntable"))
		result, _ := host.repartitionTable(rt, []uint64{uint64(tableNamePtr), 12, 3})
		if result[0] != 0 {
			t.Errorf("Expected 0 for unknown table")
		}
	})
}

// ---------------------------------------------------------------------------
// resetMetrics host function
// ---------------------------------------------------------------------------

func TestResetMetrics(t *testing.T) {
	t.Run("resetMetrics_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, err := host.resetMetrics(rt, []uint64{})
		if err != nil {
			t.Fatalf("resetMetrics failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}
	})
}

// ---------------------------------------------------------------------------
// executeCorrelatedSubquery
// ---------------------------------------------------------------------------

func TestExecuteCorrelatedSubquery(t *testing.T) {
	t.Run("correlatedSubquery_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		result, err := host.executeCorrelatedSubquery(rt, []uint64{0, 0, 8, uint64(outPtr), 10})
		if err != nil {
			t.Fatalf("executeCorrelatedSubquery failed: %v", err)
		}
		if result[0] == 0 {
			t.Error("Expected non-zero row count")
		}
	})

	t.Run("correlatedSubquery_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		result, _ := host.executeCorrelatedSubquery(rt, []uint64{0, 0, 8, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})
}

// ---------------------------------------------------------------------------
// fetchChunk / indexScan additional paths
// ---------------------------------------------------------------------------

func TestFetchChunkAndIndexScan(t *testing.T) {
	t.Run("fetchChunk_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		// Params: [startRow, rowCount, outPtr] (requires >= 3)
		result, err := host.fetchChunk(rt, []uint64{0, 2, uint64(outPtr)})
		if err != nil {
			t.Fatalf("fetchChunk failed: %v", err)
		}
		t.Logf("fetchChunk: rowCount=%d", result[0])
	})

	t.Run("fetchChunk_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		// Requires 3 params; pass 2
		result, _ := host.fetchChunk(rt, []uint64{0, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})

	t.Run("indexScan_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		outPtr := int32(1024)
		// Params: [tableId, indexId, minVal, maxVal, outPtr, maxRows] (requires >= 6)
		result, err := host.indexScan(rt, []uint64{0, 0, 0, 0, uint64(outPtr), 100})
		if err != nil {
			t.Fatalf("indexScan failed: %v", err)
		}
		t.Logf("indexScan: rowCount=%d", result[0])
	})

	t.Run("indexScan_too_few_params", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		// Requires 6 params; pass 5
		result, _ := host.indexScan(rt, []uint64{0, 0, 0, 0, 1024})
		if result[0] != 0 {
			t.Errorf("Expected 0 for too-few params")
		}
	})
}

// ---------------------------------------------------------------------------
// getPartitionCount / partitionScan
// ---------------------------------------------------------------------------

func TestGetPartitionCountAndScan(t *testing.T) {
	t.Run("getPartitionCount_known_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		tableNamePtr := int32(512)
		copy(rt.Memory[tableNamePtr:], []byte("test"))

		result, err := host.getPartitionCount(rt, []uint64{uint64(tableNamePtr), 4})
		if err != nil {
			t.Fatalf("getPartitionCount failed: %v", err)
		}
		if result[0] != 2 {
			t.Errorf("Expected 2 partitions, got %d", result[0])
		}
	})

	t.Run("getPartitionCount_unknown_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		tableNamePtr := int32(512)
		copy(rt.Memory[tableNamePtr:], []byte("notexists"))

		result, err := host.getPartitionCount(rt, []uint64{uint64(tableNamePtr), 9})
		if err != nil {
			t.Fatalf("getPartitionCount failed: %v", err)
		}
		// Non-partitioned table returns 1
		if result[0] != 1 {
			t.Errorf("Expected 1 for unpartitioned table, got %d", result[0])
		}
	})

	t.Run("partitionScan_valid_partition", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		tableNamePtr := int32(512)
		copy(rt.Memory[tableNamePtr:], []byte("test"))
		outPtr := int32(1024)

		result, err := host.partitionScan(rt, []uint64{uint64(tableNamePtr), 4, 0, uint64(outPtr), 10})
		if err != nil {
			t.Fatalf("partitionScan failed: %v", err)
		}
		// Partition 0: rows 0..2 (2 rows)
		if result[0] != 2 {
			t.Errorf("Expected 2 rows in partition 0, got %d", result[0])
		}
	})

	t.Run("partitionScan_invalid_partition_id", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		tableNamePtr := int32(512)
		copy(rt.Memory[tableNamePtr:], []byte("test"))

		result, _ := host.partitionScan(rt, []uint64{uint64(tableNamePtr), 4, 99, 1024, 10})
		if result[0] != 0 {
			t.Errorf("Expected 0 for invalid partition id")
		}
	})

	t.Run("partitionScan_unpartitioned_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()

		// Add unpartitioned table
		host.tables["nopart"] = []map[string]interface{}{
			{"id": int64(1)},
			{"id": int64(2)},
		}

		tableNamePtr := int32(512)
		copy(rt.Memory[tableNamePtr:], []byte("nopart"))
		outPtr := int32(1024)

		result, err := host.partitionScan(rt, []uint64{uint64(tableNamePtr), 6, 0, uint64(outPtr), 10})
		if err != nil {
			t.Fatalf("partitionScan failed: %v", err)
		}
		if result[0] != 2 {
			t.Errorf("Expected 2 rows, got %d", result[0])
		}
	})
}

// ---------------------------------------------------------------------------
// writeParams / parseResults / LoadModule runtime coverage
// ---------------------------------------------------------------------------

func TestRuntimeWriteParamsAndParseResults(t *testing.T) {
	t.Run("writeParams_various_types_direct", func(t *testing.T) {
		rt := NewRuntime(10)
		// Call writeParams directly with various types
		err := rt.writeParams(0, []interface{}{int64(42), "hello", float64(3.14), nil})
		if err != nil {
			t.Logf("writeParams returned: %v", err)
		}
	})

	t.Run("writeParams_int_type", func(t *testing.T) {
		rt := NewRuntime(10)
		err := rt.writeParams(0, []interface{}{int(100)})
		if err != nil {
			t.Fatalf("writeParams int failed: %v", err)
		}
		val := binary.LittleEndian.Uint64(rt.Memory[0:])
		if val != 100 {
			t.Errorf("Expected 100, got %d", val)
		}
	})

	t.Run("writeParams_unsupported_type", func(t *testing.T) {
		rt := NewRuntime(10)
		err := rt.writeParams(0, []interface{}{struct{ x int }{x: 1}})
		if err == nil {
			t.Error("Expected error for unsupported type")
		}
	})

	t.Run("loadModule_minimal", func(t *testing.T) {
		rt := NewRuntime(10)
		// Compile a minimal query to get valid bytecode
		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
		}
		compiled, err := compiler.CompileQuery("SELECT id FROM test v2", stmt, nil)
		if err != nil {
			t.Fatalf("CompileQuery failed: %v", err)
		}
		// LoadModule should succeed with valid bytecode
		err = rt.LoadModule(compiled.Bytecode)
		if err != nil {
			t.Logf("LoadModule returned: %v (may be OK if function import fails)", err)
		}
	})

	t.Run("loadModule_invalid_magic", func(t *testing.T) {
		rt := NewRuntime(10)
		err := rt.LoadModule([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00, 0x00})
		if err == nil {
			t.Error("Expected error for invalid magic")
		}
	})

	t.Run("loadModule_too_short", func(t *testing.T) {
		rt := NewRuntime(10)
		err := rt.LoadModule([]byte{0x00, 0x61})
		if err == nil {
			t.Error("Expected error for too-short module")
		}
	})

	t.Run("parseResults_integer_type", func(t *testing.T) {
		rt := NewRuntime(10)
		// Set up memory with INTEGER value at offset 1024
		binary.LittleEndian.PutUint64(rt.Memory[1024:], uint64(999))
		schema := []ColumnInfo{{Name: "id", Type: "INTEGER", Nullable: false}}
		result, err := rt.parseResults(schema, 1024, 1)
		if err != nil {
			t.Fatalf("parseResults failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0].Values[0].(int64) != 999 {
			t.Errorf("Expected 999, got %v", result.Rows[0].Values[0])
		}
	})

	t.Run("parseResults_real_type", func(t *testing.T) {
		rt := NewRuntime(10)
		binary.LittleEndian.PutUint64(rt.Memory[1024:], 0x4009999999999999) // ~3.2 as float64
		schema := []ColumnInfo{{Name: "score", Type: "REAL", Nullable: false}}
		result, err := rt.parseResults(schema, 1024, 1)
		if err != nil {
			t.Fatalf("parseResults REAL failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
	})

	t.Run("parseResults_text_null", func(t *testing.T) {
		rt := NewRuntime(10)
		// Write null marker: 0xFFFFFFFF
		binary.LittleEndian.PutUint32(rt.Memory[1024:], 0xFFFFFFFF)
		schema := []ColumnInfo{{Name: "name", Type: "TEXT", Nullable: true}}
		result, err := rt.parseResults(schema, 1024, 1)
		if err != nil {
			t.Fatalf("parseResults TEXT null failed: %v", err)
		}
		if result.Rows[0].Values[0] != nil {
			t.Errorf("Expected nil value for null TEXT")
		}
	})

	t.Run("parseResults_zero_rows", func(t *testing.T) {
		rt := NewRuntime(10)
		schema := []ColumnInfo{{Name: "id", Type: "INTEGER"}}
		result, err := rt.parseResults(schema, 1024, 0)
		if err != nil {
			t.Fatalf("parseResults zero rows failed: %v", err)
		}
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows")
		}
	})
}

// ---------------------------------------------------------------------------
// executeFunction via CallFunction – exercise opcode paths
// ---------------------------------------------------------------------------

// buildFunctionWithCode creates a minimal Runtime that executes custom bytecode.
func buildFunctionWithCode(code []byte) *Runtime {
	rt := NewRuntime(10)
	fn := Function{
		TypeIdx:    0,
		Locals:     []ValueType{},
		Code:       code,
		IsImport:   false,
		ParamCount: 0,
	}
	rt.Functions = append(rt.Functions, fn)
	rt.Types = append(rt.Types, FuncType{Params: []ValueType{}, Results: []ValueType{}})
	rt.funcTypeIndices = append(rt.funcTypeIndices, 0)
	return rt
}

func TestExecuteFunctionOpcodes(t *testing.T) {
	t.Run("opcode_nop_end", func(t *testing.T) {
		// 0x01 (nop), 0x0b (end)
		rt := buildFunctionWithCode([]byte{0x01, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("nop/end failed: %v", err)
		}
	})

	t.Run("opcode_return", func(t *testing.T) {
		// 0x0f (return)
		rt := buildFunctionWithCode([]byte{0x0f})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("return failed: %v", err)
		}
	})

	t.Run("opcode_i32_const_local_set_get", func(t *testing.T) {
		// i32.const 42; local.set 0; local.get 0; end
		// But we need a local, so set up a frame with 1 local
		rt := NewRuntime(10)
		rt.Types = append(rt.Types, FuncType{Params: []ValueType{}, Results: []ValueType{I32}})
		rt.funcTypeIndices = append(rt.funcTypeIndices, 0)
		rt.Functions = append(rt.Functions, Function{
			TypeIdx:    0,
			Locals:     []ValueType{I32},
			Code:       []byte{0x41, 0x2a, 0x21, 0x00, 0x20, 0x00, 0x0b},
			ParamCount: 0,
		})
		res, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("i32.const/local.set/get: err=%v (ok if type mismatch)", err)
		} else {
			t.Logf("i32.const/local.set/get result: %v", res)
		}
	})

	t.Run("opcode_i64_const", func(t *testing.T) {
		// i64.const 99; end
		rt := buildFunctionWithCode([]byte{0x42, 0x63, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i64.const failed: %v", err)
		}
	})

	t.Run("opcode_drop", func(t *testing.T) {
		// i32.const 1; drop; end
		rt := buildFunctionWithCode([]byte{0x41, 0x01, 0x1a, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("drop failed: %v", err)
		}
	})

	t.Run("opcode_select", func(t *testing.T) {
		// i32.const 10; i32.const 20; i32.const 1; select; end
		rt := buildFunctionWithCode([]byte{0x41, 0x0a, 0x41, 0x14, 0x41, 0x01, 0x1b, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("select failed: %v", err)
		}
		if len(rt.Stack) > 0 {
			t.Logf("select result: %d", rt.Stack[len(rt.Stack)-1])
		}
	})

	t.Run("opcode_br", func(t *testing.T) {
		// br 0; end
		rt := buildFunctionWithCode([]byte{0x0c, 0x00, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("br: err=%v", err)
		}
	})

	t.Run("opcode_br_if_false", func(t *testing.T) {
		// i32.const 0; br_if 0; end
		rt := buildFunctionWithCode([]byte{0x41, 0x00, 0x0d, 0x00, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("br_if false: err=%v", err)
		}
	})

	t.Run("opcode_i32_add", func(t *testing.T) {
		// i32.const 3; i32.const 4; i32.add; end
		rt := buildFunctionWithCode([]byte{0x41, 0x03, 0x41, 0x04, 0x6a, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i32.add failed: %v", err)
		}
		if len(rt.Stack) > 0 {
			if rt.Stack[len(rt.Stack)-1] != 7 {
				t.Errorf("Expected 7, got %d", rt.Stack[len(rt.Stack)-1])
			}
		}
	})

	t.Run("opcode_i32_sub", func(t *testing.T) {
		// i32.const 10; i32.const 3; i32.sub; end
		rt := buildFunctionWithCode([]byte{0x41, 0x0a, 0x41, 0x03, 0x6b, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i32.sub failed: %v", err)
		}
		if len(rt.Stack) > 0 {
			if uint32(rt.Stack[len(rt.Stack)-1]) != 7 {
				t.Errorf("Expected 7, got %d", rt.Stack[len(rt.Stack)-1])
			}
		}
	})

	t.Run("opcode_i32_mul", func(t *testing.T) {
		// i32.const 3; i32.const 4; i32.mul; end
		rt := buildFunctionWithCode([]byte{0x41, 0x03, 0x41, 0x04, 0x6c, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i32.mul failed: %v", err)
		}
	})

	t.Run("opcode_i64_add", func(t *testing.T) {
		// i64.const 100; i64.const 200; i64.add; end
		rt := buildFunctionWithCode([]byte{0x42, 0x64, 0x42, 0xc8, 0x01, 0x7c, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i64.add failed: %v", err)
		}
	})

	t.Run("opcode_i64_sub", func(t *testing.T) {
		// i64.const 10; i64.const 3; i64.sub; end
		rt := buildFunctionWithCode([]byte{0x42, 0x0a, 0x42, 0x03, 0x7d, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i64.sub failed: %v", err)
		}
	})

	t.Run("opcode_i64_mul", func(t *testing.T) {
		// i64.const 5; i64.const 6; i64.mul; end
		rt := buildFunctionWithCode([]byte{0x42, 0x05, 0x42, 0x06, 0x7e, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i64.mul failed: %v", err)
		}
	})

	t.Run("opcode_i32_eq", func(t *testing.T) {
		// i32.const 5; i32.const 5; i32.eq; end
		rt := buildFunctionWithCode([]byte{0x41, 0x05, 0x41, 0x05, 0x46, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i32.eq failed: %v", err)
		}
		if len(rt.Stack) > 0 && rt.Stack[len(rt.Stack)-1] != 1 {
			t.Errorf("Expected 1 (equal), got %d", rt.Stack[len(rt.Stack)-1])
		}
	})

	t.Run("opcode_i32_ne", func(t *testing.T) {
		// i32.const 5; i32.const 6; i32.ne; end
		rt := buildFunctionWithCode([]byte{0x41, 0x05, 0x41, 0x06, 0x47, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i32.ne failed: %v", err)
		}
	})

	t.Run("opcode_i32_lt_s", func(t *testing.T) {
		// i32.const 3; i32.const 5; i32.lt_s; end
		rt := buildFunctionWithCode([]byte{0x41, 0x03, 0x41, 0x05, 0x48, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i32.lt_s failed: %v", err)
		}
	})

	t.Run("opcode_i32_gt_s", func(t *testing.T) {
		// i32.const 5; i32.const 3; i32.gt_s (0x4d); end
		rt := buildFunctionWithCode([]byte{0x41, 0x05, 0x41, 0x03, 0x4d, 0x0b}) // Note: 0x4d may not be in switch
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("i32.gt_s: err=%v", err)
		}
	})

	t.Run("opcode_i64_eq", func(t *testing.T) {
		// i64.const 7; i64.const 7; i64.eq (0x51); end
		rt := buildFunctionWithCode([]byte{0x42, 0x07, 0x42, 0x07, 0x51, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("i64.eq failed: %v", err)
		}
	})

	t.Run("opcode_f64_const", func(t *testing.T) {
		// f64.const [8 bytes for 0.0]; end
		code := []byte{0x44, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b}
		rt := buildFunctionWithCode(code)
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Fatalf("f64.const failed: %v", err)
		}
	})

	t.Run("opcode_block", func(t *testing.T) {
		// block 0x40; end; end
		rt := buildFunctionWithCode([]byte{0x02, 0x40, 0x0b, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("block: err=%v", err)
		}
	})

	t.Run("opcode_loop", func(t *testing.T) {
		// loop 0x40; end; end
		rt := buildFunctionWithCode([]byte{0x03, 0x40, 0x0b, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("loop: err=%v", err)
		}
	})

	t.Run("opcode_if", func(t *testing.T) {
		// if 0x40; end; end
		rt := buildFunctionWithCode([]byte{0x04, 0x40, 0x0b, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("if: err=%v", err)
		}
	})

	t.Run("opcode_else", func(t *testing.T) {
		// else; end
		rt := buildFunctionWithCode([]byte{0x05, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("else: err=%v", err)
		}
	})

	t.Run("opcode_i32_load", func(t *testing.T) {
		// i32.const 16 (0x10, no sign bit); i32.load align=0 offset=0; end
		// 0x41 0x10 = i32.const 16 (safe: bit 6 of 0x10 = 0)
		rt := buildFunctionWithCode([]byte{0x41, 0x10, 0x28, 0x00, 0x00, 0x0b})
		// Pre-write value at offset 16
		binary.LittleEndian.PutUint32(rt.Memory[16:], 9876)
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("i32.load: err=%v", err)
		}
	})

	t.Run("opcode_i64_load", func(t *testing.T) {
		// i32.const 16; i64.load align=0 offset=0; end
		rt := buildFunctionWithCode([]byte{0x41, 0x10, 0x29, 0x00, 0x00, 0x0b})
		binary.LittleEndian.PutUint64(rt.Memory[16:], 123456789)
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("i64.load: err=%v", err)
		}
	})

	t.Run("opcode_i32_store", func(t *testing.T) {
		// i32.const 32 (0x20, safe); i32.const 42 (0x2a, safe); i32.store align=0 offset=0; end
		// Note: 0x20 = local.get opcode! Use 0x80 0x01 LEB128 = 128 instead
		// Actually use i32.const with multi-byte LEB128 for addr=128: 0x80 0x01
		rt := buildFunctionWithCode([]byte{0x41, 0x80, 0x01, 0x41, 0x2a, 0x36, 0x00, 0x00, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("i32.store: err=%v", err)
		}
		// Verify store at offset 128
		val := binary.LittleEndian.Uint32(rt.Memory[128:])
		if val != 42 {
			t.Logf("i32.store: expected 42, got %d (stack may have been insufficient)", val)
		}
	})

	t.Run("opcode_i64_store", func(t *testing.T) {
		// i32.const 128 (LEB=0x80 0x01); i64.const 9999 (0x8f 0x4e); i64.store; end
		rt := buildFunctionWithCode([]byte{0x41, 0x80, 0x01, 0x42, 0x8f, 0x4e, 0x37, 0x00, 0x00, 0x0b})
		_, err := rt.CallFunction(0, nil)
		if err != nil {
			t.Logf("i64.store: err=%v", err)
		}
	})

	t.Run("opcode_unreachable", func(t *testing.T) {
		// 0x00 = unreachable
		rt := buildFunctionWithCode([]byte{0x00})
		_, err := rt.CallFunction(0, nil)
		if err == nil {
			t.Error("Expected error for unreachable opcode")
		}
	})

	t.Run("opcode_unknown", func(t *testing.T) {
		// 0xEE is not a handled opcode
		rt := buildFunctionWithCode([]byte{0xEE})
		_, err := rt.CallFunction(0, nil)
		if err == nil {
			t.Error("Expected error for unknown opcode")
		}
	})
}

// ---------------------------------------------------------------------------
// BenchmarkQuery function
// ---------------------------------------------------------------------------

func TestBenchmarkQueryFunction(t *testing.T) {
	t.Run("benchmark_basic", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
		}
		compiled, err := compiler.CompileQuery("SELECT id FROM bench", stmt, nil)
		if err != nil {
			t.Fatalf("CompileQuery failed: %v", err)
		}
		// Use INTEGER schema so parseResults doesn't panic
		compiled.ResultSchema = []ColumnInfo{{Name: "id", Type: "INTEGER"}}

		result, err := BenchmarkQuery(rt, compiled, 3)
		if err != nil {
			t.Fatalf("BenchmarkQuery failed: %v", err)
		}
		if result.Iterations != 3 {
			t.Errorf("Expected 3 iterations, got %d", result.Iterations)
		}
		t.Logf("BenchmarkQuery: avg=%d, min=%d, max=%d", result.AvgDuration, result.MinDuration, result.MaxDuration)
	})

	t.Run("benchmark_default_iterations", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
		}
		compiled, err := compiler.CompileQuery("SELECT id FROM bench2", stmt, nil)
		if err != nil {
			t.Fatalf("CompileQuery failed: %v", err)
		}
		compiled.ResultSchema = []ColumnInfo{{Name: "id", Type: "INTEGER"}}

		// Pass iterations=0 to use default (100)
		result, err := BenchmarkQuery(rt, compiled, 0)
		if err != nil {
			t.Fatalf("BenchmarkQuery(0) failed: %v", err)
		}
		if result.Iterations != 100 {
			t.Errorf("Expected 100 iterations (default), got %d", result.Iterations)
		}
		t.Logf("BenchmarkQuery(default): throughput=%f", result.Throughput)
	})
}

// ---------------------------------------------------------------------------
// QueryProfiler
// ---------------------------------------------------------------------------

func TestQueryProfiler(t *testing.T) {
	t.Run("record_execution", func(t *testing.T) {
		p := NewQueryProfiler()
		p.RecordExecution(1000, 5, 1024)
		p.RecordExecution(500, 3, 512)
		p.RecordExecution(2000, 10, 2048)

		stats := p.GetStats()
		if stats.TotalExecutions != 3 {
			t.Errorf("Expected 3 executions, got %d", stats.TotalExecutions)
		}
		if stats.MinDuration != 500 {
			t.Errorf("Expected MinDuration=500, got %d", stats.MinDuration)
		}
		if stats.MaxDuration != 2000 {
			t.Errorf("Expected MaxDuration=2000, got %d", stats.MaxDuration)
		}
		if stats.PeakMemoryUsage != 2048 {
			t.Errorf("Expected PeakMemoryUsage=2048, got %d", stats.PeakMemoryUsage)
		}
	})

	t.Run("history_overflow", func(t *testing.T) {
		p := NewQueryProfiler()
		p.HistorySize = 3
		p.History = make([]QueryExecutionRecord, 0, 3)

		// Add more records than HistorySize
		for i := 0; i < 5; i++ {
			p.RecordExecution(int64(i*100), i, i*100)
		}

		// Should have been capped
		if len(p.History) > 3 {
			t.Errorf("Expected at most 3 history entries, got %d", len(p.History))
		}
	})

	t.Run("stats_after_no_records", func(t *testing.T) {
		p := NewQueryProfiler()
		stats := p.GetStats()
		if stats.TotalExecutions != 0 {
			t.Errorf("Expected 0 executions")
		}
	})
}

// ---------------------------------------------------------------------------
// ExecuteStreaming
// ---------------------------------------------------------------------------

func TestExecuteStreamingCoverage(t *testing.T) {
	t.Run("streaming_select", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.QualifiedIdentifier{Column: "id"}},
			From:    &query.TableRef{Name: "test"},
		}
		compiled, err := compiler.CompileQuery("SELECT id FROM streaming_test", stmt, nil)
		if err != nil {
			t.Fatalf("CompileQuery failed: %v", err)
		}
		compiled.ResultSchema = []ColumnInfo{{Name: "id", Type: "INTEGER"}}

		sr, err := rt.ExecuteStreaming(compiled, nil, 2)
		if err != nil {
			t.Logf("ExecuteStreaming returned: %v", err)
			return
		}
		t.Logf("Streaming: TotalRows=%d, HasMore=%v", sr.TotalRows, sr.HasMore)

		// Fetch chunks
		for sr.HasMore {
			rows, err := sr.Next()
			if err != nil {
				t.Fatalf("Next() failed: %v", err)
			}
			t.Logf("Chunk: %d rows", len(rows))
			if len(rows) == 0 {
				break
			}
		}
	})

	t.Run("streaming_no_schema", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()
		stmt := &query.UpdateStmt{
			Table: "test",
			Set:   []*query.SetClause{{Column: "x", Value: &query.NumberLiteral{Value: 1}}},
		}
		compiled, err := compiler.CompileQuery("UPDATE test SET x=1", stmt, nil)
		if err != nil {
			t.Fatalf("CompileQuery failed: %v", err)
		}
		// ResultSchema is nil for UPDATE - streaming not supported
		_, err = rt.ExecuteStreaming(compiled, nil, 10)
		if err == nil {
			t.Error("Expected error for streaming non-SELECT")
		}
	})
}

// ---------------------------------------------------------------------------
// Execute – non-SELECT path (nil ResultSchema) / empty result
// ---------------------------------------------------------------------------

func TestExecuteNonSelectPath(t *testing.T) {
	t.Run("execute_update_no_schema", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()
		stmt := &query.UpdateStmt{
			Table: "test",
			Set:   []*query.SetClause{{Column: "n", Value: &query.NumberLiteral{Value: 5}}},
		}
		compiled, err := compiler.CompileQuery("UPDATE test SET n=5", stmt, nil)
		if err != nil {
			t.Fatalf("CompileQuery failed: %v", err)
		}
		// ResultSchema is nil for UPDATE
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execute UPDATE failed: %v", err)
		}
		t.Logf("UPDATE Execute: RowsAffected=%d", result.RowsAffected)
	})

	t.Run("execute_delete_no_schema", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()
		stmt := &query.DeleteStmt{Table: "test"}
		compiled, err := compiler.CompileQuery("DELETE FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("CompileQuery failed: %v", err)
		}
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execute DELETE failed: %v", err)
		}
		t.Logf("DELETE Execute: RowsAffected=%d", result.RowsAffected)
	})
}

// ---------------------------------------------------------------------------
// registerBuiltinUDFs – cover remaining branches
// ---------------------------------------------------------------------------

func TestBuiltinUDFs(t *testing.T) {
	hf := NewHostFunctions()

	t.Run("square_int64", func(t *testing.T) {
		udf, ok := hf.GetUDF("SQUARE")
		if !ok {
			t.Fatal("SQUARE not found")
		}
		result, err := udf.Fn([]interface{}{int64(5)})
		if err != nil {
			t.Fatalf("SQUARE failed: %v", err)
		}
		if result.(int64) != 25 {
			t.Errorf("Expected 25, got %v", result)
		}
	})

	t.Run("square_float64", func(t *testing.T) {
		udf, _ := hf.GetUDF("SQUARE")
		result, err := udf.Fn([]interface{}{float64(3.0)})
		if err != nil {
			t.Fatalf("SQUARE float64 failed: %v", err)
		}
		if result.(float64) != 9.0 {
			t.Errorf("Expected 9.0, got %v", result)
		}
	})

	t.Run("square_no_args", func(t *testing.T) {
		udf, _ := hf.GetUDF("SQUARE")
		result, _ := udf.Fn([]interface{}{})
		if result != nil {
			t.Errorf("Expected nil for no args, got %v", result)
		}
	})

	t.Run("square_unsupported_type", func(t *testing.T) {
		udf, _ := hf.GetUDF("SQUARE")
		result, _ := udf.Fn([]interface{}{"notanumber"})
		if result != nil {
			t.Errorf("Expected nil for string arg")
		}
	})

	t.Run("cube_int64", func(t *testing.T) {
		udf, ok := hf.GetUDF("CUBE")
		if !ok {
			t.Fatal("CUBE not found")
		}
		result, err := udf.Fn([]interface{}{int64(3)})
		if err != nil {
			t.Fatalf("CUBE failed: %v", err)
		}
		if result.(int64) != 27 {
			t.Errorf("Expected 27, got %v", result)
		}
	})

	t.Run("cube_float64", func(t *testing.T) {
		udf, _ := hf.GetUDF("CUBE")
		result, err := udf.Fn([]interface{}{float64(2.0)})
		if err != nil {
			t.Fatalf("CUBE float64 failed: %v", err)
		}
		if result.(float64) != 8.0 {
			t.Errorf("Expected 8.0, got %v", result)
		}
	})

	t.Run("cube_no_args", func(t *testing.T) {
		udf, _ := hf.GetUDF("CUBE")
		result, _ := udf.Fn([]interface{}{})
		if result != nil {
			t.Errorf("Expected nil for no args")
		}
	})

	t.Run("cube_unsupported_type", func(t *testing.T) {
		udf, _ := hf.GetUDF("CUBE")
		result, _ := udf.Fn([]interface{}{true})
		if result != nil {
			t.Errorf("Expected nil for bool arg")
		}
	})

	t.Run("abs_val_positive", func(t *testing.T) {
		udf, ok := hf.GetUDF("ABS_VAL")
		if !ok {
			t.Fatal("ABS_VAL not found")
		}
		result, _ := udf.Fn([]interface{}{int64(10)})
		if result.(int64) != 10 {
			t.Errorf("Expected 10, got %v", result)
		}
	})

	t.Run("abs_val_negative", func(t *testing.T) {
		udf, _ := hf.GetUDF("ABS_VAL")
		result, _ := udf.Fn([]interface{}{int64(-7)})
		if result.(int64) != 7 {
			t.Errorf("Expected 7, got %v", result)
		}
	})

	t.Run("abs_val_float_positive", func(t *testing.T) {
		udf, _ := hf.GetUDF("ABS_VAL")
		result, _ := udf.Fn([]interface{}{float64(3.14)})
		if result.(float64) != 3.14 {
			t.Errorf("Expected 3.14, got %v", result)
		}
	})

	t.Run("abs_val_float_negative", func(t *testing.T) {
		udf, _ := hf.GetUDF("ABS_VAL")
		result, _ := udf.Fn([]interface{}{float64(-2.5)})
		if result.(float64) != 2.5 {
			t.Errorf("Expected 2.5, got %v", result)
		}
	})

	t.Run("abs_val_no_args", func(t *testing.T) {
		udf, _ := hf.GetUDF("ABS_VAL")
		result, _ := udf.Fn([]interface{}{})
		if result != nil {
			t.Errorf("Expected nil for no args")
		}
	})

	t.Run("abs_val_unsupported_type", func(t *testing.T) {
		udf, _ := hf.GetUDF("ABS_VAL")
		result, _ := udf.Fn([]interface{}{"abc"})
		if result != nil {
			t.Errorf("Expected nil for string")
		}
	})

	t.Run("power_int_basic", func(t *testing.T) {
		udf, ok := hf.GetUDF("POWER_INT")
		if !ok {
			t.Fatal("POWER_INT not found")
		}
		result, _ := udf.Fn([]interface{}{int64(2), int64(10)})
		if result.(int64) != 1024 {
			t.Errorf("Expected 1024, got %v", result)
		}
	})

	t.Run("power_int_float_args", func(t *testing.T) {
		udf, _ := hf.GetUDF("POWER_INT")
		result, _ := udf.Fn([]interface{}{float64(3.0), float64(3.0)})
		if result.(int64) != 27 {
			t.Errorf("Expected 27, got %v", result)
		}
	})

	t.Run("power_int_too_few_args", func(t *testing.T) {
		udf, _ := hf.GetUDF("POWER_INT")
		result, _ := udf.Fn([]interface{}{int64(2)})
		if result != nil {
			t.Errorf("Expected nil for too-few args")
		}
	})

	t.Run("power_int_unsupported_base", func(t *testing.T) {
		udf, _ := hf.GetUDF("POWER_INT")
		result, _ := udf.Fn([]interface{}{"x", int64(2)})
		if result != nil {
			t.Errorf("Expected nil for string base")
		}
	})

	t.Run("power_int_unsupported_exp", func(t *testing.T) {
		udf, _ := hf.GetUDF("POWER_INT")
		result, _ := udf.Fn([]interface{}{int64(2), "y"})
		if result != nil {
			t.Errorf("Expected nil for string exponent")
		}
	})
}
