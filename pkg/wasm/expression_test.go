package wasm

import (
	"bytes"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestExpressionCompiler tests SQL expression compilation to WASM
func TestExpressionCompiler(t *testing.T) {
	compiler := NewCompiler()

	t.Run("number_literal", func(t *testing.T) {
		buf := new(bytes.Buffer)
		typ, err := compiler.compileExpression(&query.NumberLiteral{Value: 42}, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected type INTEGER, got %s", typ)
		}
		// Should generate i64.const 42
		expected := []byte{0x42, 42}
		if !bytes.Equal(buf.Bytes(), expected) {
			t.Errorf("Expected %v, got %v", expected, buf.Bytes())
		}
	})

	t.Run("boolean_literal_true", func(t *testing.T) {
		buf := new(bytes.Buffer)
		typ, err := compiler.compileExpression(&query.BooleanLiteral{Value: true}, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected type BOOLEAN, got %s", typ)
		}
		// Should generate i32.const 1
		expected := []byte{0x41, 0x01}
		if !bytes.Equal(buf.Bytes(), expected) {
			t.Errorf("Expected %v, got %v", expected, buf.Bytes())
		}
	})

	t.Run("boolean_literal_false", func(t *testing.T) {
		buf := new(bytes.Buffer)
		typ, err := compiler.compileExpression(&query.BooleanLiteral{Value: false}, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected type BOOLEAN, got %s", typ)
		}
		// Should generate i32.const 0
		expected := []byte{0x41, 0x00}
		if !bytes.Equal(buf.Bytes(), expected) {
			t.Errorf("Expected %v, got %v", expected, buf.Bytes())
		}
	})

	t.Run("addition", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 10},
			Operator: query.TokenPlus,
			Right:    &query.NumberLiteral{Value: 20},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected type INTEGER, got %s", typ)
		}
		// Should generate: i64.const 10, i64.const 20, i64.add
		code := buf.Bytes()
		if len(code) < 5 {
			t.Fatalf("Expected at least 5 bytes, got %d", len(code))
		}
		if code[0] != 0x42 || code[2] != 0x42 || code[4] != 0x7c {
			t.Errorf("Expected i64.const 10, i64.const 20, i64.add pattern, got %v", code)
		}
	})

	t.Run("subtraction", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 30},
			Operator: query.TokenMinus,
			Right:    &query.NumberLiteral{Value: 10},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		if typ != "INTEGER" {
			t.Errorf("Expected type INTEGER, got %s", typ)
		}
		// Should generate: i64.const 30, i64.const 10, i64.sub
		code := buf.Bytes()
		if len(code) < 5 {
			t.Fatalf("Expected at least 5 bytes, got %d", len(code))
		}
		if code[4] != 0x7d { // i64.sub
			t.Errorf("Expected i64.sub (0x7d), got 0x%02x", code[4])
		}
	})

	t.Run("multiplication", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 5},
			Operator: query.TokenStar,
			Right:    &query.NumberLiteral{Value: 6},
		}
		_, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		// Should generate: i64.const 5, i64.const 6, i64.mul
		code := buf.Bytes()
		if len(code) < 5 {
			t.Fatalf("Expected at least 5 bytes, got %d", len(code))
		}
		if code[4] != 0x7e { // i64.mul
			t.Errorf("Expected i64.mul (0x7e), got 0x%02x", code[4])
		}
	})

	t.Run("equality_comparison", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 5},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 5},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected type BOOLEAN, got %s", typ)
		}
		// Should generate: i64.const 5, i64.const 5, i64.eq
		code := buf.Bytes()
		if len(code) < 5 {
			t.Fatalf("Expected at least 5 bytes, got %d", len(code))
		}
		if code[4] != 0x51 { // i64.eq
			t.Errorf("Expected i64.eq (0x51), got 0x%02x", code[4])
		}
	})

	t.Run("less_than_comparison", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 3},
			Operator: query.TokenLt,
			Right:    &query.NumberLiteral{Value: 7},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected type BOOLEAN, got %s", typ)
		}
		// Should generate: i64.const 3, i64.const 7, i64.lt_s
		code := buf.Bytes()
		if len(code) < 5 {
			t.Fatalf("Expected at least 5 bytes, got %d", len(code))
		}
		if code[4] != 0x53 { // i64.lt_s
			t.Errorf("Expected i64.lt_s (0x53), got 0x%02x", code[4])
		}
	})

	t.Run("greater_than_comparison", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expr := &query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 10},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 5},
		}
		typ, err := compiler.compileExpression(expr, buf)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		if typ != "BOOLEAN" {
			t.Errorf("Expected type BOOLEAN, got %s", typ)
		}
		// Should generate: i64.const 10, i64.const 5, i64.gt_s
		code := buf.Bytes()
		if len(code) < 5 {
			t.Fatalf("Expected at least 5 bytes, got %d", len(code))
		}
		if code[4] != 0x55 { // i64.gt_s
			t.Errorf("Expected i64.gt_s (0x55), got 0x%02x", code[4])
		}
	})
}
