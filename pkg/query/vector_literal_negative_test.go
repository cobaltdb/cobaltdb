package query

import (
	"reflect"
	"testing"
)

// parseVectorLiteralExpr tokenizes and parses sql, returning the statement.
func parseVectorLiteralExpr(t *testing.T, sql string) Statement {
	t.Helper()
	tokens, err := Tokenize(sql)
	if err != nil {
		t.Fatalf("Tokenize(%q): %v", sql, err)
	}
	stmt, err := NewParser(tokens).Parse()
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	return stmt
}

// TestVectorLiteralNegativeComponents verifies that vector literals accept
// negative components. The lexer emits '-' as a separate token and
// parseUnary's sign folding never sees vector components (parsePrimary
// dispatches '[' directly to parseVectorLiteral), so [-1, 0.5, 0] failed
// with "expected number in vector literal, got -" — even though embeddings
// legitimately contain negative values.
func TestVectorLiteralNegativeComponents(t *testing.T) {
	stmt := parseVectorLiteralExpr(t, "SELECT * FROM t WHERE embedding = [-1, 0.5, 0]")
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("statement type = %T, want *SelectStmt", stmt)
	}
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("WHERE type = %T, want *BinaryExpr", sel.Where)
	}
	vec, ok := bin.Right.(*VectorLiteral)
	if !ok {
		t.Fatalf("right operand type = %T, want *VectorLiteral", bin.Right)
	}
	want := []float64{-1, 0.5, 0}
	if !reflect.DeepEqual(vec.Values, want) {
		t.Errorf("vector literal values = %v, want %v", vec.Values, want)
	}
}

// TestVectorLiteralPositiveComponentsStillParse guards the existing
// positive-component form against regressions from the negative-component
// support.
func TestVectorLiteralPositiveComponentsStillParse(t *testing.T) {
	stmt := parseVectorLiteralExpr(t, "SELECT * FROM t WHERE embedding = [1, 0.5, 2]")
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("statement type = %T, want *SelectStmt", stmt)
	}
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("WHERE type = %T, want *BinaryExpr", sel.Where)
	}
	vec, ok := bin.Right.(*VectorLiteral)
	if !ok {
		t.Fatalf("right operand type = %T, want *VectorLiteral", bin.Right)
	}
	want := []float64{1, 0.5, 2}
	if !reflect.DeepEqual(vec.Values, want) {
		t.Errorf("vector literal values = %v, want %v", vec.Values, want)
	}
}

// TestUpdateSetVectorLiteralNegative verifies the UPDATE SET context, which
// is how vector embeddings are rewritten in practice.
func TestUpdateSetVectorLiteralNegative(t *testing.T) {
	stmt := parseVectorLiteralExpr(t, "UPDATE t SET embedding = [-1, 0, 0]")
	if _, ok := stmt.(*UpdateStmt); !ok {
		t.Fatalf("statement type = %T, want *UpdateStmt", stmt)
	}
}
