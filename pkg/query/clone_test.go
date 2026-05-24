package query

import "testing"

func TestCloneStatementDeepCopiesSelectTree(t *testing.T) {
	stmt, err := Parse("SELECT u.id, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id IN (SELECT user_id FROM audit) GROUP BY u.id ORDER BY u.id")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	original := stmt.(*SelectStmt)
	cloned := CloneStatement(stmt).(*SelectStmt)
	if cloned == original {
		t.Fatal("CloneStatement returned the original statement pointer")
	}
	if cloned.From == original.From {
		t.Fatal("CloneStatement did not copy FROM table reference")
	}
	if cloned.Joins[0] == original.Joins[0] {
		t.Fatal("CloneStatement did not copy JOIN clause")
	}
	if cloned.OrderBy[0] == original.OrderBy[0] {
		t.Fatal("CloneStatement did not copy ORDER BY clause")
	}

	cloned.From.Name = "mutated"
	cloned.Joins[0].Table.Name = "mutated_orders"
	cloned.GroupBy[0] = &Identifier{Name: "changed"}

	if original.From.Name != "users" {
		t.Fatalf("original FROM mutated through clone, got %q", original.From.Name)
	}
	if original.Joins[0].Table.Name != "orders" {
		t.Fatalf("original JOIN table mutated through clone, got %q", original.Joins[0].Table.Name)
	}
	groupBy := original.GroupBy[0].(*QualifiedIdentifier)
	if groupBy.Column != "id" {
		t.Fatalf("original GROUP BY mutated through clone, got %#v", original.GroupBy[0])
	}
}

func TestCloneExpressionDeepCopiesNestedExpression(t *testing.T) {
	expr := &FunctionCall{
		Name: "COALESCE",
		Args: []Expression{
			&JSONPathExpr{Column: &Identifier{Name: "doc"}, Path: "name", AsText: true},
			&StringLiteral{Value: "unknown"},
		},
	}

	cloned := CloneExpression(expr).(*FunctionCall)
	if cloned == expr {
		t.Fatal("CloneExpression returned the original expression pointer")
	}
	if cloned.Args[0] == expr.Args[0] {
		t.Fatal("CloneExpression did not copy nested arguments")
	}

	cloned.Args[0].(*JSONPathExpr).Path = "mutated"
	if expr.Args[0].(*JSONPathExpr).Path != "name" {
		t.Fatalf("original expression mutated through clone, got %q", expr.Args[0].(*JSONPathExpr).Path)
	}
}
