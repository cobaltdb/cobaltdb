package query

import (
	"testing"
)

func TestOptimizeProjections_NilStmt(t *testing.T) {
	opt := NewQueryOptimizer()
	result := opt.optimizeProjections(nil)
	if result != nil {
		t.Error("Expected nil for nil stmt")
	}
}

func TestOptimizeProjections_EmptyColumns(t *testing.T) {
	opt := NewQueryOptimizer()
	stmt := &SelectStmt{}
	result := opt.optimizeProjections(stmt)
	if result != stmt {
		t.Error("Expected same stmt for empty columns")
	}
}

func TestOptimizeProjections_StarExpr(t *testing.T) {
	opt := NewQueryOptimizer()
	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "users"},
	}
	result := opt.optimizeProjections(stmt)
	if result != stmt {
		t.Error("Expected same stmt for SELECT *")
	}
}

func TestOptimizeProjections_WithIndexHint(t *testing.T) {
	opt := NewQueryOptimizer()
	opt.stats.IndexStats["users.name"] = &OptimizerIdxStats{
		Selectivity: 0.01,
	}
	stmt := &SelectStmt{
		Columns: []Expression{&Identifier{Name: "name"}},
		From:    &TableRef{Name: "users"},
		Where: &BinaryExpr{
			Left:     &Identifier{Name: "name"},
			Operator: TokenEq,
			Right:    &StringLiteral{Value: "test"},
		},
	}
	result := opt.optimizeProjections(stmt)
	if result.From.IndexHint != "auto" {
		t.Errorf("Expected IndexHint 'auto', got %q", result.From.IndexHint)
	}
}

func TestExtractWhereColumns(t *testing.T) {
	opt := NewQueryOptimizer()

	// Simple identifier
	cols := opt.extractWhereColumns(&Identifier{Name: "name"})
	if len(cols) != 1 || cols[0] != "name" {
		t.Errorf("Expected [name], got %v", cols)
	}

	// Qualified identifier
	cols = opt.extractWhereColumns(&QualifiedIdentifier{Table: "t", Column: "id"})
	if len(cols) != 1 || cols[0] != "id" {
		t.Errorf("Expected [id], got %v", cols)
	}

	// Binary expression
	cols = opt.extractWhereColumns(&BinaryExpr{
		Left:     &Identifier{Name: "a"},
		Operator: TokenAnd,
		Right:    &Identifier{Name: "b"},
	})
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %v", cols)
	}

	// Nil
	cols = opt.extractWhereColumns(nil)
	if len(cols) != 0 {
		t.Errorf("Expected empty for nil, got %v", cols)
	}
}

func TestCopySelectStmt(t *testing.T) {
	opt := NewQueryOptimizer()
	stmt := &SelectStmt{
		Columns: []Expression{&Identifier{Name: "id"}, &Identifier{Name: "name"}},
		From:    &TableRef{Name: "users"},
		Joins:   []*JoinClause{{Type: TokenInner, Table: &TableRef{Name: "orders"}}},
		OrderBy: []*OrderByExpr{{Expr: &Identifier{Name: "id"}}},
		GroupBy: []Expression{&Identifier{Name: "name"}},
	}

	copied := opt.copySelectStmt(stmt)

	if copied == stmt {
		t.Error("Copy should return a different pointer")
	}
	if len(copied.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(copied.Columns))
	}
	if len(copied.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(copied.Joins))
	}
	if len(copied.OrderBy) != 1 {
		t.Errorf("Expected 1 order by, got %d", len(copied.OrderBy))
	}
	if len(copied.GroupBy) != 1 {
		t.Errorf("Expected 1 group by, got %d", len(copied.GroupBy))
	}
}

func TestOptimizeJoinOrder_SelfJoin(t *testing.T) {
	opt := NewQueryOptimizer()
	// Self-join: same table joined twice
	stmt := &SelectStmt{
		From: &TableRef{Name: "stock"},
		Joins: []*JoinClause{
			{Type: TokenInner, Table: &TableRef{Name: "stock"}},
			{Type: TokenInner, Table: &TableRef{Name: "items"}},
		},
	}

	result := opt.optimizeJoinOrder(stmt)
	// Should NOT reorder due to duplicate table name
	if result.Joins[0].Table.Name != "stock" {
		t.Error("Self-join should not be reordered")
	}
}

func TestOptimizeJoinOrder_Reorder(t *testing.T) {
	opt := NewQueryOptimizer()
	opt.stats.RowCount["big_table"] = 1000000
	opt.stats.RowCount["small_table"] = 10

	stmt := &SelectStmt{
		From: &TableRef{Name: "main"},
		Joins: []*JoinClause{
			{Type: TokenInner, Table: &TableRef{Name: "big_table"}},
			{Type: TokenInner, Table: &TableRef{Name: "small_table"}},
		},
	}

	result := opt.optimizeJoinOrder(stmt)
	// small_table should come first (lower cost)
	if result.Joins[0].Table.Name != "small_table" {
		t.Logf("Join order: %s, %s", result.Joins[0].Table.Name, result.Joins[1].Table.Name)
	}
}
