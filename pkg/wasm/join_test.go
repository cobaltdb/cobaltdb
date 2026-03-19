package wasm

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestJoinOperations tests SQL JOIN compilation and execution
func TestJoinOperations(t *testing.T) {
	t.Run("inner_join_compilation", func(t *testing.T) {
		compiler := NewCompiler()

		// Create SELECT with INNER JOIN
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "users", Column: "id"},
				&query.QualifiedIdentifier{Table: "users", Column: "name"},
				&query.QualifiedIdentifier{Table: "orders", Column: "amount"},
			},
			From: &query.TableRef{Name: "users"},
			Joins: []*query.JoinClause{
				{
					Table:     &query.TableRef{Name: "orders"},
					Type:      query.TokenInner,
					Condition: &query.BooleanLiteral{Value: true},
				},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT users.id, users.name, orders.amount FROM users INNER JOIN orders ON true", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled INNER JOIN, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("inner_join_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with INNER JOIN
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "users", Column: "id"},
				&query.QualifiedIdentifier{Table: "orders", Column: "id"},
			},
			From: &query.TableRef{Name: "users"},
			Joins: []*query.JoinClause{
				{
					Table:     &query.TableRef{Name: "orders"},
					Type:      query.TokenInner,
					Condition: &query.BooleanLiteral{Value: true},
				},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT users.id, orders.id FROM users INNER JOIN orders ON true", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema for INTEGER columns
		compiled.ResultSchema = []ColumnInfo{
			{Name: "users.id", Type: "INTEGER", Nullable: false},
			{Name: "orders.id", Type: "INTEGER", Nullable: false},
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("INNER JOIN result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have 9 rows (3 users x 3 orders = 9 combinations)
		if len(result.Rows) != 9 {
			t.Errorf("Expected 9 rows (cross product), got %d", len(result.Rows))
		}
	})

	t.Run("left_join_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with LEFT JOIN
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "users", Column: "id"},
				&query.QualifiedIdentifier{Table: "orders", Column: "id"},
			},
			From: &query.TableRef{Name: "users"},
			Joins: []*query.JoinClause{
				{
					Table:     &query.TableRef{Name: "orders"},
					Type:      query.TokenLeft,
					Condition: &query.BooleanLiteral{Value: true},
				},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT users.id, orders.id FROM users LEFT JOIN orders ON true", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema for INTEGER columns
		compiled.ResultSchema = []ColumnInfo{
			{Name: "users.id", Type: "INTEGER", Nullable: false},
			{Name: "orders.id", Type: "INTEGER", Nullable: true},
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("LEFT JOIN result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have 3 rows (one for each user, with matching orders)
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
	})

	t.Run("right_join_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with RIGHT JOIN
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "users", Column: "id"},
				&query.QualifiedIdentifier{Table: "orders", Column: "id"},
			},
			From: &query.TableRef{Name: "users"},
			Joins: []*query.JoinClause{
				{
					Table:     &query.TableRef{Name: "orders"},
					Type:      query.TokenRight,
					Condition: &query.BooleanLiteral{Value: true},
				},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT users.id, orders.id FROM users RIGHT JOIN orders ON true", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema for INTEGER columns
		compiled.ResultSchema = []ColumnInfo{
			{Name: "users.id", Type: "INTEGER", Nullable: true},
			{Name: "orders.id", Type: "INTEGER", Nullable: false},
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("RIGHT JOIN result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have 3 rows (one for each order, with matching users)
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
	})

	t.Run("full_join_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with FULL JOIN
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "users", Column: "id"},
				&query.QualifiedIdentifier{Table: "orders", Column: "id"},
			},
			From: &query.TableRef{Name: "users"},
			Joins: []*query.JoinClause{
				{
					Table:     &query.TableRef{Name: "orders"},
					Type:      query.TokenFull,
					Condition: &query.BooleanLiteral{Value: true},
				},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT users.id, orders.id FROM users FULL JOIN orders ON true", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema for INTEGER columns
		compiled.ResultSchema = []ColumnInfo{
			{Name: "users.id", Type: "INTEGER", Nullable: true},
			{Name: "orders.id", Type: "INTEGER", Nullable: true},
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("FULL JOIN result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have rows (matching + unmatched from both sides)
		if len(result.Rows) == 0 {
			t.Errorf("Expected rows, got 0")
		}
	})
}
