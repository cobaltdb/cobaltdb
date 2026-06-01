package query

import (
	"fmt"
	"strings"
)

// GenerateQueryKey builds a cache key from a SQL string and query arguments.
func GenerateQueryKey(sql string, args []interface{}) string {
	var builder strings.Builder
	builder.Grow(len(sql) + len(args)*16)
	builder.WriteString(sql)
	for _, arg := range args {
		builder.WriteByte('|')
		fmt.Fprint(&builder, arg)
	}
	return builder.String()
}

// IsCacheableQuery returns true if the SELECT statement is safe to cache.
// Queries without a FROM clause, with subqueries in SELECT, or with
// non-deterministic functions are not cached.
func IsCacheableQuery(stmt *SelectStmt) bool {
	if stmt.From == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if ContainsSubquery(col) {
			return false
		}
	}
	return !ContainsNonDeterministicFunctions(stmt)
}

// ContainsSubquery reports whether expr contains a subquery expression.
// Exported for backward compatibility with pkg/catalog tests.
func ContainsSubquery(expr Expression) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *SubqueryExpr, *ExistsExpr:
		return true
	case *AliasExpr:
		return ContainsSubquery(e.Expr)
	case *BinaryExpr:
		return ContainsSubquery(e.Left) || ContainsSubquery(e.Right)
	case *UnaryExpr:
		return ContainsSubquery(e.Expr)
	case *FunctionCall:
		for _, arg := range e.Args {
			if ContainsSubquery(arg) {
				return true
			}
		}
	}
	return false
}

// ContainsNonDeterministicFunctions reports whether stmt contains non-deterministic functions.
// Exported for backward compatibility with pkg/catalog tests.
func ContainsNonDeterministicFunctions(stmt *SelectStmt) bool {
	for _, col := range stmt.Columns {
		if HasNonDeterministicFunction(col) {
			return true
		}
	}
	if HasNonDeterministicFunction(stmt.Where) {
		return true
	}
	for _, ob := range stmt.OrderBy {
		if HasNonDeterministicFunction(ob.Expr) {
			return true
		}
	}
	return false
}

// HasNonDeterministicFunction reports whether expr contains a non-deterministic function.
// Exported for backward compatibility with pkg/catalog tests.
func HasNonDeterministicFunction(expr Expression) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *FunctionCall:
		nonDetFuncs := []string{"RANDOM", "RAND", "NOW", "CURRENT_TIMESTAMP", "UUID", "NEWID"}
		for _, ndf := range nonDetFuncs {
			if strings.EqualFold(e.Name, ndf) {
				return true
			}
		}
		for _, arg := range e.Args {
			if HasNonDeterministicFunction(arg) {
				return true
			}
		}
	case *AliasExpr:
		return HasNonDeterministicFunction(e.Expr)
	case *BinaryExpr:
		return HasNonDeterministicFunction(e.Left) || HasNonDeterministicFunction(e.Right)
	case *UnaryExpr:
		return HasNonDeterministicFunction(e.Expr)
	}
	return false
}

// ExtractTablesFromQuery returns the set of table names referenced by a SELECT.
func ExtractTablesFromQuery(stmt *SelectStmt) []string {
	tables := make(map[string]bool)
	if stmt.From != nil {
		tables[stmt.From.Name] = true
	}
	for _, join := range stmt.Joins {
		if join.Table != nil {
			tables[join.Table.Name] = true
		}
	}
	result := make([]string, 0, len(tables))
	for tbl := range tables {
		result = append(result, tbl)
	}
	return result
}

// QueryToSQL produces a rough SQL string from a SELECT statement.
// This is used for cache key generation and is not a full serializer.
func QueryToSQL(stmt *SelectStmt) string {
	var parts []string
	parts = append(parts, "SELECT")
	if stmt.Distinct {
		parts = append(parts, "DISTINCT")
	}
	colParts := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		colParts[i] = ExprToString(col)
	}
	parts = append(parts, strings.Join(colParts, ", "))
	if stmt.From != nil {
		parts = append(parts, "FROM", stmt.From.Name)
	}
	return strings.Join(parts, " ")
}

// ExprToString converts an expression to a string representation.
// Exported for backward compatibility with pkg/catalog tests.
func ExprToString(expr Expression) string {
	return exprToStringImpl(expr, true)
}

func exprToStringImpl(expr Expression, exported bool) string {
	if expr == nil {
		if exported {
			return ""
		}
		return "<nil>"
	}
	switch e := expr.(type) {
	case *Identifier:
		return e.Name
	case *StarExpr:
		return "*"
	case *StringLiteral:
		return fmt.Sprintf("'%s'", e.Value)
	case *NumberLiteral:
		return fmt.Sprintf("%v", e.Value)
	case *AliasExpr:
		return exprToStringImpl(e.Expr, exported) + " AS " + e.Alias
	case *FunctionCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = exprToStringImpl(arg, exported)
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
	default:
		return fmt.Sprintf("%T", expr)
	}
}
