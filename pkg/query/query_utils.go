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
	// AS OF (temporal) and locking clauses are not part of the QueryToSQL cache
	// key, so caching them could serve results for the wrong snapshot / ignore
	// the lock. Don't cache them.
	if stmt.AsOf != nil || stmt.Locking != nil {
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
		if ContainsSubquery(e.Filter) {
			return true
		}
		for _, ob := range e.OrderBy {
			if ob != nil && ContainsSubquery(ob.Expr) {
				return true
			}
		}
	case *WindowExpr:
		for _, arg := range e.Args {
			if ContainsSubquery(arg) {
				return true
			}
		}
		if ContainsSubquery(e.Filter) {
			return true
		}
		for _, partitionExpr := range e.PartitionBy {
			if ContainsSubquery(partitionExpr) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if ob != nil && ContainsSubquery(ob.Expr) {
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
		if HasNonDeterministicFunction(e.Filter) {
			return true
		}
		for _, ob := range e.OrderBy {
			if ob != nil && HasNonDeterministicFunction(ob.Expr) {
				return true
			}
		}
	case *WindowExpr:
		for _, arg := range e.Args {
			if HasNonDeterministicFunction(arg) {
				return true
			}
		}
		if HasNonDeterministicFunction(e.Filter) {
			return true
		}
		for _, partitionExpr := range e.PartitionBy {
			if HasNonDeterministicFunction(partitionExpr) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if ob != nil && HasNonDeterministicFunction(ob.Expr) {
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
	if stmt == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("SELECT")
	if stmt.Distinct {
		b.WriteString(" DISTINCT")
	}
	for i, col := range stmt.Columns {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte(' ')
		b.WriteString(ExprToString(col))
	}
	if stmt.From != nil {
		b.WriteString(" FROM ")
		b.WriteString(tableRefToString(stmt.From))
	}
	// JOINs, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET must all be part of
	// the cache key — without them, e.g. `... WHERE id=1` and `... WHERE id=2`
	// (or queries that differ only by JOIN/GROUP BY/ORDER BY/LIMIT) collided on
	// the same key and returned each other's cached rows.
	for _, j := range stmt.Joins {
		if j == nil {
			continue
		}
		b.WriteString(fmt.Sprintf(" JOIN%d", int(j.Type)))
		if j.Natural {
			b.WriteString(" NATURAL")
		}
		if j.Table != nil {
			b.WriteByte(' ')
			b.WriteString(tableRefToString(j.Table))
		}
		if j.Condition != nil {
			b.WriteString(" ON ")
			b.WriteString(ExprToString(j.Condition))
		}
		if len(j.Using) > 0 {
			b.WriteString(" USING(")
			b.WriteString(strings.Join(j.Using, ","))
			b.WriteByte(')')
		}
	}
	if stmt.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(ExprToString(stmt.Where))
	}
	if len(stmt.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, g := range stmt.GroupBy {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(ExprToString(g))
		}
	}
	if stmt.Having != nil {
		b.WriteString(" HAVING ")
		b.WriteString(ExprToString(stmt.Having))
	}
	if len(stmt.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range stmt.OrderBy {
			if i > 0 {
				b.WriteByte(',')
			}
			if o == nil {
				continue
			}
			b.WriteString(ExprToString(o.Expr))
			if o.Desc {
				b.WriteString(" DESC")
			}
			if o.NullsSpecified {
				if o.NullsFirst {
					b.WriteString(" NULLSFIRST")
				} else {
					b.WriteString(" NULLSLAST")
				}
			}
		}
	}
	if stmt.Limit != nil {
		b.WriteString(" LIMIT ")
		b.WriteString(ExprToString(stmt.Limit))
	}
	if stmt.Offset != nil {
		b.WriteString(" OFFSET ")
		b.WriteString(ExprToString(stmt.Offset))
	}
	return b.String()
}

func tableRefToString(t *TableRef) string {
	if t == nil {
		return ""
	}
	s := t.Name
	if t.Subquery != nil {
		s = "(" + QueryToSQL(t.Subquery) + ")"
	}
	if t.Alias != "" {
		s += " " + t.Alias
	}
	return s
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
	case *ColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Column
		}
		return e.Column
	case *QualifiedIdentifier:
		return e.Table + "." + e.Column
	case *BooleanLiteral:
		if e.Value {
			return "TRUE"
		}
		return "FALSE"
	case *NullLiteral:
		return "NULL"
	case *BinaryExpr:
		return "(" + exprToStringImpl(e.Left, exported) + " " + fmt.Sprintf("%d", int(e.Operator)) + " " + exprToStringImpl(e.Right, exported) + ")"
	case *UnaryExpr:
		return "(" + fmt.Sprintf("%d", int(e.Operator)) + " " + exprToStringImpl(e.Expr, exported) + ")"
	case *IsNullExpr:
		s := exprToStringImpl(e.Expr, exported) + " ISNULL"
		if e.Not {
			s += " NOT"
		}
		return s
	case *BetweenExpr:
		s := exprToStringImpl(e.Expr, exported) + " BETWEEN " + exprToStringImpl(e.Lower, exported) + " AND " + exprToStringImpl(e.Upper, exported)
		if e.Not {
			s += " NOT"
		}
		return s
	case *LikeExpr:
		s := exprToStringImpl(e.Expr, exported) + " LIKE " + exprToStringImpl(e.Pattern, exported)
		if e.Escape != nil {
			s += " ESCAPE " + exprToStringImpl(e.Escape, exported)
		}
		if e.Not {
			s += " NOT"
		}
		return s
	case *CastExpr:
		return "CAST(" + exprToStringImpl(e.Expr, exported) + " AS " + fmt.Sprintf("%d", int(e.DataType)) + ")"
	case *InExpr:
		var sb strings.Builder
		sb.WriteString(exprToStringImpl(e.Expr, exported))
		sb.WriteString(" IN(")
		for i, item := range e.List {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(exprToStringImpl(item, exported))
		}
		if e.Subquery != nil {
			sb.WriteString(";SUBQ:")
			sb.WriteString(QueryToSQL(e.Subquery))
		}
		sb.WriteByte(')')
		if e.Not {
			sb.WriteString(" NOT")
		}
		return sb.String()
	case *FunctionCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = exprToStringImpl(arg, exported)
		}
		if len(e.OrderBy) > 0 {
			orderParts := make([]string, 0, len(e.OrderBy))
			for _, ob := range e.OrderBy {
				if ob == nil {
					continue
				}
				part := exprToStringImpl(ob.Expr, exported)
				if ob.Desc {
					part += " DESC"
				} else {
					part += " ASC"
				}
				orderParts = append(orderParts, part)
			}
			args = append(args, "ORDER BY "+strings.Join(orderParts, ", "))
		}
		result := fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
		if e.Filter != nil {
			result += " FILTER (WHERE " + exprToStringImpl(e.Filter, exported) + ")"
		}
		return result
	default:
		// Collision-safe fallback for expression types not serialized above.
		// Including the value (%+v) — not just the type (%T) — prevents two
		// different predicates from producing the SAME cache key (which returned
		// wrong cached rows). For nested-pointer node types this yields distinct
		// keys per parse (so the cache simply may not hit), never a false hit.
		return fmt.Sprintf("%T%+v", expr, expr)
	}
}
