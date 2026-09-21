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

// ContainsNonDeterministicFunctions reports whether stmt contains non-deterministic
// functions in ANY clause (SELECT list, WHERE, GROUP BY, HAVING, ORDER BY,
// LIMIT/OFFSET, JOIN conditions, derived tables). Previously only Columns,
// Where and OrderBy were checked, so e.g. `HAVING MAX(ts) > NOW()` was cached.
// Exported for backward compatibility with pkg/catalog tests.
func ContainsNonDeterministicFunctions(stmt *SelectStmt) bool {
	if stmt == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if HasNonDeterministicFunction(col) {
			return true
		}
	}
	if HasNonDeterministicFunction(stmt.Where) {
		return true
	}
	if HasNonDeterministicFunction(stmt.Having) {
		return true
	}
	for _, gb := range stmt.GroupBy {
		if HasNonDeterministicFunction(gb) {
			return true
		}
	}
	for _, ob := range stmt.OrderBy {
		if ob != nil && HasNonDeterministicFunction(ob.Expr) {
			return true
		}
	}
	if HasNonDeterministicFunction(stmt.Limit) || HasNonDeterministicFunction(stmt.Offset) {
		return true
	}
	if tableRefHasNonDeterministic(stmt.From) {
		return true
	}
	for _, j := range stmt.Joins {
		if j == nil {
			continue
		}
		if HasNonDeterministicFunction(j.Condition) {
			return true
		}
		if tableRefHasNonDeterministic(j.Table) {
			return true
		}
	}
	return false
}

func tableRefHasNonDeterministic(ref *TableRef) bool {
	if ref == nil {
		return false
	}
	if ref.Subquery != nil && ContainsNonDeterministicFunctions(ref.Subquery) {
		return true
	}
	if ref.SubqueryStmt != nil {
		if u, ok := ref.SubqueryStmt.(*UnionStmt); ok {
			return unionHasNonDeterministic(u)
		}
		if s, ok := ref.SubqueryStmt.(*SelectStmt); ok {
			return ContainsNonDeterministicFunctions(s)
		}
		// Unknown derived-table statement type: treat as non-deterministic
		// (uncacheable) rather than guessing.
		return true
	}
	return false
}

func unionHasNonDeterministic(u *UnionStmt) bool {
	if u == nil {
		return false
	}
	switch l := u.Left.(type) {
	case *SelectStmt:
		if ContainsNonDeterministicFunctions(l) {
			return true
		}
	case *UnionStmt:
		if unionHasNonDeterministic(l) {
			return true
		}
	default:
		return true
	}
	switch r := u.Right.(type) {
	case *SelectStmt:
		return ContainsNonDeterministicFunctions(r)
	case *UnionStmt:
		return unionHasNonDeterministic(r)
	default:
		return true
	}
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
	case *SubqueryExpr:
		return ContainsNonDeterministicFunctions(e.Query)
	case *ExistsExpr:
		return ContainsNonDeterministicFunctions(e.Subquery)
	case *InExpr:
		if HasNonDeterministicFunction(e.Expr) {
			return true
		}
		for _, item := range e.List {
			if HasNonDeterministicFunction(item) {
				return true
			}
		}
		return ContainsNonDeterministicFunctions(e.Subquery)
	case *BetweenExpr:
		return HasNonDeterministicFunction(e.Expr) ||
			HasNonDeterministicFunction(e.Lower) ||
			HasNonDeterministicFunction(e.Upper)
	case *LikeExpr:
		return HasNonDeterministicFunction(e.Expr) ||
			HasNonDeterministicFunction(e.Pattern) ||
			HasNonDeterministicFunction(e.Escape)
	case *IsNullExpr:
		return HasNonDeterministicFunction(e.Expr)
	case *CastExpr:
		return HasNonDeterministicFunction(e.Expr)
	case *CaseExpr:
		if HasNonDeterministicFunction(e.Expr) || HasNonDeterministicFunction(e.Else) {
			return true
		}
		for _, w := range e.Whens {
			if w == nil {
				continue
			}
			if HasNonDeterministicFunction(w.Condition) || HasNonDeterministicFunction(w.Result) {
				return true
			}
		}
	}
	return false
}

// ExtractTablesFromQuery returns the set of table names referenced by a
// SELECT, including tables referenced by subqueries in any clause and by
// derived tables in FROM/JOIN. Missing those made query-cache invalidation
// skip writes to the inner tables, serving stale cached rows.
func ExtractTablesFromQuery(stmt *SelectStmt) []string {
	tables := make(map[string]bool)
	collectTablesFromSelect(stmt, tables)
	result := make([]string, 0, len(tables))
	for tbl := range tables {
		result = append(result, tbl)
	}
	return result
}

func collectTablesFromSelect(stmt *SelectStmt, tables map[string]bool) {
	if stmt == nil {
		return
	}
	collectTablesFromTableRef(stmt.From, tables)
	for _, join := range stmt.Joins {
		if join == nil {
			continue
		}
		collectTablesFromTableRef(join.Table, tables)
		collectTablesFromExpr(join.Condition, tables)
	}
	for _, col := range stmt.Columns {
		collectTablesFromExpr(col, tables)
	}
	collectTablesFromExpr(stmt.Where, tables)
	collectTablesFromExpr(stmt.Having, tables)
	for _, gb := range stmt.GroupBy {
		collectTablesFromExpr(gb, tables)
	}
	for _, ob := range stmt.OrderBy {
		if ob != nil {
			collectTablesFromExpr(ob.Expr, tables)
		}
	}
	collectTablesFromExpr(stmt.Limit, tables)
	collectTablesFromExpr(stmt.Offset, tables)
}

func collectTablesFromTableRef(ref *TableRef, tables map[string]bool) {
	if ref == nil {
		return
	}
	if ref.Subquery != nil {
		collectTablesFromSelect(ref.Subquery, tables)
		return
	}
	if ref.SubqueryStmt != nil {
		collectTablesFromStatement(ref.SubqueryStmt, tables)
		return
	}
	if ref.Name != "" {
		tables[ref.Name] = true
	}
}

func collectTablesFromStatement(stmt Statement, tables map[string]bool) {
	switch s := stmt.(type) {
	case *SelectStmt:
		collectTablesFromSelect(s, tables)
	case *UnionStmt:
		collectTablesFromStatement(s.Left, tables)
		collectTablesFromStatement(s.Right, tables)
	case *SelectStmtWithCTE:
		for _, cte := range s.CTEs {
			if cte != nil {
				collectTablesFromStatement(cte.Query, tables)
			}
		}
		collectTablesFromSelect(s.Select, tables)
	}
}

func collectTablesFromExpr(expr Expression, tables map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *AliasExpr:
		collectTablesFromExpr(e.Expr, tables)
	case *BinaryExpr:
		collectTablesFromExpr(e.Left, tables)
		collectTablesFromExpr(e.Right, tables)
	case *UnaryExpr:
		collectTablesFromExpr(e.Expr, tables)
	case *FunctionCall:
		for _, arg := range e.Args {
			collectTablesFromExpr(arg, tables)
		}
		collectTablesFromExpr(e.Filter, tables)
		for _, ob := range e.OrderBy {
			if ob != nil {
				collectTablesFromExpr(ob.Expr, tables)
			}
		}
	case *WindowExpr:
		for _, arg := range e.Args {
			collectTablesFromExpr(arg, tables)
		}
		collectTablesFromExpr(e.Filter, tables)
		for _, pe := range e.PartitionBy {
			collectTablesFromExpr(pe, tables)
		}
		for _, ob := range e.OrderBy {
			if ob != nil {
				collectTablesFromExpr(ob.Expr, tables)
			}
		}
	case *SubqueryExpr:
		collectTablesFromSelect(e.Query, tables)
	case *ExistsExpr:
		collectTablesFromSelect(e.Subquery, tables)
	case *InExpr:
		collectTablesFromExpr(e.Expr, tables)
		for _, item := range e.List {
			collectTablesFromExpr(item, tables)
		}
		collectTablesFromSelect(e.Subquery, tables)
	case *BetweenExpr:
		collectTablesFromExpr(e.Expr, tables)
		collectTablesFromExpr(e.Lower, tables)
		collectTablesFromExpr(e.Upper, tables)
	case *LikeExpr:
		collectTablesFromExpr(e.Expr, tables)
		collectTablesFromExpr(e.Pattern, tables)
		collectTablesFromExpr(e.Escape, tables)
	case *IsNullExpr:
		collectTablesFromExpr(e.Expr, tables)
	case *CastExpr:
		collectTablesFromExpr(e.Expr, tables)
	case *CaseExpr:
		collectTablesFromExpr(e.Expr, tables)
		for _, w := range e.Whens {
			if w != nil {
				collectTablesFromExpr(w.Condition, tables)
				collectTablesFromExpr(w.Result, tables)
			}
		}
		collectTablesFromExpr(e.Else, tables)
	}
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
	if t.SubqueryStmt != nil {
		// Set-op derived tables must contribute their body to the cache key:
		// emitting only the name/alias made any two union-derived queries with
		// the same alias collide and the result cache serve wrong rows.
		s = "(" + statementToCacheKey(t.SubqueryStmt) + ")"
	}
	if t.Alias != "" {
		s += " " + t.Alias
	}
	return s
}

// statementToCacheKey serializes a Statement (set-operation trees, CTEs) for
// cache-key generation. Unknown statement types emit UncacheableMarker — the
// same contract as exprToStringImpl — so they are excluded from caching
// instead of colliding.
func statementToCacheKey(stmt Statement) string {
	switch s := stmt.(type) {
	case *SelectStmt:
		return QueryToSQL(s)
	case *UnionStmt:
		key := statementToCacheKey(s.Left) + fmt.Sprintf(" SETOP%d", int(s.Op))
		if s.All {
			key += " ALL"
		}
		key += " " + statementToCacheKey(s.Right)
		if len(s.OrderBy) > 0 {
			key += " ORDER BY "
			for i, ob := range s.OrderBy {
				if ob == nil {
					continue
				}
				if i > 0 {
					key += ","
				}
				key += ExprToString(ob.Expr)
				if ob.Desc {
					key += " DESC"
				}
			}
		}
		if s.Limit != nil {
			key += " LIMIT " + ExprToString(s.Limit)
		}
		if s.Offset != nil {
			key += " OFFSET " + ExprToString(s.Offset)
		}
		return key
	default:
		return UncacheableMarker
	}
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
	case *PlaceholderExpr:
		return "?" + fmt.Sprintf("%d", e.Index)
	case *SubqueryExpr:
		return "SUBQ(" + QueryToSQL(e.Query) + ")"
	case *ExistsExpr:
		s := "EXISTS(" + QueryToSQL(e.Subquery) + ")"
		if e.Not {
			s = "NOT " + s
		}
		return s
	case *WindowExpr:
		var sb strings.Builder
		sb.WriteString("WIN:")
		sb.WriteString(e.Function)
		sb.WriteByte('(')
		for i, arg := range e.Args {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(exprToStringImpl(arg, exported))
		}
		sb.WriteByte(')')
		if e.Filter != nil {
			sb.WriteString(" FILTER(")
			sb.WriteString(exprToStringImpl(e.Filter, exported))
			sb.WriteByte(')')
		}
		sb.WriteString(" OVER(")
		if len(e.PartitionBy) > 0 {
			sb.WriteString("PARTITION BY ")
			for i, pe := range e.PartitionBy {
				if i > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(exprToStringImpl(pe, exported))
			}
		}
		if len(e.OrderBy) > 0 {
			sb.WriteString(" ORDER BY ")
			for i, ob := range e.OrderBy {
				if ob == nil {
					continue
				}
				if i > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(exprToStringImpl(ob.Expr, exported))
				if ob.Desc {
					sb.WriteString(" DESC")
				}
			}
		}
		if e.Frame != nil {
			sb.WriteString(" FRAME:")
			sb.WriteString(e.Frame.Mode)
			if e.Frame.Start != nil {
				fmt.Fprintf(&sb, " S%s:%d", e.Frame.Start.Type, e.Frame.Start.Offset)
			}
			if e.Frame.End != nil {
				fmt.Fprintf(&sb, " E%s:%d", e.Frame.End.Type, e.Frame.End.Offset)
			}
		}
		sb.WriteByte(')')
		return sb.String()
	case *CaseExpr:
		var sb strings.Builder
		sb.WriteString("CASE")
		if e.Expr != nil {
			sb.WriteByte(' ')
			sb.WriteString(exprToStringImpl(e.Expr, exported))
		}
		for _, w := range e.Whens {
			if w == nil {
				continue
			}
			sb.WriteString(" WHEN ")
			sb.WriteString(exprToStringImpl(w.Condition, exported))
			sb.WriteString(" THEN ")
			sb.WriteString(exprToStringImpl(w.Result, exported))
		}
		if e.Else != nil {
			sb.WriteString(" ELSE ")
			sb.WriteString(exprToStringImpl(e.Else, exported))
		}
		sb.WriteString(" END")
		return sb.String()
	default:
		// Unknown expression node: emit the uncacheable sentinel. The previous
		// fmt.Sprintf("%T%+v") fallback embedded pointer addresses in the key,
		// so semantically identical statements never hit and — worse — nodes
		// whose printed form elided pointers could collide across different
		// predicates. Queries whose key contains this marker must not be
		// cached (see ContainsUncacheableExpr).
		return UncacheableMarker
	}
}

// UncacheableMarker is emitted into cache keys for expression nodes that have
// no explicit serialization. A key containing this marker must not be used
// for caching: two different unknown nodes would otherwise collide.
const UncacheableMarker = "\x00UNCACHEABLE\x00"

// ContainsUncacheableExpr reports whether a generated cache key contains
// expression nodes that could not be serialized deterministically.
func ContainsUncacheableExpr(sql string) bool {
	return strings.Contains(sql, UncacheableMarker)
}
