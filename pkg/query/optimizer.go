package query

import "strings"

// QueryOptimizer provides query optimization capabilities
type QueryOptimizer struct {
	stats OptimizerStats
}

// OptimizerStats holds statistics for a table
type OptimizerStats struct {
	RowCount    map[string]int64
	ColumnStats map[string]*OptimizerColStats
	IndexStats  map[string]*OptimizerIdxStats
}

// OptimizerColStats holds statistics for a column
type OptimizerColStats struct {
	DistinctCount int64
	NullCount     int64
	MinValue      interface{}
	MaxValue      interface{}
	Histogram     []Bucket
}

// Bucket represents a histogram bucket
type Bucket struct {
	LowerBound interface{}
	UpperBound interface{}
	Count      int64
}

// OptimizerIdxStats holds statistics for an index
type OptimizerIdxStats struct {
	TableName   string
	ColumnNames []string
	Unique      bool
	Selectivity float64
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer() *QueryOptimizer {
	return &QueryOptimizer{
		stats: OptimizerStats{
			RowCount:    make(map[string]int64),
			ColumnStats: make(map[string]*OptimizerColStats),
			IndexStats:  make(map[string]*OptimizerIdxStats),
		},
	}
}

// OptimizeSelect optimizes a SELECT statement
func (qo *QueryOptimizer) OptimizeSelect(stmt *SelectStmt) (*SelectStmt, error) {
	if stmt == nil {
		return nil, nil
	}

	// Fast path: nothing to optimize for simple SELECTs without WHERE/JOINs/subqueries.
	// Skip the expensive copy when no transformation will be applied.
	if stmt.Where == nil && len(stmt.Joins) == 0 && stmt.GroupBy == nil &&
		stmt.Having == nil && len(stmt.OrderBy) == 0 && !stmt.Distinct &&
		len(stmt.Columns) > 0 && stmt.Limit == nil && stmt.Offset == nil &&
		stmt.AsOf == nil {
		return stmt, nil
	}

	// Create a copy to avoid modifying the original
	optimized := qo.copySelectStmt(stmt)

	// Push down predicates
	optimized = qo.pushDownPredicates(optimized)

	// Optimize JOIN order
	if len(optimized.Joins) > 0 {
		optimized = qo.optimizeJoinOrder(optimized)
	}

	// Optimize projections
	optimized = qo.optimizeProjections(optimized)

	return optimized, nil
}

// pushDownPredicates pushes WHERE predicates as close to the source as possible
func (qo *QueryOptimizer) pushDownPredicates(stmt *SelectStmt) *SelectStmt {
	// If there's a subquery, try to push predicates into it
	if stmt.Where != nil && stmt.From != nil {
		// Check if we can use an index
		if !stmt.From.NotIndexed && qo.canUseIndex(stmt.From.Name, stmt.Where) {
			// Mark for index usage - store hint in the TableRef
			if stmt.From.IndexHint == "" {
				stmt.From.IndexHint = "auto"
			}
		}
	}

	return stmt
}

// optimizeJoinOrder finds the optimal join order using a simple greedy algorithm.
// Reordering is only applied when provably safe: every join must be a plain
// INNER join on a named table (no aliases, derived tables, USING or NATURAL
// clauses), no table may appear twice, and each candidate join's ON clause may
// reference only tables that are already part of the join prefix. Outer joins
// are never reordered — LEFT/RIGHT/FULL join results depend on join order.
func (qo *QueryOptimizer) optimizeJoinOrder(stmt *SelectStmt) *SelectStmt {
	if len(stmt.Joins) == 0 {
		return stmt
	}

	// Only consecutive INNER joins with simple named tables are candidates.
	for _, join := range stmt.Joins {
		if join == nil || join.Table == nil {
			return stmt
		}
		if join.Type != TokenJoin && join.Type != TokenInner {
			return stmt
		}
		if join.Natural || len(join.Using) > 0 {
			return stmt
		}
		if join.Table.Alias != "" || join.Table.Subquery != nil || join.Table.SubqueryStmt != nil {
			return stmt
		}
	}
	if stmt.From == nil || stmt.From.Alias != "" || stmt.From.Subquery != nil || stmt.From.SubqueryStmt != nil {
		return stmt
	}

	// Estimate costs for different join orders
	tables := []string{stmt.From.Name}
	for _, join := range stmt.Joins {
		tables = append(tables, join.Table.Name)
	}

	// Skip reorder if any tables are duplicated (self-join)
	seen := make(map[string]bool)
	for _, t := range tables {
		lt := strings.ToLower(t)
		if seen[lt] {
			return stmt
		}
		seen[lt] = true
	}

	// Simple heuristic: put tables with WHERE predicates first
	// and smaller tables before larger ones
	optimizedTables := qo.orderTablesBySelectivity(tables, stmt.Where)

	if len(optimizedTables) > 1 {
		joinMap := make(map[string]*JoinClause)
		for _, join := range stmt.Joins {
			joinMap[strings.ToLower(join.Table.Name)] = join
		}

		reordered := make([]*JoinClause, 0, len(stmt.Joins))
		for _, tableName := range optimizedTables {
			if j, ok := joinMap[strings.ToLower(tableName)]; ok {
				reordered = append(reordered, j)
			}
		}
		// Apply the reorder only if all joins were matched AND each join's ON
		// clause references only tables already joined at that point.
		if len(reordered) == len(stmt.Joins) && joinOrderIsValid(stmt.From.Name, reordered) {
			stmt.Joins = reordered
		}
	}

	return stmt
}

// joinOrderIsValid reports whether each join's ON clause references only
// tables available at that position (the FROM table plus previously joined
// tables and the join's own table). Conditions with unqualified column
// references cannot be attributed to a table, so they conservatively
// invalidate the order.
func joinOrderIsValid(fromTable string, joins []*JoinClause) bool {
	available := map[string]bool{strings.ToLower(fromTable): true}
	for _, join := range joins {
		available[strings.ToLower(join.Table.Name)] = true
		if join.Condition != nil {
			refs, ok := qualifiedTableRefs(join.Condition)
			if !ok {
				return false
			}
			for _, r := range refs {
				if !available[r] {
					return false
				}
			}
		}
	}
	return true
}

// qualifiedTableRefs collects the (lower-cased) table names referenced by
// qualified identifiers in expr. Returns ok=false when the expression contains
// unqualified identifiers or node types whose table references cannot be
// determined statically.
func qualifiedTableRefs(expr Expression) ([]string, bool) {
	var refs []string
	var walk func(e Expression) bool
	walk = func(e Expression) bool {
		switch v := e.(type) {
		case nil:
			return true
		case *QualifiedIdentifier:
			refs = append(refs, strings.ToLower(v.Table))
			return true
		case *ColumnRef:
			if v.Table == "" {
				return false
			}
			refs = append(refs, strings.ToLower(v.Table))
			return true
		case *StringLiteral, *NumberLiteral, *BooleanLiteral, *NullLiteral, *PlaceholderExpr:
			return true
		case *BinaryExpr:
			return walk(v.Left) && walk(v.Right)
		case *UnaryExpr:
			return walk(v.Expr)
		case *IsNullExpr:
			return walk(v.Expr)
		case *BetweenExpr:
			return walk(v.Expr) && walk(v.Lower) && walk(v.Upper)
		case *LikeExpr:
			return walk(v.Expr) && walk(v.Pattern)
		default:
			// Identifier (unqualified), functions, subqueries, CASE, ...:
			// cannot attribute references, refuse reorder.
			return false
		}
	}
	if !walk(expr) {
		return nil, false
	}
	return refs, true
}

// orderTablesBySelectivity orders tables by their selectivity (most selective first)
func (qo *QueryOptimizer) orderTablesBySelectivity(tables []string, where Expression) []string {
	if len(tables) <= 1 {
		return tables
	}

	// Calculate estimated cost for each table
	type tableCost struct {
		name string
		cost float64
	}

	costs := make([]tableCost, 0, len(tables))
	for _, table := range tables {
		cost := qo.estimateTableCost(table, where)
		costs = append(costs, tableCost{name: table, cost: cost})
	}

	// Sort by cost (lower cost = more selective = first)
	for i := 0; i < len(costs)-1; i++ {
		for j := i + 1; j < len(costs); j++ {
			if costs[j].cost < costs[i].cost {
				costs[i], costs[j] = costs[j], costs[i]
			}
		}
	}

	result := make([]string, 0, len(tables))
	for _, tc := range costs {
		result = append(result, tc.name)
	}
	return result
}

// estimateTableCost estimates the cost of scanning a table
func (qo *QueryOptimizer) estimateTableCost(table string, where Expression) float64 {
	rowCount := qo.stats.RowCount[table]
	if rowCount == 0 {
		rowCount = 1000 // Default assumption
	}

	// If there's a predicate on this table, estimate selectivity
	if where != nil {
		selectivity := qo.estimateSelectivity(table, where)
		return float64(rowCount) * selectivity
	}

	return float64(rowCount)
}

// estimateSelectivity estimates the selectivity of a WHERE clause
func (qo *QueryOptimizer) estimateSelectivity(table string, where Expression) float64 {
	// Default selectivity: 10% of rows match
	defaultSelectivity := 0.1

	// Try to analyze the expression
	if where == nil {
		return 1.0
	}

	// For equality predicates on indexed columns, use index selectivity
	if binExpr, ok := where.(*BinaryExpr); ok {
		if binExpr.Operator == TokenEq {
			var colName string
			switch col := binExpr.Left.(type) {
			case *QualifiedIdentifier:
				// Only use stats when the predicate is actually on this table.
				if !strings.EqualFold(col.Table, table) {
					return defaultSelectivity
				}
				colName = col.Column
			case *Identifier:
				colName = col.Name
			}
			if colName != "" {
				key := table + "." + colName
				if idxStats, ok := qo.stats.IndexStats[key]; ok &&
					indexStatsMatch(idxStats, table, colName) {
					return idxStats.Selectivity
				}
			}
		}
	}

	return defaultSelectivity
}

// indexStatsMatch verifies that the stats entry actually describes an index on
// the given table/column. The map key alone ("table.col") cannot be trusted:
// an unqualified WHERE column is keyed under every candidate table, so stats
// registered for one table would otherwise be applied to another.
func indexStatsMatch(stats *OptimizerIdxStats, table, column string) bool {
	if stats == nil {
		return false
	}
	if stats.TableName != "" && !strings.EqualFold(stats.TableName, table) {
		return false
	}
	for _, c := range stats.ColumnNames {
		if strings.EqualFold(c, column) {
			return true
		}
	}
	return len(stats.ColumnNames) == 0
}

// canUseIndex checks if an index can be used for a WHERE clause
func (qo *QueryOptimizer) canUseIndex(table string, where Expression) bool {
	if where == nil {
		return false
	}

	// Check for simple equality or range predicates
	switch expr := where.(type) {
	case *BinaryExpr:
		if expr.Operator == TokenEq || expr.Operator == TokenGt ||
			expr.Operator == TokenLt || expr.Operator == TokenGte ||
			expr.Operator == TokenLte || expr.Operator == TokenLike {
			if _, ok := expr.Left.(*Identifier); ok {
				return true
			}
		}
		// Check both sides of AND/OR
		if expr.Operator == TokenAnd || expr.Operator == TokenOr {
			return qo.canUseIndex(table, expr.Left) || qo.canUseIndex(table, expr.Right)
		}
	}

	return false
}

// optimizeProjections applies projection optimizations
func (qo *QueryOptimizer) optimizeProjections(stmt *SelectStmt) *SelectStmt {
	if stmt == nil || len(stmt.Columns) == 0 {
		return stmt
	}

	// If SELECT * (StarExpr), no projection optimization possible
	for _, col := range stmt.Columns {
		if _, ok := col.(*StarExpr); ok {
			return stmt
		}
	}

	// Mark index hints for columns referenced in WHERE that have indexes
	if stmt.From != nil && stmt.Where != nil && !stmt.From.NotIndexed {
		cols := qo.extractWhereColumns(stmt.Where)
		for _, col := range cols {
			key := stmt.From.Name + "." + col
			if _, ok := qo.stats.IndexStats[key]; ok {
				if stmt.From.IndexHint == "" {
					stmt.From.IndexHint = "auto"
				}
				break
			}
		}
	}

	return stmt
}

// extractWhereColumns extracts column names referenced in a WHERE clause
func (qo *QueryOptimizer) extractWhereColumns(expr Expression) []string {
	var cols []string
	switch e := expr.(type) {
	case *Identifier:
		cols = append(cols, e.Name)
	case *QualifiedIdentifier:
		cols = append(cols, e.Column)
	case *BinaryExpr:
		cols = append(cols, qo.extractWhereColumns(e.Left)...)
		cols = append(cols, qo.extractWhereColumns(e.Right)...)
	}
	return cols
}

// copySelectStmt creates a copy of a SELECT statement to avoid modifying the original
func (qo *QueryOptimizer) copySelectStmt(stmt *SelectStmt) *SelectStmt {
	copied := *stmt
	// Deep copy From to avoid race on IndexHint field
	if stmt.From != nil {
		fromCopy := *stmt.From
		copied.From = &fromCopy
	}
	if len(stmt.Joins) > 0 {
		copied.Joins = make([]*JoinClause, len(stmt.Joins))
		copy(copied.Joins, stmt.Joins)
	}
	if len(stmt.Columns) > 0 {
		copied.Columns = make([]Expression, len(stmt.Columns))
		copy(copied.Columns, stmt.Columns)
	}
	if len(stmt.OrderBy) > 0 {
		copied.OrderBy = make([]*OrderByExpr, len(stmt.OrderBy))
		copy(copied.OrderBy, stmt.OrderBy)
	}
	if len(stmt.GroupBy) > 0 {
		copied.GroupBy = make([]Expression, len(stmt.GroupBy))
		copy(copied.GroupBy, stmt.GroupBy)
	}
	return &copied
}
