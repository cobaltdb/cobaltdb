package query

import ()

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
		if qo.canUseIndex(stmt.From.Name, stmt.Where) {
			// Mark for index usage - store hint in the TableRef
			if stmt.From.IndexHint == "" {
				stmt.From.IndexHint = "auto"
			}
		}
	}

	return stmt
}

// optimizeJoinOrder finds the optimal join order using a simple greedy algorithm
func (qo *QueryOptimizer) optimizeJoinOrder(stmt *SelectStmt) *SelectStmt {
	if len(stmt.Joins) == 0 {
		return stmt
	}

	// Estimate costs for different join orders
	tables := []string{stmt.From.Name}
	for _, join := range stmt.Joins {
		tables = append(tables, join.Table.Name)
	}

	// Simple heuristic: put tables with WHERE predicates first
	// and smaller tables before larger ones
	optimizedTables := qo.orderTablesBySelectivity(tables, stmt.Where)

	// Reorder JOINs based on selectivity, but only when safe:
	// Skip reorder if any tables are duplicated (self-join) or use aliases
	if len(optimizedTables) > 1 && len(stmt.Joins) > 0 {
		seen := make(map[string]bool)
		hasDuplicates := false
		for _, t := range tables {
			if seen[t] {
				hasDuplicates = true
				break
			}
			seen[t] = true
		}

		if !hasDuplicates {
			joinMap := make(map[string]*JoinClause)
			for _, join := range stmt.Joins {
				joinMap[join.Table.Name] = join
			}

			reordered := make([]*JoinClause, 0, len(stmt.Joins))
			for _, tableName := range optimizedTables {
				if j, ok := joinMap[tableName]; ok {
					reordered = append(reordered, j)
				}
			}
			// If we matched all joins, apply reorder; otherwise keep original
			if len(reordered) == len(stmt.Joins) {
				stmt.Joins = reordered
			}
		}
	}

	return stmt
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
			if col, ok := binExpr.Left.(*Identifier); ok {
				key := table + "." + col.Name
				if idxStats, ok := qo.stats.IndexStats[key]; ok {
					return idxStats.Selectivity
				}
			}
		}
	}

	return defaultSelectivity
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
	if stmt.From != nil && stmt.Where != nil {
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
