package query

import (
	"fmt"
	"math"
	"strings"
)

// PlanType represents the type of query plan
type PlanType int

const (
	PlanTypeSeqScan PlanType = iota
	PlanTypeIndexScan
	PlanTypeJoin
	PlanTypeSort
	PlanTypeAggregate
	PlanTypeLimit
	PlanTypeInsert
	PlanTypeUpdate
	PlanTypeDelete
)

// JoinMethod represents the join algorithm
type JoinMethod int

const (
	JoinMethodNestedLoop JoinMethod = iota
	JoinMethodHash
	JoinMethodMerge
)

// JoinType represents the type of join
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
)

// QueryPlan represents an execution plan for a query
type QueryPlan struct {
	Type       PlanType
	Cost       float64
	EstRows    uint64
	Table      string
	Index      string
	Columns    []string
	Filter     Expression
	Children   []*QueryPlan
	JoinMethod JoinMethod
	JoinType   JoinType
	JoinCond   Expression
	OrderBy    []OrderByClause
	Limit      int
	Offset     int
	AggFuncs   []*AggregateFunc
	GroupBy    []string
}

// OrderByClause represents an ORDER BY clause
type OrderByClause struct {
	Column string
	Desc   bool
}

// AggregateFunc represents an aggregate function
type AggregateFunc struct {
	Name   string
	Column string
}

// Planner creates query execution plans
type Planner struct {
	catalog     CatalogInterface
	stats       StatsInterface
	defaultCost CostModel
}

// CatalogInterface provides catalog access for the planner
type CatalogInterface interface {
	GetTable(name string) (*TableInfo, error)
	GetIndex(name string) (*IndexInfo, error)
	GetTableIndexes(tableName string) ([]*IndexInfo, error)
}

// TableInfo represents table metadata
type TableInfo struct {
	Name      string
	Columns   []ColumnInfo
	RowCount  uint64
	PageCount uint64
}

// ColumnInfo represents column metadata
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// IndexInfo represents index metadata
type IndexInfo struct {
	Name      string
	TableName string
	Columns   []string
	IsUnique  bool
}

// StatsInterface provides statistics access
type StatsInterface interface {
	GetTableStats(tableName string) (TableStats, bool)
	EstimateSelectivity(tableName, column, op string, value interface{}) float64
	EstimateSeqScanCost(tableName string, selectivity float64) float64
	EstimateIndexScanCost(tableName, indexName string, selectivity float64) float64
}

// TableStats represents table statistics
type TableStats struct {
	RowCount  uint64
	PageCount uint64
}

// CostModel contains cost constants
type CostModel struct {
	SeqPageCost       float64
	RandomPageCost    float64
	CpuTupleCost      float64
	CpuIndexTupleCost float64
	CpuOperatorCost   float64
}

// DefaultCostModel returns the default cost model
func DefaultCostModel() CostModel {
	return CostModel{
		SeqPageCost:       1.0,
		RandomPageCost:    4.0,
		CpuTupleCost:      0.01,
		CpuIndexTupleCost: 0.005,
		CpuOperatorCost:   0.0025,
	}
}

// NewPlanner creates a new query planner
func NewPlanner(catalog CatalogInterface, stats StatsInterface) *Planner {
	return &Planner{
		catalog:     catalog,
		stats:       stats,
		defaultCost: DefaultCostModel(),
	}
}

// Plan creates an execution plan for a SELECT statement
func (p *Planner) Plan(stmt *SelectStmt) (*QueryPlan, error) {
	// Start with the FROM clause
	plan, err := p.planFrom(stmt.From, stmt.Joins)
	if err != nil {
		return nil, err
	}

	// Add WHERE filter
	if stmt.Where != nil {
		plan = p.addFilter(plan, stmt.Where)
	}

	// Add GROUP BY and aggregates
	if len(stmt.GroupBy) > 0 || p.hasAggregates(stmt.Columns) {
		groupByCols := make([]string, 0, len(stmt.GroupBy))
		for _, expr := range stmt.GroupBy {
			if ident, ok := expr.(*Identifier); ok {
				groupByCols = append(groupByCols, ident.Name)
			}
		}
		plan = p.addAggregate(plan, stmt.Columns, groupByCols)
	}

	// Add ORDER BY
	if len(stmt.OrderBy) > 0 {
		plan = p.addSort(plan, stmt.OrderBy)
	}

	// Add LIMIT/OFFSET
	if stmt.Limit != nil {
		limit := p.evaluateLimitExpression(stmt.Limit)
		offset := 0
		if stmt.Offset != nil {
			offset = p.evaluateLimitExpression(stmt.Offset)
		}
		if limit > 0 {
			plan = p.addLimit(plan, limit, offset)
		}
	}

	// Project columns
	plan = p.addProject(plan, stmt.Columns)

	return plan, nil
}

// planFrom creates a plan for the FROM clause
func (p *Planner) planFrom(from *TableRef, joins []*JoinClause) (*QueryPlan, error) {
	if from == nil {
		return nil, fmt.Errorf("no table specified")
	}

	// Get table info
	tableInfo, err := p.catalog.GetTable(from.Name)
	if err != nil {
		return nil, err
	}

	// Start with a scan of the primary table
	plan := p.createScanPlan(from.Name, tableInfo)

	// Add joins
	for _, join := range joins {
		joinPlan, err := p.createJoinPlan(plan, join)
		if err != nil {
			return nil, err
		}
		plan = joinPlan
	}

	return plan, nil
}

// createScanPlan creates a scan plan (seq or index)
func (p *Planner) createScanPlan(tableName string, tableInfo *TableInfo) *QueryPlan {
	// For now, always use sequential scan
	// In a full implementation, we'd check if an index can be used
	plan := &QueryPlan{
		Type:    PlanTypeSeqScan,
		Table:   tableName,
		Columns: getColumnNames(tableInfo.Columns),
	}

	// Estimate cost
	if stats, ok := p.stats.GetTableStats(tableName); ok {
		plan.EstRows = stats.RowCount
		plan.Cost = p.stats.EstimateSeqScanCost(tableName, 1.0)
	} else {
		plan.EstRows = tableInfo.RowCount
		plan.Cost = float64(tableInfo.PageCount) * p.defaultCost.SeqPageCost
	}

	return plan
}

// createJoinPlan creates a join plan
func (p *Planner) createJoinPlan(left *QueryPlan, join *JoinClause) (*QueryPlan, error) {
	// Get right table info
	rightTable, err := p.catalog.GetTable(join.Table.Name)
	if err != nil {
		return nil, err
	}

	// Create scan plan for right table
	right := p.createScanPlan(join.Table.Name, rightTable)

	// Choose join method based on table sizes
	joinMethod := p.chooseJoinMethod(left, right)

	joinPlan := &QueryPlan{
		Type:       PlanTypeJoin,
		JoinMethod: joinMethod,
		JoinType:   convertJoinType(join.Type),
		JoinCond:   join.Condition,
		Children:   []*QueryPlan{left, right},
	}

	// Estimate cost and rows
	joinPlan.EstRows = p.estimateJoinRows(left, right, joinMethod)
	joinPlan.Cost = p.estimateJoinCost(left, right, joinMethod)

	return joinPlan, nil
}

// chooseJoinMethod selects the best join algorithm
func (p *Planner) chooseJoinMethod(left, right *QueryPlan) JoinMethod {
	// Simple heuristic: use hash join for larger tables, nested loop for small ones
	if left.EstRows < 100 || right.EstRows < 100 {
		return JoinMethodNestedLoop
	}

	if left.EstRows > 10000 && right.EstRows > 10000 {
		return JoinMethodHash
	}

	// Check if inputs are sorted (for merge join)
	// For now, default to hash join for medium tables
	return JoinMethodHash
}

// estimateJoinRows estimates output rows of a join
func (p *Planner) estimateJoinRows(left, right *QueryPlan, method JoinMethod) uint64 {
	// Simple estimation: assume 30% selectivity for joins
	selectivity := 0.3
	return uint64(float64(left.EstRows) * float64(right.EstRows) * selectivity)
}

// estimateJoinCost estimates the cost of a join
func (p *Planner) estimateJoinCost(left, right *QueryPlan, method JoinMethod) float64 {
	switch method {
	case JoinMethodNestedLoop:
		return left.Cost + float64(left.EstRows)*right.Cost

	case JoinMethodHash:
		// Build hash table + probe
		buildCost := right.Cost + float64(right.EstRows)*p.defaultCost.CpuTupleCost
		probeCost := left.Cost + float64(left.EstRows)*p.defaultCost.CpuOperatorCost
		return buildCost + probeCost

	case JoinMethodMerge:
		// Sort cost + merge cost
		sortCost := (float64(left.EstRows) + float64(right.EstRows)) * p.defaultCost.CpuTupleCost
		mergeCost := (float64(left.EstRows) + float64(right.EstRows)) * p.defaultCost.CpuTupleCost
		return left.Cost + right.Cost + sortCost + mergeCost

	default:
		return left.Cost + right.Cost
	}
}

// addFilter adds a filter to a plan
func (p *Planner) addFilter(child *QueryPlan, filter Expression) *QueryPlan {
	// Check if we can use an index
	if child.Type == PlanTypeSeqScan {
		if indexPlan := p.tryIndexScan(child.Table, filter); indexPlan != nil {
			return indexPlan
		}
	}

	// Add filter to existing plan
	return &QueryPlan{
		Type:     child.Type,
		Table:    child.Table,
		Index:    child.Index,
		Columns:  child.Columns,
		Filter:   filter,
		Children: child.Children,
		Cost:     child.Cost * 1.1,                     // Slight overhead for filtering
		EstRows:  uint64(float64(child.EstRows) * 0.3), // Assume 30% selectivity
	}
}

// tryIndexScan tries to create an index scan plan
func (p *Planner) tryIndexScan(tableName string, filter Expression) *QueryPlan {
	// Check if filter is suitable for index scan
	// Look for equality conditions on indexed columns
	indexes, _ := p.catalog.GetTableIndexes(tableName)

	for _, idx := range indexes {
		if canUseIndex(filter, idx) {
			// Estimate selectivity
			selectivity := 0.1 // Assume 10% for index lookup

			plan := &QueryPlan{
				Type:   PlanTypeIndexScan,
				Table:  tableName,
				Index:  idx.Name,
				Filter: filter,
			}

			if stats, ok := p.stats.GetTableStats(tableName); ok {
				plan.EstRows = uint64(float64(stats.RowCount) * selectivity)
				plan.Cost = p.stats.EstimateIndexScanCost(tableName, idx.Name, selectivity)
			} else {
				plan.EstRows = 100 // Default estimate
				plan.Cost = 100 * p.defaultCost.RandomPageCost
			}

			return plan
		}
	}

	return nil
}

// canUseIndex checks if an index can be used for a filter
func canUseIndex(filter Expression, idx *IndexInfo) bool {
	// Check for equality condition on first index column
	if binaryExpr, ok := filter.(*BinaryExpr); ok {
		if binaryExpr.Operator == TokenEq {
			if ident, ok := binaryExpr.Left.(*Identifier); ok {
				return ident.Name == idx.Columns[0]
			}
		}
	}
	return false
}

// addAggregate adds aggregation to a plan
func (p *Planner) addAggregate(child *QueryPlan, columns []Expression, groupBy []string) *QueryPlan {
	aggFuncs := extractAggregates(columns)

	return &QueryPlan{
		Type:     PlanTypeAggregate,
		Children: []*QueryPlan{child},
		AggFuncs: aggFuncs,
		GroupBy:  groupBy,
		EstRows:  estimateGroupByRows(child.EstRows, groupBy),
		Cost:     child.Cost + float64(child.EstRows)*p.defaultCost.CpuTupleCost,
	}
}

// addSort adds sorting to a plan
func (p *Planner) addSort(child *QueryPlan, orderBy []*OrderByExpr) *QueryPlan {
	orderClauses := make([]OrderByClause, len(orderBy))
	for i, ob := range orderBy {
		if ident, ok := ob.Expr.(*Identifier); ok {
			orderClauses[i] = OrderByClause{
				Column: ident.Name,
				Desc:   ob.Desc,
			}
		}
	}

	// Sort cost: O(n log n)
	sortCost := float64(child.EstRows) * log2(float64(child.EstRows)) * p.defaultCost.CpuTupleCost

	return &QueryPlan{
		Type:     PlanTypeSort,
		Children: []*QueryPlan{child},
		OrderBy:  orderClauses,
		EstRows:  child.EstRows,
		Cost:     child.Cost + sortCost,
	}
}

// addLimit adds limit/offset to a plan
func (p *Planner) addLimit(child *QueryPlan, limit, offset int) *QueryPlan {
	estRows := uint64(limit)
	if estRows > child.EstRows {
		estRows = child.EstRows
	}

	return &QueryPlan{
		Type:     PlanTypeLimit,
		Children: []*QueryPlan{child},
		Limit:    limit,
		Offset:   offset,
		EstRows:  estRows,
		Cost:     child.Cost + float64(child.EstRows)*p.defaultCost.CpuTupleCost,
	}
}

// evaluateLimitExpression evaluates a limit/offset expression to an integer
func (p *Planner) evaluateLimitExpression(expr Expression) int {
	switch e := expr.(type) {
	case *NumberLiteral:
		return int(e.Value)
	}
	return 0
}

// addProject adds column projection to a plan
func (p *Planner) addProject(child *QueryPlan, columns []Expression) *QueryPlan {
	columnNames := make([]string, 0, len(columns))
	for _, col := range columns {
		if ident, ok := col.(*Identifier); ok {
			columnNames = append(columnNames, ident.Name)
		} else if star, ok := col.(*StarExpr); ok {
			if star.Table == "" {
				columnNames = child.Columns // All columns
			}
		}
	}

	return &QueryPlan{
		Type:     child.Type,
		Table:    child.Table,
		Index:    child.Index,
		Columns:  columnNames,
		Filter:   child.Filter,
		Children: child.Children,
		EstRows:  child.EstRows,
		Cost:     child.Cost,
	}
}

// hasAggregates checks if expressions contain aggregate functions
func (p *Planner) hasAggregates(exprs []Expression) bool {
	for _, expr := range exprs {
		if containsAggregate(expr) {
			return true
		}
	}
	return false
}

// containsAggregate checks if an expression contains an aggregate
func containsAggregate(expr Expression) bool {
	switch e := expr.(type) {
	case *FunctionCall:
		name := strings.ToUpper(e.Name)
		if name == "COUNT" || name == "SUM" || name == "AVG" || name == "MIN" || name == "MAX" {
			return true
		}
		for _, arg := range e.Args {
			if containsAggregate(arg) {
				return true
			}
		}
	}
	return false
}

// extractAggregates extracts aggregate functions from expressions
func extractAggregates(exprs []Expression) []*AggregateFunc {
	var aggs []*AggregateFunc
	for _, expr := range exprs {
		if fc, ok := expr.(*FunctionCall); ok {
			name := strings.ToUpper(fc.Name)
			if name == "COUNT" || name == "SUM" || name == "AVG" || name == "MIN" || name == "MAX" {
				col := "*"
				if len(fc.Args) > 0 {
					if ident, ok := fc.Args[0].(*Identifier); ok {
						col = ident.Name
					}
				}
				aggs = append(aggs, &AggregateFunc{
					Name:   name,
					Column: col,
				})
			}
		}
	}
	return aggs
}

// estimateGroupByRows estimates rows after GROUP BY
func estimateGroupByRows(inputRows uint64, groupBy []string) uint64 {
	if len(groupBy) == 0 {
		return 1 // Single result row for aggregates without GROUP BY
	}
	// Rough estimate: assume 10% of input rows as groups
	groups := uint64(float64(inputRows) * 0.1)
	if groups < 1 {
		groups = 1
	}
	return groups
}

// convertJoinType converts parser join type to plan join type
func convertJoinType(joinType TokenType) JoinType {
	switch joinType {
	case TokenLeft:
		return JoinTypeLeft
	case TokenRight:
		return JoinTypeRight
	case TokenOuter:
		return JoinTypeFull
	default:
		return JoinTypeInner
	}
}

// getColumnNames extracts column names from ColumnInfo
func getColumnNames(columns []ColumnInfo) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}

// log2 calculates log base 2
func log2(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Using change of base: log2(x) = ln(x) / ln(2)
	return math.Log(x) / math.Log(2)
}

// Explain generates a human-readable explanation of the plan
func (p *QueryPlan) Explain() string {
	var sb strings.Builder
	p.explainImpl(&sb, 0)
	return sb.String()
}

func (p *QueryPlan) explainImpl(sb *strings.Builder, indent int) {
	prefix := strings.Repeat("  ", indent)

	switch p.Type {
	case PlanTypeSeqScan:
		fmt.Fprintf(sb, "%sSeq Scan on %s (cost=%.2f rows=%d)\n", prefix, p.Table, p.Cost, p.EstRows)
		if p.Filter != nil {
			fmt.Fprintf(sb, "%s  Filter: %v\n", prefix, p.Filter)
		}

	case PlanTypeIndexScan:
		fmt.Fprintf(sb, "%sIndex Scan using %s on %s (cost=%.2f rows=%d)\n",
			prefix, p.Index, p.Table, p.Cost, p.EstRows)
		if p.Filter != nil {
			fmt.Fprintf(sb, "%s  Filter: %v\n", prefix, p.Filter)
		}

	case PlanTypeJoin:
		joinType := "Inner"
		switch p.JoinType {
		case JoinTypeLeft:
			joinType = "Left"
		case JoinTypeRight:
			joinType = "Right"
		case JoinTypeFull:
			joinType = "Full"
		}
		method := "Nested Loop"
		switch p.JoinMethod {
		case JoinMethodHash:
			method = "Hash"
		case JoinMethodMerge:
			method = "Merge"
		}
		fmt.Fprintf(sb, "%s%s %s Join (cost=%.2f rows=%d)\n", prefix, method, joinType, p.Cost, p.EstRows)
		for _, child := range p.Children {
			child.explainImpl(sb, indent+1)
		}

	case PlanTypeSort:
		fmt.Fprintf(sb, "%sSort (cost=%.2f rows=%d)\n", prefix, p.Cost, p.EstRows)
		for _, child := range p.Children {
			child.explainImpl(sb, indent+1)
		}

	case PlanTypeAggregate:
		fmt.Fprintf(sb, "%sAggregate (cost=%.2f rows=%d)\n", prefix, p.Cost, p.EstRows)
		for _, child := range p.Children {
			child.explainImpl(sb, indent+1)
		}

	case PlanTypeLimit:
		fmt.Fprintf(sb, "%sLimit (cost=%.2f rows=%d)\n", prefix, p.Cost, p.EstRows)
		for _, child := range p.Children {
			child.explainImpl(sb, indent+1)
		}

	default:
		fmt.Fprintf(sb, "%sUnknown (cost=%.2f rows=%d)\n", prefix, p.Cost, p.EstRows)
	}
}
