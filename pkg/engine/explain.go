package engine

import (
	"fmt"
	"math"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// PlanNode represents a node in a query execution plan
type PlanNode struct {
	ID        int
	ParentID  int
	Operation string
	Detail    string
	Cost      float64
	Rows      int64
}

// QueryPlan represents a structured query execution plan
type QueryPlan struct {
	Nodes []PlanNode
}

// planBuilder holds state while building a query plan
type planBuilder struct {
	db      *DB
	nodes   []PlanNode
	nextID  int
	nodeMap map[int]int // node ID -> index in nodes slice
}

func newPlanBuilder(db *DB) *planBuilder {
	return &planBuilder{
		db:      db,
		nodes:   make([]PlanNode, 0),
		nextID:  1,
		nodeMap: make(map[int]int),
	}
}

func (pb *planBuilder) addNode(parentID int, operation, detail string, cost float64, rows int64) int {
	id := pb.nextID
	pb.nextID++
	node := PlanNode{
		ID:        id,
		ParentID:  parentID,
		Operation: operation,
		Detail:    detail,
		Cost:      cost,
		Rows:      rows,
	}
	pb.nodeMap[id] = len(pb.nodes)
	pb.nodes = append(pb.nodes, node)
	return id
}

func (pb *planBuilder) getNode(nodeID int) *PlanNode {
	if idx, ok := pb.nodeMap[nodeID]; ok {
		return &pb.nodes[idx]
	}
	return nil
}

func (pb *planBuilder) getNodeCost(nodeID int) float64 {
	if node := pb.getNode(nodeID); node != nil {
		return node.Cost
	}
	return 0
}

func (pb *planBuilder) getNodeRows(nodeID int) int64 {
	if node := pb.getNode(nodeID); node != nil {
		return node.Rows
	}
	return 1000
}

// buildQueryPlan builds a structured plan from a SELECT statement
func (db *DB) buildQueryPlan(stmt *query.SelectStmt) *QueryPlan {
	pb := newPlanBuilder(db)
	db.buildSelectPlan(stmt, pb, 0)
	return &QueryPlan{Nodes: pb.nodes}
}

// buildSelectPlan recursively builds plan nodes for a SELECT statement
func (db *DB) buildSelectPlan(stmt *query.SelectStmt, pb *planBuilder, parentID int) int {
	// Start with the FROM table scan
	var currentID int
	if stmt.From != nil {
		currentID = db.buildTableScanPlan(stmt.From, stmt.Where, pb, parentID)
	}

	// Add JOIN nodes
	for _, join := range stmt.Joins {
		currentID = db.buildJoinPlan(join, currentID, pb, parentID)
	}

	// Add Filter node for WHERE if not pushed down to index scan
	if stmt.Where != nil && currentID > 0 {
		filterDetail := expressionToString(stmt.Where)
		rows := pb.getNodeRows(currentID)
		if rows > 1 {
			rows = int64(float64(rows) * 0.1)
			if rows < 1 {
				rows = 1
			}
		}
		cost := pb.getNodeCost(currentID) + float64(rows)*0.5
		currentID = pb.addNode(parentID, "Filter", filterDetail, cost, rows)
	}

	// Add Aggregate node
	if len(stmt.GroupBy) > 0 || hasAggregates(stmt.Columns) {
		aggDetail := ""
		if len(stmt.GroupBy) > 0 {
			aggDetail = "GROUP BY " + expressionsToString(stmt.GroupBy)
		}
		if hasAggregates(stmt.Columns) {
			if aggDetail != "" {
				aggDetail += ", "
			}
			aggDetail += "AGGREGATES"
		}
		rows := int64(1)
		if len(stmt.GroupBy) > 0 {
			inputRows := pb.getNodeRows(currentID)
			rows = int64(float64(inputRows) * 0.1)
			if rows < 1 {
				rows = 1
			}
		}
		cost := pb.getNodeCost(currentID)
		if currentID > 0 {
			cost += float64(rows) * 0.1
		}
		currentID = pb.addNode(parentID, "Aggregate", aggDetail, cost, rows)
	}

	// Add Sort node
	if len(stmt.OrderBy) > 0 {
		sortDetail := orderByToString(stmt.OrderBy)
		inputRows := pb.getNodeRows(currentID)
		cost := pb.getNodeCost(currentID)
		if inputRows > 1 {
			cost += float64(inputRows) * math.Log2(float64(inputRows)) * 0.01
		}
		currentID = pb.addNode(parentID, "Sort", sortDetail, cost, inputRows)
	}

	// Add Limit node
	if stmt.Limit != nil {
		limitDetail := expressionToString(stmt.Limit)
		inputRows := pb.getNodeRows(currentID)
		limitRows := db.estimateLimitRows(stmt.Limit, inputRows)
		cost := pb.getNodeCost(currentID)
		if inputRows > 0 {
			cost *= float64(limitRows) / float64(inputRows)
		}
		currentID = pb.addNode(parentID, "Limit", limitDetail, cost, limitRows)
	}

	return currentID
}

// buildTableScanPlan builds a plan node for a table scan
func (db *DB) buildTableScanPlan(tableRef *query.TableRef, where query.Expression, pb *planBuilder, parentID int) int {
	tableName := tableRef.Name
	if tableRef.Alias != "" {
		tableName = tableRef.Alias
	}

	rows := db.estimateTableRows(tableRef.Name)
	indexHint := tableRef.IndexHint

	// Check if we can use an index
	bestIndex := ""
	if where != nil && db.optimizer != nil {
		bestIndex = db.optimizer.SelectBestIndex(tableRef.Name, where)
	}

	if bestIndex != "" || indexHint != "" {
		detail := tableName
		if bestIndex != "" {
			detail += " (index: " + bestIndex + ")"
		} else if indexHint != "" && indexHint != "auto" {
			detail += " (hint: " + indexHint + ")"
		}
		selectivity := 0.1
		if bestIndex != "" {
			stats := db.optimizer.GetTableStatistics(tableRef.Name)
			if stats != nil && stats.IndexStats[bestIndex] != nil {
				selectivity = float64(stats.IndexStats[bestIndex].Selectivity)
				if selectivity <= 0 {
					selectivity = 0.1
				}
			}
		}
		indexRows := int64(float64(rows) * selectivity)
		if indexRows < 1 {
			indexRows = 1
		}
		cost := float64(indexRows) * 0.5
		return pb.addNode(parentID, "Index Scan", detail, cost, indexRows)
	}

	cost := float64(rows) * 1.0
	return pb.addNode(parentID, "Seq Scan", tableName, cost, rows)
}

// buildJoinPlan builds a plan node for a JOIN
func (db *DB) buildJoinPlan(join *query.JoinClause, leftID int, pb *planBuilder, parentID int) int {
	joinTypeStr := joinTypeToString(join.Type)
	if join.Natural {
		joinTypeStr = "Natural " + joinTypeStr
	}

	// Build right side scan
	rightID := db.buildTableScanPlan(join.Table, nil, pb, parentID)

	// Join condition detail
	joinDetail := ""
	if join.Condition != nil {
		joinDetail = expressionToString(join.Condition)
	} else if len(join.Using) > 0 {
		joinDetail = "USING (" + strings.Join(join.Using, ", ") + ")"
	}

	leftRows := pb.getNodeRows(leftID)
	rightRows := pb.getNodeRows(rightID)
	outputRows := db.estimateJoinRows(join, leftRows, rightRows)
	cost := pb.getNodeCost(leftID) + pb.getNodeCost(rightID) + float64(outputRows)*2.0

	return pb.addNode(parentID, joinTypeStr+" Join", joinDetail, cost, outputRows)
}

// estimateTableRows estimates the row count for a table
func (db *DB) estimateTableRows(tableName string) int64 {
	if db.optimizer != nil {
		stats := db.optimizer.GetTableStatistics(tableName)
		if stats != nil && stats.RowCount > 0 {
			return stats.RowCount
		}
	}
	return 1000 // Default assumption
}

// estimateJoinRows estimates output rows for a join
func (db *DB) estimateJoinRows(join *query.JoinClause, leftRows, rightRows int64) int64 {
	switch join.Type {
	case query.TokenCross:
		return leftRows * rightRows
	case query.TokenLeft, query.TokenRight:
		return max(leftRows, rightRows)
	case query.TokenOuter:
		return leftRows + rightRows
	default: // INNER
		return int64(float64(leftRows) * float64(rightRows) * 0.1)
	}
}

// estimateLimitRows estimates output rows for LIMIT
func (db *DB) estimateLimitRows(limit query.Expression, inputRows int64) int64 {
	if limit == nil {
		return inputRows
	}
	// Try to evaluate the limit expression if it's a literal
	switch v := limit.(type) {
	case *query.NumberLiteral:
		if int64(v.Value) < inputRows {
			return int64(v.Value)
		}
		return inputRows
	}
	return inputRows
}

// formatQueryPlan formats a query plan as result rows
func formatQueryPlan(plan *QueryPlan) ([]string, [][]interface{}) {
	columns := []string{"id", "parent_id", "operation", "detail", "cost", "rows"}
	rows := make([][]interface{}, 0, len(plan.Nodes))

	for _, node := range plan.Nodes {
		rows = append(rows, []interface{}{
			int64(node.ID),
			int64(node.ParentID),
			node.Operation,
			node.Detail,
			fmt.Sprintf("%.2f", node.Cost),
			node.Rows,
		})
	}

	return columns, rows
}

// joinTypeToString converts a join token type to a string
func joinTypeToString(t query.TokenType) string {
	switch t {
	case query.TokenInner:
		return "Inner"
	case query.TokenLeft:
		return "Left"
	case query.TokenRight:
		return "Right"
	case query.TokenOuter:
		return "Full Outer"
	case query.TokenCross:
		return "Cross"
	default:
		return "Inner"
	}
}

// expressionsToString converts a slice of expressions to a comma-separated string
func expressionsToString(exprs []query.Expression) string {
	parts := make([]string, len(exprs))
	for i, expr := range exprs {
		parts[i] = expressionToString(expr)
	}
	return strings.Join(parts, ", ")
}

// orderByToString converts ORDER BY expressions to a string
func orderByToString(orderBy []*query.OrderByExpr) string {
	parts := make([]string, len(orderBy))
	for i, ob := range orderBy {
		s := expressionToString(ob.Expr)
		if ob.Desc {
			s += " DESC"
		} else {
			s += " ASC"
		}
		parts[i] = s
	}
	return strings.Join(parts, ", ")
}

// hasAggregates checks if a column list contains aggregate functions
func hasAggregates(cols []query.Expression) bool {
	for _, col := range cols {
		if isAggregateExpr(col) {
			return true
		}
	}
	return false
}

// isAggregateExpr checks if an expression is an aggregate function call
func isAggregateExpr(expr query.Expression) bool {
	fc, ok := expr.(*query.FunctionCall)
	if !ok {
		return false
	}
	switch strings.ToUpper(fc.Name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "JSON_ARRAYAGG", "JSON_OBJECTAGG":
		return true
	}
	return false
}

// buildInsertPlan builds a query plan for an INSERT statement
func (db *DB) buildInsertPlan(stmt *query.InsertStmt) *QueryPlan {
	pb := newPlanBuilder(db)
	tableName := stmt.Table
	if stmt.Table == "" {
		tableName = "<unknown>"
	}
	rows := int64(1)
	if len(stmt.Values) > 0 {
		rows = int64(len(stmt.Values))
	}
	cost := float64(rows) * 2.0
	detail := tableName
	if len(stmt.Columns) > 0 {
		detail += " (" + strings.Join(stmt.Columns, ", ") + ")"
	}
	pb.addNode(0, "Insert", detail, cost, rows)
	return &QueryPlan{Nodes: pb.nodes}
}

// buildUpdatePlan builds a query plan for an UPDATE statement
func (db *DB) buildUpdatePlan(stmt *query.UpdateStmt) *QueryPlan {
	pb := newPlanBuilder(db)
	tableName := stmt.Table
	if stmt.Table == "" {
		tableName = "<unknown>"
	}
	rows := db.estimateTableRows(tableName)
	cost := float64(rows) * 2.0
	if stmt.Where != nil {
		rows = int64(float64(rows) * 0.1)
		if rows < 1 {
			rows = 1
		}
		cost = float64(rows) * 3.0
	}
	detail := tableName
	if len(stmt.Set) > 0 {
		sets := make([]string, 0, len(stmt.Set))
		for _, sc := range stmt.Set {
			sets = append(sets, sc.Column)
		}
		detail += " SET " + strings.Join(sets, ", ")
	}
	pb.addNode(0, "Update", detail, cost, rows)
	if stmt.Where != nil {
		filterDetail := expressionToString(stmt.Where)
		pb.addNode(1, "Filter", filterDetail, float64(rows)*0.5, rows)
	}
	return &QueryPlan{Nodes: pb.nodes}
}

// buildDeletePlan builds a query plan for a DELETE statement
func (db *DB) buildDeletePlan(stmt *query.DeleteStmt) *QueryPlan {
	pb := newPlanBuilder(db)
	tableName := stmt.Table
	if stmt.Table == "" {
		tableName = "<unknown>"
	}
	rows := db.estimateTableRows(tableName)
	cost := float64(rows) * 2.0
	if stmt.Where != nil {
		rows = int64(float64(rows) * 0.1)
		if rows < 1 {
			rows = 1
		}
		cost = float64(rows) * 3.0
	}
	pb.addNode(0, "Delete", tableName, cost, rows)
	if stmt.Where != nil {
		filterDetail := expressionToString(stmt.Where)
		pb.addNode(1, "Filter", filterDetail, float64(rows)*0.5, rows)
	}
	return &QueryPlan{Nodes: pb.nodes}
}

// max returns the maximum of two int64 values
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
