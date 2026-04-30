package catalog

import (
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/parallel"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"sort"
	"strings"
)

func (c *Catalog) applyDistinct(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}

	seen := make(map[string]bool)
	var result [][]interface{}

	for _, row := range rows {
		key := rowKeyForDedup(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// groupBySpec tracks how a GROUP BY column is resolved.
type groupBySpec struct {
	index int              // >=0 for simple column, -1 for expression
	expr  query.Expression // non-nil for expression GROUP BY
}

func (c *Catalog) computeAggregatesWithGroupBy(table *TableDef, stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	// Check if table exists
	if _, exists := c.tableTrees[stmt.From.Name]; !exists {
		// Return empty result for GROUP BY on non-existent table
		return returnColumns, [][]interface{}{}, nil
	}

	// Parse GROUP BY column indices (for simple column refs) and expressions
	groupBySpecs := make([]groupBySpec, len(stmt.GroupBy))
	for i, gb := range stmt.GroupBy {
		if ident, ok := gb.(*query.Identifier); ok {
			idx := table.GetColumnIndex(ident.Name)
			if idx >= 0 {
				groupBySpecs[i] = groupBySpec{index: idx}
			} else {
				// Check if it's a SELECT alias
				resolved := false
				for j, col := range stmt.Columns {
					if ae, ok := col.(*query.AliasExpr); ok && strings.EqualFold(ae.Alias, ident.Name) {
						// Found alias - use the underlying expression
						if innerIdent, ok := ae.Expr.(*query.Identifier); ok {
							if ci := table.GetColumnIndex(innerIdent.Name); ci >= 0 {
								groupBySpecs[i] = groupBySpec{index: ci}
								resolved = true
								break
							}
						}
						// Expression alias (CASE, function, etc.)
						groupBySpecs[i] = groupBySpec{index: -1, expr: ae.Expr}
						resolved = true
						break
					}
					// Also check selectCols name for non-alias columns
					if j < len(selectCols) && strings.EqualFold(selectCols[j].name, ident.Name) && selectCols[j].index >= 0 {
						groupBySpecs[i] = groupBySpec{index: selectCols[j].index}
						resolved = true
						break
					}
				}
				if !resolved {
					groupBySpecs[i] = groupBySpec{index: -1, expr: gb}
				}
			}
		} else {
			groupBySpecs[i] = groupBySpec{index: -1, expr: gb}
		}
	}

	// Get all trees for scanning (handles partitioned tables)
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		return nil, nil, err
	}

	// Materialize all raw values first
	var allValues [][]byte
	for _, tree := range trees {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan table for GROUP BY: %w", err)
		}
		for iter.HasNext() {
			_, valueData, err := iter.Next()
			if err != nil {
				break
			}
			allValues = append(allValues, valueData)
		}
		iter.Close()
	}

	groups, groupOrder := c.buildGroupByGroups(table, stmt, args, groupBySpecs, allValues)



	// Compute empty-group result (e.g., COUNT(*) = 0 on empty table)
	resultRows := c.computeEmptyGroupResult(groups, stmt, selectCols, table, args)

	// Compute aggregate result for each group
	groupResultRows := c.computeGroupResultRows(groups, groupOrder, stmt, selectCols, table, args)
	resultRows = append(resultRows, groupResultRows...)

	// Apply ORDER BY, DISTINCT, OFFSET, LIMIT
	resultRows = c.applyGroupByPostProcessing(resultRows, stmt, selectCols, args)

	return returnColumns, resultRows, nil
}


// computeEmptyGroupResult returns a single result row for aggregate queries on empty tables.
func (c *Catalog) computeEmptyGroupResult(groups map[string][][]interface{}, stmt *query.SelectStmt, selectCols []selectColInfo, table *TableDef, args []interface{}) [][]interface{} {
	if len(groups) > 0 || len(stmt.GroupBy) > 0 || len(selectCols) == 0 {
		return nil
	}
	resultRow := make([]interface{}, len(selectCols))
	hasAggregate := false
	for i, ci := range selectCols {
		if ci.isAggregate {
			hasAggregate = true
			switch ci.aggregateType {
			case "COUNT":
				resultRow[i] = int64(0)
			case "SUM", "AVG", "MIN", "MAX":
				resultRow[i] = nil
			}
		} else if ci.hasEmbeddedAgg {
			hasAggregate = true
			val, err := c.evaluateExprWithGroupAggregates(ci.originalExpr, nil, table, args)
			if err == nil {
				resultRow[i] = val
			}
		}
	}
	if !hasAggregate {
		return nil
	}
	if stmt.Having != nil {
		havingMatched, err := evaluateHaving(c, resultRow, selectCols, table.Columns, stmt.Having, args)
		if err != nil || !havingMatched {
			return nil
		}
	}
	return [][]interface{}{resultRow}
}

// computeGroupResultRows computes aggregate result rows for each group.
func (c *Catalog) computeGroupResultRows(groups map[string][][]interface{}, groupOrder []string, stmt *query.SelectStmt, selectCols []selectColInfo, table *TableDef, args []interface{}) [][]interface{} {
	var resultRows [][]interface{}
	for _, gk := range groupOrder {
		groupRows := groups[gk]
		resultRow := make([]interface{}, len(selectCols))
		for i, ci := range selectCols {
			if ci.isAggregate {
				var values []interface{}
				for _, row := range groupRows {
					if ci.aggregateCol == "*" && ci.aggregateExpr == nil {
						values = append(values, int64(1))
					} else if ci.aggregateExpr != nil {
						v, err := evaluateExpression(c, row, table.Columns, ci.aggregateExpr, args)
						if err == nil {
							values = append(values, v)
						}
					} else {
						colIdx := table.GetColumnIndex(ci.aggregateCol)
						if colIdx >= 0 && colIdx < len(row) {
							values = append(values, row[colIdx])
						}
					}
				}
				resultRow[i] = computeAggregateValue(ci, values, groupRows)
				if ci.aggregateType == "GROUP_CONCAT" && resultRow[i] != nil {
					if joined, ok := resultRow[i].(string); ok && len(joined) > maxStringResultLen {
						resultRow[i] = joined[:maxStringResultLen]
					}
				}
			} else {
				if ci.hasEmbeddedAgg && len(groupRows) > 0 {
					val, err := c.evaluateExprWithGroupAggregates(ci.originalExpr, groupRows, table, args)
					if err == nil {
						resultRow[i] = val
					}
				} else if ci.index >= 0 && len(groupRows) > 0 && ci.index < len(groupRows[0]) {
					resultRow[i] = groupRows[0][ci.index]
				} else if ci.index == -1 && len(groupRows) > 0 {
					if i < len(stmt.Columns) {
						expr := stmt.Columns[i]
						if ae, ok := expr.(*query.AliasExpr); ok {
							expr = ae.Expr
						}
						val, err := evaluateExpression(c, groupRows[0], table.Columns, expr, args)
						if err == nil {
							resultRow[i] = val
						}
					}
				}
			}
		}
		if stmt.Having != nil {
			havingMatched, err := evaluateHaving(c, resultRow, selectCols, table.Columns, stmt.Having, args)
			if err != nil || !havingMatched {
				continue
			}
		}
		resultRows = append(resultRows, resultRow)
	}
	return resultRows
}

// applyGroupByPostProcessing applies ORDER BY, DISTINCT, OFFSET, LIMIT to grouped results.
func (c *Catalog) applyGroupByPostProcessing(resultRows [][]interface{}, stmt *query.SelectStmt, selectCols []selectColInfo, args []interface{}) [][]interface{} {
	if len(stmt.OrderBy) > 0 {
		resultRows = c.applyGroupByOrderBy(resultRows, selectCols, stmt.OrderBy)
	}
	if stmt.Distinct {
		resultRows = c.applyDistinct(resultRows)
	}
	if stmt.Offset != nil {
		offsetVal, err := evaluateExpression(c, nil, nil, stmt.Offset, args)
		if err == nil {
			if offset, ok := toInt(offsetVal); ok && offset > 0 {
				if offset >= len(resultRows) {
					resultRows = nil
				} else {
					resultRows = resultRows[offset:]
				}
			}
		}
	}
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(c, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(resultRows) {
				resultRows = resultRows[:limit]
			}
		}
	}
	return resultRows
}

	// buildGroupByGroups scans raw values and groups rows by GROUP BY columns.
	func (c *Catalog) buildGroupByGroups(table *TableDef, stmt *query.SelectStmt, args []interface{}, specs []groupBySpec, allValues [][]byte) (map[string][][]interface{}, []string) {
		groups := make(map[string][][]interface{})
		var groupOrder []string

		canParallel := c.parallelWorkers > 0 &&
			len(allValues) >= c.parallelThreshold &&
			!hasSubqueries(stmt)

		if canParallel {
			groups = parallel.ParallelGroupBy(allValues, c.parallelWorkers, c.parallelThreshold,
				func(chunk [][]byte) map[string][][]interface{} {
					localGroups := make(map[string][][]interface{})
					for _, valueData := range chunk {
						vrow, err := decodeVersionedRow(valueData, len(table.Columns))
						if err != nil {
							continue
						}
						if vrow.Version.DeletedAt > 0 {
							continue
						}
						fullRow := vrow.Data
						if stmt.Where != nil {
							matched, err := evaluateWhere(c, fullRow, table.Columns, stmt.Where, args)
							if err != nil {
								continue
							}
							if !matched {
								continue
							}
						}
						var groupKey strings.Builder
						groupKey.Grow(len(specs) * 16)
						for i, spec := range specs {
							if i > 0 {
								groupKey.WriteString("\x00")
							}
							if spec.index >= 0 && spec.index < len(fullRow) {
								groupKey.WriteString(typeTaggedKey(fullRow[spec.index]))
							} else if spec.expr != nil {
								val, err := evaluateExpression(c, fullRow, table.Columns, spec.expr, args)
								if err == nil {
									groupKey.WriteString(typeTaggedKey(val))
								}
							}
						}
						key := groupKey.String()
						localGroups[key] = append(localGroups[key], fullRow)
					}
					return localGroups
				})
			for k := range groups {
				groupOrder = append(groupOrder, k)
			}
		} else {
			for _, valueData := range allValues {
				vrow, err := decodeVersionedRow(valueData, len(table.Columns))
				if err != nil {
					continue
				}
				if vrow.Version.DeletedAt > 0 {
					continue
				}
				fullRow := vrow.Data

				if stmt.Where != nil {
					matched, err := evaluateWhere(c, fullRow, table.Columns, stmt.Where, args)
					if err != nil {
						continue
					}
					if !matched {
						continue
					}
				}

				var groupKey strings.Builder
				for i, spec := range specs {
					if i > 0 {
						groupKey.WriteString("\x00")
					}
					if spec.index >= 0 && spec.index < len(fullRow) {
						groupKey.WriteString(typeTaggedKey(fullRow[spec.index]))
					} else if spec.expr != nil {
						val, err := evaluateExpression(c, fullRow, table.Columns, spec.expr, args)
						if err == nil {
							groupKey.WriteString(typeTaggedKey(val))
						}
					}
				}
				key := groupKey.String()
				if _, exists := groups[key]; !exists {
					groupOrder = append(groupOrder, key)
				}
				groups[key] = append(groups[key], fullRow)
			}
		}

		return groups, groupOrder
	}

func (c *Catalog) evaluateExprWithGroupAggregates(expr query.Expression, groupRows [][]interface{}, table *TableDef, args []interface{}) (interface{}, error) {
	// Collect all aggregate function calls from the expression
	var aggCalls []*query.FunctionCall
	collectAggregatesFromExpr(expr, &aggCalls)

	// Compute each aggregate over the group rows
	aggResults := make(map[*query.FunctionCall]interface{})
	for _, fc := range aggCalls {
		funcName := toUpperFast(fc.Name)
		var values []interface{}
		for _, row := range groupRows {
			if len(fc.Args) == 0 || isStarArg(fc.Args[0]) {
				values = append(values, int64(1))
			} else {
				v, err := evaluateExpression(c, row, table.Columns, fc.Args[0], args)
				if err == nil {
					values = append(values, v)
				}
			}
		}

		switch funcName {
		case "COUNT":
			if len(fc.Args) == 0 || isStarArg(fc.Args[0]) {
				aggResults[fc] = int64(len(groupRows))
			} else {
				count := int64(0)
				for _, v := range values {
					if v != nil {
						count++
					}
				}
				aggResults[fc] = count
			}
		case "SUM":
			var sum float64
			hasVal := false
			for _, v := range values {
				if v != nil {
					if f, ok := toFloat64(v); ok {
						sum += f
						hasVal = true
					}
				}
			}
			if hasVal {
				aggResults[fc] = sum
			} else {
				aggResults[fc] = nil
			}
		case "AVG":
			var sum float64
			var count int64
			for _, v := range values {
				if v != nil {
					if f, ok := toFloat64(v); ok {
						sum += f
						count++
					}
				}
			}
			if count > 0 {
				aggResults[fc] = sum / float64(count)
			} else {
				aggResults[fc] = nil
			}
		case "MIN":
			var minVal interface{}
			for _, v := range values {
				if v != nil {
					if minVal == nil || compareValues(v, minVal) < 0 {
						minVal = v
					}
				}
			}
			aggResults[fc] = minVal
		case "MAX":
			var maxVal interface{}
			for _, v := range values {
				if v != nil {
					if maxVal == nil || compareValues(v, maxVal) > 0 {
						maxVal = v
					}
				}
			}
			aggResults[fc] = maxVal
		}
	}

	// Replace aggregate calls in expression with their computed values, then evaluate
	replaced := replaceAggregatesInExpr(expr, aggResults)
	var baseRow []interface{}
	var baseCols []ColumnDef
	if len(groupRows) > 0 {
		baseRow = groupRows[0]
		baseCols = table.Columns
	}
	return evaluateExpression(c, baseRow, baseCols, replaced, args)
}

func (c *Catalog) evaluateExprWithGroupAggregatesJoin(expr query.Expression, groupRows [][]interface{}, allColumns []ColumnDef, args []interface{}) (interface{}, error) {
	var aggCalls []*query.FunctionCall
	collectAggregatesFromExpr(expr, &aggCalls)

	aggResults := make(map[*query.FunctionCall]interface{})
	for _, fc := range aggCalls {
		funcName := toUpperFast(fc.Name)
		var values []interface{}
		for _, row := range groupRows {
			if len(fc.Args) == 0 || isStarArg(fc.Args[0]) {
				values = append(values, int64(1))
			} else {
				v, err := evaluateExpression(c, row, allColumns, fc.Args[0], args)
				if err == nil {
					values = append(values, v)
				}
			}
		}

		switch funcName {
		case "COUNT":
			if len(fc.Args) == 0 || isStarArg(fc.Args[0]) {
				aggResults[fc] = int64(len(groupRows))
			} else {
				count := int64(0)
				for _, v := range values {
					if v != nil {
						count++
					}
				}
				aggResults[fc] = count
			}
		case "SUM":
			var sum float64
			hasVal := false
			for _, v := range values {
				if v != nil {
					if f, ok := toFloat64(v); ok {
						sum += f
						hasVal = true
					}
				}
			}
			if hasVal {
				aggResults[fc] = sum
			} else {
				aggResults[fc] = nil
			}
		case "AVG":
			var sum float64
			var count int64
			for _, v := range values {
				if v != nil {
					if f, ok := toFloat64(v); ok {
						sum += f
						count++
					}
				}
			}
			if count > 0 {
				aggResults[fc] = sum / float64(count)
			} else {
				aggResults[fc] = nil
			}
		case "MIN":
			var minVal interface{}
			for _, v := range values {
				if v != nil {
					if minVal == nil || compareValues(v, minVal) < 0 {
						minVal = v
					}
				}
			}
			aggResults[fc] = minVal
		case "MAX":
			var maxVal interface{}
			for _, v := range values {
				if v != nil {
					if maxVal == nil || compareValues(v, maxVal) > 0 {
						maxVal = v
					}
				}
			}
			aggResults[fc] = maxVal
		}
	}

	replaced := replaceAggregatesInExpr(expr, aggResults)
	var baseRow []interface{}
	if len(groupRows) > 0 {
		baseRow = groupRows[0]
	}
	return evaluateExpression(c, baseRow, allColumns, replaced, args)
}

func (c *Catalog) applyGroupByOrderBy(rows [][]interface{}, selectCols []selectColInfo, orderBy []*query.OrderByExpr) [][]interface{} {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows
	}

	// Build a map from column name to selectCols index
	nameToIndex := make(map[string]int)
	for i, ci := range selectCols {
		nameToIndex[toUpperFast(ci.name)] = i
		// For aggregates, also add the signature format
		if ci.isAggregate {
			sig := toUpperFast(ci.aggregateType + "(" + ci.aggregateCol + ")")
			nameToIndex[sig] = i
		}
	}

	sorted := make([][]interface{}, len(rows))
	copy(sorted, rows)

	sort.Slice(sorted, func(i, j int) bool {
		for _, ob := range orderBy {
			// Get the column name from the ORDER BY expression
			var colName string
			var exprArgMatch query.Expression // for expression-arg aggregates
			if ident, ok := ob.Expr.(*query.Identifier); ok {
				colName = ident.Name
			} else if fn, ok := ob.Expr.(*query.FunctionCall); ok {
				// Handle aggregate in ORDER BY
				colName = fn.Name + "("
				if len(fn.Args) > 0 {
					switch arg := fn.Args[0].(type) {
					case *query.Identifier:
						colName += arg.Name + ")"
					case *query.QualifiedIdentifier:
						colName += arg.Column + ")"
					case *query.StarExpr:
						colName += "*)"
					default:
						// Expression argument (e.g., SUM(price * quantity))
						// Use "*)" for name matching but also store for direct comparison
						colName += "*)"
						exprArgMatch = fn.Args[0]
					}
				} else {
					colName += "*)"
				}
			} else if qi, ok := ob.Expr.(*query.QualifiedIdentifier); ok {
				colName = qi.Column
			} else if nl, ok := ob.Expr.(*query.NumberLiteral); ok {
				// Positional ORDER BY (ORDER BY 1, 2, etc.)
				pos := int(nl.Value) - 1 // 1-based to 0-based
				if pos >= 0 && pos < len(selectCols) {
					vi := sorted[i][pos]
					vj := sorted[j][pos]
					if vi == nil && vj == nil {
						continue
					}
					if vi == nil {
						return !ob.Desc
					}
					if vj == nil {
						return ob.Desc
					}
					viF, viNum := toFloat64(vi)
					vjF, vjNum := toFloat64(vj)
					if viNum && vjNum {
						if viF < vjF {
							return !ob.Desc
						} else if viF > vjF {
							return ob.Desc
						}
						continue
					}
					viS := ValueToStringKey(vi)
					vjS := ValueToStringKey(vj)
					if viS < vjS {
						return !ob.Desc
					} else if viS > vjS {
						return ob.Desc
					}
				}
				continue
			}

			idx, ok := nameToIndex[toUpperFast(colName)]
			if !ok {
				continue
			}

			// For expression-arg aggregates, verify we found the right one
			// (multiple aggregates could share "SUM(*)" name but have different expressions)
			if exprArgMatch != nil {
				// Try to find exact match by aggregateExpr
				foundExact := false
				for k, ci := range selectCols {
					if ci.isAggregate && ci.aggregateExpr == exprArgMatch {
						idx = k
						foundExact = true
						break
					}
				}
				_ = foundExact // Fallback: use the name-matched index
			}

			// Compare values
			vi := sorted[i][idx]
			vj := sorted[j][idx]

			// Handle nil values
			if vi == nil && vj == nil {
				continue
			}
			if vi == nil {
				return !ob.Desc
			}
			if vj == nil {
				return ob.Desc
			}

			// Compare based on type
			viF, viNum := toFloat64(vi)
			vjF, vjNum := toFloat64(vj)
			if viNum && vjNum {
				if viF < vjF {
					return !ob.Desc
				} else if viF > vjF {
					return ob.Desc
				}
				continue
			}
			switch vi.(type) {
			case string:
				viS := vi.(string)
				vjS := vj.(string)
				if viS < vjS {
					return !ob.Desc
				} else if viS > vjS {
					return ob.Desc
				}
			}
		}
		return false
	})

	return sorted
}
