package catalog

import (
	"fmt"
	"sort"
	"strings"
	"github.com/cobaltdb/cobaltdb/pkg/query"
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

func (c *Catalog) computeAggregatesWithGroupBy(table *TableDef, stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	tree, exists := c.tableTrees[stmt.From.Name]
	if !exists {
		// Return empty result for GROUP BY on non-existent table
		return returnColumns, [][]interface{}{}, nil
	}

	// Parse GROUP BY column indices (for simple column refs) and expressions
	type groupBySpec struct {
		index int              // >=0 for simple column, -1 for expression
		expr  query.Expression // non-nil for expression GROUP BY
	}
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

	// Group rows by GROUP BY columns
	// key is string representation of group values, value is slice of rows
	groups := make(map[string][][]interface{})
	groupOrder := []string{} // preserve insertion order

	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scan table for GROUP BY: %w", err)
	}
	defer iter.Close()

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode full row
		fullRow, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present (filters rows before grouping)
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, fullRow, table.Columns, stmt.Where, args)
			if err != nil {
				continue
			}
			if !matched {
				continue
			}
		}

		// Build group key
		var groupKey strings.Builder
		for i, spec := range groupBySpecs {
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

		// Add row to appropriate group
		key := groupKey.String()
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], fullRow)
	}

	// Compute aggregates for each group
	var resultRows [][]interface{}

	// If no groups (empty table) and no GROUP BY clause, we still need to return
	// a single row with aggregate results (e.g., COUNT(*) = 0)
	if len(groups) == 0 && len(stmt.GroupBy) == 0 && len(selectCols) > 0 {
		resultRow := make([]interface{}, len(selectCols))
		hasAggregate := false
		for i, ci := range selectCols {
			if ci.isAggregate {
				hasAggregate = true
				switch ci.aggregateType {
				case "COUNT":
					resultRow[i] = int64(0)
				case "SUM", "AVG", "MIN", "MAX":
					resultRow[i] = nil // NULL for empty set
				}
			} else if ci.hasEmbeddedAgg {
				hasAggregate = true
				// Evaluate expression with empty group (aggregates return 0/NULL)
				val, err := c.evaluateExprWithGroupAggregates(ci.originalExpr, nil, table, args)
				if err == nil {
					resultRow[i] = val
				}
			}
		}
		if hasAggregate {
			// Apply HAVING clause even for empty-table aggregate
			if stmt.Having != nil {
				havingMatched, err := evaluateHaving(c, resultRow, selectCols, table.Columns, stmt.Having, args)
				if err == nil && havingMatched {
					resultRows = append(resultRows, resultRow)
				}
			} else {
				resultRows = append(resultRows, resultRow)
			}
		}
	}

	for _, gk := range groupOrder {
		groupRows := groups[gk]
		resultRow := make([]interface{}, len(selectCols))

		for i, ci := range selectCols {
			if ci.isAggregate {
				// Collect values for this aggregate
				var values []interface{}
				for _, row := range groupRows {
					if ci.aggregateCol == "*" && ci.aggregateExpr == nil {
						// COUNT(*): just count rows
						values = append(values, int64(1))
					} else if ci.aggregateExpr != nil {
						// Expression argument (e.g., SUM(quantity * price))
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

				// Compute aggregate
				switch ci.aggregateType {
				case "COUNT":
					if ci.aggregateCol == "*" && !ci.isDistinct {
						resultRow[i] = int64(len(groupRows))
					} else if ci.isDistinct {
						// Count distinct non-null values
						seen := make(map[string]bool)
						for _, v := range values {
							if v != nil {
								seen[fmt.Sprintf("%v", v)] = true
							}
						}
						resultRow[i] = int64(len(seen))
					} else {
						count := int64(0)
						for _, v := range values {
							if v != nil {
								count++
							}
						}
						resultRow[i] = count
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
						resultRow[i] = sum
					} else {
						resultRow[i] = nil
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
						resultRow[i] = sum / float64(count)
					} else {
						resultRow[i] = nil
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
					resultRow[i] = minVal
				case "MAX":
					var maxVal interface{}
					for _, v := range values {
						if v != nil {
							if maxVal == nil || compareValues(v, maxVal) > 0 {
								maxVal = v
							}
						}
					}
					resultRow[i] = maxVal
				case "GROUP_CONCAT":
					var parts []string
					for _, v := range values {
						if v != nil {
							parts = append(parts, fmt.Sprintf("%v", v))
						}
					}
					if len(parts) > 0 {
						resultRow[i] = strings.Join(parts, ",")
					} else {
						resultRow[i] = nil
					}
				}
			} else {
				// Non-aggregate column - get value from first row in group
				if ci.hasEmbeddedAgg && len(groupRows) > 0 {
					// Expression (CASE, etc.) with embedded aggregates
					// Pre-compute aggregates over the group, then evaluate expression
					val, err := c.evaluateExprWithGroupAggregates(ci.originalExpr, groupRows, table, args)
					if err == nil {
						resultRow[i] = val
					}
				} else if ci.index >= 0 && len(groupRows) > 0 && ci.index < len(groupRows[0]) {
					resultRow[i] = groupRows[0][ci.index]
				} else if ci.index == -1 && len(groupRows) > 0 {
					// Expression column (CASE, CAST, etc.) - evaluate it
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

		// Apply HAVING clause if present
		if stmt.Having != nil {
			// Create a temporary row with column values for evaluation
			// We need to create a virtual row that has the right column structure
			havingMatched, err := evaluateHaving(c, resultRow, selectCols, table.Columns, stmt.Having, args)
			if err != nil || !havingMatched {
				continue
			}
		}

		resultRows = append(resultRows, resultRow)
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		resultRows = c.applyGroupByOrderBy(resultRows, selectCols, stmt.OrderBy)
	}

	// Apply DISTINCT if present
	if stmt.Distinct {
		resultRows = c.applyDistinct(resultRows)
	}

	// Apply OFFSET if present
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

	// Apply LIMIT if present
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(c, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(resultRows) {
				resultRows = resultRows[:limit]
			}
		}
	}

	return returnColumns, resultRows, nil
}

func (c *Catalog) evaluateExprWithGroupAggregates(expr query.Expression, groupRows [][]interface{}, table *TableDef, args []interface{}) (interface{}, error) {
	// Collect all aggregate function calls from the expression
	var aggCalls []*query.FunctionCall
	collectAggregatesFromExpr(expr, &aggCalls)

	// Compute each aggregate over the group rows
	aggResults := make(map[*query.FunctionCall]interface{})
	for _, fc := range aggCalls {
		funcName := strings.ToUpper(fc.Name)
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
		funcName := strings.ToUpper(fc.Name)
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
		nameToIndex[strings.ToUpper(ci.name)] = i
		// For aggregates, also add the signature format
		if ci.isAggregate {
			sig := strings.ToUpper(ci.aggregateType + "(" + ci.aggregateCol + ")")
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
					viS := fmt.Sprintf("%v", vi)
					vjS := fmt.Sprintf("%v", vj)
					if viS < vjS {
						return !ob.Desc
					} else if viS > vjS {
						return ob.Desc
					}
				}
				continue
			}

			idx, ok := nameToIndex[strings.ToUpper(colName)]
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
				if !foundExact {
					// Fallback: use the name-matched index
				}
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