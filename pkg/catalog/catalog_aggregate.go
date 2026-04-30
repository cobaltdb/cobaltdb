package catalog

import (
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/parallel"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"sort"
	"strconv"
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

func addHiddenOrderByCols(orderBy []*query.OrderByExpr, selectCols []selectColInfo, table *TableDef) ([]selectColInfo, int) {
	if len(orderBy) == 0 || table == nil {
		return selectCols, 0
	}

	added := 0
	for obIdx, ob := range orderBy {
		switch expr := ob.Expr.(type) {
		case *query.Identifier:
			found := false
			for _, ci := range selectCols {
				if strings.EqualFold(ci.name, expr.Name) {
					found = true
					break
				}
			}
			if !found {
				colIdx := table.GetColumnIndex(expr.Name)
				if colIdx >= 0 {
					selectCols = append(selectCols, selectColInfo{
						name:  expr.Name,
						index: colIdx,
					})
					added++
				}
			}
		case *query.NumberLiteral:
			// Positional ORDER BY - always in selectCols
		default:
			// Expression - add as hidden column to be evaluated per-row
			// Use obIdx so applyOrderBy can match by ORDER BY position
			exprName := "__orderby_" + strconv.Itoa(obIdx)
			selectCols = append(selectCols, selectColInfo{
				name:  exprName,
				index: -1, // Will be evaluated per row
			})
			added++
		}
	}
	return selectCols, added
}

// tryCountStarFastPath detects SELECT COUNT(*) FROM table and counts rows
// without decoding row data. Returns (cols, rows, true) if fast path used.
func stripHiddenCols(rows [][]interface{}, totalCols int, hiddenCount int) [][]interface{} {
	if hiddenCount <= 0 {
		return rows
	}
	visibleCount := totalCols - hiddenCount
	for i, row := range rows {
		if len(row) > visibleCount {
			rows[i] = row[:visibleCount]
		}
	}
	return rows
}

func rowKeyForDedup(vals []interface{}) string {
	var b strings.Builder
	b.Grow(len(vals) * 16)
	for i, val := range vals {
		if i > 0 {
			b.WriteByte(0) // null byte separator
		}
		if val == nil {
			b.WriteString("\x01NULL\x01") // tagged NULL marker
		} else {
			b.WriteString("V:")
			switch v := val.(type) {
			case string:
				b.WriteString(v)
			case []byte:
				b.Write(v)
			case int64:
				b.WriteString(strconv.FormatInt(v, 10))
			case int:
				b.WriteString(strconv.Itoa(v))
			case float64:
				b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
			case bool:
				if v {
					b.WriteString("true")
				} else {
					b.WriteString("false")
				}
			default:
				b.WriteString(ValueToStringKey(v))
			}
		}
	}
	return b.String()
}

func addHiddenHavingAggregates(having query.Expression, selectCols []selectColInfo, mainTableRef string) ([]selectColInfo, int) {
	if having == nil {
		return selectCols, 0
	}
	// Collect all aggregate function calls from HAVING
	var aggCalls []*query.FunctionCall
	collectAggregatesFromExpr(having, &aggCalls)

	added := 0
	for _, fc := range aggCalls {
		funcName := toUpperFast(fc.Name)
		colName := "*"
		aggTableName := mainTableRef
		var aggExpr query.Expression
		if len(fc.Args) > 0 {
			switch arg := fc.Args[0].(type) {
			case *query.Identifier:
				colName = arg.Name
			case *query.QualifiedIdentifier:
				colName = arg.Column
				aggTableName = arg.Table
			default:
				aggExpr = fc.Args[0]
			}
		}
		// Check if this aggregate is already in selectCols
		found := false
		for _, sc := range selectCols {
			if sc.isAggregate && strings.EqualFold(sc.aggregateType, funcName) && strings.EqualFold(sc.aggregateCol, colName) {
				found = true
				break
			}
		}
		if !found {
			displayName := funcName + "(" + colName + ")"
			if fc.Distinct {
				displayName = funcName + "(DISTINCT " + colName + ")"
			}
			selectCols = append(selectCols, selectColInfo{
				name:          displayName,
				tableName:     aggTableName,
				index:         -1,
				isAggregate:   true,
				aggregateType: funcName,
				aggregateCol:  colName,
				aggregateExpr: aggExpr,
				isDistinct:    fc.Distinct,
			})
			added++
		}
	}
	return selectCols, added
}

func addHiddenOrderByAggregates(orderBy []*query.OrderByExpr, selectCols []selectColInfo, mainTableRef string) ([]selectColInfo, int) {
	if len(orderBy) == 0 {
		return selectCols, 0
	}
	var aggCalls []*query.FunctionCall
	for _, ob := range orderBy {
		collectAggregatesFromExpr(ob.Expr, &aggCalls)
	}
	added := 0
	for _, fc := range aggCalls {
		funcName := toUpperFast(fc.Name)
		colName := "*"
		aggTableName := mainTableRef
		var aggExpr query.Expression
		if len(fc.Args) > 0 {
			switch arg := fc.Args[0].(type) {
			case *query.Identifier:
				colName = arg.Name
			case *query.QualifiedIdentifier:
				colName = arg.Column
				aggTableName = arg.Table
			default:
				aggExpr = fc.Args[0]
			}
		}
		// Check if this aggregate is already in selectCols
		found := false
		for _, sc := range selectCols {
			if sc.isAggregate && strings.EqualFold(sc.aggregateType, funcName) && strings.EqualFold(sc.aggregateCol, colName) {
				found = true
				break
			}
		}
		if !found {
			displayName := funcName + "(" + colName + ")"
			if fc.Distinct {
				displayName = funcName + "(DISTINCT " + colName + ")"
			}
			selectCols = append(selectCols, selectColInfo{
				name:          displayName,
				tableName:     aggTableName,
				index:         -1,
				isAggregate:   true,
				aggregateType: funcName,
				aggregateCol:  colName,
				aggregateExpr: aggExpr,
				isDistinct:    fc.Distinct,
			})
			added++
		}
	}
	return selectCols, added
}

func collectAggregatesFromExpr(expr query.Expression, result *[]*query.FunctionCall) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *query.FunctionCall:
		if strings.EqualFold(e.Name, "COUNT") || strings.EqualFold(e.Name, "SUM") || strings.EqualFold(e.Name, "AVG") || strings.EqualFold(e.Name, "MIN") || strings.EqualFold(e.Name, "MAX") || strings.EqualFold(e.Name, "GROUP_CONCAT") {
			*result = append(*result, e)
		}
		for _, arg := range e.Args {
			collectAggregatesFromExpr(arg, result)
		}
	case *query.BinaryExpr:
		collectAggregatesFromExpr(e.Left, result)
		collectAggregatesFromExpr(e.Right, result)
	case *query.UnaryExpr:
		collectAggregatesFromExpr(e.Expr, result)
	case *query.AliasExpr:
		collectAggregatesFromExpr(e.Expr, result)
	case *query.BetweenExpr:
		collectAggregatesFromExpr(e.Expr, result)
		collectAggregatesFromExpr(e.Lower, result)
		collectAggregatesFromExpr(e.Upper, result)
	case *query.InExpr:
		collectAggregatesFromExpr(e.Expr, result)
		for _, v := range e.List {
			collectAggregatesFromExpr(v, result)
		}
	case *query.IsNullExpr:
		collectAggregatesFromExpr(e.Expr, result)
	case *query.CaseExpr:
		if e.Expr != nil {
			collectAggregatesFromExpr(e.Expr, result)
		}
		for _, w := range e.Whens {
			collectAggregatesFromExpr(w.Condition, result)
			collectAggregatesFromExpr(w.Result, result)
		}
		if e.Else != nil {
			collectAggregatesFromExpr(e.Else, result)
		}
	case *query.LikeExpr:
		collectAggregatesFromExpr(e.Expr, result)
		collectAggregatesFromExpr(e.Pattern, result)
	}
}

func evaluateHaving(c *Catalog, row []interface{}, selectCols []selectColInfo, columns []ColumnDef, having query.Expression, args []interface{}) (bool, error) {
	if having == nil {
		return true, nil
	}

	// For HAVING, we need to handle aggregate functions specially
	// The aggregate results are already in the row at the indices matching selectCols
	// We need to transform the HAVING expression to use indices from the row

	// First, simplify the HAVING expression by replacing aggregate calls with their values
	havingExpr := resolveAggregateInExpr(having, selectCols, row)

	// Build columns from selectCols if not provided
	evalCols := columns
	if evalCols == nil {
		evalCols = make([]ColumnDef, len(selectCols))
		for i, sc := range selectCols {
			evalCols[i] = ColumnDef{Name: sc.name}
			if sc.tableName != "" {
				evalCols[i].sourceTbl = sc.tableName
			}
		}
	}

	// Now evaluate the simplified expression
	result, err := evaluateExpression(c, row, evalCols, havingExpr, args)
	if err != nil {
		return false, err
	}

	if result == nil {
		return false, nil
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case int, int64, float64:
		switch n := v.(type) {
		case int:
			return n != 0, nil
		case int64:
			return n != 0, nil
		case float64:
			return n != 0, nil
		}
	}
	return true, nil
}

func resolveAggregateInExpr(expr query.Expression, selectCols []selectColInfo, row []interface{}) query.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     resolveAggregateInExpr(e.Left, selectCols, row),
			Operator: e.Operator,
			Right:    resolveAggregateInExpr(e.Right, selectCols, row),
		}
	case *query.FunctionCall:
		// Check if this is an aggregate function
		funcName := toUpperFast(e.Name)
		if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" || funcName == "GROUP_CONCAT" {
			// Find the column name for this aggregate
			colName := "*"
			hasExprArg := false
			if len(e.Args) > 0 {
				if ident, ok := e.Args[0].(*query.Identifier); ok {
					colName = ident.Name
				} else if qi, ok := e.Args[0].(*query.QualifiedIdentifier); ok {
					colName = qi.Column
				} else if _, ok := e.Args[0].(*query.StarExpr); ok {
					colName = "*"
				} else {
					// Expression argument like SUM(quantity * price)
					colName = fmt.Sprintf("%v", e.Args[0])
					hasExprArg = true
				}
			}
			aggSignature := funcName + "(" + colName + ")"
			isDistinct := e.Distinct
			if isDistinct {
				aggSignature = funcName + "(DISTINCT " + colName + ")"
			}

			// Find the index in selectCols - match by aggregate type/col or by name/alias
			for i, sc := range selectCols {
				if !sc.isAggregate || i >= len(row) {
					continue
				}
				// Match by aggregate function signature
				scSignature := sc.aggregateType + "(" + sc.aggregateCol + ")"
				if sc.isDistinct {
					scSignature = sc.aggregateType + "(DISTINCT " + sc.aggregateCol + ")"
				}
				if strings.EqualFold(aggSignature, scSignature) || sc.name == aggSignature {
					return valueToLiteral(row[i])
				}
				// For expression args, match by aggregate type when both have expressions
				if hasExprArg && sc.aggregateExpr != nil && strings.EqualFold(funcName, sc.aggregateType) {
					return valueToLiteral(row[i])
				}
			}
		}
		return e
	case *query.Identifier:
		// Try to find identifier in selectCols (works for both aggregate aliases and regular columns)
		for i, sc := range selectCols {
			if strings.EqualFold(sc.name, e.Name) && i < len(row) {
				return valueToLiteral(row[i])
			}
		}
		return e
	case *query.UnaryExpr:
		return &query.UnaryExpr{
			Operator: e.Operator,
			Expr:     resolveAggregateInExpr(e.Expr, selectCols, row),
		}
	case *query.BetweenExpr:
		return &query.BetweenExpr{
			Expr:  resolveAggregateInExpr(e.Expr, selectCols, row),
			Lower: resolveAggregateInExpr(e.Lower, selectCols, row),
			Upper: resolveAggregateInExpr(e.Upper, selectCols, row),
			Not:   e.Not,
		}
	case *query.InExpr:
		resolved := &query.InExpr{
			Expr:     resolveAggregateInExpr(e.Expr, selectCols, row),
			Not:      e.Not,
			Subquery: e.Subquery,
		}
		if len(e.List) > 0 {
			resolved.List = make([]query.Expression, len(e.List))
			for i, v := range e.List {
				resolved.List[i] = resolveAggregateInExpr(v, selectCols, row)
			}
		}
		return resolved
	case *query.CaseExpr:
		resolved := &query.CaseExpr{}
		if e.Expr != nil {
			resolved.Expr = resolveAggregateInExpr(e.Expr, selectCols, row)
		}
		for _, w := range e.Whens {
			resolved.Whens = append(resolved.Whens, &query.WhenClause{
				Condition: resolveAggregateInExpr(w.Condition, selectCols, row),
				Result:    resolveAggregateInExpr(w.Result, selectCols, row),
			})
		}
		if e.Else != nil {
			resolved.Else = resolveAggregateInExpr(e.Else, selectCols, row)
		}
		return resolved
	case *query.IsNullExpr:
		return &query.IsNullExpr{
			Expr: resolveAggregateInExpr(e.Expr, selectCols, row),
			Not:  e.Not,
		}
	default:
		return e
	}
}

func replaceAggregatesInExpr(expr query.Expression, aggResults map[*query.FunctionCall]interface{}) query.Expression {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *query.FunctionCall:
		if val, ok := aggResults[e]; ok {
			return valueToLiteral(val)
		}
		// Recurse into function arguments (for COALESCE(SUM(x), 0) etc.)
		newArgs := make([]query.Expression, len(e.Args))
		changed := false
		for i, arg := range e.Args {
			newArgs[i] = replaceAggregatesInExpr(arg, aggResults)
			if newArgs[i] != arg {
				changed = true
			}
		}
		if changed {
			return &query.FunctionCall{
				Name:     e.Name,
				Args:     newArgs,
				Distinct: e.Distinct,
			}
		}
		return e
	case *query.CaseExpr:
		newCase := &query.CaseExpr{}
		if e.Expr != nil {
			newCase.Expr = replaceAggregatesInExpr(e.Expr, aggResults)
		}
		for _, w := range e.Whens {
			newCase.Whens = append(newCase.Whens, &query.WhenClause{
				Condition: replaceAggregatesInExpr(w.Condition, aggResults),
				Result:    replaceAggregatesInExpr(w.Result, aggResults),
			})
		}
		if e.Else != nil {
			newCase.Else = replaceAggregatesInExpr(e.Else, aggResults)
		}
		return newCase
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     replaceAggregatesInExpr(e.Left, aggResults),
			Operator: e.Operator,
			Right:    replaceAggregatesInExpr(e.Right, aggResults),
		}
	case *query.UnaryExpr:
		return &query.UnaryExpr{
			Operator: e.Operator,
			Expr:     replaceAggregatesInExpr(e.Expr, aggResults),
		}
	case *query.AliasExpr:
		return &query.AliasExpr{
			Expr:  replaceAggregatesInExpr(e.Expr, aggResults),
			Alias: e.Alias,
		}
	default:
		return e
	}
}
