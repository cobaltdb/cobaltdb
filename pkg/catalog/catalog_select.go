package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func (cat *Catalog) Select(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	cat.mu.RLock()
	defer cat.mu.RUnlock()

	// Check if this query can be cached
	if cat.queryCache != nil && cat.queryCache.enabled && isCacheableQuery(stmt) {
		// Generate cache key from query and args
		cacheKey := generateQueryKey(queryToSQL(stmt), args)

		// Try to get from cache
		if entry, found := cat.queryCache.Get(cacheKey); found {
			return entry.Columns, entry.Rows, nil
		}

		// Execute query
		columns, rows, err := cat.selectLocked(stmt, args)
		if err != nil {
			return nil, nil, err
		}

		// Store in cache
		tables := extractTablesFromQuery(stmt)
		cat.queryCache.Set(cacheKey, columns, rows, tables)

		return columns, rows, nil
	}

	return cat.selectLocked(stmt, args)
}

// SetRLSContext sets the context used for RLS user/role extraction in SELECT queries.
func (cat *Catalog) SetRLSContext(ctx context.Context) {
	cat.rlsCtx = ctx
}

func (c *Catalog) executeScalarSelect(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	// SELECT without FROM - evaluate each expression
	var returnColumns []string
	var rows [][]interface{}

	// Handle each column in the SELECT clause
	if len(stmt.Columns) == 0 {
		return nil, nil, errors.New("no columns specified")
	}

	// Check if this is a simple expression or contains aggregates/window functions
	hasAggregate := false
	hasWindowFunc := false

	for _, col := range stmt.Columns {
		actual := col
		if ae, ok := col.(*query.AliasExpr); ok {
			actual = ae.Expr
		}
		if fc, ok := actual.(*query.FunctionCall); ok {
			funcName := strings.ToUpper(fc.Name)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" || funcName == "GROUP_CONCAT" {
				hasAggregate = true
			}
		}
		if _, ok := actual.(*query.WindowExpr); ok {
			hasWindowFunc = true
		}
	}

	if hasAggregate {
		// Aggregate without FROM - compute single aggregate result
		return c.executeScalarAggregate(stmt, args)
	}

	if hasWindowFunc {
		// Window function without FROM - need to handle specially
		return nil, nil, errors.New("window functions require FROM clause")
	}

	// Check WHERE clause (e.g., SELECT 1 WHERE 1 = 1)
	if stmt.Where != nil {
		matched, err := evaluateWhere(c, nil, nil, stmt.Where, args)
		if err != nil {
			return nil, nil, err
		}
		if !matched {
			// WHERE is false — return empty result with column names
			for i, col := range stmt.Columns {
				colName := fmt.Sprintf("column_%d", i)
				if ae, ok := col.(*query.AliasExpr); ok {
					colName = ae.Alias
				} else if ident, ok := col.(*query.Identifier); ok {
					colName = ident.Name
				} else if fc, ok := col.(*query.FunctionCall); ok {
					colName = fc.Name
				}
				returnColumns = append(returnColumns, colName)
			}
			return returnColumns, nil, nil
		}
	}

	// Simple scalar expression - evaluate each column expression once
	row := make([]interface{}, len(stmt.Columns))
	for i, col := range stmt.Columns {
		val, err := evaluateExpression(c, nil, nil, col, args)
		if err != nil {
			return nil, nil, err
		}

		// Build column name
		colName := fmt.Sprintf("column_%d", i)
		if ae, ok := col.(*query.AliasExpr); ok {
			colName = ae.Alias
		} else if ident, ok := col.(*query.Identifier); ok {
			colName = ident.Name
		} else if fc, ok := col.(*query.FunctionCall); ok {
			colName = fc.Name
		}
		returnColumns = append(returnColumns, colName)
		row[i] = val
	}

	rows = append(rows, row)

	// Handle DISTINCT
	if stmt.Distinct {
		rows = c.applyDistinct(rows)
	}

	// Handle ORDER BY (for scalar expressions, just sort the single row)
	if len(stmt.OrderBy) > 0 {
		// No ordering needed for single row
	}

	// Handle LIMIT
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(c, nil, nil, stmt.Limit, args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(rows) {
				rows = rows[:limit]
			}
		}
	}

	return returnColumns, rows, nil
}

func (c *Catalog) executeScalarAggregate(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, error) {
	var returnColumns []string

	// Evaluate each column as an aggregate
	row := make([]interface{}, len(stmt.Columns))
	for i, col := range stmt.Columns {
		fc, ok := col.(*query.FunctionCall)
		if !ok {
			return nil, nil, errors.New("aggregate functions required in this context")
		}

		funcName := strings.ToUpper(fc.Name)
		var colName string
		var result interface{}

		switch funcName {
		case "COUNT":
			colName = "COUNT(*)"
			// COUNT(*) without FROM is always 1
			result = float64(1)
		case "SUM":
			colName = "SUM"
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(c, nil, nil, fc.Args[0], args)
				if err == nil {
					if f, ok := toFloat64(val); ok {
						result = f
					}
				}
			}
		case "AVG":
			colName = "AVG"
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(c, nil, nil, fc.Args[0], args)
				if err == nil {
					if f, ok := toFloat64(val); ok {
						result = f
					}
				}
			}
		case "MIN":
			colName = "MIN"
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(c, nil, nil, fc.Args[0], args)
				if err == nil {
					result = val
				}
			}
		case "MAX":
			colName = "MAX"
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(c, nil, nil, fc.Args[0], args)
				if err == nil {
					result = val
				}
			}
		default:
			colName = fc.Name
			result = nil
		}

		returnColumns = append(returnColumns, colName)
		row[i] = result
	}

	return returnColumns, [][]interface{}{row}, nil
}

func (c *Catalog) executeSelectWithJoin(stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo) ([]string, [][]interface{}, error) {
	var mainTableCols []ColumnDef
	var intermediateRows [][]interface{}

	// Check if main table is a CTE result
	if c.cteResults != nil {
		if cteRes, ok := c.cteResults[strings.ToLower(stmt.From.Name)]; ok {
			mainTableCols = make([]ColumnDef, len(cteRes.columns))
			for i, col := range cteRes.columns {
				mainTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
			}
			intermediateRows = make([][]interface{}, len(cteRes.rows))
			copy(intermediateRows, cteRes.rows)
		}
	}

	if mainTableCols == nil {
		// Get the main table
		mainTable, err := c.getTableLocked(stmt.From.Name)
		if err != nil {
			return nil, nil, err
		}

		mainTree, exists := c.tableTrees[stmt.From.Name]
		if !exists {
			return nil, [][]interface{}{}, nil
		}

		mainTableCols = mainTable.Columns

		// Build initial intermediate rows from main table (full column data)
		mainIter, _ := mainTree.Scan(nil, nil)
		for mainIter.HasNext() {
			_, data, err := mainIter.Next()
			if err != nil {
				break
			}
			row, err := decodeRow(data, len(mainTable.Columns))
			if err != nil {
				continue
			}
			intermediateRows = append(intermediateRows, row)
		}
		mainIter.Close()
	}

	// Track combined columns and table offsets for projection
	combinedColumns := make([]ColumnDef, len(mainTableCols))
	copy(combinedColumns, mainTableCols)
	// Set source table name for column disambiguation in JOINs
	mainAlias := stmt.From.Name
	if stmt.From.Alias != "" {
		mainAlias = stmt.From.Alias
	}
	for i := range combinedColumns {
		combinedColumns[i].sourceTbl = mainAlias
	}

	type tableOffset struct {
		name   string
		offset int
		count  int
	}
	tableOffsets := []tableOffset{{
		name:   mainAlias,
		offset: 0,
		count:  len(mainTableCols),
	}}

	// Chain through each JOIN
	for _, join := range stmt.Joins {
		var joinTableCols []ColumnDef
		var joinRows [][]interface{}

		// Check if join table is a derived table (subquery or UNION)
		if join.Table.Subquery != nil || join.Table.SubqueryStmt != nil {
			subCols, subRows, err := c.executeDerivedTable(join.Table, args)
			if err == nil {
				joinTableCols = make([]ColumnDef, len(subCols))
				for i, col := range subCols {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				joinRows = subRows
			}
		}

		// Check if join table is a CTE result
		if joinTableCols == nil && c.cteResults != nil {
			if cteRes, ok := c.cteResults[strings.ToLower(join.Table.Name)]; ok {
				// Use CTE result rows
				joinTableCols = make([]ColumnDef, len(cteRes.columns))
				for i, col := range cteRes.columns {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				joinRows = cteRes.rows
			}
		}

		if joinTableCols == nil {
			// Check if it's a view (CTE registered as view)
			if viewDef, viewErr := c.GetView(join.Table.Name); viewErr == nil {
				viewCols, viewRows, viewExecErr := c.selectLocked(viewDef, args)
				if viewExecErr == nil {
					joinTableCols = make([]ColumnDef, len(viewCols))
					for i, col := range viewCols {
						joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
					}
					joinRows = viewRows
				}
			}
		}

		if joinTableCols == nil {
			// Normal table lookup
			joinTable, err := c.getTableLocked(join.Table.Name)
			if err != nil {
				continue
			}

			joinTree, exists := c.tableTrees[join.Table.Name]
			if !exists {
				continue
			}

			joinTableCols = joinTable.Columns
			// Scan join table rows
			joinIter, _ := joinTree.Scan(nil, nil)
			for joinIter.HasNext() {
				_, data, err := joinIter.Next()
				if err != nil {
					break
				}
				row, err := decodeRow(data, len(joinTable.Columns))
				if err != nil {
					continue
				}
				joinRows = append(joinRows, row)
			}
			joinIter.Close()
		}

		isLeftJoin := join.Type == query.TokenLeft || join.Type == query.TokenFull
		isRightJoin := join.Type == query.TokenRight || join.Type == query.TokenFull
		isCrossJoin := join.Type == query.TokenCross

		// Build combined columns for evaluating ON condition
		newCombinedColumns := make([]ColumnDef, len(combinedColumns)+len(joinTableCols))
		copy(newCombinedColumns, combinedColumns)
		copy(newCombinedColumns[len(combinedColumns):], joinTableCols)
		// Set source table name for join table columns
		joinAlias := join.Table.Name
		if join.Table.Alias != "" {
			joinAlias = join.Table.Alias
		}
		for i := len(combinedColumns); i < len(newCombinedColumns); i++ {
			newCombinedColumns[i].sourceTbl = joinAlias
		}

		var newIntermediate [][]interface{}

		// joinRows is already populated above (from CTE result or B-tree scan)
		rightRows := joinRows

		if isCrossJoin {
			// CROSS JOIN: Cartesian product
			for _, leftRow := range intermediateRows {
				for _, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)

					if join.Condition != nil {
						ok, err := evaluateWhere(c, combined, newCombinedColumns, join.Condition, args)
						if err != nil || !ok {
							continue
						}
					}
					newIntermediate = append(newIntermediate, combined)
				}
			}
		} else {
			// Track which right rows were matched (for RIGHT/FULL OUTER JOIN)
			rightMatched := make([]bool, len(rightRows))

			// Iterate through left side rows
			for _, leftRow := range intermediateRows {
				matched := false

				for ri, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)

					if join.Condition != nil {
						ok, err := evaluateWhere(c, combined, newCombinedColumns, join.Condition, args)
						if err != nil || !ok {
							continue
						}
					}
					matched = true
					rightMatched[ri] = true
					newIntermediate = append(newIntermediate, combined)
				}

				if isLeftJoin && !matched {
					// NULLs for join table columns
					combined := make([]interface{}, len(leftRow)+len(joinTableCols))
					copy(combined, leftRow)
					newIntermediate = append(newIntermediate, combined)
				}
			}

			// For RIGHT/FULL OUTER JOIN: add unmatched right rows with NULL left side
			if isRightJoin {
				for ri, joinRow := range rightRows {
					if !rightMatched[ri] {
						combined := make([]interface{}, len(combinedColumns)+len(joinTableCols))
						copy(combined[len(combinedColumns):], joinRow)
						newIntermediate = append(newIntermediate, combined)
					}
				}
			}
		}

		intermediateRows = newIntermediate
		combinedColumns = newCombinedColumns
		tableOffsets = append(tableOffsets, tableOffset{
			name:   joinAlias,
			offset: tableOffsets[len(tableOffsets)-1].offset + tableOffsets[len(tableOffsets)-1].count,
			count:  len(joinTableCols),
		})
	}

	// Apply WHERE clause to joined rows
	if stmt.Where != nil {
		var filteredRows [][]interface{}
		for _, row := range intermediateRows {
			matched, err := evaluateWhere(c, row, combinedColumns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
			filteredRows = append(filteredRows, row)
		}
		intermediateRows = filteredRows
	}

	// Add hidden ORDER BY columns from joined tables not already in selectCols
	hiddenOrderByCols := 0
	if len(stmt.OrderBy) > 0 {
		for _, ob := range stmt.OrderBy {
			var colName, tblName string
			switch expr := ob.Expr.(type) {
			case *query.QualifiedIdentifier:
				colName = expr.Column
				tblName = expr.Table
			case *query.Identifier:
				if dotIdx := strings.IndexByte(expr.Name, '.'); dotIdx > 0 && dotIdx < len(expr.Name)-1 {
					tblName = expr.Name[:dotIdx]
					colName = expr.Name[dotIdx+1:]
				} else {
					colName = expr.Name
				}
			default:
				continue
			}
			// Check if already in selectCols
			found := false
			colLower := strings.ToLower(colName)
			tblLower := strings.ToLower(tblName)
			for _, ci := range selectCols {
				if strings.ToLower(ci.name) == colLower {
					if tblName == "" || strings.ToLower(ci.tableName) == tblLower {
						found = true
						break
					}
				}
			}
			if !found {
				// Find the column in the appropriate table
				if tblName != "" {
					for _, to := range tableOffsets {
						if strings.ToLower(to.name) == tblLower {
							// Find column index in that table
							for _, col := range combinedColumns {
								if strings.ToLower(col.Name) == colLower && strings.ToLower(col.sourceTbl) == tblLower {
									// Get raw index within the table
									rawIdx := -1
									// Look up the table to get column index
									tblDef, tErr := c.getTableLocked(to.name)
									if tErr == nil {
										rawIdx = tblDef.GetColumnIndex(colName)
									}
									if rawIdx < 0 {
										// Try finding by iterating through the table's known columns
										for ci, cc := range combinedColumns[to.offset : to.offset+to.count] {
											if strings.ToLower(cc.Name) == colLower {
												rawIdx = ci
												break
											}
										}
									}
									if rawIdx >= 0 {
										selectCols = append(selectCols, selectColInfo{name: colName, tableName: to.name, index: rawIdx})
										hiddenOrderByCols++
									}
									break
								}
							}
							break
						}
					}
				} else {
					// No table qualifier - search main table columns
					for ci, cc := range mainTableCols {
						if strings.EqualFold(cc.Name, colName) {
							selectCols = append(selectCols, selectColInfo{name: colName, tableName: mainAlias, index: ci})
							hiddenOrderByCols++
							break
						}
					}
				}
			}
		}
	}

	// Project selectCols from intermediate rows
	var resultRows [][]interface{}
	for _, row := range intermediateRows {
		projected := make([]interface{}, 0, len(selectCols))
		for i, ci := range selectCols {
			if ci.index == -1 && !ci.isAggregate {
				// Scalar function - evaluate it against the combined row
				if i < len(stmt.Columns) {
					if expr, ok := stmt.Columns[i].(query.Expression); ok {
						val, err := evaluateExpression(c, row, combinedColumns, expr, args)
						if err == nil {
							projected = append(projected, val)
							continue
						}
					}
				}
				projected = append(projected, nil)
				continue
			}
			if ci.index < 0 {
				// Aggregate - append nil placeholder
				projected = append(projected, nil)
				continue
			}
			found := false
			for _, to := range tableOffsets {
				if ci.tableName == to.name || (ci.tableName == "" && to.offset == 0) {
					colIdx := to.offset + ci.index
					if colIdx >= 0 && colIdx < len(row) {
						projected = append(projected, row[colIdx])
					} else {
						projected = append(projected, nil)
					}
					found = true
					break
				}
			}
			if !found {
				projected = append(projected, nil)
			}
		}
		resultRows = append(resultRows, projected)
	}

	// Evaluate window functions on projected rows
	hasWindowFuncs := false
	for _, ci := range selectCols {
		if ci.isWindow {
			hasWindowFuncs = true
			break
		}
	}
	if hasWindowFuncs {
		resultRows = c.evaluateWindowFunctions(resultRows, selectCols, nil, stmt, args, nil)
	}

	// Build return columns
	visibleCols := len(selectCols) - hiddenOrderByCols
	returnColumns := make([]string, visibleCols)
	for i := 0; i < visibleCols; i++ {
		returnColumns[i] = selectCols[i].name
	}

	// Apply ORDER BY to projected rows
	if len(stmt.OrderBy) > 0 {
		resultRows = c.applyOrderBy(resultRows, selectCols, stmt.OrderBy)
	}

	// Strip hidden ORDER BY columns
	if hiddenOrderByCols > 0 {
		resultRows = stripHiddenCols(resultRows, len(selectCols), hiddenOrderByCols)
		selectCols = selectCols[:visibleCols]
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = c.applyDistinct(resultRows)
	}

	// Apply OFFSET
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

	// Apply LIMIT
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

func (c *Catalog) executeSelectWithJoinAndGroupBy(stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	var mainTableCols []ColumnDef
	var intermediateRows [][]interface{}

	// Check if main table is a CTE result or derived table
	if stmt.From.Subquery != nil || stmt.From.SubqueryStmt != nil {
		// Derived table as main table
		subCols, subRows, err := c.executeDerivedTable(stmt.From, args)
		if err != nil {
			return nil, nil, err
		}
		mainTableCols = make([]ColumnDef, len(subCols))
		for i, col := range subCols {
			mainTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
		}
		intermediateRows = make([][]interface{}, len(subRows))
		copy(intermediateRows, subRows)
	} else if c.cteResults != nil {
		if cteRes, ok := c.cteResults[strings.ToLower(stmt.From.Name)]; ok {
			mainTableCols = make([]ColumnDef, len(cteRes.columns))
			for i, col := range cteRes.columns {
				mainTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
			}
			intermediateRows = make([][]interface{}, len(cteRes.rows))
			copy(intermediateRows, cteRes.rows)
		}
	}

	if mainTableCols == nil {
		// Get main table for column info
		mainTable, err := c.getTableLocked(stmt.From.Name)
		if err != nil {
			return nil, nil, err
		}

		mainTree, exists := c.tableTrees[stmt.From.Name]
		if !exists {
			return returnColumns, [][]interface{}{}, nil
		}

		mainTableCols = mainTable.Columns

		// Build initial intermediate rows from main table (full column data)
		mainIter, _ := mainTree.Scan(nil, nil)
		for mainIter.HasNext() {
			_, data, err := mainIter.Next()
			if err != nil {
				break
			}
			row, err := decodeRow(data, len(mainTable.Columns))
			if err != nil {
				continue
			}
			intermediateRows = append(intermediateRows, row)
		}
		mainIter.Close()
	}

	// Track combined columns from all tables
	allColumns := make([]ColumnDef, len(mainTableCols))
	copy(allColumns, mainTableCols)
	// Set source table name for column disambiguation in JOINs
	mainAlias := stmt.From.Name
	if stmt.From.Alias != "" {
		mainAlias = stmt.From.Alias
	}
	for i := range allColumns {
		allColumns[i].sourceTbl = mainAlias
	}

	// Chain through each JOIN to build full combined rows
	for _, join := range stmt.Joins {
		var joinTableCols []ColumnDef
		var rightRows [][]interface{}

		// Check if join table is a derived table
		if join.Table.Subquery != nil || join.Table.SubqueryStmt != nil {
			subCols, subRows, err := c.executeDerivedTable(join.Table, args)
			if err == nil {
				joinTableCols = make([]ColumnDef, len(subCols))
				for i, col := range subCols {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				rightRows = subRows
			}
		}

		// Check if join table is a CTE result
		if joinTableCols == nil && c.cteResults != nil {
			if cteRes, ok := c.cteResults[strings.ToLower(join.Table.Name)]; ok {
				joinTableCols = make([]ColumnDef, len(cteRes.columns))
				for i, col := range cteRes.columns {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				rightRows = cteRes.rows
			}
		}

		// Check if join table is a view (CTE registered as view)
		if joinTableCols == nil {
			if viewDef, viewErr := c.GetView(join.Table.Name); viewErr == nil {
				viewCols, viewRows, viewExecErr := c.selectLocked(viewDef, args)
				if viewExecErr == nil {
					joinTableCols = make([]ColumnDef, len(viewCols))
					for i, col := range viewCols {
						joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
					}
					rightRows = viewRows
				}
			}
		}

		if joinTableCols == nil {
			// Normal table lookup
			joinTable, err := c.getTableLocked(join.Table.Name)
			if err != nil {
				continue
			}
			joinTree, exists := c.tableTrees[join.Table.Name]
			if !exists {
				continue
			}
			joinTableCols = joinTable.Columns

			// Collect right side rows
			joinIter, _ := joinTree.Scan(nil, nil)
			for joinIter.HasNext() {
				_, data, err := joinIter.Next()
				if err != nil {
					break
				}
				joinRow, err := decodeRow(data, len(joinTable.Columns))
				if err != nil {
					continue
				}
				rightRows = append(rightRows, joinRow)
			}
			joinIter.Close()
		}

		isLeftJoin := join.Type == query.TokenLeft || join.Type == query.TokenFull
		isRightJoin := join.Type == query.TokenRight || join.Type == query.TokenFull
		isCrossJoin := join.Type == query.TokenCross

		newAllColumns := make([]ColumnDef, len(allColumns)+len(joinTableCols))
		copy(newAllColumns, allColumns)
		copy(newAllColumns[len(allColumns):], joinTableCols)
		// Set source table name for join table columns
		joinAlias := join.Table.Name
		if join.Table.Alias != "" {
			joinAlias = join.Table.Alias
		}
		for i := len(allColumns); i < len(newAllColumns); i++ {
			newAllColumns[i].sourceTbl = joinAlias
		}

		var newIntermediate [][]interface{}

		if isCrossJoin {
			// CROSS JOIN: Cartesian product - include ALL combinations
			for _, leftRow := range intermediateRows {
				for _, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)
					newIntermediate = append(newIntermediate, combined)
				}
			}
		} else {
			rightMatched := make([]bool, len(rightRows))

			for _, leftRow := range intermediateRows {
				matched := false

				for ri, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)

					if join.Condition != nil {
						ok, err := evaluateWhere(c, combined, newAllColumns, join.Condition, args)
						if err != nil || !ok {
							continue
						}
					}
					matched = true
					rightMatched[ri] = true
					newIntermediate = append(newIntermediate, combined)
				}

				if isLeftJoin && !matched {
					combined := make([]interface{}, len(leftRow)+len(joinTableCols))
					copy(combined, leftRow)
					newIntermediate = append(newIntermediate, combined)
				}
			}

			// For RIGHT/FULL OUTER JOIN: add unmatched right rows
			if isRightJoin {
				for ri, joinRow := range rightRows {
					if !rightMatched[ri] {
						combined := make([]interface{}, len(allColumns)+len(joinTableCols))
						copy(combined[len(allColumns):], joinRow)
						newIntermediate = append(newIntermediate, combined)
					}
				}
			}
		}

		intermediateRows = newIntermediate
		allColumns = newAllColumns
	}

	// Apply WHERE clause to joined rows before GROUP BY
	if stmt.Where != nil {
		var filteredRows [][]interface{}
		for _, row := range intermediateRows {
			matched, err := evaluateWhere(c, row, allColumns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
			filteredRows = append(filteredRows, row)
		}
		intermediateRows = filteredRows
	}

	// joinedRows now contains properly filtered and chained results
	joinedRows := intermediateRows

	// Parse GROUP BY column indices (relative to combined row) and expressions
	type joinGroupBySpec struct {
		index int              // >=0 for simple column, -1 for expression
		expr  query.Expression // non-nil for expression GROUP BY
	}
	joinGroupBySpecs := make([]joinGroupBySpec, len(stmt.GroupBy))
	for i, gb := range stmt.GroupBy {
		switch g := gb.(type) {
		case *query.Identifier:
			found := false
			// Check for dotted identifier like "table.column"
			if dotIdx := strings.IndexByte(g.Name, '.'); dotIdx > 0 && dotIdx < len(g.Name)-1 {
				tblName := g.Name[:dotIdx]
				colName := g.Name[dotIdx+1:]
				for j, col := range allColumns {
					if strings.EqualFold(col.Name, colName) && strings.EqualFold(col.sourceTbl, tblName) {
						joinGroupBySpecs[i] = joinGroupBySpec{index: j}
						found = true
						break
					}
				}
			}
			// Find column index in combined columns (bare name)
			if !found {
				for j, col := range allColumns {
					if col.Name == g.Name {
						joinGroupBySpecs[i] = joinGroupBySpec{index: j}
						found = true
						break
					}
				}
			}
			if !found {
				// Check if it's a SELECT alias
				for _, col := range stmt.Columns {
					if ae, ok := col.(*query.AliasExpr); ok && strings.EqualFold(ae.Alias, g.Name) {
						// Found alias - resolve to underlying column
						if innerIdent, ok := ae.Expr.(*query.Identifier); ok {
							for j, col := range allColumns {
								if col.Name == innerIdent.Name {
									joinGroupBySpecs[i] = joinGroupBySpec{index: j}
									found = true
									break
								}
							}
						} else if qi, ok := ae.Expr.(*query.QualifiedIdentifier); ok {
							for j, col := range allColumns {
								if col.Name == qi.Column {
									joinGroupBySpecs[i] = joinGroupBySpec{index: j}
									found = true
									break
								}
							}
						}
						if !found {
							joinGroupBySpecs[i] = joinGroupBySpec{index: -1, expr: ae.Expr}
							found = true
						}
						break
					}
				}
			}
			if !found {
				joinGroupBySpecs[i] = joinGroupBySpec{index: -1, expr: gb}
			}
		case *query.QualifiedIdentifier:
			targetTable := g.Table
			colName := g.Column
			found := false

			if targetTable == stmt.From.Name || targetTable == stmt.From.Alias {
				for j, col := range mainTableCols {
					if col.Name == colName {
						joinGroupBySpecs[i] = joinGroupBySpec{index: j}
						found = true
						break
					}
				}
			} else {
				offset := len(mainTableCols)
				for _, join := range stmt.Joins {
					joinTableName := join.Table.Name
					if join.Table.Alias != "" {
						joinTableName = join.Table.Alias
					}
					if joinTableName == targetTable {
						jt, err := c.getTableLocked(join.Table.Name)
						if err == nil {
							for j, col := range jt.Columns {
								if col.Name == colName {
									joinGroupBySpecs[i] = joinGroupBySpec{index: offset + j}
									found = true
									break
								}
							}
						}
						break
					}
					jt, _ := c.getTableLocked(join.Table.Name)
					if jt != nil {
						offset += len(jt.Columns)
					}
				}
			}
			if !found {
				joinGroupBySpecs[i] = joinGroupBySpec{index: -1, expr: gb}
			}
		default:
			// Expression GROUP BY (CASE, CAST, etc.)
			joinGroupBySpecs[i] = joinGroupBySpec{index: -1, expr: gb}
		}
	}

	// Group the joined rows
	groups := make(map[string][][]interface{})
	groupOrder := []string{}
	for _, row := range joinedRows {
		var groupKey strings.Builder
		for i, spec := range joinGroupBySpecs {
			if i > 0 {
				groupKey.WriteString("|")
			}
			if spec.index >= 0 && spec.index < len(row) {
				groupKey.WriteString(fmt.Sprintf("%v", row[spec.index]))
			} else if spec.expr != nil {
				val, err := evaluateExpression(c, row, allColumns, spec.expr, args)
				if err == nil {
					groupKey.WriteString(fmt.Sprintf("%v", val))
				}
			}
		}
		key := groupKey.String()
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], row)
	}

	// Compute aggregates for each group
	var resultRows [][]interface{}

	for _, gk := range groupOrder {
		groupRows := groups[gk]
		if len(groupRows) == 0 {
			continue
		}
		resultRow := make([]interface{}, len(selectCols))

		for i, ci := range selectCols {
			if ci.isAggregate {
				// Collect values for this aggregate
				var values []interface{}
				for _, row := range groupRows {
					if ci.aggregateCol == "*" && ci.aggregateExpr == nil {
						values = append(values, int64(1))
					} else if ci.aggregateExpr != nil {
						// Expression argument (e.g., SUM(quantity * price))
						v, err := evaluateExpression(c, row, allColumns, ci.aggregateExpr, args)
						if err == nil {
							values = append(values, v)
						}
					} else {
						// Find column index for aggregate column
						// Need to consider which table the column belongs to
						colIdx := -1

						// First, determine which table the aggregate column belongs to
						targetTable := ci.tableName

						// Check if the target table matches the main table (by name or alias)
						mainAlias := stmt.From.Name
						if stmt.From.Alias != "" {
							mainAlias = stmt.From.Alias
						}
						if targetTable == "" || strings.EqualFold(targetTable, mainAlias) || strings.EqualFold(targetTable, stmt.From.Name) {
							// Column is in main table
							for j, col := range mainTableCols {
								if strings.EqualFold(col.Name, ci.aggregateCol) {
									colIdx = j
									break
								}
							}
						} else {
							// Column is in a joined table - find the correct table and offset
							offset := len(mainTableCols)
							for _, join := range stmt.Joins {
								joinTable, err := c.getTableLocked(join.Table.Name)
								if err != nil {
									continue
								}
								joinAlias := join.Table.Name
								if join.Table.Alias != "" {
									joinAlias = join.Table.Alias
								}
								if strings.EqualFold(joinAlias, targetTable) || strings.EqualFold(join.Table.Name, targetTable) {
									// Found the right table, look for column
									for j, col := range joinTable.Columns {
										if strings.EqualFold(col.Name, ci.aggregateCol) {
											colIdx = offset + j
											break
										}
									}
									break
								}
								offset += len(joinTable.Columns)
							}
						}

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
				// Non-aggregate column - get value from first row
				colIdx := -1
				// Try table-qualified match first
				if ci.tableName != "" {
					for j, col := range allColumns {
						if strings.EqualFold(col.Name, ci.name) && strings.EqualFold(col.sourceTbl, ci.tableName) {
							colIdx = j
							break
						}
					}
				}
				// Fallback to name-only match
				if colIdx < 0 {
					for j, col := range allColumns {
						if col.Name == ci.name {
							colIdx = j
							break
						}
					}
				}
				if ci.hasEmbeddedAgg && len(groupRows) > 0 {
					// Expression with embedded aggregates in JOIN context
					val, err := c.evaluateExprWithGroupAggregatesJoin(ci.originalExpr, groupRows, allColumns, args)
					if err == nil {
						resultRow[i] = val
					}
				} else if colIdx >= 0 && len(groupRows) > 0 && colIdx < len(groupRows[0]) {
					resultRow[i] = groupRows[0][colIdx]
				} else if colIdx == -1 && len(groupRows) > 0 {
					// Expression column (CASE, CAST, etc.) - evaluate it
					if i < len(stmt.Columns) {
						expr := stmt.Columns[i]
						if ae, ok := expr.(*query.AliasExpr); ok {
							expr = ae.Expr
						}
						val, err := evaluateExpression(c, groupRows[0], allColumns, expr, args)
						if err == nil {
							resultRow[i] = val
						}
					}
				}
			}
		}

		resultRows = append(resultRows, resultRow)
	}

	// Apply HAVING clause to results
	if stmt.Having != nil {
		var filtered [][]interface{}
		for _, row := range resultRows {
			havingMatched, err := evaluateHaving(c, row, selectCols, nil, stmt.Having, args)
			if err == nil && havingMatched {
				filtered = append(filtered, row)
			}
		}
		resultRows = filtered
	}

	return returnColumns, resultRows, nil
}

func (c *Catalog) applyOrderBy(rows [][]interface{}, selectCols []selectColInfo, orderBy []*query.OrderByExpr) [][]interface{} {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows
	}

	// Build sort key function
	sorted := make([][]interface{}, len(rows))
	copy(sorted, rows)

	sort.Slice(sorted, func(i, j int) bool {
		for obIdx, ob := range orderBy {
			// Find column index by matching expression to selectCols
			colIdx := -1
			switch expr := ob.Expr.(type) {
			case *query.Identifier:
				// Check for dotted identifier like "table.column"
				if dotIdx := strings.IndexByte(expr.Name, '.'); dotIdx > 0 && dotIdx < len(expr.Name)-1 {
					tblLower := strings.ToLower(expr.Name[:dotIdx])
					colNameLower := strings.ToLower(expr.Name[dotIdx+1:])
					for idx, ci := range selectCols {
						if strings.ToLower(ci.name) == colNameLower && strings.ToLower(ci.tableName) == tblLower {
							colIdx = idx
							break
						}
					}
					if colIdx < 0 {
						for idx, ci := range selectCols {
							if strings.ToLower(ci.name) == colNameLower {
								colIdx = idx
								break
							}
						}
					}
				} else {
					nameLower := strings.ToLower(expr.Name)
					for idx, ci := range selectCols {
						if strings.ToLower(ci.name) == nameLower {
							colIdx = idx
							break
						}
					}
				}
			case *query.QualifiedIdentifier:
				colLower := strings.ToLower(expr.Column)
				tblLower := strings.ToLower(expr.Table)
				// First try exact match with table name
				for idx, ci := range selectCols {
					if strings.ToLower(ci.name) == colLower && strings.ToLower(ci.tableName) == tblLower {
						colIdx = idx
						break
					}
				}
				// Fallback to column-name-only match
				if colIdx < 0 {
					for idx, ci := range selectCols {
						if strings.ToLower(ci.name) == colLower {
							colIdx = idx
							break
						}
					}
				}
			case *query.NumberLiteral:
				// ORDER BY 1, 2, 3 (column position)
				if pos, ok := toFloat64(expr.Value); ok {
					colIdx = int(pos) - 1 // 1-based to 0-based
				}
			default:
				// Expression ORDER BY (e.g., ORDER BY price * quantity)
				// Match to the correct hidden ORDER BY column by index
				targetName := fmt.Sprintf("__orderby_%d", obIdx)
				for idx, ci := range selectCols {
					if ci.name == targetName {
						colIdx = idx
						break
					}
				}
			}
			if colIdx < 0 || colIdx >= len(sorted[i]) || colIdx >= len(sorted[j]) {
				continue
			}

			cmp := compareValues(sorted[i][colIdx], sorted[j][colIdx])
			if cmp != 0 {
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return sorted
}