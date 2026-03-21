package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// tableOffset tracks column offsets for each table in a JOIN
type tableOffset struct {
	name   string
	offset int
	count  int
}

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

		mainTableCols = mainTable.Columns

		// Get all trees for scanning (handles partitioned tables)
		trees, err := c.getTableTreesForScan(mainTable)
		if err != nil {
			return nil, nil, err
		}

		// Build initial intermediate rows from all partition trees (full column data)
		for _, tree := range trees {
			mainIter, err := tree.Scan(nil, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan table: %w", err)
			}
			for mainIter.HasNext() {
				_, data, err := mainIter.Next()
				if err != nil {
					break
				}
				vrow, err := decodeVersionedRow(data, len(mainTable.Columns))
				if err != nil {
					continue
				}
				// Skip deleted rows
				if vrow.Version.DeletedAt > 0 {
					continue
				}
				intermediateRows = append(intermediateRows, vrow.Data)
			}
			mainIter.Close()
		}
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
			if viewDef, viewErr := c.getViewLocked(join.Table.Name); viewErr == nil {
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
			joinIter, err := joinTree.Scan(nil, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan join table: %w", err)
			}
			defer joinIter.Close()
			for joinIter.HasNext() {
				_, data, err := joinIter.Next()
				if err != nil {
					break
				}
				vrow, err := decodeVersionedRow(data, len(joinTable.Columns))
				if err != nil {
					continue
				}
				// Skip deleted rows
				if vrow.Version.DeletedAt > 0 {
					continue
				}
				joinRows = append(joinRows, vrow.Data)
			}
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

		// Convert USING clause to join condition if present
		joinCondition := join.Condition

		// Handle NATURAL JOIN - find common column names between tables
		if join.Natural {
			var commonCols []string
			leftTableName := ""
			if len(tableOffsets) > 0 {
				leftTableName = tableOffsets[len(tableOffsets)-1].name
			}
			// Find columns common to both tables
			for _, leftCol := range combinedColumns {
				if leftCol.sourceTbl == leftTableName {
					for _, rightCol := range joinTableCols {
						if strings.EqualFold(leftCol.Name, rightCol.Name) {
							commonCols = append(commonCols, leftCol.Name)
							break
						}
					}
				}
			}
			join.Using = commonCols
		}

		if joinCondition == nil && len(join.Using) > 0 {
			// Build condition from USING columns: left.col = right.col for each column
			joinTableName := join.Table.Name
			if join.Table.Alias != "" {
				joinTableName = join.Table.Alias
			}
			joinCondition = c.buildUsingCondition(join.Using, tableOffsets, len(combinedColumns), joinTableCols, joinTableName)
		}

		// joinRows is already populated above (from CTE result or B-tree scan)
		rightRows := joinRows

		if isCrossJoin {
			// CROSS JOIN: Cartesian product
			for _, leftRow := range intermediateRows {
				for _, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)

					if joinCondition != nil {
						ok, err := evaluateWhere(c, combined, newCombinedColumns, joinCondition, args)
						if err != nil || !ok {
							continue
						}
					}
					newIntermediate = append(newIntermediate, combined)
				}
			}
		} else {
			// Try hash join for simple equality ON conditions (O(N+M) instead of O(N*M))
			leftColIdx, rightColIdx, canHashJoin := detectEqualityJoinQualified(joinCondition, combinedColumns, joinTableCols, tableOffsets, joinAlias)

			// Fallback: try unqualified equality detection if QI detection failed
			if !canHashJoin {
				leftColIdx, rightColIdx, canHashJoin = detectEqualityJoinUnique(joinCondition, combinedColumns, joinTableCols)
			}

			if canHashJoin && !isRightJoin {
				// Hash join: build hash map on right table, probe with left table
				// Skip NULL keys (SQL semantics: NULL != NULL in JOINs)
				hashMap := make(map[string][]int) // key -> right row indices
				for ri, joinRow := range rightRows {
					if rightColIdx < len(joinRow) && joinRow[rightColIdx] != nil {
						key := hashJoinKey(joinRow[rightColIdx])
						hashMap[key] = append(hashMap[key], ri)
					}
				}

				for _, leftRow := range intermediateRows {
					matched := false
					if leftColIdx < len(leftRow) && leftRow[leftColIdx] != nil {
						key := hashJoinKey(leftRow[leftColIdx])
						if indices, ok := hashMap[key]; ok {
							for _, ri := range indices {
								combined := make([]interface{}, len(leftRow)+len(rightRows[ri]))
								copy(combined, leftRow)
								copy(combined[len(leftRow):], rightRows[ri])
								newIntermediate = append(newIntermediate, combined)
								matched = true
							}
						}
					}

					if isLeftJoin && !matched {
						combined := make([]interface{}, len(leftRow)+len(joinTableCols))
						copy(combined, leftRow)
						newIntermediate = append(newIntermediate, combined)
					}
				}
			} else {
				// Fallback: nested loop join
				rightMatched := make([]bool, len(rightRows))

				for _, leftRow := range intermediateRows {
					matched := false

					for ri, joinRow := range rightRows {
						combined := make([]interface{}, len(leftRow)+len(joinRow))
						copy(combined, leftRow)
						copy(combined[len(leftRow):], joinRow)

						if joinCondition != nil {
							ok, err := evaluateWhere(c, combined, newCombinedColumns, joinCondition, args)
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

		mainTableCols = mainTable.Columns

		// Get all trees for scanning (handles partitioned tables)
		trees, err := c.getTableTreesForScan(mainTable)
		if err != nil {
			return nil, nil, err
		}

		// Build initial intermediate rows from all partition trees (full column data)
		for _, tree := range trees {
			mainIter, err := tree.Scan(nil, nil)
			if err != nil {
				return returnColumns, nil, fmt.Errorf("failed to scan table: %w", err)
			}
			for mainIter.HasNext() {
				_, data, err := mainIter.Next()
				if err != nil {
					break
				}
				vrow, err := decodeVersionedRow(data, len(mainTable.Columns))
				if err != nil {
					continue
				}
				// Skip deleted rows
				if vrow.Version.DeletedAt > 0 {
					continue
				}
				intermediateRows = append(intermediateRows, vrow.Data)
			}
			mainIter.Close()
		}
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
			if viewDef, viewErr := c.getViewLocked(join.Table.Name); viewErr == nil {
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
			joinIter, err := joinTree.Scan(nil, nil)
			if err != nil {
				return returnColumns, nil, fmt.Errorf("failed to scan join table: %w", err)
			}
			defer joinIter.Close()
			for joinIter.HasNext() {
				_, data, err := joinIter.Next()
				if err != nil {
					break
				}
				vrow, err := decodeVersionedRow(data, len(joinTable.Columns))
				if err != nil {
					continue
				}
				// Skip deleted rows
				if vrow.Version.DeletedAt > 0 {
					continue
				}
				rightRows = append(rightRows, vrow.Data)
			}
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

// buildUsingCondition creates a join condition from USING clause columns
// For USING (col1, col2), creates: left.col1 = right.col1 AND left.col2 = right.col2
func (c *Catalog) buildUsingCondition(usingCols []string, tableOffsets []tableOffset, leftColCount int, rightCols []ColumnDef, rightTableName string) query.Expression {
	if len(usingCols) == 0 {
		return nil
	}

	// Get left table info (the most recent table before the join)
	leftTableName := ""
	if len(tableOffsets) > 0 {
		leftInfo := tableOffsets[len(tableOffsets)-1]
		leftTableName = leftInfo.name
	}

	var conditions []query.Expression

	for _, colName := range usingCols {
		// Find column index in right table
		rightColIdx := -1
		for i, col := range rightCols {
			if strings.EqualFold(col.Name, colName) {
				rightColIdx = i
				break
			}
		}

		// If found in right table, create comparison
		// For USING clause, we assume the column also exists in left table
		if rightColIdx >= 0 {
			// Create left table column reference
			leftRef := &query.QualifiedIdentifier{
				Table:  leftTableName,
				Column: colName,
			}

			// Create right table column reference
			rightRef := &query.QualifiedIdentifier{
				Table:  rightTableName,
				Column: colName,
			}

			// Create comparison: left.col = right.col
			comparison := &query.BinaryExpr{
				Left:     leftRef,
				Operator: query.TokenEq,
				Right:    rightRef,
			}

			conditions = append(conditions, comparison)
		}
	}

	if len(conditions) == 0 {
		return nil
	}

	// Combine all conditions with AND
	result := conditions[0]
	for i := 1; i < len(conditions); i++ {
		result = &query.BinaryExpr{
			Left:     result,
			Operator: query.TokenAnd,
			Right:    conditions[i],
		}
	}

	return result
}

// detectEqualityJoinQualified checks if a join condition is a qualified equality
// (e.g., t1.id = t2.user_id) and resolves column indices in combined row and right row.
// Returns (leftIdxInCombinedRow, rightIdxInRightRow, true) if hash join can be used.
func detectEqualityJoinQualified(condition query.Expression, combinedCols []ColumnDef, rightCols []ColumnDef, offsets []tableOffset, rightAlias string) (int, int, bool) {
	binExpr, ok := condition.(*query.BinaryExpr)
	if !ok || binExpr.Operator != query.TokenEq {
		return 0, 0, false
	}

	// Both sides must be QualifiedIdentifier (t.col) for safe hash join
	leftQI, leftOk := binExpr.Left.(*query.QualifiedIdentifier)
	rightQI, rightOk := binExpr.Right.(*query.QualifiedIdentifier)
	if !leftOk || !rightOk {
		return 0, 0, false
	}

	// Determine which side refers to the right (new join) table
	var leftTableQI, rightTableQI *query.QualifiedIdentifier
	if strings.EqualFold(rightQI.Table, rightAlias) {
		leftTableQI = leftQI
		rightTableQI = rightQI
	} else if strings.EqualFold(leftQI.Table, rightAlias) {
		leftTableQI = rightQI
		rightTableQI = leftQI
	} else {
		return 0, 0, false // Neither side references the join table
	}

	// Find left column index in combined columns using table+column match
	leftIdx := -1
	for i, col := range combinedCols {
		if strings.EqualFold(col.Name, leftTableQI.Column) &&
			(col.sourceTbl == "" || strings.EqualFold(col.sourceTbl, leftTableQI.Table)) {
			leftIdx = i
			break
		}
	}

	// Find right column index in right-only columns
	rightIdx := -1
	for i, col := range rightCols {
		if strings.EqualFold(col.Name, rightTableQI.Column) {
			rightIdx = i
			break
		}
	}

	if leftIdx >= 0 && rightIdx >= 0 {
		return leftIdx, rightIdx, true
	}
	return 0, 0, false
}

// detectEqualityJoinUnique checks if a join condition uses unqualified column names
// that are UNIQUE in their respective column sets (no ambiguity).
func detectEqualityJoinUnique(condition query.Expression, combinedCols []ColumnDef, rightCols []ColumnDef) (int, int, bool) {
	binExpr, ok := condition.(*query.BinaryExpr)
	if !ok || binExpr.Operator != query.TokenEq {
		return 0, 0, false
	}

	// Extract column names (Identifier only, not QualifiedIdentifier which is handled above)
	var leftName, rightName string
	if id, ok := binExpr.Left.(*query.Identifier); ok {
		leftName = id.Name
	}
	if id, ok := binExpr.Right.(*query.Identifier); ok {
		rightName = id.Name
	}
	if leftName == "" || rightName == "" {
		return 0, 0, false
	}

	// Try both orderings: (leftName in combined, rightName in right) and vice versa
	for pass := 0; pass < 2; pass++ {
		var cName, rName string
		if pass == 0 {
			cName, rName = leftName, rightName
		} else {
			cName, rName = rightName, leftName
		}

		// Find cName in combinedCols — must be unique
		cIdx := -1
		cCount := 0
		for i, col := range combinedCols {
			if strings.EqualFold(col.Name, cName) {
				cIdx = i
				cCount++
			}
		}

		// Find rName in rightCols — must be unique
		rIdx := -1
		rCount := 0
		for i, col := range rightCols {
			if strings.EqualFold(col.Name, rName) {
				rIdx = i
				rCount++
			}
		}

		// Only use hash join if both columns are unambiguous (appear exactly once)
		if cIdx >= 0 && rIdx >= 0 && cCount == 1 && rCount == 1 {
			return cIdx, rIdx, true
		}
	}

	return 0, 0, false
}

// detectEqualityJoin checks if a join condition is a simple equality (a.col = b.col)
// and returns the column indices in left and right row slices.
// Returns (leftIdx, rightIdx, true) if hash join can be used.
func detectEqualityJoin(condition query.Expression, leftCols []ColumnDef, rightCols []ColumnDef) (int, int, bool) {
	binExpr, ok := condition.(*query.BinaryExpr)
	if !ok || binExpr.Operator != query.TokenEq {
		return 0, 0, false
	}

	leftIdx := -1
	rightIdx := -1

	// Try to resolve left side as a column reference
	leftColName := extractColumnName(binExpr.Left)
	rightColName := extractColumnName(binExpr.Right)

	if leftColName == "" || rightColName == "" {
		return 0, 0, false
	}

	// Check if leftColName is in leftCols and rightColName is in rightCols
	for i, col := range leftCols {
		if strings.EqualFold(col.Name, leftColName) {
			leftIdx = i
			break
		}
	}
	for i, col := range rightCols {
		if strings.EqualFold(col.Name, rightColName) {
			rightIdx = i
			break
		}
	}

	if leftIdx >= 0 && rightIdx >= 0 {
		return leftIdx, rightIdx, true
	}

	// Try swapped: leftColName in rightCols, rightColName in leftCols
	leftIdx = -1
	rightIdx = -1
	for i, col := range leftCols {
		if strings.EqualFold(col.Name, rightColName) {
			leftIdx = i
			break
		}
	}
	for i, col := range rightCols {
		if strings.EqualFold(col.Name, leftColName) {
			rightIdx = i
			break
		}
	}

	if leftIdx >= 0 && rightIdx >= 0 {
		return leftIdx, rightIdx, true
	}

	return 0, 0, false
}

// extractColumnName extracts column name from Identifier or QualifiedIdentifier
func extractColumnName(expr query.Expression) string {
	switch e := expr.(type) {
	case *query.Identifier:
		return e.Name
	case *query.QualifiedIdentifier:
		return e.Column
	}
	return ""
}

// hashJoinKey converts a value to a string key for hash join lookups.
// Uses type-specific formatting to avoid fmt.Sprintf reflection overhead.
func hashJoinKey(v interface{}) string {
	switch val := v.(type) {
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}