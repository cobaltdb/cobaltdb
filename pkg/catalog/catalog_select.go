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

// Select executes a SELECT statement and returns column names and matching rows.
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

		// Execute query (outermost path may release lock during scan)
		columns, rows, err := cat.selectLockedInternal(stmt, args, true)
		if err != nil {
			return nil, nil, err
		}

		// Store in cache
		tables := extractTablesFromQuery(stmt)
		cat.queryCache.Set(cacheKey, columns, rows, tables)

		return columns, rows, nil
	}

	return cat.selectLockedInternal(stmt, args, true)
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
			if isAggregateFuncName(toUpperFast(fc.Name)) {
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
				colName := "column_" + strconv.Itoa(i)
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
		colName := "column_" + strconv.Itoa(i)
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

		funcName := toUpperFast(fc.Name)
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

// loadMainTableRows resolves the main FROM table reference to column definitions
// and row data, checking CTE results and B-tree scans.
func (c *Catalog) loadMainTableRows(from *query.TableRef) ([]ColumnDef, [][]interface{}, error) {
	// Check if main table is a CTE result
	if c.cteResults != nil {
		if cteRes, ok := c.cteResults[toLowerFast(from.Name)]; ok {
			mainTableCols := make([]ColumnDef, len(cteRes.columns))
			for i, col := range cteRes.columns {
				mainTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
			}
			intermediateRows := make([][]interface{}, len(cteRes.rows))
			copy(intermediateRows, cteRes.rows)
			return mainTableCols, intermediateRows, nil
		}
	}

	// Get the main table
	mainTable, err := c.getTableLocked(from.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("table '%s' not found: %w", from.Name, err)
	}

	// Get all trees for scanning (handles partitioned tables)
	trees, err := c.getTableTreesForScan(mainTable)
	if err != nil {
		return mainTable.Columns, nil, nil
	}

	var intermediateRows [][]interface{}
	seen := make(map[string]int)
	for _, tree := range trees {
		mainIter, err := tree.Scan(nil, nil)
		if err != nil {
			continue
		}
		for mainIter.HasNext() {
			key, data, err := mainIter.Next()
			if err != nil {
				break
			}
			vrow, err := decodeVersionedRow(data, len(mainTable.Columns))
			if err != nil {
				continue
			}
			if vrow.Version.DeletedAt > 0 {
				continue
			}
			intermediateRows = append(intermediateRows, vrow.Data)
			seen[string(key)] = len(intermediateRows) - 1
		}
		mainIter.Close()
	}

	// Read-your-writes: overlay buffered writes (INSERT, UPDATE, DELETE).
	if ts := c.getCurrentTxn(); ts != nil {
		if m, ok := ts.getPendingWriteMap()[mainTable.Name]; ok {
			for _, pw := range m {
				k := string(pw.Key)
				vrow, err := decodeVersionedRow(pw.Value, len(mainTable.Columns))
				if err != nil {
					continue
				}
				if vrow.Version.DeletedAt > 0 {
					if idx, ok := seen[k]; ok {
						intermediateRows[idx] = nil
					}
					continue
				}
				if idx, ok := seen[k]; ok {
					intermediateRows[idx] = vrow.Data
				} else {
					intermediateRows = append(intermediateRows, vrow.Data)
					seen[k] = len(intermediateRows) - 1
			}
			}
		}
	}

	// Remove nil rows (soft-deleted via pending writes)
	var filtered [][]interface{}
	for _, row := range intermediateRows {
		if row != nil {
			filtered = append(filtered, row)
		}
	}

	return mainTable.Columns, filtered, nil
}

func (c *Catalog) executeSelectWithJoin(stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo) ([]string, [][]interface{}, error) {
	var mainTableCols []ColumnDef
	var intermediateRows [][]interface{}

	// Check if main table is a CTE result
	mainTableCols, intermediateRows, err := c.loadMainTableRows(stmt.From)
	if err != nil {
		return nil, nil, err
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
		joinTableCols, joinRows := c.resolveJoinTable(join, args)

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

		newIntermediate = c.executeJoinPass(intermediateRows, rightRows, joinTableCols, combinedColumns, newCombinedColumns, joinCondition, args, isLeftJoin, isRightJoin, isCrossJoin, joinAlias)

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

	selectCols, hiddenOrderByCols := c.resolveHiddenJoinOrderByCols(stmt, selectCols, mainTableCols, mainAlias, combinedColumns, tableOffsets)

	resultRows := c.projectJoinSelectCols(stmt, intermediateRows, selectCols, combinedColumns, tableOffsets, args)

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

// resolveHiddenJoinOrderByCols adds hidden ORDER BY columns from joined tables.
func (c *Catalog) resolveHiddenJoinOrderByCols(stmt *query.SelectStmt, selectCols []selectColInfo, mainTableCols []ColumnDef, mainAlias string, combinedColumns []ColumnDef, tableOffsets []tableOffset) ([]selectColInfo, int) {
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
			found := false
			colLower := toLowerFast(colName)
			tblLower := toLowerFast(tblName)
			for _, ci := range selectCols {
				if toLowerFast(ci.name) == colLower {
					if tblName == "" || toLowerFast(ci.tableName) == tblLower {
						found = true
						break
					}
				}
			}
			if !found {
				if tblName != "" {
					for _, to := range tableOffsets {
						if toLowerFast(to.name) == tblLower {
							for _, col := range combinedColumns {
								if toLowerFast(col.Name) == colLower && toLowerFast(col.sourceTbl) == tblLower {
									rawIdx := -1
									tblDef, tErr := c.getTableLocked(to.name)
									if tErr == nil {
										rawIdx = tblDef.GetColumnIndex(colName)
									}
									if rawIdx < 0 {
										for ci, cc := range combinedColumns[to.offset : to.offset+to.count] {
											if toLowerFast(cc.Name) == colLower {
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
	return selectCols, hiddenOrderByCols
}

// projectJoinSelectCols projects select columns from joined rows.
func (c *Catalog) projectJoinSelectCols(stmt *query.SelectStmt, intermediateRows [][]interface{}, selectCols []selectColInfo, combinedColumns []ColumnDef, tableOffsets []tableOffset, args []interface{}) [][]interface{} {
	var resultRows [][]interface{}
	for _, row := range intermediateRows {
		projected := make([]interface{}, 0, len(selectCols))
		for i, ci := range selectCols {
			if ci.index == -1 && !ci.isAggregate {
				if i < len(stmt.Columns) {
					val, err := evaluateExpression(c, row, combinedColumns, stmt.Columns[i], args)
					if err == nil {
						projected = append(projected, val)
						continue
					}
				}
				projected = append(projected, nil)
				continue
			}
			if ci.index < 0 {
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
	return resultRows
}


func (c *Catalog) executeSelectWithJoinAndGroupBy(stmt *query.SelectStmt, args []interface{}, selectCols []selectColInfo, returnColumns []string) ([]string, [][]interface{}, error) {
	var mainTableCols []ColumnDef
	var intermediateRows [][]interface{}

	// Check if main table is a CTE result or derived table
	mainTableCols, intermediateRows, err := c.loadMainTableRows(stmt.From)
	if err != nil {
		return nil, nil, err
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

	intermediateRows, allColumns = c.executeJoinChainForGroupBy(stmt, args, intermediateRows, allColumns, mainTableCols)

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
		groupKey.Grow(len(joinGroupBySpecs) * 16)
		for i, spec := range joinGroupBySpecs {
			if i > 0 {
				groupKey.WriteString("|")
			}
			if spec.index >= 0 && spec.index < len(row) {
				groupKey.WriteString(ValueToStringKey(row[spec.index]))
			} else if spec.expr != nil {
				val, err := evaluateExpression(c, row, allColumns, spec.expr, args)
				if err == nil {
					groupKey.WriteString(ValueToStringKey(val))
				}
			}
		}
		key := groupKey.String()
		existing := groups[key]
		if len(existing) == 0 {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(existing, row)
	}

	// Compute aggregates for each group
	resultRows := c.computeJoinGroupAggregates(stmt, selectCols, mainTableCols, allColumns, groupOrder, groups, args)

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

// executeJoinPass performs CROSS, hash, or nested loop join on the given row sets.
// resolveJoinTable resolves a JOIN table reference to its column definitions and
// row data by checking subqueries, CTE results, views, and regular tables.
func (c *Catalog) resolveJoinTable(join *query.JoinClause, args []interface{}) ([]ColumnDef, [][]interface{}) {
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
		if cteRes, ok := c.cteResults[toLowerFast(join.Table.Name)]; ok {
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
		// Check if it's a materialized view
		if mv, mvErr := c.getMaterializedViewLocked(join.Table.Name); mvErr == nil {
			joinTableCols = make([]ColumnDef, 0, len(mv.Data[0]))
			if len(mv.Data) > 0 {
				for colName := range mv.Data[0] {
					joinTableCols = append(joinTableCols, ColumnDef{Name: colName, Type: "TEXT"})
				}
			}
			for _, rowMap := range mv.Data {
				row := make([]interface{}, len(joinTableCols))
				for j, col := range joinTableCols {
					row[j] = rowMap[col.Name]
				}
				joinRows = append(joinRows, row)
			}
		}
	}

	if joinTableCols == nil {
		// Normal table lookup
		joinTable, err := c.getTableLocked(join.Table.Name)
		if err != nil {
			return nil, nil
		}

		joinTableCols = joinTable.Columns
		effectiveData := c.getEffectiveTableData(joinTable)
		effectiveKeys := make([]string, 0, len(effectiveData))
		for k := range effectiveData {
			effectiveKeys = append(effectiveKeys, k)
		}
		sort.Strings(effectiveKeys)
		for _, k := range effectiveKeys {
			data := effectiveData[k]
			vrow, err := decodeVersionedRow(data, len(joinTable.Columns))
			if err != nil {
				continue
			}
			joinRows = append(joinRows, vrow.Data)
		}
	}

	return joinTableCols, joinRows
}

func (c *Catalog) executeJoinPass(intermediateRows [][]interface{}, rightRows [][]interface{}, joinTableCols []ColumnDef, combinedColumns []ColumnDef, newCombinedColumns []ColumnDef, joinCondition query.Expression, args []interface{}, isLeftJoin, isRightJoin, isCrossJoin bool, joinAlias string) [][]interface{} {
	var newIntermediate [][]interface{}

	if isCrossJoin {
		// Pre-calculate total capacity for cross join (leftRows * rightRows)
		totalEst := len(intermediateRows) * len(rightRows)
		if totalEst > 0 {
			newIntermediate = make([][]interface{}, 0, totalEst)
		}
		for _, leftRow := range intermediateRows {
			leftLen := len(leftRow)
			rightLen := len(rightRows[0]) // assuming uniform right rows
			for _, joinRow := range rightRows {
				combined := make([]interface{}, leftLen+rightLen)
				copy(combined, leftRow)
				copy(combined[leftLen:], joinRow)

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
		// Pass the join alias so detectEqualityJoinQualified can handle qualified identifiers (t.col)
		leftColIdx, rightColIdx, canHashJoin := detectEqualityJoinQualified(joinCondition, combinedColumns, joinTableCols, nil, joinAlias)

		if !canHashJoin {
			leftColIdx, rightColIdx, canHashJoin = detectEqualityJoinUnique(joinCondition, combinedColumns, joinTableCols)
		}

		if canHashJoin && !isRightJoin {
			hashMap := make(map[string][]int)
			for ri, joinRow := range rightRows {
				if rightColIdx < len(joinRow) && joinRow[rightColIdx] != nil {
					key := hashJoinKey(joinRow[rightColIdx])
					existing := hashMap[key]
					hashMap[key] = append(existing, ri)
				}
			}

			// Handle empty rightRows - skip join entirely
			if len(rightRows) == 0 {
				if isLeftJoin {
					return intermediateRows
				}
				return nil
			}

			// Pre-allocate with estimated capacity for hash join
			// Worst case: all left rows match all right rows
			leftLen := len(intermediateRows)
			rightLen := len(rightRows[0])
			estimatedCombined := leftLen * rightLen
			if estimatedCombined > 0 && estimatedCombined <= 10000000 { // cap at 10M
				newIntermediate = make([][]interface{}, 0, estimatedCombined)
			}

			for _, leftRow := range intermediateRows {
				matched := false
				if leftColIdx < len(leftRow) && leftRow[leftColIdx] != nil {
					key := hashJoinKey(leftRow[leftColIdx])
					if indices, ok := hashMap[key]; ok {
						combinedLen := len(leftRow) + rightLen
						for _, ri := range indices {
							combined := make([]interface{}, combinedLen)
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
			// Handle empty rightRows in nested loop join
			if len(rightRows) == 0 {
				if isLeftJoin {
					return intermediateRows
				}
				return nil
			}

			// Nested loop join - pre-allocate with capacity estimate
			estimatedCombined := len(intermediateRows) * len(rightRows)
			if estimatedCombined > 0 && estimatedCombined <= 10000000 {
				newIntermediate = make([][]interface{}, 0, estimatedCombined)
			}
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

	return newIntermediate
}


// executeJoinChainForGroupBy chains through JOINs for GROUP BY queries.
func (c *Catalog) executeJoinChainForGroupBy(stmt *query.SelectStmt, args []interface{}, intermediateRows [][]interface{}, allColumns []ColumnDef, mainTableCols []ColumnDef) ([][]interface{}, []ColumnDef) {
	for _, join := range stmt.Joins {
		var joinTableCols []ColumnDef
		var rightRows [][]interface{}

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

		if joinTableCols == nil && c.cteResults != nil {
			if cteRes, ok := c.cteResults[toLowerFast(join.Table.Name)]; ok {
				joinTableCols = make([]ColumnDef, len(cteRes.columns))
				for i, col := range cteRes.columns {
					joinTableCols[i] = ColumnDef{Name: col, Type: "TEXT"}
				}
				rightRows = cteRes.rows
			}
		}

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
			joinTable, err := c.getTableLocked(join.Table.Name)
			if err != nil {
				continue
			}
			joinTree, exists := c.tableTrees[join.Table.Name]
			if !exists {
				continue
			}
			joinTableCols = joinTable.Columns

			joinIter, err := joinTree.Scan(nil, nil)
			if err != nil {
				continue
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
		joinAlias := join.Table.Name
		if join.Table.Alias != "" {
			joinAlias = join.Table.Alias
		}
		for i := len(allColumns); i < len(newAllColumns); i++ {
			newAllColumns[i].sourceTbl = joinAlias
		}

		var newIntermediate [][]interface{}

		if isCrossJoin {
			for _, leftRow := range intermediateRows {
				for _, joinRow := range rightRows {
					combined := make([]interface{}, len(leftRow)+len(joinRow))
					copy(combined, leftRow)
					copy(combined[len(leftRow):], joinRow)
					newIntermediate = append(newIntermediate, combined)
				}
			}
		} else {
			// Try hash join first for INNER joins
			if !isLeftJoin && !isRightJoin && len(rightRows) > 0 {
				// Detect if we can use hash join
				leftColIdx, rightColIdx, canHashJoin := detectEqualityJoinQualified(join.Condition, allColumns, joinTableCols, nil, joinAlias)
				if !canHashJoin {
					leftColIdx, rightColIdx, canHashJoin = detectEqualityJoinUnique(join.Condition, allColumns, joinTableCols)
				}

				if canHashJoin {
					hashMap := make(map[string][]int)
					for ri, joinRow := range rightRows {
						if rightColIdx < len(joinRow) && joinRow[rightColIdx] != nil {
							key := hashJoinKey(joinRow[rightColIdx])
							existing := hashMap[key]
							hashMap[key] = append(existing, ri)
						}
					}

					if len(intermediateRows) > 0 && len(rightRows) > 0 {
						rightLen := len(rightRows[0])
						estimatedCombined := len(intermediateRows) * rightLen
						if estimatedCombined > 0 && estimatedCombined <= 10000000 {
							newIntermediate = make([][]interface{}, 0, estimatedCombined)
						}
					}

					for _, leftRow := range intermediateRows {
						if leftColIdx < len(leftRow) && leftRow[leftColIdx] != nil {
							key := hashJoinKey(leftRow[leftColIdx])
							if indices, ok := hashMap[key]; ok {
								for _, ri := range indices {
									combined := make([]interface{}, len(leftRow)+len(rightRows[ri]))
									copy(combined, leftRow)
									copy(combined[len(leftRow):], rightRows[ri])
									newIntermediate = append(newIntermediate, combined)
								}
							}
						}
					}

					intermediateRows = newIntermediate
					allColumns = newAllColumns
					continue
				}
			}

			// Fall back to nested loop join for OUTER joins or non-equality joins
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

	return intermediateRows, allColumns
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
					tblLower := toLowerFast(expr.Name[:dotIdx])
					colNameLower := toLowerFast(expr.Name[dotIdx+1:])
					for idx, ci := range selectCols {
						if toLowerFast(ci.name) == colNameLower && toLowerFast(ci.tableName) == tblLower {
							colIdx = idx
							break
						}
					}
					if colIdx < 0 {
						for idx, ci := range selectCols {
							if toLowerFast(ci.name) == colNameLower {
								colIdx = idx
								break
							}
						}
					}
				} else {
					nameLower := toLowerFast(expr.Name)
					for idx, ci := range selectCols {
						if toLowerFast(ci.name) == nameLower {
							colIdx = idx
							break
						}
					}
				}
			case *query.QualifiedIdentifier:
				colLower := toLowerFast(expr.Column)
				tblLower := toLowerFast(expr.Table)
				// First try exact match with table name
				for idx, ci := range selectCols {
					if toLowerFast(ci.name) == colLower && toLowerFast(ci.tableName) == tblLower {
						colIdx = idx
						break
					}
				}
				// Fallback to column-name-only match
				if colIdx < 0 {
					for idx, ci := range selectCols {
						if toLowerFast(ci.name) == colLower {
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
				targetName := "__orderby_" + strconv.Itoa(obIdx)
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
//
//nolint:unused // retained for future hash-join planner optimizations and tests.
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

// extractColumnName extracts column name from Identifier or QualifiedIdentifier.
//
//nolint:unused // retained for future hash-join planner optimizations and tests.
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
	case []byte:
		return string(val)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}


// computeJoinGroupAggregates computes aggregate results for each group in a
// JOIN+GROUP BY query. Returns one result row per group with aggregate and
// non-aggregate column values.
func (c *Catalog) computeJoinGroupAggregates(stmt *query.SelectStmt, selectCols []selectColInfo, mainTableCols []ColumnDef, allColumns []ColumnDef, groupOrder []string, groups map[string][][]interface{}, args []interface{}) [][]interface{} {
	var resultRows [][]interface{}

	for _, gk := range groupOrder {
		groupRows := groups[gk]
		if len(groupRows) == 0 {
			continue
		}
		resultRow := make([]interface{}, len(selectCols))

		for i, ci := range selectCols {
			if ci.isAggregate {
				var values []interface{}
				for _, row := range groupRows {
					if ci.aggregateCol == "*" && ci.aggregateExpr == nil {
						values = append(values, int64(1))
					} else if ci.aggregateExpr != nil {
						v, err := evaluateExpression(c, row, allColumns, ci.aggregateExpr, args)
						if err == nil {
							values = append(values, v)
						}
					} else {
						colIdx := c.resolveJoinAggregateColumn(ci, stmt, mainTableCols, len(row))
						if colIdx >= 0 && colIdx < len(row) {
							values = append(values, row[colIdx])
						}
					}
				}

				resultRow[i] = computeAggregateValue(ci, values, groupRows)
			} else {
				colIdx := -1
				if ci.tableName != "" {
					for j, col := range allColumns {
						if strings.EqualFold(col.Name, ci.name) && strings.EqualFold(col.sourceTbl, ci.tableName) {
							colIdx = j
							break
						}
					}
				}
				if colIdx < 0 {
					for j, col := range allColumns {
						if col.Name == ci.name {
							colIdx = j
							break
						}
					}
				}
				if ci.hasEmbeddedAgg && len(groupRows) > 0 {
					val, err := c.evaluateExprWithGroupAggregatesJoin(ci.originalExpr, groupRows, allColumns, args)
					if err == nil {
						resultRow[i] = val
					}
				} else if colIdx >= 0 && len(groupRows) > 0 && colIdx < len(groupRows[0]) {
					resultRow[i] = groupRows[0][colIdx]
				} else if colIdx == -1 && len(groupRows) > 0 {
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
	return resultRows
}

// resolveJoinAggregateColumn finds the column index for an aggregate column
// across main and joined tables in a JOIN+GROUP BY query.
func (c *Catalog) resolveJoinAggregateColumn(ci selectColInfo, stmt *query.SelectStmt, mainTableCols []ColumnDef, rowLen int) int {
	colIdx := -1
	targetTable := ci.tableName

	mainAlias := stmt.From.Name
	if stmt.From.Alias != "" {
		mainAlias = stmt.From.Alias
	}
	if targetTable == "" || strings.EqualFold(targetTable, mainAlias) || strings.EqualFold(targetTable, stmt.From.Name) {
		for j, col := range mainTableCols {
			if strings.EqualFold(col.Name, ci.aggregateCol) {
				colIdx = j
				break
			}
		}
	} else {
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
	return colIdx
}

// computeAggregateValue computes a single aggregate function result from collected values.
func computeAggregateValue(ci selectColInfo, values []interface{}, groupRows [][]interface{}) interface{} {
	switch ci.aggregateType {
	case "COUNT":
		if ci.aggregateCol == "*" && !ci.isDistinct {
			return int64(len(groupRows))
		} else if ci.isDistinct {
			seen := make(map[string]bool)
			for _, v := range values {
				if v != nil {
					seen[ValueToStringKey(v)] = true
				}
			}
			return int64(len(seen))
		} else {
			count := int64(0)
			for _, v := range values {
				if v != nil {
					count++
				}
			}
			return count
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
			return sum
		}
		return nil
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
			return sum / float64(count)
		}
		return nil
	case "MIN":
		var minVal interface{}
		for _, v := range values {
			if v != nil {
				if minVal == nil || compareValues(v, minVal) < 0 {
					minVal = v
				}
			}
		}
		return minVal
	case "MAX":
		var maxVal interface{}
		for _, v := range values {
			if v != nil {
				if maxVal == nil || compareValues(v, maxVal) > 0 {
					maxVal = v
				}
			}
		}
		return maxVal
	case "GROUP_CONCAT":
		var parts []string
		for _, v := range values {
			if v != nil {
				parts = append(parts, ValueToStringKey(v))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, ",")
		}
		return nil
	}
	return nil
}

// isIdentityProjection reports whether selectCols is a 1:1 mapping of fullRow.
func isIdentityProjection(selectCols []selectColInfo, fullRowLen int) bool {
	if len(selectCols) != fullRowLen {
		return false
	}
	for i, ci := range selectCols {
		if ci.index != i || ci.isWindow || ci.isAggregate || ci.hasEmbeddedAgg {
			return false
		}
	}
	return true
}

// projectSelectedRow extracts selected column values from a full table row.
// Handles regular columns, scalar expressions, and hidden ORDER BY expression columns.
func (cat *Catalog) projectSelectedRow(fullRow []interface{}, selectCols []selectColInfo, stmt *query.SelectStmt, table *TableDef, args []interface{}, hasWindowFuncs bool) []interface{} {
	if isIdentityProjection(selectCols, len(fullRow)) {
		return fullRow
	}
	selectedRow := make([]interface{}, len(selectCols))
	for i, ci := range selectCols {
		if ci.isWindow {
			continue
		}
		if ci.index >= 0 && ci.index < len(fullRow) {
			selectedRow[i] = fullRow[ci.index]
		} else if ci.index == -1 && !ci.isAggregate {
			if i < len(stmt.Columns) {
				val, err := evaluateExpression(cat, fullRow, table.Columns, stmt.Columns[i], args)
				if err == nil {
					selectedRow[i] = val
				}
			} else if len(ci.name) > 10 && ci.name[:10] == "__orderby_" {
				var obIdx int
				if _, err := fmt.Sscanf(ci.name, "__orderby_%d", &obIdx); err == nil && obIdx < len(stmt.OrderBy) {
					val, err := evaluateExpression(cat, fullRow, table.Columns, stmt.OrderBy[obIdx].Expr, args)
					if err == nil {
						selectedRow[i] = val
					}
				}
			}
		}
	}
	return selectedRow
}

func (cat *Catalog) applyOuterQuery(stmt *query.SelectStmt, viewCols []string, viewRows [][]interface{}, args []interface{}) ([]string, [][]interface{}, error) {
	// Build column definitions from view result columns
	columns := make([]ColumnDef, len(viewCols))
	for i, name := range viewCols {
		columns[i] = ColumnDef{Name: name}
	}

	// Check if outer query has aggregates
	hasAggregates := false
	for _, col := range stmt.Columns {
		actual := col
		if ae, ok := col.(*query.AliasExpr); ok {
			actual = ae.Expr
		}
		if fc, ok := actual.(*query.FunctionCall); ok {
			if strings.EqualFold(fc.Name, "COUNT") || strings.EqualFold(fc.Name, "SUM") || strings.EqualFold(fc.Name, "AVG") || strings.EqualFold(fc.Name, "MIN") || strings.EqualFold(fc.Name, "MAX") || strings.EqualFold(fc.Name, "GROUP_CONCAT") {
				hasAggregates = true
				break
			}
		}
	}

	// Apply WHERE clause
	var filteredRows [][]interface{}
	if stmt.Where != nil {
		for _, row := range viewRows {
			matched, err := evaluateWhere(cat, row, columns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
			filteredRows = append(filteredRows, row)
		}
	} else {
		filteredRows = viewRows
	}

	// Handle aggregates or GROUP BY on the view result
	// Handle aggregates or GROUP BY on the view result
	if hasAggregates || len(stmt.GroupBy) > 0 {
		return cat.applyOuterQueryAggregates(stmt, filteredRows, columns, args)
	}
	return cat.applyOuterQueryProjection(stmt, filteredRows, viewCols, columns, args)
}


// applyOuterQueryAggregates handles the aggregate/GROUP BY path of applyOuterQuery.
func (cat *Catalog) applyOuterQueryAggregates(stmt *query.SelectStmt, filteredRows [][]interface{}, columns []ColumnDef, args []interface{}) ([]string, [][]interface{}, error) {
	// Build return column names
	returnCols := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		aliasName := ""
		actual := col
		if ae, ok := col.(*query.AliasExpr); ok {
			aliasName = ae.Alias
			actual = ae.Expr
		}
		if aliasName != "" {
			returnCols[i] = aliasName
		} else if fc, ok := actual.(*query.FunctionCall); ok {
			fn := toUpperFast(fc.Name)
			if len(fc.Args) > 0 {
				returnCols[i] = fn + "()"
			} else {
				returnCols[i] = fn + "(*)"
			}
		} else if id, ok := actual.(*query.Identifier); ok {
			returnCols[i] = id.Name
		} else {
			returnCols[i] = "col" + strconv.Itoa(i)
		}
	}

	// Group rows if GROUP BY is present
	type rowGroup struct {
		key  string
		rows [][]interface{}
	}
	var groups []rowGroup

	if len(stmt.GroupBy) > 0 {
		groupMap := make(map[string]int)
		for _, row := range filteredRows {
			var keyParts []string
			for _, gb := range stmt.GroupBy {
				val, err := evaluateExpression(cat, row, columns, gb, args)
				if err == nil {
					keyParts = append(keyParts, ValueToStringKey(val))
				} else {
					keyParts = append(keyParts, "<nil>")
				}
			}
			key := strings.Join(keyParts, "|")
			if idx, exists := groupMap[key]; exists {
				groups[idx].rows = append(groups[idx].rows, row)
			} else {
				groupMap[key] = len(groups)
				groups = append(groups, rowGroup{key: key, rows: [][]interface{}{row}})
			}
		}
	} else {
		// Single group: all rows
		groups = []rowGroup{{key: "", rows: filteredRows}}
	}

	// Compute aggregates for each group
	var resultRows [][]interface{}
	for _, group := range groups {
		resultRow := make([]interface{}, len(stmt.Columns))
		for i, col := range stmt.Columns {
			actual := col
			if ae, ok := col.(*query.AliasExpr); ok {
				actual = ae.Expr
			}
			if fc, ok := actual.(*query.FunctionCall); ok {
				fn := toUpperFast(fc.Name)
				resultRow[i] = cat.computeViewAggregate(fn, fc, group.rows, columns, args)
			} else {
				// Non-aggregate column: use value from first row in group
				if len(group.rows) > 0 {
					val, err := evaluateExpression(cat, group.rows[0], columns, actual, args)
					if err == nil {
						resultRow[i] = val
					}
				}
			}
		}
		resultRows = append(resultRows, resultRow)
	}

	// Build result columns for HAVING/ORDER BY evaluation
	resultColumns := make([]ColumnDef, len(returnCols))
	for i, name := range returnCols {
		resultColumns[i] = ColumnDef{Name: name}
	}

	// Apply HAVING filter
	if stmt.Having != nil {
		var havingRows [][]interface{}
		for _, row := range resultRows {
			ok, err := evaluateWhere(cat, row, resultColumns, stmt.Having, args)
			if err == nil && ok {
				havingRows = append(havingRows, row)
			}
		}
		resultRows = havingRows
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 && len(resultRows) > 1 {
		sort.SliceStable(resultRows, func(i, j int) bool {
			for _, ob := range stmt.OrderBy {
				vi, _ := evaluateExpression(cat, resultRows[i], resultColumns, ob.Expr, args)
				vj, _ := evaluateExpression(cat, resultRows[j], resultColumns, ob.Expr, args)
				cmp := compareValues(vi, vj)
				if ob.Desc {
					cmp = -cmp
				}
				if cmp != 0 {
					return cmp < 0
				}
			}
			return false
		})
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil {
		offsetVal, err := EvalExpression(stmt.Offset, args)
		if err == nil {
			if f, ok := toFloat64(offsetVal); ok {
				offset := int(f)
				if offset >= len(resultRows) {
					resultRows = nil
				} else if offset > 0 {
					resultRows = resultRows[offset:]
				}
			}
		}
	}
	if stmt.Limit != nil {
		limitVal, err := EvalExpression(stmt.Limit, args)
		if err == nil {
			if f, ok := toFloat64(limitVal); ok {
				limit := int(f)
				if limit >= 0 && limit <= len(resultRows) {
					resultRows = resultRows[:limit]
				}
			}
		}
	}

	return returnCols, resultRows, nil

}

// applyOuterQueryProjection handles the non-aggregate projection path of applyOuterQuery.
func (cat *Catalog) applyOuterQueryProjection(stmt *query.SelectStmt, filteredRows [][]interface{}, viewCols []string, columns []ColumnDef, args []interface{}) ([]string, [][]interface{}, error) {
// Project columns
var returnCols []string
var resultRows [][]interface{}

// Build column mapping
type colMapping struct {
	name    string
	viewIdx int // -1 if needs evaluation
}
var mappings []colMapping

for _, col := range stmt.Columns {
	aliasName := ""
	actual := col
	if ae, ok := col.(*query.AliasExpr); ok {
		aliasName = ae.Alias
		actual = ae.Expr
	}
	switch c := actual.(type) {
	case *query.StarExpr:
		for j, name := range viewCols {
			mappings = append(mappings, colMapping{name: name, viewIdx: j})
		}
	case *query.Identifier:
		found := false
		for j, name := range viewCols {
			if strings.EqualFold(name, c.Name) {
				displayName := name
				if aliasName != "" {
					displayName = aliasName
				}
				mappings = append(mappings, colMapping{name: displayName, viewIdx: j})
				found = true
				break
			}
		}
		if !found {
			mappings = append(mappings, colMapping{name: c.Name, viewIdx: -1})
		}
	default:
		name := "expr"
		if aliasName != "" {
			name = aliasName
		}
		mappings = append(mappings, colMapping{name: name, viewIdx: -1})
	}
}

returnCols = make([]string, len(mappings))
for i, m := range mappings {
	returnCols[i] = m.name
}

for _, row := range filteredRows {
	resultRow := make([]interface{}, len(mappings))
	for i, m := range mappings {
		if m.viewIdx >= 0 && m.viewIdx < len(row) {
			resultRow[i] = row[m.viewIdx]
		} else {
			// Evaluate expression against view row
			val, err := evaluateExpression(cat, row, columns, stmt.Columns[i], args)
			if err == nil {
				resultRow[i] = val
			}
		}
	}
	resultRows = append(resultRows, resultRow)
}

// Build selectColInfo for ORDER BY
selectCols := make([]selectColInfo, len(mappings))
for i, m := range mappings {
	selectCols[i] = selectColInfo{name: m.name, index: i}
}

// Add hidden ORDER BY columns from view that aren't in the outer SELECT
hiddenViewOrderByCols := 0
if len(stmt.OrderBy) > 0 {
	for _, ob := range stmt.OrderBy {
		if ident, ok := ob.Expr.(*query.Identifier); ok {
			// Check if this column is already in selectCols
			found := false
			for _, sc := range selectCols {
				if strings.EqualFold(sc.name, ident.Name) {
					found = true
					break
				}
			}
			if !found {
				// Check if it's a view column
				for j, vc := range viewCols {
					if strings.EqualFold(vc, ident.Name) {
						// Add as hidden column and append values from view rows
						selectCols = append(selectCols, selectColInfo{name: vc, index: len(mappings) + hiddenViewOrderByCols})
						for k := range resultRows {
							if j < len(filteredRows[k]) {
								resultRows[k] = append(resultRows[k], filteredRows[k][j])
							}
						}
						hiddenViewOrderByCols++
						break
					}
				}
			}
		}
	}
}

// Apply ORDER BY
if len(stmt.OrderBy) > 0 {
	resultRows = cat.applyOrderBy(resultRows, selectCols, stmt.OrderBy)
}

// Strip hidden ORDER BY columns
if hiddenViewOrderByCols > 0 {
	visibleCount := len(mappings)
	for i, row := range resultRows {
		if len(row) > visibleCount {
			resultRows[i] = row[:visibleCount]
		}
	}
}

// Apply DISTINCT
if stmt.Distinct {
	resultRows = cat.applyDistinct(resultRows)
}

// Apply OFFSET
if stmt.Offset != nil {
	offsetVal, err := evaluateExpression(cat, nil, nil, stmt.Offset, args)
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
	limitVal, err := evaluateExpression(cat, nil, nil, stmt.Limit, args)
	if err == nil {
		if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(resultRows) {
			resultRows = resultRows[:limit]
		}
	}
}

return returnCols, resultRows, nil
}

func (cat *Catalog) computeViewAggregate(fn string, fc *query.FunctionCall, rows [][]interface{}, columns []ColumnDef, args []interface{}) interface{} {
	switch fn {
	case "COUNT":
		if len(fc.Args) > 0 {
			if _, ok := fc.Args[0].(*query.StarExpr); ok {
				return int64(len(rows))
			}
			// COUNT(col) - count non-null
			count := int64(0)
			for _, row := range rows {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					count++
				}
			}
			return count
		}
		return int64(len(rows))
	case "SUM":
		sum := float64(0)
		hasVal := false
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					if f, ok := toFloat64(val); ok {
						sum += f
						hasVal = true
					}
				}
			}
		}
		if hasVal {
			return sum
		}
		return nil
	case "AVG":
		sum := float64(0)
		count := 0
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					if f, ok := toFloat64(val); ok {
						sum += f
						count++
					}
				}
			}
		}
		if count > 0 {
			return sum / float64(count)
		}
		return nil
	case "MIN":
		var minVal interface{}
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					if minVal == nil || compareValues(val, minVal) < 0 {
						minVal = val
					}
				}
			}
		}
		return minVal
	case "MAX":
		var maxVal interface{}
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					if maxVal == nil || compareValues(val, maxVal) > 0 {
						maxVal = val
					}
				}
			}
		}
		return maxVal
	case "GROUP_CONCAT":
		var parts []string
		for _, row := range rows {
			if len(fc.Args) > 0 {
				val, err := evaluateExpression(cat, row, columns, fc.Args[0], args)
				if err == nil && val != nil {
					parts = append(parts, ValueToStringKey(val))
				}
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, ",")
		}
		return nil
	}
	return nil
}

