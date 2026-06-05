package catalog

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

type applySelectPostProcessParams struct {
	rows              [][]interface{}
	selectCols        []selectColInfo
	stmt              *query.SelectStmt
	args              []interface{}
	returnColumns     []string
	hiddenOrderByCols int
	hasWindowFuncs    bool
	windowFullRows    [][]interface{}
	table             *TableDef
}

func (cat *Catalog) applySelectPostProcess(p applySelectPostProcessParams) ([]string, [][]interface{}) {
	rows := p.rows
	returnColumns := p.returnColumns

	if p.hasWindowFuncs {
		rows = cat.evaluateWindowFunctions(rows, p.selectCols, p.table, p.stmt, p.args, p.windowFullRows)
	}

	if len(p.stmt.OrderBy) > 0 {
		rows = cat.applyOrderBy(rows, p.selectCols, p.stmt.OrderBy)
	}

	if p.stmt.Distinct {
		visibleCols := len(p.selectCols)
		if p.hiddenOrderByCols > 0 {
			visibleCols = len(p.selectCols) - p.hiddenOrderByCols
		}
		seen := make(map[string]bool)
		var distinctRows [][]interface{}
		for _, row := range rows {
			visibleSlice := row
			if visibleCols < len(row) {
				visibleSlice = row[:visibleCols]
			}
			key := rowKeyForDedup(visibleSlice)
			if !seen[key] {
				seen[key] = true
				distinctRows = append(distinctRows, row)
			}
		}
		rows = distinctRows
	}

	if p.hiddenOrderByCols > 0 {
		rows = stripHiddenCols(rows, len(p.selectCols), p.hiddenOrderByCols)
	}

	if p.stmt.Offset != nil {
		offsetVal, err := evaluateExpression(cat, nil, nil, p.stmt.Offset, p.args)
		if err == nil {
			if offset, ok := toInt(offsetVal); ok && offset > 0 {
				if offset >= len(rows) {
					rows = nil
				} else {
					rows = rows[offset:]
				}
			}
		}
	}

	if p.stmt.Limit != nil {
		limitVal, err := evaluateExpression(cat, nil, nil, p.stmt.Limit, p.args)
		if err == nil {
			if limit, ok := toInt(limitVal); ok && limit >= 0 && int(limit) <= len(rows) {
				rows = rows[:limit]
			}
		}
	}

	if cat.enableRLS && cat.rlsManager != nil && p.stmt.From != nil {
		rlsCtx := cat.rlsCtx
		if rlsCtx == nil {
			rlsCtx = context.Background()
		}
		user, roles := rlsContext(rlsCtx)
		if user != "" {
			cols, filteredRows, rlsErr := cat.applyRLSFilterInternal(rlsCtx, p.stmt.From.Name, returnColumns, rows, user, roles)
			if rlsErr != nil {
				return nil, nil
			}
			returnColumns = cols
			rows = filteredRows
		}
	}

	return returnColumns, rows
}

type cteResult struct {
	cols []string
	rows [][]interface{}
	err  error
}

func (cat *Catalog) handleCTEResult(stmt *query.SelectStmt, args []interface{}, cteRes *cteResultSet) (cteResult, bool) {
	if len(stmt.Joins) != 0 {
		return cteResult{}, false
	}

	hasWindowFuncs := false
	for _, col := range stmt.Columns {
		if query.ExprContainsWindow(col) {
			hasWindowFuncs = true
			break
		}
	}

	if hasWindowFuncs {
		cols, rows, err := cat.executeCTEWindowQuery(stmt, args, cteRes)
		return cteResult{cols: cols, rows: rows, err: err}, true
	}

	cols, rows, err := cat.applyOuterQuery(stmt, cteRes.columns, cteRes.rows, args)
	return cteResult{cols: cols, rows: rows, err: err}, true
}

func (cat *Catalog) executeCTEWindowQuery(stmt *query.SelectStmt, args []interface{}, cteRes *cteResultSet) ([]string, [][]interface{}, error) {
	syntheticCols := make([]ColumnDef, len(cteRes.columns))
	for i, name := range cteRes.columns {
		syntheticCols[i] = ColumnDef{Name: name, Type: "TEXT"}
	}
	syntheticTable := &TableDef{
		Name:    stmt.From.Name,
		Columns: syntheticCols,
	}

	selectCols := make([]selectColInfo, len(stmt.Columns))
	returnColumns := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		aliasName := ""
		actual := col
		if ae, ok := col.(*query.AliasExpr); ok {
			aliasName = ae.Alias
			actual = ae.Expr
		}
		switch c := actual.(type) {
		case *query.WindowExpr:
			selectCols[i] = selectColInfo{
				name:       aliasName,
				isWindow:   true,
				windowExpr: c,
			}
			if aliasName != "" {
				returnColumns[i] = aliasName
			} else {
				returnColumns[i] = c.Function
			}
		case *query.Identifier:
			selectCols[i] = selectColInfo{name: c.Name}
			if aliasName != "" {
				returnColumns[i] = aliasName
			} else {
				returnColumns[i] = c.Name
			}
		default:
			var ews []*query.WindowExpr
			query.CollectWindowExprs(actual, &ews)
			selectCols[i] = selectColInfo{
				name:            aliasName,
				index:           -1,
				originalExpr:    actual,
				embeddedWindows: ews,
			}
			if aliasName != "" {
				returnColumns[i] = aliasName
			} else {
				returnColumns[i] = "col" + strconv.Itoa(i)
			}
		}
	}

	var filteredRows [][]interface{}
	for _, row := range cteRes.rows {
		if stmt.Where != nil {
			matched, err := evaluateWhere(cat, row, syntheticCols, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
		}
		filteredRows = append(filteredRows, row)
	}

	var projectedRows [][]interface{}
	for _, row := range filteredRows {
		projRow := make([]interface{}, len(selectCols))
		for i, ci := range selectCols {
			if ci.isWindow {
				projRow[i] = nil
			} else {
				for j, name := range cteRes.columns {
					colName := ci.name
					if colName == "" {
						continue
					}
					if strings.EqualFold(name, colName) && j < len(row) {
						projRow[i] = row[j]
						break
					}
				}
			}
		}
		projectedRows = append(projectedRows, projRow)
	}

	projectedRows = cat.evaluateWindowFunctions(projectedRows, selectCols, syntheticTable, stmt, args, filteredRows)

	if len(stmt.OrderBy) > 0 {
		sort.SliceStable(projectedRows, func(a, b int) bool {
			for _, ob := range stmt.OrderBy {
				va, _ := evaluateExpression(cat, projectedRows[a], syntheticCols, ob.Expr, args)
				vb, _ := evaluateExpression(cat, projectedRows[b], syntheticCols, ob.Expr, args)
				if va == nil {
					for j, ci := range selectCols {
						if id, ok := ob.Expr.(*query.Identifier); ok && strings.EqualFold(ci.name, id.Name) {
							va = projectedRows[a][j]
							vb = projectedRows[b][j]
							break
						}
					}
				}
				cmp := compareValues(va, vb)
				if cmp == 0 {
					continue
				}
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}

	if stmt.Offset != nil {
		offsetVal, err := evaluateExpression(cat, nil, nil, stmt.Offset, args)
		if err == nil {
			if off, ok := toInt(offsetVal); ok && off > 0 {
				if int(off) < len(projectedRows) {
					projectedRows = projectedRows[off:]
				} else {
					projectedRows = nil
				}
			}
		}
	}
	if stmt.Limit != nil {
		limitVal, err := evaluateExpression(cat, nil, nil, stmt.Limit, args)
		if err == nil {
			if lim, ok := toInt(limitVal); ok && lim >= 0 && int(lim) < len(projectedRows) {
				projectedRows = projectedRows[:lim]
			}
		}
	}

	return returnColumns, projectedRows, nil
}

// window functions, and arbitrary expressions.
func (cat *Catalog) buildSelectColumnInfo(
	stmt *query.SelectStmt,
	table *TableDef,
	mainTableRef string,
) ([]selectColInfo, []string, bool) {
	var selectCols []selectColInfo
	var hasAggregates bool

	for _, col := range stmt.Columns {
		aliasName := ""
		actualCol := col
		if ae, ok := col.(*query.AliasExpr); ok {
			aliasName = ae.Alias
			actualCol = ae.Expr
		}
		switch c := actualCol.(type) {
		case *query.Identifier:
			selectCols, _ = cat.resolveIdentifierColumn(c, aliasName, stmt, table, mainTableRef, selectCols)
		case *query.QualifiedIdentifier:
			selectCols = cat.resolveQualifiedColumn(c, aliasName, stmt, table, mainTableRef, selectCols)
		case *query.StarExpr:
			selectCols = cat.resolveStarColumns(c, stmt, table, mainTableRef, selectCols)
		case *query.FunctionCall:
			var agg bool
			selectCols, agg = cat.resolveFunctionColumn(c, aliasName, actualCol, stmt, table, mainTableRef, selectCols)
			if agg {
				hasAggregates = true
			}
		case *query.WindowExpr:
			selectCols = cat.resolveWindowColumn(c, aliasName, mainTableRef, selectCols)
		default:
			var agg bool
			selectCols, agg = cat.resolveExpressionColumn(actualCol, aliasName, mainTableRef, selectCols)
			if agg {
				hasAggregates = true
			}
		}
		if aliasName != "" && len(selectCols) > 0 {
			selectCols[len(selectCols)-1].name = aliasName
		}
	}

	returnColumns := make([]string, len(selectCols))
	for i, ci := range selectCols {
		returnColumns[i] = ci.name
	}

	return selectCols, returnColumns, hasAggregates
}

func (cat *Catalog) resolveIdentifierColumn(
	c *query.Identifier, aliasName string,
	stmt *query.SelectStmt, table *TableDef, mainTableRef string,
	selectCols []selectColInfo,
) ([]selectColInfo, bool) {
	if dotIdx := strings.IndexByte(c.Name, '.'); dotIdx > 0 && dotIdx < len(c.Name)-1 {
		qi := &query.QualifiedIdentifier{Table: c.Name[:dotIdx], Column: c.Name[dotIdx+1:]}
		colName := qi.Column
		targetTable := qi.Table

		mainTableAlias := stmt.From.Name
		if stmt.From.Alias != "" {
			mainTableAlias = stmt.From.Alias
		}

		if targetTable == stmt.From.Name || targetTable == stmt.From.Alias {
			if idx := table.GetColumnIndex(colName); idx >= 0 {
				displayName := colName
				if aliasName != "" {
					displayName = aliasName
				}
				return append(selectCols, selectColInfo{name: displayName, tableName: mainTableAlias, index: idx}), false
			}
		} else {
			for _, join := range stmt.Joins {
				joinAlias := join.Table.Name
				if join.Table.Alias != "" {
					joinAlias = join.Table.Alias
				}
				if joinAlias == targetTable || join.Table.Name == targetTable {
					joinTable, ok := cat.resolveJoinTableDef(join.Table)
					if ok {
						if idx := joinTable.GetColumnIndex(colName); idx >= 0 {
							displayName := colName
							if aliasName != "" {
								displayName = aliasName
							}
							return append(selectCols, selectColInfo{name: displayName, tableName: joinAlias, index: idx}), false
						}
					}
					break
				}
			}
		}
		return selectCols, false
	}

	if idx := table.GetColumnIndex(c.Name); idx >= 0 {
		displayName := c.Name
		if aliasName != "" {
			displayName = aliasName
		}
		return append(selectCols, selectColInfo{name: displayName, tableName: mainTableRef, index: idx}), false
	}
	// Not found in the main table: an unqualified column may belong to a joined
	// table (e.g. SELECT id, x, y FROM a JOIN b ON ... where y is a column of b).
	for _, join := range stmt.Joins {
		joinAlias := join.Table.Name
		if join.Table.Alias != "" {
			joinAlias = join.Table.Alias
		}
		joinTable, ok := cat.resolveJoinTableDef(join.Table)
		if ok {
			if idx := joinTable.GetColumnIndex(c.Name); idx >= 0 {
				displayName := c.Name
				if aliasName != "" {
					displayName = aliasName
				}
				return append(selectCols, selectColInfo{name: displayName, tableName: joinAlias, index: idx}), false
			}
		}
	}
	return selectCols, false
}

// resolveJoinTableDef returns the column metadata for a joined table, resolving
// CTE-materialized results (and derived tables) before falling back to the
// persistent catalog. Without this, JOINs whose table is a CTE silently drop
// their columns from the projection.
func (cat *Catalog) resolveJoinTableDef(ref *query.TableRef) (*TableDef, bool) {
	// Derived table (subquery): infer output column names from its SELECT list
	// so JOINs against a subquery don't drop the subquery's columns.
	if ref.Subquery != nil {
		if names, ok := derivedSelectColumnNames(ref.Subquery); ok {
			cols := make([]ColumnDef, len(names))
			for i, n := range names {
				cols[i] = ColumnDef{Name: n, Type: "TEXT"}
			}
			return &TableDef{Name: ref.Alias, Columns: cols}, true
		}
	}
	name := ref.Name
	if cat.cteResults != nil {
		if cteRes, ok := cat.cteResults[toLowerFast(name)]; ok {
			cols := make([]ColumnDef, len(cteRes.columns))
			for i, col := range cteRes.columns {
				cols[i] = ColumnDef{Name: col, Type: "TEXT"}
			}
			return &TableDef{Name: name, Columns: cols}, true
		}
	}
	if t, err := cat.getTableLocked(name); err == nil {
		return t, true
	}
	return nil, false
}

// derivedSelectColumnNames infers the output column names of a derived-table
// subquery from its SELECT list, matching the order executeDerivedTable yields.
// Returns false when a name can't be inferred (e.g. SELECT *).
func derivedSelectColumnNames(sel *query.SelectStmt) ([]string, bool) {
	names := make([]string, 0, len(sel.Columns))
	for _, col := range sel.Columns {
		if ae, ok := col.(*query.AliasExpr); ok {
			names = append(names, ae.Alias)
			continue
		}
		switch c := col.(type) {
		case *query.Identifier:
			names = append(names, c.Name)
		case *query.QualifiedIdentifier:
			names = append(names, c.Column)
		case *query.FunctionCall:
			names = append(names, c.Name+"()")
		case *query.WindowExpr:
			names = append(names, c.Function+"()")
		default:
			return nil, false
		}
	}
	return names, true
}

func (cat *Catalog) resolveQualifiedColumn(
	c *query.QualifiedIdentifier, aliasName string,
	stmt *query.SelectStmt, table *TableDef, mainTableRef string,
	selectCols []selectColInfo,
) []selectColInfo {
	targetTable := c.Table
	colName := c.Column

	mainTableAlias := stmt.From.Name
	if stmt.From.Alias != "" {
		mainTableAlias = stmt.From.Alias
	}

	if targetTable == stmt.From.Name || targetTable == stmt.From.Alias {
		if idx := table.GetColumnIndex(colName); idx >= 0 {
			return append(selectCols, selectColInfo{name: colName, tableName: mainTableAlias, index: idx})
		}
	} else {
		for _, join := range stmt.Joins {
			joinAlias := join.Table.Name
			if join.Table.Alias != "" {
				joinAlias = join.Table.Alias
			}
			if joinAlias == targetTable || join.Table.Name == targetTable {
				joinTable, ok := cat.resolveJoinTableDef(join.Table)
				if ok {
					if idx := joinTable.GetColumnIndex(colName); idx >= 0 {
						return append(selectCols, selectColInfo{name: colName, tableName: joinAlias, index: idx})
					}
				}
				break
			}
		}
	}
	return selectCols
}

func (cat *Catalog) resolveStarColumns(
	c *query.StarExpr, stmt *query.SelectStmt, table *TableDef, mainTableRef string,
	selectCols []selectColInfo,
) []selectColInfo {
	// A qualified star (table.*) restricts expansion to one table; an unqualified
	// star expands all tables.
	wantMain := c.Table == "" || c.Table == stmt.From.Name || c.Table == stmt.From.Alias
	if wantMain {
		for i, tc := range table.Columns {
			selectCols = append(selectCols, selectColInfo{name: tc.Name, tableName: mainTableRef, index: i})
		}
	}
	for _, join := range stmt.Joins {
		joinAlias := join.Table.Name
		if join.Table.Alias != "" {
			joinAlias = join.Table.Alias
		}
		if c.Table != "" && c.Table != join.Table.Name && c.Table != join.Table.Alias {
			continue
		}
		joinTable, ok := cat.resolveJoinTableDef(join.Table)
		if ok {
			for i, tc := range joinTable.Columns {
				selectCols = append(selectCols, selectColInfo{name: tc.Name, tableName: joinAlias, index: i})
			}
		}
	}
	return selectCols
}

func (cat *Catalog) resolveFunctionColumn(
	c *query.FunctionCall, aliasName string, actualCol query.Expression,
	stmt *query.SelectStmt, table *TableDef, mainTableRef string,
	selectCols []selectColInfo,
) ([]selectColInfo, bool) {
	if isAggregateFuncName(toUpperFast(c.Name)) {
		colName := "*"
		aggTableName := mainTableRef
		var aggExpr query.Expression
		if len(c.Args) > 0 {
			switch arg := c.Args[0].(type) {
			case *query.Identifier:
				colName = arg.Name
			case *query.QualifiedIdentifier:
				colName = arg.Column
				if arg.Table == stmt.From.Name || arg.Table == stmt.From.Alias {
					aggTableName = mainTableRef
				} else {
					for _, join := range stmt.Joins {
						joinAlias := join.Table.Name
						if join.Table.Alias != "" {
							joinAlias = join.Table.Alias
						}
						if joinAlias == arg.Table || join.Table.Name == arg.Table {
							aggTableName = joinAlias
							break
						}
					}
				}
			case *query.StarExpr:
				colName = "*"
			default:
				colName = fmt.Sprintf("%v", arg)
				aggExpr = c.Args[0]
			}
		}
		displayName := c.Name + "(" + colName + ")"
		if c.Distinct {
			displayName = c.Name + "(DISTINCT " + colName + ")"
		}
		return append(selectCols, selectColInfo{
			name:          displayName,
			tableName:     aggTableName,
			index:         -1,
			isAggregate:   true,
			aggregateType: c.Name,
			aggregateCol:  colName,
			aggregateExpr: aggExpr,
			isDistinct:    c.Distinct,
		}), true
	}

	var embeddedAggs []*query.FunctionCall
	collectAggregatesFromExpr(actualCol, &embeddedAggs)
	var embeddedWindows []*query.WindowExpr
	query.CollectWindowExprs(actualCol, &embeddedWindows)
	colName := c.Name + "()"
	if aliasName != "" {
		colName = aliasName
	}
	return append(selectCols, selectColInfo{
		name:            colName,
		tableName:       mainTableRef,
		index:           -1,
		hasEmbeddedAgg:  len(embeddedAggs) > 0,
		originalExpr:    actualCol,
		embeddedWindows: embeddedWindows,
	}), len(embeddedAggs) > 0
}

func (cat *Catalog) resolveWindowColumn(
	c *query.WindowExpr, aliasName string, mainTableRef string,
	selectCols []selectColInfo,
) []selectColInfo {
	displayName := c.Function + "()"
	if aliasName != "" {
		displayName = aliasName
	}
	return append(selectCols, selectColInfo{
		name:       displayName,
		tableName:  mainTableRef,
		index:      -1,
		isWindow:   true,
		windowExpr: c,
	})
}

func (cat *Catalog) resolveExpressionColumn(
	actualCol query.Expression, aliasName string, mainTableRef string,
	selectCols []selectColInfo,
) ([]selectColInfo, bool) {
	var embeddedAggs []*query.FunctionCall
	collectAggregatesFromExpr(actualCol, &embeddedAggs)
	var embeddedWindows []*query.WindowExpr
	query.CollectWindowExprs(actualCol, &embeddedWindows)
	exprName := "expr"
	if aliasName != "" {
		exprName = aliasName
	}
	return append(selectCols, selectColInfo{
		name:            exprName,
		tableName:       mainTableRef,
		index:           -1,
		hasEmbeddedAgg:  len(embeddedAggs) > 0,
		originalExpr:    actualCol,
		embeddedWindows: embeddedWindows,
	}), len(embeddedAggs) > 0
}
