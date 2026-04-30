package catalog

import (
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"sort"
	"strings"
)

// windowPartEntry tracks a row within a window function partition.
type windowPartEntry struct {
	originalIdx int
	row         []interface{}
	fullRow     []interface{}
}

func (c *Catalog) evaluateWindowFunctions(rows [][]interface{}, selectCols []selectColInfo, table *TableDef, stmt *query.SelectStmt, args []interface{}, fullRows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}

	for colIdx, ci := range selectCols {
		if !ci.isWindow || ci.windowExpr == nil {
			continue
		}

		we := ci.windowExpr

		// Build partition groups
		partitions := make(map[string][]windowPartEntry)
		partitionOrder := []string{} // preserve order of first appearance

		for i, row := range rows {
			var fRow []interface{}
			if i < len(fullRows) {
				fRow = fullRows[i]
			}
			// Compute partition key
			partKey := ""
			if len(we.PartitionBy) > 0 {
				var keyParts []string
				for _, pExpr := range we.PartitionBy {
					val := c.evalWindowExprOnRow(pExpr, row, selectCols, table, args, fRow)
					keyParts = append(keyParts, ValueToStringKey(val))
				}
				partKey = strings.Join(keyParts, "|")
			}

			if _, exists := partitions[partKey]; !exists {
				partitionOrder = append(partitionOrder, partKey)
			}
			partitions[partKey] = append(partitions[partKey], windowPartEntry{originalIdx: i, row: row, fullRow: fRow})
		}

		// Process each partition
		c.evalWindowPartitions(rows, colIdx, we, partitions, partitionOrder, selectCols, table, args)
	}

	return rows
}

func (c *Catalog) evalWindowExprOnRow(expr query.Expression, row []interface{}, selectCols []selectColInfo, table *TableDef, args []interface{}, fullRowOpt ...[]interface{}) interface{} {
	// First try to match against projected columns by name
	switch e := expr.(type) {
	case *query.Identifier:
		// Look in selectCols first
		for i, ci := range selectCols {
			if strings.EqualFold(ci.name, e.Name) && i < len(row) {
				return row[i]
			}
		}
		// Look in table columns using the full row if available
		if table != nil {
			// Try full row first (has all table columns)
			if len(fullRowOpt) > 0 && fullRowOpt[0] != nil {
				if idx := table.GetColumnIndex(e.Name); idx >= 0 && idx < len(fullRowOpt[0]) {
					return fullRowOpt[0][idx]
				}
			}
			// Fallback to projected row
			if idx := table.GetColumnIndex(e.Name); idx >= 0 && idx < len(row) {
				return row[idx]
			}
		}
	case *query.QualifiedIdentifier:
		// First try exact match with table name
		for i, ci := range selectCols {
			if strings.EqualFold(ci.name, e.Column) && strings.EqualFold(ci.tableName, e.Table) && i < len(row) {
				return row[i]
			}
		}
		// Fallback to column-name-only match
		for i, ci := range selectCols {
			if strings.EqualFold(ci.name, e.Column) && i < len(row) {
				return row[i]
			}
		}
		// Try full row for qualified identifiers too
		if table != nil && len(fullRowOpt) > 0 && fullRowOpt[0] != nil {
			if idx := table.GetColumnIndex(e.Column); idx >= 0 && idx < len(fullRowOpt[0]) {
				return fullRowOpt[0][idx]
			}
		}
	case *query.NumberLiteral:
		return e.Value
	case *query.StringLiteral:
		return e.Value
	}

	// Fallback: try evaluateExpression with full row if available
	if table != nil && len(fullRowOpt) > 0 && fullRowOpt[0] != nil {
		val, err := evaluateExpression(c, fullRowOpt[0], table.Columns, expr, args)
		if err == nil {
			return val
		}
	}
	// Final fallback: try evaluateExpression with projected row
	if table != nil {
		val, err := evaluateExpression(c, row, table.Columns, expr, args)
		if err == nil {
			return val
		}
	}
	return nil
}

func (c *Catalog) evalWindowPartitions(rows [][]interface{}, colIdx int, we *query.WindowExpr, partitions map[string][]windowPartEntry, partitionOrder []string, selectCols []selectColInfo, table *TableDef, args []interface{}) {
		// Process each partition
		for _, pk := range partitionOrder {
		entries := partitions[pk]

		// Sort within partition by window ORDER BY
		if len(we.OrderBy) > 0 {
			sort.SliceStable(entries, func(a, b int) bool {
				for _, ob := range we.OrderBy {
					va := c.evalWindowExprOnRow(ob.Expr, entries[a].row, selectCols, table, args, entries[a].fullRow)
					vb := c.evalWindowExprOnRow(ob.Expr, entries[b].row, selectCols, table, args, entries[b].fullRow)
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

			// Compute window function values
			if c.evalWindowRankFunc(rows, colIdx, entries, we, selectCols, table, args) {
				continue
			}
			if c.evalWindowOffsetFunc(rows, colIdx, entries, we, selectCols, table, args) {
				continue
			}
			if c.evalWindowAggFunc(rows, colIdx, entries, we, selectCols, table, args) {
				continue
			}
		}
}

// evalWindowRankFunc handles ROW_NUMBER, RANK, DENSE_RANK window functions.
func (c *Catalog) evalWindowRankFunc(rows [][]interface{}, colIdx int, entries []windowPartEntry, we *query.WindowExpr, selectCols []selectColInfo, table *TableDef, args []interface{}) bool {
	switch we.Function {
	case "ROW_NUMBER":
		for i, entry := range entries {
			rows[entry.originalIdx][colIdx] = int64(i + 1)
		}
		return true

	case "RANK":
		rank := 1
		for i, entry := range entries {
			if i > 0 {
				changed := false
				for _, ob := range we.OrderBy {
					va := c.evalWindowExprOnRow(ob.Expr, entries[i-1].row, selectCols, table, args, entries[i-1].fullRow)
					vb := c.evalWindowExprOnRow(ob.Expr, entry.row, selectCols, table, args, entry.fullRow)
					if compareValues(va, vb) != 0 {
						changed = true
						break
					}
				}
				if changed {
					rank = i + 1
				}
			}
			rows[entry.originalIdx][colIdx] = int64(rank)
		}
		return true

	case "DENSE_RANK":
		denseRank := 1
		for i, entry := range entries {
			if i > 0 {
				changed := false
				for _, ob := range we.OrderBy {
					va := c.evalWindowExprOnRow(ob.Expr, entries[i-1].row, selectCols, table, args, entries[i-1].fullRow)
					vb := c.evalWindowExprOnRow(ob.Expr, entry.row, selectCols, table, args, entry.fullRow)
					if compareValues(va, vb) != 0 {
						changed = true
						break
					}
				}
				if changed {
					denseRank++
				}
			}
			rows[entry.originalIdx][colIdx] = int64(denseRank)
		}
		return true
	}
	return false
}

// evalWindowOffsetFunc handles LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTILE, NTH_VALUE window functions.
func (c *Catalog) evalWindowOffsetFunc(rows [][]interface{}, colIdx int, entries []windowPartEntry, we *query.WindowExpr, selectCols []selectColInfo, table *TableDef, args []interface{}) bool {
	switch we.Function {
	case "LAG":
		offset := 1
		if len(we.Args) >= 2 {
			if num, ok := we.Args[1].(*query.NumberLiteral); ok {
				offset = int(num.Value)
			}
		}
		var defaultVal interface{}
		if len(we.Args) >= 3 {
			defaultVal = c.evalWindowExprOnRow(we.Args[2], entries[0].row, selectCols, table, args, entries[0].fullRow)
		}
		for i, entry := range entries {
			if i-offset >= 0 {
				if len(we.Args) > 0 {
					rows[entry.originalIdx][colIdx] = c.evalWindowExprOnRow(we.Args[0], entries[i-offset].row, selectCols, table, args, entries[i-offset].fullRow)
				}
			} else {
				rows[entry.originalIdx][colIdx] = defaultVal
			}
		}
		return true

	case "LEAD":
		offset := 1
		if len(we.Args) >= 2 {
			if num, ok := we.Args[1].(*query.NumberLiteral); ok {
				offset = int(num.Value)
			}
		}
		var defaultVal interface{}
		if len(we.Args) >= 3 {
			defaultVal = c.evalWindowExprOnRow(we.Args[2], entries[0].row, selectCols, table, args, entries[0].fullRow)
		}
		for i, entry := range entries {
			if i+offset < len(entries) {
				if len(we.Args) > 0 {
					rows[entry.originalIdx][colIdx] = c.evalWindowExprOnRow(we.Args[0], entries[i+offset].row, selectCols, table, args, entries[i+offset].fullRow)
				}
			} else {
				rows[entry.originalIdx][colIdx] = defaultVal
			}
		}
		return true

	case "FIRST_VALUE":
		if len(entries) > 0 && len(we.Args) > 0 {
			firstVal := c.evalWindowExprOnRow(we.Args[0], entries[0].row, selectCols, table, args, entries[0].fullRow)
			for _, entry := range entries {
				rows[entry.originalIdx][colIdx] = firstVal
			}
		}
		return true

	case "LAST_VALUE":
		if len(entries) > 0 && len(we.Args) > 0 {
			lastVal := c.evalWindowExprOnRow(we.Args[0], entries[len(entries)-1].row, selectCols, table, args, entries[len(entries)-1].fullRow)
			for _, entry := range entries {
				rows[entry.originalIdx][colIdx] = lastVal
			}
		}
		return true

	case "NTILE":
		if len(we.Args) > 0 {
			if numLit, ok := we.Args[0].(*query.NumberLiteral); ok {
				numBuckets := int(numLit.Value)
				if numBuckets > 0 {
					partitionSize := len(entries) / numBuckets
					if len(entries)%numBuckets != 0 {
						partitionSize++
					}
					for i, entry := range entries {
						bucket := int64(i/partitionSize) + 1
						if bucket > int64(numBuckets) {
							bucket = int64(numBuckets)
						}
						rows[entry.originalIdx][colIdx] = bucket
					}
				}
			}
		}
		return true

	case "NTH_VALUE":
		if len(we.Args) >= 2 {
			if num, ok := we.Args[1].(*query.NumberLiteral); ok {
				n := int(num.Value)
				if n >= 1 && n <= len(entries) {
					nthVal := c.evalWindowExprOnRow(we.Args[0], entries[n-1].row, selectCols, table, args, entries[n-1].fullRow)
					for _, entry := range entries {
						rows[entry.originalIdx][colIdx] = nthVal
					}
				}
			}
		}
		return true
	}
	return false
}

// evalWindowAggFunc handles COUNT, SUM, AVG, MIN, MAX window aggregate functions.
func (c *Catalog) evalWindowAggFunc(rows [][]interface{}, colIdx int, entries []windowPartEntry, we *query.WindowExpr, selectCols []selectColInfo, table *TableDef, args []interface{}) bool {
	switch we.Function {
	case "COUNT":
		if len(we.OrderBy) > 0 {
			if len(we.Args) > 0 {
				if _, isStar := we.Args[0].(*query.StarExpr); isStar {
					for i, entry := range entries {
						rows[entry.originalIdx][colIdx] = int64(i + 1)
					}
				} else {
					count := int64(0)
					for _, entry := range entries {
						val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
						if val != nil {
							count++
						}
						rows[entry.originalIdx][colIdx] = count
					}
				}
			} else {
				for i, entry := range entries {
					rows[entry.originalIdx][colIdx] = int64(i + 1)
				}
			}
		} else if len(we.Args) > 0 {
			if _, isStar := we.Args[0].(*query.StarExpr); isStar {
				count := int64(len(entries))
				for _, entry := range entries {
					rows[entry.originalIdx][colIdx] = count
				}
			} else {
				count := int64(0)
				for _, entry := range entries {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil {
						count++
					}
				}
				for _, entry := range entries {
					rows[entry.originalIdx][colIdx] = count
				}
			}
		} else {
			count := int64(len(entries))
			for _, entry := range entries {
				rows[entry.originalIdx][colIdx] = count
			}
		}
		return true

	case "SUM":
		if len(we.Args) > 0 {
			if len(we.OrderBy) > 0 {
				sum := 0.0
				hasVal := false
				for _, entry := range entries {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil {
						if v, ok := toFloat64(val); ok {
							sum += v
							hasVal = true
						}
					}
					if hasVal {
						rows[entry.originalIdx][colIdx] = sum
					} else {
						rows[entry.originalIdx][colIdx] = nil
					}
				}
			} else {
				sum := 0.0
				hasVal := false
				for _, entry := range entries {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil {
						if v, ok := toFloat64(val); ok {
							sum += v
							hasVal = true
						}
					}
				}
				for _, entry := range entries {
					if hasVal {
						rows[entry.originalIdx][colIdx] = sum
					} else {
						rows[entry.originalIdx][colIdx] = nil
					}
				}
			}
		}
		return true

	case "AVG":
		if len(we.Args) > 0 && len(entries) > 0 {
			if len(we.OrderBy) > 0 {
				sum := 0.0
				count := 0
				for _, entry := range entries {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil {
						if v, ok := toFloat64(val); ok {
							sum += v
							count++
						}
					}
					if count > 0 {
						rows[entry.originalIdx][colIdx] = sum / float64(count)
					} else {
						rows[entry.originalIdx][colIdx] = nil
					}
				}
			} else {
				sum := 0.0
				count := 0
				for _, entry := range entries {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil {
						if v, ok := toFloat64(val); ok {
							sum += v
							count++
						}
					}
				}
				if count > 0 {
					avg := sum / float64(count)
					for _, entry := range entries {
						rows[entry.originalIdx][colIdx] = avg
					}
				} else {
					for _, entry := range entries {
						rows[entry.originalIdx][colIdx] = nil
					}
				}
			}
		}
		return true

	case "MIN":
		if len(we.Args) > 0 && len(entries) > 0 {
			if len(we.OrderBy) > 0 {
				var minVal interface{}
				for _, entry := range entries {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil && (minVal == nil || compareValues(val, minVal) < 0) {
						minVal = val
					}
					rows[entry.originalIdx][colIdx] = minVal
				}
			} else {
				minVal := c.evalWindowExprOnRow(we.Args[0], entries[0].row, selectCols, table, args, entries[0].fullRow)
				for _, entry := range entries[1:] {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if compareValues(val, minVal) < 0 {
						minVal = val
					}
				}
				for _, entry := range entries {
					rows[entry.originalIdx][colIdx] = minVal
				}
			}
		}
		return true

	case "MAX":
		if len(we.Args) > 0 && len(entries) > 0 {
			if len(we.OrderBy) > 0 {
				var maxVal interface{}
				for _, entry := range entries {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil && (maxVal == nil || compareValues(val, maxVal) > 0) {
						maxVal = val
					}
					rows[entry.originalIdx][colIdx] = maxVal
				}
			} else {
				maxVal := c.evalWindowExprOnRow(we.Args[0], entries[0].row, selectCols, table, args, entries[0].fullRow)
				for _, entry := range entries[1:] {
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if compareValues(val, maxVal) > 0 {
						maxVal = val
					}
				}
				for _, entry := range entries {
					rows[entry.originalIdx][colIdx] = maxVal
				}
			}
		}
		return true
	}
	return false
}
