package catalog

import (
	"fmt"
	"sort"
	"strings"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

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
		type partitionEntry struct {
			originalIdx int
			row         []interface{}
			fullRow     []interface{} // full table row for evaluating non-SELECT columns
		}
		partitions := make(map[string][]partitionEntry)
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
					keyParts = append(keyParts, fmt.Sprintf("%v", val))
				}
				partKey = strings.Join(keyParts, "|")
			}

			if _, exists := partitions[partKey]; !exists {
				partitionOrder = append(partitionOrder, partKey)
			}
			partitions[partKey] = append(partitions[partKey], partitionEntry{originalIdx: i, row: row, fullRow: fRow})
		}

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
			switch we.Function {
			case "ROW_NUMBER":
				for i, entry := range entries {
					rows[entry.originalIdx][colIdx] = int64(i + 1)
				}

			case "RANK":
				rank := 1
				for i, entry := range entries {
					if i > 0 {
						// Check if ORDER BY values changed from previous row
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

			case "FIRST_VALUE":
				if len(entries) > 0 && len(we.Args) > 0 {
					firstVal := c.evalWindowExprOnRow(we.Args[0], entries[0].row, selectCols, table, args, entries[0].fullRow)
					for _, entry := range entries {
						rows[entry.originalIdx][colIdx] = firstVal
					}
				}

			case "LAST_VALUE":
				if len(entries) > 0 && len(we.Args) > 0 {
					lastVal := c.evalWindowExprOnRow(we.Args[0], entries[len(entries)-1].row, selectCols, table, args, entries[len(entries)-1].fullRow)
					for _, entry := range entries {
						rows[entry.originalIdx][colIdx] = lastVal
					}
				}

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

			case "COUNT":
				if len(we.OrderBy) > 0 {
					// Running COUNT with ORDER BY
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

			case "SUM":
				if len(we.Args) > 0 {
					if len(we.OrderBy) > 0 {
						// Running SUM with ORDER BY
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
						// Partition-wide SUM
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

			case "AVG":
				if len(we.Args) > 0 && len(entries) > 0 {
					if len(we.OrderBy) > 0 {
						// Running AVG with ORDER BY
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
						// Partition-wide AVG
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

			case "MIN":
				if len(we.Args) > 0 && len(entries) > 0 {
					if len(we.OrderBy) > 0 {
						// Running MIN with ORDER BY
						var minVal interface{}
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil && (minVal == nil || compareValues(val, minVal) < 0) {
								minVal = val
							}
							rows[entry.originalIdx][colIdx] = minVal
						}
					} else {
						// Partition-wide MIN
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

			case "MAX":
				if len(we.Args) > 0 && len(entries) > 0 {
					if len(we.OrderBy) > 0 {
						// Running MAX with ORDER BY
						var maxVal interface{}
						for _, entry := range entries {
							val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
							if val != nil && (maxVal == nil || compareValues(val, maxVal) > 0) {
								maxVal = val
							}
							rows[entry.originalIdx][colIdx] = maxVal
						}
					} else {
						// Partition-wide MAX
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
			}
		}
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