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
	orderByVals []interface{} // pre-computed ORDER BY values for peer detection
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

	// Columns that nest window functions inside an expression (e.g.
	// SUM(x) OVER () + 1): compute each window value, then re-evaluate the
	// surrounding expression with those values substituted in.
	for colIdx, ci := range selectCols {
		if len(ci.embeddedWindows) == 0 || ci.originalExpr == nil {
			continue
		}
		perRow := make(map[*query.WindowExpr][]interface{}, len(ci.embeddedWindows))
		for _, we := range ci.embeddedWindows {
			perRow[we] = c.computeWindowExprColumn(we, rows, fullRows, selectCols, table, args)
		}
		for i := range rows {
			evalRow := rows[i]
			evalCols := table.Columns
			if i < len(fullRows) && fullRows[i] != nil {
				evalRow = fullRows[i]
			}
			ctx := NewEvalContext(c, evalRow, evalCols, args)
			wv := make(map[*query.WindowExpr]interface{}, len(ci.embeddedWindows))
			for _, we := range ci.embeddedWindows {
				wv[we] = perRow[we][i]
			}
			ctx.windowValues = wv
			if val, err := ctx.evaluate(ci.originalExpr); err == nil {
				rows[i][colIdx] = val
			}
		}
	}

	return rows
}

// computeWindowExprColumn computes the per-row values of a single window
// expression over the given rows, returning a slice indexed by row position.
func (c *Catalog) computeWindowExprColumn(we *query.WindowExpr, rows, fullRows [][]interface{}, selectCols []selectColInfo, table *TableDef, args []interface{}) []interface{} {
	partitions := make(map[string][]windowPartEntry)
	partitionOrder := []string{}
	for i, row := range rows {
		var fRow []interface{}
		if i < len(fullRows) {
			fRow = fullRows[i]
		}
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

	scratch := make([][]interface{}, len(rows))
	for i := range scratch {
		scratch[i] = make([]interface{}, 1)
	}
	c.evalWindowPartitions(scratch, 0, we, partitions, partitionOrder, selectCols, table, args)
	out := make([]interface{}, len(rows))
	for i := range scratch {
		out[i] = scratch[i][0]
	}
	return out
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
			// Pre-compute ORDER BY values for peer detection. This avoids
			// re-evaluating ORDER BY expressions in JOIN/CTE contexts where
			// column resolution may differ from sort-time resolution.
			for i := range entries {
				entries[i].orderByVals = make([]interface{}, len(we.OrderBy))
				for j, ob := range we.OrderBy {
					entries[i].orderByVals[j] = c.evalWindowExprOnRow(ob.Expr, entries[i].row, selectCols, table, args, entries[i].fullRow)
				}
			}
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
				for j := range we.OrderBy {
					if compareValues(entries[i-1].orderByVals[j], entry.orderByVals[j]) != 0 {
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
				for j := range we.OrderBy {
					if compareValues(entries[i-1].orderByVals[j], entry.orderByVals[j]) != 0 {
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

	case "PERCENT_RANK":
		// (rank - 1) / (n - 1); 0 for a single row.
		n := len(entries)
		rank := 1
		for i, entry := range entries {
			if i > 0 && !c.windowSamePeer(we, entries[i-1], entry, selectCols, table, args) {
				rank = i + 1
			}
			var pr float64
			if n > 1 {
				pr = float64(rank-1) / float64(n-1)
			}
			rows[entry.originalIdx][colIdx] = pr
		}
		return true

	case "CUME_DIST":
		// (rows ordered <= current, i.e. through the last peer) / n.
		n := len(entries)
		for i := 0; i < n; {
			j := i
			for j+1 < n && c.windowSamePeer(we, entries[i], entries[j+1], selectCols, table, args) {
				j++
			}
			cd := float64(j+1) / float64(n)
			for k := i; k <= j; k++ {
				rows[entries[k].originalIdx][colIdx] = cd
			}
			i = j + 1
		}
		return true
	}
	return false
}

// windowSamePeer reports whether two entries share the same ORDER BY values
// (i.e. they are peers for ranking purposes). With no ORDER BY, all rows peer.
// Uses pre-computed orderByVals captured after sorting, avoiding re-evaluation
// that can produce wrong results in JOIN/CTE contexts.
func (c *Catalog) windowSamePeer(we *query.WindowExpr, e1, e2 windowPartEntry, selectCols []selectColInfo, table *TableDef, args []interface{}) bool {
	for i := range we.OrderBy {
		if compareValues(e1.orderByVals[i], e2.orderByVals[i]) != 0 {
			return false
		}
	}
	return true
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
			if len(we.OrderBy) > 0 && we.Frame == nil {
				// Default frame is RANGE UNBOUNDED PRECEDING AND CURRENT ROW, so
				// LAST_VALUE is the running last value: the value of the last peer
				// of the current row (peers share the ORDER BY key).
				n := len(entries)
				for i := 0; i < n; {
					j := i
					for j+1 < n && c.windowSamePeer(we, entries[i], entries[j+1], selectCols, table, args) {
						j++
					}
					lastVal := c.evalWindowExprOnRow(we.Args[0], entries[j].row, selectCols, table, args, entries[j].fullRow)
					for k := i; k <= j; k++ {
						rows[entries[k].originalIdx][colIdx] = lastVal
					}
					i = j + 1
				}
			} else {
				lastVal := c.evalWindowExprOnRow(we.Args[0], entries[len(entries)-1].row, selectCols, table, args, entries[len(entries)-1].fullRow)
				for _, entry := range entries {
					rows[entry.originalIdx][colIdx] = lastVal
				}
			}
		}
		return true

	case "NTILE":
		if len(we.Args) > 0 {
			if numLit, ok := we.Args[0].(*query.NumberLiteral); ok {
				numBuckets := int(numLit.Value)
				if numBuckets > 0 {
					// Standard SQL: the first (n % buckets) buckets are one row
					// larger than the rest. Distribute the remainder into the
					// leading buckets instead of using a single ceil-size bucket
					// (which starves the trailing buckets).
					n := len(entries)
					base := n / numBuckets
					rem := n % numBuckets
					largeCount := rem * (base + 1) // rows covered by the larger buckets
					for i, entry := range entries {
						var bucket int64
						if i < largeCount {
							bucket = int64(i/(base+1)) + 1
						} else {
							bucket = int64(rem) + int64((i-largeCount)/base) + 1
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
					if len(we.OrderBy) > 0 && we.Frame == nil {
						// Default frame ends at the current row's last peer; the
						// nth value is only visible once the frame has grown to
						// include index n-1, otherwise the result is NULL.
						total := len(entries)
						for i := 0; i < total; {
							j := i
							for j+1 < total && c.windowSamePeer(we, entries[i], entries[j+1], selectCols, table, args) {
								j++
							}
							for k := i; k <= j; k++ {
								if j >= n-1 {
									rows[entries[k].originalIdx][colIdx] = nthVal
								} else {
									rows[entries[k].originalIdx][colIdx] = nil
								}
							}
							i = j + 1
						}
					} else {
						for _, entry := range entries {
							rows[entry.originalIdx][colIdx] = nthVal
						}
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
	// Explicit window frame (ROWS/RANGE BETWEEN ...) takes a dedicated path so
	// the default (frame-less) running/partition behavior below is untouched.
	if we.Frame != nil {
		if c.evalWindowAggFrame(rows, colIdx, entries, we, selectCols, table, args) {
			return true
		}
	}
	switch we.Function {
	case "COUNT":
		isStar := len(we.Args) == 0
		if len(we.Args) > 0 {
			_, isStar = we.Args[0].(*query.StarExpr)
		}
		if len(we.OrderBy) > 0 {
			count := int64(0)
			i := 0
			for i < len(entries) {
				j := i
				for j+1 < len(entries) && c.windowSamePeer(we, entries[i], entries[j+1], selectCols, table, args) {
					j++
				}
				for k := i; k <= j; k++ {
					if c.windowEntryPassesFilter(we, entries[k], selectCols, table, args) {
						if isStar {
							count++
						} else {
							val := c.evalWindowExprOnRow(we.Args[0], entries[k].row, selectCols, table, args, entries[k].fullRow)
							if val != nil {
								count++
							}
						}
					}
				}
				for k := i; k <= j; k++ {
					rows[entries[k].originalIdx][colIdx] = count
				}
				i = j + 1
			}
		} else {
			count := int64(0)
			for _, entry := range entries {
				if c.windowEntryPassesFilter(we, entry, selectCols, table, args) {
					if isStar {
						count++
						continue
					}
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil {
						count++
					}
				}
			}
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
				i := 0
				for i < len(entries) {
					j := i
					for j+1 < len(entries) && c.windowSamePeer(we, entries[i], entries[j+1], selectCols, table, args) {
						j++
					}
					for k := i; k <= j; k++ {
						if c.windowEntryPassesFilter(we, entries[k], selectCols, table, args) {
							val := c.evalWindowExprOnRow(we.Args[0], entries[k].row, selectCols, table, args, entries[k].fullRow)
							if val != nil {
								if v, ok := toFloat64(val); ok {
									sum += v
									hasVal = true
								}
							}
						}
					}
					for k := i; k <= j; k++ {
						if hasVal {
							rows[entries[k].originalIdx][colIdx] = sum
						} else {
							rows[entries[k].originalIdx][colIdx] = nil
						}
					}
					i = j + 1
				}
			} else {
				sum := 0.0
				hasVal := false
				for _, entry := range entries {
					if c.windowEntryPassesFilter(we, entry, selectCols, table, args) {
						val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
						if val != nil {
							if v, ok := toFloat64(val); ok {
								sum += v
								hasVal = true
							}
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
				i := 0
				for i < len(entries) {
					j := i
					for j+1 < len(entries) && c.windowSamePeer(we, entries[i], entries[j+1], selectCols, table, args) {
						j++
					}
					for k := i; k <= j; k++ {
						if c.windowEntryPassesFilter(we, entries[k], selectCols, table, args) {
							val := c.evalWindowExprOnRow(we.Args[0], entries[k].row, selectCols, table, args, entries[k].fullRow)
							if val != nil {
								if v, ok := toFloat64(val); ok {
									sum += v
									count++
								}
							}
						}
					}
					for k := i; k <= j; k++ {
						if count > 0 {
							rows[entries[k].originalIdx][colIdx] = sum / float64(count)
						} else {
							rows[entries[k].originalIdx][colIdx] = nil
						}
					}
					i = j + 1
				}
			} else {
				sum := 0.0
				count := 0
				for _, entry := range entries {
					if c.windowEntryPassesFilter(we, entry, selectCols, table, args) {
						val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
						if val != nil {
							if v, ok := toFloat64(val); ok {
								sum += v
								count++
							}
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
				i := 0
				for i < len(entries) {
					j := i
					for j+1 < len(entries) && c.windowSamePeer(we, entries[i], entries[j+1], selectCols, table, args) {
						j++
					}
					for k := i; k <= j; k++ {
						if c.windowEntryPassesFilter(we, entries[k], selectCols, table, args) {
							val := c.evalWindowExprOnRow(we.Args[0], entries[k].row, selectCols, table, args, entries[k].fullRow)
							if val != nil && (minVal == nil || compareValues(val, minVal) < 0) {
								minVal = val
							}
						}
					}
					for k := i; k <= j; k++ {
						rows[entries[k].originalIdx][colIdx] = minVal
					}
					i = j + 1
				}
			} else {
				var minVal interface{}
				for _, entry := range entries {
					if !c.windowEntryPassesFilter(we, entry, selectCols, table, args) {
						continue
					}
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil && (minVal == nil || compareValues(val, minVal) < 0) {
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
				i := 0
				for i < len(entries) {
					j := i
					for j+1 < len(entries) && c.windowSamePeer(we, entries[i], entries[j+1], selectCols, table, args) {
						j++
					}
					for k := i; k <= j; k++ {
						if c.windowEntryPassesFilter(we, entries[k], selectCols, table, args) {
							val := c.evalWindowExprOnRow(we.Args[0], entries[k].row, selectCols, table, args, entries[k].fullRow)
							if val != nil && (maxVal == nil || compareValues(val, maxVal) > 0) {
								maxVal = val
							}
						}
					}
					for k := i; k <= j; k++ {
						rows[entries[k].originalIdx][colIdx] = maxVal
					}
					i = j + 1
				}
			} else {
				var maxVal interface{}
				for _, entry := range entries {
					if !c.windowEntryPassesFilter(we, entry, selectCols, table, args) {
						continue
					}
					val := c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
					if val != nil && (maxVal == nil || compareValues(val, maxVal) > 0) {
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

func (c *Catalog) windowEntryPassesFilter(we *query.WindowExpr, entry windowPartEntry, selectCols []selectColInfo, table *TableDef, args []interface{}) bool {
	if we.Filter == nil {
		return true
	}
	val := c.evalWindowExprOnRow(we.Filter, entry.row, selectCols, table, args, entry.fullRow)
	return val != nil && toBool(val)
}

// frameRowBound resolves one frame bound to a physical (inclusive) row index for
// the row at position i within a partition of n rows.
func frameRowBound(b *query.WindowFrameBound, i, n int) int {
	if b == nil {
		return i
	}
	switch b.Type {
	case "UNBOUNDED_PRECEDING":
		return 0
	case "PRECEDING":
		return i - b.Offset
	case "CURRENT_ROW":
		return i
	case "FOLLOWING":
		return i + b.Offset
	case "UNBOUNDED_FOLLOWING":
		return n - 1
	}
	return i
}

// evalWindowAggFrame computes SUM/AVG/COUNT/MIN/MAX over an explicit window frame
// (ROWS/RANGE BETWEEN ...). RANGE bounds are approximated with physical row
// offsets. Returns false for functions it does not handle.
func (c *Catalog) evalWindowAggFrame(rows [][]interface{}, colIdx int, entries []windowPartEntry, we *query.WindowExpr, selectCols []selectColInfo, table *TableDef, args []interface{}) bool {
	switch we.Function {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
	default:
		return false
	}

	n := len(entries)
	isStar := false
	if len(we.Args) > 0 {
		if _, ok := we.Args[0].(*query.StarExpr); ok {
			isStar = true
		}
	}

	// Pre-extract the argument value per ordered entry once.
	vals := make([]interface{}, n)
	filtered := make([]bool, n)
	if len(we.Args) > 0 && !isStar {
		for i, entry := range entries {
			vals[i] = c.evalWindowExprOnRow(we.Args[0], entry.row, selectCols, table, args, entry.fullRow)
		}
	}
	for i, entry := range entries {
		filtered[i] = c.windowEntryPassesFilter(we, entry, selectCols, table, args)
	}

	for i, entry := range entries {
		start := frameRowBound(we.Frame.Start, i, n)
		end := frameRowBound(we.Frame.End, i, n)
		if start < 0 {
			start = 0
		}
		if end > n-1 {
			end = n - 1
		}

		var result interface{}
		switch we.Function {
		case "COUNT":
			cnt := int64(0)
			for j := start; j <= end; j++ {
				if !filtered[j] {
					continue
				}
				if isStar || len(we.Args) == 0 || vals[j] != nil {
					cnt++
				}
			}
			result = cnt
		case "SUM":
			sum := 0.0
			has := false
			for j := start; j <= end; j++ {
				if !filtered[j] {
					continue
				}
				if vals[j] != nil {
					if f, ok := toFloat64(vals[j]); ok {
						sum += f
						has = true
					}
				}
			}
			if has {
				result = sum
			}
		case "AVG":
			sum := 0.0
			cnt := 0
			for j := start; j <= end; j++ {
				if !filtered[j] {
					continue
				}
				if vals[j] != nil {
					if f, ok := toFloat64(vals[j]); ok {
						sum += f
						cnt++
					}
				}
			}
			if cnt > 0 {
				result = sum / float64(cnt)
			}
		case "MIN":
			for j := start; j <= end; j++ {
				if !filtered[j] {
					continue
				}
				if vals[j] != nil && (result == nil || compareValues(vals[j], result) < 0) {
					result = vals[j]
				}
			}
		case "MAX":
			for j := start; j <= end; j++ {
				if !filtered[j] {
					continue
				}
				if vals[j] != nil && (result == nil || compareValues(vals[j], result) > 0) {
					result = vals[j]
				}
			}
		}
		rows[entry.originalIdx][colIdx] = result
	}
	return true
}
