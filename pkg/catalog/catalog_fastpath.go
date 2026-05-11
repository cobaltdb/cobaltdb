package catalog

import (
	"strings"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func (cat *Catalog) tryCountStarFastPath(stmt *query.SelectStmt, args []interface{}, queryTime time.Time) ([]string, [][]interface{}, bool) {
	// Only works for: single COUNT(*), single table, no JOINs, no GROUP BY, no subquery
	if len(stmt.Columns) != 1 || len(stmt.Joins) > 0 || len(stmt.GroupBy) > 0 || stmt.Having != nil {
		return nil, nil, false
	}
	if stmt.From.Subquery != nil || stmt.From.SubqueryStmt != nil {
		return nil, nil, false
	}

	// Check if it's COUNT(*) or COUNT(*) with alias
	col := stmt.Columns[0]
	if ae, ok := col.(*query.AliasExpr); ok {
		col = ae.Expr
	}
	fc, ok := col.(*query.FunctionCall)
	if !ok || !strings.EqualFold(fc.Name, "COUNT") || len(fc.Args) != 1 {
		return nil, nil, false
	}
	if _, isStar := fc.Args[0].(*query.StarExpr); !isStar {
		return nil, nil, false
	}

	// Get table and tree
	table, err := cat.getTableLocked(stmt.From.Name)
	if err != nil {
		return nil, nil, false
	}
	// Skip fast path for partitioned tables (data spread across partition trees)
	if table.Partition != nil {
		return nil, nil, false
	}
	tree, exists := cat.tableTrees[stmt.From.Name]
	if !exists {
		return nil, nil, false
	}

	// If there are pending buffered writes for this table, the fast path
	// will miss uncommitted INSERT/UPDATE/DELETE. Fall back to normal scan
	// so read-your-writes works correctly.
	if ts := cat.getCurrentTxn(); ts != nil && len(ts.pendingWrites) > 0 {
		if _, ok := ts.pendingWriteMap[stmt.From.Name]; ok {
			return nil, nil, false
		}
	}

	// Count rows by iterating B-tree keys
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return nil, nil, false
	}

	count := int64(0)
	hasWhere := stmt.Where != nil
	hasTemporalQuery := stmt.AsOf != nil

	for iter.HasNext() {
		key, valueData, err := iter.Next()
		if err != nil || key == nil {
			break
		}

		if !hasWhere && !hasTemporalQuery {
			// Fast path: no WHERE, no AS OF — skip all decoding, just count keys
			// Soft-deleted rows still have B-tree entries, so we need a minimal check
			// But for non-temporal queries, all live rows are visible
			if len(valueData) > 0 && valueData[0] == '{' {
				// Check if row has non-zero DeletedAt via quick byte scan
				if !bytesContainDeletedAt(valueData) {
					count++
					continue
				}
			}
			// Fallback to full decode for edge cases
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			if vrow.Version.DeletedAt == 0 {
				count++
			}
		} else if hasWhere {
			// WHERE clause requires row data
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			if !vrow.Version.isVisibleAt(queryTime) {
				continue
			}
			matched, err := evaluateWhere(cat, vrow.Data, table.Columns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
			count++
		} else {
			// AS OF temporal query — need version check
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			if !vrow.Version.isVisibleAt(queryTime) {
				continue
			}
			count++
		}
	}
	iter.Close()

	colName := "COUNT(*)"
	if ae, ok := stmt.Columns[0].(*query.AliasExpr); ok {
		colName = ae.Alias
	}

	return []string{colName}, [][]interface{}{{count}}, true
}

// bytesContainDeletedAt quickly checks if JSON data has a non-zero deleted_at.
// Returns true if "deleted_at" appears with a non-zero value (soft-deleted row).
// This avoids full JSON unmarshal for COUNT(*) fast path.
// trySimpleAggregateFastPath handles SELECT with only simple aggregates
// (SUM, AVG, MIN, MAX, COUNT) on a single table without GROUP BY/JOIN/subquery.
// Computes aggregates in a single streaming pass.
func (cat *Catalog) trySimpleAggregateFastPath(stmt *query.SelectStmt, args []interface{}) ([]string, [][]interface{}, bool) {
	// Requirements: no GROUP BY, no HAVING, no JOINs, no subquery, no ORDER BY, no LIMIT
	if len(stmt.GroupBy) > 0 || stmt.Having != nil || len(stmt.Joins) > 0 {
		return nil, nil, false
	}
	if stmt.From == nil || stmt.From.Subquery != nil || stmt.From.SubqueryStmt != nil {
		return nil, nil, false
	}
	if stmt.AsOf != nil || stmt.Limit != nil || len(stmt.OrderBy) > 0 {
		return nil, nil, false
	}

	// All columns must be simple aggregates (no DISTINCT, no expression args)
	type aggSpec struct {
		funcName string // SUM, AVG, MIN, MAX, COUNT
		colName  string // column name or "*"
		colIdx   int    // column index in table
		alias    string // result column name
	}
	var specs []aggSpec

	for _, col := range stmt.Columns {
		actual := col
		alias := ""
		if ae, ok := col.(*query.AliasExpr); ok {
			alias = ae.Alias
			actual = ae.Expr
		}
		fc, ok := actual.(*query.FunctionCall)
		if !ok || len(fc.Args) != 1 {
			return nil, nil, false
		}
		funcName := toUpperFast(fc.Name)
		if funcName != "SUM" && funcName != "AVG" && funcName != "MIN" && funcName != "MAX" && funcName != "COUNT" {
			return nil, nil, false
		}

		// DISTINCT aggregates are complex — fall back to normal path
		if fc.Distinct {
			return nil, nil, false
		}

		colName := "*"
		if _, isStar := fc.Args[0].(*query.StarExpr); !isStar {
			if id, ok := fc.Args[0].(*query.Identifier); ok {
				colName = id.Name
			} else {
				return nil, nil, false // expression arg, too complex
			}
		}
		if alias == "" {
			alias = funcName + "(" + colName + ")"
		}
		specs = append(specs, aggSpec{funcName: funcName, colName: colName, alias: alias})
	}

	if len(specs) == 0 {
		return nil, nil, false
	}

	// Get table
	table, err := cat.getTableLocked(stmt.From.Name)
	if err != nil {
		return nil, nil, false
	}
	if table.Partition != nil {
		return nil, nil, false
	}
	tree, exists := cat.tableTrees[stmt.From.Name]
	if !exists {
		return nil, nil, false
	}

	// If there are pending buffered writes for this table, the fast path
	// will miss uncommitted INSERT/UPDATE/DELETE. Fall back to normal scan
	// so read-your-writes works correctly.
	if ts := cat.getCurrentTxn(); ts != nil && len(ts.pendingWrites) > 0 {
		if _, ok := ts.pendingWriteMap[stmt.From.Name]; ok {
			return nil, nil, false
		}
	}

	// Resolve column indices
	for i := range specs {
		if specs[i].colName != "*" {
			specs[i].colIdx = table.GetColumnIndex(specs[i].colName)
			if specs[i].colIdx < 0 {
				return nil, nil, false
			}
		}
	}

	// Streaming aggregate state
	type aggState struct {
		count  int64
		sum    float64
		hasVal bool
		genVal interface{} // for MIN/MAX on non-numeric types (strings)
	}
	states := make([]aggState, len(specs))

	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return nil, nil, false
	}

	// Use byte-level fast path for SUM/AVG without WHERE (skip full JSON decode)
	canUseByteFastPath := stmt.Where == nil
	if canUseByteFastPath {
		for _, spec := range specs {
			if spec.funcName != "SUM" && spec.funcName != "AVG" && !(spec.funcName == "COUNT" && spec.colName == "*") {
				canUseByteFastPath = false
				break
			}
		}
	}

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			break
		}

		if canUseByteFastPath && len(valueData) > 0 && valueData[0] == '{' {
			if bytesContainDeletedAt(valueData) {
				continue
			}
			// Try byte-level extraction; fall back to full decode on failure
			allOK := true
			for i, spec := range specs {
				if spec.funcName == "COUNT" {
					states[i].count++
					continue
				}
				if fval, ok := extractColumnFloat64(valueData, spec.colIdx); ok {
					states[i].sum += fval
					states[i].count++
					states[i].hasVal = true
				} else {
					allOK = false
					break
				}
			}
			if allOK {
				continue
			}
			// Byte extraction failed for this row — undo partial updates and fall through
			for i, spec := range specs {
				if spec.funcName == "COUNT" {
					states[i].count--
				}
			}
		}

		vrow, err := decodeVersionedRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}
		if vrow.Version.DeletedAt > 0 {
			continue
		}
		row := vrow.Data

		// Apply WHERE
		if stmt.Where != nil {
			matched, err := evaluateWhere(cat, row, table.Columns, stmt.Where, args)
			if err != nil || !matched {
				continue
			}
		}

		// Update aggregates
		for i, spec := range specs {
			if spec.funcName == "COUNT" {
				if spec.colName == "*" {
					states[i].count++
				} else if spec.colIdx < len(row) && row[spec.colIdx] != nil {
					states[i].count++
				}
				continue
			}

			if spec.colIdx >= len(row) || row[spec.colIdx] == nil {
				continue
			}
			val := row[spec.colIdx]

			switch spec.funcName {
			case "SUM":
				if fval, ok := toFloat64Safe(val); ok {
					states[i].sum += fval
					states[i].hasVal = true
				}
			case "AVG":
				if fval, ok := toFloat64Safe(val); ok {
					states[i].sum += fval
					states[i].count++
					states[i].hasVal = true
				}
			case "MIN":
				if !states[i].hasVal {
					states[i].genVal = val
					states[i].hasVal = true
				} else if compareValues(val, states[i].genVal) < 0 {
					states[i].genVal = val
				}
			case "MAX":
				if !states[i].hasVal {
					states[i].genVal = val
					states[i].hasVal = true
				} else if compareValues(val, states[i].genVal) > 0 {
					states[i].genVal = val
				}
			}
		}
	}
	iter.Close()

	// Build result
	colNames := make([]string, len(specs))
	resultRow := make([]interface{}, len(specs))
	for i, spec := range specs {
		colNames[i] = spec.alias
		switch spec.funcName {
		case "COUNT":
			resultRow[i] = states[i].count
		case "SUM":
			if states[i].hasVal {
				resultRow[i] = states[i].sum
			}
		case "AVG":
			if states[i].hasVal && states[i].count > 0 {
				resultRow[i] = states[i].sum / float64(states[i].count)
			}
		case "MIN", "MAX":
			if states[i].hasVal {
				resultRow[i] = states[i].genVal
			}
		}
	}

	return colNames, [][]interface{}{resultRow}, true
}

// toFloat64Safe converts a value to float64, returning (value, true) or (0, false)
func toFloat64Safe(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	}
	return 0, false
}

func bytesContainDeletedAt(data []byte) bool {
	// Look for "deleted_at": followed by a non-zero digit
	needle := []byte(`"deleted_at":`)
	for i := 0; i <= len(data)-len(needle)-1; i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if data[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			// Check the value after the colon
			pos := i + len(needle)
			// Skip whitespace
			for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t') {
				pos++
			}
			// If value is 0, row is NOT deleted
			if pos < len(data) && data[pos] == '0' {
				return false
			}
			// Non-zero value means row IS deleted
			return true
		}
	}
	// No deleted_at field found — treat as not deleted (legacy format)
	return false
}

