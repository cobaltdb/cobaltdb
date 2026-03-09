package catalog

import (
	"fmt"
	"strings"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func (c *Catalog) ExecuteCTE(stmt *query.SelectStmtWithCTE, args []interface{}) ([]string, [][]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Store original views temporarily
	originalViews := make(map[string]*query.SelectStmt)

	// Initialize CTE results map if needed
	if c.cteResults == nil {
		c.cteResults = make(map[string]*cteResultSet)
	}
	// Track which CTE results we create so we can clean up
	var createdCTEResults []string

	// Register CTEs as temporary views or execute recursive CTEs
	for _, cte := range stmt.CTEs {
		cteName := strings.ToLower(cte.Name)

		// Check for duplicates
		if _, exists := originalViews[cte.Name]; exists {
			for name, view := range originalViews {
				c.views[name] = view
			}
			return nil, nil, fmt.Errorf("duplicate CTE name: %s", cte.Name)
		}

		// Save original view if exists
		if orig, exists := c.views[cte.Name]; exists {
			originalViews[cte.Name] = orig
		}

		// Check if this is a recursive CTE with UNION ALL
		if stmt.IsRecursive {
			if unionStmt, ok := cte.Query.(*query.UnionStmt); ok {
				// Execute recursive CTE
				err := c.executeRecursiveCTE(cte.Name, cteName, cte.Columns, unionStmt, args)
				if err != nil {
					// Clean up on error
					for _, name := range createdCTEResults {
						delete(c.cteResults, name)
					}
					return nil, nil, fmt.Errorf("recursive CTE %s: %w", cte.Name, err)
				}
				createdCTEResults = append(createdCTEResults, cteName)
				continue
			}
		}

		// Non-recursive CTE: register as a view or execute as union
		if selectQuery, ok := cte.Query.(*query.SelectStmt); ok {
			c.views[cte.Name] = selectQuery
			// Materialize CTE results so subsequent CTEs can reference them
			if len(stmt.CTEs) > 1 {
				cols, rows, err := c.selectLocked(selectQuery, args)
				if err == nil {
					c.cteResults[cteName] = &cteResultSet{columns: cols, rows: rows}
					createdCTEResults = append(createdCTEResults, cteName)
				}
			}
		} else if unionQuery, ok := cte.Query.(*query.UnionStmt); ok {
			// Execute UNION query and store results in cteResults
			cols, rows, err := c.executeCTEUnion(unionQuery, args)
			if err != nil {
				// Clean up on error
				for _, name := range createdCTEResults {
					delete(c.cteResults, name)
				}
				return nil, nil, fmt.Errorf("CTE %s: %w", cte.Name, err)
			}
			if len(cols) == 0 && len(cte.Columns) > 0 {
				cols = cte.Columns
			}
			c.cteResults[cteName] = &cteResultSet{
				columns: cols,
				rows:    rows,
			}
			createdCTEResults = append(createdCTEResults, cteName)
		}
	}

	// Execute the main query (already holding lock)
	columns, rows, err := c.selectLocked(stmt.Select, args)

	// Restore original views and clean up CTE results
	for _, cte := range stmt.CTEs {
		name := cte.Name
		if orig, exists := originalViews[name]; exists {
			c.views[name] = orig
		} else {
			delete(c.views, name)
		}
	}
	for _, name := range createdCTEResults {
		delete(c.cteResults, name)
	}

	return columns, rows, err
}

func (c *Catalog) executeDerivedTable(ref *query.TableRef, args []interface{}) ([]string, [][]interface{}, error) {
	if ref.SubqueryStmt != nil {
		switch s := ref.SubqueryStmt.(type) {
		case *query.UnionStmt:
			return c.executeCTEUnion(s, args)
		case *query.SelectStmt:
			return c.selectLocked(s, args)
		default:
			return nil, nil, fmt.Errorf("unsupported derived table statement type: %T", ref.SubqueryStmt)
		}
	}
	if ref.Subquery != nil {
		return c.selectLocked(ref.Subquery, args)
	}
	return nil, nil, fmt.Errorf("derived table has no subquery")
}

func (c *Catalog) executeCTEUnion(stmt *query.UnionStmt, args []interface{}) ([]string, [][]interface{}, error) {
	// Execute left side
	var leftCols []string
	var leftRows [][]interface{}
	var err error
	switch l := stmt.Left.(type) {
	case *query.SelectStmt:
		leftCols, leftRows, err = c.selectLocked(l, args)
	case *query.UnionStmt:
		leftCols, leftRows, err = c.executeCTEUnion(l, args)
	default:
		return nil, nil, fmt.Errorf("unsupported left side of UNION in CTE: %T", stmt.Left)
	}
	if err != nil {
		return nil, nil, err
	}

	// Execute right side
	var rightRows [][]interface{}
	rightCols, rightRows, err := c.selectLocked(stmt.Right, args)
	if err != nil {
		return nil, nil, err
	}
	_ = rightCols

	// Combine results based on set operation type
	var allRows [][]interface{}

	switch stmt.Op {
	case query.SetOpIntersect:
		// INTERSECT - only rows that appear in both sides
		rightSet := make(map[string]bool)
		for _, row := range rightRows {
			rightSet[rowKeyForDedup(row)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftRows {
			key := rowKeyForDedup(row)
			if rightSet[key] {
				if stmt.All || !seen[key] {
					seen[key] = true
					allRows = append(allRows, row)
				}
			}
		}
	case query.SetOpExcept:
		// EXCEPT - rows in left but not in right
		rightSet := make(map[string]bool)
		for _, row := range rightRows {
			rightSet[rowKeyForDedup(row)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftRows {
			key := rowKeyForDedup(row)
			if !rightSet[key] {
				if stmt.All || !seen[key] {
					seen[key] = true
					allRows = append(allRows, row)
				}
			}
		}
	default:
		// UNION / UNION ALL
		if stmt.All {
			allRows = make([][]interface{}, 0, len(leftRows)+len(rightRows))
			allRows = append(allRows, leftRows...)
			allRows = append(allRows, rightRows...)
		} else {
			seen := make(map[string]bool)
			allRows = make([][]interface{}, 0, len(leftRows)+len(rightRows))
			for _, row := range leftRows {
				key := rowKeyForDedup(row)
				if !seen[key] {
					seen[key] = true
					allRows = append(allRows, row)
				}
			}
			for _, row := range rightRows {
				key := rowKeyForDedup(row)
				if !seen[key] {
					seen[key] = true
					allRows = append(allRows, row)
				}
			}
		}
	}

	return leftCols, allRows, nil
}

func (c *Catalog) executeRecursiveCTE(name string, nameLower string, cteColumns []string, unionStmt *query.UnionStmt, args []interface{}) error {
	const maxDepth = 1000

	// The left side is the anchor member
	anchorStmt, ok := unionStmt.Left.(*query.SelectStmt)
	if !ok {
		return fmt.Errorf("recursive CTE anchor must be a SELECT statement")
	}

	// The right side is the recursive member
	recursiveStmt := unionStmt.Right

	// Step 1: Execute anchor member
	anchorCols, anchorRows, err := c.selectLocked(anchorStmt, args)
	if err != nil {
		return fmt.Errorf("anchor member: %w", err)
	}

	// Use CTE-defined column names if provided, otherwise use anchor's column names
	cteCols := anchorCols
	if len(cteColumns) > 0 && len(cteColumns) <= len(anchorCols) {
		cteCols = make([]string, len(anchorCols))
		copy(cteCols, anchorCols)
		for i, col := range cteColumns {
			cteCols[i] = col
		}
	}

	// Step 2: Iteratively execute recursive member
	// Accumulate all results
	allRows := make([][]interface{}, len(anchorRows))
	copy(allRows, anchorRows)

	// Working table: rows from the last iteration
	workingRows := anchorRows

	for depth := 0; depth < maxDepth; depth++ {
		if len(workingRows) == 0 {
			break
		}

		// Store current working set as CTE result so recursive member can reference it
		c.cteResults[nameLower] = &cteResultSet{
			columns: cteCols,
			rows:    workingRows,
		}

		// Execute recursive member
		_, newRows, err := c.selectLocked(recursiveStmt, args)
		if err != nil {
			return fmt.Errorf("recursive member (depth %d): %w", depth, err)
		}

		if len(newRows) == 0 {
			break
		}

		allRows = append(allRows, newRows...)
		workingRows = newRows
	}

	// Store final accumulated results
	c.cteResults[nameLower] = &cteResultSet{
		columns: cteCols,
		rows:    allRows,
	}

	return nil
}