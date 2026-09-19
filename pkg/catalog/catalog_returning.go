package catalog

import (
	"context"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// evaluateReturning evaluates RETURNING clause expressions against a row
// Returns expanded results (e.g., RETURNING * returns one value per column)
func (c *Catalog) evaluateReturning(returningExprs []query.Expression, row []interface{}, table *TableDef, args []interface{}) ([]interface{}, []string, error) {
	var result []interface{}
	var columns []string

	for _, expr := range returningExprs {
		vals, cols, err := c.evaluateReturningExpr(expr, row, table, args)
		if err != nil {
			return nil, nil, err
		}
		result = append(result, vals...)
		columns = append(columns, cols...)
	}

	return result, columns, nil
}

// evaluateReturningExpr evaluates a single RETURNING expression
// Returns (values, column names, error) - supports expanding * to multiple columns
func (c *Catalog) evaluateReturningExpr(expr query.Expression, row []interface{}, table *TableDef, args []interface{}) ([]interface{}, []string, error) {
	switch e := expr.(type) {
	case *query.ColumnRef:
		if e.Column == "*" {
			// Expand * to all columns
			var values []interface{}
			var cols []string
			for i, col := range table.Columns {
				if i < len(row) {
					values = append(values, row[i])
					cols = append(cols, col.Name)
				}
			}
			return values, cols, nil
		}
		// Single column reference
		colIdx := table.GetColumnIndex(e.Column)
		if colIdx >= 0 && colIdx < len(row) {
			return []interface{}{row[colIdx]}, []string{e.Column}, nil
		}
		return nil, nil, fmt.Errorf("column '%s' not found", e.Column)

	case *query.QualifiedIdentifier:
		colIdx := table.GetColumnIndex(e.Column)
		if colIdx >= 0 && colIdx < len(row) {
			return []interface{}{row[colIdx]}, []string{e.Column}, nil
		}
		return nil, nil, fmt.Errorf("column '%s' not found", e.Column)

	case *query.Identifier:
		colIdx := table.GetColumnIndex(e.Name)
		if colIdx >= 0 && colIdx < len(row) {
			return []interface{}{row[colIdx]}, []string{e.Name}, nil
		}
		return nil, nil, fmt.Errorf("column '%s' not found", e.Name)

	default:
		// For complex expressions, use the existing evaluation
		val, err := evaluateExpression(c, row, table.Columns, expr, args)
		if err != nil {
			return nil, nil, err
		}
		return []interface{}{val}, []string{fmt.Sprintf("expr_%p", expr)}, nil
	}
}

// getReturningColumns extracts column names from RETURNING expressions.
//
//lint:ignore U1000 retained for RETURNING compatibility tests and future planner hooks.
func (c *Catalog) getReturningColumns(returningExprs []query.Expression) []string {
	columns := make([]string, len(returningExprs))

	for i, expr := range returningExprs {
		switch e := expr.(type) {
		case *query.ColumnRef:
			if e.Column == "*" {
				columns[i] = "*"
			} else {
				columns[i] = e.Column
			}
		case *query.QualifiedIdentifier:
			columns[i] = e.Column
		case *query.Identifier:
			columns[i] = e.Name
		default:
			columns[i] = fmt.Sprintf("expr_%d", i)
		}
	}

	return columns
}

// setLastReturning sets the last RETURNING results under returningMu.
func (c *Catalog) setLastReturning(rows [][]interface{}, cols []string) {
	c.returningMu.Lock()
	defer c.returningMu.Unlock()
	c.lastReturningRows = rows
	c.lastReturningColumns = cols
}

// returningCaptureKey is the context key for a per-statement RETURNING capture.
type returningCaptureKey struct{}

// ReturningCapture receives one statement's RETURNING results directly.
//
// The legacy lastReturningRows slot is last-writer-wins catalog-global state:
// two concurrent RETURNING statements overwrite each other's rows between the
// statement's write and the caller's read. Attaching a capture to the
// statement's context gives the caller exact statement affinity.
type ReturningCapture struct {
	rows    [][]interface{}
	columns []string
	set     bool
}

// Results returns the captured RETURNING rows and column names.
func (rc *ReturningCapture) Results() ([][]interface{}, []string) {
	return rc.rows, rc.columns
}

// WithReturningCapture returns a context that directs RETURNING results for
// statements executed with it into cap.
func WithReturningCapture(ctx context.Context, cap *ReturningCapture) context.Context {
	return context.WithValue(ctx, returningCaptureKey{}, cap)
}

func returningCaptureFrom(ctx context.Context) *ReturningCapture {
	if ctx == nil {
		return nil
	}
	cap, _ := ctx.Value(returningCaptureKey{}).(*ReturningCapture)
	return cap
}

// storeReturning records RETURNING results for the statement executing with
// ctx: into the per-statement capture when one is attached (statement
// affinity), and into the legacy global slot for GetLastReturningRows
// readers. Every setLastReturning call site must route through this so the
// capture stays in sync with the slot.
func (c *Catalog) storeReturning(ctx context.Context, rows [][]interface{}, cols []string) {
	if capture := returningCaptureFrom(ctx); capture != nil {
		capture.rows = rows
		capture.columns = cols
		capture.set = true
	}
	c.setLastReturning(rows, cols)
}

// GetLastReturningRows returns the results from the last RETURNING clause
func (c *Catalog) GetLastReturningRows() [][]interface{} {
	c.returningMu.Lock()
	defer c.returningMu.Unlock()
	return c.lastReturningRows
}

// GetLastReturningColumns returns the column names from the last RETURNING clause
func (c *Catalog) GetLastReturningColumns() []string {
	c.returningMu.Lock()
	defer c.returningMu.Unlock()
	return c.lastReturningColumns
}

// ClearReturning clears the last RETURNING results
func (c *Catalog) ClearReturning() {
	c.returningMu.Lock()
	defer c.returningMu.Unlock()
	c.lastReturningRows = nil
	c.lastReturningColumns = nil
}
