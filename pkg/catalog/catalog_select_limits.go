package catalog

import (
	"fmt"
	"math"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func validateSelectBounds(c *Catalog, stmt *query.SelectStmt, args []interface{}) error {
	limit, hasLimit, err := evaluateSelectBound(c, stmt.Limit, args, "LIMIT")
	if err != nil {
		return err
	}
	offset, hasOffset, err := evaluateSelectBound(c, stmt.Offset, args, "OFFSET")
	if err != nil {
		return err
	}
	if hasLimit && hasOffset && limit > 0 && offset > math.MaxInt-limit {
		return fmt.Errorf("LIMIT/OFFSET combined value too large")
	}
	return nil
}

func evaluateSelectBound(c *Catalog, expr query.Expression, args []interface{}, name string) (int, bool, error) {
	if expr == nil {
		return 0, false, nil
	}
	value, err := evaluateExpression(c, nil, nil, expr, args)
	if err != nil {
		return 0, false, fmt.Errorf("%s expression error: %w", name, err)
	}
	bound, ok := exactNonNegativeInt(value)
	if !ok {
		return 0, false, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return bound, true, nil
}

func exactNonNegativeInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, false
		}
		return v, true
	case int64:
		if v < 0 || v > int64(math.MaxInt) {
			return 0, false
		}
		return int(v), true
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > float64(math.MaxInt) || math.Trunc(v) != v {
			return 0, false
		}
		return int(v), true
	default:
		return 0, false
	}
}
