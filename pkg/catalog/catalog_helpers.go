// Package catalog provides table management and query execution for CobaltDB.
package catalog

import (
	"fmt"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"math"
	"strconv"
)

func toInt(v interface{}) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		if val > int64(math.MaxInt) || val < int64(math.MinInt) {
			return 0, false // Overflow
		}
		return int(val), true
	case float64:
		if val > float64(math.MaxInt) || val < float64(math.MinInt) {
			return 0, false // Overflow
		}
		return int(val), true
	default:
		return 0, false
	}
}

func toNumber(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case string:
		n, _ := strconv.ParseFloat(val, 64)
		return n
	default:
		return 0
	}
}

func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int:
		return val != 0
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != ""
	default:
		return false
	}
}

func toBoolNullable(v interface{}) (bool, bool) {
	if v == nil {
		return false, true
	}
	return toBool(v), false
}

func isStarArg(e query.Expression) bool {
	_, ok := e.(*query.StarExpr)
	return ok
}

func valueToLiteral(v interface{}) query.Expression {
	if v == nil {
		return &query.NullLiteral{}
	}
	switch val := v.(type) {
	case string:
		return &query.StringLiteral{Value: val}
	case bool:
		return &query.BooleanLiteral{Value: val}
	default:
		_ = val
		return &query.NumberLiteral{Value: toNumber(v)}
	}
}

func valueToExpr(val interface{}) query.Expression {
	if val == nil {
		return &query.NullLiteral{}
	}
	switch v := val.(type) {
	case string:
		return &query.StringLiteral{Value: v}
	case float64:
		return &query.NumberLiteral{Value: v}
	case int:
		return &query.NumberLiteral{Value: float64(v)}
	case int64:
		return &query.NumberLiteral{Value: float64(v)}
	case bool:
		if v {
			return &query.NumberLiteral{Value: 1}
		}
		return &query.NumberLiteral{Value: 0}
	default:
		return &query.StringLiteral{Value: ValueToStringKey(v)}
	}
}

// toUpperFast returns an uppercased copy of s only if s contains lowercase
// letters. This avoids an allocation when s is already uppercase (the common
// case for SQL identifiers parsed by the query package).
func toUpperFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			return strings.ToUpper(s)
		}
	}
	return s
}

// toLowerFast returns a lowercased copy of s only if s contains uppercase
// letters. This avoids an allocation when s is already lowercase.
func toLowerFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return strings.ToLower(s)
		}
	}
	return s
}

// ValueToStringKey converts a value to a string for use in map keys (GROUP BY,
// DISTINCT, etc.). It returns "<nil>" for nil so that nil and empty string do
// not collide. Uses strconv for common numeric types to avoid fmt.Sprintf
// reflection overhead.
func ValueToStringKey(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}
