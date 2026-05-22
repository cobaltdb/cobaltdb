package catalog

import (
	"crypto/rand"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

const maxStringResultLen = 10 * 1024 * 1024 // 10 MB cap for string functions

// EvalContext bundles common parameters for expression evaluation
// This reduces parameter count and makes the API cleaner
type EvalContext struct {
	Catalog   *Catalog
	Row       []interface{}
	Columns   []ColumnDef
	Args      []interface{}
	Table     *TableDef
	TableName string
}

// NewEvalContext creates a new evaluation context
func NewEvalContext(c *Catalog, row []interface{}, columns []ColumnDef, args []interface{}) *EvalContext {
	return &EvalContext{
		Catalog: c,
		Row:     row,
		Columns: columns,
		Args:    args,
	}
}

// WithTable sets the table context
func (ctx *EvalContext) WithTable(table *TableDef, name string) *EvalContext {
	ctx.Table = table
	ctx.TableName = name
	return ctx
}

// evaluateExpression evaluates an expression with the given context
func evaluateExpression(c *Catalog, row []interface{}, columns []ColumnDef, expr query.Expression, args []interface{}) (interface{}, error) {
	ctx := NewEvalContext(c, row, columns, args)
	return ctx.evaluate(expr)
}

// evaluate evaluates an expression using the context
func (ctx *EvalContext) evaluate(expr query.Expression) (interface{}, error) {
	c := ctx.Catalog
	row := ctx.Row
	columns := ctx.Columns
	args := ctx.Args

	switch e := expr.(type) {
	case *query.BinaryExpr:
		return evaluateBinaryExpr(c, row, columns, e, args)
	case *query.Identifier:
		// Check if this is a dotted identifier like "table.column"
		if dotIdx := strings.IndexByte(e.Name, '.'); dotIdx > 0 && dotIdx < len(e.Name)-1 {
			// Treat as QualifiedIdentifier
			return evaluateExpression(c, row, columns, &query.QualifiedIdentifier{
				Table:  e.Name[:dotIdx],
				Column: e.Name[dotIdx+1:],
			}, args)
		}
		// Find column value (case-insensitive, allocation-free)
		for i, col := range columns {
			if strings.EqualFold(col.Name, e.Name) && i < len(row) {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s", e.Name)
	case *query.PlaceholderExpr:
		if e.Index < len(args) {
			return args[e.Index], nil
		}
		return nil, fmt.Errorf("placeholder index out of range")
	case *query.StringLiteral:
		return e.Value, nil
	case *query.NumberLiteral:
		return e.Value, nil
	case *query.BooleanLiteral:
		return e.Value, nil
	case *query.NullLiteral:
		return nil, nil
	case *query.QualifiedIdentifier:
		// table.column format - prefer exact table match using sourceTbl (case-insensitive)
		for i, col := range columns {
			if strings.EqualFold(col.Name, e.Column) && strings.EqualFold(col.sourceTbl, e.Table) && i < len(row) {
				return row[i], nil
			}
		}
		// Fallback: match by column name only (for non-JOIN contexts)
		for i, col := range columns {
			if strings.EqualFold(col.Name, e.Column) && i < len(row) {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s.%s", e.Table, e.Column)
	case *query.LikeExpr:
		return evaluateLike(c, row, columns, e, args)
	case *query.InExpr:
		return evaluateIn(c, row, columns, e, args)
	case *query.BetweenExpr:
		return evaluateBetween(c, row, columns, e, args)
	case *query.IsNullExpr:
		return evaluateIsNull(c, row, columns, e, args)
	case *query.FunctionCall:
		return evaluateFunctionCall(c, row, columns, e, args)
	case *query.AliasExpr:
		// Unwrap alias and evaluate the underlying expression
		return evaluateExpression(c, row, columns, e.Expr, args)
	case *query.CaseExpr:
		return evaluateCaseExpr(c, row, columns, e, args)
	case *query.CastExpr:
		return evaluateCastExpr(c, row, columns, e, args)
	case *query.VectorLiteral:
		// Return vector as []float64
		return e.Values, nil
	case *query.SubqueryExpr:
		// Scalar subquery: execute and return first column of first row
		// Support correlated subqueries by resolving outer references
		subq := resolveOuterRefsInQuery(e.Query, row, columns)
		cols, rows, err := c.selectLocked(subq, args)
		if err != nil {
			return nil, err
		}
		_ = cols
		if len(rows) == 0 || len(rows[0]) == 0 {
			return nil, nil
		}
		if len(rows) > 1 {
			return nil, fmt.Errorf("scalar subquery returned %d rows instead of 1", len(rows))
		}
		return rows[0][0], nil
	case *query.ExistsExpr:
		// Support correlated subqueries by resolving outer references
		subq := resolveOuterRefsInQuery(e.Subquery, row, columns)
		_, rows, err := c.selectLocked(subq, args)
		if err != nil {
			return nil, err
		}
		exists := len(rows) > 0
		if e.Not {
			return !exists, nil
		}
		return exists, nil
	case *query.MatchExpr:
		return evaluateMatchExprLocked(c, row, columns, e, args)
	case *query.JSONPathExpr:
		// Evaluate -> (JSON object) and ->> (JSON text) operators
		val, err := evaluateExpression(c, row, columns, e.Column, args)
		if err != nil {
			return nil, err
		}
		jsonStr, ok := toString(val)
		if !ok {
			return nil, nil
		}
		result, err := JSONExtract(jsonStr, e.Path)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, nil
		}
		if e.AsText {
			// ->> returns the value as text (unquoted string)
			switch v := result.(type) {
			case string:
				return v, nil
			default:
				return ValueToStringKey(v), nil
			}
		}
		// -> returns the raw JSON value
		return result, nil
	case *query.UnaryExpr:
		val, err := evaluateExpression(c, row, columns, e.Expr, args)
		if err != nil {
			return nil, err
		}
		switch e.Operator {
		case query.TokenMinus:
			if f, ok := toFloat64(val); ok {
				if _, isInt := val.(int); isInt {
					return int(-f), nil
				}
				if _, isInt64 := val.(int64); isInt64 {
					return int64(-f), nil
				}
				return -f, nil
			}
			return nil, fmt.Errorf("cannot negate non-numeric value")
		case query.TokenNot:
			if val == nil {
				return nil, nil // NOT NULL = NULL per SQL three-valued logic
			}
			return !toBool(val), nil
		}
		return val, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func evaluateBinaryExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.BinaryExpr, args []interface{}) (interface{}, error) {
	left, err := evaluateExpression(c, row, columns, expr.Left, args)
	if err != nil {
		return nil, err
	}

	right, err := evaluateExpression(c, row, columns, expr.Right, args)
	if err != nil {
		return nil, err
	}

	// Handle logical operators first (they have special NULL semantics per SQL standard)
	// SQL three-valued logic: NULL AND false = false, NULL OR true = true,
	// NULL AND true = NULL, NULL OR false = NULL
	switch expr.Operator {
	case query.TokenAnd:
		leftBool, leftIsNil := toBoolNullable(left)
		rightBool, rightIsNil := toBoolNullable(right)
		if (!leftIsNil && !leftBool) || (!rightIsNil && !rightBool) {
			return false, nil // false AND anything = false
		}
		if leftIsNil || rightIsNil {
			return nil, nil // NULL AND true = NULL
		}
		return leftBool && rightBool, nil
	case query.TokenOr:
		leftBool, leftIsNil := toBoolNullable(left)
		rightBool, rightIsNil := toBoolNullable(right)
		if (!leftIsNil && leftBool) || (!rightIsNil && rightBool) {
			return true, nil // true OR anything = true
		}
		if leftIsNil || rightIsNil {
			return nil, nil // NULL OR false = NULL
		}
		return leftBool || rightBool, nil
	}

	// Handle NULL comparisons (for non-logical operators)
	if left == nil || right == nil {
		switch expr.Operator {
		case query.TokenIs:
			// IS NULL - true if both are nil
			// IS NOT NULL - true if either is not nil
			if rightVal, ok := right.(bool); ok {
				if rightVal {
					return left == nil, nil
				}
				return left != nil, nil
			}
		case query.TokenEq:
			// SQL standard: NULL = anything (including NULL) is NULL (unknown)
			return nil, nil
		case query.TokenNeq:
			// SQL standard: NULL != anything (including NULL) is NULL (unknown)
			return nil, nil
		}
		return nil, nil // NULL comparison returns NULL per SQL standard
	}

	// Handle arithmetic operators (+, -, *, /)
	switch expr.Operator {
	case query.TokenPlus:
		return addValues(left, right)
	case query.TokenMinus:
		return subtractValues(left, right)
	case query.TokenStar:
		return multiplyValues(left, right)
	case query.TokenSlash:
		return divideValues(left, right)
	case query.TokenPercent:
		return moduloValues(left, right)
	case query.TokenConcat:
		return concatValues(left, right), nil
	}

	// Compare based on operator
	switch expr.Operator {
	case query.TokenEq:
		return compareValues(left, right) == 0, nil
	case query.TokenNeq:
		return compareValues(left, right) != 0, nil
	case query.TokenLt:
		return compareValues(left, right) < 0, nil
	case query.TokenGt:
		return compareValues(left, right) > 0, nil
	case query.TokenLte:
		return compareValues(left, right) <= 0, nil
	case query.TokenGte:
		return compareValues(left, right) >= 0, nil
	default:
		return false, fmt.Errorf("unsupported operator: %v", expr.Operator)
	}
}

func compareValues(a, b interface{}) int {
	// Handle NULLs: NULLs sort last (after all non-NULL values)
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // NULL sorts after non-NULL
	}
	if b == nil {
		return -1 // non-NULL sorts before NULL
	}

	// Fast path: both are non-numeric strings (avoids failed ParseFloat)
	aStr, aIsStr := toString(a)
	bStr, bIsStr := toString(b)
	if aIsStr && bIsStr && !looksLikeNumber(aStr) && !looksLikeNumber(bStr) {
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
		return 0
	}

	// Handle numeric types (includes string numbers like "123")
	aNum, aIsNum := toFloat64(a)
	bNum, bIsNum := toFloat64(b)
	if aIsNum && bIsNum {
		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
		return 0
	}

	// Handle strings
	if aIsStr && bIsStr {
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
		return 0
	}

	// Fallback to string comparison (use fast conversion for known types)
	return strings.Compare(valueToString(a), valueToString(b))
}

func evaluateCaseExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.CaseExpr, args []interface{}) (interface{}, error) {
	if expr.Expr != nil {
		// Simple CASE: CASE expr WHEN val1 THEN result1 WHEN val2 THEN result2 ELSE default END
		baseVal, err := evaluateExpression(c, row, columns, expr.Expr, args)
		if err != nil {
			return nil, err
		}
		// Per SQL standard, CASE NULL WHEN NULL is UNKNOWN (not true)
		// If base value is NULL, skip all WHEN comparisons and fall through to ELSE
		if baseVal != nil {
			for _, when := range expr.Whens {
				whenVal, err := evaluateExpression(c, row, columns, when.Condition, args)
				if err != nil {
					return nil, err
				}
				if whenVal != nil && compareValues(baseVal, whenVal) == 0 {
					return evaluateExpression(c, row, columns, when.Result, args)
				}
			}
		}
	} else {
		// Searched CASE: CASE WHEN cond1 THEN result1 WHEN cond2 THEN result2 ELSE default END
		for _, when := range expr.Whens {
			condVal, err := evaluateExpression(c, row, columns, when.Condition, args)
			if err != nil {
				return nil, err
			}
			if toBool(condVal) {
				return evaluateExpression(c, row, columns, when.Result, args)
			}
		}
	}
	if expr.Else != nil {
		return evaluateExpression(c, row, columns, expr.Else, args)
	}
	return nil, nil
}

func evaluateFunctionCall(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.FunctionCall, args []interface{}) (interface{}, error) {
	// Parser uppercases function names at parse time; avoid ToUpper allocation.
	funcName := expr.Name

	// Short-circuit evaluation for COALESCE/IFNULL - evaluate lazily
	if funcName == "COALESCE" || funcName == "IFNULL" {
		for _, arg := range expr.Args {
			val, err := evaluateExpression(c, row, columns, arg, args)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
		}
		return nil, nil
	}

	// Evaluate arguments first (eager for all other functions)
	evalArgs := make([]interface{}, len(expr.Args))
	for i, arg := range expr.Args {
		val, err := evaluateExpression(c, row, columns, arg, args)
		if err != nil {
			return nil, err
		}
		evalArgs[i] = val
	}

	// Try string functions first (largest group)
	if result, handled := evaluateStringFunction(funcName, evalArgs); handled {
		return result.val, result.err
	}

		// Try math functions
		if val, handled, err := evaluateMathFunction(funcName, evalArgs); handled {
			return val, err
		}

		// Try vector functions
		if val, handled, err := evaluateVectorFunction(funcName, evalArgs); handled {
			return val, err
		}

		// Try CAST
		if val, handled, err := evaluateCastFunction(funcName, evalArgs); handled {
			return val, err
		}

	switch funcName {
	case "COALESCE", "IFNULL":
		// Handled above with short-circuit evaluation
		return nil, nil

	case "NULLIF":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("NULLIF requires 2 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return evalArgs[0], nil
		}
		if compareValues(evalArgs[0], evalArgs[1]) == 0 {
			return nil, nil
		}
		return evalArgs[0], nil

	case "DATE", "TIME", "DATETIME":
		// Simple date/time functions - return current time for now
		// Full implementation would require time parsing
		if len(evalArgs) < 1 {
			return nil, nil
		}
		return evalArgs[0], nil

	case "NOW", "CURRENT_TIMESTAMP", "CURRENT_TIME", "CURRENT_DATE":
		// Return current timestamp
		now := time.Now()
		return now.Format("2006-01-02 15:04:05"), nil

	case "STRFTIME":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("STRFTIME requires 2 arguments")
		}
		// Simple strftime - just return the input for now
		if evalArgs[1] == nil {
			return nil, nil
		}
		return ValueToStringKey(evalArgs[1]), nil

	case "GROUP_CONCAT":
		// GROUP_CONCAT is handled in aggregate path; scalar fallback just returns the value
		if len(evalArgs) >= 1 && evalArgs[0] != nil {
			return ValueToStringKey(evalArgs[0]), nil
		}
		return nil, nil

	case "TYPEOF":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("TYPEOF requires 1 argument")
		}
		if evalArgs[0] == nil {
			return "null", nil
		}
		switch evalArgs[0].(type) {
		case int, int64:
			return "integer", nil
		case float64:
			f := evalArgs[0].(float64)
			if f == float64(int64(f)) {
				return "integer", nil
			}
			return "real", nil
		case string:
			return "text", nil
		case bool:
			return "integer", nil
		default:
			return "text", nil
		}

	case "IIF":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("IIF requires 3 arguments")
		}
		cond := evalArgs[0]
		truthy := false
		if b, ok := cond.(bool); ok {
			truthy = b
		} else if f, ok := toFloat64(cond); ok {
			truthy = f != 0
		} else if cond != nil {
			truthy = true
		}
		if truthy {
			return evalArgs[1], nil
		}
		return evalArgs[2], nil

	case "RANDOM":
		// Use crypto/rand for cryptographic randomness
		n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 63))
		if err != nil {
			return float64(0), nil // fallback on error
		}
		return float64(n.Int64()), nil

	case "ZEROBLOB":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("ZEROBLOB requires 1 argument")
		}
		n, _ := toFloat64(evalArgs[0])
		size := int(n)
		if size <= 0 {
			return "", nil
		}
		if size > maxStringResultLen {
			return nil, fmt.Errorf("ZEROBLOB size exceeds maximum allowed size (%d bytes)", maxStringResultLen)
		}
		return strings.Repeat("\x00", size), nil

	default:
		// Check for JSON functions
		return evaluateJSONFunction(funcName, evalArgs)
	}
}


// evaluateMathFunction handles ABS, ROUND, FLOOR, CEIL math functions.
func evaluateMathFunction(funcName string, evalArgs []interface{}) (interface{}, bool, error) {
	switch funcName {
	case "ABS":
		if len(evalArgs) < 1 {
			return nil, true, fmt.Errorf("ABS requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, true, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			if f < 0 {
				return -f, true, nil
			}
			return f, true, nil
		}
		return evalArgs[0], true, nil

	case "ROUND":
		if len(evalArgs) < 1 {
			return nil, true, fmt.Errorf("ROUND requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, true, nil
		}
		f, ok := toFloat64(evalArgs[0])
		if !ok {
			return evalArgs[0], true, nil
		}
		precision := 0
		if len(evalArgs) >= 2 {
			if p, ok := toFloat64(evalArgs[1]); ok {
				precision = int(p)
			}
		}
		divisor := 1.0
		for i := 0; i < precision; i++ {
			divisor *= 10
		}
		result := math.Round(f*divisor) / divisor
		if precision == 0 {
			return float64(int64(result)), true, nil
		}
		return result, true, nil

	case "FLOOR":
		if len(evalArgs) < 1 {
			return nil, true, fmt.Errorf("FLOOR requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, true, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Floor(f), true, nil
		}
		return evalArgs[0], true, nil

	case "CEIL", "CEILING":
		if len(evalArgs) < 1 {
			return nil, true, fmt.Errorf("CEIL requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, true, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Ceil(f), true, nil
		}
		return evalArgs[0], true, nil
	}
	return nil, false, nil
}



// evaluateCastFunction handles the CAST type conversion.
func evaluateCastFunction(funcName string, evalArgs []interface{}) (interface{}, bool, error) {
	if funcName != "CAST" {
		return nil, false, nil
	}
	if len(evalArgs) < 2 {
		return nil, true, fmt.Errorf("CAST requires 2 arguments")
	}
	if evalArgs[0] == nil {
		return nil, true, nil
	}
	targetType, ok := evalArgs[1].(string)
	if !ok {
		targetType = toUpperFast(ValueToStringKey(evalArgs[1]))
	}
	switch targetType {
	case "INTEGER", "INT":
		if f, ok := toFloat64(evalArgs[0]); ok {
			return int64(f), true, nil
		}
		if s, ok := toString(evalArgs[0]); ok {
			i, err := strconv.ParseInt(s, 10, 64)
			if err == nil {
				return i, true, nil
			}
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return int64(f), true, nil
			}
			return int64(0), true, nil
		}
		if b, ok := evalArgs[0].(bool); ok {
			if b {
				return int64(1), true, nil
			}
			return int64(0), true, nil
		}
	case "REAL", "FLOAT":
		if f, ok := toFloat64(evalArgs[0]); ok {
			return f, true, nil
		}
		if s, ok := toString(evalArgs[0]); ok {
			f, err := strconv.ParseFloat(s, 64)
			if err == nil {
				return f, true, nil
			}
			return 0.0, true, nil
		}
	case "TEXT", "STRING":
		return ValueToStringKey(evalArgs[0]), true, nil
	case "BOOLEAN", "BOOL":
		if b, ok := evalArgs[0].(bool); ok {
			return b, true, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return f != 0, true, nil
		}
		if s, ok := toString(evalArgs[0]); ok {
			return strings.EqualFold(s, "true") || s == "1", true, nil
		}
	}
	return evalArgs[0], true, nil
}

// evaluateVectorFunction handles COSINE_SIMILARITY, L2_DISTANCE, INNER_PRODUCT.
func evaluateVectorFunction(funcName string, evalArgs []interface{}) (interface{}, bool, error) {
	switch funcName {
	case "COSINE_SIMILARITY", "COSINE_SIMILARIT":
		if len(evalArgs) != 2 {
			return nil, true, fmt.Errorf("COSINE_SIMILARITY requires exactly 2 arguments")
		}
		v1, err := toVector(evalArgs[0])
		if err != nil {
			return nil, true, fmt.Errorf("COSINE_SIMILARITY first argument: %v", err)
		}
		v2, err := toVector(evalArgs[1])
		if err != nil {
			return nil, true, fmt.Errorf("COSINE_SIMILARITY second argument: %v", err)
		}
		return cosineSimilarity(v1, v2), true, nil

	case "L2_DISTANCE", "L2_DIST":
		if len(evalArgs) != 2 {
			return nil, true, fmt.Errorf("L2_DISTANCE requires exactly 2 arguments")
		}
		v1, err := toVector(evalArgs[0])
		if err != nil {
			return nil, true, fmt.Errorf("L2_DISTANCE first argument: %v", err)
		}
		v2, err := toVector(evalArgs[1])
		if err != nil {
			return nil, true, fmt.Errorf("L2_DISTANCE second argument: %v", err)
		}
		return l2Distance(v1, v2), true, nil

	case "INNER_PRODUCT", "DOT_PRODUCT", "DOT":
		if len(evalArgs) != 2 {
			return nil, true, fmt.Errorf("INNER_PRODUCT requires exactly 2 arguments")
		}
		v1, err := toVector(evalArgs[0])
		if err != nil {
			return nil, true, fmt.Errorf("INNER_PRODUCT first argument: %v", err)
		}
		v2, err := toVector(evalArgs[1])
		if err != nil {
			return nil, true, fmt.Errorf("INNER_PRODUCT second argument: %v", err)
		}
		return innerProduct(v1, v2), true, nil
	}
	return nil, false, nil
}

// toVector converts an interface value to a []float64 vector
func toVector(v interface{}) ([]float64, error) {
	switch vec := v.(type) {
	case []float64:
		return vec, nil
	case []interface{}:
		result := make([]float64, len(vec))
		for i, val := range vec {
			switch fv := val.(type) {
			case float64:
				result[i] = fv
			case int:
				result[i] = float64(fv)
			case int64:
				result[i] = float64(fv)
			case float32:
				result[i] = float64(fv)
			default:
				return nil, fmt.Errorf("element %d is not a number: %T", i, val)
			}
		}
		return result, nil
	case string:
		// Try to parse as JSON array
		// For now, return error - JSON parsing can be added if needed
		return nil, fmt.Errorf("cannot convert string to vector")
	case nil:
		return nil, fmt.Errorf("cannot convert NULL to vector")
	default:
		return nil, fmt.Errorf("cannot convert %T to vector", v)
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case bool:
		if n {
			return 1, true
		}
		return 0, true
	case string:
		// Fast path: skip strings that clearly aren't numbers
		if !looksLikeNumber(n) {
			return 0, false
		}
		// Try to parse string as number (SQLite-compatible behavior)
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
		return 0, false
	case *string:
		if n == nil {
			return 0, false
		}
		if !looksLikeNumber(*n) {
			return 0, false
		}
		if f, err := strconv.ParseFloat(*n, 64); err == nil {
			return f, true
		}
		return 0, false
	case StringBox:
		if n.ptr == nil {
			return 0, false
		}
		if !looksLikeNumber(*n.ptr) {
			return 0, false
		}
		if f, err := strconv.ParseFloat(*n.ptr, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// looksLikeNumber quickly rejects strings that cannot be valid numbers,
// avoiding an expensive strconv.ParseFloat allocation on failure.
func looksLikeNumber(s string) bool {
	if len(s) == 0 {
		return false
	}
	c := s[0]
	// Allow leading sign or digit or decimal point
	if c != '+' && c != '-' && c != '.' && (c < '0' || c > '9') {
		return false
	}
	// Must contain at least one digit to be a number
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			return true
		}
	}
	return false
}

// evaluateMatchExpr evaluates MATCH ... AGAINST for full-text search.
// It acquires the catalog read lock; use evaluateMatchExprLocked when the caller already holds the lock.
func evaluateMatchExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.MatchExpr, args []interface{}) (interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return evaluateMatchExprLocked(c, row, columns, expr, args)
}

// evaluateMatchExprLocked evaluates MATCH ... AGAINST for full-text search.
// Caller must hold c.mu (read or write lock).
func evaluateMatchExprLocked(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.MatchExpr, args []interface{}) (interface{}, error) {
	// Get the pattern value
	patternVal, err := evaluateExpression(c, row, columns, expr.Pattern, args)
	if err != nil {
		return nil, err
	}
	if patternVal == nil {
		return false, nil
	}
	pattern := ValueToStringKey(patternVal)

	// Tokenize the search pattern into words
	searchWords := tokenize(pattern)
	if len(searchWords) == 0 {
		return false, nil
	}

	// Try to find an FTS index that covers the columns in the MATCH expression
	for _, ftsIdx := range c.ftsIndexes {
		// Check if this FTS index matches the expression columns
		if len(expr.Columns) != len(ftsIdx.Columns) {
			continue
		}

		// Check column match
		colsMatch := true
		for i, colExpr := range expr.Columns {
			switch col := colExpr.(type) {
			case *query.Identifier:
				if !strings.EqualFold(col.Name, ftsIdx.Columns[i]) {
					colsMatch = false
					break
				}
			case *query.QualifiedIdentifier:
				if !strings.EqualFold(col.Column, ftsIdx.Columns[i]) {
					colsMatch = false
					break
				}
			default:
				colsMatch = false
			}
		}

		if colsMatch {
			// Use this FTS index to check if the row matches
			// Get the text from all indexed columns for this row
			var allText []string
			for _, colName := range ftsIdx.Columns {
				// Find the column in the row
				for i, col := range columns {
					if strings.EqualFold(col.Name, colName) && i < len(row) {
						if row[i] != nil {
							allText = append(allText, toLowerFast(ValueToStringKey(row[i])))
						}
						break
					}
				}
			}

			if len(allText) == 0 {
				return false, nil
			}

			// Check if all search words are present in the indexed text
			// AND logic: all words must be present
			for _, word := range searchWords {
				word = toLowerFast(word)
				found := false
				// Check if this word is in the FTS index
				if rowsWithWord, exists := ftsIdx.Index[word]; exists && len(rowsWithWord) > 0 {
					// Word exists in index, now check if it's in this row's text
					for _, text := range allText {
						if strings.Contains(text, word) {
							found = true
							break
						}
					}
				}
				if !found {
					return false, nil // Word not found, row doesn't match
				}
			}
			return true, nil // All words found
		}
	}

	// No matching FTS index found - do simple text search on the columns
	// Get text from all specified columns
	var allText []string
	for _, colExpr := range expr.Columns {
		colVal, err := evaluateExpression(c, row, columns, colExpr, args)
		if err != nil {
			continue
		}
		if colVal != nil {
			allText = append(allText, toLowerFast(ValueToStringKey(colVal)))
		}
	}

	if len(allText) == 0 {
		return false, nil
	}

	// Check if all search words are present
	for _, word := range searchWords {
		word = toLowerFast(word)
		found := false
		for _, text := range allText {
			if strings.Contains(text, word) {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}

	return true, nil
}

// evalBinaryExprValue evaluates a binary expression with already-evaluated operands.
func evalBinaryExprValue(left, right interface{}, operator query.TokenType) (interface{}, error) {
if left == nil || right == nil {
	switch operator {
	case query.TokenAnd:
		if left == nil && right == nil {
			return nil, nil
		}
		if left == nil {
			if toBool(right) {
				return nil, nil // NULL AND true = NULL
			}
			return false, nil // NULL AND false = false
		}
		if toBool(left) {
			return nil, nil // true AND NULL = NULL
		}
		return false, nil // false AND NULL = false
	case query.TokenOr:
		if left == nil && right == nil {
			return nil, nil
		}
		if left == nil {
			if toBool(right) {
				return true, nil // NULL OR true = true
			}
			return nil, nil // NULL OR false = NULL
		}
		if toBool(left) {
			return true, nil // true OR NULL = true
		}
		return nil, nil // false OR NULL = NULL
	case query.TokenConcat:
		// Concat with NULL returns NULL in standard SQL
		return nil, nil
	default:
		return nil, nil
	}
}
// Handle arithmetic in value expressions
lf, lok := toFloat64(left)
rf, rok := toFloat64(right)
if lok && rok {
	bothInt := isIntegerType(left) && isIntegerType(right)
	switch operator {
	case query.TokenPlus:
		if bothInt {
			return int64(lf) + int64(rf), nil
		}
		return lf + rf, nil
	case query.TokenMinus:
		if bothInt {
			return int64(lf) - int64(rf), nil
		}
		return lf - rf, nil
	case query.TokenStar:
		if bothInt {
			return int64(lf) * int64(rf), nil
		}
		return lf * rf, nil
	case query.TokenSlash:
		if rf != 0 {
			return lf / rf, nil
		}
		return nil, fmt.Errorf("division by zero")
	case query.TokenPercent:
		if rf != 0 {
			return int64(lf) % int64(rf), nil
		}
		return nil, fmt.Errorf("division by zero")
	}
}
// Comparison operators
switch operator {
case query.TokenEq:
	return compareValues(left, right) == 0, nil
case query.TokenNeq:
	return compareValues(left, right) != 0, nil
case query.TokenLt:
	return compareValues(left, right) < 0, nil
case query.TokenGt:
	return compareValues(left, right) > 0, nil
case query.TokenLte:
	return compareValues(left, right) <= 0, nil
case query.TokenGte:
	return compareValues(left, right) >= 0, nil
case query.TokenAnd:
	return toBool(left) && toBool(right), nil
case query.TokenOr:
	return toBool(left) || toBool(right), nil
case query.TokenConcat:
	return concatValues(left, right), nil
}
	return nil, fmt.Errorf("unsupported binary operator in value expression")
}

// evalFunctionCallValue evaluates a scalar function call with already-evaluated arguments.
func evalFunctionCallValue(funcName string, evalArgs []interface{}) (interface{}, error) {
	switch funcName {
case "COALESCE":
	for _, a := range evalArgs {
		if a != nil {
			return a, nil
		}
	}
	return nil, nil
case "NULLIF":
	if len(evalArgs) == 2 && compareValues(evalArgs[0], evalArgs[1]) == 0 {
		return nil, nil
	}
	if len(evalArgs) >= 1 {
		return evalArgs[0], nil
	}
	return nil, nil
case "IIF":
	if len(evalArgs) == 3 {
		if toBool(evalArgs[0]) {
			return evalArgs[1], nil
		}
		return evalArgs[2], nil
	}
	return nil, nil
case "ABS":
	if len(evalArgs) == 1 {
		if f, ok := toFloat64(evalArgs[0]); ok {
			if f < 0 {
				return -f, nil
			}
			return f, nil
		}
	}
	return nil, nil
case "UPPER":
	if len(evalArgs) == 1 && evalArgs[0] != nil {
		return toUpperFast(ValueToStringKey(evalArgs[0])), nil
	}
	return nil, nil
case "LOWER":
	if len(evalArgs) == 1 && evalArgs[0] != nil {
		return toLowerFast(ValueToStringKey(evalArgs[0])), nil
	}
	return nil, nil
case "LENGTH":
	if len(evalArgs) == 1 && evalArgs[0] != nil {
		return len(ValueToStringKey(evalArgs[0])), nil
	}
	return nil, nil
case "CONCAT":
	var sb strings.Builder
	sb.Grow(len(evalArgs) * 16)
	for _, a := range evalArgs {
		if a != nil {
			sb.WriteString(ValueToStringKey(a))
			if sb.Len() > maxStringResultLen {
				return nil, fmt.Errorf("CONCAT result exceeds maximum length")
			}
		}
	}
	return sb.String(), nil
case "IFNULL":
	if len(evalArgs) >= 2 {
		if evalArgs[0] != nil {
			return evalArgs[0], nil
		}
		return evalArgs[1], nil
	}
	return nil, nil
case "TRIM":
	if len(evalArgs) >= 1 && evalArgs[0] != nil {
		return strings.TrimSpace(ValueToStringKey(evalArgs[0])), nil
	}
	return nil, nil
case "LTRIM":
	if len(evalArgs) >= 1 && evalArgs[0] != nil {
		return strings.TrimLeft(ValueToStringKey(evalArgs[0]), " \t\n\r"), nil
	}
	return nil, nil
case "RTRIM":
	if len(evalArgs) >= 1 && evalArgs[0] != nil {
		return strings.TrimRight(ValueToStringKey(evalArgs[0]), " \t\n\r"), nil
	}
	return nil, nil
case "SUBSTR", "SUBSTRING":
	if len(evalArgs) < 2 {
		return nil, nil
	}
	if evalArgs[0] == nil || evalArgs[1] == nil {
		return nil, nil
	}
	str := ValueToStringKey(evalArgs[0])
	start, _ := toFloat64(evalArgs[1])
	startInt := int(start) - 1
	if startInt < 0 {
		startInt = 0
	}
	if startInt >= len(str) {
		return "", nil
	}
	if len(evalArgs) >= 3 && evalArgs[2] != nil {
		length, _ := toFloat64(evalArgs[2])
		lengthInt := int(length)
		if lengthInt < 0 {
			return "", nil
		}
		if startInt+lengthInt > len(str) {
			lengthInt = len(str) - startInt
		}
		return str[startInt : startInt+lengthInt], nil
	}
	return str[startInt:], nil
case "REPLACE":
	if len(evalArgs) < 3 || evalArgs[0] == nil || evalArgs[1] == nil || evalArgs[2] == nil {
		return nil, nil
	}
	str := ValueToStringKey(evalArgs[0])
	old := ValueToStringKey(evalArgs[1])
	if old == "" {
		return str, nil
	}
	newStr := ValueToStringKey(evalArgs[2])
	result := strings.ReplaceAll(str, old, newStr)
	if len(result) > maxStringResultLen {
		return nil, fmt.Errorf("REPLACE result exceeds maximum length")
	}
	return result, nil
case "INSTR":
	if len(evalArgs) >= 2 && evalArgs[0] != nil && evalArgs[1] != nil {
		str := ValueToStringKey(evalArgs[0])
		substr := ValueToStringKey(evalArgs[1])
		idx := strings.Index(str, substr)
		if idx < 0 {
			return int64(0), nil
		}
		return int64(idx + 1), nil // 1-based
	}
	return nil, nil
case "ROUND":
	if len(evalArgs) >= 1 && evalArgs[0] != nil {
		f, ok := toFloat64(evalArgs[0])
		if !ok {
			return nil, nil
		}
		decimals := 0
		if len(evalArgs) >= 2 {
			d, _ := toFloat64(evalArgs[1])
			decimals = int(d)
		}
		pow := math.Pow(10, float64(decimals))
		return math.Round(f*pow) / pow, nil
	}
	return nil, nil
case "FLOOR":
	if len(evalArgs) >= 1 && evalArgs[0] != nil {
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Floor(f), nil
		}
	}
	return nil, nil
case "CEIL", "CEILING":
	if len(evalArgs) >= 1 && evalArgs[0] != nil {
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Ceil(f), nil
		}
	}
	return nil, nil
case "TYPEOF":
	if len(evalArgs) < 1 || evalArgs[0] == nil {
		return "null", nil
	}
	switch evalArgs[0].(type) {
	case int, int64:
		return "integer", nil
	case float64:
		f := evalArgs[0].(float64)
		if f == float64(int64(f)) {
			return "integer", nil
		}
		return "real", nil
	case string:
		return "text", nil
	case bool:
		return "integer", nil
	default:
		return "text", nil
	}
case "MIN":
	if len(evalArgs) >= 2 {
		min := evalArgs[0]
		for _, a := range evalArgs[1:] {
			if a != nil && (min == nil || compareValues(a, min) < 0) {
				min = a
			}
		}
		return min, nil
	}
	if len(evalArgs) == 1 {
		return evalArgs[0], nil
	}
	return nil, nil
case "MAX":
	if len(evalArgs) >= 2 {
		max := evalArgs[0]
		for _, a := range evalArgs[1:] {
			if a != nil && (max == nil || compareValues(a, max) > 0) {
				max = a
			}
		}
		return max, nil
	}
	if len(evalArgs) == 1 {
		return evalArgs[0], nil
	}
	return nil, nil
case "REVERSE":
	if len(evalArgs) >= 1 && evalArgs[0] != nil {
		str := ValueToStringKey(evalArgs[0])
		runes := []rune(str)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	}
	return nil, nil
case "REPEAT":
	if len(evalArgs) >= 2 && evalArgs[0] != nil && evalArgs[1] != nil {
		str := ValueToStringKey(evalArgs[0])
		n, _ := toFloat64(evalArgs[1])
		if int(n) <= 0 {
			return "", nil
		}
		if int(n)*len(str) > maxStringResultLen {
			return nil, fmt.Errorf("REPEAT result exceeds maximum length")
		}
		return strings.Repeat(str, int(n)), nil
	}
	return nil, nil
default:
	return nil, fmt.Errorf("unsupported function in value expression: %s", funcName)
}
}

func evaluateWhere(c *Catalog, row []interface{}, columns []ColumnDef, where query.Expression, args []interface{}) (bool, error) {
	if where == nil {
		return true, nil
	}

	// Evaluate the expression
	result, err := evaluateExpression(c, row, columns, where, args)
	if err != nil {
		return false, err
	}

	// Convert result to boolean
	// Note: result can be nil for IS NULL expressions - this is handled below

	switch v := result.(type) {
	case bool:
		return v, nil
	case nil:
		// For IS NULL expressions, nil result means the value is null
		// but we need to check if this was from an IS NULL expression
		// If the where expression is IsNullExpr, nil result should be treated as false
		// because evaluateIsNull returns a bool, not nil
		return false, nil
	case int, int64, float64:
		// Non-zero numbers are truthy
		switch n := v.(type) {
		case int:
			return n != 0, nil
		case int64:
			return n != 0, nil
		case float64:
			return n != 0, nil
		}
	case string:
		return v != "", nil
	case *string:
		return v != nil && *v != "", nil
	case StringBox:
		return v.String() != "", nil
	}

	return false, nil
}

func isIntegerType(v interface{}) bool {
	switch val := v.(type) {
	case int:
		return true
	case int64:
		return true
	case float64:
		// JSON numbers are decoded as float64, check if it's a whole number
		return val == float64(int64(val)) && val >= -1e15 && val <= 1e15
	}
	return false
}

func addValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot add non-numeric values")
	}
	result := aNum + bNum
	if isIntegerType(a) && isIntegerType(b) {
		return int64(result), nil
	}
	return result, nil
}

func subtractValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot subtract non-numeric values")
	}
	result := aNum - bNum
	if isIntegerType(a) && isIntegerType(b) {
		return int64(result), nil
	}
	return result, nil
}

func multiplyValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot multiply non-numeric values")
	}
	result := aNum * bNum
	if isIntegerType(a) && isIntegerType(b) {
		return int64(result), nil
	}
	return result, nil
}

func divideValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot divide non-numeric values")
	}
	if bNum == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return aNum / bNum, nil
}

func moduloValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot compute modulo of non-numeric values")
	}
	if bNum == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	// Use integer modulo if both are ints
	_, aIsInt := a.(int)
	_, bIsInt := b.(int)
	_, aIsInt64 := a.(int64)
	_, bIsInt64 := b.(int64)
	if (aIsInt || aIsInt64) && (bIsInt || bIsInt64) {
		return int64(aNum) % int64(bNum), nil
	}
	return math.Mod(aNum, bNum), nil
}

func concatValues(a, b interface{}) string {
	return ValueToStringKey(a) + ValueToStringKey(b)
}

func evaluateLike(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.LikeExpr, args []interface{}) (interface{}, error) {
	left, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	pattern, err := evaluateExpression(c, row, columns, expr.Pattern, args)
	if err != nil {
		return false, err
	}

	// Handle NULL - SQL three-valued logic: NULL in LIKE → NULL (unknown)
	if left == nil || pattern == nil {
		return nil, nil
	}

	leftStr, ok := left.(string)
	if !ok {
		leftStr = ValueToStringKey(left)
	}

	patternStr, ok := pattern.(string)
	if !ok {
		patternStr = ValueToStringKey(pattern)
	}

	// Handle ESCAPE character
	escapeChar := byte(0)
	if expr.Escape != nil {
		escVal, err := evaluateExpression(c, row, columns, expr.Escape, args)
		if err == nil && escVal != nil {
			escStr := ValueToStringKey(escVal)
			if len(escStr) == 1 {
				escapeChar = escStr[0]
			}
		}
	}

	var matched bool
	if escapeChar != 0 {
		matched = matchLikeSimple(leftStr, patternStr, escapeChar)
	} else {
		matched = matchLikeSimple(leftStr, patternStr)
	}

	// Handle NOT LIKE
	if expr.Not {
		return !matched, nil
	}
	return matched, nil
}

func evaluateIsNull(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.IsNullExpr, args []interface{}) (interface{}, error) {
	val, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	isNull := val == nil
	if expr.Not {
		return !isNull, nil
	}
	return isNull, nil
}

func matchLikeSimple(s, pattern string, escapeChar ...byte) bool {
	if pattern == "" {
		return s == ""
	}

	var esc byte
	if len(escapeChar) > 0 {
		esc = escapeChar[0]
	}

	// Convert both strings to lower case for case-insensitive matching
	s = toLowerFast(s)
	pattern = toLowerFast(pattern)
	if esc >= 'A' && esc <= 'Z' {
		esc = esc + 32 // lowercase the escape char too
	}

	sIdx := 0
	pIdx := 0

	for sIdx < len(s) && pIdx < len(pattern) {
		char := pattern[pIdx]

		// Handle escape character
		if esc != 0 && char == esc && pIdx+1 < len(pattern) {
			pIdx++ // skip escape char
			// Next char is literal
			if sIdx < len(s) && s[sIdx] == pattern[pIdx] {
				sIdx++
				pIdx++
				continue
			}
			return false
		}

		// Handle %
		if char == '%' {
			// Skip consecutive %
			for pIdx < len(pattern) && pattern[pIdx] == '%' {
				pIdx++
			}
			if pIdx >= len(pattern) {
				return true
			}
			// Try matching remaining pattern at each position
			for sIdx <= len(s) {
				if matchLikeSimple(s[sIdx:], pattern[pIdx:], escapeChar...) {
					return true
				}
				if sIdx >= len(s) {
					break
				}
				sIdx++
			}
			return false
		}

		// Handle _
		if char == '_' {
			sIdx++
			pIdx++
			continue
		}

		// Literal match
		if sIdx < len(s) && s[sIdx] == char {
			sIdx++
			pIdx++
			continue
		}

		return false
	}

	// Skip any trailing % in pattern
	for pIdx < len(pattern) && pattern[pIdx] == '%' {
		pIdx++
	}

	return sIdx == len(s) && pIdx == len(pattern)
}

func evaluateIn(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.InExpr, args []interface{}) (interface{}, error) {
	left, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	// SQL three-valued logic: if left is NULL, IN/NOT IN returns NULL (unknown)
	if left == nil {
		return nil, nil
	}

	// Handle subquery: IN (SELECT ...)
	if expr.Subquery != nil {
		subq := resolveOuterRefsInQuery(expr.Subquery, row, columns)
		_, subqueryRows, err := c.selectLocked(subq, args)
		if err != nil {
			return false, err
		}
		found := false
		hasNull := false
		for _, subRow := range subqueryRows {
			if len(subRow) > 0 {
				if subRow[0] == nil {
					hasNull = true
				} else if compareValues(left, subRow[0]) == 0 {
					found = true
					break
				}
			}
		}
		if found {
			if expr.Not {
				return false, nil
			}
			return true, nil
		}
		// SQL three-valued logic: NOT IN with NULLs in list and no match → NULL (unknown)
		if hasNull {
			return nil, nil
		}
		if expr.Not {
			return true, nil
		}
		return false, nil
	}

	// Evaluate all values in the list
	var listValues []interface{}
	for _, item := range expr.List {
		val, err := evaluateExpression(c, row, columns, item, args)
		if err != nil {
			return false, err
		}
		listValues = append(listValues, val)
	}

	// Check if left is in list (with three-valued NULL logic)
	found := false
	hasNull := false
	for _, v := range listValues {
		if v == nil {
			hasNull = true
		} else if compareValues(left, v) == 0 {
			found = true
			break
		}
	}

	if found {
		if expr.Not {
			return false, nil
		}
		return true, nil
	}
	// SQL three-valued logic: IN/NOT IN with NULLs in list and no match → NULL (unknown)
	if hasNull {
		return nil, nil
	}
	if expr.Not {
		return true, nil
	}
	return false, nil
}

func evaluateCastExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.CastExpr, args []interface{}) (interface{}, error) {
	val, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	switch expr.DataType {
	case query.TokenInteger:
		if f, ok := toFloat64(val); ok {
			return int64(f), nil
		}
		if s, ok := val.(string); ok {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i, nil
			}
		}
		return int64(0), nil
	case query.TokenReal:
		if f, ok := toFloat64(val); ok {
			return f, nil
		}
		if s, ok := val.(string); ok {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f, nil
			}
		}
		return float64(0), nil
	case query.TokenText:
		return ValueToStringKey(val), nil
	case query.TokenBoolean:
		if b, ok := val.(bool); ok {
			return b, nil
		}
		if f, ok := toFloat64(val); ok {
			return f != 0, nil
		}
		if s, ok := val.(string); ok {
			return strings.EqualFold(s, "true") || s == "1", nil
		}
		return false, nil
	}
	return val, nil
}

func evaluateBetween(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.BetweenExpr, args []interface{}) (interface{}, error) {
	exprVal, err := evaluateExpression(c, row, columns, expr.Expr, args)
	if err != nil {
		return false, err
	}

	lowerVal, err := evaluateExpression(c, row, columns, expr.Lower, args)
	if err != nil {
		return false, err
	}

	upperVal, err := evaluateExpression(c, row, columns, expr.Upper, args)
	if err != nil {
		return false, err
	}

	// Handle NULL - SQL three-valued logic: NULL in BETWEEN → NULL (unknown)
	if exprVal == nil || lowerVal == nil || upperVal == nil {
		return nil, nil
	}

	// Check: lower <= expr <= upper
	lowCmp := compareValues(exprVal, lowerVal)
	highCmp := compareValues(exprVal, upperVal)

	result := lowCmp >= 0 && highCmp <= 0

	// Handle NOT BETWEEN
	if expr.Not {
		return !result, nil
	}
	return result, nil
}
