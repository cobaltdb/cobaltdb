package catalog

import (
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"math"
	"math/rand"
	"regexp"
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
		// Find column value (case-insensitive)
		nameLower := strings.ToLower(e.Name)
		for i, col := range columns {
			if strings.ToLower(col.Name) == nameLower && i < len(row) {
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
		colLower := strings.ToLower(e.Column)
		tblLower := strings.ToLower(e.Table)
		for i, col := range columns {
			if strings.ToLower(col.Name) == colLower && strings.ToLower(col.sourceTbl) == tblLower && i < len(row) {
				return row[i], nil
			}
		}
		// Fallback: match by column name only (for non-JOIN contexts)
		for i, col := range columns {
			if strings.ToLower(col.Name) == colLower && i < len(row) {
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
		return evaluateMatchExpr(c, row, columns, e, args)
	case *query.JSONPathExpr:
		// Evaluate -> (JSON object) and ->> (JSON text) operators
		val, err := evaluateExpression(c, row, columns, e.Column, args)
		if err != nil {
			return nil, err
		}
		jsonStr, ok := val.(string)
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
				return fmt.Sprintf("%v", v), nil
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
		return fmt.Sprintf("%v%v", left, right), nil
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

	// Handle numeric types
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
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
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
	funcName := strings.ToUpper(expr.Name)

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

	switch funcName {
	case "LENGTH", "LEN":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("LENGTH requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return float64(len(str)), nil

	case "UPPER":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("UPPER requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return strings.ToUpper(str), nil

	case "LOWER":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("LOWER requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		return strings.ToLower(str), nil

	case "TRIM", "LTRIM", "RTRIM":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("%s requires at least 1 argument", funcName)
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		// Optional second arg: characters to trim (default: whitespace)
		trimChars := " \t\n\r"
		if len(evalArgs) >= 2 && evalArgs[1] != nil {
			trimChars = fmt.Sprintf("%v", evalArgs[1])
		}
		switch funcName {
		case "LTRIM":
			return strings.TrimLeft(str, trimChars), nil
		case "RTRIM":
			return strings.TrimRight(str, trimChars), nil
		default:
			return strings.Trim(str, trimChars), nil
		}

	case "SUBSTR", "SUBSTRING":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("SUBSTR requires at least 2 arguments")
		}
		// SQL standard: any NULL argument returns NULL
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return nil, nil
		}
		if len(evalArgs) >= 3 && evalArgs[2] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		start, _ := toFloat64(evalArgs[1])
		startInt := int(start) - 1 // SQL SUBSTR is 1-indexed
		if startInt < 0 {
			startInt = 0
		}
		if startInt >= len(str) {
			return "", nil
		}
		if len(evalArgs) >= 3 {
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

	case "CONCAT":
		var result strings.Builder
		for _, arg := range evalArgs {
			if arg != nil {
				result.WriteString(fmt.Sprintf("%v", arg))
				if result.Len() > maxStringResultLen {
					return nil, fmt.Errorf("CONCAT result exceeds maximum length")
				}
			}
		}
		return result.String(), nil

	case "ABS":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("ABS requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			if f < 0 {
				return -f, nil
			}
			return f, nil
		}
		return evalArgs[0], nil

	case "ROUND":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("ROUND requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		f, ok := toFloat64(evalArgs[0])
		if !ok {
			return evalArgs[0], nil
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
			return float64(int64(result)), nil
		}
		return result, nil

	case "FLOOR":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("FLOOR requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Floor(f), nil
		}
		return evalArgs[0], nil

	case "CEIL", "CEILING":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("CEIL requires at least 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return math.Ceil(f), nil
		}
		return evalArgs[0], nil

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

	case "REPLACE":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("REPLACE requires 3 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil || evalArgs[2] == nil {
			return nil, nil
		}
		str, ok := evalArgs[0].(string)
		if !ok {
			str = fmt.Sprintf("%v", evalArgs[0])
		}
		old, ok2 := evalArgs[1].(string)
		if !ok2 {
			old = fmt.Sprintf("%v", evalArgs[1])
		}
		if old == "" {
			return str, nil
		}
		newStr, ok3 := evalArgs[2].(string)
		if !ok3 {
			newStr = fmt.Sprintf("%v", evalArgs[2])
		}
		result := strings.ReplaceAll(str, old, newStr)
		if len(result) > maxStringResultLen {
			return nil, fmt.Errorf("REPLACE result exceeds maximum length")
		}
		return result, nil

	case "INSTR":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("INSTR requires 2 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return nil, nil // SQL standard: NULL input returns NULL
		}
		haystack, ok := evalArgs[0].(string)
		if !ok {
			haystack = fmt.Sprintf("%v", evalArgs[0])
		}
		needle, ok := evalArgs[1].(string)
		if !ok {
			needle = fmt.Sprintf("%v", evalArgs[1])
		}
		idx := strings.Index(haystack, needle)
		if idx < 0 {
			return float64(0), nil
		}
		return float64(idx + 1), nil

	case "PRINTF":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("PRINTF requires at least 1 argument")
		}
		format, ok := evalArgs[0].(string)
		if !ok {
			format = fmt.Sprintf("%v", evalArgs[0])
		}
		// Simple printf implementation
		var result strings.Builder
		argIndex := 1
		i := 0
		for i < len(format) {
			if format[i] == '%' && i+1 < len(format) {
				nextChar := format[i+1]
				switch nextChar {
				case 's':
					if argIndex < len(evalArgs) {
						result.WriteString(fmt.Sprintf("%v", evalArgs[argIndex]))
						argIndex++
					}
					i += 2
				case 'd', 'i':
					if argIndex < len(evalArgs) {
						if f, ok := toFloat64(evalArgs[argIndex]); ok {
							result.WriteString(fmt.Sprintf("%d", int64(f)))
						}
						argIndex++
					}
					i += 2
				case 'f':
					if argIndex < len(evalArgs) {
						if f, ok := toFloat64(evalArgs[argIndex]); ok {
							result.WriteString(fmt.Sprintf("%f", f))
						}
						argIndex++
					}
					i += 2
				default:
					result.WriteByte(format[i])
					i++
				}
			} else {
				result.WriteByte(format[i])
				i++
			}
		}
		return result.String(), nil

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
		return fmt.Sprintf("%v", evalArgs[1]), nil

	case "CAST":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("CAST requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		targetType, ok := evalArgs[1].(string)
		if !ok {
			targetType = strings.ToUpper(fmt.Sprintf("%v", evalArgs[1]))
		}
		switch targetType {
		case "INTEGER", "INT":
			if f, ok := toFloat64(evalArgs[0]); ok {
				return int64(f), nil
			}
			if s, ok := evalArgs[0].(string); ok {
				i, err := strconv.ParseInt(s, 10, 64)
				if err == nil {
					return i, nil
				}
				// Try parsing as float and truncate
				if f, err := strconv.ParseFloat(s, 64); err == nil {
					return int64(f), nil
				}
				return int64(0), nil
			}
			if b, ok := evalArgs[0].(bool); ok {
				if b {
					return int64(1), nil
				}
				return int64(0), nil
			}
		case "REAL", "FLOAT":
			if f, ok := toFloat64(evalArgs[0]); ok {
				return f, nil
			}
			if s, ok := evalArgs[0].(string); ok {
				f, err := strconv.ParseFloat(s, 64)
				if err == nil {
					return f, nil
				}
				return 0.0, nil
			}
		case "TEXT", "STRING":
			return fmt.Sprintf("%v", evalArgs[0]), nil
		case "BOOLEAN", "BOOL":
			if b, ok := evalArgs[0].(bool); ok {
				return b, nil
			}
			if f, ok := toFloat64(evalArgs[0]); ok {
				return f != 0, nil
			}
			if s, ok := evalArgs[0].(string); ok {
				return strings.ToLower(s) == "true" || s == "1", nil
			}
		}
		return evalArgs[0], nil

	case "CONCAT_WS":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("CONCAT_WS requires at least 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		separator := fmt.Sprintf("%v", evalArgs[0])
		var parts []string
		for _, arg := range evalArgs[1:] {
			if arg != nil {
				parts = append(parts, fmt.Sprintf("%v", arg))
			}
		}
		result := strings.Join(parts, separator)
		if len(result) > maxStringResultLen {
			return nil, fmt.Errorf("CONCAT_WS result exceeds maximum length")
		}
		return result, nil

	case "GROUP_CONCAT":
		// GROUP_CONCAT is handled in aggregate path; scalar fallback just returns the value
		if len(evalArgs) >= 1 && evalArgs[0] != nil {
			return fmt.Sprintf("%v", evalArgs[0]), nil
		}
		return nil, nil

	case "REVERSE":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("REVERSE requires 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		runes := []rune(str)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil

	case "REPEAT":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("REPEAT requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		count, _ := toFloat64(evalArgs[1])
		if count <= 0 {
			return "", nil
		}
		if int(count) > maxStringResultLen/(len(str)+1) {
			return nil, fmt.Errorf("REPEAT result exceeds maximum allowed size (%d bytes)", maxStringResultLen)
		}
		return strings.Repeat(str, int(count)), nil

	case "LEFT":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("LEFT requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		n, _ := toFloat64(evalArgs[1])
		ni := int(n)
		if ni <= 0 {
			return "", nil
		}
		if ni >= len(str) {
			return str, nil
		}
		return str[:ni], nil

	case "RIGHT":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("RIGHT requires 2 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		n, _ := toFloat64(evalArgs[1])
		ni := int(n)
		if ni <= 0 {
			return "", nil
		}
		if ni >= len(str) {
			return str, nil
		}
		return str[len(str)-ni:], nil

	case "LPAD":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("LPAD requires 3 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		targetLen, _ := toFloat64(evalArgs[1])
		pad := fmt.Sprintf("%v", evalArgs[2])
		ti := int(targetLen)
		if ti <= 0 {
			return "", nil
		}
		if ti > maxStringResultLen {
			return nil, fmt.Errorf("LPAD result exceeds maximum allowed size (%d bytes)", maxStringResultLen)
		}
		if len(pad) == 0 {
			if len(str) >= ti {
				return str[:ti], nil
			}
			return str, nil
		}
		if len(str) >= ti {
			return str[:ti], nil
		}
		for len(str) < ti {
			str = pad + str
		}
		return str[len(str)-ti:], nil

	case "RPAD":
		if len(evalArgs) < 3 {
			return nil, fmt.Errorf("RPAD requires 3 arguments")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		targetLen, _ := toFloat64(evalArgs[1])
		pad := fmt.Sprintf("%v", evalArgs[2])
		ti := int(targetLen)
		if ti <= 0 {
			return "", nil
		}
		if ti > maxStringResultLen {
			return nil, fmt.Errorf("RPAD result exceeds maximum allowed size (%d bytes)", maxStringResultLen)
		}
		if len(pad) == 0 {
			if len(str) >= ti {
				return str[:ti], nil
			}
			return str, nil
		}
		if len(str) >= ti {
			return str[:ti], nil
		}
		for len(str) < ti {
			str = str + pad
		}
		return str[:ti], nil

	case "HEX":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("HEX requires 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		if f, ok := toFloat64(evalArgs[0]); ok {
			return fmt.Sprintf("%X", int64(f)), nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		return fmt.Sprintf("%X", []byte(str)), nil

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
		return float64(rand.Int63()), nil

	case "UNICODE":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("UNICODE requires 1 argument")
		}
		if evalArgs[0] == nil {
			return nil, nil
		}
		str := fmt.Sprintf("%v", evalArgs[0])
		if len(str) == 0 {
			return nil, nil
		}
		return float64([]rune(str)[0]), nil

	case "CHAR":
		var result strings.Builder
		for _, arg := range evalArgs {
			if arg != nil {
				if f, ok := toFloat64(arg); ok {
					result.WriteRune(rune(int(f)))
				}
			}
		}
		return result.String(), nil

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

	case "QUOTE":
		if len(evalArgs) < 1 {
			return nil, fmt.Errorf("QUOTE requires 1 argument")
		}
		if evalArgs[0] == nil {
			return "NULL", nil
		}
		if s, ok := evalArgs[0].(string); ok {
			return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
		}
		return fmt.Sprintf("%v", evalArgs[0]), nil

	case "GLOB":
		if len(evalArgs) < 2 {
			return nil, fmt.Errorf("GLOB requires 2 arguments")
		}
		if evalArgs[0] == nil || evalArgs[1] == nil {
			return nil, nil
		}
		pattern := fmt.Sprintf("%v", evalArgs[0])
		str := fmt.Sprintf("%v", evalArgs[1])
		// Simple glob: * matches any, ? matches single char
		regexPattern := "^" + strings.ReplaceAll(strings.ReplaceAll(
			regexp.QuoteMeta(pattern), `\*`, ".*"), `\?`, ".") + "$"
		matched, _ := regexp.MatchString(regexPattern, str)
		return matched, nil

	case "COSINE_SIMILARITY", "COSINE_SIMILARIT":
		if len(evalArgs) != 2 {
			return nil, fmt.Errorf("COSINE_SIMILARITY requires exactly 2 arguments")
		}
		v1, err := toVector(evalArgs[0])
		if err != nil {
			return nil, fmt.Errorf("COSINE_SIMILARITY first argument: %v", err)
		}
		v2, err := toVector(evalArgs[1])
		if err != nil {
			return nil, fmt.Errorf("COSINE_SIMILARITY second argument: %v", err)
		}
		return cosineSimilarity(v1, v2), nil

	case "L2_DISTANCE", "L2_DIST":
		if len(evalArgs) != 2 {
			return nil, fmt.Errorf("L2_DISTANCE requires exactly 2 arguments")
		}
		v1, err := toVector(evalArgs[0])
		if err != nil {
			return nil, fmt.Errorf("L2_DISTANCE first argument: %v", err)
		}
		v2, err := toVector(evalArgs[1])
		if err != nil {
			return nil, fmt.Errorf("L2_DISTANCE second argument: %v", err)
		}
		return l2Distance(v1, v2), nil

	case "INNER_PRODUCT", "DOT_PRODUCT", "DOT":
		if len(evalArgs) != 2 {
			return nil, fmt.Errorf("INNER_PRODUCT requires exactly 2 arguments")
		}
		v1, err := toVector(evalArgs[0])
		if err != nil {
			return nil, fmt.Errorf("INNER_PRODUCT first argument: %v", err)
		}
		v2, err := toVector(evalArgs[1])
		if err != nil {
			return nil, fmt.Errorf("INNER_PRODUCT second argument: %v", err)
		}
		return innerProduct(v1, v2), nil

	default:
		// Check for JSON functions
		return evaluateJSONFunction(funcName, evalArgs)
	}
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
		// Try to parse string as number (SQLite-compatible behavior)
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// evaluateMatchExpr evaluates MATCH ... AGAINST for full-text search
func evaluateMatchExpr(c *Catalog, row []interface{}, columns []ColumnDef, expr *query.MatchExpr, args []interface{}) (interface{}, error) {
	// Get the pattern value
	patternVal, err := evaluateExpression(c, row, columns, expr.Pattern, args)
	if err != nil {
		return nil, err
	}
	if patternVal == nil {
		return false, nil
	}
	pattern := fmt.Sprintf("%v", patternVal)

	// Tokenize the search pattern into words
	searchWords := tokenize(pattern)
	if len(searchWords) == 0 {
		return false, nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

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
							allText = append(allText, strings.ToLower(fmt.Sprintf("%v", row[i])))
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
				word = strings.ToLower(word)
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
			allText = append(allText, strings.ToLower(fmt.Sprintf("%v", colVal)))
		}
	}

	if len(allText) == 0 {
		return false, nil
	}

	// Check if all search words are present
	for _, word := range searchWords {
		word = strings.ToLower(word)
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
