package catalog

import (
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

type funcResult struct {
	val interface{}
	err error
}

// evaluateStringFunction handles all string-related SQL functions.
// Returns (result, true) if the function was handled, (zero, false) otherwise.
func evaluateStringFunction(funcName string, evalArgs []interface{}) (funcResult, bool) {
	switch funcName {
	case "LENGTH", "LEN":
		return evalStringLen(evalArgs), true
	case "UPPER":
		return evalStringUpper(evalArgs), true
	case "LOWER":
		return evalStringLower(evalArgs), true
	case "TRIM", "LTRIM", "RTRIM":
		return evalStringTrim(funcName, evalArgs), true
	case "SUBSTR", "SUBSTRING":
		return evalStringSubstr(evalArgs), true
	case "CONCAT":
		return evalStringConcat(evalArgs), true
	case "CONCAT_WS":
		return evalStringConcatWS(evalArgs), true
	case "REPLACE":
		return evalStringReplace(evalArgs), true
	case "INSTR":
		return evalStringInstr(evalArgs), true
	case "LOCATE", "POSITION":
		return evalStringLocate(evalArgs), true
	case "SUBSTRING_INDEX":
		return evalStringSubstringIndex(evalArgs), true
	case "ASCII":
		return evalStringAscii(evalArgs), true
	case "PRINTF":
		return evalStringPrintf(evalArgs), true
	case "REVERSE":
		return evalStringReverse(evalArgs), true
	case "REPEAT":
		return evalStringRepeat(evalArgs), true
	case "LEFT":
		return evalStringLeft(evalArgs), true
	case "RIGHT":
		return evalStringRight(evalArgs), true
	case "LPAD":
		return evalStringLPad(evalArgs), true
	case "RPAD":
		return evalStringRPad(evalArgs), true
	case "HEX":
		return evalStringHex(evalArgs), true
	case "UNICODE":
		return evalStringUnicode(evalArgs), true
	case "CHAR":
		return evalStringChar(evalArgs), true
	case "QUOTE":
		return evalStringQuote(evalArgs), true
	case "GLOB":
		return evalStringGlob(evalArgs), true
	default:
		return funcResult{}, false
	}
}

func argString(evalArgs []interface{}, idx int) (string, bool) {
	if idx >= len(evalArgs) || evalArgs[idx] == nil {
		return "", false
	}
	if s, ok := evalArgs[idx].(string); ok {
		return s, true
	}
	return ValueToStringKey(evalArgs[idx]), true
}

func evalStringLen(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("LENGTH requires at least 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	s, _ := argString(evalArgs, 0)
	return funcResult{float64(len(s)), nil}
}

func evalStringUpper(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("UPPER requires at least 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	s, _ := argString(evalArgs, 0)
	return funcResult{toUpperFast(s), nil}
}

func evalStringLower(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("LOWER requires at least 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	s, _ := argString(evalArgs, 0)
	return funcResult{toLowerFast(s), nil}
}

func evalStringTrim(funcName string, evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("%s requires at least 1 argument", funcName)}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str, _ := argString(evalArgs, 0)
	trimChars := " \t\n\r"
	if len(evalArgs) >= 2 && evalArgs[1] != nil {
		trimChars = ValueToStringKey(evalArgs[1])
	}
	switch funcName {
	case "LTRIM":
		return funcResult{strings.TrimLeft(str, trimChars), nil}
	case "RTRIM":
		return funcResult{strings.TrimRight(str, trimChars), nil}
	default:
		return funcResult{strings.Trim(str, trimChars), nil}
	}
}

func evalStringSubstr(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("SUBSTR requires at least 2 arguments")}
	}
	if evalArgs[0] == nil || evalArgs[1] == nil {
		return funcResult{nil, nil}
	}
	if len(evalArgs) >= 3 && evalArgs[2] == nil {
		return funcResult{nil, nil}
	}
	str, _ := argString(evalArgs, 0)
	start, _ := toFloat64(evalArgs[1])
	startInt := int(start) - 1
	if startInt < 0 {
		startInt = 0
	}
	if startInt >= len(str) {
		return funcResult{"", nil}
	}
	if len(evalArgs) >= 3 {
		length, _ := toFloat64(evalArgs[2])
		lengthInt := int(length)
		if lengthInt < 0 {
			return funcResult{"", nil}
		}
		if startInt+lengthInt > len(str) {
			lengthInt = len(str) - startInt
		}
		return funcResult{str[startInt : startInt+lengthInt], nil}
	}
	return funcResult{str[startInt:], nil}
}

func evalStringConcat(evalArgs []interface{}) funcResult {
	var result strings.Builder
	result.Grow(len(evalArgs) * 16)
	for _, arg := range evalArgs {
		if arg != nil {
			result.WriteString(ValueToStringKey(arg))
			if result.Len() > maxStringResultLen {
				return funcResult{nil, fmt.Errorf("CONCAT result exceeds maximum length")}
			}
		}
	}
	return funcResult{result.String(), nil}
}

func evalStringConcatWS(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("CONCAT_WS requires at least 2 arguments")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	separator := ValueToStringKey(evalArgs[0])
	var parts []string
	for _, arg := range evalArgs[1:] {
		if arg != nil {
			parts = append(parts, ValueToStringKey(arg))
		}
	}
	result := strings.Join(parts, separator)
	if len(result) > maxStringResultLen {
		return funcResult{nil, fmt.Errorf("CONCAT_WS result exceeds maximum length")}
	}
	return funcResult{result, nil}
}

func evalStringReplace(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 3 {
		return funcResult{nil, fmt.Errorf("REPLACE requires 3 arguments")}
	}
	if evalArgs[0] == nil || evalArgs[1] == nil || evalArgs[2] == nil {
		return funcResult{nil, nil}
	}
	str, _ := argString(evalArgs, 0)
	old, _ := argString(evalArgs, 1)
	if old == "" {
		return funcResult{str, nil}
	}
	newStr, _ := argString(evalArgs, 2)
	result := strings.ReplaceAll(str, old, newStr)
	if len(result) > maxStringResultLen {
		return funcResult{nil, fmt.Errorf("REPLACE result exceeds maximum length")}
	}
	return funcResult{result, nil}
}

func evalStringInstr(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("INSTR requires 2 arguments")}
	}
	if evalArgs[0] == nil || evalArgs[1] == nil {
		return funcResult{nil, nil}
	}
	haystack, _ := argString(evalArgs, 0)
	needle, _ := argString(evalArgs, 1)
	idx := strings.Index(haystack, needle)
	if idx < 0 {
		return funcResult{float64(0), nil}
	}
	return funcResult{float64(idx + 1), nil}
}

// evalStringLocate implements LOCATE(substr, str [, pos]) and POSITION(substr, str):
// 1-based index of the first occurrence of substr in str, 0 if not found.
func evalStringLocate(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("LOCATE requires at least 2 arguments")}
	}
	if evalArgs[0] == nil || evalArgs[1] == nil {
		return funcResult{nil, nil}
	}
	needle, _ := argString(evalArgs, 0)
	haystack, _ := argString(evalArgs, 1)
	start := 0
	if len(evalArgs) >= 3 {
		if f, ok := toFloat64(evalArgs[2]); ok {
			start = int(f) - 1
			if start < 0 {
				start = 0
			}
		}
	}
	if start > len(haystack) {
		return funcResult{float64(0), nil}
	}
	idx := strings.Index(haystack[start:], needle)
	if idx < 0 {
		return funcResult{float64(0), nil}
	}
	return funcResult{float64(start + idx + 1), nil}
}

// evalStringSubstringIndex implements SUBSTRING_INDEX(str, delim, count): the
// substring before the count-th delimiter (from the left if count > 0, from the
// right if count < 0).
func evalStringSubstringIndex(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 3 {
		return funcResult{nil, fmt.Errorf("SUBSTRING_INDEX requires 3 arguments")}
	}
	if evalArgs[0] == nil || evalArgs[1] == nil || evalArgs[2] == nil {
		return funcResult{nil, nil}
	}
	str, _ := argString(evalArgs, 0)
	delim, _ := argString(evalArgs, 1)
	cf, _ := toFloat64(evalArgs[2])
	count := int(cf)
	if delim == "" || count == 0 {
		return funcResult{"", nil}
	}
	parts := strings.Split(str, delim)
	if count > 0 {
		if count >= len(parts) {
			return funcResult{str, nil}
		}
		return funcResult{strings.Join(parts[:count], delim), nil}
	}
	count = -count
	if count >= len(parts) {
		return funcResult{str, nil}
	}
	return funcResult{strings.Join(parts[len(parts)-count:], delim), nil}
}

// evalStringAscii implements ASCII(str): the numeric code of the first byte, 0
// for an empty string.
func evalStringAscii(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("ASCII requires 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	s := ValueToStringKey(evalArgs[0])
	if len(s) == 0 {
		return funcResult{float64(0), nil}
	}
	return funcResult{float64(s[0]), nil}
}

func evalStringPrintf(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("PRINTF requires at least 1 argument")}
	}
	format, _ := argString(evalArgs, 0)
	var result strings.Builder
	result.Grow(len(format) + len(evalArgs)*16)
	argIndex := 1
	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			nextChar := format[i+1]
			switch nextChar {
			case 's':
				if argIndex < len(evalArgs) {
					result.WriteString(ValueToStringKey(evalArgs[argIndex]))
					argIndex++
				}
				i += 2
			case 'd', 'i':
				if argIndex < len(evalArgs) {
					if f, ok := toFloat64(evalArgs[argIndex]); ok {
						result.WriteString(strconv.FormatInt(int64(f), 10))
					}
					argIndex++
				}
				i += 2
			case 'f':
				if argIndex < len(evalArgs) {
					if f, ok := toFloat64(evalArgs[argIndex]); ok {
						result.WriteString(strconv.FormatFloat(f, 'f', 6, 64))
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
	return funcResult{result.String(), nil}
}

func evalStringReverse(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("REVERSE requires 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return funcResult{string(runes), nil}
}

func evalStringRepeat(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("REPEAT requires 2 arguments")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	count, _ := toFloat64(evalArgs[1])
	if count <= 0 {
		return funcResult{"", nil}
	}
	if int(count) > maxStringResultLen/(len(str)+1) {
		return funcResult{nil, fmt.Errorf("REPEAT result exceeds maximum allowed size (%d bytes)", maxStringResultLen)}
	}
	return funcResult{strings.Repeat(str, int(count)), nil}
}

func evalStringLeft(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("LEFT requires 2 arguments")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	n, _ := toFloat64(evalArgs[1])
	ni := int(n)
	if ni <= 0 {
		return funcResult{"", nil}
	}
	if ni >= len(str) {
		return funcResult{str, nil}
	}
	return funcResult{str[:ni], nil}
}

func evalStringRight(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("RIGHT requires 2 arguments")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	n, _ := toFloat64(evalArgs[1])
	ni := int(n)
	if ni <= 0 {
		return funcResult{"", nil}
	}
	if ni >= len(str) {
		return funcResult{str, nil}
	}
	return funcResult{str[len(str)-ni:], nil}
}

func evalStringLPad(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 3 {
		return funcResult{nil, fmt.Errorf("LPAD requires 3 arguments")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	targetLen, _ := toFloat64(evalArgs[1])
	pad := ValueToStringKey(evalArgs[2])
	ti := int(targetLen)
	if ti <= 0 {
		return funcResult{"", nil}
	}
	if ti > maxStringResultLen {
		return funcResult{nil, fmt.Errorf("LPAD result exceeds maximum allowed size (%d bytes)", maxStringResultLen)}
	}
	if len(pad) == 0 {
		if len(str) >= ti {
			return funcResult{str[:ti], nil}
		}
		return funcResult{str, nil}
	}
	if len(str) >= ti {
		return funcResult{str[:ti], nil}
	}
	for len(str) < ti {
		str = pad + str
	}
	return funcResult{str[len(str)-ti:], nil}
}

func evalStringRPad(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 3 {
		return funcResult{nil, fmt.Errorf("RPAD requires 3 arguments")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	targetLen, _ := toFloat64(evalArgs[1])
	pad := ValueToStringKey(evalArgs[2])
	ti := int(targetLen)
	if ti <= 0 {
		return funcResult{"", nil}
	}
	if ti > maxStringResultLen {
		return funcResult{nil, fmt.Errorf("RPAD result exceeds maximum allowed size (%d bytes)", maxStringResultLen)}
	}
	if len(pad) == 0 {
		if len(str) >= ti {
			return funcResult{str[:ti], nil}
		}
		return funcResult{str, nil}
	}
	if len(str) >= ti {
		return funcResult{str[:ti], nil}
	}
	for len(str) < ti {
		str = str + pad
	}
	return funcResult{str[:ti], nil}
}

func evalStringHex(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("HEX requires 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	if f, ok := toFloat64(evalArgs[0]); ok {
		return funcResult{strings.ToUpper(strconv.FormatInt(int64(f), 16)), nil}
	}
	str := ValueToStringKey(evalArgs[0])
	return funcResult{strings.ToUpper(hex.EncodeToString([]byte(str))), nil}
}

func evalStringUnicode(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("UNICODE requires 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	if len(str) == 0 {
		return funcResult{nil, nil}
	}
	return funcResult{float64([]rune(str)[0]), nil}
}

func evalStringChar(evalArgs []interface{}) funcResult {
	var result strings.Builder
	result.Grow(len(evalArgs) * 4)
	for _, arg := range evalArgs {
		if arg != nil {
			if f, ok := toFloat64(arg); ok {
				if f >= 0 && f <= utf8.MaxRune && math.Trunc(f) == f {
					result.WriteRune(rune(f))
				}
			}
		}
	}
	return funcResult{result.String(), nil}
}

func evalStringQuote(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("QUOTE requires 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{"NULL", nil}
	}
	if s, ok := toString(evalArgs[0]); ok {
		return funcResult{"'" + strings.ReplaceAll(s, "'", "''") + "'", nil}
	}
	return funcResult{ValueToStringKey(evalArgs[0]), nil}
}

func evalStringGlob(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("GLOB requires 2 arguments")}
	}
	if evalArgs[0] == nil || evalArgs[1] == nil {
		return funcResult{nil, nil}
	}
	pattern := ValueToStringKey(evalArgs[0])
	str := ValueToStringKey(evalArgs[1])
	regexPattern := "^" + strings.ReplaceAll(strings.ReplaceAll(
		regexp.QuoteMeta(pattern), `\*`, ".*"), `\?`, ".") + "$"
	matched, _ := regexp.MatchString(regexPattern, str)
	return funcResult{matched, nil}
}
