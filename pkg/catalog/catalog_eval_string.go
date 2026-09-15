package catalog

import (
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

type funcResult struct {
	val interface{}
	err error
}

func boundedStringSizeArg(arg interface{}, functionName string, max int) (int, error) {
	n, ok := toFloat64(arg)
	if !ok || n <= 0 {
		return 0, nil
	}
	if math.IsNaN(n) || math.IsInf(n, 0) || n > float64(max) {
		return 0, fmt.Errorf("%s result exceeds maximum allowed size (%d bytes)", functionName, maxStringResultLen)
	}
	return int(n), nil
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

// runeSubstr implements MySQL/SQLite SUBSTR on character (rune) boundaries so
// multibyte UTF-8 input is never split mid-rune (byte slicing produced invalid
// UTF-8, e.g. SUBSTR('héllo',1,2) -> "h\xc3"). start is 1-based; a negative
// start counts from the end. When hasLen is false the substring runs to the end.
func runeSubstr(str string, start int, hasLen bool, length int) string {
	runes := []rune(str)
	n := len(runes)
	var startIdx int
	if start < 0 {
		startIdx = n + start
	} else {
		startIdx = start - 1
	}
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= n {
		return ""
	}
	if !hasLen {
		return string(runes[startIdx:])
	}
	if length < 0 {
		return ""
	}
	end := startIdx + length
	if end > n {
		end = n
	}
	return string(runes[startIdx:end])
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
	if len(evalArgs) >= 3 {
		length, _ := toFloat64(evalArgs[2])
		return funcResult{runeSubstr(str, int(start), true, int(length)), nil}
	}
	return funcResult{runeSubstr(str, int(start), false, 0), nil}
}

func evalStringConcat(evalArgs []interface{}) funcResult {
	var result strings.Builder
	result.Grow(len(evalArgs) * 16)
	for _, arg := range evalArgs {
		// MySQL: CONCAT returns NULL if ANY argument is NULL (unlike CONCAT_WS,
		// which skips NULLs). The old code skipped NULLs here, diverging from the
		// MySQL wire-compat target and from the || operator (which returns NULL).
		if arg == nil {
			return funcResult{nil, nil}
		}
		result.WriteString(ValueToStringKey(arg))
		if result.Len() > maxStringResultLen {
			return funcResult{nil, fmt.Errorf("CONCAT result exceeds maximum length")}
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
	// Character (rune) position, not byte offset (MySQL): INSTR('héllo','l') = 3.
	return funcResult{float64(utf8.RuneCountInString(haystack[:idx]) + 1), nil}
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
	// Character-based positions (MySQL): both the optional start and the returned
	// index count runes, not bytes.
	runes := []rune(haystack)
	start := 0
	if len(evalArgs) >= 3 {
		if f, ok := toFloat64(evalArgs[2]); ok {
			start = int(f) - 1
			if start < 0 {
				start = 0
			}
		}
	}
	if start > len(runes) {
		return funcResult{float64(0), nil}
	}
	sub := string(runes[start:])
	idx := strings.Index(sub, needle)
	if idx < 0 {
		return funcResult{float64(0), nil}
	}
	return funcResult{float64(start + utf8.RuneCountInString(sub[:idx]) + 1), nil}
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

// printfSpec holds the parsed fields of one printf conversion spec:
// %[flags][width][.precision]verb. Width counts runes (the file's
// character-based convention) and is clamped to maxStringResultLen.
type printfSpec struct {
	leftAlign bool // '-'
	plusSign  bool // '+'
	zeroPad   bool // '0'
	spaceSign bool // ' '
	altForm   bool // '#'
	width     int  // minimum field width; 0 = unset
	precision int  // -1 = unset
}

// parsePrintfSpec parses the conversion spec starting at the '%' in
// format[i]. It returns the parsed spec, the verb byte, the index just past
// the verb, and false when the format ends inside the spec (the caller then
// emits a literal '%' and advances one byte, matching the historical behavior
// for dangling specs like "%5").
func parsePrintfSpec(format string, i int) (printfSpec, byte, int, bool) {
	var spec printfSpec
	spec.precision = -1
	j := i + 1
	for j < len(format) && strings.ContainsRune("-+0 #", rune(format[j])) {
		switch format[j] {
		case '-':
			spec.leftAlign = true
		case '+':
			spec.plusSign = true
		case '0':
			spec.zeroPad = true
		case ' ':
			spec.spaceSign = true
		case '#':
			spec.altForm = true
		}
		j++
	}
	for j < len(format) && format[j] >= '0' && format[j] <= '9' {
		if spec.width < maxStringResultLen { // clamp before multiply: no overflow
			spec.width = spec.width*10 + int(format[j]-'0')
			if spec.width > maxStringResultLen {
				spec.width = maxStringResultLen
			}
		}
		j++
	}
	if j < len(format) && format[j] == '.' {
		j++
		spec.precision = 0
		for j < len(format) && format[j] >= '0' && format[j] <= '9' {
			if spec.precision < maxStringResultLen {
				spec.precision = spec.precision*10 + int(format[j]-'0')
				if spec.precision > maxStringResultLen {
					spec.precision = maxStringResultLen
				}
			}
			j++
		}
	}
	if j >= len(format) {
		return spec, 0, 0, false // format ends inside the spec
	}
	return spec, format[j], j + 1, true
}

// isPrintfVerb reports whether verb is a supported conversion character.
func isPrintfVerb(verb byte) bool {
	switch verb {
	case 's', 'q', 'c', 'd', 'i', 'x', 'o', 'f', 'e', 'g':
		return true
	}
	return false
}

// specPad pads body to a minimum rune width: left-aligned appends spaces,
// otherwise leading spaces are added.
func specPad(body string, width int, leftAlign bool) string {
	pad := width - utf8.RuneCountInString(body)
	if pad <= 0 {
		return body
	}
	if leftAlign {
		return body + strings.Repeat(" ", pad)
	}
	return strings.Repeat(" ", pad) + body
}

// specZeroPad widens a rendered numeric field to width by inserting zeros
// after any leading sign and after a 0x/0X alt-form prefix, so "%05d" of -42
// renders "-0042" and "%#08x" of 255 renders "0x0000ff".
func specZeroPad(body string, width int) string {
	pad := width - utf8.RuneCountInString(body)
	if pad <= 0 {
		return body
	}
	sign := ""
	digits := body
	if len(digits) > 0 && (digits[0] == '-' || digits[0] == '+' || digits[0] == ' ') {
		sign, digits = digits[:1], digits[1:]
	}
	if len(digits) >= 2 && digits[0] == '0' && (digits[1] == 'x' || digits[1] == 'X') {
		sign += digits[:2]
		digits = digits[2:]
	}
	return sign + strings.Repeat("0", pad) + digits
}

// formatPrintfInteger renders x for d/i (base 10), x (base 16, lowercase), or
// o (base 8). d/i honor the +/space sign flags; x/o are unsigned (C/SQLite:
// no sign, two's-complement magnitude). '#' adds a 0x prefix for nonzero hex
// and a leading 0 for octal. Precision is a minimum digit count ("%.0d" of 0
// renders empty); the 0 flag is honored only when no precision is given.
func formatPrintfInteger(spec printfSpec, verb byte, x int64) string {
	base := 10
	switch verb {
	case 'x':
		base = 16
	case 'o':
		base = 8
	}
	signed := verb == 'd' || verb == 'i'
	neg := signed && x < 0
	var magnitude uint64
	if signed && x < 0 {
		magnitude = uint64(-(x + 1)) + 1 // #nosec G115 -- MinInt64-safe |x|: -(x+1) >= 0 for every int64 x, so no wrap
	} else {
		magnitude = uint64(x) // #nosec G115 -- %x/%o are unsigned conversions (C/SQLite): deliberate two's-complement bit pattern
	}
	digits := strconv.FormatUint(magnitude, base)
	sign := ""
	switch {
	case neg:
		sign = "-"
	case spec.plusSign:
		sign = "+"
	case spec.spaceSign:
		sign = " "
	}
	prefix := ""
	if spec.altForm && x != 0 {
		switch verb {
		case 'x':
			prefix = "0x"
		case 'o':
			if !strings.HasPrefix(digits, "0") {
				prefix = "0"
			}
		}
	}
	if spec.precision >= 0 {
		if spec.precision == 0 && x == 0 {
			digits = ""
		} else if len(digits) < spec.precision {
			digits = strings.Repeat("0", spec.precision-len(digits)) + digits
		}
	}
	body := sign + prefix + digits
	if utf8.RuneCountInString(body) >= spec.width {
		return body
	}
	if spec.leftAlign {
		return body + strings.Repeat(" ", spec.width-utf8.RuneCountInString(body))
	}
	if spec.zeroPad && spec.precision < 0 {
		return specZeroPad(body, spec.width)
	}
	return strings.Repeat(" ", spec.width-utf8.RuneCountInString(body)) + body
}

// formatPrintfFloat renders f, e, or g with the given precision (default 6;
// an explicit 0 precision for g behaves as 1, per C). The +/space flags apply
// to non-negative values, the 0 flag zero-pads after the sign, and '#' keeps
// a bare decimal point for %f/%e with precision 0 (ignored for %g).
func formatPrintfFloat(spec printfSpec, verb byte, f float64) string {
	prec := spec.precision
	if prec < 0 {
		prec = 6
	}
	var body string
	switch verb {
	case 'e':
		body = strconv.FormatFloat(f, 'e', prec, 64)
		if spec.altForm && prec == 0 {
			body = strings.Replace(body, "e", ".e", 1)
		}
	case 'g':
		p := prec
		if p == 0 {
			p = 1
		}
		body = strconv.FormatFloat(f, 'g', p, 64)
	default: // 'f'
		body = strconv.FormatFloat(f, 'f', prec, 64)
		if spec.altForm && prec == 0 {
			body += "."
		}
	}
	if f >= 0 {
		switch {
		case spec.plusSign:
			body = "+" + body
		case spec.spaceSign:
			body = " " + body
		}
	}
	if utf8.RuneCountInString(body) >= spec.width {
		return body
	}
	if spec.leftAlign {
		return body + strings.Repeat(" ", spec.width-utf8.RuneCountInString(body))
	}
	if spec.zeroPad {
		return specZeroPad(body, spec.width)
	}
	return strings.Repeat(" ", spec.width-utf8.RuneCountInString(body)) + body
}

// formatPrintfString applies precision (maximum runes) and width to a %s/%q
// body. Width and precision count runes, matching the character-based
// convention of the other string functions in this file.
func formatPrintfString(spec printfSpec, body string) string {
	if spec.precision >= 0 {
		runes := []rune(body)
		if len(runes) > spec.precision {
			runes = runes[:spec.precision]
		}
		body = string(runes)
	}
	return specPad(body, spec.width, spec.leftAlign)
}

// evalStringPrintf implements PRINTF(format, ...) with C/SQLite-style
// conversion specs: %s %q %c %d %i %x %o %f %e %g, the - + 0 space # flags,
// a minimum width, and a precision. %% emits one literal % and consumes no
// argument; an unknown or dangling spec emits a literal % and its characters
// are processed as ordinary text; a spec whose argument is missing renders
// empty. Width and precision count runes, not bytes.
func evalStringPrintf(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("PRINTF requires at least 1 argument")}
	}
	format, _ := argString(evalArgs, 0)
	var result strings.Builder
	result.Grow(len(format) + len(evalArgs)*16)
	argIndex := 1
	nextArg := func() (interface{}, bool) {
		if argIndex >= len(evalArgs) {
			return nil, false
		}
		arg := evalArgs[argIndex]
		argIndex++
		return arg, true
	}
	i := 0
	for i < len(format) {
		if format[i] != '%' {
			result.WriteByte(format[i])
			i++
			continue
		}
		if i+1 >= len(format) {
			result.WriteByte('%') // trailing lone %
			i++
			continue
		}
		if format[i+1] == '%' {
			result.WriteByte('%') // %% escape: one literal %, no argument consumed
			i += 2
			continue
		}
		spec, verb, next, ok := parsePrintfSpec(format, i)
		if !ok || !isPrintfVerb(verb) {
			// Dangling spec ("%5" at end of format) or unknown verb ("%z"):
			// a literal %, with the remaining bytes processed as ordinary
			// text and no argument consumed.
			result.WriteByte('%')
			i++
			continue
		}
		i = next
		arg, haveArg := nextArg()
		switch verb {
		case 's':
			if haveArg {
				result.WriteString(formatPrintfString(spec, ValueToStringKey(arg)))
			}
		case 'q':
			if haveArg {
				escaped := strings.ReplaceAll(ValueToStringKey(arg), "'", "''")
				result.WriteString(formatPrintfString(spec, escaped))
			}
		case 'c':
			if haveArg {
				var body string
				if f, okF := toFloat64(arg); okF && f >= 0 && f <= utf8.MaxRune && math.Trunc(f) == f {
					body = string(rune(f))
				}
				result.WriteString(specPad(body, spec.width, spec.leftAlign))
			}
		case 'd', 'i', 'x', 'o':
			if haveArg {
				if f, okF := toFloat64(arg); okF {
					result.WriteString(formatPrintfInteger(spec, verb, int64(f)))
				}
			}
		case 'f', 'e', 'g':
			if haveArg {
				if f, okF := toFloat64(arg); okF {
					result.WriteString(formatPrintfFloat(spec, verb, f))
				}
			}
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
	maxCount := maxStringResultLen
	if len(str) > 0 {
		maxCount = maxStringResultLen / len(str)
	}
	count, err := boundedStringSizeArg(evalArgs[1], "REPEAT", maxCount)
	if err != nil {
		return funcResult{nil, err}
	}
	if count <= 0 {
		return funcResult{"", nil}
	}
	return funcResult{strings.Repeat(str, count), nil}
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
	// Character-based (MySQL): LEFT('héllo',2) = "hé", not a byte prefix that
	// splits the é into invalid UTF-8.
	runes := []rune(str)
	if ni >= len(runes) {
		return funcResult{str, nil}
	}
	return funcResult{string(runes[:ni]), nil}
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
	// Character-based (MySQL): RIGHT('héllo',2) = "lo" on rune boundaries.
	runes := []rune(str)
	if ni >= len(runes) {
		return funcResult{str, nil}
	}
	return funcResult{string(runes[len(runes)-ni:]), nil}
}

func evalStringLPad(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 3 {
		return funcResult{nil, fmt.Errorf("LPAD requires 3 arguments")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	pad := ValueToStringKey(evalArgs[2])
	ti, err := boundedStringSizeArg(evalArgs[1], "LPAD", maxStringResultLen)
	if err != nil {
		return funcResult{nil, err}
	}
	if ti <= 0 {
		return funcResult{"", nil}
	}
	// Character-based (MySQL): len, truncation, and padding count runes, not
	// bytes, so the result is always valid UTF-8 and the padded length is in
	// characters (LPAD('hé',1,'x') = "h", LPAD('é',3,'ab') = "abé").
	runes := []rune(str)
	if len(runes) >= ti {
		return funcResult{string(runes[:ti]), nil}
	}
	if pad == "" {
		return funcResult{str, nil}
	}
	// MySQL fills the gap with whole pad copies and truncates the FINAL
	// PARTIAL copy to its FIRST characters (LPAD('a',2,'xyz') = "xa",
	// LPAD('hi',5,'??') = "???hi") — never to padstr's tail.
	padRunes := []rune(pad)
	gap := ti - len(runes)
	full, rem := gap/len(padRunes), gap%len(padRunes)
	prefix := make([]rune, 0, gap)
	for i := 0; i < full; i++ {
		prefix = append(prefix, padRunes...)
	}
	prefix = append(prefix, padRunes[:rem]...)
	return funcResult{string(append(prefix, runes...)), nil}
}

func evalStringRPad(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 3 {
		return funcResult{nil, fmt.Errorf("RPAD requires 3 arguments")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	str := ValueToStringKey(evalArgs[0])
	pad := ValueToStringKey(evalArgs[2])
	ti, err := boundedStringSizeArg(evalArgs[1], "RPAD", maxStringResultLen)
	if err != nil {
		return funcResult{nil, err}
	}
	if ti <= 0 {
		return funcResult{"", nil}
	}
	// Character-based (MySQL): len, truncation, and padding count runes, not
	// bytes, so the result is always valid UTF-8 and the padded length is in
	// characters (RPAD('héé',2,'x') = "hé", RPAD('é',4,'x') = "éxxx").
	runes := []rune(str)
	if len(runes) >= ti {
		return funcResult{string(runes[:ti]), nil}
	}
	if pad == "" {
		return funcResult{str, nil}
	}
	for len(runes) < ti {
		runes = append(runes, []rune(pad)...)
	}
	return funcResult{string(runes[:ti]), nil}
}

func evalStringHex(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 1 {
		return funcResult{nil, fmt.Errorf("HEX requires 1 argument")}
	}
	if evalArgs[0] == nil {
		return funcResult{nil, nil}
	}
	// A string argument is byte-encoded (MySQL/SQLite): HEX('41') = "3431", not
	// "29". Check the concrete type FIRST — toFloat64 would happily parse a
	// numeric-looking string like '41' as the number 41 and take the wrong branch.
	if s, ok := toString(evalArgs[0]); ok {
		return funcResult{strings.ToUpper(hex.EncodeToString([]byte(s))), nil}
	}
	if f, ok := toFloat64(evalArgs[0]); ok {
		// Numeric HEX treats the value as an unsigned 64-bit integer (MySQL):
		// HEX(-1) is "FFFFFFFFFFFFFFFF", not "-1". FormatInt base-16 emitted a
		// signed "-"-prefixed string, which is never valid hex.
		// #nosec G115 -- intentional unsigned reinterpretation for MySQL HEX semantics (HEX(-1) = "FFFFFFFFFFFFFFFF")
		return funcResult{strings.ToUpper(strconv.FormatUint(uint64(int64(f)), 16)), nil}
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

// globRegexCache caches compiled GLOB patterns so repeated evaluations of the
// same pattern do not pay a regexp compile per row. Bounded: once full, new
// patterns are compiled per call instead of being stored (compiled regexes
// are immutable and safe for concurrent use, so sync.Map suffices).
var globRegexCache sync.Map // pattern string -> *regexp.Regexp

var globRegexCacheSize atomic.Int64

// maxGlobRegexCacheEntries bounds the compiled-pattern cache; beyond it,
// patterns are compiled per call (correctness is unaffected).
const maxGlobRegexCacheEntries = 1024

// evalStringGlob implements GLOB(pattern, str) with SQLite-style file-globbing
// semantics (docs/MYSQL_COMPATIBILITY.md): * matches any run of characters,
// ? exactly one character, and [...] a character class with ranges ([a-z])
// and ^ negation ([^0-9]); a ] directly after [ or [^ is a literal member and
// an unterminated [ is a literal. Matching is case-sensitive. A pattern whose
// class can never match anything (e.g. the reversed range [z-a]) yields false
// rather than an error, as in SQLite.
func evalStringGlob(evalArgs []interface{}) funcResult {
	if len(evalArgs) < 2 {
		return funcResult{nil, fmt.Errorf("GLOB requires 2 arguments")}
	}
	if evalArgs[0] == nil || evalArgs[1] == nil {
		return funcResult{nil, nil}
	}
	pattern := ValueToStringKey(evalArgs[0])
	str := ValueToStringKey(evalArgs[1])
	re := globRegexFor(pattern)
	if re == nil {
		// Uncompilable pattern (e.g. a reversed character range): matches nothing.
		return funcResult{false, nil}
	}
	return funcResult{re.MatchString(str), nil}
}

// globRegexFor returns the compiled regex for a GLOB pattern, or nil when the
// pattern cannot compile (which, as in SQLite, can never match).
func globRegexFor(pattern string) *regexp.Regexp {
	if re, ok := globRegexCache.Load(pattern); ok {
		return re.(*regexp.Regexp)
	}
	re, err := regexp.Compile(globToRegex(pattern))
	if err != nil {
		return nil
	}
	if globRegexCacheSize.Add(1) <= maxGlobRegexCacheEntries {
		globRegexCache.Store(pattern, re)
	} else {
		globRegexCacheSize.Add(-1)
	}
	return re
}

// globToRegex converts a SQLite-style GLOB pattern into an anchored regular
// expression. * -> .*, ? -> ., bracket expressions via globBracketClass,
// everything else matched literally. ^/$ anchor the whole pattern (Go's $ is
// end-of-text, so there is no trailing-newline surprise).
func globToRegex(pattern string) string {
	runes := []rune(pattern)
	var b strings.Builder
	b.Grow(len(runes) + 8)
	b.WriteByte('^')
	for i := 0; i < len(runes); {
		switch runes[i] {
		case '*':
			b.WriteString(".*")
			i++
		case '?':
			b.WriteByte('.')
			i++
		case '[':
			if cls, next, ok := globBracketClass(runes, i); ok {
				b.WriteString(cls)
				i = next
			} else {
				b.WriteString(`\[`) // unterminated [ matches a literal [
				i++
			}
		default:
			b.WriteString(regexp.QuoteMeta(string(runes[i])))
			i++
		}
	}
	b.WriteByte('$')
	return b.String()
}

// globBracketClass converts the bracket expression beginning at runes[start]
// into a RE2 character class. It returns the class text, the index just past
// the closing ], and false when the expression is unterminated (the caller
// then emits a literal [). Ranges (a-z) are passed through; \, ] and [ are
// escaped so pattern data cannot terminate or nest the class; a leading ] is
// a literal member per the fnmatch/SQLite convention ([]] matches "]").
func globBracketClass(runes []rune, start int) (string, int, bool) {
	i := start + 1
	var b strings.Builder
	b.WriteByte('[')
	if i < len(runes) && runes[i] == '^' {
		b.WriteByte('^')
		i++
	}
	if i < len(runes) && runes[i] == ']' {
		b.WriteString(`\]`) // leading ] is a literal class member
		i++
	}
	for i < len(runes) && runes[i] != ']' {
		switch runes[i] {
		case '\\':
			b.WriteString(`\\`)
		case '[':
			b.WriteString(`\[`)
		default:
			b.WriteRune(runes[i])
		}
		i++
	}
	if i >= len(runes) {
		return "", 0, false // unterminated: caller falls back to a literal [
	}
	b.WriteByte(']')
	return b.String(), i + 1, true
}
