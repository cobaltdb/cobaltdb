package catalog

import (
	"crypto/md5" // #nosec G501 -- MD5() is a MySQL SQL builtin, not used for security
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- SHA1() is a MySQL SQL builtin, not used for security
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// This file implements MySQL scalar functions that are commonly emitted by
// ORMs and MySQL clients but were missing from the main dispatch table.
// Registering them here via init keeps the large literal in catalog_eval.go
// unchanged; package-level map initialisation runs before init, so the map is
// always populated by the time these entries are added.
func init() {
	for name, handler := range mysqlCompatHandlers {
		scalarFunctionHandlers[name] = handler
	}
	// MySQL's IF(cond, then, else) is IIF under another name. The lazy path in
	// FunctionCall.Evaluate already treats them alike; this covers callers that
	// pre-evaluate arguments and dispatch through the scalar table.
	scalarFunctionHandlers["IF"] = scalarFunctionHandlers["IIF"]
}

// mysqlCompatDateArg resolves an optional date argument, defaulting to now.
// It reports false when an argument was supplied but is NULL or unparseable,
// which MySQL surfaces as a NULL result.
func mysqlCompatDateArg(args []interface{}) (time.Time, bool) {
	if len(args) == 0 || args[0] == nil {
		if len(args) > 0 {
			return time.Time{}, false
		}
		return time.Now(), true
	}
	t, ok := parseFlexibleTime(ValueToStringKey(args[0]))
	if !ok {
		return time.Time{}, false
	}
	return t, true
}

var mysqlCompatHandlers = map[string]functionHandler{
	// ---- string length ----

	// CHAR_LENGTH counts characters, unlike LENGTH which counts bytes.
	"CHAR_LENGTH":      charLengthHandler,
	"CHARACTER_LENGTH": charLengthHandler,

	"BIT_LENGTH": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		return int64(len(ValueToStringKey(args[0]))) * 8, nil
	},

	// ORD returns the numeric value of the leftmost character. For a
	// multi-byte character MySQL folds the bytes big-endian:
	// (b0<<16)+(b1<<8)+b2 for a 3-byte sequence.
	"ORD": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		s := ValueToStringKey(args[0])
		if s == "" {
			return int64(0), nil
		}
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && size <= 1 {
			return int64(s[0]), nil
		}
		var v int64
		for i := 0; i < size; i++ {
			v = v<<8 | int64(s[i])
		}
		return v, nil
	},

	"SPACE": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		n, err := boundedStringSizeArg(args[0], "SPACE", maxStringResultLen)
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return "", nil
		}
		return strings.Repeat(" ", n), nil
	},

	// ELT(N, str1, str2, ...) returns the N-th string (1-based).
	"ELT": func(args []interface{}) (interface{}, error) {
		if len(args) < 2 || args[0] == nil {
			return nil, nil
		}
		n, ok := toFloat64(args[0])
		if !ok {
			return nil, nil
		}
		idx := int(n)
		if idx < 1 || idx > len(args)-1 {
			return nil, nil
		}
		if args[idx] == nil {
			return nil, nil
		}
		return ValueToStringKey(args[idx]), nil
	},

	// FIELD(str, s1, s2, ...) returns the 1-based index of the first match,
	// or 0 when there is none.
	"FIELD": func(args []interface{}) (interface{}, error) {
		if len(args) < 2 || args[0] == nil {
			return int64(0), nil
		}
		needle := ValueToStringKey(args[0])
		for i := 1; i < len(args); i++ {
			if args[i] == nil {
				continue
			}
			if ValueToStringKey(args[i]) == needle {
				return int64(i), nil
			}
		}
		return int64(0), nil
	},

	// ---- encoding / radix ----

	"UNHEX": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		b, err := hex.DecodeString(ValueToStringKey(args[0]))
		if err != nil {
			// MySQL returns NULL for non-hex input rather than erroring.
			return nil, nil
		}
		return string(b), nil
	},

	"BIN": func(args []interface{}) (interface{}, error) {
		return formatRadix(args, 2)
	},
	"OCT": func(args []interface{}) (interface{}, error) {
		return formatRadix(args, 8)
	},

	"TO_BASE64": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		return base64.StdEncoding.EncodeToString([]byte(ValueToStringKey(args[0]))), nil
	},
	"FROM_BASE64": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		b, err := base64.StdEncoding.DecodeString(ValueToStringKey(args[0]))
		if err != nil {
			return nil, nil
		}
		return string(b), nil
	},

	// ---- hashing ----

	"MD5": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		sum := md5.Sum([]byte(ValueToStringKey(args[0]))) // #nosec G401 -- SQL builtin, not security
		return hex.EncodeToString(sum[:]), nil
	},
	"SHA1": sha1Handler,
	"SHA":  sha1Handler,
	"SHA2": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		width := 256
		if len(args) >= 2 && args[1] != nil {
			if f, ok := toFloat64(args[1]); ok && f != 0 {
				width = int(f)
			}
		}
		data := []byte(ValueToStringKey(args[0]))
		switch width {
		case 224:
			sum := sha256.Sum224(data)
			return hex.EncodeToString(sum[:]), nil
		case 256:
			sum := sha256.Sum256(data)
			return hex.EncodeToString(sum[:]), nil
		case 384:
			sum := sha512.Sum384(data)
			return hex.EncodeToString(sum[:]), nil
		case 512:
			sum := sha512.Sum512(data)
			return hex.EncodeToString(sum[:]), nil
		default:
			// MySQL returns NULL for unsupported widths.
			return nil, nil
		}
	},
	"CRC32": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		return int64(crc32.ChecksumIEEE([]byte(ValueToStringKey(args[0])))), nil
	},

	// ---- random / identity ----

	// RAND returns a float in [0,1). crypto/rand is used so the result is not
	// predictable; MySQL's seeded RAND(N) reproducibility is not supported.
	"RAND": func(args []interface{}) (interface{}, error) {
		const precision = 1 << 53
		n, err := rand.Int(rand.Reader, big.NewInt(precision))
		if err != nil {
			return float64(0), nil
		}
		return float64(n.Int64()) / float64(precision), nil
	},

	"UUID": func(args []interface{}) (interface{}, error) {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, fmt.Errorf("UUID: %w", err)
		}
		b[6] = (b[6] & 0x0f) | 0x40 // version 4
		b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
		return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
	},

	// ---- NULL handling ----

	"ISNULL": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 {
			return nil, fmt.Errorf("ISNULL requires 1 argument")
		}
		if args[0] == nil {
			return int64(1), nil
		}
		return int64(0), nil
	},

	// ---- date / time ----

	"CURDATE": func(args []interface{}) (interface{}, error) {
		return time.Now().Format("2006-01-02"), nil
	},
	"CURTIME": func(args []interface{}) (interface{}, error) {
		return time.Now().Format("15:04:05"), nil
	},

	"UNIX_TIMESTAMP": func(args []interface{}) (interface{}, error) {
		t, ok := mysqlCompatDateArg(args)
		if !ok {
			return nil, nil
		}
		return t.Unix(), nil
	},

	"FROM_UNIXTIME": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		secs, ok := toFloat64(args[0])
		if !ok {
			return nil, nil
		}
		t := time.Unix(int64(secs), 0)
		if len(args) >= 2 && args[1] != nil {
			return applyMySQLDateFormat(ValueToStringKey(args[1]), t), nil
		}
		return t.Format("2006-01-02 15:04:05"), nil
	},

	"DATE_FORMAT": func(args []interface{}) (interface{}, error) {
		if len(args) < 2 || args[0] == nil || args[1] == nil {
			return nil, nil
		}
		t, ok := parseFlexibleTime(ValueToStringKey(args[0]))
		if !ok {
			return nil, nil
		}
		return applyMySQLDateFormat(ValueToStringKey(args[1]), t), nil
	},

	"LAST_DAY": func(args []interface{}) (interface{}, error) {
		t, ok := mysqlCompatDateArg(args)
		if !ok {
			return nil, nil
		}
		// Day 0 of the following month is the last day of this one.
		last := time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, t.Location())
		return last.Format("2006-01-02"), nil
	},
}

func charLengthHandler(args []interface{}) (interface{}, error) {
	if len(args) < 1 || args[0] == nil {
		return nil, nil
	}
	return int64(utf8.RuneCountInString(ValueToStringKey(args[0]))), nil
}

func sha1Handler(args []interface{}) (interface{}, error) {
	if len(args) < 1 || args[0] == nil {
		return nil, nil
	}
	sum := sha1.Sum([]byte(ValueToStringKey(args[0]))) // #nosec G401 -- SQL builtin, not security
	return hex.EncodeToString(sum[:]), nil
}

// formatRadix renders an integer argument in the given base, as BIN and OCT do.
func formatRadix(args []interface{}, base int) (interface{}, error) {
	if len(args) < 1 || args[0] == nil {
		return nil, nil
	}
	n, ok := toFloat64(args[0])
	if !ok {
		return nil, nil
	}
	return strconv.FormatInt(int64(n), base), nil
}

// applyMySQLDateFormat implements MySQL's DATE_FORMAT specifiers. These differ
// from strftime (handled by applyStrftime): MySQL uses %i for minutes and %M
// for the month name, where strftime uses %M for minutes.
func applyMySQLDateFormat(format string, t time.Time) string {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			b.WriteByte(format[i])
			continue
		}
		i++
		switch format[i] {
		case 'Y':
			fmt.Fprintf(&b, "%04d", t.Year())
		case 'y':
			fmt.Fprintf(&b, "%02d", t.Year()%100)
		case 'm':
			fmt.Fprintf(&b, "%02d", int(t.Month()))
		case 'c':
			fmt.Fprintf(&b, "%d", int(t.Month()))
		case 'd':
			fmt.Fprintf(&b, "%02d", t.Day())
		case 'e':
			fmt.Fprintf(&b, "%d", t.Day())
		case 'H':
			fmt.Fprintf(&b, "%02d", t.Hour())
		case 'k':
			fmt.Fprintf(&b, "%d", t.Hour())
		case 'h', 'I':
			fmt.Fprintf(&b, "%02d", hour12(t))
		case 'l':
			fmt.Fprintf(&b, "%d", hour12(t))
		case 'i':
			fmt.Fprintf(&b, "%02d", t.Minute())
		case 'S', 's':
			fmt.Fprintf(&b, "%02d", t.Second())
		case 'f':
			fmt.Fprintf(&b, "%06d", t.Nanosecond()/1000)
		case 'p':
			b.WriteString(t.Format("PM"))
		case 'M':
			b.WriteString(t.Month().String())
		case 'b':
			b.WriteString(t.Format("Jan"))
		case 'W':
			b.WriteString(t.Weekday().String())
		case 'a':
			b.WriteString(t.Format("Mon"))
		case 'j':
			fmt.Fprintf(&b, "%03d", t.YearDay())
		case 'T':
			fmt.Fprintf(&b, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
		case 'r':
			fmt.Fprintf(&b, "%02d:%02d:%02d %s", hour12(t), t.Minute(), t.Second(), t.Format("PM"))
		case 'U', 'u':
			_, wk := t.ISOWeek()
			fmt.Fprintf(&b, "%02d", wk)
		case 'w':
			fmt.Fprintf(&b, "%d", int(t.Weekday()))
		case '%':
			b.WriteByte('%')
		default:
			b.WriteByte(format[i])
		}
	}
	return b.String()
}

// hour12 converts to a 12-hour clock value (12 for midnight and noon).
func hour12(t time.Time) int {
	h := t.Hour() % 12
	if h == 0 {
		h = 12
	}
	return h
}
