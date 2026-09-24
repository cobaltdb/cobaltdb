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
	"math"
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

	// TO_BASE64 follows MySQL: standard base-64 alphabet with a newline
	// embedded after every 76 characters of encoded output (FROM_BASE64
	// skips them again). NULL arguments yield NULL.
	"TO_BASE64": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		return insertBase64Breaks(base64.StdEncoding.EncodeToString([]byte(ValueToStringKey(args[0])))), nil
	},
	// FROM_BASE64 follows MySQL: whitespace (including the newlines
	// TO_BASE64 embeds) is ignored while decoding, and an invalid base-64
	// string yields NULL rather than an error.
	"FROM_BASE64": func(args []interface{}) (interface{}, error) {
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		b, err := base64.StdEncoding.DecodeString(stripBase64Whitespace(ValueToStringKey(args[0])))
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

	// WEEK returns the week number for a date under WEEK()'s 0..7 mode table
	// (default mode 0). Unlike YEARWEEK it does not roll the year: modes
	// 0/1/4/5 report week 00 for days before week 1, and modes 2/3/6/7 use
	// week-year numbering (range 1-53). NULL dates and NULL or out-of-range
	// modes yield NULL.
	"WEEK": func(args []interface{}) (interface{}, error) {
		t, mode, ok := mysqlDateAndMode(args)
		if !ok {
			return nil, nil
		}
		return int64(mysqlWeek(t, mode)), nil
	},

	// YEARWEEK returns year*100 + week with week-year semantics: the result
	// year may differ from the date's year for the first and last week of
	// the year, so WEEK()'s week 00 never appears. The optional mode uses
	// WEEK()'s 0..7 table (rounded like MySQL's numeric cast); modes outside
	// that range yield NULL.
	"YEARWEEK": func(args []interface{}) (interface{}, error) {
		t, mode, ok := mysqlDateAndMode(args)
		if !ok {
			return nil, nil
		}
		year, week := mysqlYearWeek(t, mode)
		return int64(year)*100 + int64(week), nil
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
		case 'D':
			fmt.Fprintf(&b, "%d%s", t.Day(), mysqlDayOrdinal(t.Day()))
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
		case 'U':
			fmt.Fprintf(&b, "%02d", mysqlWeekMode0(t))
		case 'u':
			fmt.Fprintf(&b, "%02d", mysqlWeekMode1(t))
		case 'V':
			wk, _ := mysqlWeekMode2(t)
			fmt.Fprintf(&b, "%02d", wk)
		case 'v':
			_, wk := t.ISOWeek()
			fmt.Fprintf(&b, "%02d", wk)
		case 'X':
			_, yr := mysqlWeekMode2(t)
			fmt.Fprintf(&b, "%04d", yr)
		case 'x':
			yr, _ := t.ISOWeek()
			fmt.Fprintf(&b, "%04d", yr)
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

// mysqlWeekMode0 implements DATE_FORMAT %U (MySQL WEEK() mode 0): weeks start
// on Sunday and week 01 begins at the first Sunday of the year; the days
// before it report week 00. ISOWeek() must not be used here: ISO numbering is
// Monday-first with a 4-day rule and rolls across year boundaries.
func mysqlWeekMode0(t time.Time) int {
	jan1 := time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	firstSunday := 1 + ((7 - int(jan1.Weekday())) % 7) // YearDay of the first Sunday
	yd := t.YearDay()
	if yd < firstSunday {
		return 0
	}
	return (yd-firstSunday)/7 + 1
}

// mysqlWeekMode1 implements DATE_FORMAT %u (MySQL WEEK() mode 1): weeks start
// on Monday and week 01 is the first week with four or more days in the year.
// Years beginning Friday–Sunday contribute only 1–3 days to their opening
// Monday-week, so those leading days report week 00; years beginning
// Monday–Thursday keep the week containing Jan 1 as week 01.
func mysqlWeekMode1(t time.Time) int {
	jan1 := time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	mondayIndex := (int(jan1.Weekday()) + 6) % 7 // Monday = 0 .. Sunday = 6
	yd := t.YearDay()
	if mondayIndex >= 4 {
		if yd <= 7-mondayIndex {
			return 0
		}
		return (yd-8+mondayIndex)/7 + 1
	}
	return (yd-1+mondayIndex)/7 + 1
}

// mysqlWeekMode2 reports DATE_FORMAT %V/%X (MySQL WEEK() mode 2): %U numbering
// with week-year semantics. Days in week 00 belong to the previous year's
// final week (%V = that week number, %X = that year); the year end never rolls
// forward because week 01 always starts at the first Sunday of the year.
func mysqlWeekMode2(t time.Time) (week, year int) {
	year = t.Year()
	week = mysqlWeekMode0(t)
	if week != 0 {
		return week, year
	}
	// Week 00: the week opened in the previous year, so report that year's
	// final mode-0 week (Dec 31 is always past the first Sunday, so the
	// recursion terminates after one step).
	prevDec31 := time.Date(year-1, 12, 31, 0, 0, 0, 0, t.Location())
	return mysqlWeekMode0(prevDec31), year - 1
}

// mysqlDayOrdinal renders DATE_FORMAT %D's English ordinal suffix
// ("1st".."31st"). The 11th-13th take "th" despite ending in 1-3.
func mysqlDayOrdinal(day int) string {
	if d := day % 100; d == 11 || d == 12 || d == 13 {
		return "th"
	}
	switch day % 10 {
	case 1:
		return "st"
	case 2:
		return "nd"
	case 3:
		return "rd"
	default:
		return "th"
	}
}

// mysqlYearWeek reports MySQL YEARWEEK(date, mode): the week-year variant of
// WEEK()'s mode table, in which the result year rolls for the first and last
// week of the year. Because YEARWEEK never reports week 00, WEEK()'s
// "leading days are week 0" cases collapse into the neighbouring year and
// the eight modes reduce to four week-year algorithms: {0,2} Sunday-first
// week 1 at the first Sunday, {1,3} ISO-8601, {4,6} Sunday-first week 1 at
// the first 4-day week, {5,7} Monday-first week 1 at the first Monday.
func mysqlYearWeek(t time.Time, mode int) (int, int) {
	switch {
	case mode == 1 || mode == 3:
		return t.ISOWeek()
	case mode == 4 || mode == 6:
		return mysqlYearWeekMajority(t, time.Sunday)
	case mode == 5 || mode == 7:
		return mysqlYearWeekFirstAnchor(t, time.Monday)
	default: // modes 0 and 2
		w, y := mysqlWeekMode2(t)
		return y, w
	}
}

// mysqlWeekStart returns the start of t's week, whose days run from
// anchorWeekday. Calendar arithmetic is done in UTC so day boundaries are
// exact regardless of the location attached to t.
func mysqlWeekStart(t time.Time, anchorWeekday time.Weekday) time.Time {
	utc := t.UTC()
	return utc.AddDate(0, 0, -((int(utc.Weekday()) - int(anchorWeekday) + 7) % 7))
}

// mysqlFirstWeekdayOnOrAfter returns the first anchorWeekday of the year.
func mysqlFirstWeekdayOnOrAfter(year int, anchorWeekday time.Weekday) time.Time {
	jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	return jan1.AddDate(0, 0, (int(anchorWeekday)-int(jan1.Weekday())+7)%7)
}

// mysqlYearWeekFirstAnchor implements YEARWEEK modes {0,2} (Sunday) and
// {5,7} (Monday): week 1 starts at the first anchorWeekday on or after
// Jan 1. Days before week 1 roll back to the previous year's final week;
// there is no forward roll, because week 1 always starts inside its own
// year (consecutive years' first anchors are exact multiples of 7 days
// apart).
func mysqlYearWeekFirstAnchor(t time.Time, anchorWeekday time.Weekday) (year, week int) {
	year = t.Year()
	weekStart := mysqlWeekStart(t, anchorWeekday)
	start1 := mysqlWeekStart(mysqlFirstWeekdayOnOrAfter(year, anchorWeekday), anchorWeekday)
	if weekStart.Before(start1) {
		year--
		start1 = mysqlWeekStart(mysqlFirstWeekdayOnOrAfter(year, anchorWeekday), anchorWeekday)
	}
	week = int(weekStart.Sub(start1).Round(24*time.Hour)/(7*24*time.Hour)) + 1
	return year, week
}

// mysqlYearWeekMajority implements YEARWEEK modes {4,6}: weeks start on
// Sunday and week 1 is the first week with four or more days in the year —
// equivalently the first week whose mid-week day (Wednesday) falls in that
// year. Days of a late-December week whose majority year is the next year
// roll forward; days of an opening week with fewer than four days in the
// new year roll back to the previous year.
func mysqlYearWeekMajority(t time.Time, anchorWeekday time.Weekday) (year, week int) {
	weekStart := mysqlWeekStart(t, anchorWeekday)
	year = weekStart.AddDate(0, 0, 3).Year()
	start1 := mysqlWeekStart(time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC), anchorWeekday)
	if start1.AddDate(0, 0, 3).Year() != year {
		start1 = start1.AddDate(0, 0, 7)
	}
	week = int(weekStart.Sub(start1).Round(24*time.Hour)/(7*24*time.Hour)) + 1
	return year, week
}

// mysqlDateAndMode parses the (date[, mode]) argument shape shared by the
// YEARWEEK and WEEK builtins. A NULL or unparseable date, a NULL mode, a
// non-numeric mode, or a mode outside 0..7 (after MySQL-style rounding)
// reports ok=false, which the callers surface as a NULL result.
func mysqlDateAndMode(args []interface{}) (time.Time, int, bool) {
	if len(args) < 1 || args[0] == nil {
		return time.Time{}, 0, false
	}
	t, ok := parseFlexibleTime(ValueToStringKey(args[0]))
	if !ok {
		return time.Time{}, 0, false
	}
	mode := 0
	if len(args) >= 2 {
		if args[1] == nil {
			return time.Time{}, 0, false
		}
		f, ok := toFloat64(args[1])
		if !ok {
			return time.Time{}, 0, false
		}
		mode = int(math.Round(f))
	}
	if mode < 0 || mode > 7 {
		return time.Time{}, 0, false
	}
	return t, mode, true
}

// mysqlWeek reports MySQL WEEK(date, mode): the week number under the 0..7
// mode table WITHOUT week-year rolling. Modes 0/1/4/5 report week 00 for the
// days before week 1; modes 2/3/6/7 use their week-year variants (the
// range 1-53 rows of the manual's mode table).
func mysqlWeek(t time.Time, mode int) int {
	switch {
	case mode == 1:
		return mysqlWeekMode1(t)
	case mode == 2:
		w, _ := mysqlWeekMode2(t)
		return w
	case mode == 3:
		_, w := t.ISOWeek()
		return w
	case mode == 4:
		return mysqlWeekMajorityNoRoll(t, time.Sunday)
	case mode == 5:
		return mysqlWeekFirstAnchorNoRoll(t, time.Monday)
	case mode == 6:
		_, w := mysqlYearWeekMajority(t, time.Sunday)
		return w
	case mode == 7:
		_, w := mysqlYearWeekFirstAnchor(t, time.Monday)
		return w
	default: // mode 0
		return mysqlWeekMode0(t)
	}
}

// mysqlWeekMajorityNoRoll implements WEEK() mode 4: weeks start on Sunday and
// week 1 is the first week with four or more days in the year. Days of an
// opening sub-4-day week report week 00, and days of the final straddling
// week stay in the current year's numbering (mode 6 is the rolling variant).
func mysqlWeekMajorityNoRoll(t time.Time, anchorWeekday time.Weekday) int {
	weekStart := mysqlWeekStart(t, anchorWeekday)
	start1 := mysqlWeekStart(time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC), anchorWeekday)
	if start1.AddDate(0, 0, 3).Year() != t.Year() {
		start1 = start1.AddDate(0, 0, 7)
	}
	if weekStart.Before(start1) {
		return 0
	}
	return int(weekStart.Sub(start1).Round(24*time.Hour)/(7*24*time.Hour)) + 1
}

// mysqlWeekFirstAnchorNoRoll implements WEEK() mode 5: weeks start on Monday
// and week 1 begins at the first Monday of the year; earlier days report
// week 00 (mode 7 is the rolling variant).
func mysqlWeekFirstAnchorNoRoll(t time.Time, anchorWeekday time.Weekday) int {
	weekStart := mysqlWeekStart(t, anchorWeekday)
	start1 := mysqlWeekStart(mysqlFirstWeekdayOnOrAfter(t.Year(), anchorWeekday), anchorWeekday)
	if weekStart.Before(start1) {
		return 0
	}
	return int(weekStart.Sub(start1).Round(24*time.Hour)/(7*24*time.Hour)) + 1
}

// base64LineLength is MySQL's TO_BASE64 wrap width: the encoded output
// embeds a newline after every 76 characters.
const base64LineLength = 76

// insertBase64Breaks embeds a newline after every 76 characters of an
// already-encoded base-64 string (MySQL TO_BASE64 semantics); output whose
// length is an exact multiple of 76 gets no trailing newline.
func insertBase64Breaks(encoded string) string {
	if len(encoded) <= base64LineLength {
		return encoded
	}
	var b strings.Builder
	b.Grow(len(encoded) + len(encoded)/base64LineLength)
	for i := 0; i < len(encoded); i += base64LineLength {
		if i > 0 {
			b.WriteByte('\n')
		}
		end := i + base64LineLength
		if end > len(encoded) {
			end = len(encoded)
		}
		b.WriteString(encoded[i:end])
	}
	return b.String()
}

// stripBase64Whitespace removes the whitespace MySQL's FROM_BASE64 ignores:
// the newlines TO_BASE64 embeds plus spaces, tabs, carriage returns, form
// and vertical feeds.
func stripBase64Whitespace(s string) string {
	if !strings.ContainsAny(s, " \t\r\n\f\v") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ' ', '\t', '\r', '\n', '\f', '\v':
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}
