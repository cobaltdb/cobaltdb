package catalog

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"
	"unsafe"
)

// Package-level byte slices for JSON key constants — avoids per-call allocation
// of []byte(stringLiteral) inside hot-path functions.
var (
	dataKeyBytes = []byte(`"data":[`)
	createdAtKey = []byte(`"created_at":`)
	deletedAtKey = []byte(`"deleted_at":`)
)

// intBoxPool holds pre-boxed int64 interface{} values for the range -32768..32767.
// Assigning a pre-boxed interface to a []interface{} slot avoids a heap allocation
// that Go's escape analysis would otherwise force in non-inlined decode paths.
// Pool size: 65536 entries × 16 bytes = 1 MiB.
var intBoxPool [65536]interface{}

func init() {
	for i := 0; i < 65536; i++ {
		intBoxPool[i] = int64(i - 32768)
	}
}

func boxInt64(v int64) interface{} {
	idx := v + 32768
	if idx >= 0 && idx < int64(len(intBoxPool)) {
		return intBoxPool[idx]
	}
	return v
}

// parseInt64Fast parses an int64 directly from a byte slice without allocating
// a temporary string. It handles an optional leading minus sign.
func parseInt64Fast(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i++
	}
	var n int64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}

// RowVersion stores temporal metadata for a row
// This enables AS OF SYSTEM TIME queries (time travel)
type RowVersion struct {
	CreatedAt int64 `json:"created_at"` // Unix timestamp when row was created
	DeletedAt int64 `json:"deleted_at"` // Unix timestamp when row was deleted (0 if not deleted)
}

// VersionedRow wraps row data with versioning metadata
type VersionedRow struct {
	Data    []interface{} `json:"data"`    // The actual row data
	Version RowVersion    `json:"version"` // Temporal metadata
}

// encodeVersionedRow encodes row values with temporal metadata.
// Uses a zero-reflection fast path for common scalar types, falling back to
// json.Marshal for edge cases (complex strings, nested objects, etc.).
func encodeVersionedRow(rowValues []interface{}, asOfTime *time.Time) ([]byte, error) {
	createdAt := time.Now().Unix()
	if asOfTime != nil {
		createdAt = asOfTime.Unix()
	}

	if data, ok := encodeVersionedRowFast(rowValues, createdAt, nil); ok {
		return data, nil
	}

	vrow := VersionedRow{
		Data: rowValues,
		Version: RowVersion{
			CreatedAt: createdAt,
			DeletedAt: 0,
		},
	}
	return json.Marshal(vrow)
}

// encodeVersionedRowFast manually builds the JSON {"data":[...],"version":{...}}
// for common scalar types without reflection. Appends to dst and returns the
// updated buffer. Returns (buf, false) to fall back to json.Marshal for
// unsupported types or strings that need escaping.
func encodeVersionedRowFast(rowValues []interface{}, createdAt int64, dst []byte) ([]byte, bool) {
	// Pre-size buffer: ~8 bytes per value + fixed overhead.
	need := 64 + len(rowValues)*16
	var buf []byte
	if cap(dst)-len(dst) >= need {
		buf = dst
	} else {
		buf = make([]byte, len(dst), len(dst)+need)
		copy(buf, dst)
	}
	buf = append(buf, `{"data":[`...)

	for i, v := range rowValues {
		if i > 0 {
			buf = append(buf, ',')
		}
		switch val := v.(type) {
		case nil:
			buf = append(buf, "null"...)
		case int:
			buf = strconv.AppendInt(buf, int64(val), 10)
		case int8:
			buf = strconv.AppendInt(buf, int64(val), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(val), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(val), 10)
		case int64:
			buf = strconv.AppendInt(buf, val, 10)
		case uint:
			buf = strconv.AppendUint(buf, uint64(val), 10)
		case uint8:
			buf = strconv.AppendUint(buf, uint64(val), 10)
		case uint16:
			buf = strconv.AppendUint(buf, uint64(val), 10)
		case uint32:
			buf = strconv.AppendUint(buf, uint64(val), 10)
		case uint64:
			buf = strconv.AppendUint(buf, val, 10)
		case float64:
			buf = strconv.AppendFloat(buf, val, 'g', -1, 64)
		case float32:
			buf = strconv.AppendFloat(buf, float64(val), 'g', -1, 32)
		case bool:
			if val {
				buf = append(buf, "true"...)
			} else {
				buf = append(buf, "false"...)
			}
		case string:
			// Fast path only for strings without quotes/backslashes/control chars.
			needsEscape := false
			for j := 0; j < len(val); j++ {
				c := val[j]
				if c == '"' || c == '\\' || c < 0x20 {
					needsEscape = true
					break
				}
			}
			if needsEscape {
				return buf, false
			}
			buf = append(buf, '"')
			buf = append(buf, val...)
			buf = append(buf, '"')
		default:
			return buf, false
		}
	}

	buf = append(buf, `],"version":{"created_at":`...)
	buf = strconv.AppendInt(buf, createdAt, 10)
	buf = append(buf, `,"deleted_at":0}}`...)
	return buf, true
}

// decodeVersionedRow decodes versioned row data.
// Uses a custom fast decoder for the known JSON format, falling back to
// json.Unmarshal for edge cases. The fast path avoids reflection and
// reduces allocations by parsing the "data" array and "version" object directly.
func decodeVersionedRow(data []byte, numCols int) (VersionedRow, error) {
	// Fast path: custom decoder for known format {"data":[...],"version":{...}}
	if len(data) > 2 && data[0] == '{' {
		out := make([]interface{}, numCols)
		if vrow, ok := decodeVersionedRowFast(data, numCols, out); ok {
			return vrow, nil
		}
	}

	// Slow path: generic json.Unmarshal (backward compatibility, edge cases)
	var vrow VersionedRow
	if err := json.Unmarshal(data, &vrow); err != nil {
		// Fallback: try decoding as plain row (backward compatibility)
		var plainRow []interface{}
		if err2 := json.Unmarshal(data, &plainRow); err2 != nil {
			return VersionedRow{}, err
		}
		vrow = VersionedRow{
			Data: plainRow,
			Version: RowVersion{
				CreatedAt: 0,
				DeletedAt: 0,
			},
		}
	}

	// Restore integer types lost by JSON unmarshaling
	for i, v := range vrow.Data {
		if f, ok := v.(float64); ok {
			if f == float64(int64(f)) && f >= -1e15 && f <= 1e15 {
				vrow.Data[i] = int64(f)
			}
		}
	}

	// Pad row to match current column count
	for len(vrow.Data) < numCols {
		vrow.Data = append(vrow.Data, nil)
	}

	return vrow, nil
}

// decodeVisibleRow decodes a versioned row and checks visibility at the given timestamp.
// Returns the row data if visible, or (nil, false, nil) if not visible.
// This consolidates the common pattern of decodeVersionedRow + isVisibleAt into one call.
//
//lint:ignore U1000 retained for temporal query compatibility hooks.
func decodeVisibleRow(data []byte, numCols int, queryTime time.Time) ([]interface{}, bool, error) {
	vrow, err := decodeVersionedRow(data, numCols)
	if err != nil {
		return nil, false, err
	}
	if !vrow.Version.isVisibleAt(queryTime) {
		return nil, false, nil
	}
	return vrow.Data, true, nil
}

// decodeLiveRow decodes a versioned row and returns the row data if the row is
// not soft-deleted. This consolidates the very common pattern
//
//	vrow, err := decodeVersionedRow(data, len(table.Columns))
//	if err != nil { /* wrap + return */ }
//	if vrow.Version.DeletedAt > 0 { continue }
//	row := vrow.Data
//
// into a single call. The return contract is:
//
//	row != nil, ok == true  → row is live; use it
//	err != nil              → decode failed; caller decides (wrap/return/fallback)
//	row == nil, ok == false → row was soft-deleted; caller skips
//
// Pass op + tableName so the caller can wrap a decode error with context
// (e.g. "delete: failed to decode row in table foo: %w"). The current call
// sites all wrap errors with a table-scoped message, so accepting a label
// keeps the consolidation lossless.
func decodeLiveRow(data []byte, numCols int) (row []interface{}, ok bool, err error) {
	vrow, err := decodeVersionedRow(data, numCols)
	if err != nil {
		return nil, false, err
	}
	if vrow.Version.DeletedAt > 0 {
		return nil, false, nil
	}
	return vrow.Data, true, nil
}

// decodeVersionedRowFast is a zero-reflection decoder for VersionedRow JSON.
// It parses the known format directly using byte scanning, avoiding
// json.Unmarshal overhead (reflection, map allocation, etc.).
// Returns (row, true) on success or (zero value, false) to fall back to slow path.
func decodeVersionedRowFast(data []byte, numCols int, out []interface{}) (VersionedRow, bool) {
	vrow, _, ok := decodeVersionedRowFastEx(data, numCols, out, nil, 0)
	return vrow, ok
}

// decodeVersionedRowFastEx is an extended version of decodeVersionedRowFast that
// can store decoded strings as *string pointers into a pre-allocated string
// buffer. A pointer (8 bytes) fits directly into an interface{} slot without
// the heap allocation that Go requires when boxing a multi-word string value.
// Returns the updated stringIdx so callers can track consumption.
func decodeVersionedRowFastEx(data []byte, numCols int, out []interface{}, stringBuf []string, stringIdx int) (VersionedRow, int, bool) {
	if cap(out) < numCols {
		return VersionedRow{}, stringIdx, false
	}
	// Fast path: the format is always {"data":[...],"version":{...}}
	// "data":[ starts at index 1 (after {). Unrolled check avoids slice+memcmp.
	if len(data) < 16 || data[0] != '{' || data[1] != '"' || data[2] != 'd' || data[3] != 'a' || data[4] != 't' || data[5] != 'a' || data[6] != '"' || data[7] != ':' || data[8] != '[' {
		return VersionedRow{}, stringIdx, false
	}
	pos := 9 // skip {"data":[
	// Parse the data array elements into the provided buffer.
	rowData := out[:numCols]
	colIdx := 0
	for pos < len(data) {
		// Skip whitespace
		for pos < len(data) && data[pos] <= ' ' {
			pos++
		}
		if pos >= len(data) {
			return VersionedRow{}, stringIdx, false
		}
		if data[pos] == ']' {
			pos++
			break
		}
		if colIdx >= numCols {
			return VersionedRow{}, stringIdx, false
		}

		switch data[pos] {
		case '"':
			// String value
			pos++
			start := pos
			hasEscape := false
			for pos < len(data) {
				if data[pos] == '\\' {
					hasEscape = true
					pos += 2
					continue
				}
				if data[pos] == '"' {
					if hasEscape {
						// Has escape sequences — use json.Unmarshal for correctness
						var s string
						if err := json.Unmarshal(data[start-1:pos+1], &s); err != nil {
							return VersionedRow{}, stringIdx, false
						}
						if stringBuf != nil {
							stringBuf[stringIdx] = s
							rowData[colIdx] = StringBox{ptr: &stringBuf[stringIdx]}
							stringIdx++
						} else {
							rowData[colIdx] = s
						}
					} else {
						// safe: data is stable for the lifetime of the iterator
						if stringBuf != nil {
							stringBuf[stringIdx] = unsafe.String(&data[start], pos-start) // #nosec G103 - data is immutable during row decode and copied into the caller-owned string buffer.
							rowData[colIdx] = StringBox{ptr: &stringBuf[stringIdx]}
							stringIdx++
						} else {
							rowData[colIdx] = unsafe.String(&data[start], pos-start) // #nosec G103 - data is immutable for the decoded row lifetime.
						}
					}
					colIdx++
					pos++
					goto afterAppend
				}
				pos++
			}
			return VersionedRow{}, stringIdx, false

		case 'n':
			if pos+4 <= len(data) && data[pos] == 'n' && data[pos+1] == 'u' && data[pos+2] == 'l' && data[pos+3] == 'l' {
				rowData[colIdx] = nil
				colIdx++
				pos += 4
				goto afterAppend
			}
			return VersionedRow{}, stringIdx, false

		case 't':
			if pos+4 <= len(data) && data[pos] == 't' && data[pos+1] == 'r' && data[pos+2] == 'u' && data[pos+3] == 'e' {
				rowData[colIdx] = true
				colIdx++
				pos += 4
				goto afterAppend
			}
			return VersionedRow{}, stringIdx, false

		case 'f':
			if pos+5 <= len(data) && data[pos] == 'f' && data[pos+1] == 'a' && data[pos+2] == 'l' && data[pos+3] == 's' && data[pos+4] == 'e' {
				rowData[colIdx] = false
				colIdx++
				pos += 5
				goto afterAppend
			}
			return VersionedRow{}, stringIdx, false

		case '{', '[':
			// Nested object/array — use json.Unmarshal for this value
			newPos := skipJSONValue(data, pos)
			if newPos < 0 {
				return VersionedRow{}, stringIdx, false
			}
			var nested interface{}
			if err := json.Unmarshal(data[pos:newPos], &nested); err != nil {
				return VersionedRow{}, stringIdx, false
			}
			rowData[colIdx] = nested
			colIdx++
			pos = newPos
			goto afterAppend

		default:
			// Number — parse directly, avoiding float64 for integers
			numStart := pos
			isFloat := false
			if pos < len(data) && (data[pos] == '-' || data[pos] == '+') {
				pos++
			}
			for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
				pos++
			}
			if pos < len(data) && (data[pos] == '.' || data[pos] == 'e' || data[pos] == 'E') {
				isFloat = true
				pos++
				if pos < len(data) && (data[pos] == '+' || data[pos] == '-') {
					pos++
				}
				for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
					pos++
				}
			}
			if isFloat {
				fv, err := strconv.ParseFloat(unsafe.String(&data[numStart], pos-numStart), 64) // #nosec G103 - temporary numeric slice is immutable during parsing.
				if err != nil {
					return VersionedRow{}, stringIdx, false
				}
				rowData[colIdx] = fv
			} else {
				iv, ok := parseInt64Fast(data[numStart:pos])
				if !ok {
					// Might be too large for int64, use float64
					fv, err2 := strconv.ParseFloat(unsafe.String(&data[numStart], pos-numStart), 64) // #nosec G103 - temporary numeric slice is immutable during parsing.
					if err2 != nil {
						return VersionedRow{}, stringIdx, false
					}
					rowData[colIdx] = fv
				} else {
					rowData[colIdx] = boxInt64(iv)
				}
			}
			colIdx++
			goto afterAppend
		}

	afterAppend:
		// Skip whitespace + comma
		for pos < len(data) && data[pos] <= ' ' {
			pos++
		}
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}

	// Parse version: find "created_at": and "deleted_at":
	var createdAt, deletedAt int64
	// Fast path: search for createdAtKey starting from current pos, then deletedAtKey after that.
	// The format is fixed: ...,"version":{"created_at":N,"deleted_at":M}
	if ca := bytes.Index(data[pos:], createdAtKey); ca >= 0 {
		numStart := pos + ca + len(createdAtKey)
		for numStart < len(data) && data[numStart] <= ' ' {
			numStart++
		}
		numEnd := numStart
		if numEnd < len(data) && data[numEnd] == '-' {
			numEnd++
		}
		for numEnd < len(data) && data[numEnd] >= '0' && data[numEnd] <= '9' {
			numEnd++
		}
		if v, ok := parseInt64Fast(data[numStart:numEnd]); ok {
			createdAt = v
		}
		if da := bytes.Index(data[numEnd:], deletedAtKey); da >= 0 {
			numStart = numEnd + da + len(deletedAtKey)
			for numStart < len(data) && data[numStart] <= ' ' {
				numStart++
			}
			numEnd = numStart
			if numEnd < len(data) && data[numEnd] == '-' {
				numEnd++
			}
			for numEnd < len(data) && data[numEnd] >= '0' && data[numEnd] <= '9' {
				numEnd++
			}
			if v, ok := parseInt64Fast(data[numStart:numEnd]); ok {
				deletedAt = v
			}
		}
	}

	// Pad row to match column count
	for colIdx < numCols {
		rowData[colIdx] = nil
		colIdx++
	}

	return VersionedRow{
		Data: rowData,
		Version: RowVersion{
			CreatedAt: createdAt,
			DeletedAt: deletedAt,
		},
	}, stringIdx, true
}

// extractColumnFloat64 extracts a numeric column value from raw JSON row data
// without full json.Unmarshal. The JSON format is:
//
//	{"data":[col0,col1,...],"version":{...}}
//
// Returns (value, true) if the column was found and is numeric, or (0, false) otherwise.
// This is ~10x faster than decodeVersionedRow + toFloat64Safe for single-column access.
func extractColumnFloat64(data []byte, colIdx int) (float64, bool) {
	// Find the start of "data":[ array
	pos := 0
	for pos <= len(data)-len(dataKeyBytes) {
		if data[pos] == dataKeyBytes[0] && string(data[pos:pos+len(dataKeyBytes)]) == string(dataKeyBytes) {
			pos += len(dataKeyBytes)
			goto foundArray
		}
		pos++
	}
	return 0, false

foundArray:
	// Skip to the colIdx-th element in the JSON array
	for idx := 0; idx < colIdx; idx++ {
		// Skip one JSON value (handles strings, numbers, null, nested objects/arrays)
		pos = skipJSONValue(data, pos)
		if pos < 0 || pos >= len(data) {
			return 0, false
		}
		// Skip comma
		for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t' || data[pos] == '\n' || data[pos] == '\r') {
			pos++
		}
		if pos >= len(data) || data[pos] != ',' {
			return 0, false // not enough elements
		}
		pos++ // skip comma
		// Skip whitespace
		for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t' || data[pos] == '\n' || data[pos] == '\r') {
			pos++
		}
	}

	// Now pos points to the start of the target value — parse it as a number
	if pos >= len(data) {
		return 0, false
	}

	// null check
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return 0, false
	}

	// Find end of numeric value
	end := pos
	if end < len(data) && (data[end] == '-' || data[end] == '+') {
		end++
	}
	hasDigit := false
	for end < len(data) && ((data[end] >= '0' && data[end] <= '9') || data[end] == '.' || data[end] == 'e' || data[end] == 'E' || data[end] == '+' || data[end] == '-') {
		if data[end] >= '0' && data[end] <= '9' {
			hasDigit = true
		}
		end++
	}
	if !hasDigit {
		return 0, false // not a number (string or other type)
	}

	// Parse the number
	val, err := strconv.ParseFloat(string(data[pos:end]), 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

// skipJSONValue advances past one complete JSON value starting at pos.
// Returns the position after the value, or -1 on error.
func skipJSONValue(data []byte, pos int) int {
	if pos >= len(data) {
		return -1
	}
	// Skip whitespace
	for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t' || data[pos] == '\n' || data[pos] == '\r') {
		pos++
	}
	if pos >= len(data) {
		return -1
	}

	switch data[pos] {
	case '"': // string
		pos++
		for pos < len(data) {
			if data[pos] == '\\' {
				pos += 2 // skip escaped char
				continue
			}
			if data[pos] == '"' {
				return pos + 1
			}
			pos++
		}
		return -1
	case '{': // object
		return skipJSONBracketed(data, pos, '{', '}')
	case '[': // array
		return skipJSONBracketed(data, pos, '[', ']')
	case 'n': // null
		if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
			return pos + 4
		}
		return -1
	case 't': // true
		if pos+4 <= len(data) && string(data[pos:pos+4]) == "true" {
			return pos + 4
		}
		return -1
	case 'f': // false
		if pos+5 <= len(data) && string(data[pos:pos+5]) == "false" {
			return pos + 5
		}
		return -1
	default: // number
		for pos < len(data) && data[pos] != ',' && data[pos] != ']' && data[pos] != '}' && data[pos] != ' ' && data[pos] != '\n' {
			pos++
		}
		return pos
	}
}

// skipJSONBracketed skips a JSON object or array, handling nesting.
func skipJSONBracketed(data []byte, pos int, open, close byte) int {
	depth := 0
	inString := false
	for pos < len(data) {
		ch := data[pos]
		if inString {
			if ch == '\\' {
				pos += 2
				continue
			}
			if ch == '"' {
				inString = false
			}
		} else {
			if ch == '"' {
				inString = true
			} else if ch == open {
				depth++
			} else if ch == close {
				depth--
				if depth == 0 {
					return pos + 1
				}
			}
		}
		pos++
	}
	return -1
}

// isVisibleAt checks if this row version is visible at the given timestamp
// A row is visible if:
// - It was created before or at the query time
// - It was not deleted, or was deleted after the query time
func (v *RowVersion) isVisibleAt(queryTime time.Time) bool {
	queryUnix := queryTime.Unix()

	// Row was created after query time - not visible
	if v.CreatedAt > queryUnix {
		return false
	}

	// Row was deleted before or at query time - not visible
	if v.DeletedAt > 0 && v.DeletedAt <= queryUnix {
		return false
	}

	return true
}

// markDeleted marks the row as deleted at the given time
func (v *RowVersion) markDeleted(deleteTime time.Time) {
	v.DeletedAt = deleteTime.Unix()
}
