package catalog

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"
	"unsafe"
)

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
		buf = dst[:len(dst)]
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
		if vrow, ok := decodeVersionedRowFast(data, numCols); ok {
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

// decodeVersionedRowFast is a zero-reflection decoder for VersionedRow JSON.
// It parses the known format directly using byte scanning, avoiding
// json.Unmarshal overhead (reflection, map allocation, etc.).
// Returns (row, true) on success or (zero value, false) to fall back to slow path.
func decodeVersionedRowFast(data []byte, numCols int) (VersionedRow, bool) {
	// Find "data":[ array
	dataKey := []byte(`"data":[`)
	pos := 1 // skip {
	for pos <= len(data)-len(dataKey) {
		if data[pos] == 'd' && pos+len(dataKey) <= len(data) && bytes.Equal(data[pos-1:pos-1+len(dataKey)], dataKey) {
			pos = pos - 1 + len(dataKey)
			goto foundData
		}
		pos++
	}
	return VersionedRow{}, false

foundData:
	// Parse the data array elements
	rowData := make([]interface{}, 0, numCols)
	for pos < len(data) {
		// Skip whitespace
		for pos < len(data) && data[pos] <= ' ' {
			pos++
		}
		if pos >= len(data) {
			return VersionedRow{}, false
		}
		if data[pos] == ']' {
			pos++
			break
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
							return VersionedRow{}, false
						}
						rowData = append(rowData, s)
					} else {
						// safe: data is stable for the lifetime of the iterator
						rowData = append(rowData, unsafe.String(&data[start], pos-start))
					}
					pos++
					goto afterAppend
				}
				pos++
			}
			return VersionedRow{}, false

		case 'n':
			if pos+4 <= len(data) && data[pos] == 'n' && data[pos+1] == 'u' && data[pos+2] == 'l' && data[pos+3] == 'l' {
				rowData = append(rowData, nil)
				pos += 4
				goto afterAppend
			}
			return VersionedRow{}, false

		case 't':
			if pos+4 <= len(data) && data[pos] == 't' && data[pos+1] == 'r' && data[pos+2] == 'u' && data[pos+3] == 'e' {
				rowData = append(rowData, true)
				pos += 4
				goto afterAppend
			}
			return VersionedRow{}, false

		case 'f':
			if pos+5 <= len(data) && data[pos] == 'f' && data[pos+1] == 'a' && data[pos+2] == 'l' && data[pos+3] == 's' && data[pos+4] == 'e' {
				rowData = append(rowData, false)
				pos += 5
				goto afterAppend
			}
			return VersionedRow{}, false

		case '{', '[':
			// Nested object/array — use json.Unmarshal for this value
			newPos := skipJSONValue(data, pos)
			if newPos < 0 {
				return VersionedRow{}, false
			}
			var nested interface{}
			if err := json.Unmarshal(data[pos:newPos], &nested); err != nil {
				return VersionedRow{}, false
			}
			rowData = append(rowData, nested)
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
				fv, err := strconv.ParseFloat(unsafe.String(&data[numStart], pos-numStart), 64)
				if err != nil {
					return VersionedRow{}, false
				}
				rowData = append(rowData, fv)
			} else {
				iv, ok := parseInt64Fast(data[numStart:pos])
				if !ok {
					// Might be too large for int64, use float64
					fv, err2 := strconv.ParseFloat(unsafe.String(&data[numStart], pos-numStart), 64)
					if err2 != nil {
						return VersionedRow{}, false
					}
					rowData = append(rowData, fv)
				} else {
					rowData = append(rowData, iv) // Direct int64, no float64→int64 conversion needed
				}
			}
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
	caKey := []byte(`"created_at":`)
	daKey := []byte(`"deleted_at":`)
	for pos < len(data) {
		if data[pos] == 'c' && pos > 0 && data[pos-1] == '"' && pos-1+len(caKey) <= len(data) && bytes.Equal(data[pos-1:pos-1+len(caKey)], caKey) {
			numStart := pos - 1 + len(caKey)
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
		} else if data[pos] == 'd' && pos > 0 && data[pos-1] == '"' && pos-1+len(daKey) <= len(data) && bytes.Equal(data[pos-1:pos-1+len(daKey)], daKey) {
			numStart := pos - 1 + len(daKey)
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
				deletedAt = v
			}
		}
		pos++
	}

	// Pad row to match column count
	for len(rowData) < numCols {
		rowData = append(rowData, nil)
	}

	return VersionedRow{
		Data: rowData,
		Version: RowVersion{
			CreatedAt: createdAt,
			DeletedAt: deletedAt,
		},
	}, true
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
	dataKey := []byte(`"data":[`)
	pos := 0
	for pos <= len(data)-len(dataKey) {
		if data[pos] == dataKey[0] && string(data[pos:pos+len(dataKey)]) == string(dataKey) {
			pos += len(dataKey)
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
