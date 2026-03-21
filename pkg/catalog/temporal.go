package catalog

import (
	"encoding/json"
	"strconv"
	"time"
)

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

// encodeVersionedRow encodes row values with temporal metadata
func encodeVersionedRow(rowValues []interface{}, asOfTime *time.Time) ([]byte, error) {
	createdAt := time.Now().Unix()
	if asOfTime != nil {
		createdAt = asOfTime.Unix()
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

// decodeVersionedRow decodes versioned row data.
// Uses a custom fast decoder for the known JSON format, falling back to
// json.Unmarshal for edge cases. The fast path avoids reflection and
// reduces allocations by parsing the "data" array and "version" object directly.
func decodeVersionedRow(data []byte, numCols int) (*VersionedRow, error) {
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
			return nil, err
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

	return &vrow, nil
}

// decodeVersionedRowFast is a zero-reflection decoder for VersionedRow JSON.
// It parses the known format directly using byte scanning, avoiding
// json.Unmarshal overhead (reflection, map allocation, etc.).
// Returns (row, true) on success or (nil, false) to fall back to slow path.
func decodeVersionedRowFast(data []byte, numCols int) (*VersionedRow, bool) {
	// Find "data":[ array
	dataKey := []byte(`"data":[`)
	pos := 1 // skip {
	for pos <= len(data)-len(dataKey) {
		if data[pos] == 'd' && pos+len(dataKey) <= len(data) && string(data[pos-1:pos-1+len(dataKey)]) == string(dataKey) {
			pos = pos - 1 + len(dataKey)
			goto foundData
		}
		pos++
	}
	return nil, false

foundData:
	// Parse the data array elements
	rowData := make([]interface{}, 0, numCols)
	for pos < len(data) {
		// Skip whitespace
		for pos < len(data) && data[pos] <= ' ' {
			pos++
		}
		if pos >= len(data) {
			return nil, false
		}
		if data[pos] == ']' {
			pos++
			break
		}

		// Parse one value
		var val interface{}
		var newPos int

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
							return nil, false
						}
						val = s
					} else {
						val = string(data[start:pos])
					}
					pos++
					goto valueReady
				}
				pos++
			}
			return nil, false

		case 'n':
			if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
				val = nil
				pos += 4
				goto valueReady
			}
			return nil, false

		case 't':
			if pos+4 <= len(data) && string(data[pos:pos+4]) == "true" {
				val = true
				pos += 4
				goto valueReady
			}
			return nil, false

		case 'f':
			if pos+5 <= len(data) && string(data[pos:pos+5]) == "false" {
				val = false
				pos += 5
				goto valueReady
			}
			return nil, false

		case '{', '[':
			// Nested object/array — use json.Unmarshal for this value
			newPos = skipJSONValue(data, pos)
			if newPos < 0 {
				return nil, false
			}
			if err := json.Unmarshal(data[pos:newPos], &val); err != nil {
				return nil, false
			}
			pos = newPos
			goto valueReady

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
			numStr := string(data[numStart:pos])
			if isFloat {
				fv, err := strconv.ParseFloat(numStr, 64)
				if err != nil {
					return nil, false
				}
				val = fv
			} else {
				iv, err := strconv.ParseInt(numStr, 10, 64)
				if err != nil {
					// Might be too large for int64, use float64
					fv, err2 := strconv.ParseFloat(numStr, 64)
					if err2 != nil {
						return nil, false
					}
					val = fv
				} else {
					val = iv // Direct int64, no float64→int64 conversion needed
				}
			}
			goto valueReady
		}

	valueReady:
		rowData = append(rowData, val)

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
		if data[pos] == 'c' && pos > 0 && data[pos-1] == '"' && pos-1+len(caKey) <= len(data) && string(data[pos-1:pos-1+len(caKey)]) == string(caKey) {
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
			if v, err := strconv.ParseInt(string(data[numStart:numEnd]), 10, 64); err == nil {
				createdAt = v
			}
		} else if data[pos] == 'd' && pos > 0 && data[pos-1] == '"' && pos-1+len(daKey) <= len(data) && string(data[pos-1:pos-1+len(daKey)]) == string(daKey) {
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
			if v, err := strconv.ParseInt(string(data[numStart:numEnd]), 10, 64); err == nil {
				deletedAt = v
			}
		}
		pos++
	}

	// Pad row to match column count
	for len(rowData) < numCols {
		rowData = append(rowData, nil)
	}

	return &VersionedRow{
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
