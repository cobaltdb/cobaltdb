package catalog

import (
	"encoding/json"
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

// decodeVersionedRow decodes versioned row data
func decodeVersionedRow(data []byte, numCols int) (*VersionedRow, error) {
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
