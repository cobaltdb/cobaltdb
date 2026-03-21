package catalog

import (
	"encoding/json"
	"testing"
	"time"
)

func TestExtractColumnFloat64(t *testing.T) {
	tests := []struct {
		name     string
		data     []interface{}
		colIdx   int
		wantVal  float64
		wantOK   bool
	}{
		{"int col0", []interface{}{int64(42)}, 0, 42, true},
		{"int col1", []interface{}{int64(1), int64(800)}, 1, 800, true},
		{"float col1", []interface{}{int64(1), 3.14}, 1, 3.14, true},
		{"string col skip", []interface{}{int64(1), "test-user", int64(500)}, 2, 500, true},
		{"null value", []interface{}{int64(1), nil}, 1, 0, false},
		{"col out of range", []interface{}{int64(1)}, 5, 0, false},
		{"negative number", []interface{}{int64(1), int64(-200)}, 1, -200, true},
		{"large number", []interface{}{int64(1), int64(999999999)}, 1, 999999999, true},
		{"zero", []interface{}{int64(1), int64(0)}, 1, 0, true},
		{"bool skip", []interface{}{true, int64(100)}, 1, 100, true},
		{"nested json skip", []interface{}{map[string]interface{}{"a": 1}, int64(42)}, 1, 42, true},
		{"array skip", []interface{}{[]interface{}{1, 2, 3}, int64(99)}, 1, 99, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vrow := VersionedRow{
				Data:    tt.data,
				Version: RowVersion{CreatedAt: time.Now().Unix(), DeletedAt: 0},
			}
			raw, err := json.Marshal(vrow)
			if err != nil {
				t.Fatalf("marshal failed: %v", err)
			}

			gotVal, gotOK := extractColumnFloat64(raw, tt.colIdx)
			if gotOK != tt.wantOK {
				t.Errorf("ok: got %v, want %v (json: %s)", gotOK, tt.wantOK, raw)
			}
			if gotOK && gotVal != tt.wantVal {
				t.Errorf("val: got %g, want %g (json: %s)", gotVal, tt.wantVal, raw)
			}
		})
	}
}
