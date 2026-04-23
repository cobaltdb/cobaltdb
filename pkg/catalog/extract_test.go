package catalog

import (
	"encoding/json"
	"testing"
	"time"
)

func TestExtractColumnFloat64(t *testing.T) {
	tests := []struct {
		name    string
		data    []interface{}
		colIdx  int
		wantVal float64
		wantOK  bool
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

func TestDecodeVersionedRowFast(t *testing.T) {
	tests := []struct {
		name    string
		data    []interface{}
		version RowVersion
		numCols int
	}{
		{
			"simple integers",
			[]interface{}{int64(1), int64(42), int64(100)},
			RowVersion{CreatedAt: 1000, DeletedAt: 0},
			3,
		},
		{
			"mixed types",
			[]interface{}{int64(1), "hello", 3.14, true, nil},
			RowVersion{CreatedAt: 2000, DeletedAt: 500},
			5,
		},
		{
			"strings with special chars",
			[]interface{}{int64(1), "hello\nworld", "tab\there"},
			RowVersion{CreatedAt: 3000, DeletedAt: 0},
			3,
		},
		{
			"empty data",
			[]interface{}{},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			0,
		},
		{
			"nested object",
			[]interface{}{int64(1), map[string]interface{}{"key": "val"}},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			2,
		},
		{
			"nested array",
			[]interface{}{int64(1), []interface{}{"a", "b", "c"}},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			2,
		},
		{
			"pad columns",
			[]interface{}{int64(1)},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			5, // numCols > len(data) → should pad
		},
		{
			"large integer",
			[]interface{}{int64(9999999999999)},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			1,
		},
		{
			"negative integer",
			[]interface{}{int64(-42)},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			1,
		},
		{
			"float value",
			[]interface{}{1.5, 2.7},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			2,
		},
		{
			"deleted row",
			[]interface{}{int64(1), "deleted"},
			RowVersion{CreatedAt: 100, DeletedAt: 999},
			2,
		},
		{
			"false value",
			[]interface{}{false, int64(0)},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			2,
		},
		{
			"string with quotes",
			[]interface{}{int64(1), `say "hello"`},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			2,
		},
		{
			"string with backslash",
			[]interface{}{int64(1), `path\to\file`},
			RowVersion{CreatedAt: 100, DeletedAt: 0},
			2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vrow := VersionedRow{Data: tt.data, Version: tt.version}
			raw, err := json.Marshal(vrow)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			got, err := decodeVersionedRow(raw, tt.numCols)
			if err != nil {
				t.Fatalf("decode: %v (json: %s)", err, raw)
			}

			// Check version
			if got.Version.CreatedAt != tt.version.CreatedAt {
				t.Errorf("CreatedAt: got %d, want %d", got.Version.CreatedAt, tt.version.CreatedAt)
			}
			if got.Version.DeletedAt != tt.version.DeletedAt {
				t.Errorf("DeletedAt: got %d, want %d", got.Version.DeletedAt, tt.version.DeletedAt)
			}

			// Check data length (should be at least numCols)
			if len(got.Data) < tt.numCols {
				t.Errorf("Data length: got %d, want >= %d", len(got.Data), tt.numCols)
			}

			// Check data values match original (up to original length)
			ref := &VersionedRow{Data: tt.data, Version: tt.version}
			refRaw, _ := json.Marshal(ref)
			refDecoded, _ := decodeVersionedRowSlow(refRaw, tt.numCols)

			for i := 0; i < len(tt.data); i++ {
				if i >= len(got.Data) {
					t.Errorf("Data[%d]: missing", i)
					continue
				}
				gotJSON, _ := json.Marshal(got.Data[i])
				refJSON, _ := json.Marshal(refDecoded.Data[i])
				if string(gotJSON) != string(refJSON) {
					t.Errorf("Data[%d]: got %s, want %s", i, gotJSON, refJSON)
				}
			}
		})
	}
}

// decodeVersionedRowSlow is the original json.Unmarshal path for comparison.
func decodeVersionedRowSlow(data []byte, numCols int) (*VersionedRow, error) {
	var vrow VersionedRow
	if err := json.Unmarshal(data, &vrow); err != nil {
		return nil, err
	}
	for i, v := range vrow.Data {
		if f, ok := v.(float64); ok {
			if f == float64(int64(f)) && f >= -1e15 && f <= 1e15 {
				vrow.Data[i] = int64(f)
			}
		}
	}
	for len(vrow.Data) < numCols {
		vrow.Data = append(vrow.Data, nil)
	}
	return &vrow, nil
}

func TestDecodeVersionedRowFast_Fallback(t *testing.T) {
	// Plain array format (backward compatibility) — should fall back to slow path
	raw := []byte(`[1, "hello", 42]`)
	got, err := decodeVersionedRow(raw, 3)
	if err != nil {
		t.Fatalf("decode plain array: %v", err)
	}
	if len(got.Data) != 3 {
		t.Errorf("Data length: got %d, want 3", len(got.Data))
	}
}

func TestDecodeVersionedRowFast_InvalidJSON(t *testing.T) {
	_, err := decodeVersionedRow([]byte(`{invalid`), 1)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestSkipJSONValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int // expected position after skip
	}{
		{"string", `"hello",next`, 7},
		{"string escape", `"he\"llo",next`, 9},
		{"number", `42,next`, 2},
		{"negative", `-17,next`, 3},
		{"float", `3.14,next`, 4},
		{"true", `true,next`, 4},
		{"false", `false,next`, 5},
		{"null", `null,next`, 4},
		{"object", `{"a":1},next`, 7},
		{"array", `[1,2,3],next`, 7},
		{"nested", `{"a":{"b":[1,2]}},next`, 17},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := skipJSONValue([]byte(tt.input), 0)
			if got != tt.want {
				t.Errorf("skipJSONValue(%q, 0) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestSkipJSONBracketed(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{"empty object", `{}`, 2},
		{"empty array", `[]`, 2},
		{"nested", `{"a":{"b":1}}`, 13},
		{"string in object", `{"key":"val"}`, 13},
		{"escaped quote", `{"k":"v\"x"}`, 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(tt.input)
			open, close := data[0], byte('}')
			if data[0] == '[' {
				close = ']'
			}
			got := skipJSONBracketed(data, 0, open, close)
			if got != tt.want {
				t.Errorf("skipJSONBracketed(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func BenchmarkDecodeVersionedRow(b *testing.B) {
	vrow := VersionedRow{
		Data:    []interface{}{int64(1), "user-42", int64(25), 50000.5},
		Version: RowVersion{CreatedAt: time.Now().Unix(), DeletedAt: 0},
	}
	raw, _ := json.Marshal(vrow)

	b.Run("Fast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decodeVersionedRow(raw, 4)
		}
	})

	b.Run("Slow", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decodeVersionedRowSlow(raw, 4)
		}
	})
}
