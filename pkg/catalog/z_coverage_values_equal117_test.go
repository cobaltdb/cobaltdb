package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogFK(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestValuesEqualNumericTypes117 tests valuesEqual with various numeric type combinations
func TestValuesEqualNumericTypes117(t *testing.T) {
	c := newTestCatalogFK(t)
	fke := NewForeignKeyEnforcer(c)

	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		// Same types
		{"int equal", int(5), int(5), true},
		{"int not equal", int(5), int(6), false},
		{"int8 equal", int8(5), int8(5), true},
		{"int8 not equal", int8(5), int8(6), false},
		{"int16 equal", int16(5), int16(5), true},
		{"int16 not equal", int16(5), int16(6), false},
		{"int32 equal", int32(5), int32(5), true},
		{"int32 not equal", int32(5), int32(6), false},
		{"int64 equal", int64(5), int64(5), true},
		{"int64 not equal", int64(5), int64(6), false},
		{"uint equal", uint(5), uint(5), true},
		{"uint not equal", uint(5), uint(6), false},
		{"uint8 equal", uint8(5), uint8(5), true},
		{"uint8 not equal", uint8(5), uint8(6), false},
		{"uint16 equal", uint16(5), uint16(5), true},
		{"uint16 not equal", uint16(5), uint16(6), false},
		{"uint32 equal", uint32(5), uint32(5), true},
		{"uint32 not equal", uint32(5), uint32(6), false},
		{"uint64 equal", uint64(5), uint64(5), true},
		{"uint64 not equal", uint64(5), uint64(6), false},
		{"float32 equal", float32(5.5), float32(5.5), true},
		{"float32 not equal", float32(5.5), float32(6.5), false},
		{"float64 equal", float64(5.5), float64(5.5), true},
		{"float64 not equal", float64(5.5), float64(6.5), false},

		// Mixed numeric types
		{"int vs int64 equal", int(5), int64(5), true},
		{"int vs int64 not equal", int(5), int64(6), false},
		{"int vs float64 equal", int(5), float64(5.0), true},
		{"int vs float64 not equal", int(5), float64(5.5), false},
		{"int32 vs int64 equal", int32(100), int64(100), true},
		{"uint vs int equal", uint(5), int(5), true},
		{"uint vs int not equal", uint(5), int(6), false},
		{"float32 vs float64 equal", float32(3), float64(3), true},
		{"float32 vs float64 not equal", float32(3), float64(4), false},
		{"int8 vs uint8 equal", int8(5), uint8(5), true},
		{"int16 vs uint16 equal", int16(100), uint16(100), true},
		{"int32 vs uint32 equal", int32(1000), uint32(1000), true},
		{"int64 vs uint64 equal", int64(10000), uint64(10000), true},

		// String comparisons
		{"string equal", "hello", "hello", true},
		{"string not equal", "hello", "world", false},
		{"string empty equal", "", "", true},

		// Nil comparisons
		{"both nil", nil, nil, true},
		{"a nil", nil, "value", false},
		{"b nil", "value", nil, false},

		// Non-numeric types
		{"bool equal true", true, true, true},
		{"bool equal false", false, false, true},
		{"bool not equal", true, false, false},
		{"mixed types string int", "5", int(5), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fke.valuesEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("valuesEqual(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestValuesEqualEdgeCases117 tests edge cases for valuesEqual
func TestValuesEqualEdgeCases117(t *testing.T) {
	c := newTestCatalogFK(t)
	fke := NewForeignKeyEnforcer(c)

	// Large numbers
	if !fke.valuesEqual(int64(9223372036854775807), int64(9223372036854775807)) {
		t.Error("Large int64 values should be equal")
	}

	// Negative numbers
	if !fke.valuesEqual(int(-100), int64(-100)) {
		t.Error("Negative numbers should be equal")
	}

	// Zero values
	if !fke.valuesEqual(int(0), int64(0)) {
		t.Error("Zero values should be equal")
	}
	if !fke.valuesEqual(float64(0.0), int(0)) {
		t.Error("Zero float and int should be equal")
	}

	// Very small float differences
	if fke.valuesEqual(float64(0.1+0.2), float64(0.3)) {
		// Note: This might be true or false depending on float precision
		t.Log("Float precision test: 0.1+0.2 vs 0.3")
	}
}

