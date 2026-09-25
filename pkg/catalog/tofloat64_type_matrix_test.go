package catalog

import (
	"math"
	"testing"
	"time"
)

// TestToFloat64TypeMatrix sweeps the numeric-coercion primitive across every
// Go numeric type the engine accepts as a wire parameter or public-API
// argument. validateWireParams accepts uint/uint8/uint16/uint32/uint64 and
// float32; toFloat64 gates division, modulo, binary arithmetic, and the math
// functions, so a missing case makes those operations reject numeric input.
func TestToFloat64TypeMatrix(t *testing.T) {
	cases := []struct {
		name string
		v    interface{}
		want float64
		ok   bool
	}{
		{"int", int(7), 7, true},
		{"int8", int8(7), 7, true},
		{"int16", int16(7), 7, true},
		{"int32", int32(7), 7, true},
		{"int64", int64(7), 7, true},
		{"uint", uint(7), 7, true},
		{"uint8", uint8(7), 7, true},
		{"uint16", uint16(7), 7, true},
		{"uint32", uint32(7), 7, true},
		{"uint64", uint64(7), 7, true},
		{"float64", float64(2.5), 2.5, true},
		{"float32", float32(2.5), 2.5, true},
		{"bool-true", true, 1, true},
		{"bool-false", false, 0, true},
		{"string-numeric", "2.5", 2.5, true},
		{"string-nonnumeric", "abc", 0, false},
		{"nil", nil, 0, false},
	}
	for _, tc := range cases {
		got, ok := toFloat64(tc.v)
		if ok != tc.ok || (ok && math.Abs(got-tc.want) > 1e-9) {
			t.Errorf("toFloat64(%s) = (%v, %v), want (%v, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
	// uint64 values above float64's exact-integer range still convert (with
	// rounding); they must be ACCEPTED, not rejected as non-numeric.
	if _, ok := toFloat64(uint64(math.MaxUint64)); !ok {
		t.Errorf("toFloat64(uint64 max) rejected — unsigned integers are numeric values")
	}
	// A time.Time is not a number by contract; it must stay rejected.
	if _, ok := toFloat64(time.Now()); ok {
		t.Errorf("toFloat64(time.Time) unexpectedly accepted")
	}
}
