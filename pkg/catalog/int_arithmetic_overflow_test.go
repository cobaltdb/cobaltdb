package catalog

import (
	"math"
	"math/big"
	"testing"
)

// TestScalarIntArithmeticDoesNotWrapOnOverflow pins that addValues,
// subtractValues and multiplyValues fall back to float64 on int64 overflow
// instead of silently wrapping around.
//
// Contract basis: sumAccumulator (catalog_aggregate.go) is documented as
// falling back to float64 "on ... int64 overflow, mirroring the addValues
// pattern used by the + operator", and TestSumAccumulatorPrecision pins
// "Overflow falls back to float64 rather than wrapping". The scalar operators
// were the unpatched half of that stated pattern: they performed unguarded
// int64 arithmetic, so MaxInt64 * 2 returned -2 and MinInt64 - 1 returned
// MaxInt64 — sign-flipped, silently wrong results with no error.
//
// The int64 domain is kept for the non-overflowing case on purpose: float64
// only represents integers exactly up to 2^53, so the controls below guard
// against a fix that simply routes every operand through float64.
func TestScalarIntArithmeticDoesNotWrapOnOverflow(t *testing.T) {
	overflowCases := []struct {
		name string
		got  interface{}
		want float64 // exact mathematical result, representable in float64
	}{
		// MaxInt64 + 1 == 2^63 (wrapped to MinInt64 before the fix)
		{"add_overflow", mustAdd(t, math.MaxInt64, 1), 9223372036854775808},
		// MaxInt64 * 2 == 2^64 - 2 (wrapped to -2 before the fix)
		{"mul_overflow", mustMul(t, math.MaxInt64, 2), 18446744073709551614},
		// MinInt64 - 1 == -(2^63 + 1) (wrapped to MaxInt64 before the fix)
		{"sub_overflow", mustSub(t, math.MinInt64, 1), -9223372036854775809},
	}
	for _, tc := range overflowCases {
		if got, ok := toFloat64(tc.got); !ok || got != tc.want {
			t.Errorf("%s = %v (%T), want %v (int64 wraparound must fall back to float64)",
				tc.name, tc.got, tc.got, tc.want)
		}
	}

	// Controls: in-range integer arithmetic must stay EXACT int64, well above
	// the 2^53 boundary where float64 would start losing integers.
	exactCases := []struct {
		name string
		got  interface{}
		want int64
	}{
		{"add_exact_above_2pow53", mustAdd(t, 9007199254740993, 1), 9007199254740994},
		{"mul_exact_above_2pow53", mustMul(t, 9007199254740993, 2), 18014398509481986},
		{"sub_exact_above_2pow53", mustSub(t, 9007199254740993, 1), 9007199254740992},
		// Boundary: one below MaxInt64 must NOT be treated as overflow.
		{"add_maxint64_boundary", mustAdd(t, math.MaxInt64-1, 1), math.MaxInt64},
		{"add_zero", mustAdd(t, math.MaxInt64, 0), math.MaxInt64},
		{"mul_zero", mustMul(t, math.MaxInt64, 0), 0},
		{"mul_one", mustMul(t, math.MinInt64, 1), math.MinInt64},
		{"add_mixed_signs", mustAdd(t, -5, 3), -2},
		{"add_both_negative", mustAdd(t, -5, -3), -8},
		{"sub_negative_minuend", mustSub(t, -5, 3), -8},
		{"mul_negative_operands", mustMul(t, -5, -3), 15},
	}
	for _, tc := range exactCases {
		if got, ok := tc.got.(int64); !ok || got != tc.want {
			t.Errorf("%s = %v (%T), want exact int64 %d", tc.name, tc.got, tc.got, tc.want)
		}
	}
}

// TestInt64CheckedHelpers uses math/big as an independent oracle: each helper
// must report "representable" exactly when the true result fits in int64, and
// when it does, must return that exact value.
func TestInt64CheckedHelpers(t *testing.T) {
	operands := []int64{
		math.MinInt64, math.MinInt64 + 1, -1 << 40, -5, -3, -1, 0, 1, 5,
		1 << 40, math.MaxInt64 - 1, math.MaxInt64,
	}
	bigOf := func(v int64) *big.Int { return big.NewInt(v) }
	fitsInt64 := func(v *big.Int) bool { return v.IsInt64() }

	for _, a := range operands {
		for _, b := range operands {
			wantSum := big.NewInt(0).Add(bigOf(a), bigOf(b))
			if got, ok := int64AddChecked(a, b); ok != fitsInt64(wantSum) ||
				(ok && got != a+b) {
				t.Errorf("int64AddChecked(%d,%d) = (%d,%v), want (%d,%v)", a, b, got, ok, a+b, fitsInt64(wantSum))
			}

			wantDiff := big.NewInt(0).Sub(bigOf(a), bigOf(b))
			if got, ok := int64SubChecked(a, b); ok != fitsInt64(wantDiff) ||
				(ok && got != a-b) {
				t.Errorf("int64SubChecked(%d,%d) = (%d,%v), want (%d,%v)", a, b, got, ok, a-b, fitsInt64(wantDiff))
			}

			wantProd := big.NewInt(0).Mul(bigOf(a), bigOf(b))
			if got, ok := int64MulChecked(a, b); ok != fitsInt64(wantProd) ||
				(ok && got != a*b) {
				t.Errorf("int64MulChecked(%d,%d) = (%d,%v), want (%d,%v)", a, b, got, ok, a*b, fitsInt64(wantProd))
			}
		}
	}
}

func mustAdd(t *testing.T, a, b int64) interface{} {
	t.Helper()
	v, err := addValues(a, b)
	if err != nil {
		t.Fatalf("addValues(%d,%d): %v", a, b, err)
	}
	return v
}

func mustSub(t *testing.T, a, b int64) interface{} {
	t.Helper()
	v, err := subtractValues(a, b)
	if err != nil {
		t.Fatalf("subtractValues(%d,%d): %v", a, b, err)
	}
	return v
}

func mustMul(t *testing.T, a, b int64) interface{} {
	t.Helper()
	v, err := multiplyValues(a, b)
	if err != nil {
		t.Fatalf("multiplyValues(%d,%d): %v", a, b, err)
	}
	return v
}
