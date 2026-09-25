package engine

import (
	"context"
	"testing"
)

// TestMathFuncsContracts pins the MySQL value contracts of the rounding and
// math builtins (ROUND/FLOOR/CEIL/ABS/MOD) through the real dispatch:
//
//   - ROUND rounds half away from zero (ROUND(±2.5) = ±3),
//   - ROUND(x, negative D) rounds left of the decimal point
//     (ROUND(123.456, -1) = 120 — the fixed counting-loop bug),
//   - FLOOR/CEIL follow the floor/ceiling semantics on negatives
//     (FLOOR(-2.5) = -3, CEIL(-2.5) = -2),
//   - MOD by zero yields NULL.
//
// Values compare numerically across the engine's numeric result types.
func TestMathFuncsContracts(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	cases := []struct {
		sql  string
		want float64
	}{
		{"SELECT ROUND(2.5)", 3},
		{"SELECT ROUND(-2.5)", -3},
		{"SELECT ROUND(2.4)", 2},
		{"SELECT ROUND(-2.4)", -2},
		{"SELECT ROUND(2.6)", 3},
		{"SELECT ROUND(-2.6)", -3},
		{"SELECT ROUND(123.456, -1)", 120},
		{"SELECT ROUND(123.456, -2)", 100},
		{"SELECT ROUND(125.0, -1)", 130},
		{"SELECT ROUND(123.456, 0)", 123},
		{"SELECT ROUND(123.456, 1)", 123.5},
		{"SELECT ROUND(123.456, 2)", 123.46},
		{"SELECT FLOOR(2.5)", 2},
		{"SELECT FLOOR(-2.5)", -3},
		{"SELECT FLOOR(-0.1)", -1},
		{"SELECT CEIL(2.5)", 3},
		{"SELECT CEIL(-2.5)", -2},
		{"SELECT CEIL(-0.1)", 0},
		{"SELECT ABS(-2.5)", 2.5},
		{"SELECT ABS(2.5)", 2.5},
		{"SELECT ABS(-7)", 7},
		{"SELECT MOD(10, 3)", 1},
		{"SELECT MOD(-10, 3)", -1},
	}
	for _, tc := range cases {
		rows, err := db.Query(ctx, tc.sql)
		if err != nil {
			t.Fatalf("%s: query: %v", tc.sql, err)
		}
		if !rows.Next() {
			t.Fatalf("%s: no rows", tc.sql)
		}
		var got interface{}
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("%s: scan: %v", tc.sql, err)
		}
		_ = rows.Close()
		gf, ok := asFloat(got)
		if !ok {
			t.Fatalf("%s = %T(%v), want numeric %v", tc.sql, got, got, tc.want)
		}
		if gf != tc.want {
			t.Fatalf("%s = %v, want %v", tc.sql, gf, tc.want)
		}
	}

	// NULL and MOD-by-zero contracts.
	for _, sql := range []string{
		"SELECT ROUND(NULL)",
		"SELECT FLOOR(NULL)",
		"SELECT CEIL(NULL)",
		"SELECT ABS(NULL)",
		"SELECT MOD(10, 0)",
	} {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: query: %v", sql, err)
		}
		if !rows.Next() {
			t.Fatalf("%s: no rows", sql)
		}
		var got interface{}
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("%s: scan: %v", sql, err)
		}
		_ = rows.Close()
		if got != nil {
			t.Fatalf("%s = %v, want NULL", sql, got)
		}
	}
}

// asFloat converts the engine's numeric result types for value comparison.
func asFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}
