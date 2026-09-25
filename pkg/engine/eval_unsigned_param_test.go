package engine

import (
	"context"
	"testing"
)

// TestUnsignedParamArithmeticAndFunctions pins that wire-accepted unsigned
// and float32 parameters flow through the numeric operations. validateWireParams
// accepts uint/uint8/uint16/uint32/uint64/float32; toFloat64 gates division,
// modulo, and the math functions, so a missing case rejects numeric input
// ("cannot divide non-numeric values").
func TestUnsignedParamArithmeticAndFunctions(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// asFloat compares numerically across the engine's numeric result types
	// (division returns float64; integer modulo returns int64).
	asFloat := func(v interface{}) (float64, bool) {
		switch n := v.(type) {
		case float64:
			return n, true
		case int64:
			return float64(n), true
		case int:
			return float64(n), true
		}
		return 0, false
	}

	queryScalar := func(query string, args ...interface{}) (interface{}, error) {
		rows, err := db.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("%s: no rows", query)
		}
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("%s: scan: %v", query, err)
		}
		return v, nil
	}

	if v, err := queryScalar("SELECT ? / 2", uint64(8)); err != nil {
		t.Errorf("SELECT ? / 2 with uint64 param: %v", err)
	} else if f, ok := asFloat(v); !ok || f != 4 {
		t.Errorf("SELECT ? / 2 with uint64(8) = %v, want 4", v)
	}
	if v, err := queryScalar("SELECT ? % 3", uint64(10)); err != nil {
		t.Errorf("SELECT ? %% 3 with uint64 param: %v", err)
	} else if f, ok := asFloat(v); !ok || f != 1 {
		t.Errorf("SELECT ? %% 3 with uint64(10) = %v, want 1", v)
	}
	if v, err := queryScalar("SELECT ABS(?)", uint(7)); err != nil {
		t.Errorf("SELECT ABS(?) with uint param: %v", err)
	} else if f, ok := asFloat(v); !ok || f != 7 {
		t.Errorf("SELECT ABS(?) with uint(7) = %v, want 7", v)
	}
	if v, err := queryScalar("SELECT ? + 1", float32(1.5)); err != nil {
		t.Errorf("SELECT ? + 1 with float32 param: %v", err)
	} else if f, ok := asFloat(v); !ok || f != 2.5 {
		t.Errorf("SELECT ? + 1 with float32(1.5) = %v, want 2.5", v)
	}
}
