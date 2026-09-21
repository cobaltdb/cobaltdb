package engine

import (
	"context"
	"fmt"
	"testing"
)

// Regression for the toFloat64 int8/int16/int32 gap: the numeric coercion
// helper lacked the small signed integer cases that compareAsInt64 and
// toInt64 both have. Values of those types are accepted bind parameters
// (validateWireParams allows int8/int16/int32), so `SELECT ? / 2` with an
// int32 param failed with "cannot divide non-numeric values" while the same
// parameter worked for comparisons (+/-/* take the wholeInt64 path). The
// divide/modulo/ABS/ROUND/FLOOR/CEIL paths and the evalBinaryExprValue
// arithmetic gate all route through toFloat64 and must accept them.
func TestSmallIntParamArithmeticAndFunctions(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	scalar := func(query string, args ...interface{}) interface{} {
		t.Helper()
		var v interface{}
		if err := db.QueryRow(ctx, query, args...).Scan(&v); err != nil {
			return fmt.Errorf("query %q: %w", query, err)
		}
		return v
	}
	eq := func(got interface{}, want string) bool {
		return fmt.Sprintf("%v", got) == want
	}

	// Controls: comparisons and +/-/* already handle small ints via
	// compareAsInt64 / wholeInt64.
	if got := scalar("SELECT ? = 6", int32(6)); !eq(got, "true") {
		t.Errorf("int32 comparison = %v, want true", got)
	}
	if got := scalar("SELECT ? + 3", int32(4)); !eq(got, "7") {
		t.Errorf("int32 add = %v, want 7", got)
	}
	if got := scalar("SELECT ? * 2", int16(3)); !eq(got, "6") {
		t.Errorf("int16 multiply = %v, want 6", got)
	}

	// The defect: divide/modulo and the numeric scalar functions route
	// through toFloat64, which did not know int8/int16/int32.
	if got := scalar("SELECT ? / 2", int32(6)); !eq(got, "3") {
		t.Errorf("int32 divide = %v, want 3", got)
	}
	if got := scalar("SELECT ? % 4", int32(10)); !eq(got, "2") {
		t.Errorf("int32 modulo = %v, want 2", got)
	}
	if got := scalar("SELECT ABS(?)", int32(-7)); !eq(got, "7") {
		t.Errorf("int32 ABS = %v, want 7", got)
	}
	if got := scalar("SELECT ROUND(?)", int32(5)); !eq(got, "5") {
		t.Errorf("int32 ROUND = %v, want 5", got)
	}
	if got := scalar("SELECT ? / 2", int16(8)); !eq(got, "4") {
		t.Errorf("int16 divide = %v, want 4", got)
	}
	if got := scalar("SELECT ? % 3", int8(7)); !eq(got, "1") {
		t.Errorf("int8 modulo = %v, want 1", got)
	}
}
