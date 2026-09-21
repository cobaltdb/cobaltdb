package engine

import (
	"context"
	"testing"
)

// Regression for the TYPEOF small-int classification gap: int8/int16/int32 are
// accepted bind-parameter types (validateWireParams), but both TYPEOF
// implementations (the scalar function table and the evalFunctionCallValue
// switch) classified them through the default branch as "text". Every integer
// type classifies as "integer".
func TestTypeOfSmallIntParamsClassifyAsInteger(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	for _, v := range []interface{}{int8(5), int16(5), int32(5)} {
		rows, err := db.Query(ctx, "SELECT TYPEOF(?)", v)
		if err != nil {
			t.Fatalf("TYPEOF(%T): %v", v, err)
		}
		cols := rows.Columns()
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if !rows.Next() {
			t.Fatalf("TYPEOF(%T): no row returned", v)
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		_ = rows.Close()
		got, _ := vals[0].(string)
		if got != "integer" {
			t.Fatalf("TYPEOF(%T) = %q, want %q (small ints fell through to the default branch)", v, got, "integer")
		}
	}
}
