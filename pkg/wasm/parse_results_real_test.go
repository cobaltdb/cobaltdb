//go:build wasm_experimental

package wasm

import (
	"encoding/binary"
	"math"
	"testing"
)

// TestParseResultsRealDecodesFloat64 pins REAL column decoding: parseResults
// must convert the 8 stored bytes from IEEE-754 bits back to float64 via
// math.Float64frombits. Pre-fix it returned the raw bit pattern as uint64
// (e.g. 3.14 -> 4614253070214989087), corrupting every REAL result column —
// the type the AVG aggregate codegen emits (compiler.go).
func TestParseResultsRealDecodesFloat64(t *testing.T) {
	rt := NewRuntime(10)
	binary.LittleEndian.PutUint64(rt.Memory[1024:], math.Float64bits(3.14))
	schema := []ColumnInfo{{Name: "avg", Type: "REAL", Nullable: false}}

	result, err := rt.parseResults(schema, 1024, 1)
	if err != nil {
		t.Fatalf("parseResults REAL failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	got, ok := result.Rows[0].Values[0].(float64)
	if !ok {
		t.Fatalf("REAL value decoded as %T (%v), want float64", result.Rows[0].Values[0], result.Rows[0].Values[0])
	}
	if got != 3.14 {
		t.Fatalf("REAL value corrupted: got %v (%[1]v's bits: %#x), want 3.14", got, math.Float64bits(got))
	}
}
