//go:build wasm_experimental

package wasm

import (
	"strings"
	"testing"
)

// Regression tests for review finding CRIT-2: writeParams and parseResults
// previously indexed rt.Memory[offset:] without any bounds check, so malformed
// inputs (oversized string, negative-after-cast length, offset past end) would
// panic. They must now return errors instead.

func TestWriteParams_OffsetPastEnd(t *testing.T) {
	rt := NewRuntime(1) // 64KB
	pastEnd := int32(len(rt.Memory))
	err := rt.writeParams(pastEnd, []interface{}{int64(1)})
	if err == nil {
		t.Fatal("expected error writing past end of memory")
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriteParams_StringOverflow(t *testing.T) {
	rt := NewRuntime(1)
	// String that would overflow memory but fits in int32.
	big := make([]byte, len(rt.Memory)+1)
	err := rt.writeParams(0, []interface{}{string(big)})
	if err == nil {
		t.Fatal("expected error for string larger than memory")
	}
}

func TestWriteParams_NegativeOffset(t *testing.T) {
	rt := NewRuntime(1)
	err := rt.writeParams(-1, []interface{}{int64(1)})
	if err == nil {
		t.Fatal("expected error for negative offset")
	}
}

func TestParseResults_TruncatedMemory(t *testing.T) {
	rt := NewRuntime(1)
	schema := []ColumnInfo{{Name: "x", Type: "INTEGER"}}
	// Ask for one row at an offset where 8 bytes won't fit.
	pastEnd := int32(len(rt.Memory) - 4)
	_, err := rt.parseResults(schema, pastEnd, 1)
	if err == nil {
		t.Fatal("expected error parsing past end of memory")
	}
}

func TestParseResults_TextLengthOverflow(t *testing.T) {
	rt := NewRuntime(1)
	// Plant a TEXT row whose declared length runs off the end of memory.
	// Header: length = 0xFFFFFFFE (large but not the null marker 0xFFFFFFFF).
	rt.Memory[0] = 0xFE
	rt.Memory[1] = 0xFF
	rt.Memory[2] = 0xFF
	rt.Memory[3] = 0xFF
	schema := []ColumnInfo{{Name: "s", Type: "TEXT"}}
	_, err := rt.parseResults(schema, 0, 1)
	if err == nil {
		t.Fatal("expected error for TEXT length exceeding memory")
	}
}
