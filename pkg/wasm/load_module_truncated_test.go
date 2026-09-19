//go:build wasm_experimental

package wasm

import (
	"encoding/binary"
	"testing"
)

// Truncated/malformed module fixtures. LoadModule (and CallFunction for the
// interpreter case) must return an ERROR for every one of these — never panic.
// Pre-fix, readLeb128/readLeb128Signed indexed data[pos] without bounds checks
// and three direct byte reads (import kind, memory flags, local type) had no
// offset guards, so truncated bytecode panicked with index-out-of-range —
// violating the CRIT-2 contract ("must now return errors instead").
func TestLoadModuleTruncatedReturnsError(t *testing.T) {
	magicVersion := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}

	cases := []struct {
		name string
		mod  []byte
	}{
		{
			name: "section_id_without_size", // readLeb128 panics reading section size
			mod:  append(append([]byte{}, magicVersion...), 0x01),
		},
		{
			name: "import_section_truncated_before_kind", // kind := data[offset] OOB
			mod: append(append([]byte{}, magicVersion...),
				0x02, 0x05, 0x01, 0x01, 'm', 0x01, 'f'),
		},
		{
			name: "memory_section_count_only", // flags := data[offset] OOB
			mod:  append(append([]byte{}, magicVersion...), 0x05, 0x01, 0x01),
		},
		{
			name: "code_section_locals_truncated", // readLeb128 past funcData end
			mod:  append(append([]byte{}, magicVersion...), 0x0a, 0x03, 0x01, 0x01, 0x01),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("LoadModule panicked on truncated module: %v", r)
				}
			}()
			rt := NewRuntime(1)
			if err := rt.LoadModule(tc.mod); err == nil {
				t.Fatalf("expected an error for truncated module, got nil")
			}
		})
	}
}

// TestCallFunctionTruncatedOperandReturnsError pins the interpreter side: an
// i64.const whose LEB128 operand is truncated (continuation bit, no following
// byte) must surface as an execution error, not an index-out-of-range panic.
func TestCallFunctionTruncatedOperandReturnsError(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("CallFunction panicked on truncated operand: %v", r)
		}
	}()

	module := append([]byte{}, []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}...)
	// Type section: one type, func () -> ().
	module = append(module, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00)
	// Function section: one function, type 0.
	module = append(module, 0x03, 0x02, 0x01, 0x00)
	// Code section: one body, no locals, i64.const (0x42) with a truncated
	// LEB128 operand (0x80 sets the continuation bit, then EOF).
	module = append(module, 0x0a, 0x05, 0x01, 0x03, 0x00, 0x42, 0x80)

	rt := NewRuntime(1)
	if err := rt.LoadModule(module); err != nil {
		t.Fatalf("module should parse cleanly: %v", err)
	}
	if _, err := rt.CallFunction(0, nil); err == nil {
		t.Fatalf("expected an execution error for truncated i64.const operand, got nil")
	}
	_ = binary.LittleEndian // keep import meaningful for future fixtures
}
