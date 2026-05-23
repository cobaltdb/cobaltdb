package wasm

import "testing"

func TestCompilerUint32ReportsInvalidValues(t *testing.T) {
	if _, err := compilerUint32(-1, "test"); err == nil {
		t.Fatal("expected negative value error")
	}
	if _, err := compilerUint32(int(^uint(0)>>1), "test"); err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestCompilerUint64ReportsInvalidValues(t *testing.T) {
	if _, err := compilerUint64(-1, "test"); err == nil {
		t.Fatal("expected negative value error")
	}
}
