package catalog

import (
	"bytes"
	"testing"
)

// TestVersionedRowBinaryRoundTrip verifies the row codec round-trips binary
// values ([]byte and non-UTF-8 strings) without corruption, while leaving
// normal scalar/string values (incl. valid multibyte UTF-8) unchanged.
func TestVersionedRowBinaryRoundTrip(t *testing.T) {
	bytesCases := map[int][]byte{
		1: {0x00, 0xFF, 0x41},      // control + high byte + ascii
		2: {1, 2, 3},               // plain bytes
		3: []byte("hello"),         // ascii bytes
		4: {},                      // empty
		5: {'"', '\\', 0x00, 0x80}, // quote, backslash, null, high
	}
	for n, want := range bytesCases {
		row := []interface{}{int64(n), want}
		enc, err := encodeVersionedRow(row, nil)
		if err != nil {
			t.Fatalf("encode %d: %v", n, err)
		}
		dec, err := decodeVersionedRow(enc, 2)
		if err != nil {
			t.Fatalf("decode %d: %v", n, err)
		}
		got, ok := dec.Data[1].([]byte)
		if !ok {
			t.Fatalf("case %d: value decoded as %T, want []byte", n, dec.Data[1])
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d: got %v, want %v", n, got, want)
		}
	}

	// Non-UTF-8 string round-trips (as bytes).
	{
		bad := string([]byte{0xFF, 0xFE, 0x41})
		row := []interface{}{int64(9), bad}
		enc, _ := encodeVersionedRow(row, nil)
		dec, err := decodeVersionedRow(enc, 2)
		if err != nil {
			t.Fatalf("decode non-utf8: %v", err)
		}
		got, ok := dec.Data[1].([]byte)
		if !ok || string(got) != bad {
			t.Fatalf("non-utf8 string round-trip: got %v (%T), want bytes %v", dec.Data[1], dec.Data[1], []byte(bad))
		}
	}

	// Normal values (incl. valid multibyte UTF-8) stay as-is and keep their type.
	normal := []interface{}{int64(10), "café 日本語", "ascii", true, nil, float64(3.5)}
	enc, _ := encodeVersionedRow(normal, nil)
	if len(enc) > 0 && enc[0] == binRowMarker {
		t.Fatal("a row with no binary values must not use the binary marker")
	}
	dec, err := decodeVersionedRow(enc, len(normal))
	if err != nil {
		t.Fatalf("decode normal: %v", err)
	}
	if dec.Data[1] != "café 日本語" || dec.Data[2] != "ascii" || dec.Data[3] != true || dec.Data[5] != float64(3.5) {
		t.Fatalf("normal round-trip changed values: %#v", dec.Data)
	}
	if dec.Data[0] != int64(10) {
		t.Fatalf("integer not restored: %T %v", dec.Data[0], dec.Data[0])
	}
}
