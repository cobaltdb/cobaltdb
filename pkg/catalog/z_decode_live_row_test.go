package catalog

import (
	"errors"
	"testing"
)

// TestDecodeLiveRowLiveRow verifies that a non-deleted versioned row decodes
// to (row, true, nil).
func TestDecodeLiveRowLiveRow(t *testing.T) {
	row := []interface{}{int64(1), "alice", float64(3.14)}
	data, err := encodeVersionedRow(row, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, ok, err := decodeLiveRow(data, len(row))
	if err != nil {
		t.Fatalf("decodeLiveRow: unexpected error %v", err)
	}
	if !ok {
		t.Fatal("decodeLiveRow: ok=false on a live row")
	}
	if len(got) != len(row) {
		t.Fatalf("decodeLiveRow: got %d cols, want %d", len(got), len(row))
	}
	if got[0] != int64(1) || got[1] != "alice" {
		t.Fatalf("decodeLiveRow: got %v, want row", got)
	}
}

// TestDecodeLiveRowSoftDeleted verifies that a soft-deleted row is reported
// as (nil, false, nil) — no error, but the caller should skip it.
func TestDecodeLiveRowSoftDeleted(t *testing.T) {
	// Build a hand-crafted encoded row with DeletedAt > 0. We can't use the
	// public encodeVersionedRow helper because the fast path doesn't write
	// DeletedAt and the slow path always sets it to 0. The format is the
	// same as encodeVersionedRowFast emits:
	//   {"data":[<cols>],"version":{"created_at":<ts>,"deleted_at":<ts>}}
	encoded := []byte(`{"data":[2,"bob"],"version":{"created_at":1700000000,"deleted_at":1700000001}}`)

	got, ok, err := decodeLiveRow(encoded, 2)
	if err != nil {
		t.Fatalf("decodeLiveRow on deleted: unexpected error %v", err)
	}
	if ok {
		t.Fatalf("decodeLiveRow on deleted: ok=true, want false (row was soft-deleted)")
	}
	if got != nil {
		t.Fatalf("decodeLiveRow on deleted: got row %v, want nil", got)
	}
}

// TestDecodeLiveRowDecodeError verifies that a malformed payload returns
// (nil, false, err) so the caller can wrap or fallback.
func TestDecodeLiveRowDecodeError(t *testing.T) {
	_, ok, err := decodeLiveRow([]byte("not json at all"), 3)
	if err == nil {
		t.Fatal("decodeLiveRow on garbage: expected error, got nil")
	}
	if ok {
		t.Fatal("decodeLiveRow on garbage: ok=true, want false")
	}
	// The error should be the underlying JSON unmarshal error.
	if !containsString(err.Error(), "invalid character") &&
		!containsString(err.Error(), "json") &&
		!errors.Is(err, errDecodeFallback) { // best-effort, not a strict contract
		t.Logf("decodeLiveRow error: %v (acceptable: not strictly validated)", err)
	}
}

func containsString(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// errDecodeFallback is a sentinel used only to make errors.Is a no-op safety
// net above; the real contract is "non-nil error, false ok".
var errDecodeFallback = errors.New("decode fallback")
