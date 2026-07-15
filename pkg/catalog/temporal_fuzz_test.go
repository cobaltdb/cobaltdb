package catalog

import (
	"testing"
	"time"
)

// FuzzDecodeVersionedRowFastEx fuzzes decodeVersionedRowFastEx which contains
// unsafe.String calls at lines 526, 530, 604, and 613. The function parses
// raw page data into row values — a parser bug could cause unsafe.String to
// construct a string that reads past the slice boundary.
//
// Input is a raw byte slice fed directly to the decoder. The corpus starts
// with valid encodings so the fuzzer learns the wire format.
func FuzzDecodeVersionedRowFastEx(f *testing.F) {
	// Seed corpus: valid encoded rows.
	seedCases := []struct {
		values []interface{}
	}{
		{[]interface{}{int64(1)}},
		{[]interface{}{"hello"}},
		{[]interface{}{3.14}},
		{[]interface{}{nil}},
		{[]interface{}{true}},
		{[]interface{}{int64(1), "two", 3.0, nil, false}},
		{[]interface{}{"a longer string value with spaces"}},
		{[]interface{}{int64(-1), uint64(1<<64 - 1), float64(1.5)}},
		{[]interface{}{"unicode: 日本語"}},
		{[]interface{}{[]byte("binary data")}},
		{[]interface{}{int64(0), int64(1), int64(2), int64(3), int64(4)}},
	}
	for _, c := range seedCases {
		enc, err := encodeVersionedRow(c.values, nil)
		if err == nil {
			// encodeVersionedRow prepends a timestamp — we seed that too.
			f.Add(enc)
			// Also seed the fast-path binary encoding (temporal_test uses this).
			now := time.Now().UnixNano()
			buf := make([]byte, 0, 256)
			if fast, ok := encodeVersionedRowFast(c.values, now, buf); ok {
				f.Add(fast)
			}
		}
	}

	// Also seed edge cases: empty, partial, truncated.
	f.Add([]byte{'{', '"', 'd', 'a', 't', 'a', '"', ':', '['})
	f.Add([]byte{'{', '"', 'd', 'a', 't', 'a', '"', ':', '[', ']'})
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Cap input to a reasonable size to avoid OOM in the fuzzer.
		if len(data) > 4096 {
			return
		}
		// Vary the number of columns to exercise different parsing paths.
		for _, numCols := range []int{1, 2, 5, 10, 0, 100} {
			n := max(0, numCols)
			out := make([]interface{}, n)
			stringBuf := make([]string, n)
			// Recover from any panic — the whole point is that unsafe.String
			// should NEVER panic, not even with corrupt data.
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("panic with numCols=%d, len(data)=%d: %v", numCols, len(data), r)
					}
				}()
				decodeVersionedRowFastEx(data, numCols, out, stringBuf, 0)
			}()
		}
	})
}

// FuzzDecodeBinaryVersionedRow fuzzes decodeBinaryVersionedRow which also
// parses raw bytes into row values using slice bounds derived from the data.
func FuzzDecodeBinaryVersionedRow(f *testing.F) {
	// Seed with a valid binary-encoded row.
	values := []interface{}{int64(42), "hello", 3.14, nil, true}
	enc, err := encodeBinaryVersionedRow(values, RowVersion{CreatedAt: 1000})
	if err == nil {
		f.Add(enc)
	}
	// Edge cases.
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{255, 255, 255, 255})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic with len(data)=%d: %v", len(data), r)
				}
			}()
			decodeBinaryVersionedRow(data, 5)
		}()
	})
}

// FuzzDecodeLiveRowFull fuzzes decodeLiveRowFull which processes encoded rows.
func FuzzDecodeLiveRowFull(f *testing.F) {
	values := []interface{}{int64(1), "test"}
	enc, err := encodeVersionedRow(values, nil)
	if err == nil {
		f.Add(enc)
	}
	f.Add([]byte{'{', '"', 'd', 'a', 't', 'a', '"', ':', '[', '"', 'a', '"', ']'})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic with len(data)=%d: %v", len(data), r)
				}
			}()
			decodeLiveRowFull(data, 3)
		}()
	})
}
