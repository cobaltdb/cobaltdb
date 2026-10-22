package catalog

import (
	"math"
	"testing"
)

func TestBinaryRowPreservesIntegerBoundaries(t *testing.T) {
	for _, want := range []int64{math.MinInt64, -9007199254740993, 9007199254740993, math.MaxInt64} {
		encoded, err := encodeVersionedRow([]interface{}{want, []byte{0xff}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		row, err := decodeVersionedRow(encoded, 2)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := row.Data[0].(int64); !ok || got != want {
			t.Fatalf("decoded %T(%v), want int64(%d)", row.Data[0], row.Data[0], want)
		}
	}
}

func TestBinaryRowRejectsTrailingData(t *testing.T) {
	const row = `{"data":[9007199254740993,"/w=="],"bin":[1],"version":{}}`
	for _, suffix := range []string{"}", "]", "{}", "null", "garbage"} {
		t.Run(suffix, func(t *testing.T) {
			if _, err := decodeBinaryVersionedRow([]byte(row+suffix), 2); err == nil {
				t.Fatalf("accepted trailing data %q", suffix)
			}
		})
	}
	if _, err := decodeBinaryVersionedRow([]byte(row+" \n\t"), 2); err != nil {
		t.Fatalf("rejected trailing whitespace: %v", err)
	}
}
