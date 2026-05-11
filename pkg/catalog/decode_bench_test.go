package catalog

import "testing"

var sampleData = []byte(`{"created_at":1,"deleted_at":0,"data":[1,"value-123"]}`)
var sampleDataInt = []byte(`{"created_at":1,"deleted_at":0,"data":[1,2]}`)

func BenchmarkDecodeOneRowUseIntOnly(b *testing.B) {
	out := make([]interface{}, 0, 2)
	for i := 0; i < b.N; i++ {
		vrow, ok := decodeVersionedRowFast(sampleData, 2, out)
		if !ok {
			b.Fatal("decode failed")
		}
		_ = vrow.Data[0]
	}
}

func BenchmarkDecodeOneRowUseStringOnly(b *testing.B) {
	out := make([]interface{}, 0, 2)
	for i := 0; i < b.N; i++ {
		vrow, ok := decodeVersionedRowFast(sampleData, 2, out)
		if !ok {
			b.Fatal("decode failed")
		}
		_ = vrow.Data[1]
	}
}

func BenchmarkDecodeOneRowIntRow(b *testing.B) {
	out := make([]interface{}, 0, 2)
	for i := 0; i < b.N; i++ {
		vrow, ok := decodeVersionedRowFast(sampleDataInt, 2, out)
		if !ok {
			b.Fatal("decode failed")
		}
		_ = vrow.Data[0]
		_ = vrow.Data[1]
	}
}
