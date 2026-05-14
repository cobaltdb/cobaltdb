package catalog

import (
	"testing"
)

func BenchmarkDecodeScanRow(b *testing.B) {
	// Matches the JSON encoded by encodeVersionedRowFast for (0, "value-0")
	data := []byte(`{"data":[0,"value-0"],"version":{"created_at":1700000000,"deleted_at":0}}`)
	out := make([]interface{}, 2)
	b.ReportAllocs()
	b.ResetTimer()
	var sink VersionedRow
	for i := 0; i < b.N; i++ {
		sink, _ = decodeVersionedRowFast(data, 2, out)
	}
	_ = sink
}
