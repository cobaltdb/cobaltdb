package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func BenchmarkScanAndDecode(b *testing.B) {
	pool := storage.NewBufferPool(256, storage.NewMemory())
	tree, _ := btree.NewBTree(pool)

	// Insert 1000 rows
	for i := 0; i < 1000; i++ {
		row, _ := encodeVersionedRow([]interface{}{int64(i), "value-123"}, nil)
		tree.Put(int64ToStrBytes(int64(i)), row)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := tree.Scan(nil, nil)
		out := make([]interface{}, 0, 2)
		for iter.HasNext() {
			_, valueData, _ := iter.NextString()
			out = out[:0]
			vrow, _ := decodeVersionedRowFast(valueData, 2, out)
			_ = vrow.Data[0]
			_ = vrow.Data[1]
		}
		iter.Close()
	}
	b.StopTimer()
}
