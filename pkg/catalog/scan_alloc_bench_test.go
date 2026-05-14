package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func BenchmarkScanTableRowsNoStringNoGrowth(b *testing.B) {
	pool := storage.NewBufferPool(256, storage.NewMemory())
	tree, _ := btree.NewBTree(pool)

	for i := 0; i < 1000; i++ {
		row, _ := encodeVersionedRow([]interface{}{int64(i), int64(i)}, nil)
		tree.Put(int64ToStrBytes(int64(i)), row)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := tree.Scan(nil, nil)
		numCols := 2
		flatBuf := make([]interface{}, 2000)
		rows := make([][]interface{}, 0, 1000)
		rowIdx := 0
		for iter.HasNext() {
			_, valueData, _ := iter.NextString()
			start := rowIdx * numCols
			end := start + numCols
			row := flatBuf[start:end]
			vrow, _ := decodeVersionedRowFast(valueData, numCols, row)
			rows = append(rows, vrow.Data)
			rowIdx++
		}
		iter.Close()
		_ = rows
	}
}

func BenchmarkScanTableRowsStringOnly(b *testing.B) {
	pool := storage.NewBufferPool(256, storage.NewMemory())
	tree, _ := btree.NewBTree(pool)

	for i := 0; i < 1000; i++ {
		row, _ := encodeVersionedRow([]interface{}{"value-123", "value-123"}, nil)
		tree.Put(int64ToStrBytes(int64(i)), row)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := tree.Scan(nil, nil)
		numCols := 2
		flatBuf := make([]interface{}, 2000)
		rows := make([][]interface{}, 0, 1000)
		rowIdx := 0
		for iter.HasNext() {
			_, valueData, _ := iter.NextString()
			start := rowIdx * numCols
			end := start + numCols
			row := flatBuf[start:end]
			vrow, _ := decodeVersionedRowFast(valueData, numCols, row)
			rows = append(rows, vrow.Data)
			rowIdx++
		}
		iter.Close()
		_ = rows
	}
}

func BenchmarkScanOnlyNoDecode(b *testing.B) {
	pool := storage.NewBufferPool(256, storage.NewMemory())
	tree, _ := btree.NewBTree(pool)

	for i := 0; i < 1000; i++ {
		row, _ := encodeVersionedRow([]interface{}{int64(i), int64(i)}, nil)
		tree.Put(int64ToStrBytes(int64(i)), row)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := tree.Scan(nil, nil)
		for iter.HasNext() {
			_, valueData, _ := iter.NextString()
			_ = valueData
		}
		iter.Close()
	}
}
