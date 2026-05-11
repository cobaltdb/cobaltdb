package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func BenchmarkSelectDirect(b *testing.B) {
	pool := storage.NewBufferPool(256, storage.NewMemory())
	cat := &Catalog{pool: pool}
	cat.tables = make(map[string]*TableDef)
	cat.tableTrees = make(map[string]btree.TreeStore)
	cat.indexes = make(map[string]*IndexDef)

	// Create table
	cat.tables["bench_direct"] = &TableDef{
		Name: "bench_direct",
		Columns: []ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "TEXT"},
		},
	}

	tree, _ := btree.NewBTree(pool)
	cat.tableTrees["bench_direct"] = tree

	// Insert 1000 rows
	for i := 0; i < 1000; i++ {
		row, _ := encodeVersionedRow([]interface{}{int64(i), "value-123"}, nil)
		tree.Put(int64ToStrBytes(int64(i)), row)
	}

	// Build SELECT statement once
	parsed, _ := query.Parse("SELECT id, value FROM bench_direct")
	stmt := parsed.(*query.SelectStmt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, rows, err := cat.Select(stmt, nil)
		if err != nil {
			b.Fatal(err)
		}
		_ = rows
	}
	b.StopTimer()
}

func int64ToStrBytes(n int64) []byte {
	var buf [20]byte
	i := 19
	neg := n < 0
	if neg {
		n = -n
	}
	for {
		buf[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			break
		}
		i--
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return buf[i:]
}
