package btree

import (
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	tree, _ := NewBTree(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(string(rune('a'+i%26)) + string(rune('a'+i/26)))
		tree.Put(key, []byte("value"))
	}
}

func BenchmarkPutSequential(b *testing.B) {
	tree, _ := NewBTree(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(strconv.Itoa(i))
		tree.Put(key, []byte("value"))
	}
}

func BenchmarkGet(b *testing.B) {
	tree, _ := NewBTree(nil)

	// Insert test data
	for i := 0; i < 10000; i++ {
		key := []byte(strconv.Itoa(i))
		tree.Put(key, []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get([]byte(strconv.Itoa(i % 10000)))
	}
}

func BenchmarkScan(b *testing.B) {
	tree, _ := NewBTree(nil)

	// Insert test data
	for i := 0; i < 1000; i++ {
		key := []byte(strconv.Itoa(i))
		tree.Put(key, []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := tree.Scan(nil, nil)
		count := 0
		for iter.HasNext() {
			iter.Next()
			count++
		}
		iter.Close()
	}
}

func BenchmarkDelete(b *testing.B) {
	tree, _ := NewBTree(nil)

	// Insert test data
	for i := 0; i < 1000; i++ {
		key := []byte(strconv.Itoa(i))
		tree.Put(key, []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(strconv.Itoa(i % 1000))
		tree.Delete(key)
		// Re-insert for next iteration
		tree.Put(key, []byte("value"))
	}
}

func BenchmarkPutLarge(b *testing.B) {
	tree, _ := NewBTree(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			key := []byte(fmt.Sprintf("%d", i*1000+j))
			tree.Put(key, []byte("value"))
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	tree, _ := NewBTree(nil)

	// Insert test data
	for i := 0; i < 1000; i++ {
		key := []byte(strconv.Itoa(i))
		tree.Put(key, []byte("value1"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(strconv.Itoa(i % 1000))
		tree.Put(key, []byte("value2"))
	}
}
