package btree

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestOpenBTreeWithLimitRebuildsAccountingAndEvictsLoadedEntries(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTreeWithLimit(pool, 0)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"alpha": strings.Repeat("a", 32),
		"beta":  strings.Repeat("b", 32),
		"gamma": strings.Repeat("c", 32),
	}
	for key, value := range values {
		if err := tree.PutString(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tree.Flush(); err != nil {
		t.Fatal(err)
	}

	limit := int64(len("alpha") + len(values["alpha"]))
	reopened, err := OpenBTreeWithLimitStrict(pool, tree.RootPageID(), limit)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.MemoryUsed(); got > limit {
		t.Fatalf("loaded memory usage = %d, limit = %d", got, limit)
	}
	if got := reopened.Size(); got != len(values) {
		t.Fatalf("loaded key count = %d, want %d", got, len(values))
	}
	for key, want := range values {
		got, err := reopened.GetString(key)
		if err != nil {
			t.Fatalf("GetString(%q): %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("GetString(%q) = %q, want %q", key, got, want)
		}
	}

	var lruEntries int
	for i := range reopened.shards {
		sh := &reopened.shards[i]
		if sh.lruList.Len() != len(sh.lruMap) {
			t.Fatalf("shard %d LRU list/map mismatch: %d/%d", i, sh.lruList.Len(), len(sh.lruMap))
		}
		lruEntries += len(sh.lruMap)
	}
	if lruEntries == 0 {
		t.Fatal("reopened tree did not rebuild any LRU entries")
	}
}

func TestOpenBTreeStrictRejectsTruncatedSerializedEntry(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	rootPage, err := pool.NewPage(storage.PageTypeLeaf)
	if err != nil {
		t.Fatal(err)
	}
	rootID := rootPage.ID()

	pageData := rootPage.Data()[storage.PageHeaderSize:]
	binary.LittleEndian.PutUint32(pageData[0:4], 1)      // totalCount
	binary.LittleEndian.PutUint32(pageData[4:8], 0)      // overflowCount
	binary.LittleEndian.PutUint16(pageData[8:10], 50000) // keyLen exceeds available page data
	rootPage.SetDirty(true)
	pool.Unpin(rootPage)
	if err := pool.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	tree, err := OpenBTreeWithLimitStrict(pool, rootID, 0)
	if err == nil {
		t.Fatal("expected corrupt serialized BTree entry to fail strict open")
	}
	if tree == nil || tree.LoadError() == nil {
		t.Fatal("expected load error to be retained on returned tree")
	}
	if !strings.Contains(err.Error(), "exceeds remaining data") {
		t.Fatalf("expected corruption error, got %v", err)
	}
}

func TestOpenBTreeStrictRejectsImpossibleEntryCount(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	rootPage, err := pool.NewPage(storage.PageTypeLeaf)
	if err != nil {
		t.Fatal(err)
	}
	rootID := rootPage.ID()

	pageData := rootPage.Data()[storage.PageHeaderSize:]
	binary.LittleEndian.PutUint32(pageData[0:4], ^uint32(0)) // totalCount
	binary.LittleEndian.PutUint32(pageData[4:8], 0)          // overflowCount
	rootPage.SetDirty(true)
	pool.Unpin(rootPage)
	if err := pool.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	_, err = OpenBTreeWithLimitStrict(pool, rootID, 0)
	if err == nil {
		t.Fatal("expected impossible serialized BTree entry count to fail strict open")
	}
	if !strings.Contains(err.Error(), "entry count") {
		t.Fatalf("expected entry count corruption error, got %v", err)
	}
}
