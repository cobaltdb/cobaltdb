package btree

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

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
