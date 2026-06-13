package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestDiskBTreeGetRejectsCorruptPageType(t *testing.T) {
	tree, pm, cleanup := setupDiskBTree(t)
	defer cleanup()

	if err := tree.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	page, err := pm.GetPage(tree.RootPageID())
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	page.Data()[4] = 0xff
	pm.GetPool().Unpin(page)

	_, err = tree.Get([]byte("key"))
	if !errors.Is(err, storage.ErrPageCorrupted) {
		t.Fatalf("Get error = %v, want ErrPageCorrupted", err)
	}
}

func TestDiskBTreeGetRejectsTruncatedEntryPayload(t *testing.T) {
	tree, pm, cleanup := setupDiskBTree(t)
	defer cleanup()

	if err := tree.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	page, err := pm.GetPage(tree.RootPageID())
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data := page.Data()
	binary.LittleEndian.PutUint16(data[storage.PageHeaderSize:storage.PageHeaderSize+2], storage.PageSize)
	binary.LittleEndian.PutUint16(data[storage.PageHeaderSize+2:storage.PageHeaderSize+4], storage.PageSize)
	pm.GetPool().Unpin(page)

	_, err = tree.Get([]byte("key"))
	if !errors.Is(err, storage.ErrPageCorrupted) {
		t.Fatalf("Get error = %v, want ErrPageCorrupted", err)
	}
}

func TestDiskBTreeGetRejectsEntryPayloadBeyondFreeStart(t *testing.T) {
	tree, pm, cleanup := setupDiskBTree(t)
	defer cleanup()

	if err := tree.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	page, err := pm.GetPage(tree.RootPageID())
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data := page.Data()
	binary.LittleEndian.PutUint16(data[storage.PageHeaderSize+2:storage.PageHeaderSize+4], 128)
	pm.GetPool().Unpin(page)

	_, err = tree.Get([]byte("key"))
	if !errors.Is(err, storage.ErrPageCorrupted) {
		t.Fatalf("Get error = %v, want ErrPageCorrupted", err)
	}
}

func TestNewDiskBTreeCountsSplitTreeOnReopen(t *testing.T) {
	tree, pm, cleanup := setupDiskBTree(t)
	defer cleanup()

	for i := 0; i < 130; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		if err := tree.Put(key, []byte("value")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	reopened, err := NewDiskBTree(pm)
	if err != nil {
		t.Fatalf("NewDiskBTree after split: %v", err)
	}
	if reopened.Size() != 130 {
		t.Fatalf("reopened size = %d, want 130", reopened.Size())
	}
}
