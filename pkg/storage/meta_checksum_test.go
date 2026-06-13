package storage

import (
	"errors"
	"testing"
)

func TestMetaPageSerializeWritesChecksum(t *testing.T) {
	meta := NewMetaPage()
	page := NewPage(0, PageTypeMeta)

	meta.Serialize(page.Data)
	if meta.Checksum == 0 {
		t.Fatal("expected Serialize to populate checksum")
	}

	decoded := &MetaPage{}
	if err := decoded.Deserialize(page.Data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if decoded.Checksum != meta.Checksum {
		t.Fatalf("checksum mismatch after round trip: got %d want %d", decoded.Checksum, meta.Checksum)
	}
}

func TestMetaPageDeserializeRejectsChecksumMismatch(t *testing.T) {
	meta := NewMetaPage()
	page := NewPage(0, PageTypeMeta)
	meta.Serialize(page.Data)

	page.Data[24] ^= 0xff // corrupt TxnCounter while leaving magic/version/page size valid

	decoded := &MetaPage{}
	err := decoded.Deserialize(page.Data)
	if !errors.Is(err, ErrPageCorrupted) {
		t.Fatalf("expected page corruption error, got %v", err)
	}
}

func TestMetaPageDeserializeAllowsLegacyZeroChecksum(t *testing.T) {
	meta := NewMetaPage()
	page := NewPage(0, PageTypeMeta)
	meta.Serialize(page.Data)
	page.Data[32] = 0
	page.Data[33] = 0
	page.Data[34] = 0
	page.Data[35] = 0

	decoded := &MetaPage{}
	if err := decoded.Deserialize(page.Data); err != nil {
		t.Fatalf("legacy zero-checksum metadata should still deserialize: %v", err)
	}
	if decoded.Checksum != 0 {
		t.Fatalf("decoded checksum = %d, want legacy zero", decoded.Checksum)
	}
}

func TestNewPageManagerRejectsOutOfRangeMetaReferences(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*MetaPage)
	}{
		{
			name: "root page outside page count",
			setup: func(meta *MetaPage) {
				meta.RootPageID = meta.PageCount
			},
		},
		{
			name: "free list outside page count",
			setup: func(meta *MetaPage) {
				meta.FreeListID = meta.PageCount
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := NewMemory()
			page := NewPage(0, PageTypeMeta)
			meta := NewMetaPage()
			tt.setup(meta)
			meta.Serialize(page.Data)

			if _, err := backend.WriteAt(page.Data, 0); err != nil {
				t.Fatalf("failed to seed meta page: %v", err)
			}

			pool := NewBufferPool(4, backend)
			defer pool.Close()

			_, err := NewPageManager(pool)
			if !errors.Is(err, ErrPageCorrupted) {
				t.Fatalf("expected page corruption error, got %v", err)
			}
		})
	}
}
