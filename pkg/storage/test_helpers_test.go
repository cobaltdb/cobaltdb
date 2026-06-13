package storage

import "testing"

func writeTestPage(t *testing.T, backend Backend, pageID uint32, pageType PageType) {
	t.Helper()

	page := NewPage(pageID, pageType)
	if pageID == 0 && pageType == PageTypeMeta {
		meta := NewMetaPage()
		meta.Serialize(page.Data)
	}
	if _, err := backend.WriteAt(page.Data, int64(pageID)*int64(PageSize)); err != nil {
		t.Fatalf("write test page %d: %v", pageID, err)
	}
}

type shortWriteBackend struct {
	Backend
	limit int
}

func (b *shortWriteBackend) WriteAt(buf []byte, offset int64) (int, error) {
	if b.limit >= 0 && len(buf) > b.limit {
		return b.Backend.WriteAt(buf[:b.limit], offset)
	}
	return b.Backend.WriteAt(buf, offset)
}

type shortReadBackend struct {
	Backend
	limit int
}

func (b *shortReadBackend) ReadAt(buf []byte, offset int64) (int, error) {
	if b.limit >= 0 && len(buf) > b.limit {
		return b.Backend.ReadAt(buf[:b.limit], offset)
	}
	return b.Backend.ReadAt(buf, offset)
}
