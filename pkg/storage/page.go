package storage

import (
	"encoding/binary"
	"errors"
)

const (
	// PageSize is the default page size (4KB)
	PageSize = 4096
	// MaxPageSize is the maximum allowed page size (64KB)
	MaxPageSize = 65536
)

var (
	ErrInvalidPageID   = errors.New("invalid page ID")
	ErrInvalidPageType = errors.New("invalid page type")
	ErrPageCorrupted   = errors.New("page is corrupted")
)

// PageType represents the type of a page
type PageType uint8

const (
	// PageTypeMeta is the database metadata page (page 0)
	PageTypeMeta PageType = 0x01
	// PageTypeInternal is a B+Tree internal node
	PageTypeInternal PageType = 0x02
	// PageTypeLeaf is a B+Tree leaf node
	PageTypeLeaf PageType = 0x03
	// PageTypeOverflow is for large values that don't fit in a page
	PageTypeOverflow PageType = 0x04
	// PageTypeFreeList tracks free pages
	PageTypeFreeList PageType = 0x05
)

// PageHeader is the header stored at the beginning of each page (16 bytes)
type PageHeader struct {
	PageID    uint32   // 4 bytes - page number
	PageType  PageType // 1 byte - type flag
	Flags     uint8    // 1 byte - dirty, pinned, etc.
	CellCount uint16   // 2 bytes - number of cells in page
	FreeStart uint16   // 2 bytes - offset to free space start
	FreeEnd   uint16   // 2 bytes - offset to free space end
	RightPtr  uint32   // 4 bytes - right sibling / overflow pointer
}

const PageHeaderSize = 16

// Page represents a database page
type Page struct {
	Header PageHeader
	Data   []byte // PageSize bytes, includes header at offset 0
}

// NewPage creates a new page with the given ID and type
func NewPage(pageID uint32, pageType PageType) *Page {
	data := make([]byte, PageSize)
	page := &Page{
		Header: PageHeader{
			PageID:    pageID,
			PageType:  pageType,
			Flags:     0,
			CellCount: 0,
			FreeStart: PageHeaderSize,
			FreeEnd:   uint16(PageSize),
			RightPtr:  0,
		},
		Data: data,
	}
	page.SerializeHeader()
	return page
}

// SerializeHeader writes the header to the page data
func (p *Page) SerializeHeader() {
	binary.LittleEndian.PutUint32(p.Data[0:4], p.Header.PageID)
	p.Data[4] = byte(p.Header.PageType)
	p.Data[5] = p.Header.Flags
	binary.LittleEndian.PutUint16(p.Data[6:8], p.Header.CellCount)
	binary.LittleEndian.PutUint16(p.Data[8:10], p.Header.FreeStart)
	binary.LittleEndian.PutUint16(p.Data[10:12], p.Header.FreeEnd)
	binary.LittleEndian.PutUint32(p.Data[12:16], p.Header.RightPtr)
}

// DeserializeHeader reads the header from the page data
func (p *Page) DeserializeHeader() {
	p.Header.PageID = binary.LittleEndian.Uint32(p.Data[0:4])
	p.Header.PageType = PageType(p.Data[4])
	p.Header.Flags = p.Data[5]
	p.Header.CellCount = binary.LittleEndian.Uint16(p.Data[6:8])
	p.Header.FreeStart = binary.LittleEndian.Uint16(p.Data[8:10])
	p.Header.FreeEnd = binary.LittleEndian.Uint16(p.Data[10:12])
	p.Header.RightPtr = binary.LittleEndian.Uint32(p.Data[12:16])
}

// FreeSpace returns the amount of free space in the page
func (p *Page) FreeSpace() int {
	return int(p.Header.FreeEnd) - int(p.Header.FreeStart)
}

// IsDirty returns true if the page has been modified
func (p *Page) IsDirty() bool {
	return p.Header.Flags&0x01 != 0
}

// SetDirty marks the page as dirty
func (p *Page) SetDirty(dirty bool) {
	if dirty {
		p.Header.Flags |= 0x01
	} else {
		p.Header.Flags &= ^uint8(0x01)
	}
	p.SerializeHeader()
}

// IsPinned returns true if the page is pinned in memory
func (p *Page) IsPinned() bool {
	return p.Header.Flags&0x02 != 0
}

// SetPinned marks the page as pinned
func (p *Page) SetPinned(pinned bool) {
	if pinned {
		p.Header.Flags |= 0x02
	} else {
		p.Header.Flags &= ^uint8(0x02)
	}
	p.SerializeHeader()
}

// MetaPage is the database metadata stored in page 0
type MetaPage struct {
	Magic      [4]byte // "CBDB" (CobaltDB)
	Version    uint32  // format version
	PageSize   uint32  // page size in bytes
	PageCount  uint32  // total pages in file
	FreeListID uint32  // page ID of free list head
	RootPageID uint32  // root page of system catalog B+Tree
	TxnCounter uint64  // monotonic transaction counter
	Checksum   uint32  // CRC32 of this page
}

const (
	MagicString = "CBDB"
	Version     = 1
)

// NewMetaPage creates a new metadata page
func NewMetaPage() *MetaPage {
	return &MetaPage{
		Magic:      [4]byte{'C', 'B', 'D', 'B'},
		Version:    Version,
		PageSize:   PageSize,
		PageCount:  1, // Page 0 is the meta page
		FreeListID: 0,
		RootPageID: 0,
		TxnCounter: 0,
		Checksum:   0,
	}
}

// Serialize writes the meta page to a page's data
func (m *MetaPage) Serialize(data []byte) {
	copy(data[0:4], m.Magic[:])
	binary.LittleEndian.PutUint32(data[4:8], m.Version)
	binary.LittleEndian.PutUint32(data[8:12], m.PageSize)
	binary.LittleEndian.PutUint32(data[12:16], m.PageCount)
	binary.LittleEndian.PutUint32(data[16:20], m.FreeListID)
	binary.LittleEndian.PutUint32(data[20:24], m.RootPageID)
	binary.LittleEndian.PutUint64(data[24:32], m.TxnCounter)
	// Checksum is calculated separately
}

// Deserialize reads the meta page from a page's data
func (m *MetaPage) Deserialize(data []byte) error {
	copy(m.Magic[:], data[0:4])
	m.Version = binary.LittleEndian.Uint32(data[4:8])
	m.PageSize = binary.LittleEndian.Uint32(data[8:12])
	m.PageCount = binary.LittleEndian.Uint32(data[12:16])
	m.FreeListID = binary.LittleEndian.Uint32(data[16:20])
	m.RootPageID = binary.LittleEndian.Uint32(data[20:24])
	m.TxnCounter = binary.LittleEndian.Uint64(data[24:32])
	m.Checksum = binary.LittleEndian.Uint32(data[32:36])

	// Verify magic
	if string(m.Magic[:]) != MagicString {
		return ErrPageCorrupted
	}

	return nil
}

// Validate checks if the meta page is valid
func (m *MetaPage) Validate() error {
	if string(m.Magic[:]) != MagicString {
		return ErrPageCorrupted
	}
	if m.Version != Version {
		return errors.New("unsupported database version")
	}
	if m.PageSize != PageSize {
		return errors.New("unsupported page size")
	}
	return nil
}
