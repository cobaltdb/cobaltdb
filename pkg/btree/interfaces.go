package btree

// TreeIterator defines the common iterator interface for both BTree and DiskBTree.
type TreeIterator interface {
	HasNext() bool
	Next() ([]byte, []byte, error)
	NextString() (string, []byte, error)
	Close() error
	First() bool
	Valid() bool
}

// TreeStore defines the full interface for tree storage operations.
// Both in-memory BTree and disk-based DiskBTree implement this interface.
type TreeStore interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(startKey, endKey []byte) (TreeIterator, error)
	PutBatch(keys [][]byte, values [][]byte) error
	DeleteBatch(keys [][]byte) error
	Size() int
	Flush() error
	RootPageID() uint32
}
