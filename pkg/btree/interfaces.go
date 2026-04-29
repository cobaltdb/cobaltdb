package btree

// KVStore defines the interface for key-value storage operations.
// Both in-memory and disk-backed B-trees implement this interface.
type KVStore interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(startKey, endKey []byte) (*Iterator, error)
	Size() int
}
