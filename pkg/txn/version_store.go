package txn

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyDeleted  = errors.New("key deleted")
)

// VersionedValue holds a single version of a key's value.
type VersionedValue struct {
	Value     []byte
	Version   uint64 // commit timestamp (transaction ID)
	Deleted   bool
	Prev      *VersionedValue
}

// versionValuePool recycles VersionedValue structs to eliminate one heap
// allocation per MVCC commit.
var versionValuePool sync.Pool

func init() {
	for i := 0; i < 1024; i++ {
		versionValuePool.Put(&VersionedValue{})
	}
}

// VersionStore maintains version chains per key for MVCC snapshot reads.
type VersionStore struct {
	mu       sync.RWMutex
	versions map[WriteKey]*VersionedValue // key → head of version chain
	count    int64 // total version entries for GC tracking
}

// NewVersionStore creates a new VersionStore.
func NewVersionStore() *VersionStore {
	return &VersionStore{
		versions: make(map[WriteKey]*VersionedValue),
	}
}

// Commit adds a new version for a key at the given commit timestamp.
func (vs *VersionStore) Commit(key WriteKey, value []byte, commitTS uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	prev := vs.versions[key]
	var vv *VersionedValue
	if v := versionValuePool.Get(); v != nil {
		vv = v.(*VersionedValue)
		vv.Value = value
		vv.Version = commitTS
		vv.Deleted = false
		vv.Prev = prev
	} else {
		vv = &VersionedValue{
			Value:   value,
			Version: commitTS,
			Prev:    prev,
		}
	}
	vs.versions[key] = vv
	vs.count++
}

// Delete marks a key as deleted at the given commit timestamp.
func (vs *VersionStore) Delete(key WriteKey, commitTS uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	prev := vs.versions[key]
	var vv *VersionedValue
	if v := versionValuePool.Get(); v != nil {
		vv = v.(*VersionedValue)
		vv.Value = nil
		vv.Version = commitTS
		vv.Deleted = true
		vv.Prev = prev
	} else {
		vv = &VersionedValue{
			Value:   nil,
			Version: commitTS,
			Deleted: true,
			Prev:    prev,
		}
	}
	vs.versions[key] = vv
	vs.count++
}

// GetAtSnapshot returns the value visible at the given snapshot timestamp.
// It walks the version chain to find the newest version <= snapshotTS.
func (vs *VersionStore) GetAtSnapshot(key WriteKey, snapshotTS uint64) ([]byte, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	chain := vs.versions[key]
	for chain != nil {
		if chain.Version <= snapshotTS {
			if chain.Deleted {
				return nil, ErrKeyDeleted
			}
			return chain.Value, nil
		}
		chain = chain.Prev
	}
	return nil, ErrKeyNotFound
}

// GetCurrent returns the latest committed value for a key.
func (vs *VersionStore) GetCurrent(key WriteKey) ([]byte, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	head := vs.versions[key]
	if head == nil {
		return nil, ErrKeyNotFound
	}
	if head.Deleted {
		return nil, ErrKeyDeleted
	}
	return head.Value, nil
}

// GetLatestVersion returns the latest version timestamp for a key.
func (vs *VersionStore) GetLatestVersion(key WriteKey) uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	head := vs.versions[key]
	if head == nil {
		return 0
	}
	return head.Version
}

// Prune removes version entries older than the minimum active snapshot.
// Versions with commitTS < minActiveTS that have a newer version are garbage collected.
func (vs *VersionStore) Prune(minActiveTS uint64) int {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	pruned := 0
	for key, head := range vs.versions {
		if head == nil || head.Prev == nil {
			continue
		}

		// Walk the chain and prune versions older than minActiveTS
		// that have at least one visible version >= minActiveTS
		pruned = pruneChain(vs.versions, key, head, minActiveTS, pruned)
	}

	vs.count -= int64(pruned)
	return pruned
}

func pruneChain(versions map[WriteKey]*VersionedValue, key WriteKey, head *VersionedValue, minActiveTS uint64, pruned int) int {
	// Find the oldest version that is still visible to at least one active txn
	current := head
	var lastVisible *VersionedValue

	for current != nil {
		if current.Version <= minActiveTS {
			lastVisible = current
			break
		}
		current = current.Prev
	}

	// If we found a visible version, truncate everything before it
	if lastVisible != nil && lastVisible.Prev != nil {
		count := countChain(lastVisible.Prev)
		// Return pruned nodes to the pool for reuse.
		for node := lastVisible.Prev; node != nil; {
			next := node.Prev
			node.Value = nil
			node.Prev = nil
			versionValuePool.Put(node)
			node = next
		}
		lastVisible.Prev = nil
		pruned += count
	}

	return pruned
}

func countChain(v *VersionedValue) int {
	count := 0
	for v != nil {
		count++
		v = v.Prev
	}
	return count
}

// Clear empties the store without deallocating the map buckets.
func (vs *VersionStore) Clear() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for key, head := range vs.versions {
		// Return chained nodes to the pool for reuse.
		for node := head; node != nil; {
			next := node.Prev
			node.Value = nil
			node.Prev = nil
			versionValuePool.Put(node)
			node = next
		}
		delete(vs.versions, key)
	}
	vs.count = 0
}

// Len returns the number of keys in the store.
func (vs *VersionStore) Len() int {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return len(vs.versions)
}

// VersionCount returns the total number of version entries.
func (vs *VersionStore) VersionCount() int64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.count
}
