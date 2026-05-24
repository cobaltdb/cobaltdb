package txn

import (
	"testing"
)

func TestVersionStoreCommitAndGet(t *testing.T) {
	vs := NewVersionStore()

	vs.Commit(WriteKey{Key: "key1"}, []byte("v1"), 1)
	vs.Commit(WriteKey{Key: "key1"}, []byte("v2"), 2)
	vs.Commit(WriteKey{Key: "key1"}, []byte("v3"), 3)

	// Snapshot reads
	val, err := vs.GetAtSnapshot(WriteKey{Key: "key1"}, 1)
	if err != nil || string(val) != "v1" {
		t.Fatalf("expected v1 at snapshot 1, got %s, err=%v", val, err)
	}

	val, err = vs.GetAtSnapshot(WriteKey{Key: "key1"}, 2)
	if err != nil || string(val) != "v2" {
		t.Fatalf("expected v2 at snapshot 2, got %s, err=%v", val, err)
	}

	val, err = vs.GetAtSnapshot(WriteKey{Key: "key1"}, 3)
	if err != nil || string(val) != "v3" {
		t.Fatalf("expected v3 at snapshot 3, got %s, err=%v", val, err)
	}

	// Snapshot 0 should return not found
	_, err = vs.GetAtSnapshot(WriteKey{Key: "key1"}, 0)
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound at snapshot 0, got %v", err)
	}

	// Nonexistent key
	_, err = vs.GetAtSnapshot(WriteKey{Key: "nokey"}, 3)
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for nonexistent key, got %v", err)
	}
}

func TestVersionStoreDelete(t *testing.T) {
	vs := NewVersionStore()

	vs.Commit(WriteKey{Key: "key1"}, []byte("v1"), 1)
	vs.Delete(WriteKey{Key: "key1"}, 2)

	// Snapshot 1 should see v1
	val, err := vs.GetAtSnapshot(WriteKey{Key: "key1"}, 1)
	if err != nil || string(val) != "v1" {
		t.Fatalf("expected v1, got %s, err=%v", val, err)
	}

	// Snapshot 2 should see deleted
	_, err = vs.GetAtSnapshot(WriteKey{Key: "key1"}, 2)
	if err != ErrKeyDeleted {
		t.Fatalf("expected ErrKeyDeleted at snapshot 2, got %v", err)
	}

	// Snapshot 3 should also see deleted
	_, err = vs.GetAtSnapshot(WriteKey{Key: "key1"}, 3)
	if err != ErrKeyDeleted {
		t.Fatalf("expected ErrKeyDeleted at snapshot 3, got %v", err)
	}
}

func TestVersionStoreGetCurrent(t *testing.T) {
	vs := NewVersionStore()

	_, err := vs.GetCurrent(WriteKey{Key: "key1"})
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	vs.Commit(WriteKey{Key: "key1"}, []byte("v1"), 1)
	vs.Commit(WriteKey{Key: "key1"}, []byte("v2"), 2)

	val, err := vs.GetCurrent(WriteKey{Key: "key1"})
	if err != nil || string(val) != "v2" {
		t.Fatalf("expected v2, got %s, err=%v", val, err)
	}

	vs.Delete(WriteKey{Key: "key1"}, 3)
	_, err = vs.GetCurrent(WriteKey{Key: "key1"})
	if err != ErrKeyDeleted {
		t.Fatalf("expected ErrKeyDeleted after delete, got %v", err)
	}
}

func TestVersionStoreCopiesCommittedValues(t *testing.T) {
	vs := NewVersionStore()
	key := WriteKey{Key: "key1"}
	value := []byte("v1")

	vs.Commit(key, value, 1)
	value[0] = 'x'

	current, err := vs.GetCurrent(key)
	if err != nil {
		t.Fatalf("GetCurrent: %v", err)
	}
	if string(current) != "v1" {
		t.Fatalf("Commit retained caller-owned value: %q", current)
	}

	current[0] = 'y'
	currentAgain, err := vs.GetCurrent(key)
	if err != nil {
		t.Fatalf("GetCurrent second read: %v", err)
	}
	if string(currentAgain) != "v1" {
		t.Fatalf("GetCurrent returned mutable value: %q", currentAgain)
	}

	snapshot, err := vs.GetAtSnapshot(key, 1)
	if err != nil {
		t.Fatalf("GetAtSnapshot: %v", err)
	}
	snapshot[0] = 'z'
	snapshotAgain, err := vs.GetAtSnapshot(key, 1)
	if err != nil {
		t.Fatalf("GetAtSnapshot second read: %v", err)
	}
	if string(snapshotAgain) != "v1" {
		t.Fatalf("GetAtSnapshot returned mutable value: %q", snapshotAgain)
	}
}

func TestVersionStorePrune(t *testing.T) {
	vs := NewVersionStore()

	vs.Commit(WriteKey{Key: "key1"}, []byte("v1"), 1)
	vs.Commit(WriteKey{Key: "key1"}, []byte("v2"), 2)
	vs.Commit(WriteKey{Key: "key1"}, []byte("v3"), 3)
	vs.Commit(WriteKey{Key: "key1"}, []byte("v4"), 4)

	if vs.VersionCount() != 4 {
		t.Fatalf("expected 4 versions, got %d", vs.VersionCount())
	}

	// Prune versions older than snapshot 3
	pruned := vs.Prune(3)
	if pruned < 2 {
		t.Fatalf("expected at least 2 pruned, got %d", pruned)
	}

	// v3 should still be visible at snapshot 3
	val, err := vs.GetAtSnapshot(WriteKey{Key: "key1"}, 3)
	if err != nil || string(val) != "v3" {
		t.Fatalf("expected v3 at snapshot 3 after prune, got %s, err=%v", val, err)
	}

	// v4 should still be visible
	val, err = vs.GetAtSnapshot(WriteKey{Key: "key1"}, 4)
	if err != nil || string(val) != "v4" {
		t.Fatalf("expected v4 at snapshot 4 after prune, got %s, err=%v", val, err)
	}
}

func TestVersionStoreLatestVersion(t *testing.T) {
	vs := NewVersionStore()

	if vs.GetLatestVersion(WriteKey{Key: "key1"}) != 0 {
		t.Fatal("expected 0 for nonexistent key")
	}

	vs.Commit(WriteKey{Key: "key1"}, []byte("v1"), 5)
	if vs.GetLatestVersion(WriteKey{Key: "key1"}) != 5 {
		t.Fatalf("expected 5, got %d", vs.GetLatestVersion(WriteKey{Key: "key1"}))
	}

	vs.Commit(WriteKey{Key: "key1"}, []byte("v2"), 10)
	if vs.GetLatestVersion(WriteKey{Key: "key1"}) != 10 {
		t.Fatalf("expected 10, got %d", vs.GetLatestVersion(WriteKey{Key: "key1"}))
	}
}

func TestVersionStoreLen(t *testing.T) {
	vs := NewVersionStore()

	if vs.Len() != 0 {
		t.Fatal("expected 0 keys")
	}

	vs.Commit(WriteKey{Key: "a"}, []byte("1"), 1)
	vs.Commit(WriteKey{Key: "b"}, []byte("2"), 1)
	vs.Commit(WriteKey{Key: "c"}, []byte("3"), 1)

	if vs.Len() != 3 {
		t.Fatalf("expected 3 keys, got %d", vs.Len())
	}
}
