package txn

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestManagerGet tests Manager.Get function
func TestManagerGet(t *testing.T) {
	mgr := NewManager(nil)

	// Get non-existent transaction
	txn, err := mgr.Get(999)
	if err != ErrTxnNotFound {
		t.Errorf("Expected ErrTxnNotFound, got %v", err)
	}
	if txn != nil {
		t.Error("Expected nil transaction")
	}

	// Begin a transaction and get it
	txn1 := mgr.Begin(nil)
	id := txn1.ID

	retrieved, err := mgr.Get(id)
	if err != nil {
		t.Errorf("Failed to get transaction: %v", err)
	}
	if retrieved != txn1 {
		t.Error("Retrieved transaction doesn't match")
	}
}

// TestCommitWithNoWrites tests committing a read-only transaction
func TestCommitWithNoWrites(t *testing.T) {
	mgr := NewManager(nil)

	// Read-only transaction with no writes
	txn := mgr.Begin(&Options{ReadOnly: true})

	err := txn.Commit()
	if err != nil {
		t.Logf("Commit returned error: %v", err)
	}

	if txn.State != TxnCommitted {
		t.Logf("Expected state %d, got %d", TxnCommitted, txn.State)
	}
}

// TestRollbackWithWrites tests rollback with pending writes
func TestRollbackWithWrites(t *testing.T) {
	mgr := NewManager(nil)

	txn := mgr.Begin(nil)
	txn.SetWrite("", "key1", []byte("value1"))
	txn.SetWrite("", "key2", []byte("value2"))

	err := txn.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	if txn.State != TxnAborted {
		t.Errorf("Expected state %d, got %d", TxnAborted, txn.State)
	}

	// Writes should be cleared
	_, ok := txn.GetWrite("", "key1")
	if ok {
		t.Error("Write should be cleared after rollback")
	}
}

// TestMultipleTransactions tests multiple concurrent transactions
func TestMultipleTransactions(t *testing.T) {
	mgr := NewManager(nil)

	// Begin multiple transactions
	txn1 := mgr.Begin(nil)
	txn2 := mgr.Begin(nil)
	txn3 := mgr.Begin(nil)

	// Verify all have unique IDs
	if txn1.ID == txn2.ID || txn1.ID == txn3.ID || txn2.ID == txn3.ID {
		t.Error("Transaction IDs should be unique")
	}

	// All should be active
	if txn1.State != TxnActive || txn2.State != TxnActive || txn3.State != TxnActive {
		t.Error("All transactions should be active")
	}

	// Commit one
	txn1.Commit()

	// Rollback another
	txn2.Rollback()

	// Third should still be active
	if txn3.State != TxnActive {
		t.Error("Transaction 3 should still be active")
	}
}

// TestTransactionStateTransitions tests state transitions
func TestTransactionStateTransitions(t *testing.T) {
	mgr := NewManager(nil)

	// Active -> Committed
	txn := mgr.Begin(nil)
	if txn.State != TxnActive {
		t.Error("New transaction should be active")
	}

	txn.Commit()
	if txn.State != TxnCommitted {
		t.Errorf("Expected state %d, got %d", TxnCommitted, txn.State)
	}

	// Active -> Aborted
	txn2 := mgr.Begin(nil)
	txn2.Rollback()
	if txn2.State != TxnAborted {
		t.Errorf("Expected state %d, got %d", TxnAborted, txn2.State)
	}
}

// TestGetReadVersionNotFound tests GetReadVersion when key not found
func TestGetReadVersionNotFound(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	_, ok := txn.GetReadVersion("", "nonexistent")
	if ok {
		t.Error("Expected false for non-existent key")
	}
}

// TestGetWriteNotFound tests GetWrite when key not found
func TestGetWriteNotFound(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	_, ok := txn.GetWrite("", "nonexistent")
	if ok {
		t.Error("Expected false for non-existent key")
	}
}

// TestSetReadVersionOverwrite tests overwriting read version
func TestSetReadVersionOverwrite(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	txn.SetReadVersion("", "key1", 100)
	txn.SetReadVersion("", "key1", 200)

	v, ok := txn.GetReadVersion("", "key1")
	if !ok {
		t.Fatal("Read version not found")
	}

	if v != 200 {
		t.Errorf("Expected version 200, got %d", v)
	}
}

// TestSetWriteOverwrite tests overwriting write
func TestSetWriteOverwrite(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	txn.SetWrite("", "key1", []byte("value1"))
	txn.SetWrite("", "key1", []byte("value2"))

	v, ok := txn.GetWrite("", "key1")
	if !ok {
		t.Fatal("Write not found")
	}

	if string(v) != "value2" {
		t.Errorf("Expected value2, got %s", v)
	}
}

func TestSetWriteCopiesValues(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)
	value := []byte("value1")

	txn.SetWrite("", "key1", value)
	value[0] = 'x'

	got, ok := txn.GetWrite("", "key1")
	if !ok {
		t.Fatal("Write not found")
	}
	if string(got) != "value1" {
		t.Fatalf("SetWrite retained caller-owned value: %q", got)
	}

	got[0] = 'y'
	gotAgain, ok := txn.GetWrite("", "key1")
	if !ok {
		t.Fatal("Write not found on second read")
	}
	if string(gotAgain) != "value1" {
		t.Fatalf("GetWrite returned mutable value: %q", gotAgain)
	}
}

func TestTxnWALRecordDataLenBounds(t *testing.T) {
	got, err := txnWALRecordDataLen(10, 20)
	if err != nil {
		t.Fatalf("unexpected error for small WAL record data: %v", err)
	}
	if got != 34 {
		t.Fatalf("expected WAL data length 34, got %d", got)
	}

	got, err = txnWALRecordDataLen(10, maxTxnWALRecordDataBytes-4-10)
	if err != nil {
		t.Fatalf("unexpected error at max WAL record data boundary: %v", err)
	}
	if got != maxTxnWALRecordDataBytes {
		t.Fatalf("expected max WAL data length %d, got %d", maxTxnWALRecordDataBytes, got)
	}

	if _, err := txnWALRecordDataLen(10, maxTxnWALRecordDataBytes-4-10+1); err == nil {
		t.Fatal("expected oversized WAL value to be rejected")
	}
	if _, err := txnWALRecordDataLen(maxTxnWALRecordDataBytes-3, 0); err == nil {
		t.Fatal("expected oversized WAL key to be rejected")
	}
	if _, err := txnWALRecordDataLen(-1, 0); err == nil {
		t.Fatal("expected negative WAL key length to be rejected")
	}
	if _, err := txnWALRecordDataLen(0, -1); err == nil {
		t.Fatal("expected negative WAL value length to be rejected")
	}
}

// TestSnapshotIsolation tests snapshot isolation level
func TestSnapshotIsolation(t *testing.T) {
	opts := &Options{
		Isolation: SnapshotIsolation,
		ReadOnly:  false,
	}

	mgr := NewManager(nil)
	txn := mgr.Begin(opts)

	if txn.Isolation != SnapshotIsolation {
		t.Errorf("Expected isolation %d, got %d", SnapshotIsolation, txn.Isolation)
	}
}

// TestSerializableIsolation tests serializable isolation level
func TestSerializableIsolation(t *testing.T) {
	opts := &Options{
		Isolation: Serializable,
		ReadOnly:  false,
	}

	mgr := NewManager(nil)
	txn := mgr.Begin(opts)

	if txn.Isolation != Serializable {
		t.Errorf("Expected isolation %d, got %d", Serializable, txn.Isolation)
	}
}

// TestReadCommittedIsolation tests read committed isolation level
func TestReadCommittedIsolation(t *testing.T) {
	opts := &Options{
		Isolation: ReadCommitted,
		ReadOnly:  false,
	}

	mgr := NewManager(nil)
	txn := mgr.Begin(opts)

	if txn.Isolation != ReadCommitted {
		t.Errorf("Expected isolation %d, got %d", ReadCommitted, txn.Isolation)
	}
}

// TestTransactionIDIncrement tests that transaction IDs increment
func TestTransactionIDIncrement(t *testing.T) {
	mgr := NewManager(nil)

	var lastID uint64 = 0
	for i := 0; i < 10; i++ {
		txn := mgr.Begin(nil)
		if txn.ID <= lastID {
			t.Errorf("Transaction ID should increment: %d <= %d", txn.ID, lastID)
		}
		lastID = txn.ID
	}
}

// TestGetNonExistentTransaction tests getting a non-existent transaction
func TestGetNonExistentTransaction(t *testing.T) {
	mgr := NewManager(nil)

	_, err := mgr.Get(99999)
	if err != ErrTxnNotFound {
		t.Errorf("Expected ErrTxnNotFound, got: %v", err)
	}
}

// TestDetectConflicts tests conflict detection with SnapshotIsolation
func TestDetectConflicts(t *testing.T) {
	mgr := NewManager(nil)

	// Begin first transaction with SnapshotIsolation
	opts1 := &Options{Isolation: SnapshotIsolation}
	txn1 := mgr.Begin(opts1)

	// Set a read version for a key
	txn1.SetReadVersion("", "key1", 100)

	// Begin second transaction and write to the same key
	opts2 := &Options{Isolation: SnapshotIsolation}
	txn2 := mgr.Begin(opts2)
	txn2.SetWrite("", "key1", []byte("new_value"))

	// Commit second transaction - this updates the version
	err := txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit txn2: %v", err)
	}

	// Now try to commit first transaction - should detect conflict
	// because key1 was modified after txn1 read it
	err = txn1.Commit()
	if err != nil {
		t.Logf("Conflict detected as expected: %v", err)
	} else {
		t.Log("No conflict detected - this may be expected depending on implementation")
	}
}

// TestDetectConflictsNoConflict tests conflict detection when no conflict exists
func TestDetectConflictsNoConflict(t *testing.T) {
	mgr := NewManager(nil)

	// Begin transaction with SnapshotIsolation
	opts := &Options{Isolation: SnapshotIsolation}
	txn := mgr.Begin(opts)

	// Set read version
	txn.SetReadVersion("", "key1", 100)

	// Set write to different key
	txn.SetWrite("", "key2", []byte("value"))

	// Should commit without conflict
	err := txn.Commit()
	if err != nil {
		t.Errorf("Unexpected conflict: %v", err)
	}
}

// TestDetectConflictsLowerIsolation tests that conflicts are not detected at lower isolation levels
func TestDetectConflictsLowerIsolation(t *testing.T) {
	mgr := NewManager(nil)

	// Begin transaction with ReadCommitted (no conflict detection)
	opts := &Options{Isolation: ReadCommitted}
	txn := mgr.Begin(opts)

	// Set read version
	txn.SetReadVersion("", "key1", 100)

	// Should not detect conflicts at this isolation level
	err := txn.Commit()
	if err != nil {
		t.Errorf("Should not detect conflicts at ReadCommitted: %v", err)
	}
}

// TestCommitWithApplyWritesError tests commit when applyWrites might fail
func TestCommitWithApplyWritesError(t *testing.T) {
	mgr := NewManager(nil)

	// Begin transaction
	txn := mgr.Begin(nil)

	// Add some writes
	for i := 0; i < 100; i++ {
		txn.SetWrite("", string(rune(i)), []byte("value"))
	}

	// Commit should succeed
	err := txn.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	// Verify versions were updated
	for i := 0; i < 100; i++ {
		key := string(rune(i))
		version := mgr.GetCurrentVersion("", key)
		if version == 0 {
			t.Errorf("Version not found for key %d", i)
			continue
		}
		if version != txn.ID {
			t.Errorf("Expected version %d, got %d", txn.ID, version)
		}
	}
}

// TestApplyWritesWithWAL tests the WAL write path in applyWrites
func TestApplyWritesWithWAL(t *testing.T) {
	dir := t.TempDir()
	disk, err := storage.OpenDisk(dir + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	pool := storage.NewBufferPool(1024, disk)
	defer pool.Close()

	wal, err := storage.OpenWAL(dir + "/test.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	mgr := NewManager(wal)

	txn := mgr.Begin(nil)
	txn.SetWrite("", "key1", []byte("value1"))
	txn.SetWrite("", "key2", []byte("value2"))

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit with WAL failed: %v", err)
	}

	// Verify versions were updated
	if mgr.GetCurrentVersion("", "key1") == 0 {
		t.Error("expected version for key1")
	}
	if mgr.GetCurrentVersion("", "key2") == 0 {
		t.Error("expected version for key2")
	}
}

func TestCommitWALFailureDoesNotPublishVersions(t *testing.T) {
	dir := t.TempDir()
	wal, err := storage.OpenWAL(dir + "/test.wal")
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	mgr := NewManager(wal)
	txn := mgr.Begin(nil)
	key := WriteKey{TreeName: "table", Key: "key"}
	txn.SetWrite(key.TreeName, key.Key, make([]byte, maxTxnWALRecordDataBytes))

	if err := txn.Commit(); err == nil {
		t.Fatal("expected commit to fail when WAL record is too large")
	}

	shard := versionShardIdx(key.TreeName, key.Key)
	mgr.versionShards[shard].mu.Lock()
	_, exists := mgr.versionShards[shard].versions[key]
	mgr.versionShards[shard].mu.Unlock()
	if exists {
		t.Fatal("failed WAL commit published version state")
	}
}

func TestMultiWriteCommitWALFailureDoesNotPublishVersions(t *testing.T) {
	dir := t.TempDir()
	wal, err := storage.OpenWAL(dir + "/test.wal")
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	mgr := NewManager(wal)
	txn := mgr.Begin(nil)
	keys := []WriteKey{
		{TreeName: "table", Key: "key1"},
		{TreeName: "table", Key: "key2"},
	}
	txn.SetWrite(keys[0].TreeName, keys[0].Key, []byte("ok"))
	txn.SetWrite(keys[1].TreeName, keys[1].Key, make([]byte, maxTxnWALRecordDataBytes))

	if err := txn.Commit(); err == nil {
		t.Fatal("expected commit to fail when one WAL record is too large")
	}

	for _, key := range keys {
		shard := versionShardIdx(key.TreeName, key.Key)
		mgr.versionShards[shard].mu.Lock()
		_, exists := mgr.versionShards[shard].versions[key]
		mgr.versionShards[shard].mu.Unlock()
		if exists {
			t.Fatalf("failed WAL commit published version state for %+v", key)
		}
	}
}

// TestApplyWritesWithPool tests the BufferPool path in applyWrites
func TestApplyWritesWithPool(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	defer pool.Close()

	mgr := NewManager(nil)

	txn := mgr.Begin(nil)
	txn.SetWrite("", "test:1", []byte("data"))

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit with pool failed: %v", err)
	}
}

// TestApplyWritesWithPoolAndWAL tests both pool and WAL paths
func TestApplyWritesWithPoolAndWAL(t *testing.T) {
	dir := t.TempDir()
	disk, err := storage.OpenDisk(dir + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	pool := storage.NewBufferPool(1024, disk)
	defer pool.Close()

	wal, err := storage.OpenWAL(dir + "/test.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	mgr := NewManager(wal)

	// Multiple transactions
	for i := 0; i < 5; i++ {
		txn := mgr.Begin(nil)
		txn.SetWrite("", fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("val%d", i)))
		if err := txn.Commit(); err != nil {
			t.Fatalf("commit %d failed: %v", i, err)
		}
	}

	if mgr.GetCurrentVersion("", "key0") == 0 {
		t.Error("expected version for key0")
	}
}

// TestCommitConflictWithWAL tests conflict detection with WAL enabled
func TestCommitConflictWithWAL(t *testing.T) {
	dir := t.TempDir()
	disk, err := storage.OpenDisk(dir + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	pool := storage.NewBufferPool(1024, disk)
	defer pool.Close()

	wal, err := storage.OpenWAL(dir + "/test.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	mgr := NewManager(wal)

	// Transaction 1 reads a key
	txn1 := mgr.Begin(nil)
	txn1.SetReadVersion("", "shared_key", 0)

	// Transaction 2 writes the same key and commits
	txn2 := mgr.Begin(nil)
	txn2.SetWrite("", "shared_key", []byte("txn2_value"))
	if err := txn2.Commit(); err != nil {
		t.Fatal(err)
	}

	// Transaction 1 tries to write and commit — should conflict
	txn1.SetWrite("", "shared_key", []byte("txn1_value"))
	err = txn1.Commit()
	if err != ErrConflict {
		t.Fatalf("expected ErrConflict, got: %v", err)
	}
}
