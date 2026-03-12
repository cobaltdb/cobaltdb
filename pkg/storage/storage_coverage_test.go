package storage

import (
	"os"
	"testing"
)

// ===========================
// Encryption tests
// ===========================

func TestNewEncryptedBackendDisabled(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{Enabled: false}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatalf("NewEncryptedBackend disabled: %v", err)
	}
	// Passthrough writes
	data := []byte("hello world12345") // 16 bytes
	n, err := eb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != len(data) {
		t.Errorf("WriteAt: expected %d, got %d", len(data), n)
	}
	// Passthrough reads
	buf := make([]byte, 16)
	n, err = eb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf[:n]) != "hello world12345" {
		t.Errorf("ReadAt: expected 'hello world12345', got %q", buf[:n])
	}
	// Size, Sync, Truncate, Close
	if eb.Size() != mem.Size() {
		t.Error("Size mismatch")
	}
	if err := eb.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
	if err := eb.Truncate(0); err != nil {
		t.Errorf("Truncate: %v", err)
	}
	if err := eb.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestNewEncryptedBackendEmptyKey(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{Enabled: true, Key: nil}
	_, err := NewEncryptedBackend(mem, cfg)
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey, got %v", err)
	}
}

func TestEncryptedBackendPBKDF2(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("my-secret-password-at-least-long"),
		Salt:        []byte("1234567890123456"),
		Algorithm:   "aes-256-gcm",
		UseArgon2:   false,
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatalf("NewEncryptedBackend: %v", err)
	}
	defer eb.Close()

	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	_, err = eb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	buf := make([]byte, PageSize)
	_, err = eb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	for i := 0; i < PageSize; i++ {
		if buf[i] != data[i] {
			t.Fatalf("Mismatch at byte %d: expected %d, got %d", i, data[i], buf[i])
		}
	}
}

func TestEncryptedBackendArgon2(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:   true,
		Key:       []byte("argon2-test-password-long-enough"),
		Salt:      []byte("0987654321098765"),
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatalf("NewEncryptedBackend: %v", err)
	}
	defer eb.Close()

	data := make([]byte, PageSize)
	data[0] = 0xAA
	data[PageSize-1] = 0xBB
	_, err = eb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	buf := make([]byte, PageSize)
	_, err = eb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if buf[0] != 0xAA || buf[PageSize-1] != 0xBB {
		t.Error("Decrypted data does not match")
	}
}

func TestEncryptedBackendAutoSalt(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled: true,
		Key:     []byte("auto-salt-test-password-32bytes!"),
		Salt:    nil,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatalf("NewEncryptedBackend: %v", err)
	}
	defer eb.Close()

	salt := eb.GetSalt()
	if len(salt) != 16 {
		t.Errorf("Expected 16-byte salt, got %d", len(salt))
	}
}

func TestEncryptedBackendSize(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("size-test-password-32-bytes-here"),
		Salt:        []byte("1234567890123456"),
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatalf("NewEncryptedBackend: %v", err)
	}
	defer eb.Close()

	if s := eb.Size(); s != 0 {
		t.Errorf("Expected size 0, got %d", s)
	}

	data := make([]byte, PageSize)
	eb.WriteAt(data, 0)

	s := eb.Size()
	t.Logf("Size after one page write: %d", s)
}

func TestEncryptedBackendTruncate(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("truncate-test-password-32-bytes!"),
		Salt:        []byte("1234567890123456"),
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatalf("NewEncryptedBackend: %v", err)
	}
	defer eb.Close()

	err = eb.Truncate(PageSize * 2)
	if err != nil {
		t.Errorf("Truncate: %v", err)
	}
}

func TestEncryptedBackendCloseClears(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("close-test-password-32-bytes-ok!"),
		Salt:        []byte("1234567890123456"),
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatalf("NewEncryptedBackend: %v", err)
	}

	if eb.sessionKey == nil {
		t.Fatal("Expected session key before close")
	}

	err = eb.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	if eb.sessionKey != nil {
		t.Error("Session key should be nil after close")
	}
}

// ===========================
// WAL AppendWithoutSync / Sync tests
// ===========================

func TestWALAppendWithoutSync(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_nosync.log"
	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	record := &WALRecord{
		TxnID:  1,
		Type:   WALInsert,
		PageID: 10,
		Offset: 0,
		Data:   []byte("test data"),
	}

	err = wal.AppendWithoutSync(record)
	if err != nil {
		t.Fatalf("AppendWithoutSync: %v", err)
	}

	if wal.LSN() == 0 {
		t.Error("LSN should be incremented")
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func TestWALSyncClosed(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_sync_closed.log"
	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	wal.Close()

	err = wal.Sync()
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

func TestWALAppendWithoutSyncMultiple(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_nosync_multi.log"
	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 10; i++ {
		record := &WALRecord{
			TxnID:  uint64(i + 1),
			Type:   WALInsert,
			PageID: uint32(i),
			Offset: 0,
			Data:   []byte("batch data"),
		}
		if err := wal.AppendWithoutSync(record); err != nil {
			t.Fatalf("AppendWithoutSync %d: %v", i, err)
		}
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if wal.LSN() != 10 {
		t.Errorf("Expected LSN=10, got %d", wal.LSN())
	}
}

func TestWALRecoverAfterAppendWithoutSync(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_recover_nosync.log"
	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	wal.AppendWithoutSync(&WALRecord{TxnID: 1, Type: WALInsert, PageID: 1, Data: []byte("data1")})
	wal.AppendWithoutSync(&WALRecord{TxnID: 2, Type: WALUpdate, PageID: 2, Data: []byte("data2")})
	wal.Sync()
	wal.Close()

	wal2, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("Reopen WAL: %v", err)
	}
	defer wal2.Close()

	mem := NewMemory()
	mem.WriteAt(make([]byte, PageSize*10), 0)
	pool := NewBufferPool(64, mem)
	defer pool.Close()

	err = wal2.Recover(pool)
	if err != nil {
		t.Logf("Recover: %v (may be expected for test data)", err)
	}
}

// ===========================
// BufferPoolStats HitCount/MissCount tests
// ===========================

func TestBufferPoolStatsHitCountCov(t *testing.T) {
	mem := NewMemory()
	pool := NewBufferPool(16, mem)
	defer pool.Close()

	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage: %v", err)
	}
	pool.Unpin(page)

	page2, err := pool.GetPage(page.ID())
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	pool.Unpin(page2)

	hc := pool.HitCount()
	if hc == 0 {
		t.Error("Expected non-zero HitCount after GetPage of cached page")
	}
}

func TestBufferPoolStatsMissCountCov(t *testing.T) {
	mem := NewMemory()
	pool := NewBufferPool(4, mem)
	defer pool.Close()

	pageData := make([]byte, PageSize)
	pageData[0] = 0x01
	mem.WriteAt(pageData, 0)

	_, err := pool.GetPage(0)
	if err != nil {
		t.Logf("GetPage(0): %v (may be expected)", err)
	}

	mc := pool.MissCount()
	t.Logf("MissCount: %d", mc)
}

// ===========================
// WAL large record rejection
// ===========================

func TestWALAppendLargeRecordRejected(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_large.log"
	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	record := &WALRecord{
		TxnID:  1,
		Type:   WALInsert,
		PageID: 1,
		Data:   make([]byte, 70000),
	}
	err = wal.Append(record)
	if err == nil {
		t.Error("Expected error for large record")
	}
}

// ===========================
// WAL Checkpoint tests
// ===========================

func TestWALCheckpointWritesLSN(t *testing.T) {
	tmpFile := t.TempDir() + "/wal_cp.log"
	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, PageID: 1, Data: []byte("d1")})
	wal.Append(&WALRecord{TxnID: 2, Type: WALInsert, PageID: 2, Data: []byte("d2")})

	_ = wal.LSN()

	mem := NewMemory()
	pool := NewBufferPool(64, mem)
	defer pool.Close()

	err = wal.Checkpoint(pool)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	cpLSN := wal.CheckpointLSN()
	if cpLSN == 0 {
		t.Error("Expected non-zero checkpoint LSN after Checkpoint")
	}
}

// ===========================
// Memory backend edge cases
// ===========================

func TestMemoryWriteAtNegativeOffset(t *testing.T) {
	mem := NewMemory()
	_, err := mem.WriteAt([]byte("test"), -1)
	if err == nil {
		t.Error("Expected error for negative offset")
	}
}

func TestMemoryReadAtNegativeOffset(t *testing.T) {
	mem := NewMemory()
	buf := make([]byte, 10)
	_, err := mem.ReadAt(buf, -1)
	if err == nil {
		t.Error("Expected error for negative offset")
	}
}

// ===========================
// Disk backend edge cases
// ===========================

func TestDiskSyncAndSize(t *testing.T) {
	tmpFile := t.TempDir() + "/disk_test.dat"
	disk, err := OpenDisk(tmpFile)
	if err != nil {
		t.Fatalf("OpenDisk: %v", err)
	}
	defer disk.Close()

	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}
	disk.WriteAt(data, 0)
	disk.Sync()

	if disk.Size() < 256 {
		t.Errorf("Expected size >= 256, got %d", disk.Size())
	}
}

func TestDiskTruncateToZero(t *testing.T) {
	tmpFile := t.TempDir() + "/disk_trunc.dat"
	disk, err := OpenDisk(tmpFile)
	if err != nil {
		t.Fatalf("OpenDisk: %v", err)
	}
	defer disk.Close()

	disk.WriteAt(make([]byte, 1024), 0)
	err = disk.Truncate(0)
	if err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if disk.Size() != 0 {
		t.Errorf("Expected size 0 after truncate, got %d", disk.Size())
	}
}

func TestDiskOpenNonexistentDir(t *testing.T) {
	_, err := OpenDisk(t.TempDir() + "/a/b/c/d/file.dat")
	if err == nil {
		t.Error("Expected error for nonexistent directory")
	}
}

// ===========================
// Buffer pool Close edge case
// ===========================

func TestBufferPoolCloseFlushes(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/bp_close.dat"
	disk, err := OpenDisk(tmpFile)
	if err != nil {
		t.Fatalf("OpenDisk: %v", err)
	}

	pool := NewBufferPool(16, disk)
	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage: %v", err)
	}
	page.SetDirty(true)
	pool.Unpin(page)

	// Close pool (flushes dirty pages) and then close disk to release file handle
	err = pool.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	disk.Close()

	fi, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() == 0 {
		t.Error("Expected non-zero file after close with dirty pages")
	}
}
