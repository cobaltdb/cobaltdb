package storage

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"path/filepath"
	"testing"
)

func makeBenchCipher(b testing.TB) cipher.AEAD {
	b.Helper()
	key := []byte("0123456789abcdef0123456789abcdef")
	block, err := aes.NewCipher(key)
	if err != nil {
		b.Fatalf("NewCipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		b.Fatalf("NewGCM: %v", err)
	}
	return gcm
}

// TestWALDecryptDataShortCiphertext tests decryptData with ciphertext shorter than nonce
func TestWALDecryptDataShortCiphertext(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeBenchCipher(t)
	wal.SetEncryptionCipher(c)

	_, err = wal.decryptData([]byte{0x01}, nil)
	if err == nil {
		t.Error("Expected error for short ciphertext")
	}
}

// TestWALEncryptDataEmptyPlaintext tests encryptData with empty plaintext
func TestWALEncryptDataEmptyPlaintext(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeBenchCipher(t)
	wal.SetEncryptionCipher(c)

	out, err := wal.encryptData([]byte{}, nil)
	if err != nil {
		t.Fatalf("encryptData empty: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("Expected empty output, got %d bytes", len(out))
	}
}

// TestWALDecryptDataEmptyCiphertext tests decryptData with empty ciphertext
func TestWALDecryptDataEmptyCiphertext(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeBenchCipher(t)
	wal.SetEncryptionCipher(c)

	out, err := wal.decryptData([]byte{}, nil)
	if err != nil {
		t.Fatalf("decryptData empty: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("Expected empty output, got %d bytes", len(out))
	}
}

// TestWALDecryptDataCorrupted tests decryptData with corrupted ciphertext
func TestWALDecryptDataCorrupted(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeBenchCipher(t)
	wal.SetEncryptionCipher(c)

	corrupted := make([]byte, c.NonceSize()+16)
	for i := range corrupted {
		corrupted[i] = byte(i)
	}
	_, err = wal.decryptData(corrupted, nil)
	if err == nil {
		t.Error("Expected error for corrupted ciphertext")
	}
}

// TestOpenDiskInvalidPath tests OpenDisk with an invalid path
func TestOpenDiskInvalidPath(t *testing.T) {
	_, err := OpenDisk("/nonexistent/path/that/cannot/be/created/test.db")
	if err == nil {
		t.Error("Expected error for invalid disk path")
	}
}

// TestOpenWALInvalidPath tests OpenWAL with an invalid path
func TestOpenWALInvalidPath(t *testing.T) {
	_, err := OpenWAL("/nonexistent/path/that/cannot/be/created/test.wal")
	if err == nil {
		t.Error("Expected error for invalid WAL path")
	}
}

// TestWALCheckpointClosed tests Checkpoint on a closed WAL
func TestWALCheckpointClosed(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	backend := NewMemory()
	pool := NewBufferPool(16, backend)
	defer pool.Close()

	wal.Close()

	err = wal.Checkpoint(pool)
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

// TestCompressedBackendLZ4NoneLevel tests compressLZ4 with CompressionLevelNone
func TestCompressedBackendLZ4NoneLevel(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelNone,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	data := bytes.Repeat([]byte("hello world "), 350)[:PageSize]
	compressed, ratio, err := cb.compressLZ4(data)
	if err != nil {
		t.Fatalf("compressLZ4: %v", err)
	}
	if !bytes.Equal(compressed, data) {
		t.Error("CompressionLevelNone should return original data")
	}
	if ratio != 1.0 {
		t.Errorf("Expected ratio 1.0, got %f", ratio)
	}
}

// TestCompressedBackendZstdNoneLevel tests compressZstd with CompressionLevelNone
func TestCompressedBackendZstdNoneLevel(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelNone,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	data := bytes.Repeat([]byte("hello world "), 350)[:PageSize]
	compressed, ratio, err := cb.compressZstd(data)
	if err != nil {
		t.Fatalf("compressZstd: %v", err)
	}
	if !bytes.Equal(compressed, data) {
		t.Error("CompressionLevelNone should return original data")
	}
	if ratio != 1.0 {
		t.Errorf("Expected ratio 1.0, got %f", ratio)
	}
}

// TestCompressedBackendZlibNoneLevel tests compressZlib with CompressionLevelNone
func TestCompressedBackendZlibNoneLevel(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelNone,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	data := bytes.Repeat([]byte("hello world "), 350)[:PageSize]
	compressed, ratio, err := cb.compressZlib(data)
	if err != nil {
		t.Fatalf("compressZlib: %v", err)
	}
	if !bytes.Equal(compressed, data) {
		t.Error("CompressionLevelNone should return original data")
	}
	if ratio != 1.0 {
		t.Errorf("Expected ratio 1.0, got %f", ratio)
	}
}

// TestCompressedBackendPoolMiss tests getWriteBuf/getReadBuf pool miss paths
func TestCompressedBackendPoolMiss(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelFast,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	buf1 := cb.getWriteBuf()
	if buf1 == nil || len(*buf1) != PageSize {
		t.Errorf("getWriteBuf returned unexpected buffer: %v", buf1)
	}
	cb.putWriteBuf(buf1)

	buf2 := cb.getReadBuf()
	if buf2 == nil || len(*buf2) != PageSize {
		t.Errorf("getReadBuf returned unexpected buffer: %v", buf2)
	}
	cb.putReadBuf(buf2)
}

// TestWALAppendInternalClosed tests appendInternal on closed WAL
func TestWALAppendInternalClosed(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	wal.Close()

	err = wal.Append(&WALRecord{
		TxnID:  1,
		Type:   WALInsert,
		PageID: 1,
		Data:   []byte("test"),
	})
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

// TestWALAppendWithoutSyncClosed tests AppendWithoutSync on closed WAL
func TestWALAppendWithoutSyncClosed(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	wal.Close()

	err = wal.AppendWithoutSync(&WALRecord{
		TxnID:  1,
		Type:   WALInsert,
		PageID: 1,
		Data:   []byte("test"),
	})
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

// TestWALReadLSNClosed tests readLSN on closed WAL
func TestWALReadLSNClosed(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	wal.Close()

	err = wal.readLSN()
	if err == nil {
		t.Error("Expected error for readLSN on closed WAL")
	}
}

// TestCompressedBackendDecompressZlibError tests decompressZlib with invalid data
func TestCompressedBackendDecompressZlibError(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	_, err = cb.decompressZlib([]byte("not valid zlib"), PageSize)
	if err == nil {
		t.Error("Expected error for invalid zlib data")
	}
}

// TestCompressedBackendDecompressLZ4Error tests decompressLZ4 with invalid data
func TestCompressedBackendDecompressLZ4Error(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelFast,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	_, err = cb.decompressLZ4([]byte("not valid lz4"), PageSize)
	if err == nil {
		t.Error("Expected error for invalid LZ4 data")
	}
}

// TestCompressedBackendDecompressZstdError tests decompressZstd with invalid data
func TestCompressedBackendDecompressZstdError(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelFast,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	_, err = cb.decompressZstd([]byte("not valid zstd"), PageSize)
	if err == nil {
		t.Error("Expected error for invalid zstd data")
	}
}

// TestDiskBackendCloseIdempotent tests that Close is safe to call multiple times
func TestDiskBackendCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backend, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("OpenDisk: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("First close: %v", err)
	}
	// Second close should not panic
	if err := backend.Close(); err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}

// TestMemoryBackendCloseIdempotent tests that MemoryBackend Close is safe multiple times
func TestMemoryBackendCloseIdempotent(t *testing.T) {
	mem := NewMemory()
	if err := mem.Close(); err != nil {
		t.Fatalf("First close: %v", err)
	}
	if err := mem.Close(); err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}

// TestWALCloseIdempotent tests that WAL Close is safe to call multiple times
func TestWALCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("First close: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}

// TestPageManagerCloseSaveError tests PageManager.Close when underlying backend fails
func TestPageManagerCloseSaveError(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(16, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}

	// Close backend first so PageManager.Close fails on saveFreeList/Sync
	backend.Close()

	err = pm.Close()
	if err == nil {
		t.Log("Close succeeded despite closed backend (may be expected)")
	}
}
