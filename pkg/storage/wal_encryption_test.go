package storage

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

var errFailingWALWriter = errors.New("failing WAL writer")

type failingWALWriter struct{}

func (failingWALWriter) Write([]byte) (int, error) {
	return 0, errFailingWALWriter
}

func makeTestCipher(t *testing.T) cipher.AEAD {
	t.Helper()
	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes = AES-256
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("NewGCM: %v", err)
	}
	return gcm
}

func TestWALEncryptionRoundtrip(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	// Create WAL with encryption
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)

	// Write encrypted records
	for i := 0; i < 5; i++ {
		err := wal.Append(&WALRecord{
			TxnID:  uint64(i + 1),
			Type:   WALInsert,
			PageID: uint32(i),
			Data:   []byte("sensitive transaction data"),
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	wal.Close()

	// Verify the WAL file does NOT contain plaintext
	data, _ := os.ReadFile(walPath)
	if containsBytes(data, []byte("sensitive transaction data")) {
		t.Error("WAL file contains plaintext data - encryption not working")
	}

	// Reopen and verify records can be decrypted
	wal2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Reopen WAL: %v", err)
	}
	wal2.SetEncryptionCipher(c)

	if wal2.LSN() != 5 {
		t.Errorf("Expected LSN 5, got %d", wal2.LSN())
	}
	wal2.Close()
}

func TestWALWithoutEncryption(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "plain.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	// No cipher set - writes should be plaintext
	err = wal.Append(&WALRecord{
		TxnID:  1,
		Type:   WALInsert,
		PageID: 0,
		Data:   []byte("plaintext data here"),
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	wal.Close()

	// Plaintext should be readable in file
	data, _ := os.ReadFile(walPath)
	if !containsBytes(data, []byte("plaintext data here")) {
		t.Error("Unencrypted WAL should contain plaintext data")
	}
}

func TestWALEncryptionEmptyData(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "empty.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)

	// Write record with no data (e.g., commit record)
	err = wal.Append(&WALRecord{
		TxnID: 1,
		Type:  WALCommit,
	})
	if err != nil {
		t.Fatalf("Append commit: %v", err)
	}

	wal.Close()
}

func TestWALEncryptionRejectsCiphertextPayloadOverflow(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "overflow.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)

	record := &WALRecord{
		TxnID: 1,
		Type:  WALInsert,
		Data:  make([]byte, walMaxRecordDataSize),
	}
	err = wal.Append(record)
	if err == nil || !errors.Is(err, ErrInvalidWALRecord) && !containsBytes([]byte(err.Error()), []byte("encrypted WAL record data size")) {
		t.Fatalf("expected encrypted WAL size error, got %v", err)
	}
	if record.LSN != 0 {
		t.Fatalf("rejected record LSN was mutated: %d", record.LSN)
	}
	if wal.LSN() != 0 {
		t.Fatalf("rejected append advanced WAL LSN: %d", wal.LSN())
	}
}

func TestWALEncryptionAllowsMaxFittingCiphertextPayload(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "max-fitting.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)
	maxPlaintext := walMaxRecordDataSize - c.NonceSize() - c.Overhead()
	if err := wal.Append(&WALRecord{
		TxnID: 1,
		Type:  WALInsert,
		Data:  make([]byte, maxPlaintext),
	}); err != nil {
		t.Fatalf("Append max fitting encrypted record: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Reopen WAL: %v", err)
	}
	reopened.SetEncryptionCipher(c)
	defer reopened.Close()
	if reopened.LSN() != 1 {
		t.Fatalf("expected reopened LSN 1, got %d", reopened.LSN())
	}
}

func TestWALEncryptionBatchRejectsCiphertextPayloadOverflow(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "batch-overflow.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	wal.SetEncryptionCipher(makeTestCipher(t))
	err = wal.AppendBatch([]*WALRecord{{
		TxnID: 1,
		Type:  WALInsert,
		Data:  make([]byte, walMaxRecordDataSize),
	}})
	if err == nil || !containsBytes([]byte(err.Error()), []byte("encrypted WAL record data size")) {
		t.Fatalf("expected encrypted batch WAL size error, got %v", err)
	}
	if wal.LSN() != 0 {
		t.Fatalf("rejected batch advanced WAL LSN: %d", wal.LSN())
	}
}

func TestWALAppendBatchWithoutSyncEncryptedOverflowIsAtomic(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "batch-without-sync-overflow.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)

	err = wal.AppendBatchWithoutSync([]*WALRecord{
		{TxnID: 1, Type: WALInsert, Data: []byte("valid")},
		{TxnID: 1, Type: WALInsert, Data: make([]byte, walMaxRecordDataSize)},
	})
	if err == nil || !containsBytes([]byte(err.Error()), []byte("encrypted WAL record data size")) {
		t.Fatalf("expected encrypted batch WAL size error, got %v", err)
	}
	if wal.LSN() != 0 {
		t.Fatalf("rejected batch advanced WAL LSN: %d", wal.LSN())
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Reopen WAL: %v", err)
	}
	reopened.SetEncryptionCipher(c)
	defer reopened.Close()
	if reopened.LSN() != 0 {
		t.Fatalf("rejected batch persisted partial record, reopened LSN %d", reopened.LSN())
	}
}

func TestWALAppendBatchConcurrentCipherConfiguration(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "batch-cipher-concurrent.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				wal.SetEncryptionCipher(c)
			}
		}
	}()

	for i := 0; i < 20; i++ {
		if err := wal.AppendBatch([]*WALRecord{{
			TxnID: uint64(i + 1),
			Type:  WALInsert,
			Data:  []byte("encrypted batch payload"),
		}}); err != nil {
			close(stop)
			<-done
			t.Fatalf("AppendBatch %d: %v", i, err)
		}
	}
	close(stop)
	<-done

	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Reopen WAL: %v", err)
	}
	reopened.SetEncryptionCipher(c)
	defer reopened.Close()
	if reopened.LSN() != 20 {
		t.Fatalf("expected reopened LSN 20, got %d", reopened.LSN())
	}
}

func TestWALEncryptedAppendRestoresDataOnWriteError(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "restore-on-error.wal")
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()

	original := []byte("sensitive payload")
	record := &WALRecord{
		Type: WALInsert,
		Data: append([]byte(nil), original...),
	}
	wal := &WAL{
		file:      file,
		bufWriter: bufio.NewWriterSize(failingWALWriter{}, 1),
		cipher:    makeTestCipher(t),
	}

	err = wal.appendInternal(record, false)
	if !errors.Is(err, errFailingWALWriter) {
		t.Fatalf("expected writer error, got %v", err)
	}
	if string(record.Data) != string(original) {
		t.Fatalf("record data was not restored: got %q, want %q", record.Data, original)
	}
	if record.LSN != 0 {
		t.Fatalf("failed append assigned record LSN: got %d, want 0", record.LSN)
	}
	if wal.LSN() != 0 {
		t.Fatalf("failed append advanced WAL LSN: got %d, want 0", wal.LSN())
	}
}

func containsBytes(haystack, needle []byte) bool {
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// TestWALDecryptDataShortCiphertext covers decryptData when the input
// is shorter than the nonce. Ported from coverage_boost_storage_test.go
// — the padding file targeted encryptData/decryptData edge cases that
// no untagged test exercised.
func TestWALDecryptDataShortCiphertext(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)

	_, err = wal.decryptData([]byte{0x01}, nil)
	if err == nil {
		t.Error("Expected error for short ciphertext")
	}
}

// TestWALEncryptDataEmptyPlaintext covers encryptData with empty input.
// Ported from coverage_boost_storage_test.go.
func TestWALEncryptDataEmptyPlaintext(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)

	out, err := wal.encryptData([]byte{}, nil)
	if err != nil {
		t.Fatalf("encryptData empty: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("Expected empty output, got %d bytes", len(out))
	}
}

// TestWALDecryptDataEmptyCiphertext covers decryptData with empty input.
// Ported from coverage_boost_storage_test.go.
func TestWALDecryptDataEmptyCiphertext(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeTestCipher(t)
	wal.SetEncryptionCipher(c)

	out, err := wal.decryptData([]byte{}, nil)
	if err != nil {
		t.Fatalf("decryptData empty: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("Expected empty output, got %d bytes", len(out))
	}
}

// TestWALDecryptDataCorrupted covers decryptData with non-empty but
// undecryptable ciphertext. The padding file used a pattern of
// incremental bytes; here we use a fixed-size buffer filled with
// bytes that will fail the AEAD authentication check.
// Ported from coverage_boost_storage_test.go.
func TestWALDecryptDataCorrupted(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	c := makeTestCipher(t)
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
