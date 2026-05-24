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
