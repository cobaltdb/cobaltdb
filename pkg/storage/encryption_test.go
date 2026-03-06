package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestNewEncryptedBackend(t *testing.T) {
	// Create a temporary file backend
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}
	if eb == nil {
		t.Fatal("Encrypted backend is nil")
	}
	if eb.cipher == nil {
		t.Fatal("Cipher not initialized")
	}
}

func TestNewEncryptedBackendDisabled(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	config := &EncryptionConfig{
		Enabled: false,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}
	if eb == nil {
		t.Fatal("Encrypted backend is nil")
	}
	// Cipher should be nil when disabled
	if eb.cipher != nil {
		t.Error("Cipher should be nil when encryption is disabled")
	}
}

func TestNewEncryptedBackendInvalidKey(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	// Empty key
	config := &EncryptionConfig{
		Enabled: true,
		Key:     []byte{},
	}

	_, err = NewEncryptedBackend(backend, config)
	if err == nil {
		t.Error("Expected error for empty key")
	}
}

func TestDeriveKeyFromPassword(t *testing.T) {
	password := "test_password"

	// Generate salt
	salt := make([]byte, 16)
	for i := range salt {
		salt[i] = byte(i)
	}

	key := DeriveKeyFromPassword(password, salt)
	if len(key) != 32 {
		t.Errorf("Expected 32-byte key, got %d bytes", len(key))
	}

	// Same password and salt should produce same key
	key2 := DeriveKeyFromPassword(password, salt)
	if !bytes.Equal(key, key2) {
		t.Error("Same password and salt should produce same key")
	}

	// Different salt should produce different key
	differentSalt := make([]byte, 16)
	for i := range differentSalt {
		differentSalt[i] = byte(i + 1)
	}
	key3 := DeriveKeyFromPassword(password, differentSalt)
	if bytes.Equal(key, key3) {
		t.Error("Different salt should produce different key")
	}
}

func TestGenerateSecureKey(t *testing.T) {
	key1, err := GenerateSecureKey()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	if len(key1) != 32 {
		t.Errorf("Expected 32-byte key, got %d bytes", len(key1))
	}

	key2, err := GenerateSecureKey()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	// Two keys should be different (with very high probability)
	if bytes.Equal(key1, key2) {
		t.Error("Two generated keys should be different")
	}
}

func TestEncryptedReadWrite(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}

	// Write data (page size)
	plaintext := make([]byte, PageSize)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	n, err := eb.WriteAt(plaintext, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	if n == 0 {
		t.Error("Write returned 0 bytes")
	}

	// Sync to ensure data is written
	if err := eb.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Read data back
	readBuf := make([]byte, PageSize)
	n, err = eb.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	// Decrypted should match original
	if !bytes.Equal(readBuf, plaintext) {
		t.Error("Decrypted data doesn't match original")
	}
}

func TestEncryptedWriteReadAtOffset(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: false, // Test PBKDF2
		PBKDF2Iters: 10000,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}

	// Write multiple pages at offsets that account for encryption overhead
	// Each encrypted page is PageSize + nonce + auth tag
	encryptedPageSize := int64(PageSize + eb.cipher.NonceSize() + eb.cipher.Overhead())

	for pageNum := int64(0); pageNum < 3; pageNum++ {
		plaintext := make([]byte, PageSize)
		for i := range plaintext {
			plaintext[i] = byte((int(pageNum)*1000 + i) % 256)
		}

		offset := pageNum * encryptedPageSize
		_, err := eb.WriteAt(plaintext, offset)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", pageNum, err)
		}
	}

	// Sync to ensure data is written
	if err := eb.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Read back each page
	for pageNum := int64(0); pageNum < 3; pageNum++ {
		readBuf := make([]byte, PageSize)
		offset := pageNum * encryptedPageSize
		_, err := eb.ReadAt(readBuf, offset)
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", pageNum, err)
		}

		// Verify content
		expected := byte(int(pageNum*1000) % 256)
		if readBuf[0] != expected {
			t.Errorf("Page %d: expected first byte %d, got %d", pageNum, expected, readBuf[0])
		}
	}
}

func TestEncryptionHeader(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Create header
	header := &EncryptionHeader{
		Version:       1,
		Algorithm:     1,
		KeyDerivation: 1,
	}
	copy(header.Magic[:], "COBALTEN")
	for i := range header.Salt {
		header.Salt[i] = byte(i)
	}
	header.Iterations = 100000

	// Write header
	err = WriteHeader(backend, header)
	if err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	// Sync and close
	backend.Sync()
	backend.Close()

	// Reopen and read header
	backend, err = OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to reopen backend: %v", err)
	}
	defer backend.Close()

	readHeader, err := ReadHeader(backend)
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	if readHeader.Version != 1 {
		t.Errorf("Expected version 1, got %d", readHeader.Version)
	}

	if readHeader.Algorithm != 1 {
		t.Errorf("Expected algorithm 1, got %d", readHeader.Algorithm)
	}

	if readHeader.Iterations != 100000 {
		t.Errorf("Expected 100000 iterations, got %d", readHeader.Iterations)
	}

	if string(readHeader.Magic[:]) != "COBALTEN" {
		t.Errorf("Expected magic 'COBALTEN', got '%s'", string(readHeader.Magic[:]))
	}
}

func TestIsEncrypted(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Not encrypted initially
	if IsEncrypted(backend) {
		t.Error("New file should not be encrypted")
	}

	// Write header
	header := &EncryptionHeader{
		Version:       1,
		Algorithm:     1,
		KeyDerivation: 1,
	}
	copy(header.Magic[:], "COBALTEN")

	err = WriteHeader(backend, header)
	if err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	backend.Sync()
	backend.Close()

	// Reopen and check
	backend, err = OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to reopen backend: %v", err)
	}
	defer backend.Close()

	if !IsEncrypted(backend) {
		t.Error("File should be encrypted after writing header")
	}
}

func TestEncryptedBackendSize(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	key := make([]byte, 32)
	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}

	// Write one page
	plaintext := make([]byte, PageSize)
	eb.WriteAt(plaintext, 0)
	eb.Sync()

	// Check size
	size := eb.Size()
	if size != int64(PageSize) {
		t.Errorf("Expected size %d, got %d", PageSize, size)
	}

	// Backend size should be larger (encrypted)
	backendSize := backend.Size()
	if backendSize <= int64(PageSize) {
		t.Error("Backend size should be larger than plaintext due to encryption overhead")
	}
}

func TestEncryptedBackendTruncate(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	key := make([]byte, 32)
	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}

	// Truncate to 2 pages
	err = eb.Truncate(int64(PageSize) * 2)
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	// Backend should be larger due to encryption overhead
	if backend.Size() <= int64(PageSize)*2 {
		t.Error("Truncated backend should account for encryption overhead")
	}
}

func TestEncryptedBackendClose(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	key := make([]byte, 32)
	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}

	// Close should clear session key
	err = eb.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}
}

func TestDifferentKeys(t *testing.T) {
	tempDir := t.TempDir()

	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	for i := range key1 {
		key1[i] = byte(i)
		key2[i] = byte(i + 1)
	}

	// Create encrypted file with key1
	backendPath1 := filepath.Join(tempDir, "test1.db")
	backend1, _ := OpenDisk(backendPath1)
	config1 := &EncryptionConfig{
		Enabled:   true,
		Key:       key1,
		Salt:      []byte("salt123456789012"),
		Algorithm: "aes-256-gcm",
		UseArgon2: false,
		PBKDF2Iters: 1000,
	}
	eb1, _ := NewEncryptedBackend(backend1, config1)

	plaintext := []byte("Secret message that needs to be encrypted properly")
	paddedPlaintext := make([]byte, PageSize)
	copy(paddedPlaintext, plaintext)

	eb1.WriteAt(paddedPlaintext, 0)
	eb1.Sync()
	eb1.Close()

	// Try to read with key2 (should fail to decrypt correctly)
	backend2, _ := OpenDisk(backendPath1)
	config2 := &EncryptionConfig{
		Enabled:   true,
		Key:       key2,
		Salt:      []byte("salt123456789012"),
		Algorithm: "aes-256-gcm",
		UseArgon2: false,
		PBKDF2Iters: 1000,
	}
	eb2, _ := NewEncryptedBackend(backend2, config2)

	readBuf := make([]byte, PageSize)
	_, err := eb2.ReadAt(readBuf, 0)
	// Decryption should fail with wrong key
	if err == nil {
		t.Error("Expected error when decrypting with wrong key")
	}
	eb2.Close()
}

func TestReadHeaderInvalidFile(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	// Write some non-header data
	backend.WriteAt([]byte("not a valid header"), 0)

	// Try to read header
	_, err = ReadHeader(backend)
	if err == nil {
		t.Error("Expected error when reading invalid header")
	}
}

func TestEncryptedPageSize(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	key := make([]byte, 32)
	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}

	encPageSize := EncryptedPageSize(eb.cipher)
	expectedSize := PageSize + eb.cipher.NonceSize() + eb.cipher.Overhead()

	if encPageSize != expectedSize {
		t.Errorf("Expected encrypted page size %d, got %d", expectedSize, encPageSize)
	}

	if encPageSize <= PageSize {
		t.Error("Encrypted page size should be larger than plaintext page size")
	}
}

func TestGetSalt(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	salt := make([]byte, 16)
	for i := range salt {
		salt[i] = byte(i * 2)
	}

	key := make([]byte, 32)
	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Salt:      salt,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}

	retrievedSalt := eb.GetSalt()
	if !bytes.Equal(retrievedSalt, salt) {
		t.Error("Retrieved salt doesn't match original")
	}
}

func TestEncryptedBackendPassThroughWhenDisabled(t *testing.T) {
	tempDir := t.TempDir()
	backendPath := filepath.Join(tempDir, "test.db")

	backend, err := OpenDisk(backendPath)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	config := &EncryptionConfig{
		Enabled: false,
	}

	eb, err := NewEncryptedBackend(backend, config)
	if err != nil {
		t.Fatalf("Failed to create encrypted backend: %v", err)
	}

	// Write data
	data := []byte("Hello, World!")
	_, err = eb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Read back
	readBuf := make([]byte, len(data))
	_, err = eb.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if !bytes.Equal(readBuf, data) {
		t.Error("Data should pass through unchanged when encryption is disabled")
	}
}

func BenchmarkEncryptedWrite(b *testing.B) {
	tempDir := b.TempDir()
	backendPath := filepath.Join(tempDir, "bench.db")

	backend, _ := OpenDisk(backendPath)
	defer backend.Close()

	key := make([]byte, 32)
	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, _ := NewEncryptedBackend(backend, config)
	data := make([]byte, PageSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.WriteAt(data, 0)
	}
	b.StopTimer()
}

func BenchmarkEncryptedRead(b *testing.B) {
	tempDir := b.TempDir()
	backendPath := filepath.Join(tempDir, "bench.db")

	backend, _ := OpenDisk(backendPath)
	defer backend.Close()

	key := make([]byte, 32)
	config := &EncryptionConfig{
		Enabled:   true,
		Key:       key,
		Algorithm: "aes-256-gcm",
		UseArgon2: true,
	}

	eb, _ := NewEncryptedBackend(backend, config)
	data := make([]byte, PageSize)
	eb.WriteAt(data, 0)

	readBuf := make([]byte, PageSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.ReadAt(readBuf, 0)
	}
	b.StopTimer()
}
