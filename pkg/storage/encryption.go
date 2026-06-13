package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/pbkdf2"
)

var (
	ErrInvalidKey       = errors.New("invalid encryption key")
	ErrInvalidSalt      = errors.New("invalid salt")
	ErrInvalidAlgorithm = errors.New("invalid encryption algorithm")
	ErrEncryptionFailed = errors.New("encryption failed")
	ErrDecryptionFailed = errors.New("decryption failed")
	ErrKeyDerivation    = errors.New("key derivation failed")
)

const (
	defaultPBKDF2Iters = 100000
	maxPBKDF2Iters     = 10_000_000
)

// EncryptionConfig holds encryption configuration
type EncryptionConfig struct {
	Enabled     bool   // Whether encryption is enabled
	Key         []byte // Raw encryption key (32 bytes for AES-256)
	Salt        []byte // Salt for key derivation
	Algorithm   string // "aes-256-gcm" (default)
	UseArgon2   bool   // Use Argon2id for key derivation (recommended)
	PBKDF2Iters int    // PBKDF2 iterations (default: 100000)
}

// EncryptedBackend wraps a Backend with transparent encryption/decryption
type EncryptedBackend struct {
	backend    Backend
	config     *EncryptionConfig
	cipher     cipher.AEAD
	sessionKey []byte
	mu         sync.RWMutex
	readPool   sync.Pool // pool for encrypted read buffers
	closed     bool
}

// NewEncryptedBackend creates a new encrypted backend wrapper
func NewEncryptedBackend(backend Backend, config *EncryptionConfig) (*EncryptedBackend, error) {
	if config == nil {
		return nil, errors.New("encryption config is nil")
	}
	config = cloneEncryptionConfig(config)

	if !config.Enabled {
		return &EncryptedBackend{backend: backend, config: config}, nil
	}

	if err := validateEncryptionConfig(config); err != nil {
		return nil, err
	}

	eb := &EncryptedBackend{
		backend: backend,
		config:  config,
	}

	// Derive encryption key
	if err := eb.deriveKey(); err != nil {
		return nil, err
	}

	// Initialize cipher
	block, err := aes.NewCipher(eb.sessionKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEncryptionFailed, err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEncryptionFailed, err)
	}

	eb.cipher = aead
	return eb, nil
}

func validateEncryptionConfig(config *EncryptionConfig) error {
	if len(config.Key) == 0 {
		return ErrInvalidKey
	}
	if config.Algorithm != "" && config.Algorithm != "aes-256-gcm" {
		return fmt.Errorf("%w: %s", ErrInvalidAlgorithm, config.Algorithm)
	}
	if !config.UseArgon2 {
		if config.PBKDF2Iters < 0 {
			return fmt.Errorf("%w: PBKDF2 iterations cannot be negative: %d", ErrKeyDerivation, config.PBKDF2Iters)
		}
		if config.PBKDF2Iters > maxPBKDF2Iters {
			return fmt.Errorf("%w: PBKDF2 iterations exceeds maximum (%d): %d", ErrKeyDerivation, maxPBKDF2Iters, config.PBKDF2Iters)
		}
	}
	return nil
}

func cloneEncryptionConfig(config *EncryptionConfig) *EncryptionConfig {
	normalized := *config
	if len(config.Key) > 0 {
		normalized.Key = append([]byte(nil), config.Key...)
	}
	if len(config.Salt) > 0 {
		normalized.Salt = append([]byte(nil), config.Salt...)
	}
	return &normalized
}

// deriveKey derives a 32-byte key from the provided key using PBKDF2 or Argon2
func (eb *EncryptedBackend) deriveKey() error {
	key := eb.config.Key
	salt := eb.config.Salt

	// Generate salt if not provided
	if len(salt) == 0 {
		salt = make([]byte, 16)
		if _, err := io.ReadFull(rand.Reader, salt); err != nil {
			return fmt.Errorf("%w: %w", ErrKeyDerivation, err)
		}
		eb.config.Salt = salt
	}
	if len(salt) > maxEncryptionSaltBytes {
		return fmt.Errorf("%w: salt is too large: %d bytes (max %d)", ErrInvalidSalt, len(salt), maxEncryptionSaltBytes)
	}

	// Derive 32-byte key for AES-256
	if eb.config.UseArgon2 {
		// Argon2id: memory-hard, resistant to GPU attacks
		eb.sessionKey = argon2.IDKey(key, salt, 3, 64*1024, 4, 32)
	} else {
		// PBKDF2 with SHA-256
		iters := eb.config.PBKDF2Iters
		if iters == 0 {
			iters = defaultPBKDF2Iters
		}
		eb.sessionKey = pbkdf2.Key(key, salt, iters, 32, sha256.New)
	}

	return nil
}

// ReadAt reads and decrypts data from the backend
// physicalOffset maps a page-aligned logical offset to its physical offset in
// the encrypted backend. Each logical PageSize block is stored as a larger
// encrypted block (nonce + ciphertext + tag), so physical offsets must be
// scaled by that block size — otherwise consecutive encrypted pages overlap and
// corrupt each other on disk (the meta page's tail gets overwritten by page 1),
// making the database unreadable on reopen. Size()/Truncate() already assume
// this scaled layout.
func (eb *EncryptedBackend) physicalOffset(offset int64) int64 {
	encryptedSize := int64(PageSize + eb.cipher.NonceSize() + eb.cipher.Overhead())
	return (offset / int64(PageSize)) * encryptedSize
}

func validateEncryptedPageIO(bufLen int, offset int64) error {
	if offset < 0 || offset%int64(PageSize) != 0 {
		return fmt.Errorf("%w: encrypted backend offset must be page-aligned: %d", ErrInvalidOffset, offset)
	}
	if bufLen != PageSize {
		return fmt.Errorf("%w: encrypted backend requires full page I/O: got %d want %d", ErrInvalidSize, bufLen, PageSize)
	}
	return nil
}

func (eb *EncryptedBackend) ReadAt(buf []byte, offset int64) (int, error) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if eb.closed {
		return 0, ErrBackendClosed
	}

	if !eb.config.Enabled {
		return eb.backend.ReadAt(buf, offset)
	}

	if err := validateEncryptedPageIO(len(buf), offset); err != nil {
		return 0, err
	}

	// Read encrypted data (page size + nonce + tag)
	pageSize := PageSize
	encryptedSize := pageSize + eb.cipher.NonceSize() + eb.cipher.Overhead()

	// Use pooled buffer to reduce allocations on hot read path
	var encryptedBuf []byte
	if poolBuf := eb.readPool.Get(); poolBuf != nil {
		switch b := poolBuf.(type) {
		case *[]byte:
			encryptedBuf = *b
		case []byte:
			encryptedBuf = b
		}
	}
	if len(encryptedBuf) < encryptedSize {
		encryptedBuf = make([]byte, encryptedSize)
	}
	defer func() {
		pooled := encryptedBuf
		eb.readPool.Put(&pooled)
	}()

	n, err := eb.backend.ReadAt(encryptedBuf, eb.physicalOffset(offset))
	if err != nil {
		return 0, err
	}

	if n < eb.cipher.NonceSize()+eb.cipher.Overhead() {
		// Not enough data for decryption (might be a new/empty page)
		return 0, io.EOF
	}

	// Extract nonce and ciphertext
	nonceSize := eb.cipher.NonceSize()
	nonce := encryptedBuf[:nonceSize]
	ciphertext := encryptedBuf[nonceSize:n]

	// Decrypt with page offset as authenticated data (AAD)
	aad := make([]byte, 8)
	aadOffset, err := checkedUint64Offset(offset)
	if err != nil {
		return 0, err
	}
	binary.LittleEndian.PutUint64(aad, aadOffset)
	plaintext, err := eb.cipher.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDecryptionFailed, err)
	}

	// Copy decrypted data to buffer
	copied := copy(buf, plaintext)
	return copied, nil
}

// WriteAt encrypts and writes data to the backend
func (eb *EncryptedBackend) WriteAt(buf []byte, offset int64) (int, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.closed {
		return 0, ErrBackendClosed
	}

	if !eb.config.Enabled {
		return eb.backend.WriteAt(buf, offset)
	}

	if err := validateEncryptedPageIO(len(buf), offset); err != nil {
		return 0, err
	}

	// Generate random nonce
	nonce := make([]byte, eb.cipher.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrEncryptionFailed, err)
	}

	// Encrypt data with page offset as authenticated data (AAD)
	aad := make([]byte, 8)
	aadOffset, err := checkedUint64Offset(offset)
	if err != nil {
		return 0, err
	}
	binary.LittleEndian.PutUint64(aad, aadOffset)
	ciphertext := eb.cipher.Seal(nonce, nonce, buf, aad)

	// Write encrypted data at the scaled physical offset (see physicalOffset).
	if _, err := WriteFullAt(eb.backend, ciphertext, eb.physicalOffset(offset)); err != nil {
		return 0, err
	}
	return len(buf), nil
}

// Sync ensures all data is written to disk
func (eb *EncryptedBackend) Sync() error {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if eb.closed {
		return ErrBackendClosed
	}

	return eb.backend.Sync()
}

// Size returns the current data size (unencrypted size)
func (eb *EncryptedBackend) Size() int64 {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if eb.closed {
		return 0
	}

	size := eb.backend.Size()
	if !eb.config.Enabled || size == 0 {
		return size
	}

	// Calculate unencrypted size from encrypted size
	encryptedSize := PageSize + eb.cipher.NonceSize() + eb.cipher.Overhead()
	numPages := (size + int64(encryptedSize) - 1) / int64(encryptedSize)
	return numPages * PageSize
}

// Truncate resizes the underlying storage
func (eb *EncryptedBackend) Truncate(size int64) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.closed {
		return ErrBackendClosed
	}

	if !eb.config.Enabled {
		return eb.backend.Truncate(size)
	}

	// Calculate encrypted size
	numPages := (size + PageSize - 1) / PageSize
	encryptedSize := numPages * int64(PageSize+int64(eb.cipher.NonceSize()+eb.cipher.Overhead()))
	return eb.backend.Truncate(encryptedSize)
}

// Close closes the underlying backend
func (eb *EncryptedBackend) Close() error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.closed {
		return nil
	}

	// Clear sensitive data
	if eb.sessionKey != nil {
		for i := range eb.sessionKey {
			eb.sessionKey[i] = 0
		}
		eb.sessionKey = nil
	}

	// Clear original key from memory
	for i := range eb.config.Key {
		eb.config.Key[i] = 0
	}

	eb.cipher = nil
	eb.closed = true
	return eb.backend.Close()
}

// GetCipher returns the AEAD cipher used for encryption.
// This can be used to encrypt other data stores (e.g., WAL) with the same key.
func (eb *EncryptedBackend) GetCipher() cipher.AEAD {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if eb.closed {
		return nil
	}

	return eb.cipher
}

// GetSalt returns a copy of the salt used for key derivation
func (eb *EncryptedBackend) GetSalt() []byte {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if len(eb.config.Salt) == 0 {
		return nil
	}
	salt := make([]byte, len(eb.config.Salt))
	copy(salt, eb.config.Salt)
	return salt
}

const (
	saltFileMarker         = "CBLT_SALT_V1"
	maxEncryptionSaltBytes = 4096
)

// PersistSalt writes the salt to a sidecar file (<dbpath>.salt).
// This must be called after NewEncryptedBackend when a new salt is generated.
func PersistSalt(dbPath string, salt []byte) error {
	if len(salt) == 0 {
		return nil
	}
	if len(salt) > maxEncryptionSaltBytes {
		return fmt.Errorf("%w: salt is too large: %d bytes (max %d)", ErrInvalidSalt, len(salt), maxEncryptionSaltBytes)
	}
	saltPath, err := saltSidecarPath(dbPath)
	if err != nil {
		return err
	}
	data := make([]byte, 0, len(saltFileMarker)+1+len(salt))
	data = append(data, saltFileMarker...)
	data = append(data, '\n')
	data = append(data, salt...)
	return writeFileAtomic(saltPath, data, 0600)
}

// LoadSalt reads a previously persisted salt from the sidecar file.
// Returns nil without error if the file does not exist.
func LoadSalt(dbPath string) ([]byte, error) {
	saltPath, err := saltSidecarPath(dbPath)
	if err != nil {
		return nil, err
	}
	data, err := readSaltFile(saltPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read salt file: %w", err)
	}

	// Verify marker
	markerLen := len(saltFileMarker) + 1 // marker + newline
	if len(data) < markerLen || string(data[:len(saltFileMarker)]) != saltFileMarker {
		return nil, ErrInvalidSalt
	}

	salt := make([]byte, len(data)-markerLen)
	copy(salt, data[markerLen:])
	if len(salt) == 0 {
		return nil, ErrInvalidSalt
	}
	if len(salt) > maxEncryptionSaltBytes {
		return nil, fmt.Errorf("%w: salt is too large: %d bytes (max %d)", ErrInvalidSalt, len(salt), maxEncryptionSaltBytes)
	}
	return salt, nil
}

func readSaltFile(path string) ([]byte, error) {
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("salt file must not be a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("salt file must be a regular file: %s", path)
	}
	maxSaltFileBytes := int64(len(saltFileMarker) + 1 + maxEncryptionSaltBytes)
	if info.Size() > maxSaltFileBytes {
		return nil, fmt.Errorf("%w: salt file is too large: %d bytes (max %d)", ErrInvalidSalt, info.Size(), maxSaltFileBytes)
	}

	file, err := os.Open(path) // #nosec G304 - salt sidecar path is derived from a cleaned database path and validated before use.
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("salt file must be a regular file: %s", path)
	}
	if openedInfo.Size() > maxSaltFileBytes {
		return nil, fmt.Errorf("%w: salt file is too large: %d bytes (max %d)", ErrInvalidSalt, openedInfo.Size(), maxSaltFileBytes)
	}
	if !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("salt file changed while opening: %s", path)
	}
	if err := file.Chmod(0600); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxSaltFileBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxSaltFileBytes {
		return nil, fmt.Errorf("%w: salt file is too large: %d bytes (max %d)", ErrInvalidSalt, len(data), maxSaltFileBytes)
	}
	return data, nil
}

func saltSidecarPath(dbPath string) (string, error) {
	if strings.TrimSpace(dbPath) == "" {
		return "", fmt.Errorf("database path cannot be empty")
	}
	return filepath.Clean(dbPath) + ".salt", nil
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	path = filepath.Clean(path)
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	if err := prepareAtomicFileDir(dir); err != nil {
		return err
	}

	file, err := os.CreateTemp(dir, base+".tmp-*") // #nosec G304 - caller provides a cleaned sidecar path.
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpPath := file.Name()
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := file.Chmod(perm); err != nil {
		return fmt.Errorf("failed to set temporary file permissions: %w", err)
	}
	if _, err := writeFileFull(file, data); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}
	closed = true

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to replace file: %w", err)
	}
	tmpPath = ""
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}
	return nil
}

func writeFileFull(writer io.Writer, data []byte) (int, error) {
	n, err := writer.Write(data)
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func prepareAtomicFileDir(dir string) error {
	dir = filepath.Clean(dir)
	if dir == "." {
		return rejectAtomicFileDirSymlinks(dir)
	}
	if err := rejectAtomicFileDirSymlinks(dir); err != nil {
		return err
	}

	info, statErr := os.Lstat(dir)
	preexisting := statErr == nil
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			return fmt.Errorf("failed to stat atomic file directory: %w", statErr)
		}
	} else {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("atomic file directory must not be a symlink: %s", dir)
		}
		if !info.IsDir() {
			return fmt.Errorf("atomic file directory must be a directory: %s", dir)
		}
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("failed to create atomic file directory: %w", err)
	}
	if !preexisting {
		if err := os.Chmod(dir, 0750); err != nil {
			return fmt.Errorf("failed to set atomic file directory permissions: %w", err)
		}
	}
	if err := rejectAtomicFileDirSymlinks(dir); err != nil {
		return err
	}

	openedInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !openedInfo.IsDir() {
		return fmt.Errorf("atomic file directory must be a directory: %s", dir)
	}
	if preexisting && !os.SameFile(info, openedInfo) {
		return fmt.Errorf("atomic file directory changed while opening: %s", dir)
	}
	return nil
}

func rejectAtomicFileDirSymlinks(path string) error {
	return rejectStoragePathSymlinkComponents(path, "atomic file directory")
}

func rejectStoragePathSymlinkComponents(path, label string) error {
	path = filepath.Clean(path)
	if path == "." || path == string(os.PathSeparator) {
		return nil
	}

	current := "."
	if filepath.IsAbs(path) {
		current = string(os.PathSeparator)
		path = strings.TrimPrefix(path, string(os.PathSeparator))
	}
	for _, part := range strings.Split(path, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to stat %s component: %w", label, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s component must not be a symlink: %s", label, current)
		}
	}
	return nil
}

func syncDir(dir string) error {
	file, err := os.Open(dir) // #nosec G304 - caller passes a cleaned directory path.
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
