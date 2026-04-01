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
	"sync"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/pbkdf2"
)

var (
	ErrInvalidKey       = errors.New("invalid encryption key")
	ErrInvalidSalt      = errors.New("invalid salt")
	ErrEncryptionFailed = errors.New("encryption failed")
	ErrDecryptionFailed = errors.New("decryption failed")
	ErrKeyDerivation    = errors.New("key derivation failed")
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
}

// NewEncryptedBackend creates a new encrypted backend wrapper
func NewEncryptedBackend(backend Backend, config *EncryptionConfig) (*EncryptedBackend, error) {
	if config == nil {
		return nil, errors.New("encryption config is nil")
	}

	if !config.Enabled {
		return &EncryptedBackend{backend: backend, config: config}, nil
	}

	if len(config.Key) == 0 {
		return nil, ErrInvalidKey
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

	// Derive 32-byte key for AES-256
	if eb.config.UseArgon2 {
		// Argon2id: memory-hard, resistant to GPU attacks
		eb.sessionKey = argon2.IDKey(key, salt, 3, 64*1024, 4, 32)
	} else {
		// PBKDF2 with SHA-256
		iters := eb.config.PBKDF2Iters
		if iters == 0 {
			iters = 100000
		}
		eb.sessionKey = pbkdf2.Key(key, salt, iters, 32, sha256.New)
	}

	return nil
}

// ReadAt reads and decrypts data from the backend
func (eb *EncryptedBackend) ReadAt(buf []byte, offset int64) (int, error) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if !eb.config.Enabled {
		return eb.backend.ReadAt(buf, offset)
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

	n, err := eb.backend.ReadAt(encryptedBuf, offset)
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
	binary.LittleEndian.PutUint64(aad, uint64(offset))
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

	if !eb.config.Enabled {
		return eb.backend.WriteAt(buf, offset)
	}

	// Generate random nonce
	nonce := make([]byte, eb.cipher.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrEncryptionFailed, err)
	}

	// Encrypt data with page offset as authenticated data (AAD)
	aad := make([]byte, 8)
	binary.LittleEndian.PutUint64(aad, uint64(offset))
	ciphertext := eb.cipher.Seal(nonce, nonce, buf, aad)

	// Write encrypted data
	return eb.backend.WriteAt(ciphertext, offset)
}

// Sync ensures all data is written to disk
func (eb *EncryptedBackend) Sync() error {
	return eb.backend.Sync()
}

// Size returns the current data size (unencrypted size)
func (eb *EncryptedBackend) Size() int64 {
	size := eb.backend.Size()
	if !eb.config.Enabled || size == 0 {
		return size
	}

	// Calculate unencrypted size from encrypted size
	encryptedSize := PageSize + eb.cipher.NonceSize() + eb.cipher.Overhead()
	numPages := size / int64(encryptedSize)
	return numPages * PageSize
}

// Truncate resizes the underlying storage
func (eb *EncryptedBackend) Truncate(size int64) error {
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

	return eb.backend.Close()
}

// GetCipher returns the AEAD cipher used for encryption.
// This can be used to encrypt other data stores (e.g., WAL) with the same key.
func (eb *EncryptedBackend) GetCipher() cipher.AEAD {
	return eb.cipher
}

// GetSalt returns the salt used for key derivation
func (eb *EncryptedBackend) GetSalt() []byte {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.config.Salt
}
