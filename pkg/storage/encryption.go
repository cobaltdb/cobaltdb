package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
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
}

// NewEncryptedBackend creates a new encrypted backend wrapper
func NewEncryptedBackend(backend Backend, config *EncryptionConfig) (*EncryptedBackend, error) {
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
		return nil, fmt.Errorf("%w: %v", ErrEncryptionFailed, err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryptionFailed, err)
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
			return fmt.Errorf("%w: %v", ErrKeyDerivation, err)
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

	encryptedBuf := make([]byte, encryptedSize)
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

	// Decrypt
	plaintext, err := eb.cipher.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrDecryptionFailed, err)
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
		return 0, fmt.Errorf("%w: %v", ErrEncryptionFailed, err)
	}

	// Encrypt data
	ciphertext := eb.cipher.Seal(nonce, nonce, buf, nil)

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

	return eb.backend.Close()
}

// GetSalt returns the salt used for key derivation
func (eb *EncryptedBackend) GetSalt() []byte {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.config.Salt
}

// GenerateSecureKey generates a random 32-byte encryption key
func GenerateSecureKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	return key, nil
}

// DeriveKeyFromPassword derives a key from a password using Argon2id
func DeriveKeyFromPassword(password string, salt []byte) []byte {
	if salt == nil {
		salt = make([]byte, 16)
		rand.Read(salt)
	}
	return argon2.IDKey([]byte(password), salt, 3, 64*1024, 4, 32)
}

// EncryptionHeader stores encryption metadata at the start of the file
type EncryptionHeader struct {
	Magic         [8]byte  // "COBALTEN"
	Version       uint16   // 1
	Algorithm     uint16   // 1 = AES-256-GCM
	KeyDerivation uint16   // 1 = Argon2id, 2 = PBKDF2
	Reserved      uint16   // Padding
	Salt          [16]byte // Salt for key derivation
	Iterations    uint32   // PBKDF2 iterations (if used)
}

// WriteHeader writes encryption header to the file
func WriteHeader(backend Backend, header *EncryptionHeader) error {
	buf := make([]byte, 64)
	copy(buf[0:8], header.Magic[:])
	binary.LittleEndian.PutUint16(buf[8:10], header.Version)
	binary.LittleEndian.PutUint16(buf[10:12], header.Algorithm)
	binary.LittleEndian.PutUint16(buf[12:14], header.KeyDerivation)
	binary.LittleEndian.PutUint16(buf[14:16], header.Reserved)
	copy(buf[16:32], header.Salt[:])
	binary.LittleEndian.PutUint32(buf[32:36], header.Iterations)

	_, err := backend.WriteAt(buf, 0)
	return err
}

// ReadHeader reads encryption header from the file
func ReadHeader(backend Backend) (*EncryptionHeader, error) {
	buf := make([]byte, 64)
	_, err := backend.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}

	var header EncryptionHeader
	copy(header.Magic[:], buf[0:8])
	header.Version = binary.LittleEndian.Uint16(buf[8:10])
	header.Algorithm = binary.LittleEndian.Uint16(buf[10:12])
	header.KeyDerivation = binary.LittleEndian.Uint16(buf[12:14])
	header.Reserved = binary.LittleEndian.Uint16(buf[14:16])
	copy(header.Salt[:], buf[16:32])
	header.Iterations = binary.LittleEndian.Uint32(buf[32:36])

	// Verify magic
	if subtle.ConstantTimeCompare(header.Magic[:], []byte("COBALTEN")) != 1 {
		return nil, errors.New("invalid encryption header")
	}

	return &header, nil
}

// IsEncrypted checks if a file is encrypted by reading the magic header
func IsEncrypted(backend Backend) bool {
	header, err := ReadHeader(backend)
	return err == nil && header != nil
}

// EncryptedPageSize returns the size of an encrypted page
func EncryptedPageSize(cipher cipher.AEAD) int {
	return PageSize + cipher.NonceSize() + cipher.Overhead()
}
