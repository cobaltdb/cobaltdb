package storage

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

var (
	// compressionMagicZlib marks a page compressed with zlib.
	compressionMagicZlib = []byte{0xC0, 0xD1, 0xB0, 0xD1}
	// compressionMagicLZ4 marks a page compressed with LZ4.
	compressionMagicLZ4 = []byte{0xC0, 0xD2, 0xB0, 0xD2}
	// compressionMagicZstd marks a page compressed with zstd.
	compressionMagicZstd = []byte{0xC0, 0xD3, 0xB0, 0xD3}
	// compressionHeaderSize is the size of the compression header.
	compressionHeaderSize = 8
)

// CompressionAlgorithm identifies the compression codec.
type CompressionAlgorithm int

const (
	CompressionAlgorithmZlib CompressionAlgorithm = iota
	CompressionAlgorithmLZ4
	CompressionAlgorithmZstd
)

// CompressionLevel controls the compression level.
type CompressionLevel int

const (
	CompressionLevelNone    CompressionLevel = iota // disabled
	CompressionLevelFast                            // BestSpeed
	CompressionLevelDefault                         // DefaultCompression
	CompressionLevelBest                            // BestCompression
)

// CompressionConfig holds page-level compression settings.
type CompressionConfig struct {
	Enabled   bool                 // Whether compression is enabled
	Algorithm CompressionAlgorithm // Compression algorithm
	Level     CompressionLevel     // Compression level
	MinRatio  float64              // Minimum compression ratio to store compressed (e.g., 0.9 = must save 10%)
}

// DefaultCompressionConfig returns a sensible default.
func DefaultCompressionConfig() *CompressionConfig {
	return &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelFast,
		MinRatio:  0.9, // Only store compressed if size <= 90% of original
	}
}

// CompressedBackend wraps a Backend with transparent page-level compression.
// Each page slot is PageSize bytes logically. Physically, compressed pages
// occupy less space (header + compressed payload) creating sparse-file holes
// on supported filesystems.
type CompressedBackend struct {
	backend     Backend
	config      *CompressionConfig
	mu          sync.RWMutex
	closed      bool
	logicalSize int64

	// Pools reuse buffers, writers, and readers to reduce allocations.
	writeBufPool sync.Pool
	readBufPool  sync.Pool
	zlibWriters  sync.Pool
	zstdEncoders sync.Pool
	lz4Readers   sync.Pool
}

// NewCompressedBackend creates a compressed backend wrapper.
func NewCompressedBackend(backend Backend, config *CompressionConfig) (*CompressedBackend, error) {
	if config == nil {
		config = DefaultCompressionConfig()
	} else {
		normalized, err := normalizeCompressionConfig(config)
		if err != nil {
			return nil, err
		}
		config = normalized
	}
	cb := &CompressedBackend{
		backend:     backend,
		config:      config,
		logicalSize: compressedLogicalSize(backend.Size()),
	}
	cb.writeBufPool.New = func() interface{} {
		b := make([]byte, PageSize)
		return &b
	}
	cb.readBufPool.New = func() interface{} {
		b := make([]byte, PageSize)
		return &b
	}
	cb.lz4Readers.New = func() interface{} {
		return lz4.NewReader(bytes.NewReader(nil))
	}
	return cb, nil
}

func compressedLogicalSize(physicalSize int64) int64 {
	if physicalSize <= 0 {
		return 0
	}
	pageSize := int64(PageSize)
	return ((physicalSize + pageSize - 1) / pageSize) * pageSize
}

func normalizeCompressionConfig(config *CompressionConfig) (*CompressionConfig, error) {
	defaults := DefaultCompressionConfig()
	normalized := *config
	if !normalized.Enabled {
		return &normalized, nil
	}
	switch normalized.Algorithm {
	case CompressionAlgorithmZlib, CompressionAlgorithmLZ4, CompressionAlgorithmZstd:
	default:
		return nil, fmt.Errorf("invalid compression algorithm: %d", normalized.Algorithm)
	}
	switch normalized.Level {
	case CompressionLevelNone, CompressionLevelFast, CompressionLevelDefault, CompressionLevelBest:
	default:
		return nil, fmt.Errorf("invalid compression level: %d", normalized.Level)
	}
	if normalized.MinRatio < 0 || normalized.MinRatio > 1 || math.IsNaN(normalized.MinRatio) || math.IsInf(normalized.MinRatio, 0) {
		return nil, fmt.Errorf("invalid compression min ratio: %v", normalized.MinRatio)
	}
	if normalized.Algorithm == defaults.Algorithm && normalized.Level == CompressionLevelNone && normalized.MinRatio == 0 {
		normalized.Level = defaults.Level
	}
	if normalized.MinRatio == 0 {
		normalized.MinRatio = defaults.MinRatio
	}
	return &normalized, nil
}

// magicForAlgorithm returns the magic bytes for the configured algorithm.
func (cb *CompressedBackend) magicForAlgorithm() []byte {
	switch cb.config.Algorithm {
	case CompressionAlgorithmLZ4:
		return compressionMagicLZ4
	case CompressionAlgorithmZstd:
		return compressionMagicZstd
	default:
		return compressionMagicZlib
	}
}

// ReadAt reads a page from the backend, decompressing if necessary.
func (cb *CompressedBackend) ReadAt(buf []byte, offset int64) (int, error) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.closed {
		return 0, ErrBackendClosed
	}

	if !cb.config.Enabled {
		return cb.backend.ReadAt(buf, offset)
	}

	// Read the full logical page slot.
	readBuf := cb.getReadBuf()
	defer cb.putReadBuf(readBuf)

	n, err := cb.backend.ReadAt(*readBuf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if n == 0 {
		return 0, io.EOF
	}

	data := (*readBuf)[:n]

	// Check for any compression magic.
	if len(data) >= compressionHeaderSize {
		algo := cb.algorithmFromMagic(data[:4])
		if algo != -1 {
			originalSize := binary.LittleEndian.Uint16(data[4:6])
			payloadSize := binary.LittleEndian.Uint16(data[6:8])

			if int(originalSize) > len(buf) {
				return 0, fmt.Errorf("compressed page original size %d exceeds buffer %d", originalSize, len(buf))
			}

			payload := data[compressionHeaderSize:]
			if len(payload) < int(payloadSize) {
				return 0, fmt.Errorf("compressed page payload truncated: header declares %d bytes, found %d",
					payloadSize, len(payload))
			}
			if len(payload) > int(payloadSize) {
				payload = payload[:payloadSize]
			}

			decompressed, err := cb.decompressWithAlgorithm(payload, int(originalSize), algo)
			if err != nil {
				return 0, fmt.Errorf("decompression failed at offset %d: %w", offset, err)
			}
			copied := copy(buf, decompressed)
			return copied, nil
		}
	}

	// Raw page — copy directly.
	copied := copy(buf, data)
	return copied, nil
}

// algorithmFromMagic maps magic bytes to a CompressionAlgorithm.
// Returns -1 if no known magic is matched.
func (cb *CompressedBackend) algorithmFromMagic(magic []byte) CompressionAlgorithm {
	switch {
	case bytes.Equal(magic, compressionMagicZlib):
		return CompressionAlgorithmZlib
	case bytes.Equal(magic, compressionMagicLZ4):
		return CompressionAlgorithmLZ4
	case bytes.Equal(magic, compressionMagicZstd):
		return CompressionAlgorithmZstd
	default:
		return -1
	}
}

// WriteAt compresses and writes a page to the backend.
func (cb *CompressedBackend) WriteAt(buf []byte, offset int64) (int, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.closed {
		return 0, ErrBackendClosed
	}
	if err := validateCompressedWriteRange(offset, len(buf)); err != nil {
		return 0, err
	}

	if !cb.config.Enabled {
		n, err := cb.backend.WriteAt(buf, offset)
		cb.updateLogicalSize(offset, n, err)
		return n, err
	}

	// Attempt compression.
	compressed, ratio, err := cb.compress(buf)
	if err != nil || len(compressed) >= len(buf) {
		// Fallback to raw write on compression error or if compression
		// didn't actually shrink the data.
		return cb.writeRaw(buf, offset)
	}

	// Only store compressed if it meets the minimum ratio threshold AND the
	// header + payload fit within a single page slot. Without the size guard,
	// a MinRatio near 1.0 could admit a payload whose 8-byte header pushes
	// the physical record past PageSize, spilling into the next page's slot.
	if ratio <= cb.config.MinRatio && compressionHeaderSize+len(compressed) <= PageSize {
		// The enclosing PageSize fit check guarantees both lengths fit uint16.
		originalSize := uint16(len(buf))          // #nosec G115 -- bounded by PageSize above.
		compressedSize := uint16(len(compressed)) // #nosec G115 -- bounded by PageSize above.

		// Assemble header + payload into one buffer and issue a single write.
		// Two separate WriteAt calls would let a crash between them leave a
		// header on disk that describes stale payload bytes.
		writeBuf := cb.getWriteBuf()
		defer cb.putWriteBuf(writeBuf)

		record := (*writeBuf)[:compressionHeaderSize+len(compressed)]
		copy(record[:4], cb.magicForAlgorithm())
		binary.LittleEndian.PutUint16(record[4:6], originalSize)
		binary.LittleEndian.PutUint16(record[6:8], compressedSize)
		copy(record[compressionHeaderSize:], compressed)

		if n, err := WriteFullAt(cb.backend, record, offset); err != nil {
			return n, err
		}
		cb.updateLogicalSize(offset, len(buf), nil)
		return len(buf), nil
	}

	// Store raw — compression didn't save enough space.
	return cb.writeRaw(buf, offset)
}

// writeRaw stores a page verbatim (no compression header). On read, raw pages
// are distinguished from compressed records solely by their leading 4 bytes NOT
// matching a compression magic — a full raw page leaves no room to prepend an
// 8-byte header. If a raw page's first 4 bytes collided with a magic, ReadAt
// would misinterpret it as a compressed record and either error or return
// silently-wrong bytes, so we refuse the write loudly instead of persisting a
// silently-unreadable page.
//
// In practice this never triggers through the engine: the page manager always
// writes a small little-endian page ID into bytes[0:4] (page.SerializeHeader),
// which cannot equal the 0xC0 0xD1.. magics until page IDs approach ~3.5e9
// (~14 TB). The guard makes CompressedBackend sound for arbitrary page content
// at the library level.
func (cb *CompressedBackend) writeRaw(buf []byte, offset int64) (int, error) {
	if len(buf) >= compressionHeaderSize && cb.algorithmFromMagic(buf[:4]) != -1 {
		return 0, fmt.Errorf("compression: cannot store raw page at offset %d: leading bytes collide with a compression magic (page unreadable if stored)", offset)
	}
	n, err := WriteFullAt(cb.backend, buf, offset)
	cb.updateLogicalSize(offset, n, err)
	return n, err
}

func (cb *CompressedBackend) updateLogicalSize(offset int64, n int, err error) {
	if err != nil || n <= 0 {
		return
	}
	if int64(n) > maxMemoryOffset-offset {
		return
	}
	end := offset + int64(n)
	if end > cb.logicalSize {
		cb.logicalSize = end
	}
}

func validateCompressedWriteRange(offset int64, length int) error {
	if offset < 0 {
		return ErrInvalidOffset
	}
	if int64(length) > maxMemoryOffset-offset {
		return ErrInvalidSize
	}
	return nil
}

// Sync delegates to the underlying backend.
func (cb *CompressedBackend) Sync() error {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.closed {
		return ErrBackendClosed
	}

	return cb.backend.Sync()
}

// Size returns the logical (uncompressed) size.
func (cb *CompressedBackend) Size() int64 {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.closed {
		return 0
	}

	return cb.logicalSize
}

// Truncate delegates to the underlying backend.
// It takes the write lock because it mutates cb.logicalSize.
func (cb *CompressedBackend) Truncate(size int64) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.closed {
		return ErrBackendClosed
	}
	if size < 0 {
		return ErrInvalidSize
	}

	if err := cb.backend.Truncate(size); err != nil {
		return err
	}
	cb.logicalSize = size
	return nil
}

// Close closes the underlying backend.
func (cb *CompressedBackend) Close() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.closed {
		return nil
	}

	cb.closed = true
	return cb.backend.Close()
}

// compress compresses data using the configured algorithm.
func (cb *CompressedBackend) compress(data []byte) ([]byte, float64, error) {
	switch cb.config.Algorithm {
	case CompressionAlgorithmLZ4:
		return cb.compressLZ4(data)
	case CompressionAlgorithmZstd:
		return cb.compressZstd(data)
	default:
		return cb.compressZlib(data)
	}
}

// compressZlib compresses data using zlib.
func (cb *CompressedBackend) compressZlib(data []byte) ([]byte, float64, error) {
	level := zlib.DefaultCompression
	switch cb.config.Level {
	case CompressionLevelFast:
		level = zlib.BestSpeed
	case CompressionLevelBest:
		level = zlib.BestCompression
	case CompressionLevelNone:
		return data, 1.0, nil
	}

	var buf bytes.Buffer
	w, ok := cb.zlibWriters.Get().(*zlib.Writer)
	if ok {
		w.Reset(&buf)
	} else {
		// level is one of zlib's validated constants.
		w, _ = zlib.NewWriterLevel(&buf, level)
	}

	// bytes.Buffer writes cannot fail; Close is the codec's fallible finalization step.
	_, _ = w.Write(data)
	_ = w.Close()
	cb.zlibWriters.Put(w)

	compressed := buf.Bytes()
	ratio := float64(len(compressed)) / float64(len(data))
	return compressed, ratio, nil
}

// compressLZ4 compresses data using LZ4.
func (cb *CompressedBackend) compressLZ4(data []byte) ([]byte, float64, error) {
	if cb.config.Level == CompressionLevelNone {
		return data, 1.0, nil
	}

	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if cb.config.Level == CompressionLevelBest {
		_ = w.Apply(lz4.CompressionLevelOption(lz4.Level9))
	} else {
		_ = w.Apply(lz4.CompressionLevelOption(lz4.Level1))
	}

	// bytes.Buffer writes cannot fail; Close is the codec's fallible finalization step.
	_, _ = w.Write(data)
	_ = w.Close()

	compressed := buf.Bytes()
	ratio := float64(len(compressed)) / float64(len(data))
	return compressed, ratio, nil
}

// compressZstd compresses data using zstd.
func (cb *CompressedBackend) compressZstd(data []byte) ([]byte, float64, error) {
	if cb.config.Level == CompressionLevelNone {
		return data, 1.0, nil
	}

	var buf bytes.Buffer
	enc, ok := cb.zstdEncoders.Get().(*zstd.Encoder)
	if ok {
		enc.Reset(&buf)
	} else {
		level := zstd.SpeedDefault
		switch cb.config.Level {
		case CompressionLevelFast:
			level = zstd.SpeedFastest
		case CompressionLevelBest:
			level = zstd.SpeedBestCompression
		}
		// level is one of zstd's validated constants.
		enc, _ = zstd.NewWriter(&buf, zstd.WithEncoderLevel(level))
	}

	// bytes.Buffer writes cannot fail; Close is the codec's fallible finalization step.
	_, _ = enc.Write(data)
	_ = enc.Close()
	cb.zstdEncoders.Put(enc)

	compressed := buf.Bytes()
	ratio := float64(len(compressed)) / float64(len(data))
	return compressed, ratio, nil
}

// decompressWithAlgorithm decompresses data using the identified algorithm.
func (cb *CompressedBackend) decompressWithAlgorithm(data []byte, originalSize int, algo CompressionAlgorithm) ([]byte, error) {
	switch algo {
	case CompressionAlgorithmLZ4:
		return cb.decompressLZ4(data, originalSize)
	case CompressionAlgorithmZstd:
		return cb.decompressZstd(data, originalSize)
	default:
		return cb.decompressZlib(data, originalSize)
	}
}

// decompressZlib decompresses zlib-compressed data.
func (cb *CompressedBackend) decompressZlib(data []byte, originalSize int) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	out := make([]byte, originalSize)
	n, err := io.ReadFull(r, out)
	if err := requireFullDecompressed(n, originalSize, err); err != nil {
		return nil, err
	}
	return out[:n], nil
}

// decompressLZ4 decompresses LZ4-compressed data.
func (cb *CompressedBackend) decompressLZ4(data []byte, originalSize int) ([]byte, error) {
	r, ok := cb.lz4Readers.Get().(*lz4.Reader)
	if !ok {
		r = lz4.NewReader(bytes.NewReader(data))
	} else {
		r.Reset(bytes.NewReader(data))
	}
	defer cb.lz4Readers.Put(r)
	out := make([]byte, originalSize)
	n, err := io.ReadFull(r, out)
	if err := requireFullDecompressed(n, originalSize, err); err != nil {
		return nil, err
	}
	return out[:n], nil
}

// decompressZstd decompresses zstd-compressed data.
func (cb *CompressedBackend) decompressZstd(data []byte, originalSize int) ([]byte, error) {
	// zstd.NewReader only configures a decoder; malformed frames are reported by ReadFull below.
	r, _ := zstd.NewReader(bytes.NewReader(data))
	defer r.Close()

	out := make([]byte, originalSize)
	n, err := io.ReadFull(r, out)
	if err := requireFullDecompressed(n, originalSize, err); err != nil {
		return nil, err
	}
	return out[:n], nil
}

func requireFullDecompressed(n, originalSize int, err error) error {
	if err != nil {
		return fmt.Errorf("compressed page produced %d of %d bytes: %w", n, originalSize, err)
	}
	if n != originalSize {
		return fmt.Errorf("compressed page produced %d of %d bytes", n, originalSize)
	}
	return nil
}

// getWriteBuf returns a pooled buffer for writes.
func (cb *CompressedBackend) getWriteBuf() *[]byte {
	b, ok := cb.writeBufPool.Get().(*[]byte)
	if !ok {
		buf := make([]byte, PageSize)
		return &buf
	}
	return b
}

// putWriteBuf returns a buffer to the pool.
func (cb *CompressedBackend) putWriteBuf(b *[]byte) {
	cb.writeBufPool.Put(b)
}

// getReadBuf returns a pooled buffer for reads.
func (cb *CompressedBackend) getReadBuf() *[]byte {
	b, ok := cb.readBufPool.Get().(*[]byte)
	if !ok {
		buf := make([]byte, PageSize)
		return &buf
	}
	return b
}

// putReadBuf returns a buffer to the pool.
func (cb *CompressedBackend) putReadBuf(b *[]byte) {
	cb.readBufPool.Put(b)
}
