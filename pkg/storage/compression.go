package storage

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	CompressionLevelFast                             // BestSpeed
	CompressionLevelDefault                          // DefaultCompression
	CompressionLevelBest                             // BestCompression
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
	backend Backend
	config  *CompressionConfig

	// Pools reuse buffers and writers to reduce allocations.
	writeBufPool sync.Pool
	readBufPool  sync.Pool
	zlibWriters  sync.Pool
	zstdEncoders sync.Pool
}

// NewCompressedBackend creates a compressed backend wrapper.
func NewCompressedBackend(backend Backend, config *CompressionConfig) (*CompressedBackend, error) {
	if config == nil {
		config = DefaultCompressionConfig()
	}
	cb := &CompressedBackend{
		backend: backend,
		config:  config,
	}
	cb.writeBufPool.New = func() interface{} {
		b := make([]byte, PageSize)
		return &b
	}
	cb.readBufPool.New = func() interface{} {
		b := make([]byte, PageSize)
		return &b
	}
	return cb, nil
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
	if !cb.config.Enabled {
		return cb.backend.WriteAt(buf, offset)
	}

	// Attempt compression.
	compressed, ratio, err := cb.compress(buf)
	if err != nil || len(compressed) >= len(buf) {
		// Fallback to raw write on compression error or if compression
		// didn't actually shrink the data.
		return cb.backend.WriteAt(buf, offset)
	}

	// Only store compressed if it meets the minimum ratio threshold.
	if ratio <= cb.config.MinRatio {
		// Store compressed with header.
		writeBuf := cb.getWriteBuf()
		defer cb.putWriteBuf(writeBuf)

		header := (*writeBuf)[:compressionHeaderSize]
		copy(header[:4], cb.magicForAlgorithm())
		binary.LittleEndian.PutUint16(header[4:6], uint16(len(buf)))
		binary.LittleEndian.PutUint16(header[6:8], uint16(len(compressed)))

		// Write header + compressed data.
		n, err := cb.backend.WriteAt(header, offset)
		if err != nil {
			return n, err
		}
		n2, err := cb.backend.WriteAt(compressed, offset+int64(compressionHeaderSize))
		return n + n2, err
	}

	// Store raw — compression didn't save enough space.
	return cb.backend.WriteAt(buf, offset)
}

// Sync delegates to the underlying backend.
func (cb *CompressedBackend) Sync() error {
	return cb.backend.Sync()
}

// Size returns the logical (uncompressed) size.
func (cb *CompressedBackend) Size() int64 {
	return cb.backend.Size()
}

// Truncate delegates to the underlying backend.
func (cb *CompressedBackend) Truncate(size int64) error {
	return cb.backend.Truncate(size)
}

// Close closes the underlying backend.
func (cb *CompressedBackend) Close() error {
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
		var err error
		w, err = zlib.NewWriterLevel(&buf, level)
		if err != nil {
			return nil, 1.0, err
		}
	}

	_, err := w.Write(data)
	if err != nil {
		cb.zlibWriters.Put(w)
		return nil, 1.0, err
	}
	err = w.Close()
	cb.zlibWriters.Put(w)
	if err != nil {
		return nil, 1.0, err
	}

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
		if err := w.Apply(lz4.CompressionLevelOption(lz4.Level9)); err != nil {
			return nil, 1.0, err
		}
	} else {
		if err := w.Apply(lz4.CompressionLevelOption(lz4.Level1)); err != nil {
			return nil, 1.0, err
		}
	}

	if _, err := w.Write(data); err != nil {
		return nil, 1.0, err
	}
	if err := w.Close(); err != nil {
		return nil, 1.0, err
	}

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
		var err error
		enc, err = zstd.NewWriter(&buf, zstd.WithEncoderLevel(level))
		if err != nil {
			return nil, 1.0, err
		}
	}

	_, err := enc.Write(data)
	if err != nil {
		cb.zstdEncoders.Put(enc)
		return nil, 1.0, err
	}
	err = enc.Close()
	cb.zstdEncoders.Put(enc)
	if err != nil {
		return nil, 1.0, err
	}

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
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return out[:n], nil
}

// decompressLZ4 decompresses LZ4-compressed data.
func (cb *CompressedBackend) decompressLZ4(data []byte, originalSize int) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	out := make([]byte, originalSize)
	n, err := io.ReadFull(r, out)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return out[:n], nil
}

// decompressZstd decompresses zstd-compressed data.
func (cb *CompressedBackend) decompressZstd(data []byte, originalSize int) ([]byte, error) {
	r, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	out := make([]byte, originalSize)
	n, err := io.ReadFull(r, out)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return out[:n], nil
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
