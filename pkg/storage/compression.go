package storage

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

var (
	// compressionMagic marks a page as compressed.
	compressionMagic = []byte{0xC0, 0xD1, 0xB0, 0xD1}
	// compressionHeaderSize is the size of the compression header.
	compressionHeaderSize = 8
)

// CompressionLevel controls the zlib compression level.
type CompressionLevel int

const (
	CompressionLevelNone    CompressionLevel = iota // disabled
	CompressionLevelFast                             // BestSpeed
	CompressionLevelDefault                          // DefaultCompression
	CompressionLevelBest                             // BestCompression
)

// CompressionConfig holds page-level compression settings.
type CompressionConfig struct {
	Enabled bool             // Whether compression is enabled
	Level   CompressionLevel // Compression level
	MinRatio float64         // Minimum compression ratio to store compressed (e.g., 0.9 = must save 10%)
}

// DefaultCompressionConfig returns a sensible default.
func DefaultCompressionConfig() *CompressionConfig {
	return &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelFast,
		MinRatio: 0.9, // Only store compressed if size <= 90% of original
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

	// Check for compression magic.
	if len(data) >= compressionHeaderSize && bytes.Equal(data[:4], compressionMagic) {
		originalSize := binary.LittleEndian.Uint16(data[4:6])
		payloadSize := binary.LittleEndian.Uint16(data[6:8])

		if int(originalSize) > len(buf) {
			return 0, fmt.Errorf("compressed page original size %d exceeds buffer %d", originalSize, len(buf))
		}

		payload := data[compressionHeaderSize:]
		if len(payload) < int(payloadSize) {
			payload = payload[:len(payload)]
		} else {
			payload = payload[:payloadSize]
		}

		decompressed, err := cb.decompress(payload, int(originalSize))
		if err != nil {
			return 0, fmt.Errorf("decompression failed at offset %d: %w", offset, err)
		}
		copied := copy(buf, decompressed)
		return copied, nil
	}

	// Raw page — copy directly.
	copied := copy(buf, data)
	return copied, nil
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
		copy(header[:4], compressionMagic)
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

// compress compresses data using zlib and returns the compressed bytes,
// the compression ratio (compressed/original), and any error.
func (cb *CompressedBackend) compress(data []byte) ([]byte, float64, error) {
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

// decompress decompresses zlib-compressed data.
func (cb *CompressedBackend) decompress(data []byte, originalSize int) ([]byte, error) {
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
