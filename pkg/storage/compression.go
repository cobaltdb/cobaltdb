package storage

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// CompressionType represents the compression algorithm type
type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionZlib                 // Standard zlib (DEFLATE)
	CompressionGzip                 // gzip format (better compression)
	CompressionFast                 // Fast zlib (BestSpeed level)
)

// String returns the string representation of compression type
func (ct CompressionType) String() string {
	switch ct {
	case CompressionNone:
		return "none"
	case CompressionZlib:
		return "zlib"
	case CompressionGzip:
		return "gzip"
	case CompressionFast:
		return "fast"
	default:
		return "unknown"
	}
}

// CompressionConfig configures compression settings
type CompressionConfig struct {
	Enabled     bool
	Type        CompressionType
	Level       int // For zlib: 0-9, for zstd: 1-22
	MinPageSize int // Minimum page size to compress (smaller pages skip compression)
}

// DefaultCompressionConfig returns default compression configuration
func DefaultCompressionConfig() *CompressionConfig {
	return &CompressionConfig{
		Enabled:     true,
		Type:        CompressionFast,
		Level:       flate.BestSpeed,
		MinPageSize: 512,
	}
}

// CompressionStats tracks compression statistics
type CompressionStats struct {
	PagesCompressed   uint64
	PagesDecompressed uint64
	BytesOriginal     uint64
	BytesCompressed   uint64
	TotalTimeMs       uint64
}

// CompressionRatio returns the compression ratio (higher is better)
func (cs *CompressionStats) CompressionRatio() float64 {
	if cs.BytesOriginal == 0 {
		return 0
	}
	return float64(cs.BytesOriginal) / float64(cs.BytesCompressed)
}

// SpaceSaved returns percentage of space saved
func (cs *CompressionStats) SpaceSaved() float64 {
	if cs.BytesOriginal == 0 {
		return 0
	}
	return (1 - float64(cs.BytesCompressed)/float64(cs.BytesOriginal)) * 100
}

// Compressor handles data compression
type Compressor struct {
	config *CompressionConfig
	stats  *CompressionStats
}

// NewCompressor creates a new compressor
func NewCompressor(config *CompressionConfig) *Compressor {
	if config == nil {
		config = DefaultCompressionConfig()
	}
	return &Compressor{
		config: config,
		stats:  &CompressionStats{},
	}
}

// Compress compresses data using the configured algorithm
func (c *Compressor) Compress(data []byte) ([]byte, error) {
	if !c.config.Enabled || len(data) < c.config.MinPageSize {
		// Return uncompressed with header
		return c.wrapUncompressed(data), nil
	}

	switch c.config.Type {
	case CompressionZlib:
		return c.compressZlib(data)
	case CompressionGzip:
		return c.compressGzip(data)
	case CompressionFast:
		return c.compressFast(data)
	default:
		return c.wrapUncompressed(data), nil
	}
}

// Decompress decompresses data
func (c *Compressor) Decompress(data []byte) ([]byte, error) {
	if len(data) < 5 {
		return nil, errors.New("data too short")
	}

	// Read compression type from header
	compType := CompressionType(data[0])
	originalSize := binary.BigEndian.Uint32(data[1:5])

	switch compType {
	case CompressionNone:
		return data[5:], nil
	case CompressionZlib:
		return c.decompressZlib(data[5:], originalSize)
	case CompressionGzip:
		return c.decompressGzip(data[5:])
	case CompressionFast:
		return c.decompressFast(data[5:], originalSize)
	default:
		return nil, fmt.Errorf("unknown compression type: %d", compType)
	}
}

// wrapUncompressed wraps uncompressed data with header
func (c *Compressor) wrapUncompressed(data []byte) []byte {
	result := make([]byte, 5+len(data))
	result[0] = byte(CompressionNone)
	binary.BigEndian.PutUint32(result[1:5], uint32(len(data)))
	copy(result[5:], data)
	return result
}

// compressZlib compresses using zlib
func (c *Compressor) compressZlib(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	level := c.config.Level
	if level < 0 || level > 9 {
		level = zlib.DefaultCompression
	}

	w, err := zlib.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	w.Close()

	compressed := buf.Bytes()

	// Only use compression if it actually saves space
	if len(compressed) >= len(data) {
		return c.wrapUncompressed(data), nil
	}

	// Update stats
	c.stats.PagesCompressed++
	c.stats.BytesOriginal += uint64(len(data))
	c.stats.BytesCompressed += uint64(len(compressed) + 5)

	// Wrap with header
	result := make([]byte, 5+len(compressed))
	result[0] = byte(CompressionZlib)
	binary.BigEndian.PutUint32(result[1:5], uint32(len(data)))
	copy(result[5:], compressed)

	return result, nil
}

// decompressZlib decompresses zlib data
func (c *Compressor) decompressZlib(data []byte, originalSize uint32) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	result := make([]byte, originalSize)
	_, err = io.ReadFull(r, result)
	if err != nil {
		return nil, err
	}

	c.stats.PagesDecompressed++
	return result, nil
}

// compressGzip compresses using gzip (better compression ratio)
func (c *Compressor) compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	level := c.config.Level
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		level = gzip.DefaultCompression
	}

	w, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	w.Close()

	compressed := buf.Bytes()

	// Only use compression if it actually saves space
	if len(compressed) >= len(data) {
		return c.wrapUncompressed(data), nil
	}

	// Update stats
	c.stats.PagesCompressed++
	c.stats.BytesOriginal += uint64(len(data))
	c.stats.BytesCompressed += uint64(len(compressed) + 5)

	// Wrap with header
	result := make([]byte, 5+len(compressed))
	result[0] = byte(CompressionGzip)
	binary.BigEndian.PutUint32(result[1:5], uint32(len(data)))
	copy(result[5:], compressed)

	return result, nil
}

// decompressGzip decompresses gzip data
func (c *Compressor) decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	c.stats.PagesDecompressed++
	return result, nil
}

// compressFast compresses using zlib with BestSpeed (fast compression)
func (c *Compressor) compressFast(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	w, err := zlib.NewWriterLevel(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	w.Close()

	compressed := buf.Bytes()

	// Only use compression if it actually saves space
	if len(compressed) >= len(data) {
		return c.wrapUncompressed(data), nil
	}

	// Update stats
	c.stats.PagesCompressed++
	c.stats.BytesOriginal += uint64(len(data))
	c.stats.BytesCompressed += uint64(len(compressed) + 5)

	// Wrap with header
	result := make([]byte, 5+len(compressed))
	result[0] = byte(CompressionFast)
	binary.BigEndian.PutUint32(result[1:5], uint32(len(data)))
	copy(result[5:], compressed)

	return result, nil
}

// decompressFast decompresses fast (flate) compressed data
func (c *Compressor) decompressFast(data []byte, originalSize uint32) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	result := make([]byte, originalSize)
	_, err = io.ReadFull(r, result)
	if err != nil {
		return nil, err
	}

	c.stats.PagesDecompressed++
	return result, nil
}

// GetStats returns compression statistics
func (c *Compressor) GetStats() CompressionStats {
	return *c.stats
}

// ResetStats resets compression statistics
func (c *Compressor) ResetStats() {
	c.stats = &CompressionStats{}
}

// PageCompressor compresses database pages
type PageCompressor struct {
	compressor *Compressor
	pageSize   int
}

// NewPageCompressor creates a new page compressor
func NewPageCompressor(config *CompressionConfig, pageSize int) *PageCompressor {
	return &PageCompressor{
		compressor: NewCompressor(config),
		pageSize:   pageSize,
	}
}

// CompressPage compresses a database page
func (pc *PageCompressor) CompressPage(page []byte) ([]byte, error) {
	if len(page) != pc.pageSize {
		return nil, fmt.Errorf("invalid page size: expected %d, got %d", pc.pageSize, len(page))
	}

	return pc.compressor.Compress(page)
}

// DecompressPage decompresses a database page
func (pc *PageCompressor) DecompressPage(data []byte) ([]byte, error) {
	return pc.compressor.Decompress(data)
}

// GetStats returns page compression statistics
func (pc *PageCompressor) GetStats() CompressionStats {
	return pc.compressor.GetStats()
}

// WALCompressor compresses WAL records
type WALCompressor struct {
	compressor *Compressor
}

// NewWALCompressor creates a new WAL compressor
func NewWALCompressor(config *CompressionConfig) *WALCompressor {
	return &WALCompressor{
		compressor: NewCompressor(config),
	}
}

// CompressRecord compresses a WAL record
func (wc *WALCompressor) CompressRecord(record []byte) ([]byte, error) {
	return wc.compressor.Compress(record)
}

// DecompressRecord decompresses a WAL record
func (wc *WALCompressor) DecompressRecord(data []byte) ([]byte, error) {
	return wc.compressor.Decompress(data)
}

// CompressionEstimator estimates compression ratios
type CompressionEstimator struct {
	samples    []compressionSample
	maxSamples int
}

type compressionSample struct {
	original   int
	compressed int
}

// NewCompressionEstimator creates a new estimator
func NewCompressionEstimator() *CompressionEstimator {
	return &CompressionEstimator{
		samples:    make([]compressionSample, 0, 100),
		maxSamples: 100,
	}
}

// AddSample adds a compression sample
func (ce *CompressionEstimator) AddSample(original, compressed int) {
	ce.samples = append(ce.samples, compressionSample{original, compressed})
	if len(ce.samples) > ce.maxSamples {
		ce.samples = ce.samples[1:]
	}
}

// EstimateCompressionRatio estimates the compression ratio
func (ce *CompressionEstimator) EstimateCompressionRatio() float64 {
	if len(ce.samples) == 0 {
		return 1.0 // No compression
	}

	var totalOriginal, totalCompressed int
	for _, s := range ce.samples {
		totalOriginal += s.original
		totalCompressed += s.compressed
	}

	if totalCompressed == 0 {
		return 1.0
	}

	return float64(totalOriginal) / float64(totalCompressed)
}

// ShouldCompress returns true if compression is recommended
func (ce *CompressionEstimator) ShouldCompress() bool {
	if len(ce.samples) == 0 {
		return true // Default to compressing until we have data
	}
	return ce.EstimateCompressionRatio() > 1.1 // At least 10% savings
}
