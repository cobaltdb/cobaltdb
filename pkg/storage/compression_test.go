package storage

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/binary"
	"testing"
)

func TestCompressionTypeString(t *testing.T) {
	tests := []struct {
		ct       CompressionType
		expected string
	}{
		{CompressionNone, "none"},
		{CompressionZlib, "zlib"},
		{CompressionGzip, "gzip"},
		{CompressionFast, "fast"},
		{CompressionType(99), "unknown"},
	}

	for _, tc := range tests {
		if tc.ct.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.ct.String())
		}
	}
}

func TestDefaultCompressionConfig(t *testing.T) {
	config := DefaultCompressionConfig()

	if !config.Enabled {
		t.Error("Expected compression to be enabled by default")
	}

	if config.Type != CompressionFast {
		t.Errorf("Expected fast compression, got %s", config.Type)
	}

	if config.Level != flate.BestSpeed {
		t.Errorf("Expected BestSpeed level, got %d", config.Level)
	}

	if config.MinPageSize != 512 {
		t.Errorf("Expected min page size 512, got %d", config.MinPageSize)
	}
}

func TestCompressionStats(t *testing.T) {
	stats := &CompressionStats{
		PagesCompressed: 100,
		BytesOriginal:   10000,
		BytesCompressed: 5000,
	}

	ratio := stats.CompressionRatio()
	if ratio != 2.0 {
		t.Errorf("Expected ratio 2.0, got %f", ratio)
	}

	saved := stats.SpaceSaved()
	if saved != 50.0 {
		t.Errorf("Expected 50%% space saved, got %f%%", saved)
	}

	// Test with zero values
	emptyStats := &CompressionStats{}
	if emptyStats.CompressionRatio() != 0 {
		t.Error("Empty stats should have ratio 0")
	}
	if emptyStats.SpaceSaved() != 0 {
		t.Error("Empty stats should have 0% saved")
	}
}

func TestCompressorCompressDecompress(t *testing.T) {
	data := []byte("This is test data that will be compressed and decompressed. " +
		"Let's add more content to make it compressible. " +
		"The quick brown fox jumps over the lazy dog. " +
		"The quick brown fox jumps over the lazy dog.")

	// Test each compression type
	types := []CompressionType{CompressionNone, CompressionZlib, CompressionGzip, CompressionFast}

	for _, compType := range types {
		config := &CompressionConfig{
			Enabled:     true,
			Type:        compType,
			Level:       3,
			MinPageSize: 10, // Small to ensure compression is tried
		}

		compressor := NewCompressor(config)

		compressed, err := compressor.Compress(data)
		if err != nil {
			t.Errorf("Failed to compress with %s: %v", compType, err)
			continue
		}

		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Errorf("Failed to decompress with %s: %v", compType, err)
			continue
		}

		if !bytes.Equal(decompressed, data) {
			t.Errorf("Data mismatch with %s: expected %s, got %s", compType, data, decompressed)
		}
	}
}

func TestCompressorSmallData(t *testing.T) {
	// Small data should not be compressed (below MinPageSize)
	config := &CompressionConfig{
		Enabled:     true,
		Type:        CompressionFast,
		MinPageSize: 100,
	}

	compressor := NewCompressor(config)
	smallData := []byte("small")

	compressed, err := compressor.Compress(smallData)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	// Should be wrapped uncompressed
	if compressed[0] != byte(CompressionNone) {
		t.Error("Small data should not be compressed")
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	if !bytes.Equal(decompressed, smallData) {
		t.Error("Data mismatch")
	}
}

func TestCompressorDisabled(t *testing.T) {
	config := &CompressionConfig{
		Enabled:     false,
		Type:        CompressionFast,
		MinPageSize: 10,
	}

	compressor := NewCompressor(config)
	data := []byte("This is test data that would normally be compressed")

	compressed, err := compressor.Compress(data)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	// Should be wrapped uncompressed when disabled
	if compressed[0] != byte(CompressionNone) {
		t.Error("Disabled compression should not compress data")
	}
}

func TestCompressorDecompressError(t *testing.T) {
	compressor := NewCompressor(nil)

	// Too short
	_, err := compressor.Decompress([]byte{0x00})
	if err == nil {
		t.Error("Expected error for too short data")
	}

	// Unknown compression type
	data := make([]byte, 10)
	data[0] = byte(CompressionType(99))
	binary.BigEndian.PutUint32(data[1:5], 100)
	_, err = compressor.Decompress(data)
	if err == nil {
		t.Error("Expected error for unknown compression type")
	}
}

func TestPageCompressor(t *testing.T) {
	config := DefaultCompressionConfig()
	pc := NewPageCompressor(config, 4096)

	// Create a page-sized data
	page := make([]byte, 4096)
	for i := range page {
		page[i] = byte(i % 256)
	}

	compressed, err := pc.CompressPage(page)
	if err != nil {
		t.Fatalf("Failed to compress page: %v", err)
	}

	decompressed, err := pc.DecompressPage(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress page: %v", err)
	}

	if !bytes.Equal(decompressed, page) {
		t.Error("Page data mismatch")
	}

	// Test wrong page size
	wrongPage := make([]byte, 2048)
	_, err = pc.CompressPage(wrongPage)
	if err == nil {
		t.Error("Expected error for wrong page size")
	}
}

func TestWALCompressor(t *testing.T) {
	config := DefaultCompressionConfig()
	wc := NewWALCompressor(config)

	record := []byte("WAL record data that will be compressed")

	compressed, err := wc.CompressRecord(record)
	if err != nil {
		t.Fatalf("Failed to compress record: %v", err)
	}

	decompressed, err := wc.DecompressRecord(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress record: %v", err)
	}

	if !bytes.Equal(decompressed, record) {
		t.Error("Record data mismatch")
	}
}

func TestCompressionEstimator(t *testing.T) {
	estimator := NewCompressionEstimator()

	// Initial estimate should be 1.0 (no compression)
	if estimator.EstimateCompressionRatio() != 1.0 {
		t.Error("Initial estimate should be 1.0")
	}

	if !estimator.ShouldCompress() {
		t.Error("Initial estimator should allow compression")
	}

	// Add samples with 2:1 compression ratio
	for i := 0; i < 10; i++ {
		estimator.AddSample(1000, 500)
	}

	ratio := estimator.EstimateCompressionRatio()
	if ratio != 2.0 {
		t.Errorf("Expected ratio 2.0, got %f", ratio)
	}

	if !estimator.ShouldCompress() {
		t.Error("Should recommend compression with 2:1 ratio")
	}

	// Add samples with poor compression (1.05:1)
	for i := 0; i < 100; i++ {
		estimator.AddSample(1000, 950)
	}

	if estimator.ShouldCompress() {
		t.Error("Should not recommend compression with low ratio")
	}
}

func TestCompressionEstimatorMaxSamples(t *testing.T) {
	estimator := NewCompressionEstimator()
	estimator.maxSamples = 10

	for i := 0; i < 20; i++ {
		estimator.AddSample(1000, 500)
	}

	if len(estimator.samples) != 10 {
		t.Errorf("Expected 10 samples, got %d", len(estimator.samples))
	}
}

func TestCompressorStats(t *testing.T) {
	config := &CompressionConfig{
		Enabled:     true,
		Type:        CompressionFast,
		MinPageSize: 10,
	}

	compressor := NewCompressor(config)
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte('A' + (i % 26))
	}

	// Compress some data
	for i := 0; i < 5; i++ {
		compressed, err := compressor.Compress(data)
		if err != nil {
			t.Fatalf("Failed to compress: %v", err)
		}

		// Decompress to update decompression stats
		_, err = compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Failed to decompress: %v", err)
		}
	}

	stats := compressor.GetStats()
	if stats.PagesCompressed == 0 {
		t.Error("Expected some pages compressed")
	}
	if stats.PagesDecompressed == 0 {
		t.Error("Expected some pages decompressed")
	}
	if stats.BytesOriginal == 0 {
		t.Error("Expected bytes original to be tracked")
	}

	// Reset stats
	compressor.ResetStats()
	stats = compressor.GetStats()
	if stats.PagesCompressed != 0 {
		t.Error("Stats should be reset")
	}
}

func BenchmarkCompressGzip(b *testing.B) {
	config := &CompressionConfig{
		Enabled:     true,
		Type:        CompressionGzip,
		Level:       gzip.DefaultCompression,
		MinPageSize: 10,
	}
	compressor := NewCompressor(config)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := compressor.Compress(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecompressGzip(b *testing.B) {
	config := &CompressionConfig{
		Enabled:     true,
		Type:        CompressionGzip,
		Level:       gzip.DefaultCompression,
		MinPageSize: 10,
	}
	compressor := NewCompressor(config)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := compressor.Decompress(compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompressFast(b *testing.B) {
	config := &CompressionConfig{
		Enabled:     true,
		Type:        CompressionFast,
		MinPageSize: 10,
	}
	compressor := NewCompressor(config)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := compressor.Compress(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecompressFast(b *testing.B) {
	config := &CompressionConfig{
		Enabled:     true,
		Type:        CompressionFast,
		MinPageSize: 10,
	}
	compressor := NewCompressor(config)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := compressor.Decompress(compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
}
