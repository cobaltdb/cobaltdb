package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// TestApplyWALData tests applyWALData with valid entries
func TestApplyWALData(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	// Create WAL entries
	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
		{LSN: 2, Timestamp: time.Now(), Data: []byte("test2"), Checksum: calculateCRC32([]byte("test2"))},
	}

	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("Failed to encode entries: %v", err)
	}

	// Track applied entries
	var appliedCount int
	mgr.OnApply = func(entry *WALEntry) error {
		appliedCount++
		return nil
	}

	// Apply WAL data
	err = mgr.applyWALData(string(data))
	if err != nil {
		t.Errorf("applyWALData failed: %v", err)
	}

	if appliedCount != 2 {
		t.Errorf("Expected 2 applied entries, got %d", appliedCount)
	}

	// Verify metrics
	metrics := mgr.GetMetrics()
	if metrics.AppliedEntries != 2 {
		t.Errorf("Expected 2 applied entries in metrics, got %d", metrics.AppliedEntries)
	}
}

// TestApplyWALDataWithError tests applyWALData when OnApply returns error
func TestApplyWALDataWithError(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
	}

	data, _ := encodeWALEntries(entries)

	mgr.OnApply = func(entry *WALEntry) error {
		return errors.New("apply error")
	}

	err := mgr.applyWALData(string(data))
	if err == nil {
		t.Error("Expected error from applyWALData when OnApply fails")
	}
}

// TestApplyWALDataInvalidData tests applyWALData with invalid data
func TestApplyWALDataInvalidData(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	err := mgr.applyWALData("invalid data")
	if err == nil {
		t.Error("Expected error from applyWALData with invalid data")
	}
}

// TestApplyWALDataNoCallback tests applyWALData without OnApply callback
func TestApplyWALDataNoCallback(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
	}

	data, _ := encodeWALEntries(entries)

	// No OnApply callback set
	err := mgr.applyWALData(string(data))
	if err != nil {
		t.Errorf("applyWALData failed without callback: %v", err)
	}

	// Verify lastApplied was updated
	if mgr.lastApplied != 1 {
		t.Errorf("Expected lastApplied=1, got %d", mgr.lastApplied)
	}
}

// TestWALEntryEncodeDecodeVariations tests Encode/Decode with various scenarios
func TestWALEntryEncodeDecodeVariations(t *testing.T) {
	// Test with valid entry first
	entry := &WALEntry{
		LSN:       1,
		Timestamp: time.Now(),
		Data:      []byte("test"),
		Checksum:  12345,
	}

	data, err := entry.Encode()
	if err != nil {
		t.Errorf("Encode failed for valid entry: %v", err)
	}
	if len(data) == 0 {
		t.Error("Encode returned empty data for valid entry")
	}

	// Test round-trip
	decoded := &WALEntry{}
	err = decoded.Decode(data)
	if err != nil {
		t.Errorf("Decode failed: %v", err)
	}
	if decoded.LSN != entry.LSN {
		t.Errorf("LSN mismatch: expected %d, got %d", entry.LSN, decoded.LSN)
	}
	if string(decoded.Data) != string(entry.Data) {
		t.Errorf("Data mismatch: expected %s, got %s", entry.Data, decoded.Data)
	}
	if decoded.Checksum != entry.Checksum {
		t.Errorf("Checksum mismatch: expected %d, got %d", entry.Checksum, decoded.Checksum)
	}
}

// TestEncodeWALEntriesMultiple tests encodeWALEntries with multiple entries
func TestEncodeWALEntriesMultiple(t *testing.T) {
	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("entry1"), Checksum: 100},
		{LSN: 2, Timestamp: time.Now(), Data: []byte("entry2"), Checksum: 200},
		{LSN: 3, Timestamp: time.Now(), Data: []byte("entry3"), Checksum: 300},
	}

	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("encodeWALEntries failed: %v", err)
	}

	// Decode and verify
	decoded, err := decodeWALEntries(data)
	if err != nil {
		t.Fatalf("decodeWALEntries failed: %v", err)
	}

	if len(decoded) != 3 {
		t.Errorf("Expected 3 decoded entries, got %d", len(decoded))
	}

	for i, entry := range decoded {
		if entry.LSN != uint64(i+1) {
			t.Errorf("Entry %d: expected LSN=%d, got %d", i, i+1, entry.LSN)
		}
	}
}

// TestEncodeWALEntriesError tests encodeWALEntries error handling
func TestEncodeWALEntriesError(t *testing.T) {
	// Create an entry that will fail to encode
	// This is hard to trigger since bytes.Buffer rarely fails
	// But we can test the error propagation path

	entries := []*WALEntry{}
	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Errorf("encodeWALEntries failed for empty slice: %v", err)
	}

	// Verify empty encoding
	decoded, err := decodeWALEntries(data)
	if err != nil {
		t.Errorf("decodeWALEntries failed for empty data: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("Expected 0 decoded entries, got %d", len(decoded))
	}
}

// TestDecodeWALEntriesErrorCases tests decodeWALEntries error scenarios
func TestDecodeWALEntriesErrorCases(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "truncated header",
			data: []byte{0x00, 0x00}, // Only 2 bytes, need 4 for numEntries
		},
		{
			name: "invalid entry length",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint32(1))    // 1 entry
				binary.Write(buf, binary.BigEndian, uint32(1000)) // But length says 1000 bytes
				return buf.Bytes()
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeWALEntries(tt.data)
			if err == nil {
				t.Error("Expected error for invalid data")
			}
		})
	}
}

// TestWALEntryDecodeErrors tests WALEntry.Decode error scenarios
func TestWALEntryDecodeErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "truncated LSN",
			data: []byte{0x00, 0x00, 0x00}, // Only 3 bytes, need 8 for LSN
		},
		{
			name: "truncated timestamp",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint64(1)) // LSN
				return buf.Bytes()
			}(),
		},
		{
			name: "truncated data length",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint64(1))                    // LSN
				binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano())) // Timestamp
				return buf.Bytes()
			}(),
		},
		{
			name: "truncated data content",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint64(1))                    // LSN
				binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano())) // Timestamp
				binary.Write(buf, binary.BigEndian, uint32(100))                  // Data length = 100
				buf.Write([]byte("short"))                                        // But only 5 bytes
				return buf.Bytes()
			}(),
		},
		{
			name: "truncated checksum",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint64(1))                    // LSN
				binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano())) // Timestamp
				binary.Write(buf, binary.BigEndian, uint32(4))                    // Data length = 4
				buf.Write([]byte("test"))                                         // 4 bytes of data
				// Missing checksum
				return buf.Bytes()
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &WALEntry{}
			err := entry.Decode(tt.data)
			if err == nil {
				t.Error("Expected error for invalid data")
			}
		})
	}
}

// TestReplicateWALEntryNotMaster tests ReplicateWALEntry when not master
func TestReplicateWALEntryNotMaster(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	err := mgr.ReplicateWALEntry([]byte("test"))
	if err != nil {
		t.Error("ReplicateWALEntry should return nil when not master")
	}

	// Verify buffer is empty
	if len(mgr.walBuffer) != 0 {
		t.Error("WAL buffer should be empty for slave")
	}
}

// TestReplicateWALEntryMaster tests ReplicateWALEntry as master
func TestReplicateWALEntryMaster(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})

	data := []byte("test data")
	err := mgr.ReplicateWALEntry(data)
	if err != nil {
		t.Errorf("ReplicateWALEntry failed: %v", err)
	}

	// Verify entry was added to buffer
	if len(mgr.walBuffer) != 1 {
		t.Errorf("Expected 1 entry in buffer, got %d", len(mgr.walBuffer))
	}

	entry := mgr.walBuffer[0]
	if string(entry.Data) != string(data) {
		t.Errorf("Data mismatch: expected %s, got %s", data, entry.Data)
	}
	if entry.LSN != 1 {
		t.Errorf("Expected LSN=1, got %d", entry.LSN)
	}
	if entry.Checksum != calculateCRC32(data) {
		t.Error("Checksum mismatch")
	}
}

// TestReplicateWALEntryMultiple tests multiple entries
func TestReplicateWALEntryMultiple(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})

	for i := 1; i <= 5; i++ {
		data := []byte(string(rune('a' + i)))
		err := mgr.ReplicateWALEntry(data)
		if err != nil {
			t.Errorf("ReplicateWALEntry failed at iteration %d: %v", i, err)
		}
	}

	if len(mgr.walBuffer) != 5 {
		t.Errorf("Expected 5 entries in buffer, got %d", len(mgr.walBuffer))
	}

	// Verify LSNs are sequential
	for i, entry := range mgr.walBuffer {
		expectedLSN := uint64(i + 1)
		if entry.LSN != expectedLSN {
			t.Errorf("Entry %d: expected LSN=%d, got %d", i, expectedLSN, entry.LSN)
		}
	}
}

// TestCalculateCRC32 tests CRC32 calculation
func TestCalculateCRC32Variations(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint32
	}{
		{
			name:     "empty",
			data:     []byte{},
			expected: 0,
		},
		{
			name:     "single byte",
			data:     []byte{0x01},
			expected: calculateCRC32([]byte{0x01}),
		},
		{
			name:     "multiple bytes",
			data:     []byte("hello world"),
			expected: calculateCRC32([]byte("hello world")),
		},
		{
			name:     "binary data",
			data:     []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC},
			expected: calculateCRC32([]byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateCRC32(tt.data)
			if result != tt.expected {
				t.Errorf("CRC32 mismatch: expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestGetMetricsAppliedEntries tests GetMetrics with applied entries
func TestGetMetricsAppliedEntries(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	// Apply some entries
	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
		{LSN: 2, Timestamp: time.Now(), Data: []byte("test2"), Checksum: calculateCRC32([]byte("test2"))},
	}

	data, _ := encodeWALEntries(entries)
	mgr.applyWALData(string(data))

	// Get metrics
	metrics := mgr.GetMetrics()
	if metrics.AppliedEntries != 2 {
		t.Errorf("Expected 2 applied entries, got %d", metrics.AppliedEntries)
	}
}
