package replication

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

func TestWALEntryEncodeDecode(t *testing.T) {
	entry := &WALEntry{
		LSN:       12345,
		Timestamp: time.Now(),
		Data:      []byte("test data for WAL entry"),
		Checksum:  calculateCRC32([]byte("test data for WAL entry")),
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Failed to encode WAL entry: %v", err)
	}

	decoded := &WALEntry{}
	if err := decoded.Decode(encoded); err != nil {
		t.Fatalf("Failed to decode WAL entry: %v", err)
	}

	if decoded.LSN != entry.LSN {
		t.Errorf("LSN mismatch: got %d, want %d", decoded.LSN, entry.LSN)
	}

	if string(decoded.Data) != string(entry.Data) {
		t.Errorf("Data mismatch: got %s, want %s", decoded.Data, entry.Data)
	}

	if decoded.Checksum != entry.Checksum {
		t.Errorf("Checksum mismatch: got %d, want %d", decoded.Checksum, entry.Checksum)
	}
}

func TestWALEntryDecodeRejectsTrailingData(t *testing.T) {
	entry := &WALEntry{
		LSN:       12345,
		Timestamp: time.Now(),
		Data:      []byte("test data for WAL entry"),
		Checksum:  calculateCRC32([]byte("test data for WAL entry")),
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	encoded = append(encoded, 0xde, 0xad, 0xbe, 0xef)

	var decoded WALEntry
	err = decoded.Decode(encoded)
	if err == nil {
		t.Fatal("expected trailing WAL entry data to be rejected")
	}
	if !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("expected trailing data error, got %v", err)
	}
}

func TestWALEntryEncodeRejectsOversizedData(t *testing.T) {
	entry := &WALEntry{
		LSN:       1,
		Timestamp: time.Now(),
		Data:      make([]byte, maxWALEntryDataBytes+1),
	}

	_, err := entry.Encode()
	if err == nil {
		t.Fatal("expected oversized WAL entry data to be rejected")
	}
	if !strings.Contains(err.Error(), "WAL entry data too large") {
		t.Fatalf("expected WAL entry size error, got %v", err)
	}
}

func TestWALEntryDecodeRejectsOversizedDataBeforeAllocation(t *testing.T) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, uint64(1)); err != nil {
		t.Fatalf("write LSN: %v", err)
	}
	if err := binary.Write(buf, binary.BigEndian, time.Now().UnixNano()); err != nil {
		t.Fatalf("write timestamp: %v", err)
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(maxWALEntryDataBytes+1)); err != nil {
		t.Fatalf("write data length: %v", err)
	}
	buf.Write(make([]byte, 4))

	var decoded WALEntry
	err := decoded.Decode(buf.Bytes())
	if err == nil {
		t.Fatal("expected oversized WAL entry data to be rejected")
	}
	if !strings.Contains(err.Error(), "WAL entry data too large") {
		t.Fatalf("expected WAL entry size error, got %v", err)
	}
}

func TestDecodeWALEntriesRejectsTrailingPayload(t *testing.T) {
	entry := &WALEntry{
		LSN:       12345,
		Timestamp: time.Now(),
		Data:      []byte("replicated payload"),
		Checksum:  calculateCRC32([]byte("replicated payload")),
	}

	encoded, err := encodeWALEntries([]*WALEntry{entry})
	if err != nil {
		t.Fatalf("encodeWALEntries: %v", err)
	}
	encoded = append(encoded, 0xde, 0xad, 0xbe, 0xef)

	_, err = decodeWALEntries(encoded)
	if err == nil {
		t.Fatal("expected trailing WAL entries payload to be rejected")
	}
	if !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("expected trailing data error, got %v", err)
	}
}

func TestManagerCreation(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.ListenAddr = "127.0.0.1:0"

	mgr := NewManager(config)
	if mgr == nil {
		t.Fatal("Failed to create manager")
	}

	if mgr.config.Role != RoleMaster {
		t.Errorf("Role mismatch: got %v, want %v", mgr.config.Role, RoleMaster)
	}
}

func TestReplicationListenAddressRequiresAuth(t *testing.T) {
	tests := []struct {
		address string
		want    bool
	}{
		{"127.0.0.1:0", false},
		{"[::1]:0", false},
		{"localhost:0", false},
		{":0", true},
		{"0.0.0.0:0", true},
		{"[::]:0", true},
		{"192.0.2.10:9000", true},
		{"replica.example.com:9000", true},
		{"invalid:address:format", false},
	}

	for _, tt := range tests {
		t.Run(tt.address, func(t *testing.T) {
			if got := replicationListenAddressRequiresAuth(tt.address); got != tt.want {
				t.Fatalf("replicationListenAddressRequiresAuth(%q) = %v, want %v", tt.address, got, tt.want)
			}
		})
	}
}

func TestStartMasterRejectsUnauthenticatedNonLoopbackListener(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.ListenAddr = "0.0.0.0:0"

	mgr := NewManager(config)
	err := mgr.startMaster()
	if err == nil {
		_ = mgr.Stop()
		t.Fatal("expected unauthenticated non-loopback listener to be rejected")
	}
	if !strings.Contains(err.Error(), "auth token is required") {
		t.Fatalf("expected auth token error, got %v", err)
	}
}

func TestReplicationAuthTokenEqual(t *testing.T) {
	if !replicationAuthTokenEqual("secret-token", "secret-token") {
		t.Fatal("matching tokens should authenticate")
	}

	tests := []struct {
		name     string
		provided string
		expected string
	}{
		{name: "different content", provided: "secret-tokem", expected: "secret-token"},
		{name: "shorter provided", provided: "secret", expected: "secret-token"},
		{name: "longer provided", provided: "secret-token-extra", expected: "secret-token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if replicationAuthTokenEqual(tt.provided, tt.expected) {
				t.Fatalf("tokens %q and %q should not authenticate", tt.provided, tt.expected)
			}
		})
	}
}

func TestReplicationAsyncMode(t *testing.T) {
	// Create master
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.Mode = ModeAsync

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	// Get actual address
	addr := master.listener.Addr().String()

	// Create slave
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}
	defer slave.Stop()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Verify slave connected
	metrics := master.GetMetrics()
	if metrics.ActiveSlaves != 1 {
		t.Errorf("Expected 1 active slave, got %d", metrics.ActiveSlaves)
	}
}

func TestReplicateWALEntry(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	entry := []byte("INSERT INTO test VALUES (1, 'data')")
	if err := mgr.ReplicateWALEntry(entry); err != nil {
		t.Fatalf("Failed to replicate WAL entry: %v", err)
	}

	if len(mgr.walBuffer) != 1 {
		t.Errorf("Expected 1 entry in WAL buffer, got %d", len(mgr.walBuffer))
	}

	if mgr.currentLSN != 1 {
		t.Errorf("Expected LSN 1, got %d", mgr.currentLSN)
	}
}

func TestGetStatus(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	status := mgr.GetStatus()
	if status.Role != "master" {
		t.Errorf("Expected role 'master', got %s", status.Role)
	}
}

func TestReplicationModeString(t *testing.T) {
	tests := []struct {
		mode     ReplicationMode
		expected string
	}{
		{ModeAsync, "async"},
		{ModeSync, "sync"},
		{ModeFullSync, "full_sync"},
		{ReplicationMode(999), "unknown"},
	}

	for _, test := range tests {
		result := replicationModeString(test.mode)
		if result != test.expected {
			t.Errorf("replicationModeString(%v) = %s, want %s", test.mode, result, test.expected)
		}
	}
}
