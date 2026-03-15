package replication

import (
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
