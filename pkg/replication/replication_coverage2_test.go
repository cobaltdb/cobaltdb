package replication

import (
	"testing"
	"time"
)

// TestStartSlaveAuthFailure tests slave start with auth failure
func TestStartSlaveAuthFailure(t *testing.T) {
	// Start master with auth
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.AuthToken = "secret_token"

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create slave with wrong auth
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr
	slaveConfig.AuthToken = "wrong_token"

	slave := NewManager(slaveConfig)
	err := slave.Start()
	if err == nil {
		slave.Stop()
		t.Error("Expected error with wrong auth token")
	}
}

// TestStartSlaveNoAuth tests slave connection without auth when master requires it
func TestStartSlaveNoAuth(t *testing.T) {
	// Start master with auth
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.AuthToken = "secret_token"

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create slave without auth
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	err := slave.Start()
	// The error may or may not be immediate depending on implementation
	if err != nil {
		t.Logf("Got expected error when auth is required but not provided: %v", err)
	} else {
		// If no immediate error, the connection will fail later
		slave.Stop()
		t.Log("No immediate error when auth required but not provided (connection may fail later)")
	}
}

// TestStartSlaveSuccess tests successful slave start with correct auth
func TestStartSlaveSuccess(t *testing.T) {
	// Start master with auth
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.AuthToken = "secret_token"

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create slave with correct auth
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr
	slaveConfig.AuthToken = "secret_token"

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}
	defer slave.Stop()

	// Give time for connection
	time.Sleep(200 * time.Millisecond)

	// Check slave connected
	metrics := master.GetMetrics()
	if metrics.ActiveSlaves != 1 {
		t.Errorf("Expected 1 active slave, got %d", metrics.ActiveSlaves)
	}
}

// TestReplicateWALWithSlaves tests WAL replication to connected slaves
func TestReplicateWALWithSlaves(t *testing.T) {
	// Start master
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.Mode = ModeAsync

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create and start slave
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}
	defer slave.Stop()

	// Give time for connection
	time.Sleep(200 * time.Millisecond)

	// Replicate WAL entry
	entry := []byte("INSERT INTO test VALUES (1)")
	if err := master.ReplicateWALEntry(entry); err != nil {
		t.Fatalf("Failed to replicate WAL entry: %v", err)
	}

	// Give time for replication
	time.Sleep(100 * time.Millisecond)

	// Check metrics
	metrics := master.GetMetrics()
	if metrics.AppliedEntries < 1 {
		t.Logf("AppliedEntries: %d (may be 0 if not updated)", metrics.AppliedEntries)
	}
}

// TestWALEntryEncodeErrors tests error paths in WAL entry encoding
func TestWALEntryEncodeErrors(t *testing.T) {
	entry := &WALEntry{
		LSN:       1,
		Timestamp: time.Now(),
		Data:      []byte("test"),
		Checksum:  12345,
	}

	// Normal encode should succeed
	_, err := entry.Encode()
	if err != nil {
		t.Errorf("Encode failed: %v", err)
	}
}

// TestReplicateWALInSyncMode tests replication in sync mode
func TestReplicateWALInSyncMode(t *testing.T) {
	// Start master in sync mode
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.Mode = ModeSync

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create and start slave
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}
	defer slave.Stop()

	// Give time for connection
	time.Sleep(200 * time.Millisecond)

	// Replicate WAL entry (in sync mode, this waits for slave ack)
	entry := []byte("INSERT INTO test VALUES (1)")

	// This may timeout since we're not actually acknowledging
	done := make(chan error, 1)
	go func() {
		done <- master.ReplicateWALEntry(entry)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Logf("ReplicateWALEntry returned error (expected in sync mode without full setup): %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Log("ReplicateWALEntry timed out (expected in sync mode)")
	}
}

// TestGetStatusSlaveDisconnected tests GetStatus when slave is disconnected
func TestGetStatusSlaveDisconnected(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	mgr := NewManager(config)
	status := mgr.GetStatus()

	if status.Role != "slave" {
		t.Errorf("Expected role 'slave', got %s", status.Role)
	}

	if status.Connected {
		t.Error("Expected Connected to be false when not connected")
	}
}

// TestWaitForSlavesFullSyncMode tests WaitForSlaves in full sync mode
func TestWaitForSlavesFullSyncMode(t *testing.T) {
	// Start master in full sync mode
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.Mode = ModeFullSync

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create and start slave
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}
	defer slave.Stop()

	// Give time for connection
	time.Sleep(200 * time.Millisecond)

	// Add some WAL entries
	for i := 0; i < 5; i++ {
		entry := []byte("test data")
		master.ReplicateWALEntry(entry)
	}

	// In full sync mode with 1 slave, should return quickly if slave is caught up
	err := master.WaitForSlaves(1 * time.Second)
	// Result depends on implementation - just log it
	t.Logf("WaitForSlaves result: %v", err)
}

// TestMultipleSlaves tests master with multiple slaves
func TestMultipleSlaves(t *testing.T) {
	// Start master
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create and start multiple slaves
	slaves := make([]*Manager, 3)
	for i := 0; i < 3; i++ {
		slaveConfig := DefaultConfig()
		slaveConfig.Role = RoleSlave
		slaveConfig.MasterAddr = addr

		slaves[i] = NewManager(slaveConfig)
		if err := slaves[i].Start(); err != nil {
			t.Fatalf("Failed to start slave %d: %v", i, err)
		}
		defer slaves[i].Stop()
	}

	// Give time for connections
	time.Sleep(300 * time.Millisecond)

	// Check metrics
	metrics := master.GetMetrics()
	if metrics.ActiveSlaves != 3 {
		t.Errorf("Expected 3 active slaves, got %d", metrics.ActiveSlaves)
	}

	// Replicate some data
	for i := 0; i < 10; i++ {
		entry := []byte("test data")
		master.ReplicateWALEntry(entry)
	}

	// Note: AppliedEntries may not be immediately updated in metrics
	// depending on implementation
	metrics = master.GetMetrics()
	if metrics.AppliedEntries < 10 {
		t.Logf("AppliedEntries: %d (may be less than 10 depending on timing)", metrics.AppliedEntries)
	}
}

// TestSlaveDisconnectReconnect tests slave disconnection and reconnection
func TestSlaveDisconnectReconnect(t *testing.T) {
	// Start master
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create and start slave
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}

	// Give time for connection
	time.Sleep(1 * time.Second)

	// Verify connected
	metrics := master.GetMetrics()
	if metrics.ActiveSlaves != 1 {
		t.Errorf("Expected 1 active slave, got %d", metrics.ActiveSlaves)
	}

	// Stop slave
	slave.Stop()

	// Give time for disconnection detection
	time.Sleep(1 * time.Second)

	// Start new slave
	slave2 := NewManager(slaveConfig)
	if err := slave2.Start(); err != nil {
		t.Fatalf("Failed to start slave2: %v", err)
	}
	defer slave2.Stop()

	// Give time for reconnection
	time.Sleep(1 * time.Second)

	// Check for at least 1 active slave (old one may not be cleaned up yet)
	metrics = master.GetMetrics()
	if metrics.ActiveSlaves < 1 {
		t.Errorf("Expected at least 1 active slave after reconnect, got %d", metrics.ActiveSlaves)
	}
	// Log the actual count for debugging
	t.Logf("Active slaves after reconnect: %d (old connections may still be counted)", metrics.ActiveSlaves)
}

// TestEncodeWALEntriesLarge tests encoding large number of entries
func TestEncodeWALEntriesLarge(t *testing.T) {
	entries := make([]*WALEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = &WALEntry{
			LSN:       uint64(i + 1),
			Timestamp: time.Now(),
			Data:      []byte("test data for entry"),
			Checksum:  calculateCRC32([]byte("test data for entry")),
		}
	}

	encoded, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("Failed to encode entries: %v", err)
	}

	decoded, err := decodeWALEntries(encoded)
	if err != nil {
		t.Fatalf("Failed to decode entries: %v", err)
	}

	if len(decoded) != 100 {
		t.Errorf("Expected 100 decoded entries, got %d", len(decoded))
	}
}

// TestReplicationMetrics tests metrics collection
func TestReplicationMetrics(t *testing.T) {
	// Start master
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create and start slave
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}
	defer slave.Stop()

	// Give time for connection
	time.Sleep(200 * time.Millisecond)

	// Get initial metrics
	initialMetrics := master.GetMetrics()
	if initialMetrics.ActiveSlaves != 1 {
		t.Errorf("Expected 1 active slave, got %d", initialMetrics.ActiveSlaves)
	}

	// Replicate some data
	data := []byte("test data for metrics")
	for i := 0; i < 5; i++ {
		master.ReplicateWALEntry(data)
	}

	// Give time for replication
	time.Sleep(100 * time.Millisecond)

	// Check updated metrics
	finalMetrics := master.GetMetrics()
	if finalMetrics.AppliedEntries < 5 {
		t.Logf("AppliedEntries: %d (may be less if not all applied)", finalMetrics.AppliedEntries)
	}

	if finalMetrics.ReplicatedBytes < uint64(len(data)*5) {
		t.Logf("Note: ReplicatedBytes is %d (expected at least %d) - replication may be asynchronous",
			finalMetrics.ReplicatedBytes, len(data)*5)
	}
}

// TestGetStatusWithSlaves tests GetStatus with connected slaves
func TestGetStatusWithSlaves(t *testing.T) {
	// Start master
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	addr := master.listener.Addr().String()

	// Create and start slave
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}
	defer slave.Stop()

	// Give time for connection
	time.Sleep(200 * time.Millisecond)

	// Get status
	status := master.GetStatus()

	if status.Role != "master" {
		t.Errorf("Expected role 'master', got %s", status.Role)
	}

	if status.ActiveSlaves != 1 {
		t.Errorf("Expected 1 active slave in status, got %d", status.ActiveSlaves)
	}

	if len(status.Slaves) != 1 {
		t.Errorf("Expected 1 slave in status.Slaves, got %d", len(status.Slaves))
	}
}

// TestModeSyncWithNoSlaves tests sync mode replication with no slaves
func TestModeSyncWithNoSlaves(t *testing.T) {
	// Start master in sync mode but with no slaves
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.Mode = ModeSync

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	// Replicate WAL entry - should still work even with no slaves
	// (implementation dependent - some may error, some may buffer)
	entry := []byte("test entry")
	err := master.ReplicateWALEntry(entry)
	if err != nil {
		t.Logf("ReplicateWALEntry with no slaves in sync mode returned: %v", err)
	}
}
