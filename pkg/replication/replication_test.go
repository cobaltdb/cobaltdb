package replication

import (
	"fmt"
	"os"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Role != RoleStandalone {
		t.Errorf("Expected RoleStandalone, got %d", config.Role)
	}
	if config.ReplicationPort != ":4201" {
		t.Errorf("Expected port :4201, got %s", config.ReplicationPort)
	}
	if config.BufferSize != 1000 {
		t.Errorf("Expected buffer size 1000, got %d", config.BufferSize)
	}
}

func TestNewManager(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	// Create a temporary WAL file
	walPath := fmt.Sprintf("test_wal_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	if manager == nil {
		t.Fatal("Manager is nil")
	}

	if manager.role != RoleMaster {
		t.Errorf("Expected role Master, got %d", manager.role)
	}
}

func TestManagerStartStopStandalone(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleStandalone

	walPath := fmt.Sprintf("test_wal_standalone_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Start should succeed immediately for standalone
	if err := manager.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Stop should succeed
	if err := manager.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if manager.GetState() != StateDisconnected {
		t.Errorf("Expected state Disconnected, got %d", manager.GetState())
	}
}

func TestGetSlaveCount(t *testing.T) {
	config := DefaultConfig()

	walPath := fmt.Sprintf("test_wal_count_%d.log", os.Getpid())
	wal, _ := storage.OpenWAL(walPath)
	defer wal.Close()
	defer os.Remove(walPath)

	// Standalone should have 0 slaves
	manager := NewManager(config, wal)
	if manager.GetSlaveCount() != 0 {
		t.Errorf("Expected 0 slaves for standalone, got %d", manager.GetSlaveCount())
	}

	// Master should have 0 slaves initially
	config.Role = RoleMaster
	manager2 := NewManager(config, wal)
	if manager2.GetSlaveCount() != 0 {
		t.Errorf("Expected 0 slaves for new master, got %d", manager2.GetSlaveCount())
	}
}

func TestPromote(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = "localhost:4201"

	walPath := fmt.Sprintf("test_wal_promote_%d.log", os.Getpid())
	wal, _ := storage.OpenWAL(walPath)
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Promoting a slave that isn't started should fail gracefully
	// (it will try to stop, which should be fine)
	_ = manager.Promote()
	// This may fail because we can't start the master listener
	// but the role should be changed
	if manager.role != RoleMaster {
		t.Errorf("Expected role to be Master after promote, got %d", manager.role)
	}
}

func TestReplicationStateTransitions(t *testing.T) {
	config := DefaultConfig()

	walPath := fmt.Sprintf("test_wal_state_%d.log", os.Getpid())
	wal, _ := storage.OpenWAL(walPath)
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Initial state
	if manager.GetState() != StateDisconnected {
		t.Errorf("Expected initial state Disconnected, got %d", manager.GetState())
	}

	// Set state directly
	manager.setState(StateConnecting)
	if manager.GetState() != StateConnecting {
		t.Errorf("Expected state Connecting, got %d", manager.GetState())
	}

	manager.setState(StateReplicating)
	if manager.GetState() != StateReplicating {
		t.Errorf("Expected state Replicating, got %d", manager.GetState())
	}

	manager.setState(StateError)
	if manager.GetState() != StateError {
		t.Errorf("Expected state Error, got %d", manager.GetState())
	}
}

func TestReplicationEvent(t *testing.T) {
	event := &ReplicationEvent{
		LSN:  1,
		Type: storage.WALInsert,
		Data: []byte("test data"),
	}

	if event.LSN != 1 {
		t.Errorf("Expected LSN 1, got %d", event.LSN)
	}
	if event.Type != storage.WALInsert {
		t.Errorf("Expected type WALInsert, got %d", event.Type)
	}
	if string(event.Data) != "test data" {
		t.Errorf("Expected data 'test data', got %s", string(event.Data))
	}
}
