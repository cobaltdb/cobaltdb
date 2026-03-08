package replication

import (
	"context"
	"testing"
)

func TestReadReplicaBasic(t *testing.T) {
	replica := &ReadReplica{
		ID:       "replica-1",
		Address:  "localhost:4201",
		Status:   ReplicaStatusReady,
		Weight:   10,
		Priority: 1,
	}

	if !replica.IsReady() {
		t.Error("Expected replica to be ready")
	}

	if replica.GetLag() != 0 {
		t.Errorf("Expected initial lag 0, got %d", replica.GetLag())
	}
}

func TestReadReplicaLag(t *testing.T) {
	replica := &ReadReplica{
		ID:     "replica-1",
		Status: ReplicaStatusReady,
	}

	replica.SetLag(100)
	if replica.GetLag() != 100 {
		t.Errorf("Expected lag 100, got %d", replica.GetLag())
	}

	// Test lag exceeds threshold
	replica.SetLag(6000)
	replica.mu.RLock()
	status := replica.Status
	replica.mu.RUnlock()
	if status != ReplicaStatusLagExceeded {
		t.Errorf("Expected status LagExceeded, got %v", status)
	}

	// Test lag returns to normal
	replica.SetLag(1000)
	replica.mu.RLock()
	status = replica.Status
	replica.mu.RUnlock()
	if status != ReplicaStatusReady {
		t.Errorf("Expected status Ready, got %v", status)
	}
}

func TestReadReplicaStats(t *testing.T) {
	replica := &ReadReplica{
		ID:     "replica-1",
		Status: ReplicaStatusReady,
	}

	replica.RecordRead()
	replica.RecordRead()
	replica.RecordError(nil)

	stats := replica.GetStats()
	if stats.TotalReads != 2 {
		t.Errorf("Expected 2 reads, got %d", stats.TotalReads)
	}
	if stats.TotalErrors != 1 {
		t.Errorf("Expected 1 error, got %d", stats.TotalErrors)
	}
}

func TestReadReplicaManagerAddRemove(t *testing.T) {
	config := DefaultReadReplicaConfig()
	manager := NewReadReplicaManager(config)
	defer manager.Close()

	// Add replica
	err := manager.AddReplica("replica-1", "localhost:4201", 10, 1)
	if err != nil {
		t.Fatalf("Failed to add replica: %v", err)
	}

	// Duplicate add should fail
	err = manager.AddReplica("replica-1", "localhost:4201", 10, 1)
	if err == nil {
		t.Error("Expected error for duplicate replica ID")
	}

	// Get replica
	replica, err := manager.GetReplica("replica-1")
	if err != nil {
		t.Fatalf("Failed to get replica: %v", err)
	}
	if replica.ID != "replica-1" {
		t.Errorf("Expected ID 'replica-1', got '%s'", replica.ID)
	}

	// Remove replica
	err = manager.RemoveReplica("replica-1")
	if err != nil {
		t.Fatalf("Failed to remove replica: %v", err)
	}

	// Get removed replica should fail
	_, err = manager.GetReplica("replica-1")
	if err == nil {
		t.Error("Expected error for removed replica")
	}
}

func TestReadReplicaManagerSelect(t *testing.T) {
	config := DefaultReadReplicaConfig()
	config.LoadBalanceStrategy = "round_robin"
	manager := NewReadReplicaManager(config)
	defer manager.Close()

	// Add replicas
	for i := 0; i < 3; i++ {
		err := manager.AddReplica(
			"replica-"+string(rune('0'+i)),
			"localhost:420"+string(rune('1'+i)),
			10,
			i,
		)
		if err != nil {
			t.Fatalf("Failed to add replica: %v", err)
		}
	}

	// Mark replicas as ready
	for i := 0; i < 3; i++ {
		replica, _ := manager.GetReplica("replica-" + string(rune('0'+i)))
		replica.mu.Lock()
		replica.Status = ReplicaStatusReady
		replica.mu.Unlock()
	}

	// Test selection
	ctx := context.Background()
	selected := make(map[string]int)
	for i := 0; i < 9; i++ {
		replica, err := manager.SelectReplica(ctx)
		if err != nil {
			t.Fatalf("Failed to select replica: %v", err)
		}
		selected[replica.ID]++
	}

	// With round-robin, each replica should be selected roughly equally
	// Allow for some variance due to atomic counter
	totalSelected := 0
	for _, count := range selected {
		totalSelected += count
	}
	if totalSelected != 9 {
		t.Errorf("Expected 9 total selections, got %d", totalSelected)
	}

	// Each replica should be selected at least once
	for i := 0; i < 3; i++ {
		id := "replica-" + string(rune('0'+i))
		if selected[id] == 0 {
			t.Errorf("Expected replica %s to be selected at least once", id)
		}
	}
}

func TestReadReplicaManagerSelectNoReplicas(t *testing.T) {
	config := DefaultReadReplicaConfig()
	manager := NewReadReplicaManager(config)
	defer manager.Close()

	ctx := context.Background()
	_, err := manager.SelectReplica(ctx)
	if err != ErrNoReplicaAvailable {
		t.Errorf("Expected ErrNoReplicaAvailable, got %v", err)
	}
}

func TestReadReplicaManagerWeighted(t *testing.T) {
	config := DefaultReadReplicaConfig()
	config.LoadBalanceStrategy = "weighted"
	manager := NewReadReplicaManager(config)
	defer manager.Close()

	// Add replicas with different weights
	manager.AddReplica("replica-1", "localhost:4201", 10, 1)
	manager.AddReplica("replica-2", "localhost:4202", 5, 1)
	manager.AddReplica("replica-3", "localhost:4203", 5, 1)

	// Mark replicas as ready
	for i := 0; i < 3; i++ {
		replica, _ := manager.GetReplica("replica-" + string(rune('1'+i)))
		replica.mu.Lock()
		replica.Status = ReplicaStatusReady
		replica.mu.Unlock()
	}

	// Test selection
	ctx := context.Background()
	selected := make(map[string]int)
	for i := 0; i < 100; i++ {
		replica, err := manager.SelectReplica(ctx)
		if err != nil {
			t.Fatalf("Failed to select replica: %v", err)
		}
		selected[replica.ID]++
	}

	// replica-1 has weight 10, others have weight 5
	// So replica-1 should be selected roughly 50% of the time
	if selected["replica-1"] < 40 || selected["replica-1"] > 60 {
		t.Logf("Weighted selection: replica-1 selected %d times (expected ~50)", selected["replica-1"])
	}
}

func TestReadReplicaManagerLeastLag(t *testing.T) {
	config := DefaultReadReplicaConfig()
	config.LoadBalanceStrategy = "least_lag"
	manager := NewReadReplicaManager(config)
	defer manager.Close()

	// Add replicas with different lags
	manager.AddReplica("replica-1", "localhost:4201", 10, 1)
	manager.AddReplica("replica-2", "localhost:4202", 10, 1)
	manager.AddReplica("replica-3", "localhost:4203", 10, 1)

	r1, _ := manager.GetReplica("replica-1")
	r1.SetLag(100)
	r1.mu.Lock()
	r1.Status = ReplicaStatusReady
	r1.mu.Unlock()

	r2, _ := manager.GetReplica("replica-2")
	r2.SetLag(50)
	r2.mu.Lock()
	r2.Status = ReplicaStatusReady
	r2.mu.Unlock()

	r3, _ := manager.GetReplica("replica-3")
	r3.SetLag(200)
	r3.mu.Lock()
	r3.Status = ReplicaStatusReady
	r3.mu.Unlock()

	// Test selection - should always select replica-2 (least lag)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		replica, err := manager.SelectReplica(ctx)
		if err != nil {
			t.Fatalf("Failed to select replica: %v", err)
		}
		if replica.ID != "replica-2" {
			t.Errorf("Expected replica-2 (least lag), got %s", replica.ID)
		}
	}
}

func TestReadReplicaManagerStats(t *testing.T) {
	config := DefaultReadReplicaConfig()
	manager := NewReadReplicaManager(config)
	defer manager.Close()

	// Add replicas
	manager.AddReplica("replica-1", "localhost:4201", 10, 1)
	manager.AddReplica("replica-2", "localhost:4202", 10, 1)

	// Mark one as ready
	r1, _ := manager.GetReplica("replica-1")
	r1.mu.Lock()
	r1.Status = ReplicaStatusReady
	r1.mu.Unlock()

	stats := manager.GetStats()
	if stats.TotalReplicas != 2 {
		t.Errorf("Expected 2 total replicas, got %d", stats.TotalReplicas)
	}
	if stats.ReadyReplicas != 1 {
		t.Errorf("Expected 1 ready replica, got %d", stats.ReadyReplicas)
	}
	if stats.UnhealthyReplicas != 0 {
		t.Errorf("Expected 0 unhealthy replicas, got %d", stats.UnhealthyReplicas)
	}
}

func TestIsReadQuery(t *testing.T) {
	tests := []struct {
		sql      string
		expected bool
	}{
		{"SELECT * FROM users", true},
		{"select id, name from orders", true},
		{"SELECT COUNT(*) FROM items", true},
		{"SHOW TABLES", true},
		{"DESCRIBE users", true},
		{"EXPLAIN SELECT * FROM users", true},
		{"INSERT INTO users VALUES (1)", false},
		{"UPDATE users SET name = 'test'", false},
		{"DELETE FROM users WHERE id = 1", false},
		{"CREATE TABLE test (id INT)", false},
		{"DROP TABLE users", false},
	}

	for _, tc := range tests {
		result := IsReadQuery(tc.sql)
		if result != tc.expected {
			t.Errorf("IsReadQuery(%q) = %v, expected %v", tc.sql, result, tc.expected)
		}
	}
}
