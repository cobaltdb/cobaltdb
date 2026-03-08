package cluster

import (
	"testing"
	"time"
)

func TestKeyRangeContains(t *testing.T) {
	tests := []struct {
		name     string
		range_   KeyRange
		key      uint64
		expected bool
	}{
		{
			name:     "key in range",
			range_:   KeyRange{Start: 100, End: 200},
			key:      150,
			expected: true,
		},
		{
			name:     "key at start",
			range_:   KeyRange{Start: 100, End: 200},
			key:      100,
			expected: true,
		},
		{
			name:     "key at end",
			range_:   KeyRange{Start: 100, End: 200},
			key:      200,
			expected: true,
		},
		{
			name:     "key below range",
			range_:   KeyRange{Start: 100, End: 200},
			key:      50,
			expected: false,
		},
		{
			name:     "key above range",
			range_:   KeyRange{Start: 100, End: 200},
			key:      250,
			expected: false,
		},
		{
			name:     "wraparound range contains",
			range_:   KeyRange{Start: 200, End: 100},
			key:      250,
			expected: true,
		},
		{
			name:     "wraparound range contains other side",
			range_:   KeyRange{Start: 200, End: 100},
			key:      50,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.range_.Contains(tt.key)
			if result != tt.expected {
				t.Errorf("Contains(%d) = %v, want %v", tt.key, result, tt.expected)
			}
		})
	}
}

func TestDefaultShardHash(t *testing.T) {
	key := []byte("test-key")
	hash1 := DefaultShardHash(key)
	hash2 := DefaultShardHash(key)

	if hash1 != hash2 {
		t.Errorf("Hash should be deterministic: %d != %d", hash1, hash2)
	}

	// Different keys should (usually) have different hashes
	key2 := []byte("different-key")
	hash3 := DefaultShardHash(key2)

	if hash1 == hash3 {
		t.Log("Warning: hash collision detected (rare but possible)")
	}
}

func TestConsistentShardHash(t *testing.T) {
	key := []byte("test-key")
	hash1 := ConsistentShardHash(key)
	hash2 := ConsistentShardHash(key)

	if hash1 != hash2 {
		t.Errorf("Hash should be deterministic: %d != %d", hash1, hash2)
	}
}

func TestShardRouter(t *testing.T) {
	router := NewShardRouter(DefaultShardHash)

	// Add shards
	shards := []*Shard{
		{
			ID:       "shard-1",
			NodeID:   "node-1",
			KeyRange: KeyRange{Start: 0, End: (1 << 63) - 1},
			Status:   ShardStatusActive,
		},
		{
			ID:       "shard-2",
			NodeID:   "node-2",
			KeyRange: KeyRange{Start: 1 << 63, End: ^uint64(0)},
			Status:   ShardStatusActive,
		},
	}

	for _, shard := range shards {
		if err := router.AddShard(shard); err != nil {
			t.Fatalf("Failed to add shard: %v", err)
		}
	}

	// Test getting shard for key
	key := []byte("test-key")
	shard, err := router.GetShardForKey(key)
	if err != nil {
		t.Fatalf("Failed to get shard for key: %v", err)
	}

	if shard == nil {
		t.Fatal("Expected non-nil shard")
	}

	// Test getting all shards
	allShards := router.GetAllShards()
	if len(allShards) != 2 {
		t.Errorf("Expected 2 shards, got %d", len(allShards))
	}

	// Test getting shard by ID
	shardByID, err := router.GetShardByID("shard-1")
	if err != nil {
		t.Fatalf("Failed to get shard by ID: %v", err)
	}

	if shardByID.ID != "shard-1" {
		t.Errorf("Expected shard-1, got %s", shardByID.ID)
	}

	// Test removing shard
	if err := router.RemoveShard("shard-1"); err != nil {
		t.Fatalf("Failed to remove shard: %v", err)
	}

	_, err = router.GetShardByID("shard-1")
	if err != ErrShardNotFound {
		t.Error("Expected ErrShardNotFound after removal")
	}
}

func TestShardRouterDuplicate(t *testing.T) {
	router := NewShardRouter(DefaultShardHash)

	shard := &Shard{
		ID:       "shard-1",
		NodeID:   "node-1",
		KeyRange: KeyRange{Start: 0, End: 100},
		Status:   ShardStatusActive,
	}

	if err := router.AddShard(shard); err != nil {
		t.Fatalf("Failed to add shard: %v", err)
	}

	// Try to add duplicate
	if err := router.AddShard(shard); err == nil {
		t.Error("Expected error for duplicate shard")
	}
}

func TestShardRouterEmpty(t *testing.T) {
	router := NewShardRouter(DefaultShardHash)

	_, err := router.GetShardForKey([]byte("test"))
	if err != ErrShardNotFound {
		t.Errorf("Expected ErrShardNotFound, got %v", err)
	}
}

func TestShardManager(t *testing.T) {
	config := DefaultShardConfig()
	manager := NewShardManager(config)

	nodes := []*Node{
		{ID: "node-1", Address: "localhost:4201", Status: NodeStatusActive, Weight: 1},
		{ID: "node-2", Address: "localhost:4202", Status: NodeStatusActive, Weight: 1},
	}

	// Create shards
	if err := manager.CreateShards(4, nodes); err != nil {
		t.Fatalf("Failed to create shards: %v", err)
	}

	// Test getting all shards
	shards := manager.GetAllShards()
	if len(shards) != 4 {
		t.Errorf("Expected 4 shards, got %d", len(shards))
	}

	// Test getting shard for key
	key := []byte("test-key")
	shard, err := manager.GetShardForKey(key)
	if err != nil {
		t.Fatalf("Failed to get shard for key: %v", err)
	}

	if shard == nil {
		t.Fatal("Expected non-nil shard")
	}

	// Test updating shard status
	if err := manager.UpdateShardStatus(shard.ID, ShardStatusReadOnly); err != nil {
		t.Fatalf("Failed to update shard status: %v", err)
	}

	updatedShard, _ := manager.GetShardByID(shard.ID)
	if updatedShard.Status != ShardStatusReadOnly {
		t.Errorf("Expected status read_only, got %s", updatedShard.Status.String())
	}
}

func TestShardManagerNoNodes(t *testing.T) {
	config := DefaultShardConfig()
	manager := NewShardManager(config)

	err := manager.CreateShards(4, nil)
	if err == nil {
		t.Error("Expected error when creating shards with no nodes")
	}
}

func TestNode(t *testing.T) {
	node := &Node{
		ID:      "node-1",
		Address: "localhost:4201",
		Status:  NodeStatusActive,
		Weight:  1,
	}

	if !node.IsAvailable() {
		t.Error("Expected node to be available")
	}

	// Add shards
	node.AddShard("shard-1")
	node.AddShard("shard-2")

	if len(node.ShardIDs) != 2 {
		t.Errorf("Expected 2 shards, got %d", len(node.ShardIDs))
	}

	// Remove shard
	node.RemoveShard("shard-1")

	if len(node.ShardIDs) != 1 {
		t.Errorf("Expected 1 shard, got %d", len(node.ShardIDs))
	}

	// Update status to offline
	node.mu.Lock()
	node.Status = NodeStatusOffline
	node.mu.Unlock()

	if node.IsAvailable() {
		t.Error("Expected node to be unavailable")
	}
}

func TestNodeManager(t *testing.T) {
	manager := NewNodeManager()

	nodes := []*Node{
		{ID: "node-1", Address: "localhost:4201", Status: NodeStatusActive, Weight: 1},
		{ID: "node-2", Address: "localhost:4202", Status: NodeStatusActive, Weight: 1},
	}

	// Add nodes
	for _, node := range nodes {
		if err := manager.AddNode(node); err != nil {
			t.Fatalf("Failed to add node: %v", err)
		}
	}

	// Test getting all nodes
	allNodes := manager.GetAllNodes()
	if len(allNodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(allNodes))
	}

	// Test getting healthy nodes
	healthyNodes := manager.GetHealthyNodes()
	if len(healthyNodes) != 2 {
		t.Errorf("Expected 2 healthy nodes, got %d", len(healthyNodes))
	}

	// Test getting node
	node, err := manager.GetNode("node-1")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	if node.ID != "node-1" {
		t.Errorf("Expected node-1, got %s", node.ID)
	}

	// Test getting node for key
	key := []byte("test-key")
	assignedNode, err := manager.GetNodeForKey(key)
	if err != nil {
		t.Fatalf("Failed to get node for key: %v", err)
	}

	if assignedNode == nil {
		t.Fatal("Expected non-nil node")
	}

	// Test updating node status
	if err := manager.UpdateNodeStatus("node-1", NodeStatusOffline); err != nil {
		t.Fatalf("Failed to update node status: %v", err)
	}

	// Check healthy nodes again
	healthyNodes = manager.GetHealthyNodes()
	if len(healthyNodes) != 1 {
		t.Errorf("Expected 1 healthy node, got %d", len(healthyNodes))
	}

	// Test removing node
	if err := manager.RemoveNode("node-2"); err != nil {
		t.Fatalf("Failed to remove node: %v", err)
	}

	allNodes = manager.GetAllNodes()
	if len(allNodes) != 1 {
		t.Errorf("Expected 1 node after removal, got %d", len(allNodes))
	}
}

func TestNodeManagerDuplicate(t *testing.T) {
	manager := NewNodeManager()

	node := &Node{ID: "node-1", Address: "localhost:4201", Status: NodeStatusActive, Weight: 1}

	if err := manager.AddNode(node); err != nil {
		t.Fatalf("Failed to add node: %v", err)
	}

	// Try to add duplicate
	if err := manager.AddNode(node); err == nil {
		t.Error("Expected error for duplicate node")
	}
}

func TestNodeManagerNotFound(t *testing.T) {
	manager := NewNodeManager()

	_, err := manager.GetNode("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent node")
	}

	err = manager.RemoveNode("nonexistent")
	if err == nil {
		t.Error("Expected error when removing non-existent node")
	}
}

func TestNodeManagerEmpty(t *testing.T) {
	manager := NewNodeManager()

	_, err := manager.GetNodeForKey([]byte("test"))
	if err != ErrNodeNotAvailable {
		t.Errorf("Expected ErrNodeNotAvailable, got %v", err)
	}
}

func TestHashRing(t *testing.T) {
	ring := NewHashRing(10) // Small ring for testing

	// Add nodes
	ring.AddNode("node-1")
	ring.AddNode("node-2")

	// Get node for key
	key := []byte("test-key")
	node := ring.GetNode(key)

	if node == "" {
		t.Error("Expected non-empty node")
	}

	// Same key should map to same node
	node2 := ring.GetNode(key)
	if node != node2 {
		t.Errorf("Consistent hashing failed: %s != %s", node, node2)
	}

	// Remove node and check redistribution
	ring.RemoveNode("node-1")

	node3 := ring.GetNode(key)
	if node3 == "" {
		t.Error("Expected non-empty node after removal")
	}
}

func TestHashRingEmpty(t *testing.T) {
	ring := NewHashRing(10)

	node := ring.GetNode([]byte("test"))
	if node != "" {
		t.Error("Expected empty node for empty ring")
	}
}

func TestShardStatusString(t *testing.T) {
	tests := []struct {
		status   ShardStatus
		expected string
	}{
		{ShardStatusActive, "active"},
		{ShardStatusReadOnly, "read_only"},
		{ShardStatusMigrating, "migrating"},
		{ShardStatusOffline, "offline"},
		{ShardStatus(999), "unknown"},
	}

	for _, tt := range tests {
		result := tt.status.String()
		if result != tt.expected {
			t.Errorf("ShardStatus(%d).String() = %s, want %s", tt.status, result, tt.expected)
		}
	}
}

func TestNodeStatusString(t *testing.T) {
	tests := []struct {
		status   NodeStatus
		expected string
	}{
		{NodeStatusActive, "active"},
		{NodeStatusDraining, "draining"},
		{NodeStatusOffline, "offline"},
		{NodeStatusFailed, "failed"},
		{NodeStatus(999), "unknown"},
	}

	for _, tt := range tests {
		result := tt.status.String()
		if result != tt.expected {
			t.Errorf("NodeStatus(%d).String() = %s, want %s", tt.status, result, tt.expected)
		}
	}
}

func TestShardMigrator(t *testing.T) {
	config := DefaultShardConfig()
	manager := NewShardManager(config)

	nodes := []*Node{
		{ID: "node-1", Address: "localhost:4201", Status: NodeStatusActive, Weight: 1},
		{ID: "node-2", Address: "localhost:4202", Status: NodeStatusActive, Weight: 1},
	}

	// Create shards
	if err := manager.CreateShards(2, nodes); err != nil {
		t.Fatalf("Failed to create shards: %v", err)
	}

	migrator := NewShardMigrator(manager)

	// Start migration
	shards := manager.GetAllShards()
	if len(shards) == 0 {
		t.Fatal("Expected at least one shard")
	}

	shardID := shards[0].ID
	task, err := migrator.StartMigration(shardID, "node-2")
	if err != nil {
		t.Fatalf("Failed to start migration: %v", err)
	}

	if task.ShardID != shardID {
		t.Errorf("Expected shard %s, got %s", shardID, task.ShardID)
	}

	// Check status
	status, err := migrator.GetMigrationStatus(shardID)
	if err != nil {
		t.Fatalf("Failed to get migration status: %v", err)
	}

	if status.Status != MigrationStatusPending {
		t.Errorf("Expected status pending, got %v", status.Status)
	}

	// Complete migration
	if err := migrator.CompleteMigration(shardID); err != nil {
		t.Fatalf("Failed to complete migration: %v", err)
	}

	// Check shard status
	shard, _ := manager.GetShardByID(shardID)
	if shard.Status != ShardStatusActive {
		t.Errorf("Expected shard status active, got %s", shard.Status.String())
	}

	// Try to get status after completion
	_, err = migrator.GetMigrationStatus(shardID)
	if err == nil {
		t.Error("Expected error when getting status of completed migration")
	}
}

func TestShardMigratorDuplicate(t *testing.T) {
	config := DefaultShardConfig()
	manager := NewShardManager(config)

	nodes := []*Node{
		{ID: "node-1", Address: "localhost:4201", Status: NodeStatusActive, Weight: 1},
		{ID: "node-2", Address: "localhost:4202", Status: NodeStatusActive, Weight: 1},
	}

	if err := manager.CreateShards(1, nodes); err != nil {
		t.Fatalf("Failed to create shards: %v", err)
	}

	migrator := NewShardMigrator(manager)

	shards := manager.GetAllShards()
	shardID := shards[0].ID

	// Start first migration
	_, err := migrator.StartMigration(shardID, "node-2")
	if err != nil {
		t.Fatalf("Failed to start first migration: %v", err)
	}

	// Try to start second migration for same shard
	_, err = migrator.StartMigration(shardID, "node-2")
	if err == nil {
		t.Error("Expected error for duplicate migration")
	}
}

func TestKeyRangeString(t *testing.T) {
	r := KeyRange{Start: 100, End: 200}
	expected := "[100-200]"
	if r.String() != expected {
		t.Errorf("Expected %s, got %s", expected, r.String())
	}
}

func TestShard(t *testing.T) {
	shard := &Shard{
		ID:        "shard-1",
		NodeID:    "node-1",
		KeyRange:  KeyRange{Start: 0, End: 100},
		Status:    ShardStatusActive,
		CreatedAt: time.Now(),
		RowCount:  1000,
		SizeBytes: 1024 * 1024,
	}

	if shard.ID != "shard-1" {
		t.Errorf("Expected ID shard-1, got %s", shard.ID)
	}

	if shard.Status != ShardStatusActive {
		t.Errorf("Expected status active, got %v", shard.Status)
	}
}

func TestDefaultShardConfig(t *testing.T) {
	config := DefaultShardConfig()

	if config.DefaultShardCount != 4 {
		t.Errorf("Expected DefaultShardCount 4, got %d", config.DefaultShardCount)
	}

	if config.ReplicationFactor != 1 {
		t.Errorf("Expected ReplicationFactor 1, got %d", config.ReplicationFactor)
	}

	if config.MinShardSize != 1<<30 {
		t.Errorf("Expected MinShardSize 1GB, got %d", config.MinShardSize)
	}

	if config.MaxShardSize != 10<<30 {
		t.Errorf("Expected MaxShardSize 10GB, got %d", config.MaxShardSize)
	}

	if !config.AutoRebalance {
		t.Error("Expected AutoRebalance to be true")
	}
}

func TestDistributedTransaction(t *testing.T) {
	txn := &DistributedTransaction{
		ID:        "txn-1",
		Shards:    []string{"shard-1", "shard-2"},
		StartTime: time.Now(),
		Status:    TxnStatusPending,
	}

	if txn.ID != "txn-1" {
		t.Errorf("Expected ID txn-1, got %s", txn.ID)
	}

	if len(txn.Shards) != 2 {
		t.Errorf("Expected 2 shards, got %d", len(txn.Shards))
	}

	if txn.Status != TxnStatusPending {
		t.Errorf("Expected status pending, got %v", txn.Status)
	}
}

func TestRebalancePlan(t *testing.T) {
	plan := &RebalancePlan{
		ShardID:       "shard-1",
		SourceNodeID:  "node-1",
		TargetNodeID:  "node-2",
		EstimatedSize: 1024 * 1024 * 100, // 100MB
		EstimatedTime: 5 * time.Minute,
	}

	if plan.ShardID != "shard-1" {
		t.Errorf("Expected ShardID shard-1, got %s", plan.ShardID)
	}

	if plan.SourceNodeID != "node-1" {
		t.Errorf("Expected SourceNodeID node-1, got %s", plan.SourceNodeID)
	}
}

func TestClusterStats(t *testing.T) {
	stats := &ClusterStats{
		TotalNodes:     5,
		HealthyNodes:   4,
		TotalShards:    20,
		ActiveShards:   18,
		TotalRows:      1000000,
		TotalSizeBytes: 10 * 1024 * 1024 * 1024, // 10GB
		ReplicationLag: 100 * time.Millisecond,
		LastRebalance:  time.Now(),
	}

	if stats.TotalNodes != 5 {
		t.Errorf("Expected TotalNodes 5, got %d", stats.TotalNodes)
	}

	if stats.HealthyNodes != 4 {
		t.Errorf("Expected HealthyNodes 4, got %d", stats.HealthyNodes)
	}
}

func TestShardStats(t *testing.T) {
	stats := &ShardStats{
		ShardID:      "shard-1",
		NodeID:       "node-1",
		RowCount:     50000,
		SizeBytes:    512 * 1024 * 1024,
		QueryCount:   10000,
		ErrorCount:   10,
		AvgQueryTime: 5 * time.Millisecond,
		LastAccessed: time.Now(),
	}

	if stats.ShardID != "shard-1" {
		t.Errorf("Expected ShardID shard-1, got %s", stats.ShardID)
	}

	if stats.RowCount != 50000 {
		t.Errorf("Expected RowCount 50000, got %d", stats.RowCount)
	}
}

func TestMigrationTask(t *testing.T) {
	task := &MigrationTask{
		ShardID:      "shard-1",
		SourceNodeID: "node-1",
		TargetNodeID: "node-2",
		StartTime:    time.Now(),
		Progress:     50.0,
		Status:       MigrationStatusCopying,
	}

	if task.ShardID != "shard-1" {
		t.Errorf("Expected ShardID shard-1, got %s", task.ShardID)
	}

	if task.Status != MigrationStatusCopying {
		t.Errorf("Expected status copying, got %v", task.Status)
	}

	if task.Progress != 50.0 {
		t.Errorf("Expected Progress 50.0, got %f", task.Progress)
	}
}

func TestShardingStrategy(t *testing.T) {
	// Verify strategy constants
	if StrategyHash != 0 {
		t.Errorf("Expected StrategyHash = 0, got %d", StrategyHash)
	}

	if StrategyRange != 1 {
		t.Errorf("Expected StrategyRange = 1, got %d", StrategyRange)
	}

	if StrategyLookup != 2 {
		t.Errorf("Expected StrategyLookup = 2, got %d", StrategyLookup)
	}
}

func TestTxnStatus(t *testing.T) {
	if TxnStatusPending != 0 {
		t.Errorf("Expected TxnStatusPending = 0, got %d", TxnStatusPending)
	}

	if TxnStatusPrepared != 1 {
		t.Errorf("Expected TxnStatusPrepared = 1, got %d", TxnStatusPrepared)
	}

	if TxnStatusCommitted != 2 {
		t.Errorf("Expected TxnStatusCommitted = 2, got %d", TxnStatusCommitted)
	}

	if TxnStatusAborted != 3 {
		t.Errorf("Expected TxnStatusAborted = 3, got %d", TxnStatusAborted)
	}
}

func TestMigrationStatus(t *testing.T) {
	if MigrationStatusPending != 0 {
		t.Errorf("Expected MigrationStatusPending = 0, got %d", MigrationStatusPending)
	}

	if MigrationStatusComplete != 4 {
		t.Errorf("Expected MigrationStatusComplete = 4, got %d", MigrationStatusComplete)
	}

	if MigrationStatusFailed != 5 {
		t.Errorf("Expected MigrationStatusFailed = 5, got %d", MigrationStatusFailed)
	}
}
