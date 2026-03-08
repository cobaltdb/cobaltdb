package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// ErrShardNotFound is returned when a shard cannot be found
var ErrShardNotFound = errors.New("shard not found")

// ErrInvalidShardKey is returned when a shard key is invalid
var ErrInvalidShardKey = errors.New("invalid shard key")

// ErrNodeNotAvailable is returned when a cluster node is not available
var ErrNodeNotAvailable = errors.New("cluster node not available")

// Shard represents a single database shard
type Shard struct {
	ID        string
	NodeID    string
	KeyRange  KeyRange
	Status    ShardStatus
	CreatedAt time.Time
	UpdatedAt time.Time
	RowCount  int64
	SizeBytes int64
	mu        sync.RWMutex
}

// ShardStatus represents the status of a shard
type ShardStatus int

const (
	ShardStatusActive ShardStatus = iota
	ShardStatusReadOnly
	ShardStatusMigrating
	ShardStatusOffline
)

func (s ShardStatus) String() string {
	switch s {
	case ShardStatusActive:
		return "active"
	case ShardStatusReadOnly:
		return "read_only"
	case ShardStatusMigrating:
		return "migrating"
	case ShardStatusOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// KeyRange represents a range of shard keys
type KeyRange struct {
	Start uint64
	End   uint64
}

// Contains checks if a key is within this range
func (r KeyRange) Contains(key uint64) bool {
	if r.Start <= r.End {
		return key >= r.Start && key <= r.End
	}
	// Wraparound range (for consistent hashing)
	return key >= r.Start || key <= r.End
}

// String returns a string representation of the key range
func (r KeyRange) String() string {
	return fmt.Sprintf("[%d-%d]", r.Start, r.End)
}

// ShardRouter routes queries to the appropriate shard
type ShardRouter struct {
	shards   map[string]*Shard
	hashFunc ShardHashFunc
	mu       sync.RWMutex
}

// ShardHashFunc is a function that computes a shard key hash
type ShardHashFunc func(key []byte) uint64

// DefaultShardHash uses FNV-1a hash for fast distribution
func DefaultShardHash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

// ConsistentShardHash uses SHA-256 for more uniform distribution
func ConsistentShardHash(key []byte) uint64 {
	h := sha256.Sum256(key)
	return binary.BigEndian.Uint64(h[:8])
}

// NewShardRouter creates a new shard router
func NewShardRouter(hashFunc ShardHashFunc) *ShardRouter {
	if hashFunc == nil {
		hashFunc = DefaultShardHash
	}
	return &ShardRouter{
		shards:   make(map[string]*Shard),
		hashFunc: hashFunc,
	}
}

// AddShard adds a shard to the router
func (r *ShardRouter) AddShard(shard *Shard) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.shards[shard.ID]; exists {
		return fmt.Errorf("shard %s already exists", shard.ID)
	}

	r.shards[shard.ID] = shard
	return nil
}

// RemoveShard removes a shard from the router
func (r *ShardRouter) RemoveShard(shardID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.shards[shardID]; !exists {
		return ErrShardNotFound
	}

	delete(r.shards, shardID)
	return nil
}

// GetShardForKey returns the shard that should handle the given key
func (r *ShardRouter) GetShardForKey(key []byte) (*Shard, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.shards) == 0 {
		return nil, ErrShardNotFound
	}

	hash := r.hashFunc(key)

	// Find the shard whose range contains this hash
	for _, shard := range r.shards {
		if shard.KeyRange.Contains(hash) && shard.Status == ShardStatusActive {
			return shard, nil
		}
	}

	return nil, ErrShardNotFound
}

// GetAllShards returns all registered shards
func (r *ShardRouter) GetAllShards() []*Shard {
	r.mu.RLock()
	defer r.mu.RUnlock()

	shards := make([]*Shard, 0, len(r.shards))
	for _, shard := range r.shards {
		shards = append(shards, shard)
	}
	return shards
}

// GetShardByID returns a shard by its ID
func (r *ShardRouter) GetShardByID(shardID string) (*Shard, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	shard, exists := r.shards[shardID]
	if !exists {
		return nil, ErrShardNotFound
	}
	return shard, nil
}

// ShardingStrategy defines how data is distributed across shards
type ShardingStrategy int

const (
	// StrategyHash uses hash-based sharding
	StrategyHash ShardingStrategy = iota
	// StrategyRange uses range-based sharding
	StrategyRange
	// StrategyLookup uses a lookup table for sharding
	StrategyLookup
)

// ShardManager manages shards and their configuration
type ShardManager struct {
	router   *ShardRouter
	nodes    map[string]*Node
	strategy ShardingStrategy
	config   *ShardConfig
	mu       sync.RWMutex
}

// ShardConfig contains shard configuration
type ShardConfig struct {
	DefaultShardCount int
	ReplicationFactor int
	MinShardSize      int64
	MaxShardSize      int64
	AutoRebalance     bool
}

// DefaultShardConfig returns default shard configuration
func DefaultShardConfig() *ShardConfig {
	return &ShardConfig{
		DefaultShardCount: 4,
		ReplicationFactor: 1,
		MinShardSize:      1 << 30,  // 1GB
		MaxShardSize:      10 << 30, // 10GB
		AutoRebalance:     true,
	}
}

// NewShardManager creates a new shard manager
func NewShardManager(config *ShardConfig) *ShardManager {
	if config == nil {
		config = DefaultShardConfig()
	}
	return &ShardManager{
		router:   NewShardRouter(nil),
		nodes:    make(map[string]*Node),
		strategy: StrategyHash,
		config:   config,
	}
}

// CreateShards creates the initial set of shards
func (m *ShardManager) CreateShards(count int, nodes []*Node) error {
	if count <= 0 {
		count = m.config.DefaultShardCount
	}

	if len(nodes) == 0 {
		return errors.New("at least one node required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Register nodes
	for _, node := range nodes {
		m.nodes[node.ID] = node
	}

	// Create shards with even key range distribution
	// For uint64, divide into count equal ranges
	rangeSize := ^uint64(0) / uint64(count)

	for i := 0; i < count; i++ {
		start := uint64(i) * rangeSize
		end := start + rangeSize - 1
		if i == count-1 {
			end = ^uint64(0) // Last shard gets remainder
		}

		shard := &Shard{
			ID:        fmt.Sprintf("shard-%d", i),
			NodeID:    nodes[i%len(nodes)].ID,
			KeyRange:  KeyRange{Start: start, End: end},
			Status:    ShardStatusActive,
			CreatedAt: time.Now(),
		}

		if err := m.router.AddShard(shard); err != nil {
			return err
		}
	}

	return nil
}

// GetShardForKey returns the appropriate shard for a key
func (m *ShardManager) GetShardForKey(key []byte) (*Shard, error) {
	return m.router.GetShardForKey(key)
}

// GetShardByID returns a shard by ID
func (m *ShardManager) GetShardByID(shardID string) (*Shard, error) {
	return m.router.GetShardByID(shardID)
}

// GetAllShards returns all shards
func (m *ShardManager) GetAllShards() []*Shard {
	return m.router.GetAllShards()
}

// UpdateShardStatus updates the status of a shard
func (m *ShardManager) UpdateShardStatus(shardID string, status ShardStatus) error {
	shard, err := m.router.GetShardByID(shardID)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	shard.Status = status
	shard.UpdatedAt = time.Now()
	shard.mu.Unlock()

	return nil
}

// Node represents a cluster node
type Node struct {
	ID       string
	Address  string
	Status   NodeStatus
	Weight   int
	ShardIDs []string
	mu       sync.RWMutex
}

// NodeStatus represents the status of a node
type NodeStatus int

const (
	NodeStatusActive NodeStatus = iota
	NodeStatusDraining
	NodeStatusOffline
	NodeStatusFailed
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusActive:
		return "active"
	case NodeStatusDraining:
		return "draining"
	case NodeStatusOffline:
		return "offline"
	case NodeStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// IsAvailable returns true if the node is available for operations
func (n *Node) IsAvailable() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Status == NodeStatusActive
}

// AddShard adds a shard to this node
func (n *Node) AddShard(shardID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ShardIDs = append(n.ShardIDs, shardID)
}

// RemoveShard removes a shard from this node
func (n *Node) RemoveShard(shardID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, id := range n.ShardIDs {
		if id == shardID {
			n.ShardIDs = append(n.ShardIDs[:i], n.ShardIDs[i+1:]...)
			break
		}
	}
}

// NodeManager manages cluster nodes
type NodeManager struct {
	nodes    map[string]*Node
	hashRing *HashRing
	mu       sync.RWMutex
}

// NewNodeManager creates a new node manager
func NewNodeManager() *NodeManager {
	return &NodeManager{
		nodes:    make(map[string]*Node),
		hashRing: NewHashRing(150), // 150 virtual nodes per physical node
	}
}

// AddNode adds a node to the cluster
func (m *NodeManager) AddNode(node *Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[node.ID]; exists {
		return fmt.Errorf("node %s already exists", node.ID)
	}

	m.nodes[node.ID] = node
	m.hashRing.AddNode(node.ID)

	return nil
}

// RemoveNode removes a node from the cluster
func (m *NodeManager) RemoveNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	delete(m.nodes, nodeID)
	m.hashRing.RemoveNode(nodeID)

	return nil
}

// GetNode returns a node by ID
func (m *NodeManager) GetNode(nodeID string) (*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return node, nil
}

// GetAllNodes returns all nodes in the cluster
func (m *NodeManager) GetAllNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetHealthyNodes returns all healthy nodes
func (m *NodeManager) GetHealthyNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*Node, 0)
	for _, node := range m.nodes {
		if node.IsAvailable() {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetNodeForKey returns the node responsible for a key using consistent hashing
func (m *NodeManager) GetNodeForKey(key []byte) (*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil, ErrNodeNotAvailable
	}

	nodeID := m.hashRing.GetNode(key)
	if nodeID == "" {
		return nil, ErrNodeNotAvailable
	}

	node, exists := m.nodes[nodeID]
	if !exists || !node.IsAvailable() {
		// Try to find another available node
		for _, n := range m.nodes {
			if n.IsAvailable() {
				return n, nil
			}
		}
		return nil, ErrNodeNotAvailable
	}

	return node, nil
}

// UpdateNodeStatus updates a node's status
func (m *NodeManager) UpdateNodeStatus(nodeID string, status NodeStatus) error {
	node, err := m.GetNode(nodeID)
	if err != nil {
		return err
	}

	node.mu.Lock()
	node.Status = status
	node.mu.Unlock()

	return nil
}

// HashRing implements consistent hashing
type HashRing struct {
	replicas int
	ring     map[uint64]string
	sorted   []uint64
	mu       sync.RWMutex
}

// NewHashRing creates a new hash ring with the specified number of replicas
func NewHashRing(replicas int) *HashRing {
	if replicas <= 0 {
		replicas = 150
	}
	return &HashRing{
		replicas: replicas,
		ring:     make(map[uint64]string),
	}
}

// AddNode adds a node to the hash ring
func (r *HashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		key := fmt.Sprintf("%s:%d", nodeID, i)
		hash := DefaultShardHash([]byte(key))
		r.ring[hash] = nodeID
		r.sorted = append(r.sorted, hash)
	}

	// Sort the ring
	r.sortRing()
}

// RemoveNode removes a node from the hash ring
func (r *HashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		key := fmt.Sprintf("%s:%d", nodeID, i)
		hash := DefaultShardHash([]byte(key))
		delete(r.ring, hash)
	}

	// Rebuild sorted list
	r.sorted = make([]uint64, 0, len(r.ring))
	for hash := range r.ring {
		r.sorted = append(r.sorted, hash)
	}
	r.sortRing()
}

// GetNode returns the node responsible for a key
func (r *HashRing) GetNode(key []byte) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sorted) == 0 {
		return ""
	}

	hash := DefaultShardHash(key)

	// Binary search for the first hash >= key hash
	idx := r.findNode(hash)
	if idx >= len(r.sorted) {
		idx = 0
	}

	return r.ring[r.sorted[idx]]
}

// findNode finds the index of the first hash >= target
func (r *HashRing) findNode(target uint64) int {
	// Binary search
	left, right := 0, len(r.sorted)
	for left < right {
		mid := (left + right) / 2
		if r.sorted[mid] < target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// sortRing sorts the hash ring
func (r *HashRing) sortRing() {
	// Simple bubble sort for now (small number of elements)
	for i := 0; i < len(r.sorted); i++ {
		for j := i + 1; j < len(r.sorted); j++ {
			if r.sorted[i] > r.sorted[j] {
				r.sorted[i], r.sorted[j] = r.sorted[j], r.sorted[i]
			}
		}
	}
}

// DistributedTransaction represents a transaction across multiple shards
type DistributedTransaction struct {
	ID        string
	Shards    []string
	StartTime time.Time
	Status    TxnStatus
	mu        sync.RWMutex
}

// TxnStatus represents the status of a distributed transaction
type TxnStatus int

const (
	TxnStatusPending TxnStatus = iota
	TxnStatusPrepared
	TxnStatusCommitted
	TxnStatusAborted
)

// ShardClient is the interface for communicating with a shard
type ShardClient interface {
	Execute(ctx context.Context, query string, args ...interface{}) error
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
	Begin() error
	Commit() error
	Rollback() error
}

// Rows represents query results
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
}

// ClusterStats contains cluster-wide statistics
type ClusterStats struct {
	TotalNodes     int
	HealthyNodes   int
	TotalShards    int
	ActiveShards   int
	TotalRows      int64
	TotalSizeBytes int64
	ReplicationLag time.Duration
	LastRebalance  time.Time
}

// ShardStats contains statistics for a single shard
type ShardStats struct {
	ShardID      string
	NodeID       string
	RowCount     int64
	SizeBytes    int64
	QueryCount   int64
	ErrorCount   int64
	AvgQueryTime time.Duration
	LastAccessed time.Time
}

// RebalancePlan represents a plan for rebalancing shards
type RebalancePlan struct {
	ShardID       string
	SourceNodeID  string
	TargetNodeID  string
	EstimatedSize int64
	EstimatedTime time.Duration
}

// ShardMigrator handles shard migration between nodes
type ShardMigrator struct {
	manager *ShardManager
	active  map[string]*MigrationTask
	mu      sync.RWMutex
}

// MigrationTask represents an ongoing shard migration
type MigrationTask struct {
	ShardID      string
	SourceNodeID string
	TargetNodeID string
	StartTime    time.Time
	Progress     float64
	Status       MigrationStatus
}

// MigrationStatus represents the status of a migration
type MigrationStatus int

const (
	MigrationStatusPending MigrationStatus = iota
	MigrationStatusCopying
	MigrationStatusSyncing
	MigrationStatusSwitching
	MigrationStatusComplete
	MigrationStatusFailed
)

// NewShardMigrator creates a new shard migrator
func NewShardMigrator(manager *ShardManager) *ShardMigrator {
	return &ShardMigrator{
		manager: manager,
		active:  make(map[string]*MigrationTask),
	}
}

// StartMigration starts migrating a shard to a new node
func (m *ShardMigrator) StartMigration(shardID, targetNodeID string) (*MigrationTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.active[shardID]; exists {
		return nil, fmt.Errorf("migration already in progress for shard %s", shardID)
	}

	shard, err := m.manager.GetShardByID(shardID)
	if err != nil {
		return nil, err
	}

	task := &MigrationTask{
		ShardID:      shardID,
		SourceNodeID: shard.NodeID,
		TargetNodeID: targetNodeID,
		StartTime:    time.Now(),
		Progress:     0,
		Status:       MigrationStatusPending,
	}

	m.active[shardID] = task

	// Set shard to migrating status
	m.manager.UpdateShardStatus(shardID, ShardStatusMigrating)

	return task, nil
}

// GetMigrationStatus returns the status of an ongoing migration
func (m *ShardMigrator) GetMigrationStatus(shardID string) (*MigrationTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.active[shardID]
	if !exists {
		return nil, fmt.Errorf("no migration found for shard %s", shardID)
	}

	return task, nil
}

// CompleteMigration marks a migration as complete
func (m *ShardMigrator) CompleteMigration(shardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, exists := m.active[shardID]
	if !exists {
		return fmt.Errorf("no migration found for shard %s", shardID)
	}

	// Update shard node assignment
	shard, err := m.manager.GetShardByID(shardID)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	shard.NodeID = task.TargetNodeID
	shard.Status = ShardStatusActive
	shard.UpdatedAt = time.Now()
	shard.mu.Unlock()

	delete(m.active, shardID)
	return nil
}
