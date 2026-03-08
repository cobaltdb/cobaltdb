package txn

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DeadlockDetector detects and resolves deadlocks in the transaction manager
type DeadlockDetector struct {
	manager *Manager

	// Wait-for graph
	waitFor   map[uint64]map[uint64]bool // txnID -> set of txnIDs it's waiting for
	waitForMu sync.RWMutex

	// Lock information
	lockHolders map[string][]uint64 // resource -> list of holding txns
	lockWaiters map[string][]uint64 // resource -> list of waiting txns
	lockMu      sync.RWMutex

	// Wait start times for each transaction
	waitStartTimes   map[uint64]time.Time
	waitStartTimesMu sync.RWMutex

	// Transaction work tracking (for MinWork/MaxWork strategies)
	txnWork   map[uint64]uint64 // txnID -> work units (e.g., rows modified)
	txnWorkMu sync.RWMutex

	// Configuration
	enabled        atomic.Bool
	detectInterval time.Duration
	victimStrategy VictimStrategy

	// Statistics
	detections     atomic.Uint64
	resolutions    atomic.Uint64
	victimsAborted atomic.Uint64
}

// VictimStrategy determines which transaction to abort in a deadlock
type VictimStrategy int

const (
	// VictimYoungest aborts the newest transaction
	VictimYoungest VictimStrategy = iota
	// VictimOldest aborts the oldest transaction
	VictimOldest
	// VictimMinWork aborts the transaction with least work done
	VictimMinWork
	// VictimMaxWork aborts the transaction with most work done
	VictimMaxWork
	// VictimWaitTime aborts the transaction waiting the longest
	VictimWaitTime
)

// DeadlockConfig configures the deadlock detector
type DeadlockConfig struct {
	Enabled        bool
	DetectInterval time.Duration
	VictimStrategy VictimStrategy
}

// DefaultDeadlockConfig returns default configuration
func DefaultDeadlockConfig() *DeadlockConfig {
	return &DeadlockConfig{
		Enabled:        true,
		DetectInterval: 1 * time.Second,
		VictimStrategy: VictimYoungest,
	}
}

// NewDeadlockDetector creates a new deadlock detector
func NewDeadlockDetector(manager *Manager, config *DeadlockConfig) *DeadlockDetector {
	if config == nil {
		config = DefaultDeadlockConfig()
	}

	d := &DeadlockDetector{
		manager:        manager,
		waitFor:        make(map[uint64]map[uint64]bool),
		lockHolders:    make(map[string][]uint64),
		lockWaiters:    make(map[string][]uint64),
		waitStartTimes: make(map[uint64]time.Time),
		txnWork:        make(map[uint64]uint64),
		detectInterval: config.DetectInterval,
		victimStrategy: config.VictimStrategy,
	}

	d.enabled.Store(config.Enabled)

	return d
}

// Enable enables deadlock detection
func (d *DeadlockDetector) Enable() {
	d.enabled.Store(true)
}

// Disable disables deadlock detection
func (d *DeadlockDetector) Disable() {
	d.enabled.Store(false)
}

// IsEnabled returns true if deadlock detection is enabled
func (d *DeadlockDetector) IsEnabled() bool {
	return d.enabled.Load()
}

// RecordWait records that txnID is waiting for resource held by otherTxnIDs
func (d *DeadlockDetector) RecordWait(txnID uint64, resource string, holderTxnIDs []uint64) {
	if !d.enabled.Load() {
		return
	}

	d.waitForMu.Lock()
	defer d.waitForMu.Unlock()

	// Add wait-for edges
	if d.waitFor[txnID] == nil {
		d.waitFor[txnID] = make(map[uint64]bool)
	}
	for _, holderID := range holderTxnIDs {
		d.waitFor[txnID][holderID] = true
	}

	// Record wait start time
	d.waitStartTimesMu.Lock()
	if _, exists := d.waitStartTimes[txnID]; !exists {
		d.waitStartTimes[txnID] = time.Now()
	}
	d.waitStartTimesMu.Unlock()

	// Update lock tracking
	d.lockMu.Lock()
	defer d.lockMu.Unlock()

	if d.lockWaiters[resource] == nil {
		d.lockWaiters[resource] = make([]uint64, 0)
	}
	// Add to waiters if not already there
	found := false
	for _, id := range d.lockWaiters[resource] {
		if id == txnID {
			found = true
			break
		}
	}
	if !found {
		d.lockWaiters[resource] = append(d.lockWaiters[resource], txnID)
	}

	d.lockHolders[resource] = holderTxnIDs
}

// RecordLockGranted records that txnID has been granted a lock
func (d *DeadlockDetector) RecordLockGranted(txnID uint64, resource string) {
	if !d.enabled.Load() {
		return
	}

	d.waitForMu.Lock()
	defer d.waitForMu.Unlock()

	// Remove all wait-for edges from txnID
	delete(d.waitFor, txnID)

	// Clear wait start time
	d.waitStartTimesMu.Lock()
	delete(d.waitStartTimes, txnID)
	d.waitStartTimesMu.Unlock()

	// Remove from lock waiters
	d.lockMu.Lock()
	defer d.lockMu.Unlock()

	if waiters, exists := d.lockWaiters[resource]; exists {
		newWaiters := make([]uint64, 0, len(waiters))
		for _, id := range waiters {
			if id != txnID {
				newWaiters = append(newWaiters, id)
			}
		}
		d.lockWaiters[resource] = newWaiters
	}

	// Add to holders
	if d.lockHolders[resource] == nil {
		d.lockHolders[resource] = make([]uint64, 0)
	}
	found := false
	for _, id := range d.lockHolders[resource] {
		if id == txnID {
			found = true
			break
		}
	}
	if !found {
		d.lockHolders[resource] = append(d.lockHolders[resource], txnID)
	}
}

// RecordTransactionEnd records that a transaction has ended
func (d *DeadlockDetector) RecordTransactionEnd(txnID uint64) {
	if !d.enabled.Load() {
		return
	}

	d.waitForMu.Lock()
	defer d.waitForMu.Unlock()

	// Remove all wait-for edges to and from txnID
	delete(d.waitFor, txnID)
	for _, waits := range d.waitFor {
		delete(waits, txnID)
	}

	// Clear wait start time
	d.waitStartTimesMu.Lock()
	delete(d.waitStartTimes, txnID)
	d.waitStartTimesMu.Unlock()

	// Remove work tracking
	d.txnWorkMu.Lock()
	delete(d.txnWork, txnID)
	d.txnWorkMu.Unlock()

	// Remove from lock tracking
	d.lockMu.Lock()
	defer d.lockMu.Unlock()

	for resource, holders := range d.lockHolders {
		newHolders := make([]uint64, 0, len(holders))
		for _, id := range holders {
			if id != txnID {
				newHolders = append(newHolders, id)
			}
		}
		if len(newHolders) == 0 {
			delete(d.lockHolders, resource)
		} else {
			d.lockHolders[resource] = newHolders
		}
	}

	for resource, waiters := range d.lockWaiters {
		newWaiters := make([]uint64, 0, len(waiters))
		for _, id := range waiters {
			if id != txnID {
				newWaiters = append(newWaiters, id)
			}
		}
		if len(newWaiters) == 0 {
			delete(d.lockWaiters, resource)
		} else {
			d.lockWaiters[resource] = newWaiters
		}
	}
}

// RecordWork records work done by a transaction (for MinWork/MaxWork strategies)
func (d *DeadlockDetector) RecordWork(txnID uint64, workUnits uint64) {
	if !d.enabled.Load() {
		return
	}

	d.txnWorkMu.Lock()
	defer d.txnWorkMu.Unlock()
	d.txnWork[txnID] += workUnits
}

// DetectDeadlocks detects deadlocks in the wait-for graph
func (d *DeadlockDetector) DetectDeadlocks() [][]uint64 {
	if !d.enabled.Load() {
		return nil
	}

	d.waitForMu.RLock()
	defer d.waitForMu.RUnlock()

	var cycles [][]uint64
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)
	path := make([]uint64, 0)

	var dfs func(uint64) bool
	dfs = func(node uint64) bool {
		visited[node] = true
		recStack[node] = true
		path = append(path, node)

		for neighbor := range d.waitFor[node] {
			if !visited[neighbor] {
				if dfs(neighbor) {
					return true
				}
			} else if recStack[neighbor] {
				// Found a cycle - extract it
				cycle := d.extractCycle(path, neighbor)
				cycles = append(cycles, cycle)
				return true
			}
		}

		path = path[:len(path)-1]
		recStack[node] = false
		return false
	}

	for txnID := range d.waitFor {
		if !visited[txnID] {
			dfs(txnID)
		}
	}

	if len(cycles) > 0 {
		d.detections.Add(uint64(len(cycles)))
	}

	return cycles
}

// extractCycle extracts the cycle from the DFS path
func (d *DeadlockDetector) extractCycle(path []uint64, start uint64) []uint64 {
	var cycle []uint64
	found := false
	for _, node := range path {
		if node == start {
			found = true
		}
		if found {
			cycle = append(cycle, node)
		}
	}
	cycle = append(cycle, start) // Close the cycle
	return cycle
}

// ResolveDeadlock resolves a deadlock by aborting a victim transaction
func (d *DeadlockDetector) ResolveDeadlock(cycle []uint64) (uint64, error) {
	if len(cycle) == 0 {
		return 0, fmt.Errorf("empty deadlock cycle")
	}

	// Select victim
	victim := d.selectVictim(cycle)

	// Try to abort the victim transaction
	aborted := false
	if d.manager != nil {
		// Get the transaction and roll it back
		txn := d.manager.GetTransaction(victim)
		if txn != nil {
			if err := txn.Rollback(); err == nil {
				aborted = true
				d.victimsAborted.Add(1)
			}
		}
	}

	// Record resolution and clean up
	d.resolutions.Add(1)
	d.RecordTransactionEnd(victim)

	if !aborted {
		// Still return the victim ID, but caller knows it wasn't actually aborted
		return victim, fmt.Errorf("could not abort transaction %d", victim)
	}

	return victim, nil
}

// selectVictim selects a transaction to abort based on the strategy
func (d *DeadlockDetector) selectVictim(cycle []uint64) uint64 {
	switch d.victimStrategy {
	case VictimYoungest:
		// Select the one with highest ID (newest)
		maxID := cycle[0]
		for _, id := range cycle[1:] {
			if id > maxID {
				maxID = id
			}
		}
		return maxID

	case VictimOldest:
		// Select the one with lowest ID (oldest)
		minID := cycle[0]
		for _, id := range cycle[1:] {
			if id < minID {
				minID = id
			}
		}
		return minID

	case VictimMinWork:
		// Select the transaction with least work done
		d.txnWorkMu.RLock()
		defer d.txnWorkMu.RUnlock()

		minWork := uint64(^uint64(0)) // Max uint64
		var minWorkTxn uint64
		for _, id := range cycle {
			work := d.txnWork[id]
			if work < minWork {
				minWork = work
				minWorkTxn = id
			}
		}
		return minWorkTxn

	case VictimMaxWork:
		// Select the transaction with most work done
		d.txnWorkMu.RLock()
		defer d.txnWorkMu.RUnlock()

		var maxWork uint64
		var maxWorkTxn uint64
		for _, id := range cycle {
			work := d.txnWork[id]
			if work > maxWork {
				maxWork = work
				maxWorkTxn = id
			}
		}
		return maxWorkTxn

	case VictimWaitTime:
		// Select the transaction waiting the longest
		d.waitStartTimesMu.RLock()
		defer d.waitStartTimesMu.RUnlock()

		var longestWait time.Duration
		var longestWaitTxn uint64
		now := time.Now()

		for _, id := range cycle {
			if startTime, exists := d.waitStartTimes[id]; exists {
				waitTime := now.Sub(startTime)
				if waitTime > longestWait {
					longestWait = waitTime
					longestWaitTxn = id
				}
			}
		}
		if longestWaitTxn == 0 {
			// Fallback to youngest if no wait times recorded
			return d.selectVictimWithStrategy(cycle, VictimYoungest)
		}
		return longestWaitTxn

	default:
		return cycle[0]
	}
}

// selectVictimWithStrategy selects victim using a specific strategy
func (d *DeadlockDetector) selectVictimWithStrategy(cycle []uint64, strategy VictimStrategy) uint64 {
	switch strategy {
	case VictimYoungest:
		maxID := cycle[0]
		for _, id := range cycle[1:] {
			if id > maxID {
				maxID = id
			}
		}
		return maxID
	case VictimOldest:
		minID := cycle[0]
		for _, id := range cycle[1:] {
			if id < minID {
				minID = id
			}
		}
		return minID
	default:
		return cycle[0]
	}
}

// GetWaitForGraph returns a copy of the wait-for graph
func (d *DeadlockDetector) GetWaitForGraph() map[uint64][]uint64 {
	d.waitForMu.RLock()
	defer d.waitForMu.RUnlock()

	graph := make(map[uint64][]uint64)
	for txnID, waits := range d.waitFor {
		for waitID := range waits {
			graph[txnID] = append(graph[txnID], waitID)
		}
	}

	return graph
}

// GetStats returns deadlock detection statistics
func (d *DeadlockDetector) GetStats() DeadlockStats {
	return DeadlockStats{
		Enabled:        d.enabled.Load(),
		Detections:     d.detections.Load(),
		Resolutions:    d.resolutions.Load(),
		VictimsAborted: d.victimsAborted.Load(),
	}
}

// DeadlockStats contains deadlock detection statistics
type DeadlockStats struct {
	Enabled        bool   `json:"enabled"`
	Detections     uint64 `json:"detections"`
	Resolutions    uint64 `json:"resolutions"`
	VictimsAborted uint64 `json:"victims_aborted"`
}

// ClearStats clears deadlock statistics
func (d *DeadlockDetector) ClearStats() {
	d.detections.Store(0)
	d.resolutions.Store(0)
	d.victimsAborted.Store(0)
}

// LockMonitor monitors lock waits and triggers deadlock detection
type LockMonitor struct {
	detector      *DeadlockDetector
	checkInterval time.Duration
	stopCh        chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
}

// NewLockMonitor creates a new lock monitor
func NewLockMonitor(detector *DeadlockDetector, checkInterval time.Duration) *LockMonitor {
	return &LockMonitor{
		detector:      detector,
		checkInterval: checkInterval,
		stopCh:        make(chan struct{}),
	}
}

// Start starts the lock monitor
func (m *LockMonitor) Start() {
	m.wg.Add(1)
	go m.monitorLoop()
}

// Stop stops the lock monitor
func (m *LockMonitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	m.wg.Wait()
}

func (m *LockMonitor) monitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			cycles := m.detector.DetectDeadlocks()
			for _, cycle := range cycles {
				if victim, err := m.detector.ResolveDeadlock(cycle); err == nil {
					// Log successful resolution (could integrate with proper logging)
					_ = victim
				}
			}
		}
	}
}

// WaitEdge represents an edge in the wait-for graph
type WaitEdge struct {
	From     uint64
	To       uint64
	Resource string
}

// GetWaitEdges returns all wait edges in the graph
func (d *DeadlockDetector) GetWaitEdges() []WaitEdge {
	d.waitForMu.RLock()
	defer d.waitForMu.RUnlock()

	d.lockMu.RLock()
	defer d.lockMu.RUnlock()

	var edges []WaitEdge

	// Build edges from wait-for graph and lock information
	for resource, waiters := range d.lockWaiters {
		holders := d.lockHolders[resource]
		for _, waiter := range waiters {
			for _, holder := range holders {
				edges = append(edges, WaitEdge{
					From:     waiter,
					To:       holder,
					Resource: resource,
				})
			}
		}
	}

	return edges
}

// IsWaiting returns true if the transaction is waiting
func (d *DeadlockDetector) IsWaiting(txnID uint64) bool {
	d.waitForMu.RLock()
	defer d.waitForMu.RUnlock()

	return len(d.waitFor[txnID]) > 0
}

// GetWaitTime returns how long a transaction has been waiting
func (d *DeadlockDetector) GetWaitTime(txnID uint64) time.Duration {
	d.waitStartTimesMu.RLock()
	defer d.waitStartTimesMu.RUnlock()

	if startTime, exists := d.waitStartTimes[txnID]; exists {
		return time.Since(startTime)
	}
	return 0
}

// GetLongestWaiting returns the transaction ID that has been waiting the longest
func (d *DeadlockDetector) GetLongestWaiting() (uint64, time.Duration) {
	d.waitStartTimesMu.RLock()
	defer d.waitStartTimesMu.RUnlock()

	var longestTxn uint64
	var longestWait time.Duration
	now := time.Now()

	for txnID, startTime := range d.waitStartTimes {
		waitTime := now.Sub(startTime)
		if waitTime > longestWait {
			longestWait = waitTime
			longestTxn = txnID
		}
	}

	return longestTxn, longestWait
}
