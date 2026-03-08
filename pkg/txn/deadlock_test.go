package txn

import (
	"testing"
	"time"
)

func TestDeadlockDetectorBasic(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	if !detector.IsEnabled() {
		t.Error("Expected detector to be enabled by default")
	}
}

func TestDeadlockDetectorDisableEnable(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	detector.Disable()
	if detector.IsEnabled() {
		t.Error("Expected detector to be disabled")
	}

	detector.Enable()
	if !detector.IsEnabled() {
		t.Error("Expected detector to be enabled")
	}
}

func TestDeadlockDetectorRecordWait(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Record that txn 2 is waiting for txn 1
	detector.RecordWait(2, "resource1", []uint64{1})

	graph := detector.GetWaitForGraph()
	if len(graph) != 1 {
		t.Fatalf("Expected 1 entry in wait-for graph, got %d", len(graph))
	}

	if len(graph[2]) != 1 || graph[2][0] != 1 {
		t.Error("Expected txn 2 to be waiting for txn 1")
	}
}

func TestDeadlockDetectorRecordLockGranted(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Record a wait
	detector.RecordWait(2, "resource1", []uint64{1})

	// Grant the lock to txn 2
	detector.RecordLockGranted(2, "resource1")

	// T 2 should no longer be waiting
	if detector.IsWaiting(2) {
		t.Error("Expected txn 2 to not be waiting after lock granted")
	}
}

func TestDeadlockDetectorRecordTransactionEnd(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Set up a wait chain: 3 -> 2 -> 1
	detector.RecordWait(2, "resource1", []uint64{1})
	detector.RecordWait(3, "resource2", []uint64{2})

	// End transaction 2
	detector.RecordTransactionEnd(2)

	// Transaction 3 should no longer be waiting for 2
	graph := detector.GetWaitForGraph()
	if _, exists := graph[3]; exists {
		t.Error("Expected txn 3 to not be waiting after txn 2 ended")
	}
}

func TestDeadlockDetectorDetectNoDeadlock(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Set up a wait chain without cycle: 3 -> 2 -> 1
	detector.RecordWait(2, "resource1", []uint64{1})
	detector.RecordWait(3, "resource2", []uint64{2})

	cycles := detector.DetectDeadlocks()
	if len(cycles) != 0 {
		t.Errorf("Expected no deadlocks, found %d", len(cycles))
	}
}

func TestDeadlockDetectorDetectSimpleCycle(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Set up a simple cycle: 1 -> 2 -> 1
	detector.RecordWait(1, "resource2", []uint64{2})
	detector.RecordWait(2, "resource1", []uint64{1})

	cycles := detector.DetectDeadlocks()
	if len(cycles) == 0 {
		t.Fatal("Expected deadlock detection")
	}

	// Check stats
	stats := detector.GetStats()
	if stats.Detections != 1 {
		t.Errorf("Expected 1 detection, got %d", stats.Detections)
	}
}

func TestDeadlockDetectorDetectComplexCycle(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Set up a cycle: 1 -> 2 -> 3 -> 1
	detector.RecordWait(1, "resource2", []uint64{2})
	detector.RecordWait(2, "resource3", []uint64{3})
	detector.RecordWait(3, "resource1", []uint64{1})

	cycles := detector.DetectDeadlocks()
	if len(cycles) == 0 {
		t.Fatal("Expected deadlock detection")
	}

	// Verify the cycle contains all 3 transactions
	cycle := cycles[0]
	if len(cycle) != 4 { // 3 transactions + closing transaction
		t.Errorf("Expected cycle of length 4, got %d", len(cycle))
	}
}

func TestDeadlockDetectorDisabled(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	detector.Disable()

	// Set up a cycle
	detector.RecordWait(1, "resource2", []uint64{2})
	detector.RecordWait(2, "resource1", []uint64{1})

	// Should not detect when disabled
	cycles := detector.DetectDeadlocks()
	if len(cycles) != 0 {
		t.Error("Expected no deadlock detection when disabled")
	}
}

func TestDeadlockDetectorSelectVictimYoungest(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := &DeadlockConfig{
		Enabled:        true,
		DetectInterval: 1 * time.Second,
		VictimStrategy: VictimYoungest,
	}
	detector := NewDeadlockDetector(mgr, config)

	cycle := []uint64{1, 5, 3, 10, 2}
	victim := detector.selectVictim(cycle)

	if victim != 10 {
		t.Errorf("Expected victim 10 (youngest), got %d", victim)
	}
}

func TestDeadlockDetectorSelectVictimOldest(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := &DeadlockConfig{
		Enabled:        true,
		DetectInterval: 1 * time.Second,
		VictimStrategy: VictimOldest,
	}
	detector := NewDeadlockDetector(mgr, config)

	cycle := []uint64{5, 3, 10, 1, 2}
	victim := detector.selectVictim(cycle)

	if victim != 1 {
		t.Errorf("Expected victim 1 (oldest), got %d", victim)
	}
}

func TestDeadlockDetectorResolveDeadlock(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Create actual transactions
	txn1 := mgr.Begin(nil)
	txn2 := mgr.Begin(nil)

	// Set up a cycle
	detector.RecordWait(txn1.ID, "resource2", []uint64{txn2.ID})
	detector.RecordWait(txn2.ID, "resource1", []uint64{txn1.ID})

	cycle := []uint64{txn1.ID, txn2.ID}
	victim, err := detector.ResolveDeadlock(cycle)
	if err != nil {
		t.Fatalf("Failed to resolve deadlock: %v", err)
	}

	// Victim should be one of the transactions in the cycle
	found := false
	for _, id := range cycle {
		if id == victim {
			found = true
			break
		}
	}
	if !found {
		t.Error("Victim should be from the deadlock cycle")
	}

	// Stats should be updated
	stats := detector.GetStats()
	if stats.Resolutions != 1 {
		t.Errorf("Expected 1 resolution, got %d", stats.Resolutions)
	}
}

func TestDeadlockDetectorResolveEmptyCycle(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	_, err := detector.ResolveDeadlock([]uint64{})
	if err == nil {
		t.Error("Expected error for empty cycle")
	}
}

func TestDeadlockDetectorStats(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Initial stats
	stats := detector.GetStats()
	if stats.Detections != 0 {
		t.Error("Expected 0 initial detections")
	}
	if stats.Resolutions != 0 {
		t.Error("Expected 0 initial resolutions")
	}

	// Trigger a deadlock
	detector.RecordWait(1, "resource2", []uint64{2})
	detector.RecordWait(2, "resource1", []uint64{1})
	detector.DetectDeadlocks()

	stats = detector.GetStats()
	if stats.Detections != 1 {
		t.Errorf("Expected 1 detection, got %d", stats.Detections)
	}

	// Clear stats
	detector.ClearStats()
	stats = detector.GetStats()
	if stats.Detections != 0 {
		t.Error("Expected 0 detections after clear")
	}
}

func TestDeadlockDetectorGetWaitEdges(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Set up waits
	detector.RecordWait(2, "resource1", []uint64{1})
	detector.RecordWait(3, "resource2", []uint64{2})

	edges := detector.GetWaitEdges()

	// Should have 2 edges: 2->1 and 3->2
	if len(edges) != 2 {
		t.Errorf("Expected 2 edges, got %d", len(edges))
	}

	// Verify edge data
	found := false
	for _, edge := range edges {
		if edge.From == 2 && edge.To == 1 && edge.Resource == "resource1" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected edge from 2 to 1 on resource1")
	}
}

func TestDeadlockDetectorIsWaiting(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	if detector.IsWaiting(1) {
		t.Error("Expected txn 1 to not be waiting initially")
	}

	detector.RecordWait(1, "resource1", []uint64{2})

	if !detector.IsWaiting(1) {
		t.Error("Expected txn 1 to be waiting")
	}

	detector.RecordLockGranted(1, "resource1")

	if detector.IsWaiting(1) {
		t.Error("Expected txn 1 to not be waiting after lock granted")
	}
}

func TestLockMonitor(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	monitor := NewLockMonitor(detector, 100*time.Millisecond)
	monitor.Start()

	// Let it run for a bit
	time.Sleep(50 * time.Millisecond)

	// Stop the monitor
	monitor.Stop()
}

func TestDefaultDeadlockConfig(t *testing.T) {
	config := DefaultDeadlockConfig()

	if !config.Enabled {
		t.Error("Expected enabled by default")
	}

	if config.DetectInterval != 1*time.Second {
		t.Errorf("Expected interval 1s, got %v", config.DetectInterval)
	}

	if config.VictimStrategy != VictimYoungest {
		t.Errorf("Expected VictimYoungest strategy, got %v", config.VictimStrategy)
	}
}

func TestDeadlockDetectorNilConfig(t *testing.T) {
	mgr := NewManager(nil, nil)
	detector := NewDeadlockDetector(mgr, nil)

	if detector == nil {
		t.Fatal("Expected detector to be created")
	}

	if !detector.IsEnabled() {
		t.Error("Expected detector to be enabled with default config")
	}
}

func TestDeadlockDetectorMultipleResources(t *testing.T) {
	mgr := NewManager(nil, nil)
	config := DefaultDeadlockConfig()
	detector := NewDeadlockDetector(mgr, config)

	// Set up a complex wait scenario with multiple resources
	detector.RecordWait(2, "resource1", []uint64{1})
	detector.RecordWait(3, "resource1", []uint64{1})
	detector.RecordWait(1, "resource2", []uint64{2})

	// This creates: 3 -> 1 -> 2 (3 waits for 1, 1 waits for 2)
	// And: 2 -> 1 (2 waits for 1)

	graph := detector.GetWaitForGraph()
	if len(graph[1]) != 1 {
		t.Errorf("Expected txn 1 to wait for 1 txn, got %d", len(graph[1]))
	}
	if len(graph[2]) != 1 {
		t.Errorf("Expected txn 2 to wait for 1 txn, got %d", len(graph[2]))
	}
	if len(graph[3]) != 1 {
		t.Errorf("Expected txn 3 to wait for 1 txn, got %d", len(graph[3]))
	}
}
