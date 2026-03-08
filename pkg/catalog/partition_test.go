package catalog

import (
	"testing"
	"time"
)

func TestPartitionManagerCreateRangePartitionedTable(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
		{Name: "p2", LowerBound: 100, UpperBound: 200},
		{Name: "p3", LowerBound: 200, UpperBound: nil},
	}

	pt, err := pm.CreateRangePartitionedTable("test_table", "id", defs)
	if err != nil {
		t.Fatalf("Failed to create partitioned table: %v", err)
	}

	if pt.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", pt.TableName)
	}

	if pt.Type != PartitionTypeRange {
		t.Errorf("Expected type RANGE, got %v", pt.Type)
	}

	if len(pt.Partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(pt.Partitions))
	}
}

func TestPartitionManagerCreateListPartitionedTable(t *testing.T) {
	pm := NewPartitionManager()

	defs := []ListPartitionDef{
		{Name: "p_east", Values: []interface{}{"NY", "NJ", "PA"}},
		{Name: "p_west", Values: []interface{}{"CA", "OR", "WA"}},
		{Name: "p_other", Values: []interface{}{"TX", "FL"}},
	}

	pt, err := pm.CreateListPartitionedTable("regions", "state", defs)
	if err != nil {
		t.Fatalf("Failed to create partitioned table: %v", err)
	}

	if pt.Type != PartitionTypeList {
		t.Errorf("Expected type LIST, got %v", pt.Type)
	}

	if len(pt.Partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(pt.Partitions))
	}
}

func TestPartitionManagerCreateHashPartitionedTable(t *testing.T) {
	pm := NewPartitionManager()

	pt, err := pm.CreateHashPartitionedTable("hash_table", "hash_col", 4)
	if err != nil {
		t.Fatalf("Failed to create hash partitioned table: %v", err)
	}

	if pt.Type != PartitionTypeHash {
		t.Errorf("Expected type HASH, got %v", pt.Type)
	}

	if len(pt.Partitions) != 4 {
		t.Errorf("Expected 4 partitions, got %d", len(pt.Partitions))
	}

	// Check hash modulus
	for _, p := range pt.Partitions {
		if p.HashModulus != 4 {
			t.Errorf("Expected hash modulus 4, got %d", p.HashModulus)
		}
	}
}

func TestPartitionContainsValueRange(t *testing.T) {
	partition := &Partition{
		Type:       PartitionTypeRange,
		LowerBound: 0,
		UpperBound: 100,
	}

	// Value within range
	if !partition.ContainsValue(50) {
		t.Error("Expected 50 to be in range [0, 100)")
	}

	// Value at lower bound (inclusive)
	if !partition.ContainsValue(0) {
		t.Error("Expected 0 to be in range [0, 100)")
	}

	// Value at upper bound (exclusive)
	if partition.ContainsValue(100) {
		t.Error("Expected 100 to not be in range [0, 100)")
	}

	// Value outside range
	if partition.ContainsValue(150) {
		t.Error("Expected 150 to not be in range [0, 100)")
	}
}

func TestPartitionContainsValueRangeUnbounded(t *testing.T) {
	partition := &Partition{
		Type:       PartitionTypeRange,
		LowerBound: 100,
		UpperBound: nil,
	}

	// Value within unbounded range
	if !partition.ContainsValue(150) {
		t.Error("Expected 150 to be in unbounded range")
	}

	// Value below lower bound
	if partition.ContainsValue(50) {
		t.Error("Expected 50 to not be in unbounded range starting at 100")
	}
}

func TestPartitionContainsValueList(t *testing.T) {
	partition := &Partition{
		Type:   PartitionTypeList,
		Values: []interface{}{"NY", "NJ", "PA"},
	}

	// Value in list
	if !partition.ContainsValue("NY") {
		t.Error("Expected 'NY' to be in list")
	}

	// Value not in list
	if partition.ContainsValue("CA") {
		t.Error("Expected 'CA' to not be in list")
	}
}

func TestPartitionContainsValueHash(t *testing.T) {
	partition := &Partition{
		Type:          PartitionTypeHash,
		HashModulus:   4,
		HashRemainder: 1,
	}

	// Values will hash to different partitions
	// Just verify the function works
	foundMatch := false
	for i := 0; i < 100; i++ {
		if partition.ContainsValue(i) {
			foundMatch = true
			break
		}
	}

	if !foundMatch {
		t.Error("Expected at least one value to hash to remainder 1")
	}
}

func TestPartitionedTableGetPartitionForValue(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
		{Name: "p2", LowerBound: 100, UpperBound: 200},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Get partition for value
	p := pt.GetPartitionForValue(50)
	if p == nil {
		t.Fatal("Expected to find partition for value 50")
	}
	if p.Name != "p1" {
		t.Errorf("Expected partition 'p1', got '%s'", p.Name)
	}

	p = pt.GetPartitionForValue(150)
	if p == nil {
		t.Fatal("Expected to find partition for value 150")
	}
	if p.Name != "p2" {
		t.Errorf("Expected partition 'p2', got '%s'", p.Name)
	}

	p = pt.GetPartitionForValue(250)
	if p != nil {
		t.Error("Expected no partition for value 250")
	}
}

func TestPartitionedTableAddPartition(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Add a new partition
	err := pt.AddPartition("p2", 100, 200)
	if err != nil {
		t.Fatalf("Failed to add partition: %v", err)
	}

	if len(pt.Partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(pt.Partitions))
	}

	// Verify new partition exists
	p := pt.GetPartitionForValue(150)
	if p == nil || p.Name != "p2" {
		t.Error("Expected to find new partition")
	}
}

func TestPartitionedTableDropPartition(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
		{Name: "p2", LowerBound: 100, UpperBound: 200},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Drop a partition
	err := pt.DropPartition("p1")
	if err != nil {
		t.Fatalf("Failed to drop partition: %v", err)
	}

	if len(pt.Partitions) != 1 {
		t.Errorf("Expected 1 partition, got %d", len(pt.Partitions))
	}

	// Verify partition is gone
	p := pt.GetPartitionForValue(50)
	if p != nil {
		t.Error("Expected partition p1 to be dropped")
	}
}

func TestPartitionedTableDropPartitionNotFound(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Try to drop non-existent partition
	err := pt.DropPartition("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent partition")
	}
}

func TestPartitionManagerGetPartitionedTable(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
	}

	pm.CreateRangePartitionedTable("test", "id", defs)

	// Get existing table
	pt, exists := pm.GetPartitionedTable("test")
	if !exists {
		t.Error("Expected table to exist")
	}
	if pt.TableName != "test" {
		t.Errorf("Expected table name 'test', got '%s'", pt.TableName)
	}

	// Get non-existent table
	_, exists = pm.GetPartitionedTable("nonexistent")
	if exists {
		t.Error("Expected table to not exist")
	}
}

func TestPartitionManagerIsPartitionedTable(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
	}

	pm.CreateRangePartitionedTable("test", "id", defs)

	if !pm.IsPartitionedTable("test") {
		t.Error("Expected 'test' to be a partitioned table")
	}

	if pm.IsPartitionedTable("nonexistent") {
		t.Error("Expected 'nonexistent' to not be a partitioned table")
	}
}

func TestPartitionManagerDropPartitionedTable(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
	}

	pm.CreateRangePartitionedTable("test", "id", defs)

	if !pm.IsPartitionedTable("test") {
		t.Fatal("Expected table to exist")
	}

	err := pm.DropPartitionedTable("test")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	if pm.IsPartitionedTable("test") {
		t.Error("Expected table to be dropped")
	}
}

func TestPartitionedTableGetStats(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
		{Name: "p2", LowerBound: 100, UpperBound: 200},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Set some stats
	pt.Partitions[0].RowCount = 50
	pt.Partitions[0].SizeBytes = 1024
	pt.Partitions[1].RowCount = 75
	pt.Partitions[1].SizeBytes = 2048

	stats := pt.GetStats()

	if stats.TotalPartitions != 2 {
		t.Errorf("Expected 2 partitions, got %d", stats.TotalPartitions)
	}

	if stats.TotalRows != 125 {
		t.Errorf("Expected 125 total rows, got %d", stats.TotalRows)
	}

	if stats.TotalSize != 3072 {
		t.Errorf("Expected 3072 total size, got %d", stats.TotalSize)
	}
}

func TestPartitionedTableGetPartitionsForRange(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
		{Name: "p2", LowerBound: 100, UpperBound: 200},
		{Name: "p3", LowerBound: 200, UpperBound: 300},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Query range [50, 150) should overlap with p1 and p2
	partitions := pt.GetPartitionsForRange(50, 150)
	if len(partitions) != 2 {
		t.Errorf("Expected 2 partitions for range [50, 150), got %d", len(partitions))
	}
}

func TestPartitionedTableAutoCreateTimePartition(t *testing.T) {
	pm := NewPartitionManager()

	// Create initial partition
	defs := []RangePartitionDef{
		{Name: "p_initial", LowerBound: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), UpperBound: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "created_at", defs)
	pt.AutoPartition = true
	pt.AutoPartitionInterval = 24 * time.Hour

	// Try to create partition for time within existing range - should not create
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	p, err := pt.AutoCreateTimePartition(now)
	if err != nil {
		t.Fatalf("AutoCreateTimePartition failed: %v", err)
	}
	if p != nil {
		t.Error("Expected no new partition for time within existing range")
	}

	// Try to create partition for time after existing range
	now = time.Date(2024, 1, 2, 1, 0, 0, 0, time.UTC)
	p, err = pt.AutoCreateTimePartition(now)
	if err != nil {
		t.Fatalf("AutoCreateTimePartition failed: %v", err)
	}
	if p == nil {
		t.Fatal("Expected new partition to be created")
	}

	if len(pt.Partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(pt.Partitions))
	}
}

func TestPartitionedTablePrunePartitions(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p_old", LowerBound: 0, UpperBound: 100},
		{Name: "p_new", LowerBound: 100, UpperBound: 200},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Prune partitions with upper bound <= 100
	pruned := pt.PrunePartitions(func(p *Partition) bool {
		if p.UpperBound != nil {
			if ub, ok := p.UpperBound.(int); ok && ub <= 100 {
				return true
			}
		}
		return false
	})

	if pruned != 1 {
		t.Errorf("Expected 1 pruned partition, got %d", pruned)
	}

	if len(pt.Partitions) != 1 {
		t.Errorf("Expected 1 remaining partition, got %d", len(pt.Partitions))
	}
}

func TestPartitionedTableMergePartitions(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 50},
		{Name: "p2", LowerBound: 50, UpperBound: 100},
		{Name: "p3", LowerBound: 100, UpperBound: 200},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Merge p1 and p2
	err := pt.MergePartitions([]string{"p1", "p2"}, "p_merged")
	if err != nil {
		t.Fatalf("Failed to merge partitions: %v", err)
	}

	if len(pt.Partitions) != 2 {
		t.Errorf("Expected 2 partitions after merge, got %d", len(pt.Partitions))
	}

	// Check merged partition
	p := pt.GetPartitionForValue(25)
	if p == nil || p.Name != "p_merged" {
		t.Error("Expected merged partition to cover values from both original partitions")
	}
}

func TestPartitionedTableSplitPartition(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
	}

	pt, _ := pm.CreateRangePartitionedTable("test", "id", defs)

	// Split p1 at 50
	err := pt.SplitPartition("p1", 50, "p1a", "p1b")
	if err != nil {
		t.Fatalf("Failed to split partition: %v", err)
	}

	if len(pt.Partitions) != 2 {
		t.Errorf("Expected 2 partitions after split, got %d", len(pt.Partitions))
	}

	// Check first partition
	p := pt.GetPartitionForValue(25)
	if p == nil || p.Name != "p1a" {
		t.Error("Expected p1a to cover lower half")
	}

	// Check second partition
	p = pt.GetPartitionForValue(75)
	if p == nil || p.Name != "p1b" {
		t.Error("Expected p1b to cover upper half")
	}
}

func TestPartitionedTableRebalanceHashPartitions(t *testing.T) {
	pm := NewPartitionManager()

	pt, _ := pm.CreateHashPartitionedTable("test", "hash_col", 4)

	if len(pt.Partitions) != 4 {
		t.Fatalf("Expected 4 partitions, got %d", len(pt.Partitions))
	}

	// Rebalance to 8 partitions
	err := pt.RebalanceHashPartitions(8)
	if err != nil {
		t.Fatalf("Failed to rebalance partitions: %v", err)
	}

	if len(pt.Partitions) != 8 {
		t.Errorf("Expected 8 partitions after rebalance, got %d", len(pt.Partitions))
	}

	// Check new hash modulus
	for _, p := range pt.Partitions {
		if p.HashModulus != 8 {
			t.Errorf("Expected hash modulus 8, got %d", p.HashModulus)
		}
	}
}

// Note: TestCompareValues is defined in catalog_more_test.go

func TestPartitionPrunerPrunePartitionsForQuery(t *testing.T) {
	pm := NewPartitionManager()

	defs := []RangePartitionDef{
		{Name: "p1", LowerBound: 0, UpperBound: 100},
		{Name: "p2", LowerBound: 100, UpperBound: 200},
	}

	pm.CreateRangePartitionedTable("test", "id", defs)

	pruner := NewPartitionPruner(pm)

	// Prune for specific value
	partitions, err := pruner.PrunePartitionsForQuery("test", 50)
	if err != nil {
		t.Fatalf("PrunePartitionsForQuery failed: %v", err)
	}

	if len(partitions) != 1 {
		t.Errorf("Expected 1 partition, got %d", len(partitions))
	}

	if partitions[0].Name != "p1" {
		t.Errorf("Expected partition p1, got %s", partitions[0].Name)
	}

	// Prune without value (should return all)
	partitions, err = pruner.PrunePartitionsForQuery("test", nil)
	if err != nil {
		t.Fatalf("PrunePartitionsForQuery failed: %v", err)
	}

	if len(partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(partitions))
	}
}

func TestPartitionTypeString(t *testing.T) {
	tests := []struct {
		pt       PartitionType
		expected string
	}{
		{PartitionTypeNone, "NONE"},
		{PartitionTypeRange, "RANGE"},
		{PartitionTypeList, "LIST"},
		{PartitionTypeHash, "HASH"},
	}

	for _, tc := range tests {
		if tc.pt.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.pt.String())
		}
	}
}
