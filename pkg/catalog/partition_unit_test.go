package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestPartitionTypeValues(t *testing.T) {
	// Verify partition type constants
	t.Logf("PartitionTypeNone = %d", query.PartitionTypeNone)
	t.Logf("PartitionTypeRange = %d", query.PartitionTypeRange)
	t.Logf("PartitionTypeList = %d", query.PartitionTypeList)
	t.Logf("PartitionTypeHash = %d", query.PartitionTypeHash)
	t.Logf("PartitionTypeKey = %d", query.PartitionTypeKey)

	// Test getPartitionTreeName
	table := &TableDef{
		Name: "test",
		Partition: &PartitionInfo{
			Type:     query.PartitionTypeHash,
			Column:   "id",
			NumParts: 4,
			Partitions: []PartitionDef{
				{Name: "p0"},
				{Name: "p1"},
				{Name: "p2"},
				{Name: "p3"},
			},
		},
	}

	// Test HASH partitioning
	treeName := table.getPartitionTreeName(int64(1))
	t.Logf("Value 1 -> %s", treeName)
	if treeName == "" {
		t.Error("Expected tree name for value 1, got empty")
	}

	treeName = table.getPartitionTreeName(int64(5))
	t.Logf("Value 5 -> %s", treeName)
	if treeName == "" {
		t.Error("Expected tree name for value 5, got empty")
	}
}

func TestPartitionTypeRange(t *testing.T) {
	table := &TableDef{
		Name: "test",
		Partition: &PartitionInfo{
			Type:   query.PartitionTypeRange,
			Column: "year",
			Partitions: []PartitionDef{
				{Name: "p0", MinValue: -9223372036854775808, MaxValue: 2020},
				{Name: "p1", MinValue: 2020, MaxValue: 2030},
			},
		},
	}

	treeName := table.getPartitionTreeName(int64(2019))
	t.Logf("Value 2019 -> %s", treeName)
	if treeName != "test:p0" {
		t.Errorf("Expected test:p0, got %s", treeName)
	}

	treeName = table.getPartitionTreeName(int64(2025))
	t.Logf("Value 2025 -> %s", treeName)
	if treeName != "test:p1" {
		t.Errorf("Expected test:p1, got %s", treeName)
	}
}
