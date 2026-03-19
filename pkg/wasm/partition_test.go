package wasm

import (
	"encoding/binary"
	"testing"
)

// TestPartitionSupport tests partitioned query operations
func TestPartitionSupport(t *testing.T) {
	t.Run("get_partition_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write table name to memory
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "test")

		// Call getPartitionCount
		params := []uint64{uint64(tableNamePtr), 4}
		result, err := host.getPartitionCount(rt, params)
		if err != nil {
			t.Fatalf("getPartitionCount failed: %v", err)
		}

		// Should have 2 partitions for test table
		if result[0] != 2 {
			t.Errorf("Expected 2 partitions, got %d", result[0])
		}

		t.Logf("Partition count for 'test' table: %d", result[0])
	})

	t.Run("get_partition_count_nonexistent_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write non-existent table name
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "nonexistent")

		// Call getPartitionCount
		params := []uint64{uint64(tableNamePtr), 11}
		result, err := host.getPartitionCount(rt, params)
		if err != nil {
			t.Fatalf("getPartitionCount failed: %v", err)
		}

		// Non-partitioned tables return 1 (single implicit partition)
		if result[0] != 1 {
			t.Errorf("Expected 1 partition for non-partitioned table, got %d", result[0])
		}

		t.Logf("Partition count for non-partitioned table: %d", result[0])
	})

	t.Run("partition_scan", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write table name to memory
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "test")

		// Output buffer
		outPtr := int32(2048)

		// Scan partition 0
		params := []uint64{uint64(tableNamePtr), 4, 0, uint64(outPtr), 100}
		result, err := host.partitionScan(rt, params)
		if err != nil {
			t.Fatalf("partitionScan failed: %v", err)
		}

		// Partition 0 should have 2 rows
		if result[0] != 2 {
			t.Errorf("Expected 2 rows in partition 0, got %d", result[0])
		}

		// Read row IDs from memory
		for i := uint64(0); i < result[0]; i++ {
			rowId := binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):])
			t.Logf("Partition 0, Row %d: id=%d", i, rowId)
		}

		// Scan partition 1
		params = []uint64{uint64(tableNamePtr), 4, 1, uint64(outPtr), 100}
		result, err = host.partitionScan(rt, params)
		if err != nil {
			t.Fatalf("partitionScan failed: %v", err)
		}

		// Partition 1 should have 1 row
		if result[0] != 1 {
			t.Errorf("Expected 1 row in partition 1, got %d", result[0])
		}

		rowId := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		t.Logf("Partition 1, Row 0: id=%d", rowId)
	})

	t.Run("partition_scan_invalid_partition", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write table name to memory
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "test")

		// Try to scan invalid partition (99)
		outPtr := int32(2048)
		params := []uint64{uint64(tableNamePtr), 4, 99, uint64(outPtr), 100}
		result, err := host.partitionScan(rt, params)
		if err != nil {
			t.Fatalf("partitionScan failed: %v", err)
		}

		if result[0] != 0 {
			t.Errorf("Expected 0 rows for invalid partition, got %d", result[0])
		}

		t.Log("Invalid partition correctly returned 0 rows")
	})

	t.Run("parallel_aggregate_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write table name
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "test")

		// Write column name (not used for COUNT but required)
		colNamePtr := int32(1100)
		copy(rt.Memory[colNamePtr:], "id")

		// Output buffer
		outPtr := int32(2048)

		// Call parallelAggregate with COUNT (type 0)
		params := []uint64{uint64(tableNamePtr), 4, 0, uint64(colNamePtr), 2, uint64(outPtr)}
		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Read result
		count := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		if count != 3 {
			t.Errorf("Expected count=3, got %d", count)
		}

		t.Logf("Parallel COUNT(*) = %d", count)
	})

	t.Run("parallel_aggregate_sum", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Add test data with values
		host.tables["sales"] = []map[string]interface{}{
			{"id": int64(1), "amount": int64(100)},
			{"id": int64(2), "amount": int64(200)},
			{"id": int64(3), "amount": int64(300)},
		}
		// Create partitions for sales table
		host.partitions["sales"] = []Partition{
			{ID: 0, TableName: "sales", StartRow: 0, EndRow: 2},
			{ID: 1, TableName: "sales", StartRow: 2, EndRow: 3},
		}

		// Write table name
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "sales")

		// Write column name
		colNamePtr := int32(1100)
		copy(rt.Memory[colNamePtr:], "amount")

		// Output buffer
		outPtr := int32(2048)

		// Call parallelAggregate with SUM (type 1)
		params := []uint64{uint64(tableNamePtr), 5, 1, uint64(colNamePtr), 6, uint64(outPtr)}
		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Read result
		sum := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		if sum != 600 {
			t.Errorf("Expected sum=600, got %d", sum)
		}

		t.Logf("Parallel SUM(amount) = %d", sum)
	})

	t.Run("parallel_aggregate_min_max", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Output buffer
		outPtr := int32(2048)

		// Write table name
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "test")

		// Write column name
		colNamePtr := int32(1100)
		copy(rt.Memory[colNamePtr:], "id")

		// Call parallelAggregate with MIN (type 3)
		params := []uint64{uint64(tableNamePtr), 4, 3, uint64(colNamePtr), 2, uint64(outPtr)}
		result, err := host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate MIN failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		minVal := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		t.Logf("Parallel MIN(id) = %d", minVal)

		// Call parallelAggregate with MAX (type 4)
		params = []uint64{uint64(tableNamePtr), 4, 4, uint64(colNamePtr), 2, uint64(outPtr)}
		result, err = host.parallelAggregate(rt, params)
		if err != nil {
			t.Fatalf("parallelAggregate MAX failed: %v", err)
		}

		maxVal := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		t.Logf("Parallel MAX(id) = %d", maxVal)
	})

	t.Run("repartition_table", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write table name
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "test")

		// Repartition to 3 partitions
		params := []uint64{uint64(tableNamePtr), 4, 3}
		result, err := host.repartitionTable(rt, params)
		if err != nil {
			t.Fatalf("repartitionTable failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Verify new partition count
		params = []uint64{uint64(tableNamePtr), 4}
		result, err = host.getPartitionCount(rt, params)
		if err != nil {
			t.Fatalf("getPartitionCount failed: %v", err)
		}

		if result[0] != 3 {
			t.Errorf("Expected 3 partitions after repartitioning, got %d", result[0])
		}

		t.Logf("Table repartitioned to %d partitions", result[0])

		// Verify partition boundaries
		partitions := host.partitions["test"]
		for i, p := range partitions {
			t.Logf("Partition %d: rows %d to %d", p.ID, p.StartRow, p.EndRow)
			if p.ID != i {
				t.Errorf("Expected partition ID %d, got %d", i, p.ID)
			}
		}
	})

	t.Run("repartition_table_invalid_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write table name
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "test")

		// Try invalid partition counts
		for _, count := range []uint64{0, 101} {
			params := []uint64{uint64(tableNamePtr), 4, count}
			result, err := host.repartitionTable(rt, params)
			if err != nil {
				t.Fatalf("repartitionTable failed: %v", err)
			}

			if result[0] != 0 {
				t.Errorf("Expected failure (0) for partition count %d, got %d", count, result[0])
			}
		}

		t.Log("Invalid partition counts correctly rejected")
	})

	t.Run("parallel_query_simulation", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Simulate parallel scan across all partitions
		tableNamePtr := int32(1024)
		copy(rt.Memory[tableNamePtr:], "test")

		// Get partition count
		params := []uint64{uint64(tableNamePtr), 4}
		partitionCount, _ := host.getPartitionCount(rt, params)

		totalRows := uint64(0)
		outPtr := int32(2048)

		// Scan each partition
		for i := uint64(0); i < partitionCount[0]; i++ {
			params = []uint64{uint64(tableNamePtr), 4, i, uint64(outPtr), 100}
			result, err := host.partitionScan(rt, params)
			if err != nil {
				t.Fatalf("partitionScan failed for partition %d: %v", i, err)
			}

			t.Logf("Partition %d returned %d rows", i, result[0])
			totalRows += result[0]
		}

		if totalRows != 3 {
			t.Errorf("Expected 3 total rows, got %d", totalRows)
		}

		t.Logf("Parallel scan returned %d total rows across %d partitions", totalRows, partitionCount[0])
	})
}
