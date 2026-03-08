package engine

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPoolBasic(t *testing.T) {
	pool := NewWorkerPool(2)
	defer pool.Stop()

	var counter atomic.Int32

	// Submit some tasks
	for i := 0; i < 5; i++ {
		pool.Submit(Task{
			ID: i,
			Executor: func(data interface{}) (interface{}, error) {
				counter.Add(1)
				return nil, nil
			},
		})
	}

	// Drain results
	for i := 0; i < 5; i++ {
		select {
		case <-pool.Results():
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for results")
		}
	}

	if counter.Load() != 5 {
		t.Errorf("Expected 5 tasks executed, got %d", counter.Load())
	}
}

func TestWorkerPoolStats(t *testing.T) {
	pool := NewWorkerPool(4)
	defer pool.Stop()

	stats := pool.Stats()
	if stats.Workers != 4 {
		t.Errorf("Expected 4 workers, got %d", stats.Workers)
	}
}

func TestParallelQueryExecutorBasic(t *testing.T) {
	config := DefaultParallelConfig()
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	if !pe.config.Enabled {
		t.Error("Expected parallel execution to be enabled")
	}

	if pe.config.MaxWorkers != runtime.NumCPU() {
		t.Errorf("Expected MaxWorkers %d, got %d", runtime.NumCPU(), pe.config.MaxWorkers)
	}
}

func TestParallelScan(t *testing.T) {
	config := DefaultParallelConfig()
	config.MinRowsPerWorker = 10
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	// Create test data
	data := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		data[i] = i
	}

	processor := func(partition []interface{}) ([]interface{}, error) {
		result := make([]interface{}, len(partition))
		for i, v := range partition {
			val := v.(int)
			result[i] = val * 2
		}
		return result, nil
	}

	results, err := pe.ParallelScan(context.Background(), data, processor)
	if err != nil {
		t.Fatalf("ParallelScan failed: %v", err)
	}

	// Count total results
	total := 0
	for _, r := range results {
		total += len(r)
	}

	if total != 100 {
		t.Errorf("Expected 100 results, got %d", total)
	}
}

func TestParallelScanDisabled(t *testing.T) {
	config := DefaultParallelConfig()
	config.Enabled = false
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	data := []interface{}{1, 2, 3, 4, 5}

	processor := func(partition []interface{}) ([]interface{}, error) {
		return partition, nil
	}

	results, err := pe.ParallelScan(context.Background(), data, processor)
	if err != nil {
		t.Fatalf("ParallelScan failed: %v", err)
	}

	// Should return single partition when disabled
	if len(results) != 1 {
		t.Errorf("Expected 1 partition when disabled, got %d", len(results))
	}
}

func TestParallelAggregate(t *testing.T) {
	config := DefaultParallelConfig()
	config.MinRowsPerWorker = 10
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	// Create test data: numbers 0-99
	data := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		data[i] = i
	}

	groupBy := func(item interface{}) string {
		val := item.(int)
		if val < 50 {
			return "low"
		}
		return "high"
	}

	aggregate := func(items []interface{}) interface{} {
		sum := 0
		for _, item := range items {
			sum += item.(int)
		}
		return sum
	}

	results, err := pe.ParallelAggregate(context.Background(), data, groupBy, aggregate)
	if err != nil {
		t.Fatalf("ParallelAggregate failed: %v", err)
	}

	// Verify results
	if len(results) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(results))
	}

	lowSum := results["low"].(int)
	highSum := results["high"].(int)

	// Sum of 0-49 = 1225
	if lowSum != 1225 {
		t.Errorf("Expected low sum 1225, got %d", lowSum)
	}

	// Sum of 50-99 = 3725
	if highSum != 3725 {
		t.Errorf("Expected high sum 3725, got %d", highSum)
	}
}

func TestParallelAggregateDisabled(t *testing.T) {
	config := DefaultParallelConfig()
	config.Enabled = false
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	data := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	groupBy := func(item interface{}) string {
		val := item.(int)
		if val%2 == 0 {
			return "even"
		}
		return "odd"
	}

	aggregate := func(items []interface{}) interface{} {
		return len(items)
	}

	results, err := pe.ParallelAggregate(context.Background(), data, groupBy, aggregate)
	if err != nil {
		t.Fatalf("ParallelAggregate failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(results))
	}
}

func TestParallelSort(t *testing.T) {
	config := DefaultParallelConfig()
	config.MinRowsPerWorker = 10
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	// Create test data in reverse order
	data := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		data[i] = 99 - i
	}

	less := func(a, b interface{}) bool {
		return a.(int) < b.(int)
	}

	sorted, err := pe.ParallelSort(context.Background(), data, less)
	if err != nil {
		t.Fatalf("ParallelSort failed: %v", err)
	}

	if len(sorted) != 100 {
		t.Errorf("Expected 100 sorted items, got %d", len(sorted))
	}

	// Verify sorted order
	for i := 0; i < len(sorted); i++ {
		if sorted[i].(int) != i {
			t.Errorf("Expected item %d at index %d, got %d", i, i, sorted[i].(int))
		}
	}
}

func TestParallelSortSmallDataset(t *testing.T) {
	config := DefaultParallelConfig()
	config.MinRowsPerWorker = 1000
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	// Small dataset should use sequential sort
	data := []interface{}{5, 3, 1, 4, 2}

	less := func(a, b interface{}) bool {
		return a.(int) < b.(int)
	}

	sorted, err := pe.ParallelSort(context.Background(), data, less)
	if err != nil {
		t.Fatalf("ParallelSort failed: %v", err)
	}

	expected := []int{1, 2, 3, 4, 5}
	for i, v := range sorted {
		if v.(int) != expected[i] {
			t.Errorf("Expected %d at index %d, got %d", expected[i], i, v.(int))
		}
	}
}

func TestCalculatePartitions(t *testing.T) {
	config := DefaultParallelConfig()
	config.MinRowsPerWorker = 100
	config.MaxWorkersPerQuery = 8
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	tests := []struct {
		rows     int
		expected int
	}{
		{50, 1},   // Less than min rows per worker
		{100, 1},  // Equal to min rows
		{200, 2},  // 2x min rows
		{800, 8},  // Should cap at MaxWorkersPerQuery
		{1000, 8}, // Should still cap
	}

	for _, tc := range tests {
		workers := pe.calculatePartitions(tc.rows)
		if workers != tc.expected {
			t.Errorf("Rows %d: expected %d partitions, got %d", tc.rows, tc.expected, workers)
		}
	}
}

func TestSplitData(t *testing.T) {
	data := make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		data[i] = i
	}

	partitions := splitData(data, 3)
	if len(partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(partitions))
	}

	// Count total items
	total := 0
	for _, p := range partitions {
		total += len(p)
	}
	if total != 10 {
		t.Errorf("Expected 10 total items, got %d", total)
	}
}

func TestCalculateCost(t *testing.T) {
	config := DefaultParallelConfig()
	config.MinRowsPerWorker = 100
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	// Large dataset - should use parallel
	cost := pe.CalculateCost(1000)
	if !cost.UseParallel {
		t.Error("Expected UseParallel=true for large dataset")
	}
	if cost.Workers < 2 {
		t.Error("Expected multiple workers for large dataset")
	}

	// Small dataset - should not use parallel
	cost = pe.CalculateCost(50)
	if cost.UseParallel {
		t.Error("Expected UseParallel=false for small dataset")
	}
	if cost.Workers != 1 {
		t.Error("Expected 1 worker for small dataset")
	}
}

func TestCalculateCostDisabled(t *testing.T) {
	config := DefaultParallelConfig()
	config.Enabled = false
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	cost := pe.CalculateCost(10000)
	if cost.UseParallel {
		t.Error("Expected UseParallel=false when disabled")
	}
	if cost.Workers != 1 {
		t.Error("Expected 1 worker when disabled")
	}
}

func TestDefaultParallelConfig(t *testing.T) {
	config := DefaultParallelConfig()

	if !config.Enabled {
		t.Error("Expected enabled by default")
	}

	if config.MaxWorkers != runtime.NumCPU() {
		t.Errorf("Expected MaxWorkers %d, got %d", runtime.NumCPU(), config.MaxWorkers)
	}

	if config.MinRowsPerWorker != 1000 {
		t.Errorf("Expected MinRowsPerWorker 1000, got %d", config.MinRowsPerWorker)
	}

	if config.MaxWorkersPerQuery != 4 {
		t.Errorf("Expected MaxWorkersPerQuery 4, got %d", config.MaxWorkersPerQuery)
	}
}

func TestParallelQueryExecutorNilConfig(t *testing.T) {
	pe := NewParallelQueryExecutor(nil)
	defer pe.Close()

	if pe.config == nil {
		t.Fatal("Expected config to be set")
	}

	if !pe.config.Enabled {
		t.Error("Expected enabled by default")
	}
}

func TestParallelQueryExecutorStats(t *testing.T) {
	config := DefaultParallelConfig()
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	stats := pe.Stats()

	if !stats.Enabled {
		t.Error("Expected stats.Enabled=true")
	}

	if stats.MaxWorkers != config.MaxWorkers {
		t.Errorf("Expected MaxWorkers %d, got %d", config.MaxWorkers, stats.MaxWorkers)
	}
}

func TestParallelScanContextCancellation(t *testing.T) {
	config := DefaultParallelConfig()
	config.MinRowsPerWorker = 10
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	ctx, cancel := context.WithCancel(context.Background())

	data := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = i
	}

	processor := func(partition []interface{}) ([]interface{}, error) {
		time.Sleep(100 * time.Millisecond)
		return partition, nil
	}

	// Cancel context immediately
	cancel()

	_, err := pe.ParallelScan(ctx, data, processor)
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestSequentialSort(t *testing.T) {
	pe := NewParallelQueryExecutor(DefaultParallelConfig())
	defer pe.Close()

	data := []interface{}{5, 3, 8, 1, 9, 2}
	less := func(a, b interface{}) bool {
		return a.(int) < b.(int)
	}

	sorted := pe.sequentialSort(data, less)

	expected := []int{1, 2, 3, 5, 8, 9}
	for i, v := range sorted {
		if v.(int) != expected[i] {
			t.Errorf("Expected %d at index %d, got %d", expected[i], i, v.(int))
		}
	}
}

func TestMergeTwo(t *testing.T) {
	pe := NewParallelQueryExecutor(DefaultParallelConfig())
	defer pe.Close()

	a := []interface{}{1, 3, 5}
	b := []interface{}{2, 4, 6}
	less := func(a, b interface{}) bool {
		return a.(int) < b.(int)
	}

	merged := pe.mergeTwo(a, b, less)

	expected := []int{1, 2, 3, 4, 5, 6}
	if len(merged) != len(expected) {
		t.Fatalf("Expected %d items, got %d", len(expected), len(merged))
	}

	for i, v := range merged {
		if v.(int) != expected[i] {
			t.Errorf("Expected %d at index %d, got %d", expected[i], i, v.(int))
		}
	}
}

func BenchmarkParallelScan(b *testing.B) {
	config := DefaultParallelConfig()
	pe := NewParallelQueryExecutor(config)
	defer pe.Close()

	data := make([]interface{}, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = i
	}

	processor := func(partition []interface{}) ([]interface{}, error) {
		result := make([]interface{}, len(partition))
		for i, v := range partition {
			result[i] = v.(int) * 2
		}
		return result, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pe.ParallelScan(context.Background(), data, processor)
	}
}

func BenchmarkSequentialScan(b *testing.B) {
	data := make([]interface{}, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = i
	}

	processor := func(partition []interface{}) ([]interface{}, error) {
		result := make([]interface{}, len(partition))
		for i, v := range partition {
			result[i] = v.(int) * 2
		}
		return result, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor(data)
	}
}
