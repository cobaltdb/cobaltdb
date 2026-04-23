package wasm

import (
	"encoding/binary"
	"testing"
)

// TestProfilingSupport tests query performance profiling
func TestProfilingSupport(t *testing.T) {
	t.Run("get_query_metrics", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		outPtr := int32(1024)
		params := []uint64{uint64(outPtr)}
		result, err := host.getQueryMetrics(rt, params)
		if err != nil {
			t.Fatalf("getQueryMetrics failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Read metrics: [totalExecs, totalTime, minTime, maxTime, avgTime]
		totalExecs := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		totalTime := binary.LittleEndian.Uint64(rt.Memory[outPtr+8:])
		minTime := binary.LittleEndian.Uint64(rt.Memory[outPtr+16:])
		maxTime := binary.LittleEndian.Uint64(rt.Memory[outPtr+24:])
		avgTime := binary.LittleEndian.Uint64(rt.Memory[outPtr+32:])

		t.Logf("Query metrics: execs=%d, total=%dns, min=%dns, max=%dns, avg=%dns",
			totalExecs, totalTime, minTime, maxTime, avgTime)
	})

	t.Run("get_memory_stats", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		outPtr := int32(1024)
		params := []uint64{uint64(outPtr)}
		result, err := host.getMemoryStats(rt, params)
		if err != nil {
			t.Fatalf("getMemoryStats failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		totalMem := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		usedMem := binary.LittleEndian.Uint64(rt.Memory[outPtr+8:])
		peakMem := binary.LittleEndian.Uint64(rt.Memory[outPtr+16:])
		allocCount := binary.LittleEndian.Uint64(rt.Memory[outPtr+24:])

		t.Logf("Memory stats: total=%d, used=%d, peak=%d, allocs=%d",
			totalMem, usedMem, peakMem, allocCount)
	})

	t.Run("reset_metrics", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		params := []uint64{}
		result, err := host.resetMetrics(rt, params)
		if err != nil {
			t.Fatalf("resetMetrics failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		t.Log("Metrics reset successfully")
	})

	t.Run("log_profiling_event", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Log a query_end event
		params := []uint64{1, 50000, 100} // eventType=1 (query_end), duration=50000ns, rows=100
		result, err := host.logProfilingEvent(rt, params)
		if err != nil {
			t.Fatalf("logProfilingEvent failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		t.Log("Profiling event logged successfully")
	})

	t.Run("get_opcode_stats", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		outPtr := int32(1024)
		maxOpcodes := 10

		params := []uint64{uint64(outPtr), uint64(maxOpcodes)}
		result, err := host.getOpcodeStats(rt, params)
		if err != nil {
			t.Fatalf("getOpcodeStats failed: %v", err)
		}

		count := result[0]
		if count == 0 {
			t.Error("Expected non-zero opcode count")
		}

		t.Logf("Opcode stats: returned %d opcodes", count)

		// Read first opcode stat
		opcode := rt.Memory[outPtr]
		opCount := binary.LittleEndian.Uint64(rt.Memory[outPtr+8:])
		t.Logf("First opcode: 0x%02x, count: %d", opcode, opCount)
	})

	t.Run("query_profiler_creation", func(t *testing.T) {
		profiler := NewQueryProfiler()

		if profiler.TotalExecutions != 0 {
			t.Errorf("Expected 0 executions initially, got %d", profiler.TotalExecutions)
		}

		if profiler.HistorySize != 100 {
			t.Errorf("Expected history size 100, got %d", profiler.HistorySize)
		}

		t.Log("Query profiler created successfully")
	})

	t.Run("query_profiler_record", func(t *testing.T) {
		profiler := NewQueryProfiler()

		// Record some executions
		profiler.RecordExecution(100000, 50, 1024)
		profiler.RecordExecution(150000, 75, 2048)
		profiler.RecordExecution(80000, 40, 512)

		if profiler.TotalExecutions != 3 {
			t.Errorf("Expected 3 executions, got %d", profiler.TotalExecutions)
		}

		stats := profiler.GetStats()
		t.Logf("Profiler stats: execs=%d, total=%dns, min=%dns, max=%dns, avg=%dns",
			stats.TotalExecutions, stats.TotalDuration,
			stats.MinDuration, stats.MaxDuration, stats.AvgDuration)

		if stats.MinDuration != 80000 {
			t.Errorf("Expected min duration 80000, got %d", stats.MinDuration)
		}

		if stats.MaxDuration != 150000 {
			t.Errorf("Expected max duration 150000, got %d", stats.MaxDuration)
		}
	})

	t.Run("query_profiler_history", func(t *testing.T) {
		profiler := NewQueryProfiler()

		// Record more than history size
		for i := 0; i < 110; i++ {
			profiler.RecordExecution(int64(100000+i), i+1, 1024)
		}

		if len(profiler.History) > profiler.HistorySize {
			t.Errorf("History size %d exceeds limit %d", len(profiler.History), profiler.HistorySize)
		}

		t.Logf("History size: %d (limit: %d)", len(profiler.History), profiler.HistorySize)
	})

	t.Run("benchmark_result_structure", func(t *testing.T) {
		// Test BenchmarkResult structure without actually running benchmark
		result := &BenchmarkResult{
			QueryName:     "test_query",
			Iterations:    100,
			TotalDuration: 5000000000, // 5 seconds
			AvgDuration:   50000000,   // 50ms
			MinDuration:   40000000,   // 40ms
			MaxDuration:   60000000,   // 60ms
			RowsPerSecond: 2000.0,
			Throughput:    20.0,
		}

		if result.Iterations != 100 {
			t.Errorf("Expected 100 iterations, got %d", result.Iterations)
		}

		t.Logf("Benchmark result structure: %s - %d iterations, %.2f rows/sec",
			result.QueryName, result.Iterations, result.RowsPerSecond)
	})

	t.Run("profiling_integration", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Get initial metrics
		outPtr := int32(1024)
		params := []uint64{uint64(outPtr)}
		host.getQueryMetrics(rt, params)

		// Log some events
		host.logProfilingEvent(rt, []uint64{0, 0, 0})       // query_start
		host.logProfilingEvent(rt, []uint64{1, 100000, 50}) // query_end

		// Get memory stats
		host.getMemoryStats(rt, params)

		// Reset metrics
		host.resetMetrics(rt, []uint64{})

		t.Log("Profiling integration test completed")
	})
}
