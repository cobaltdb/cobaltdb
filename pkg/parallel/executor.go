package parallel

import (
	"runtime"
	"sync"
)

// defaultWorkers returns a sensible worker count.
func defaultWorkers(requested int) int {
	if requested > 0 {
		return requested
	}
	if n := runtime.NumCPU(); n > 1 {
		return n
	}
	return 1
}

// chunkSize returns the size of each chunk when splitting n items across workers.
func chunkSize(n, workers int) int {
	if workers <= 1 {
		return n
	}
	sz := n / workers
	if sz < 1 {
		sz = 1
	}
	return sz
}

// ParallelSelectRows splits values into chunks and processes them in parallel.
// Results are merged in chunk order for deterministic output.
func ParallelSelectRows(values [][]byte, workers int, threshold int, processFn func([][]byte) [][]interface{}) [][]interface{} {
	n := len(values)
	workers = defaultWorkers(workers)
	if workers <= 1 || n < threshold {
		return processFn(values)
	}

	sz := chunkSize(n, workers)
	numChunks := (n + sz - 1) / sz
	results := make([][][]interface{}, numChunks)

	var wg sync.WaitGroup
	for i := 0; i < numChunks; i++ {
		start := i * sz
		end := start + sz
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(idx, s, e int) {
			defer wg.Done()
			results[idx] = processFn(values[s:e])
		}(i, start, end)
	}
	wg.Wait()

	// Merge in order
	var totalLen int
	for _, r := range results {
		totalLen += len(r)
	}
	merged := make([][]interface{}, 0, totalLen)
	for _, r := range results {
		merged = append(merged, r...)
	}
	return merged
}

// ParallelGroupBy splits values into chunks and groups them in parallel.
// Local maps are merged into a single map; row slices are concatenated.
func ParallelGroupBy(values [][]byte, workers int, threshold int, groupFn func([][]byte) map[string][][]interface{}) map[string][][]interface{} {
	n := len(values)
	workers = defaultWorkers(workers)
	if workers <= 1 || n < threshold {
		return groupFn(values)
	}

	sz := chunkSize(n, workers)
	numChunks := (n + sz - 1) / sz
	localMaps := make([]map[string][][]interface{}, numChunks)

	var wg sync.WaitGroup
	for i := 0; i < numChunks; i++ {
		start := i * sz
		end := start + sz
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(idx, s, e int) {
			defer wg.Done()
			localMaps[idx] = groupFn(values[s:e])
		}(i, start, end)
	}
	wg.Wait()

	// Merge maps
	merged := make(map[string][][]interface{})
	for _, m := range localMaps {
		for k, rows := range m {
			merged[k] = append(merged[k], rows...)
		}
	}
	return merged
}

// ParallelAggregate splits values into chunks and computes partial aggregates in parallel.
// The mergeFn combines partial results into a final result.
func ParallelAggregate(values [][]byte, workers int, threshold int, partialFn func([][]byte) []interface{}, mergeFn func(dst, src []interface{})) []interface{} {
	n := len(values)
	workers = defaultWorkers(workers)
	if workers <= 1 || n < threshold {
		return partialFn(values)
	}

	sz := chunkSize(n, workers)
	numChunks := (n + sz - 1) / sz
	partials := make([][]interface{}, numChunks)

	var wg sync.WaitGroup
	for i := 0; i < numChunks; i++ {
		start := i * sz
		end := start + sz
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(idx, s, e int) {
			defer wg.Done()
			partials[idx] = partialFn(values[s:e])
		}(i, start, end)
	}
	wg.Wait()

	// Merge partials
	var result []interface{}
	for _, p := range partials {
		if result == nil {
			result = p
			continue
		}
		mergeFn(result, p)
	}
	return result
}
