package parallel

import (
	"runtime"
	"sync"
)

const maxParallelWorkers = 1024

// defaultWorkers returns a sensible worker count.
func defaultWorkers(requested int) int {
	if requested > 0 {
		if requested > maxParallelWorkers {
			return maxParallelWorkers
		}
		return requested
	}
	if n := runtime.NumCPU(); n > 1 {
		if n > maxParallelWorkers {
			return maxParallelWorkers
		}
		return n
	}
	return 1
}

// chunkSize returns the size of each chunk when splitting n items across workers.
func chunkSize(n, workers int) int {
	if workers <= 1 {
		return n
	}
	sz := (n + workers - 1) / workers
	if sz < 1 {
		sz = 1
	}
	return sz
}

// panicCapture records the first panic raised by a worker goroutine so the
// caller can re-raise it on its own goroutine. Without this, a panic inside a
// worker goroutine (e.g. a bad row decode) is unrecoverable and crashes the
// whole process. Re-raising on the calling goroutine lets the engine's
// query-level recover turn it into a failed query instead.
type panicCapture struct {
	mu  sync.Mutex
	val interface{}
	set bool
}

// recoverWorker must be deferred inside each worker goroutine.
func (pc *panicCapture) recoverWorker() {
	if r := recover(); r != nil {
		pc.mu.Lock()
		if !pc.set {
			pc.val, pc.set = r, true
		}
		pc.mu.Unlock()
	}
}

// repanic re-raises a captured worker panic on the calling goroutine.
func (pc *panicCapture) repanic() {
	pc.mu.Lock()
	set, val := pc.set, pc.val
	pc.mu.Unlock()
	if set {
		panic(val)
	}
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
	var pc panicCapture
	for i := 0; i < numChunks; i++ {
		start := i * sz
		end := start + sz
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(idx, s, e int) {
			defer wg.Done()
			defer pc.recoverWorker()
			results[idx] = processFn(values[s:e])
		}(i, start, end)
	}
	wg.Wait()
	pc.repanic()

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
	var pc panicCapture
	for i := 0; i < numChunks; i++ {
		start := i * sz
		end := start + sz
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(idx, s, e int) {
			defer wg.Done()
			defer pc.recoverWorker()
			localMaps[idx] = groupFn(values[s:e])
		}(i, start, end)
	}
	wg.Wait()
	pc.repanic()

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
	var pc panicCapture
	for i := 0; i < numChunks; i++ {
		start := i * sz
		end := start + sz
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(idx, s, e int) {
			defer wg.Done()
			defer pc.recoverWorker()
			partials[idx] = partialFn(values[s:e])
		}(i, start, end)
	}
	wg.Wait()
	pc.repanic()

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
