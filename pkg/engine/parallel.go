package engine

import (
	"context"
	"runtime"
	"sync"
	"time"
)

// ParallelConfig configures query parallelization
type ParallelConfig struct {
	Enabled            bool
	MaxWorkers         int
	MinRowsPerWorker   int
	MaxWorkersPerQuery int
	EnableParallelScan bool
	EnableParallelAgg  bool
	EnableParallelSort bool
	EnableParallelJoin bool
}

// DefaultParallelConfig returns default parallel configuration
func DefaultParallelConfig() *ParallelConfig {
	return &ParallelConfig{
		Enabled:            true,
		MaxWorkers:         runtime.NumCPU(),
		MinRowsPerWorker:   1000,
		MaxWorkersPerQuery: 4,
		EnableParallelScan: true,
		EnableParallelAgg:  true,
		EnableParallelSort: true,
		EnableParallelJoin: false,
	}
}

// ParallelQueryExecutor executes queries in parallel
type ParallelQueryExecutor struct {
	config     *ParallelConfig
	workerPool *WorkerPool
}

// WorkerPool manages a pool of workers for parallel execution
type WorkerPool struct {
	workers  int
	tasks    chan Task
	results  chan TaskResult
	wg       sync.WaitGroup
	stopCh   chan struct{}
	stopOnce sync.Once
}

// Task represents a unit of work
type Task struct {
	ID       int
	Data     interface{}
	Executor func(interface{}) (interface{}, error)
}

// TaskResult represents a task result
type TaskResult struct {
	TaskID int
	Data   interface{}
	Error  error
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers int) *WorkerPool {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	pool := &WorkerPool{
		workers: workers,
		tasks:   make(chan Task, workers*2),
		results: make(chan TaskResult, workers*2),
		stopCh:  make(chan struct{}),
	}

	// Start workers
	for i := 0; i < workers; i++ {
		pool.wg.Add(1)
		go pool.worker(i)
	}

	return pool
}

func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		case task, ok := <-p.tasks:
			if !ok {
				return
			}

			data, err := task.Executor(task.Data)
			p.results <- TaskResult{
				TaskID: task.ID,
				Data:   data,
				Error:  err,
			}
		}
	}
}

// Submit submits a task to the pool
func (p *WorkerPool) Submit(task Task) {
	select {
	case p.tasks <- task:
	case <-p.stopCh:
	}
}

// Results returns the results channel
func (p *WorkerPool) Results() <-chan TaskResult {
	return p.results
}

// Stop stops the worker pool
func (p *WorkerPool) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		close(p.tasks)
		p.wg.Wait()
		close(p.results)
	})
}

// Stats returns worker pool statistics
func (p *WorkerPool) Stats() WorkerPoolStats {
	return WorkerPoolStats{
		Workers: p.workers,
	}
}

// WorkerPoolStats contains worker pool statistics
type WorkerPoolStats struct {
	Workers int `json:"workers"`
}

// NewParallelQueryExecutor creates a new parallel query executor
func NewParallelQueryExecutor(config *ParallelConfig) *ParallelQueryExecutor {
	if config == nil {
		config = DefaultParallelConfig()
	}

	return &ParallelQueryExecutor{
		config:     config,
		workerPool: NewWorkerPool(config.MaxWorkers),
	}
}

// Close closes the parallel executor
func (pe *ParallelQueryExecutor) Close() {
	pe.workerPool.Stop()
}

// ParallelScanResult represents a parallel scan result
type ParallelScanResult struct {
	Partitions [][]interface{}
	Errors     []error
}

// ParallelScan performs a parallel scan of data
func (pe *ParallelQueryExecutor) ParallelScan(ctx context.Context, data []interface{}, processor func([]interface{}) ([]interface{}, error)) ([][]interface{}, error) {
	if !pe.config.Enabled || !pe.config.EnableParallelScan {
		// Fallback to sequential processing
		result, err := processor(data)
		if err != nil {
			return nil, err
		}
		return [][]interface{}{result}, nil
	}

	// Determine number of partitions
	numPartitions := pe.calculatePartitions(len(data))
	if numPartitions <= 1 {
		result, err := processor(data)
		if err != nil {
			return nil, err
		}
		return [][]interface{}{result}, nil
	}

	// Split data into partitions
	partitions := splitData(data, numPartitions)

	// Process partitions in parallel
	var wg sync.WaitGroup
	results := make([][]interface{}, numPartitions)
	errors := make([]error, numPartitions)

	for i, partition := range partitions {
		wg.Add(1)
		go func(idx int, p []interface{}) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				errors[idx] = ctx.Err()
				return
			default:
			}

			result, err := processor(p)
			if err != nil {
				errors[idx] = err
				return
			}
			results[idx] = result
		}(i, partition)
	}

	wg.Wait()

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// ParallelAggregate performs parallel aggregation
func (pe *ParallelQueryExecutor) ParallelAggregate(ctx context.Context, data []interface{}, groupBy func(interface{}) string, aggregate func([]interface{}) interface{}) (map[string]interface{}, error) {
	if !pe.config.Enabled || !pe.config.EnableParallelAgg {
		// Fallback to sequential processing
		return pe.sequentialAggregate(data, groupBy, aggregate)
	}

	// First pass: group data in parallel
	numPartitions := pe.calculatePartitions(len(data))
	if numPartitions <= 1 {
		return pe.sequentialAggregate(data, groupBy, aggregate)
	}

	partitions := splitData(data, numPartitions)

	// Phase 1: Local aggregation per partition
	type localAgg struct {
		groups map[string][]interface{}
	}

	localResults := make([]localAgg, numPartitions)
	var wg sync.WaitGroup

	for i, partition := range partitions {
		wg.Add(1)
		go func(idx int, p []interface{}) {
			defer wg.Done()

			groups := make(map[string][]interface{})
			for _, item := range p {
				key := groupBy(item)
				groups[key] = append(groups[key], item)
			}
			localResults[idx].groups = groups
		}(i, partition)
	}

	wg.Wait()

	// Phase 2: Merge groups and final aggregation
	mergedGroups := make(map[string][]interface{})
	for _, local := range localResults {
		for key, items := range local.groups {
			mergedGroups[key] = append(mergedGroups[key], items...)
		}
	}

	// Phase 3: Parallel final aggregation
	results := make(map[string]interface{})
	resultMu := sync.Mutex{}

	wg = sync.WaitGroup{}
	for key, items := range mergedGroups {
		wg.Add(1)
		go func(k string, vals []interface{}) {
			defer wg.Done()

			result := aggregate(vals)
			resultMu.Lock()
			results[k] = result
			resultMu.Unlock()
		}(key, items)
	}

	wg.Wait()

	return results, nil
}

func (pe *ParallelQueryExecutor) sequentialAggregate(data []interface{}, groupBy func(interface{}) string, aggregate func([]interface{}) interface{}) (map[string]interface{}, error) {
	groups := make(map[string][]interface{})
	for _, item := range data {
		key := groupBy(item)
		groups[key] = append(groups[key], item)
	}

	results := make(map[string]interface{})
	for key, items := range groups {
		results[key] = aggregate(items)
	}

	return results, nil
}

// ParallelSort performs a parallel sort
func (pe *ParallelQueryExecutor) ParallelSort(ctx context.Context, data []interface{}, less func(a, b interface{}) bool) ([]interface{}, error) {
	if !pe.config.Enabled || !pe.config.EnableParallelSort || len(data) < pe.config.MinRowsPerWorker*2 {
		// Sequential sort for small datasets
		return pe.sequentialSort(data, less), nil
	}

	// Use sample sort algorithm for parallel sorting
	numPartitions := pe.calculatePartitions(len(data))
	if numPartitions <= 1 {
		return pe.sequentialSort(data, less), nil
	}

	// Phase 1: Sort partitions in parallel
	partitions := splitData(data, numPartitions)
	sortedPartitions := make([][]interface{}, numPartitions)
	var wg sync.WaitGroup

	for i, partition := range partitions {
		wg.Add(1)
		go func(idx int, p []interface{}) {
			defer wg.Done()
			sortedPartitions[idx] = pe.sequentialSort(p, less)
		}(i, partition)
	}

	wg.Wait()

	// Phase 2: K-way merge
	return pe.kWayMerge(sortedPartitions, less), nil
}

func (pe *ParallelQueryExecutor) sequentialSort(data []interface{}, less func(a, b interface{}) bool) []interface{} {
	result := make([]interface{}, len(data))
	copy(result, data)

	// Simple quicksort
	pe.quicksort(result, 0, len(result)-1, less)
	return result
}

func (pe *ParallelQueryExecutor) quicksort(data []interface{}, low, high int, less func(a, b interface{}) bool) {
	if low < high {
		pi := pe.partition(data, low, high, less)
		pe.quicksort(data, low, pi-1, less)
		pe.quicksort(data, pi+1, high, less)
	}
}

func (pe *ParallelQueryExecutor) partition(data []interface{}, low, high int, less func(a, b interface{}) bool) int {
	pivot := data[high]
	i := low - 1

	for j := low; j < high; j++ {
		if less(data[j], pivot) {
			i++
			data[i], data[j] = data[j], data[i]
		}
	}

	data[i+1], data[high] = data[high], data[i+1]
	return i + 1
}

func (pe *ParallelQueryExecutor) kWayMerge(partitions [][]interface{}, less func(a, b interface{}) bool) []interface{} {
	// Simple k-way merge using a min-heap would be optimal
	// For now, use a simpler approach: merge pairwise
	if len(partitions) == 0 {
		return nil
	}

	result := partitions[0]
	for i := 1; i < len(partitions); i++ {
		result = pe.mergeTwo(result, partitions[i], less)
	}

	return result
}

func (pe *ParallelQueryExecutor) mergeTwo(a, b []interface{}, less func(a, b interface{}) bool) []interface{} {
	result := make([]interface{}, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if less(a[i], b[j]) {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}

	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// CalculateCost estimates the cost of parallel execution
func (pe *ParallelQueryExecutor) CalculateCost(numRows int) ParallelCost {
	if !pe.config.Enabled {
		return ParallelCost{
			UseParallel: false,
			Workers:     1,
		}
	}

	workers := pe.calculatePartitions(numRows)
	if workers <= 1 {
		return ParallelCost{
			UseParallel: false,
			Workers:     1,
		}
	}

	return ParallelCost{
		UseParallel: true,
		Workers:     workers,
		Estimated:   true,
	}
}

// ParallelCost represents the cost of parallel execution
type ParallelCost struct {
	UseParallel bool
	Workers     int
	Estimated   bool
}

// Helper functions

func (pe *ParallelQueryExecutor) calculatePartitions(numRows int) int {
	if numRows < pe.config.MinRowsPerWorker {
		return 1
	}

	workers := numRows / pe.config.MinRowsPerWorker
	if workers > pe.config.MaxWorkersPerQuery {
		workers = pe.config.MaxWorkersPerQuery
	}
	if workers > pe.config.MaxWorkers {
		workers = pe.config.MaxWorkers
	}
	if workers < 1 {
		workers = 1
	}

	return workers
}

func splitData(data []interface{}, numPartitions int) [][]interface{} {
	if numPartitions <= 1 {
		return [][]interface{}{data}
	}

	partitionSize := len(data) / numPartitions
	if partitionSize == 0 {
		partitionSize = 1
	}

	partitions := make([][]interface{}, 0, numPartitions)
	for i := 0; i < len(data); i += partitionSize {
		end := i + partitionSize
		if end > len(data) || len(partitions) == numPartitions-1 {
			end = len(data)
		}
		partitions = append(partitions, data[i:end])
		if end == len(data) {
			break
		}
	}

	return partitions
}

// Stats returns parallel executor statistics
func (pe *ParallelQueryExecutor) Stats() ParallelStats {
	poolStats := pe.workerPool.Stats()

	return ParallelStats{
		Enabled:    pe.config.Enabled,
		MaxWorkers: pe.config.MaxWorkers,
		Workers:    poolStats.Workers,
	}
}

// ParallelStats contains parallel execution statistics
type ParallelStats struct {
	Enabled    bool `json:"enabled"`
	MaxWorkers int  `json:"max_workers"`
	Workers    int  `json:"workers"`
}

// ParallelQueryStats tracks statistics for a single parallel query
type ParallelQueryStats struct {
	QueryID       string
	StartTime     time.Time
	EndTime       time.Time
	WorkersUsed   int
	RowsProcessed int64
	Errors        []error
}
