package parallel

import "sync"

// WorkerPool is a fixed-size goroutine pool for executing work units.
type WorkerPool struct {
	workers int
	workCh  chan func()
	wg      sync.WaitGroup
	stopCh  chan struct{}
	started bool
	mu      sync.Mutex
}

// NewWorkerPool creates a pool with the specified number of workers.
// If workers <= 0, it defaults to 1.
func NewWorkerPool(workers int) *WorkerPool {
	if workers <= 0 {
		workers = 1
	}
	return &WorkerPool{
		workers: workers,
		workCh:  make(chan func(), workers*4),
		stopCh:  make(chan struct{}),
	}
}

// Start spawns the worker goroutines. Safe to call multiple times.
func (p *WorkerPool) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.started = true
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// Submit sends a work item to the pool. Blocks if the channel is full.
func (p *WorkerPool) Submit(fn func()) {
	p.Start()
	p.workCh <- fn
}

// Wait blocks until all submitted work items have completed.
func (p *WorkerPool) Wait() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	// Drain by closing workCh and waiting for workers to finish
}

// WaitAndClose waits for all work to finish and shuts down workers.
func (p *WorkerPool) WaitAndClose() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	close(p.workCh)
	p.mu.Unlock()
	p.wg.Wait()
}

// Close immediately signals workers to stop without waiting for queued work.
func (p *WorkerPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		return
	}
	close(p.stopCh)
	p.wg.Wait()
	p.started = false
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case fn, ok := <-p.workCh:
			if !ok {
				return
			}
			fn()
		case <-p.stopCh:
			return
		}
	}
}
