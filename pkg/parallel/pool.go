package parallel

import "sync"

// WorkerPool is a fixed-size goroutine pool for executing work units.
type WorkerPool struct {
	workers int
	workCh  chan func()
	wg      sync.WaitGroup
	pending sync.WaitGroup
	stopCh  chan struct{}
	started bool
	closed  bool
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
	if p.started || p.closed {
		return
	}
	p.startLocked()
}

func (p *WorkerPool) startLocked() {
	p.started = true
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// Submit sends a work item to the pool. It blocks if the channel is full and
// returns without queuing when the pool has already been closed.
func (p *WorkerPool) Submit(fn func()) {
	_ = p.TrySubmit(fn)
}

// TrySubmit sends a work item to the pool and reports whether it was queued.
func (p *WorkerPool) TrySubmit(fn func()) bool {
	if fn == nil {
		return false
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false
	}
	if !p.started {
		p.startLocked()
	}
	p.pending.Add(1)
	workCh := p.workCh
	stopCh := p.stopCh
	p.mu.Unlock()

	select {
	case workCh <- fn:
		return true
	case <-stopCh:
		p.pending.Done()
		return false
	}
}

// Wait blocks until all submitted work items have completed.
func (p *WorkerPool) Wait() {
	p.pending.Wait()
}

// WaitAndClose waits for all work to finish and shuts down workers.
func (p *WorkerPool) WaitAndClose() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.Wait()
		return
	}
	p.closed = true
	if !p.started {
		close(p.stopCh)
		p.mu.Unlock()
		return
	}
	workCh := p.workCh
	p.mu.Unlock()

	p.Wait()
	close(workCh)
	close(p.stopCh)
	p.wg.Wait()

	p.mu.Lock()
	p.started = false
	p.mu.Unlock()
}

// Close immediately signals workers to stop without waiting for queued work.
func (p *WorkerPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	if !p.started {
		close(p.stopCh)
		p.mu.Unlock()
		return
	}
	stopCh := p.stopCh
	workCh := p.workCh
	close(stopCh)
	p.mu.Unlock()

	p.wg.Wait()
	for {
		select {
		case <-workCh:
			p.pending.Done()
		default:
			p.mu.Lock()
			p.started = false
			p.mu.Unlock()
			return
		}
	}
}

func (p *WorkerPool) run(fn func()) {
	defer func() {
		_ = recover()
		p.pending.Done()
	}()
	fn()
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case fn, ok := <-p.workCh:
			if !ok {
				return
			}
			p.run(fn)
		case <-p.stopCh:
			return
		}
	}
}
