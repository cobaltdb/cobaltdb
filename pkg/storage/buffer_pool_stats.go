package storage

import (
	"sync"
	"sync/atomic"
	"time"
)

// BufferPoolStats holds buffer pool statistics
type BufferPoolStats struct {
	// Basic stats
	Capacity    int   `json:"capacity"`
	PageCount   int   `json:"page_count"`
	DirtyCount  int   `json:"dirty_count"`
	PinnedCount int32 `json:"pinned_count"`
	FreeCount   int   `json:"free_count"`

	// Hit/miss stats
	HitCount  uint64  `json:"hit_count"`
	MissCount uint64  `json:"miss_count"`
	HitRatio  float64 `json:"hit_ratio"`

	// I/O stats
	ReadCount     uint64 `json:"read_count"`
	WriteCount    uint64 `json:"write_count"`
	EvictionCount uint64 `json:"eviction_count"`

	// Performance
	AvgReadTime  float64 `json:"avg_read_time_ms"`
	AvgWriteTime float64 `json:"avg_write_time_ms"`

	// Timestamp
	CollectedAt time.Time `json:"collected_at"`
}

// bufferPoolStatsCollector collects statistics for the buffer pool
type bufferPoolStatsCollector struct {
	hitCount      uint64
	missCount     uint64
	readCount     uint64
	writeCount    uint64
	evictionCount uint64
	readTimes     []time.Duration // Circular buffer for recent read times
	writeTimes    []time.Duration // Circular buffer for recent write times
	timeIndex     int
	maxSamples    int

	mu sync.RWMutex
}

// newBufferPoolStatsCollector creates a new stats collector
func newBufferPoolStatsCollector() *bufferPoolStatsCollector {
	return &bufferPoolStatsCollector{
		readTimes:  make([]time.Duration, 0, 100),
		writeTimes: make([]time.Duration, 0, 100),
		maxSamples: 100,
	}
}

// recordHit records a cache hit
func (sc *bufferPoolStatsCollector) recordHit() {
	atomic.AddUint64(&sc.hitCount, 1)
}

// recordMiss records a cache miss
func (sc *bufferPoolStatsCollector) recordMiss() {
	atomic.AddUint64(&sc.missCount, 1)
}

// recordRead records a disk read
func (sc *bufferPoolStatsCollector) recordRead(duration time.Duration) {
	atomic.AddUint64(&sc.readCount, 1)
	sc.addReadTime(duration)
}

// recordWrite records a disk write
func (sc *bufferPoolStatsCollector) recordWrite(duration time.Duration) {
	atomic.AddUint64(&sc.writeCount, 1)
	sc.addWriteTime(duration)
}

// recordEviction records a page eviction
func (sc *bufferPoolStatsCollector) recordEviction() {
	atomic.AddUint64(&sc.evictionCount, 1)
}

// addReadTime adds a read time sample (thread-safe with lock)
func (sc *bufferPoolStatsCollector) addReadTime(d time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if len(sc.readTimes) < sc.maxSamples {
		sc.readTimes = append(sc.readTimes, d)
	} else {
		sc.readTimes[sc.timeIndex%sc.maxSamples] = d
	}
	sc.timeIndex++
}

// addWriteTime adds a write time sample (thread-safe with lock)
func (sc *bufferPoolStatsCollector) addWriteTime(d time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if len(sc.writeTimes) < sc.maxSamples {
		sc.writeTimes = append(sc.writeTimes, d)
	} else {
		sc.writeTimes[sc.timeIndex%sc.maxSamples] = d
	}
	sc.timeIndex++
}

// getAvgReadTime returns average read time in milliseconds
func (sc *bufferPoolStatsCollector) getAvgReadTime() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if len(sc.readTimes) == 0 {
		return 0
	}

	var sum time.Duration
	for _, t := range sc.readTimes {
		sum += t
	}
	return float64(sum.Milliseconds()) / float64(len(sc.readTimes))
}

// getAvgWriteTime returns average write time in milliseconds
func (sc *bufferPoolStatsCollector) getAvgWriteTime() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if len(sc.writeTimes) == 0 {
		return 0
	}

	var sum time.Duration
	for _, t := range sc.writeTimes {
		sum += t
	}
	return float64(sum.Milliseconds()) / float64(len(sc.writeTimes))
}

// getHitRatio returns the cache hit ratio
func (sc *bufferPoolStatsCollector) getHitRatio() float64 {
	hits := atomic.LoadUint64(&sc.hitCount)
	misses := atomic.LoadUint64(&sc.missCount)
	total := hits + misses

	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// Stats returns the collected statistics
func (bp *BufferPool) Stats() BufferPoolStats {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	// Count dirty and pinned pages
	var dirtyCount, pinnedCount int32
	for _, page := range bp.pages {
		if page.IsDirty() {
			dirtyCount++
		}
		if page.IsPinned() {
			pinnedCount++
		}
	}

	return BufferPoolStats{
		Capacity:      bp.capacity,
		PageCount:     len(bp.pages),
		DirtyCount:    int(dirtyCount),
		PinnedCount:   pinnedCount,
		FreeCount:     bp.capacity - len(bp.pages),
		HitCount:      atomic.LoadUint64(&bp.stats.hitCount),
		MissCount:     atomic.LoadUint64(&bp.stats.missCount),
		HitRatio:      bp.stats.getHitRatio(),
		ReadCount:     atomic.LoadUint64(&bp.stats.readCount),
		WriteCount:    atomic.LoadUint64(&bp.stats.writeCount),
		EvictionCount: atomic.LoadUint64(&bp.stats.evictionCount),
		AvgReadTime:   bp.stats.getAvgReadTime(),
		AvgWriteTime:  bp.stats.getAvgWriteTime(),
		CollectedAt:   time.Now(),
	}
}

// HitCount returns the total number of cache hits
func (bp *BufferPool) HitCount() uint64 {
	return atomic.LoadUint64(&bp.stats.hitCount)
}

// MissCount returns the total number of cache misses
func (bp *BufferPool) MissCount() uint64 {
	return atomic.LoadUint64(&bp.stats.missCount)
}

// EvictionCount returns the total number of page evictions
func (bp *BufferPool) EvictionCount() uint64 {
	return atomic.LoadUint64(&bp.stats.evictionCount)
}
