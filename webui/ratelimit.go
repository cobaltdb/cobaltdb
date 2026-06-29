package main

import (
	"sync"
	"time"
)

// tokenBucket is a single refilling bucket. capacity is the burst size; refill
// adds `rate` tokens per second up to capacity.
type tokenBucket struct {
	tokens   float64
	capacity float64
	rate     float64
	last     time.Time
}

func (b *tokenBucket) allow(now time.Time) bool {
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// rateLimiter holds one token bucket per key (principal ID). It is safe for
// concurrent use and self-prunes idle buckets to bound memory.
type rateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*tokenBucket
	capacity float64
	rate     float64
	now      func() time.Time
	lastGC   time.Time
}

// newRateLimiter builds a limiter allowing `perMinute` requests per key with a
// burst of `burst`. A non-positive perMinute disables limiting (allow always).
func newRateLimiter(perMinute, burst int) *rateLimiter {
	rl := &rateLimiter{
		buckets: make(map[string]*tokenBucket),
		now:     time.Now,
	}
	if perMinute > 0 {
		rl.rate = float64(perMinute) / 60.0
		rl.capacity = float64(burst)
		if rl.capacity < 1 {
			rl.capacity = 1
		}
	}
	return rl
}

func (rl *rateLimiter) clock() time.Time {
	if rl.now != nil {
		return rl.now()
	}
	return time.Now()
}

// enabled reports whether limiting is active.
func (rl *rateLimiter) enabled() bool {
	return rl != nil && rl.rate > 0
}

// allow consumes one token for key, returning false when the bucket is empty.
func (rl *rateLimiter) allow(key string) bool {
	if !rl.enabled() {
		return true
	}
	now := rl.clock()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.gcLocked(now)

	b, ok := rl.buckets[key]
	if !ok {
		// New bucket starts full minus the request it is about to make.
		b = &tokenBucket{tokens: rl.capacity, capacity: rl.capacity, rate: rl.rate, last: now}
		rl.buckets[key] = b
	}
	return b.allow(now)
}

// gcLocked drops buckets that have been idle long enough to be fully refilled,
// bounding memory under churning key sets. Caller must hold rl.mu.
func (rl *rateLimiter) gcLocked(now time.Time) {
	if now.Sub(rl.lastGC) < time.Minute {
		return
	}
	rl.lastGC = now
	// A bucket idle for capacity/rate seconds is back to full and indistinguishable
	// from a fresh one, so it is safe to drop.
	idleReset := time.Duration(0)
	if rl.rate > 0 {
		idleReset = time.Duration((rl.capacity/rl.rate)*float64(time.Second)) + time.Second
	}
	for k, b := range rl.buckets {
		if now.Sub(b.last) > idleReset {
			delete(rl.buckets, k)
		}
	}
}
