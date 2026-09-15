package server

import (
	"errors"
	"runtime"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

var ErrLoadShed = errors.New("server overloaded; retry later")

const defaultLoadShedQueueDepth = 50

// AdmissionController is shared by wire transports so all SQL entry points
// make the same overload decision before reaching the engine.
type AdmissionController interface {
	Admit(critical bool) error
}

type LoadShedder struct {
	primaryBreaker *engine.CircuitBreaker
	breakers       *engine.CircuitBreakerManager
	queueThreshold int
	goroutineLimit int
	queueDepth     func() int
	numGoroutine   func() int
}

func NewLoadShedder(db *engine.DB, primary *engine.CircuitBreaker, breakers *engine.CircuitBreakerManager, queueThreshold int) *LoadShedder {
	if queueThreshold <= 0 {
		queueThreshold = defaultLoadShedQueueDepth
	}
	baseline := runtime.NumGoroutine()
	if baseline < 1 {
		baseline = 1
	}
	queueDepth := func() int { return 0 }
	if db != nil {
		queueDepth = db.ConnectionQueueDepth
	}
	return &LoadShedder{
		primaryBreaker: primary,
		breakers:       breakers,
		queueThreshold: queueThreshold,
		goroutineLimit: baseline * 2,
		queueDepth:     queueDepth,
		numGoroutine:   runtime.NumGoroutine,
	}
}

// Admit decides whether a unit of work may proceed. It enforces the queue
// depth and goroutine safety limits.
//
// The circuit-breaker half-open state is deliberately NOT a shed condition:
// the breakers themselves bound half-open recovery to HalfOpenMaxRequests
// concurrent probes and reject the excess with ErrCircuitOpen. Shedding here
// as well would starve the breaker of the sequential successful probes it
// needs to reach MinSuccesses and wedge the server in half-open shedding
// forever (probe 1 succeeds, every later probe is shed, MinSuccesses is never
// reached). The critical flag is retained for the AdmissionController
// contract; admission no longer differentiates on it.
func (l *LoadShedder) Admit(critical bool) error {
	if l == nil {
		return nil
	}
	if l.queueDepth != nil && l.queueDepth() >= l.queueThreshold {
		return ErrLoadShed
	}
	if l.numGoroutine != nil && l.goroutineLimit > 0 && l.numGoroutine() > l.goroutineLimit {
		return ErrLoadShed
	}
	return nil
}
