package parallel

import (
	"testing"
)

// A panic inside a worker goroutine must surface on the calling goroutine so the
// engine's query-level recover can turn it into a failed query. Before the fix,
// the panic was either silently swallowed or crashed the whole process.

func makeValues(n int) [][]byte {
	v := make([][]byte, n)
	for i := range v {
		v[i] = []byte{byte(i)}
	}
	return v
}

func TestParallelSelectRowsPropagatesWorkerPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected worker panic to propagate to caller, got none")
		}
	}()
	// workers>1 and n>=threshold forces the parallel path.
	ParallelSelectRows(makeValues(100), 4, 1, func(chunk [][]byte) [][]interface{} {
		panic("boom")
	})
	t.Fatal("ParallelSelectRows returned normally; panic did not propagate")
}

func TestParallelGroupByPropagatesWorkerPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected worker panic to propagate to caller, got none")
		}
	}()
	ParallelGroupBy(makeValues(100), 4, 1, func(chunk [][]byte) map[string][][]interface{} {
		panic("boom")
	})
	t.Fatal("ParallelGroupBy returned normally; panic did not propagate")
}

func TestParallelAggregatePropagatesWorkerPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected worker panic to propagate to caller, got none")
		}
	}()
	ParallelAggregate(makeValues(100), 4, 1,
		func(chunk [][]byte) []interface{} { panic("boom") },
		func(dst, src []interface{}) {})
	t.Fatal("ParallelAggregate returned normally; panic did not propagate")
}

// The non-parallel fast path (single worker / below threshold) should also let a
// panic surface naturally.
func TestParallelSelectRowsSerialPathStillPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on serial path")
		}
	}()
	ParallelSelectRows(makeValues(1), 1, 1000, func(chunk [][]byte) [][]interface{} {
		panic("boom")
	})
}
