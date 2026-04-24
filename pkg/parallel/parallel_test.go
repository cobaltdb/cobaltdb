package parallel

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func TestParallelSelectRows(t *testing.T) {
	values := make([][]byte, 1000)
	for i := range values {
		values[i] = []byte(fmt.Sprintf("row-%d", i))
	}

	processFn := func(chunk [][]byte) [][]interface{} {
		result := make([][]interface{}, len(chunk))
		for i, v := range chunk {
			result[i] = []interface{}{string(v)}
		}
		return result
	}

	// Sequential path (workers=1)
	seq := ParallelSelectRows(values, 1, 10, processFn)
	if len(seq) != 1000 {
		t.Fatalf("expected 1000 rows, got %d", len(seq))
	}

	// Parallel path
	par := ParallelSelectRows(values, 4, 10, processFn)
	if len(par) != 1000 {
		t.Fatalf("expected 1000 rows parallel, got %d", len(par))
	}

	// Verify contents match (order may differ, so use a map)
	seqMap := make(map[string]bool)
	for _, r := range seq {
		seqMap[r[0].(string)] = true
	}
	for _, r := range par {
		if !seqMap[r[0].(string)] {
			t.Fatalf("missing row: %v", r[0])
		}
	}
}

func TestParallelSelectRowsBelowThreshold(t *testing.T) {
	values := make([][]byte, 5)
	for i := range values {
		values[i] = []byte(fmt.Sprintf("row-%d", i))
	}

	var counter atomic.Int32
	processFn := func(chunk [][]byte) [][]interface{} {
		counter.Add(1)
		result := make([][]interface{}, len(chunk))
		for i, v := range chunk {
			result[i] = []interface{}{string(v)}
		}
		return result
	}

	// Below threshold should stay sequential (single call)
	_ = ParallelSelectRows(values, 4, 100, processFn)
	if counter.Load() != 1 {
		t.Fatalf("expected sequential processing (1 call), got %d", counter.Load())
	}
}

func TestParallelGroupBy(t *testing.T) {
	values := make([][]byte, 1000)
	for i := range values {
		values[i] = []byte(fmt.Sprintf("group-%d", i%10))
	}

	groupFn := func(chunk [][]byte) map[string][][]interface{} {
		m := make(map[string][][]interface{})
		for _, v := range chunk {
			k := string(v)
			m[k] = append(m[k], []interface{}{k})
		}
		return m
	}

	seq := ParallelGroupBy(values, 1, 10, groupFn)
	par := ParallelGroupBy(values, 4, 10, groupFn)

	if len(seq) != 10 {
		t.Fatalf("expected 10 groups sequential, got %d", len(seq))
	}
	if len(par) != 10 {
		t.Fatalf("expected 10 groups parallel, got %d", len(par))
	}

	for k, rows := range seq {
		parRows, ok := par[k]
		if !ok {
			t.Fatalf("missing group %s in parallel result", k)
		}
		if len(rows) != len(parRows) {
			t.Fatalf("group %s: expected %d rows, got %d", k, len(rows), len(parRows))
		}
	}
}

func TestParallelAggregate(t *testing.T) {
	values := make([][]byte, 1000)
	for i := range values {
		values[i] = []byte{byte(i)}
	}

	partialFn := func(chunk [][]byte) []interface{} {
		var sum int64
		for _, v := range chunk {
			sum += int64(v[0])
		}
		return []interface{}{sum}
	}

	mergeFn := func(dst, src []interface{}) {
		dst[0] = dst[0].(int64) + src[0].(int64)
	}

	seq := ParallelAggregate(values, 1, 10, partialFn, mergeFn)
	par := ParallelAggregate(values, 4, 10, partialFn, mergeFn)

	if len(seq) != 1 || len(par) != 1 {
		t.Fatal("expected single aggregate result")
	}
	if seq[0].(int64) != par[0].(int64) {
		t.Fatalf("sums differ: sequential=%d parallel=%d", seq[0].(int64), par[0].(int64))
	}
}

func TestChunkSize(t *testing.T) {
	if chunkSize(100, 4) != 25 {
		t.Fatalf("expected 25, got %d", chunkSize(100, 4))
	}
	if chunkSize(5, 10) != 1 {
		t.Fatalf("expected 1, got %d", chunkSize(5, 10))
	}
	if chunkSize(100, 1) != 100 {
		t.Fatalf("expected 100, got %d", chunkSize(100, 1))
	}
}

func TestDefaultWorkers(t *testing.T) {
	if defaultWorkers(4) != 4 {
		t.Fatalf("expected 4, got %d", defaultWorkers(4))
	}
	if defaultWorkers(0) < 1 {
		t.Fatalf("expected >=1, got %d", defaultWorkers(0))
	}
	if defaultWorkers(-1) < 1 {
		t.Fatalf("expected >=1, got %d", defaultWorkers(-1))
	}
}

func TestPoolBasic(t *testing.T) {
	pool := NewWorkerPool(2)
	defer pool.Close()

	var counter atomic.Int32
	for i := 0; i < 10; i++ {
		pool.Submit(func() {
			counter.Add(1)
		})
	}
	pool.WaitAndClose()
	if counter.Load() != 10 {
		t.Fatalf("expected 10, got %d", counter.Load())
	}
}

func TestPoolMultipleStarts(t *testing.T) {
	pool := NewWorkerPool(2)
	defer pool.Close()

	pool.Start()
	pool.Start() // should be no-op

	var counter atomic.Int32
	pool.Submit(func() {
		counter.Add(1)
	})
	pool.WaitAndClose()
	if counter.Load() != 1 {
		t.Fatalf("expected 1, got %d", counter.Load())
	}
}

func TestPoolWaitBeforeStart(t *testing.T) {
	pool := NewWorkerPool(2)
	defer pool.Close()

	// Wait before start should not block forever
	pool.Wait()
}
