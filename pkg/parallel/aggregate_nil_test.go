package parallel

// Regression test: ParallelAggregate must skip nil partials during the merge
// (a chunk whose partialFn returns nil — e.g. all rows filtered — previously
// could be passed to mergeFn as src, or a leading nil could become the merge
// target and drop subsequent partials into mergeFn(nil, ...)).

import "testing"

func TestParallelAggregateSkipsNilPartials(t *testing.T) {
	// 100 values, workers=4, threshold=10 → 4 chunks of 25.
	values := make([][]byte, 100)
	for i := range values {
		values[i] = []byte{byte(i)}
	}

	// Chunks starting at 0 and 50 (first byte 0 and 50, both even ÷ 25 chunks:
	// chunk index 0 and 2) return nil; the others return their row count.
	partialFn := func(chunk [][]byte) []interface{} {
		first := int(chunk[0][0])
		if (first/25)%2 == 0 {
			return nil // simulate "everything filtered out" for chunks 0 and 2
		}
		return []interface{}{int64(len(chunk))}
	}
	mergeFn := func(dst, src []interface{}) {
		if dst == nil || src == nil {
			t.Fatal("mergeFn must never receive a nil partial")
		}
		dst[0] = dst[0].(int64) + src[0].(int64)
	}

	result := ParallelAggregate(values, 4, 10, partialFn, mergeFn)
	if result == nil {
		t.Fatal("expected non-nil result from non-nil partials")
	}
	if got := result[0].(int64); got != 50 {
		t.Fatalf("merged partials = %d, want 50 (two chunks of 25)", got)
	}
}

func TestParallelAggregateAllNilPartials(t *testing.T) {
	values := make([][]byte, 40)
	for i := range values {
		values[i] = []byte{byte(i)}
	}
	partialFn := func(chunk [][]byte) []interface{} { return nil }
	mergeFn := func(dst, src []interface{}) {
		t.Fatal("mergeFn must not be called when every partial is nil")
	}
	if result := ParallelAggregate(values, 4, 10, partialFn, mergeFn); result != nil {
		t.Fatalf("expected nil result when all partials are nil, got %v", result)
	}
}

// TestParallelAggregateLeadingNilDoesNotDropPartials pins the specific old
// bug shape: a nil first partial must not cause the second partial to be
// merged into a nil dst.
func TestParallelAggregateLeadingNilDoesNotDropPartials(t *testing.T) {
	values := make([][]byte, 60)
	for i := range values {
		values[i] = []byte{byte(i)}
	}
	// 3 chunks of 20 (workers=3): chunk 0 → nil, chunks 1,2 → sums.
	partialFn := func(chunk [][]byte) []interface{} {
		if int(chunk[0][0]) == 0 {
			return nil
		}
		var sum int64
		for _, v := range chunk {
			sum += int64(v[0])
		}
		return []interface{}{sum}
	}
	mergeFn := func(dst, src []interface{}) {
		dst[0] = dst[0].(int64) + src[0].(int64)
	}
	result := ParallelAggregate(values, 3, 10, partialFn, mergeFn)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// Sum of 20..59 = (20+59)*40/2 = 1580.
	if got := result[0].(int64); got != 1580 {
		t.Fatalf("merged sum = %d, want 1580", got)
	}
}
