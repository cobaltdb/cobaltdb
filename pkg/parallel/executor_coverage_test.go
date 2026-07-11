package parallel

import "testing"

func TestDefaultWorkersUsesAvailableCPUCount(t *testing.T) {
	original := availableCPUCount
	t.Cleanup(func() { availableCPUCount = original })

	availableCPUCount = func() int { return maxParallelWorkers + 1 }
	if got := defaultWorkers(0); got != maxParallelWorkers {
		t.Fatalf("defaultWorkers with excessive CPUs = %d, want %d", got, maxParallelWorkers)
	}

	availableCPUCount = func() int { return 1 }
	if got := defaultWorkers(0); got != 1 {
		t.Fatalf("defaultWorkers with one CPU = %d, want 1", got)
	}
}

func TestChunkSizeDefendsAgainstInvalidItemCount(t *testing.T) {
	if got := chunkSize(0, 4); got != 1 {
		t.Fatalf("chunkSize(0, 4) = %d, want 1", got)
	}
}

func TestParallelGroupByAndAggregateUnevenFinalChunk(t *testing.T) {
	values := make([][]byte, 5)
	for i := range values {
		values[i] = []byte{byte(i)}
	}

	groups := ParallelGroupBy(values, 2, 1, func(chunk [][]byte) map[string][][]interface{} {
		return map[string][][]interface{}{"all": {{len(chunk)}}}
	})
	if got := len(groups["all"]); got != 2 {
		t.Fatalf("group chunks = %d, want 2", got)
	}
	if got := groups["all"][1][0]; got != 2 {
		t.Fatalf("final group chunk length = %v, want 2", got)
	}

	aggregate := ParallelAggregate(values, 2, 1, func(chunk [][]byte) []interface{} {
		return []interface{}{len(chunk)}
	}, func(dst, src []interface{}) {
		dst[0] = dst[0].(int) + src[0].(int)
	})
	if got := aggregate[0]; got != 5 {
		t.Fatalf("aggregate count = %v, want 5", got)
	}
}
