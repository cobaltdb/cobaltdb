package metrics

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHistogramPreservesFineBucketBoundaries(t *testing.T) {
	h := NewHistogram("precision", "", nil, []float64{0.001, 0.005, 0.01, 0.025, 1.25, 10})
	if snap := h.GetSnapshot(); snap.Count != 0 || len(snap.Buckets) != 0 {
		t.Fatal("empty histogram changed")
	}
	for _, value := range []float64{0, 0.001, 0.005, 0.01, 0.025, 1.25, 10, 11} {
		h.Observe(value)
	}
	snap := h.GetSnapshot()
	want := map[string]uint64{"0.001": 2, "0.005": 3, "0.01": 4, "0.025": 5, "1.25": 6, "10.00": 7}
	if snap.Count != 8 || len(snap.Buckets) != len(want) {
		t.Fatalf("wrong population/boundary count: %+v", snap)
	}
	for key, count := range want {
		if snap.Buckets[key] != count {
			t.Fatalf("wrong boundary %s", key)
		}
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	var restored HistogramSnapshot
	if err := json.Unmarshal(data, &restored); err != nil || restored.Buckets["0.001"] != 2 {
		t.Fatalf("wire precision: %s %v", data, err)
	}
	snap.Buckets["0.001"] = 99
	if h.GetSnapshot().Buckets["0.001"] != 2 {
		t.Fatal("snapshot mutated metric")
	}
	c := NewCollector(time.Hour)
	c.RecordQuery(500*time.Microsecond, false)
	c.RecordWrite(500 * time.Microsecond)
	for _, snapshot := range []HistogramSnapshot{c.QueryHistogram.GetSnapshot(), c.WriteHistogram.GetSnapshot()} {
		if snapshot.Count != 1 || snapshot.Buckets["0.001"] != 1 || snapshot.Buckets["0.005"] != 1 || snapshot.Buckets["0.01"] != 1 {
			t.Fatalf("collector boundary collision: %+v", snapshot)
		}
	}
	negative := NewHistogram("negative", "", nil, []float64{-0.005, -0.001, 0})
	negative.Observe(-0.005)
	negative.Observe(-0.001)
	if snapshot := negative.GetSnapshot(); snapshot.Buckets["-0.005"] != 1 || snapshot.Buckets["-0.001"] != 2 || snapshot.Buckets["0.00"] != 2 {
		t.Fatalf("negative boundary precision: %+v", snapshot)
	}
}
