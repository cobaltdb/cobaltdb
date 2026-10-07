package scheduler

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestJobSnapshotJSONMilliseconds(t *testing.T) {
	for _, interval := range []time.Duration{1500 * time.Millisecond, 0, time.Nanosecond, -1500 * time.Millisecond, time.Duration(1<<63 - 1)} {
		t.Run(interval.String(), func(t *testing.T) {
			snapshot := (&Job{ID: "json-control", Interval: interval, RunCount: 3}).Snapshot()
			for _, value := range []any{snapshot, &snapshot, []JobSnapshot{snapshot}} {
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(string(data), "[") {
					data = data[1 : len(data)-1]
				}
				var wire struct {
					ID       string `json:"id"`
					Interval int64  `json:"interval_ms"`
					RunCount int64  `json:"run_count"`
				}
				if err := json.Unmarshal(data, &wire); err != nil {
					t.Fatal(err)
				}
				if wire.Interval != interval.Milliseconds() || wire.ID != snapshot.ID || wire.RunCount != 3 || strings.Contains(string(data), "last_error") {
					t.Fatalf("wrong JSON representation: %s", data)
				}
				var restored JobSnapshot
				if err := json.Unmarshal(data, &restored); err != nil {
					t.Fatal(err)
				}
				want := interval.Milliseconds() * int64(time.Millisecond)
				if restored.Interval != time.Duration(want) || restored.ID != snapshot.ID || restored.RunCount != 3 || snapshot.Interval != interval {
					t.Fatalf("incorrect duration round trip: %+v", restored)
				}
			}
		})
	}
	t.Run("decode-boundaries", func(t *testing.T) {
		original := JobSnapshot{Interval: time.Second}
		for _, data := range []string{`{"name":"patched"}`, `{"interval_ms":null}`} {
			if err := json.Unmarshal([]byte(data), &original); err != nil || original.Interval != time.Second {
				t.Fatalf("omitted/null interval changed existing duration: %+v, %v", original, err)
			}
		}
		for _, data := range []string{`{"interval_ms":9223372036854775807}`, `{"interval_ms":-9223372036854775808}`, `{"interval_ms":"invalid"}`, `{`} {
			if err := json.Unmarshal([]byte(data), &original); err == nil {
				t.Fatalf("invalid interval accepted: %s", data)
			}
			if original.Interval != time.Second {
				t.Fatal("invalid interval overwrote prior duration")
			}
		}
	})
}
