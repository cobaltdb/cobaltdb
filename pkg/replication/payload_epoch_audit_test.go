package replication

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStatementPayloadEpochAndAbsentTimestamp(t *testing.T) {
	for _, want := range []time.Time{time.Time{}, time.Unix(0, 0), time.Unix(0, -1), time.Unix(0, 1)} {
		t.Run(want.UTC().Format(time.RFC3339Nano), func(t *testing.T) {
			data, err := EncodeStatementPayload("SELECT ?", []interface{}{int64(7)}, want)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := DecodeStatementPayload(data)
			if err != nil {
				t.Fatal(err)
			}
			if !payload.Timestamp.Equal(want) {
				t.Fatalf("timestamp=%v, want %v", payload.Timestamp, want)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			_, present := fields["ts"]
			if present == want.IsZero() {
				t.Fatalf("timestamp presence=%v for %v", present, want)
			}
		})
	}
}
