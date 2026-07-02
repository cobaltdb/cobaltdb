package replication

import (
	"bytes"
	"math"
	"testing"
	"time"
)

func TestStatementPayloadRoundTrip(t *testing.T) {
	now := time.Now()
	args := []interface{}{
		nil,
		true,
		false,
		int64(42),
		int64(-9007199254740993), // beyond float64 integer precision
		uint64(math.MaxUint64),
		3.14159,
		math.MaxFloat64,
		"hello world",
		"",
		[]byte{0x00, 0x01, 0xFF},
		now,
	}

	data, err := EncodeStatementPayload("INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", args, now)
	if err != nil {
		t.Fatalf("EncodeStatementPayload failed: %v", err)
	}

	payload, err := DecodeStatementPayload(data)
	if err != nil {
		t.Fatalf("DecodeStatementPayload failed: %v", err)
	}

	if payload.Version != StatementPayloadVersion {
		t.Errorf("version = %d, want %d", payload.Version, StatementPayloadVersion)
	}
	if payload.SQL != "INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?,?,?,?)" {
		t.Errorf("unexpected SQL: %q", payload.SQL)
	}
	if !payload.Timestamp.Equal(time.Unix(0, now.UnixNano())) {
		t.Errorf("timestamp = %v, want %v", payload.Timestamp, now)
	}
	if len(payload.Args) != len(args) {
		t.Fatalf("args count = %d, want %d", len(payload.Args), len(args))
	}

	if payload.Args[0] != nil {
		t.Errorf("arg0 = %v, want nil", payload.Args[0])
	}
	if v, ok := payload.Args[1].(bool); !ok || !v {
		t.Errorf("arg1 = %v (%T), want true", payload.Args[1], payload.Args[1])
	}
	if v, ok := payload.Args[2].(bool); !ok || v {
		t.Errorf("arg2 = %v (%T), want false", payload.Args[2], payload.Args[2])
	}
	if v, ok := payload.Args[3].(int64); !ok || v != 42 {
		t.Errorf("arg3 = %v (%T), want int64(42)", payload.Args[3], payload.Args[3])
	}
	if v, ok := payload.Args[4].(int64); !ok || v != -9007199254740993 {
		t.Errorf("arg4 = %v (%T), want exact int64", payload.Args[4], payload.Args[4])
	}
	if v, ok := payload.Args[5].(uint64); !ok || v != math.MaxUint64 {
		t.Errorf("arg5 = %v (%T), want max uint64", payload.Args[5], payload.Args[5])
	}
	if v, ok := payload.Args[6].(float64); !ok || v != 3.14159 {
		t.Errorf("arg6 = %v (%T), want 3.14159", payload.Args[6], payload.Args[6])
	}
	if v, ok := payload.Args[7].(float64); !ok || v != math.MaxFloat64 {
		t.Errorf("arg7 = %v (%T), want MaxFloat64", payload.Args[7], payload.Args[7])
	}
	if v, ok := payload.Args[8].(string); !ok || v != "hello world" {
		t.Errorf("arg8 = %v (%T), want string", payload.Args[8], payload.Args[8])
	}
	if v, ok := payload.Args[9].(string); !ok || v != "" {
		t.Errorf("arg9 = %v (%T), want empty string", payload.Args[9], payload.Args[9])
	}
	if v, ok := payload.Args[10].([]byte); !ok || !bytes.Equal(v, []byte{0x00, 0x01, 0xFF}) {
		t.Errorf("arg10 = %v (%T), want []byte", payload.Args[10], payload.Args[10])
	}
	if v, ok := payload.Args[11].(time.Time); !ok || !v.Equal(now) {
		t.Errorf("arg11 = %v (%T), want time %v", payload.Args[11], payload.Args[11], now)
	}
}

func TestStatementPayloadIntTypesNormalizeToInt64(t *testing.T) {
	data, err := EncodeStatementPayload("Q", []interface{}{int(7), int8(-8), int16(16), int32(-32), uint(9), uint32(33)}, time.Time{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	payload, err := DecodeStatementPayload(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	wantInts := []int64{7, -8, 16, -32}
	for i, want := range wantInts {
		if v, ok := payload.Args[i].(int64); !ok || v != want {
			t.Errorf("arg%d = %v (%T), want int64(%d)", i, payload.Args[i], payload.Args[i], want)
		}
	}
	wantUints := []uint64{9, 33}
	for i, want := range wantUints {
		idx := len(wantInts) + i
		if v, ok := payload.Args[idx].(uint64); !ok || v != want {
			t.Errorf("arg%d = %v (%T), want uint64(%d)", idx, payload.Args[idx], payload.Args[idx], want)
		}
	}
}

func TestStatementPayloadNoArgsNoTimestamp(t *testing.T) {
	data, err := EncodeStatementPayload("CREATE TABLE t (id INT)", nil, time.Time{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	payload, err := DecodeStatementPayload(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(payload.Args) != 0 {
		t.Errorf("expected no args, got %v", payload.Args)
	}
	if !payload.Timestamp.IsZero() {
		t.Errorf("expected zero timestamp, got %v", payload.Timestamp)
	}
}

func TestStatementPayloadFallbackStringifiesUnknownTypes(t *testing.T) {
	type custom struct{ A int }
	data, err := EncodeStatementPayload("Q", []interface{}{custom{A: 1}}, time.Time{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	payload, err := DecodeStatementPayload(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := payload.Args[0].(string); !ok {
		t.Errorf("fallback arg should decode as string, got %T", payload.Args[0])
	}
}

func TestDecodeStatementPayloadErrors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"not JSON", []byte("INSERT|users|1,2")},
		{"wrong version", []byte(`{"v":99,"sql":"SELECT 1"}`)},
		{"empty SQL", []byte(`{"v":1,"sql":""}`)},
		{"bad arg tag", []byte(`{"v":1,"sql":"Q","args":[{"t":"zz","v":"1"}]}`)},
		{"bad int value", []byte(`{"v":1,"sql":"Q","args":[{"t":"i","v":"abc"}]}`)},
		{"bad base64", []byte(`{"v":1,"sql":"Q","args":[{"t":"x","v":"!!!"}]}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeStatementPayload(tc.data); err == nil {
				t.Errorf("expected decode error for %s", tc.name)
			}
		})
	}
}

// TestWaitForSlavesSyncModeNoSlavesTimesOut verifies the sync-mode durability
// contract: with no connected slaves an ACK is impossible, so WaitForSlaves
// must time out instead of vacuously succeeding.
func TestWaitForSlavesSyncModeNoSlavesTimesOut(t *testing.T) {
	for _, mode := range []ReplicationMode{ModeSync, ModeFullSync} {
		config := DefaultConfig()
		config.Role = RoleMaster
		config.Mode = mode

		mgr := NewManager(config)
		start := time.Now()
		err := mgr.WaitForSlaves(100 * time.Millisecond)
		if err == nil {
			t.Errorf("mode %v: expected timeout error with no slaves", mode)
		}
		if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
			t.Errorf("mode %v: WaitForSlaves returned after %v, expected to block ~100ms", mode, elapsed)
		}
	}
}

// TestWaitForSlavesSyncModeOneOfTwoAcked verifies ack accounting: "sync"
// requires at least one caught-up slave, "full_sync" requires all.
func TestWaitForSlavesSyncModeOneOfTwoAcked(t *testing.T) {
	newMgr := func(mode ReplicationMode) *Manager {
		config := DefaultConfig()
		config.Role = RoleMaster
		config.Mode = mode
		mgr := NewManager(config)
		mgr.currentLSN = 10
		mgr.slaves["caught-up"] = &SlaveConnection{ID: "caught-up", LastLSN: 10, LastPing: time.Now()}
		mgr.slaves["lagging"] = &SlaveConnection{ID: "lagging", LastLSN: 5, LastPing: time.Now()}
		return mgr
	}

	if err := newMgr(ModeSync).WaitForSlaves(200 * time.Millisecond); err != nil {
		t.Errorf("sync mode with one acked slave should succeed, got %v", err)
	}
	if err := newMgr(ModeFullSync).WaitForSlaves(100 * time.Millisecond); err == nil {
		t.Error("full_sync mode with a lagging slave should time out")
	}
}

// TestReplicateWALEntryAssignsMonotonicPositions verifies LSN/position
// tracking on the master side.
func TestReplicateWALEntryAssignsMonotonicPositions(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	mgr := NewManager(config)

	for i := 0; i < 5; i++ {
		if err := mgr.ReplicateWALEntry([]byte("entry")); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}
	if got := mgr.CurrentLSN(); got != 5 {
		t.Errorf("CurrentLSN = %d, want 5", got)
	}
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	for i, entry := range mgr.walBuffer {
		if entry.LSN != uint64(i+1) {
			t.Errorf("walBuffer[%d].LSN = %d, want %d", i, entry.LSN, i+1)
		}
	}
}

// Slave idempotence (skipping entries <= lastApplied) is covered by
// TestApplyWALDataSkipsAlreadyAppliedEntries in replication_coverage3_test.go.
