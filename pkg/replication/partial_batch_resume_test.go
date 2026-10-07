package replication

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReplicationPartialBatchPersistsAppliedPrefix(t *testing.T) {
	for _, mode := range []string{"success", "fail_first", "fail_second", "panic_second", "checksum_second", "empty", "persist_failure"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &Config{Role: RoleSlave, StateFile: filepath.Join(t.TempDir(), "state.json")}
			m := NewManager(cfg)
			if err := m.saveReplicationState(); err != nil {
				t.Fatal(err)
			}
			failure, persistFailure := errors.New("apply failure"), errors.New("persist failure")
			applied := []uint64{}
			m.OnApply = func(e *WALEntry) error {
				if (mode == "fail_first" && e.LSN == 1) || ((mode == "fail_second" || mode == "persist_failure") && e.LSN == 2) {
					return failure
				}
				if mode == "panic_second" && e.LSN == 2 {
					panic("injected")
				}
				applied = append(applied, e.LSN)
				return nil
			}
			entries := []*WALEntry{}
			if mode != "empty" {
				for _, lsn := range []uint64{1, 2} {
					data := []byte(fmt.Sprintf("entry%d", lsn))
					entries = append(entries, &WALEntry{LSN: lsn, Timestamp: time.Unix(0, 1), Data: data, Checksum: calculateCRC32(data)})
				}
			}
			if mode == "checksum_second" {
				entries[1].Checksum++
			}
			data, err := encodeWALEntries(entries)
			if err != nil {
				t.Fatal(err)
			}
			originalRename := replicationRename
			if mode == "persist_failure" {
				replicationRename = func(string, string) error { return persistFailure }
			}
			err = m.applyWALDataBytes(data)
			replicationRename = originalRename
			want := uint64(len(applied))
			if mode == "success" || mode == "empty" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("expected error")
			}
			if mode == "fail_first" || mode == "fail_second" || mode == "persist_failure" {
				if !errors.Is(err, failure) {
					t.Fatalf("lost original error: %v", err)
				}
			}
			if mode == "persist_failure" && !errors.Is(err, persistFailure) {
				t.Fatalf("lost persistence error: %v", err)
			}
			if mode == "panic_second" && !strings.Contains(err.Error(), "callback panic") {
				t.Fatalf("panic error: %v", err)
			}
			reopened := NewManager(cfg)
			if err := reopened.loadReplicationState(); err != nil {
				t.Fatal(err)
			}
			wantResume := want
			if mode == "persist_failure" {
				wantResume = 0
			}
			if m.LastAppliedLSN() != want || reopened.LastAppliedLSN() != wantResume {
				t.Fatal("resume diverged")
			}
			if mode == "fail_second" || mode == "panic_second" || mode == "checksum_second" {
				entries[1].Checksum = calculateCRC32(entries[1].Data)
				retry, _ := encodeWALEntries(entries)
				reopened.OnApply = func(e *WALEntry) error { applied = append(applied, e.LSN); return nil }
				if err := reopened.applyWALDataBytes(retry); err != nil {
					t.Fatal(err)
				}
				if len(applied) != 2 || applied[0] != 1 || applied[1] != 2 {
					t.Fatalf("replayed committed prefix: %v", applied)
				}
				if err := reopened.applyWALDataBytes(retry); err != nil || len(applied) != 2 {
					t.Fatalf("duplicate retry: %v %v", applied, err)
				}
			}
		})
	}
}
