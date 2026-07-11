package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type replicationFaultConn struct {
	readData                                                       []byte
	writes                                                         bytes.Buffer
	readErr, writeErr, readDeadlineErr, writeDeadlineErr, closeErr error
	shortWrite                                                     bool
}

func (c *replicationFaultConn) Read(p []byte) (int, error) {
	if len(c.readData) > 0 {
		n := copy(p, c.readData)
		c.readData = c.readData[n:]
		return n, nil
	}
	if c.readErr != nil {
		return 0, c.readErr
	}
	return 0, io.EOF
}
func (c *replicationFaultConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.shortWrite && len(p) > 0 {
		return len(p) - 1, nil
	}
	return c.writes.Write(p)
}
func (c *replicationFaultConn) Close() error                     { return c.closeErr }
func (c *replicationFaultConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *replicationFaultConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *replicationFaultConn) SetDeadline(time.Time) error      { return nil }
func (c *replicationFaultConn) SetReadDeadline(time.Time) error  { return c.readDeadlineErr }
func (c *replicationFaultConn) SetWriteDeadline(time.Time) error { return c.writeDeadlineErr }

func TestReplicationPayloadAllTypes(t *testing.T) {
	ts := time.Date(2025, 1, 2, 3, 4, 5, 6, time.UTC)
	args := []interface{}{nil, true, int(1), int8(2), int16(3), int32(4), int64(5), uint(6), uint8(7), uint16(8), uint32(9), uint64(10), float32(1.5), float64(2.5), "s", []byte("x"), ts, struct{ X int }{1}}
	data, err := EncodeStatementPayload("SELECT 1", args, ts)
	if err != nil {
		t.Fatal(err)
	}
	p, err := DecodeStatementPayload(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Args) != len(args) || !p.Timestamp.Equal(ts) {
		t.Fatalf("round trip: %+v", p)
	}
	for _, bad := range []taggedArg{{Type: "b", Value: "x"}, {Type: "i", Value: "x"}, {Type: "u", Value: "x"}, {Type: "f", Value: "x"}, {Type: "x", Value: "!"}, {Type: "t", Value: "x"}, {Type: "?"}} {
		if _, err := decodeTaggedArg(bad); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
}

func TestReplicationEncodingEdges(t *testing.T) {
	if _, err := replicationUint32Len(-1, "x"); err == nil {
		t.Fatal("negative")
	}
	entry := &WALEntry{LSN: 9, Timestamp: time.Unix(0, 123), Data: []byte("data"), Checksum: 7}
	enc, err := entry.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var out WALEntry
	if err := out.Decode(enc); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(enc); n++ {
		var e WALEntry
		_ = e.Decode(enc[:n])
	}
	bad := append([]byte(nil), enc...)
	binary.BigEndian.PutUint32(bad[16:20], uint32(maxWALEntryDataBytes+1))
	if err := out.Decode(bad); err == nil {
		t.Fatal("oversize accepted")
	}
	entries := []*WALEntry{{Data: make([]byte, maxWALEntryDataBytes+1)}}
	if _, err := encodeWALEntries(entries); err == nil {
		t.Fatal("oversized entry accepted")
	}
}

func TestReplicationCallbacksAndControlFailures(t *testing.T) {
	m := NewManager(&Config{Role: RoleMaster})
	if _, _, err := m.callOnSnapshot(); err == nil {
		t.Fatal("missing snapshot")
	}
	m.OnSnapshot = func() ([]byte, uint64, error) { panic("boom") }
	if _, _, err := m.callOnSnapshot(); err == nil {
		t.Fatal("panic lost")
	}
	m.OnApplySnapshot = func([]byte, uint64) error { panic("boom") }
	if err := m.callOnApplySnapshot(nil, 0); err == nil {
		t.Fatal("panic lost")
	}
	m.OnApply = func(*WALEntry) error { panic("boom") }
	if err := m.callOnApply(&WALEntry{}); err == nil {
		t.Fatal("panic lost")
	}
	m.OnLag = func(string, time.Duration) { panic("boom") }
	m.callOnLag("s", time.Second)
	m.OnDisconnect = func(string, error) { panic("boom") }
	m.callOnDisconnect("s", errors.New("x"))

	for _, line := range []string{"", "BAD\n", "RESUME nope\n", "RESUME 1 extra\n", "RESUME_SNAPSHOT nope\n"} {
		s := &SlaveConnection{Reader: bufio.NewReader(strings.NewReader(line))}
		_, _ = m.receiveResumeRequest(s)
	}
	for _, line := range []string{"BAD\n", "START nope\n", "RESYNC nope\n", "PING nope\n"} {
		_ = m.handleMasterMessage(line)
	}
}

func TestReplicationSnapshotAndFrameEdges(t *testing.T) {
	m := NewManager(&Config{Role: RoleMaster})
	for _, fc := range []*replicationFaultConn{{writeDeadlineErr: errors.New("deadline")}, {writeErr: errors.New("write")}, {shortWrite: true}} {
		s := &SlaveConnection{Conn: fc, Writer: bufio.NewWriter(fc)}
		_ = m.sendResyncRequired(s, 1)
		_ = m.sendInitialSnapshot(s, 1)
		_ = m.sendHeartbeat(s)
		_ = m.sendWALToSlave(s, []byte("x"))
	}
	m.OnSnapshot = func() ([]byte, uint64, error) { return nil, 0, errors.New("snapshot") }
	_ = m.sendInitialSnapshot(&SlaveConnection{NeedsSnapshot: true, Writer: bufio.NewWriter(io.Discard)}, 0)
	m.OnSnapshot = func() ([]byte, uint64, error) { return make([]byte, maxReplicationSnapshotSize+1), 1, nil }
	_ = m.sendInitialSnapshot(&SlaveConnection{NeedsSnapshot: true, Writer: bufio.NewWriter(io.Discard)}, 0)
	m.OnSnapshot = func() ([]byte, uint64, error) { return []byte("x"), 1, nil }
	_ = m.sendInitialSnapshot(&SlaveConnection{NeedsSnapshot: true, Writer: bufio.NewWriter(&replicationFaultConn{writeErr: errors.New("x")})}, 0)

	slave := NewManager(&Config{Role: RoleSlave})
	slave.masterConn = &replicationFaultConn{writeErr: errors.New("ack")}
	slave.OnApplySnapshot = func([]byte, uint64) error { return errors.New("apply") }
	for _, frame := range []string{"SNAPSHOT nope 1\nx", "SNAPSHOT 1 nope\nx", "SNAPSHOT 1 999999999999\n", "SNAPSHOT 1 2\nx", "PING nope\n", "UNKNOWN\n"} {
		_ = slave.readMasterFrame(bufio.NewReader(strings.NewReader(frame)))
	}
	for _, raw := range [][]byte{{0, 0, 0, 0}, {0xff, 0xff, 0xff, 0xff}, {0, 0, 0, 4, 1}} {
		_ = slave.readMasterFrame(bufio.NewReader(bytes.NewReader(raw)))
	}
}

func TestReplicationStateFileEdges(t *testing.T) {
	m := NewManager(&Config{Role: RoleSlave})
	if err := m.loadReplicationState(); err != nil {
		t.Fatal(err)
	}
	if err := m.saveReplicationState(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cases := []struct {
		name, content string
		mode          os.FileMode
	}{{"empty", "", 0600}, {"bad", "{", 0600}, {"zero", "{\"last_applied\":0}", 0600}}
	for _, tc := range cases {
		p := filepath.Join(dir, tc.name)
		if err := os.WriteFile(p, []byte(tc.content), tc.mode); err != nil {
			t.Fatal(err)
		}
		mm := NewManager(&Config{Role: RoleSlave, StateFile: p})
		_ = mm.loadReplicationState()
	}
	p := filepath.Join(dir, "missing", "state")
	m = NewManager(&Config{Role: RoleSlave, StateFile: p})
	atomic.StoreUint64(&m.lastApplied, 3)
	if err := m.saveReplicationState(); err != nil {
		t.Fatal(err)
	}
	if _, err := openReplicationStateFile(filepath.Join(dir, "does-not-exist")); err == nil {
		t.Fatal("missing opened")
	}
	if _, err := cleanReplicationStatePath(""); err == nil {
		t.Fatal("empty path")
	}
	fileParent := filepath.Join(dir, "fileparent")
	if err := os.WriteFile(fileParent, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	_ = prepareReplicationStateDir(filepath.Join(fileParent, "state"))
	_ = rejectReplicationStateDirSymlinks(filepath.Join(dir, "missing", "state"))
}

func TestReplicationPromotionAndRejoinAllBranches(t *testing.T) {
	valid := PromotionRequest{FencingToken: "token", Epoch: 2, OldPrimaryFenced: true, AllowConnectedPeer: true}
	m := NewManager(&Config{Role: RoleMaster})
	_ = m.PromoteToMasterWithFencing(valid)
	m = NewManager(&Config{Role: RoleSlave})
	for _, req := range []PromotionRequest{{}, {FencingToken: "x"}, {FencingToken: "x", OldPrimaryFenced: true}, {FencingToken: "x", OldPrimaryFenced: true, Epoch: 1, ExpiresAt: time.Now().Add(-time.Second)}, {FencingToken: "x", OldPrimaryFenced: true, Epoch: 1, RequiredLSN: 2}} {
		_ = m.PromoteToMasterWithFencing(req)
	}
	atomic.StoreUint64(&m.promotionEpoch, 2)
	_ = m.PromoteToMasterWithFencing(valid)
	atomic.StoreUint64(&m.promotionEpoch, 0)
	m.masterConn = &replicationFaultConn{}
	req := valid
	req.AllowConnectedPeer = false
	_ = m.PromoteToMasterWithFencing(req)
	req.AllowConnectedPeer = true
	if err := m.PromoteToMasterWithFencing(req); err != nil {
		t.Fatal(err)
	}

	master := NewManager(&Config{Role: RoleMaster})
	for _, req := range []PrimaryFenceRequest{{}, {FencingToken: "x"}, {FencingToken: "x", Epoch: 1, ExpiresAt: time.Now().Add(-time.Second)}} {
		_ = master.FencePrimary(req)
	}
	if err := master.FencePrimary(PrimaryFenceRequest{FencingToken: "x", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	_ = master.FencePrimary(PrimaryFenceRequest{FencingToken: "x", Epoch: 2})
	_ = NewManager(&Config{Role: RoleSlave}).FencePrimary(PrimaryFenceRequest{})

	for _, req := range []RejoinRequest{{}, {FencingToken: "x"}, {FencingToken: "x", NewMasterAddr: "new"}} {
		_ = master.RejoinAsReplica(req)
	}
	master = NewManager(&Config{Role: RoleMaster})
	atomic.StoreUint64(&master.fencedEpoch, 3)
	_ = master.RejoinAsReplica(RejoinRequest{FencingToken: "x", NewMasterAddr: "new", Epoch: 2, LastAppliedLSN: 1})
	_ = master.RejoinAsReplica(RejoinRequest{FencingToken: "x", NewMasterAddr: "new", Epoch: 3})
	master.listener, _ = net.Listen("tcp", "127.0.0.1:0")
	master.masterConn = &replicationFaultConn{}
	master.slaves["s"] = &SlaveConnection{Conn: &replicationFaultConn{}}
	if err := master.RejoinAsReplica(RejoinRequest{FencingToken: "x", NewMasterAddr: "new", Epoch: 3, RequireSnapshot: true}); err != nil {
		t.Fatal(err)
	}
	_ = NewManager(&Config{Role: RoleSlave}).RejoinAsReplica(RejoinRequest{})
}

func TestReplicationWaitLagAndRetentionEdges(t *testing.T) {
	m := NewManager(&Config{Role: RoleMaster, Mode: ModeSync})
	atomic.StoreUint64(&m.currentLSN, 2)
	if err := m.WaitForSlaves(0); err == nil {
		t.Fatal("timeout expected")
	}
	m.slaves["s"] = &SlaveConnection{LastLSN: 2, LastPing: time.Time{}}
	if err := m.WaitForSlaves(time.Second); err != nil {
		t.Fatal(err)
	}
	m.slaves["s"].LastLSN = 1
	_ = m.currentReplicationLagMillis(time.Now())
	m.slaves["s"].LastPing = time.Now().Add(-time.Second)
	_ = m.currentReplicationLagMillis(time.Now())
	m.walBuffer = []*WALEntry{nil, {LSN: 1, Data: []byte("x")}}
	m.walBufferBytes = 99
	m.dropWALPrefixLocked(1)
	_ = retainedWALEntriesBytes(m.walBuffer)
	_ = retainedWALBytes(nil)
	m.DropConnections()
	for _, mode := range []ReplicationMode{ModeAsync, ModeSync, ModeFullSync, 99} {
		if replicationModeString(mode) == "" {
			t.Fatal("empty")
		}
	}
}
