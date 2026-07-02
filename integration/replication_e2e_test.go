package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func openReplicationMaster(t *testing.T, dir, mode string, syncTimeout time.Duration) *engine.DB {
	t.Helper()
	opts := engine.DefaultOptions()
	opts.Replication.Role = "master"
	opts.Replication.ListenAddr = "127.0.0.1:0"
	opts.Replication.Mode = mode
	if syncTimeout > 0 {
		opts.Replication.SyncTimeout = syncTimeout
	}
	master, err := engine.Open(filepath.Join(dir, "master.db"), opts)
	if err != nil {
		t.Fatalf("failed to open master: %v", err)
	}
	return master
}

func openReplicationSlave(t *testing.T, dir, masterAddr string) *engine.DB {
	t.Helper()
	opts := engine.DefaultOptions()
	opts.Replication.Role = "slave"
	opts.Replication.MasterAddr = masterAddr
	opts.Replication.StateFile = filepath.Join(dir, "slave.state.json")
	slave, err := engine.Open(filepath.Join(dir, "slave.db"), opts)
	if err != nil {
		t.Fatalf("failed to open slave: %v", err)
	}
	return slave
}

// queryUserRows returns "id:name:age" strings ordered by id, or an error.
func queryUserRows(db *engine.DB) ([]string, error) {
	rows, err := db.Query(context.Background(), "SELECT id, name, age FROM users ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id, age int64
		var name string
		if err := rows.Scan(&id, &name, &age); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%d:%s:%d", id, name, age))
	}
	return out, nil
}

// waitForConvergence polls until the slave's users table matches the master's.
func waitForConvergence(t *testing.T, master, slave *engine.DB, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastMaster, lastSlave []string
	var lastErr error
	for time.Now().Before(deadline) {
		masterRows, mErr := queryUserRows(master)
		slaveRows, sErr := queryUserRows(slave)
		lastMaster, lastSlave = masterRows, slaveRows
		lastErr = sErr
		if mErr == nil && sErr == nil && equalStringSlices(masterRows, slaveRows) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("slave did not converge within %s\n master: %v\n slave:  %v (last slave err: %v)",
		timeout, lastMaster, lastSlave, lastErr)
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestReplicationEndToEnd verifies statement-based replication end to end:
// DDL + DML on the master appear on the slave, the link surviving a forced
// disconnect with automatic reconnect and convergence.
func TestReplicationEndToEnd(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	master := openReplicationMaster(t, dir, "async", 0)
	defer master.Close()

	addr := master.GetReplicationManager().ListenAddr()
	if addr == "" {
		t.Fatal("master replication manager is not listening")
	}

	slave := openReplicationSlave(t, dir, addr)
	defer slave.Close()

	// DDL + DML on the master
	if _, err := master.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE on master: %v", err)
	}
	if _, err := master.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)"); err != nil {
		t.Fatalf("INSERT literal on master: %v", err)
	}
	// Prepared-statement style write with bound args
	if _, err := master.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", int64(2), "bob", int64(25)); err != nil {
		t.Fatalf("INSERT with args on master: %v", err)
	}
	if _, err := master.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (3, 'carol', 41)"); err != nil {
		t.Fatalf("INSERT on master: %v", err)
	}
	if _, err := master.Exec(ctx, "UPDATE users SET age = ? WHERE id = ?", int64(31), int64(1)); err != nil {
		t.Fatalf("UPDATE on master: %v", err)
	}
	if _, err := master.Exec(ctx, "DELETE FROM users WHERE id = 3"); err != nil {
		t.Fatalf("DELETE on master: %v", err)
	}

	waitForConvergence(t, master, slave, 15*time.Second)

	// Kill the replication link from the slave side; the slave must reconnect
	// with backoff and resume from its persisted position.
	slave.GetReplicationManager().DropConnections()

	if _, err := master.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 52)"); err != nil {
		t.Fatalf("INSERT after disconnect: %v", err)
	}
	if _, err := master.Exec(ctx, "UPDATE users SET name = 'alice2' WHERE id = 1"); err != nil {
		t.Fatalf("UPDATE after disconnect: %v", err)
	}

	waitForConvergence(t, master, slave, 15*time.Second)

	// Also verify explicit transactions replicate on COMMIT, and rolled-back
	// transactions do not replicate at all.
	tx, err := master.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (5, 'eve', 19)"); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx Commit: %v", err)
	}

	rb, err := master.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin rollback tx: %v", err)
	}
	if _, err := rb.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (6, 'mallory', 66)"); err != nil {
		t.Fatalf("rollback tx INSERT: %v", err)
	}
	if err := rb.Rollback(); err != nil {
		t.Fatalf("tx Rollback: %v", err)
	}

	waitForConvergence(t, master, slave, 15*time.Second)

	slaveRows, err := queryUserRows(slave)
	if err != nil {
		t.Fatalf("query slave: %v", err)
	}
	for _, row := range slaveRows {
		if strings.Contains(row, "mallory") {
			t.Errorf("rolled-back insert leaked to slave: %v", slaveRows)
		}
	}
	found := false
	for _, row := range slaveRows {
		if row == "5:eve:19" {
			found = true
		}
	}
	if !found {
		t.Errorf("committed transaction insert missing on slave: %v", slaveRows)
	}
}

// TestReplicationSlaveSnapshotOnConnect verifies a slave that connects AFTER
// the master already has data receives a snapshot and then live changes.
func TestReplicationSlaveSnapshotOnConnect(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	master := openReplicationMaster(t, dir, "async", 0)
	defer master.Close()

	if _, err := master.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := master.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'pre-existing', 99)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	slave := openReplicationSlave(t, dir, master.GetReplicationManager().ListenAddr())
	defer slave.Close()

	waitForConvergence(t, master, slave, 15*time.Second)

	// Live change after the snapshot-based catch-up
	if _, err := master.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (2, 'post-snapshot', 1)"); err != nil {
		t.Fatalf("INSERT post-snapshot: %v", err)
	}
	waitForConvergence(t, master, slave, 15*time.Second)
}

// TestReplicationSyncWritesDuringSlaveConnect exercises the interaction
// between sync-mode writes (which push entries and wait for ACKs) and a slave
// connecting mid-stream (which triggers a master-side snapshot): this
// combination previously had lock-inversion deadlock potential between the
// snapshot capture lock and the slave connection mutex.
func TestReplicationSyncWritesDuringSlaveConnect(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	master := openReplicationMaster(t, dir, "sync", 500*time.Millisecond)
	defer master.Close()

	// Pre-existing data so the connecting slave takes the snapshot path.
	if _, err := master.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := master.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'seed', 1)"); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}

	// Concurrent writer while the slave connects and receives its snapshot.
	writerDone := make(chan error, 1)
	go func() {
		for i := 2; i <= 20; i++ {
			if _, err := master.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
				int64(i), fmt.Sprintf("user%d", i), int64(i)); err != nil {
				writerDone <- err
				return
			}
		}
		writerDone <- nil
	}()

	slave := openReplicationSlave(t, dir, master.GetReplicationManager().ListenAddr())
	defer slave.Close()

	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("concurrent writer failed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent writer deadlocked while slave was connecting")
	}

	waitForConvergence(t, master, slave, 15*time.Second)
}

// TestReplicationFullSyncFailsWithoutAck verifies the full_sync durability
// contract: with no connected slave the write commits locally but returns an
// error after the (short) ACK timeout.
func TestReplicationFullSyncFailsWithoutAck(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	master := openReplicationMaster(t, dir, "full_sync", 300*time.Millisecond)
	defer master.Close()

	start := time.Now()
	_, err := master.Exec(ctx, "CREATE TABLE t_sync (id INTEGER PRIMARY KEY)")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("full_sync write with no slaves should return a replication sync error")
	}
	if !strings.Contains(err.Error(), "replication sync") {
		t.Fatalf("expected replication sync error, got: %v", err)
	}
	if elapsed < 250*time.Millisecond {
		t.Errorf("write returned after %v; expected it to block for ~300ms ACK timeout", elapsed)
	}

	// The write still committed locally (degrade-explicitly semantics).
	rows, err := master.Query(ctx, "SELECT COUNT(*) FROM t_sync")
	if err != nil {
		t.Fatalf("table should exist locally despite sync error: %v", err)
	}
	rows.Close()

	// Once a slave connects and ACKs, sync writes succeed.
	slave := openReplicationSlave(t, dir, master.GetReplicationManager().ListenAddr())
	defer slave.Close()

	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err = master.Exec(ctx, "INSERT INTO t_sync (id) VALUES (1)")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("full_sync write kept failing after slave connected: %v", err)
		}
		// The first attempts can race the slave's initial catch-up; retry with
		// a fresh id is unnecessary since the insert failed only on sync wait?
		// No: on sync timeout the row IS committed locally, so remove it
		// before retrying to keep the statement idempotent.
		if _, delErr := master.Exec(ctx, "DELETE FROM t_sync WHERE id = 1"); delErr != nil &&
			!strings.Contains(delErr.Error(), "replication sync") {
			t.Fatalf("cleanup delete failed: %v", delErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestReplicationSyncStrictErrorsWithoutAck verifies "sync" mode with
// SyncStrict: the write returns an error when no slave ACKs in time. Without
// SyncStrict, sync mode degrades to async with only a logged warning.
func TestReplicationSyncStrictErrorsWithoutAck(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	opts := engine.DefaultOptions()
	opts.Replication.Role = "master"
	opts.Replication.ListenAddr = "127.0.0.1:0"
	opts.Replication.Mode = "sync"
	opts.Replication.SyncTimeout = 200 * time.Millisecond
	opts.Replication.SyncStrict = true
	master, err := engine.Open(filepath.Join(dir, "master.db"), opts)
	if err != nil {
		t.Fatalf("open master: %v", err)
	}
	defer master.Close()

	if _, err := master.Exec(ctx, "CREATE TABLE t_strict (id INTEGER)"); err == nil {
		t.Fatal("strict sync write with no slaves should return an error")
	}

	// Non-strict sync mode: same situation must NOT error (degrades to async).
	dir2 := t.TempDir()
	opts2 := engine.DefaultOptions()
	opts2.Replication.Role = "master"
	opts2.Replication.ListenAddr = "127.0.0.1:0"
	opts2.Replication.Mode = "sync"
	opts2.Replication.SyncTimeout = 200 * time.Millisecond
	master2, err := engine.Open(filepath.Join(dir2, "master.db"), opts2)
	if err != nil {
		t.Fatalf("open master2: %v", err)
	}
	defer master2.Close()

	if _, err := master2.Exec(ctx, "CREATE TABLE t_degrade (id INTEGER)"); err != nil {
		t.Fatalf("non-strict sync write should degrade to async, got error: %v", err)
	}
}
