package replication

import (
	"sync"
	"testing"
	"time"
)

func TestSlaveStatusClearsConnectionOnMasterDisconnect(t *testing.T) {
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.SyncInterval = 10 * time.Millisecond

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("start master: %v", err)
	}

	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = master.listener.Addr().String()
	slaveConfig.SyncInterval = 10 * time.Millisecond

	slave := NewManager(slaveConfig)
	disconnected := make(chan struct{})
	var once sync.Once
	slave.OnDisconnect = func(peer string, err error) {
		if peer == "master" {
			once.Do(func() { close(disconnected) })
		}
	}

	if err := slave.Start(); err != nil {
		_ = master.Stop()
		t.Fatalf("start slave: %v", err)
	}
	defer slave.Stop()

	waitForActiveSlaves(t, master, 1)

	if err := master.Stop(); err != nil {
		t.Fatalf("stop master: %v", err)
	}

	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("slave did not observe master disconnect")
	}

	if status := slave.GetStatus(); status.Connected {
		t.Fatalf("slave status still reports connected after master disconnect: %+v", status)
	}
}

func waitForActiveSlaves(t *testing.T, mgr *Manager, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := mgr.GetMetrics().ActiveSlaves; got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("active slaves did not reach %d, got %d", want, mgr.GetMetrics().ActiveSlaves)
}
