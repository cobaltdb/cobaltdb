//go:build unix

package server

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Issue #3: a partial lifecycle config must not subscribe to every OS signal.
// Terminal resize signals must leave the server running; SIGTERM must stop it.
func TestLifecycleIgnoresWindowResizeSignal(t *testing.T) {
	const helperEnv = "COBALTDB_TEST_LIFECYCLE_SIGNAL_HELPER"
	if os.Getenv(helperEnv) == "1" {
		lifecycle := NewLifecycle(&LifecycleConfig{
			EnableSignalHandling: true,
			DrainTimeout:         time.Millisecond,
		})
		if err := lifecycle.Start(); err != nil {
			t.Fatal(err)
		}
		fmt.Println("lifecycle-ready")
		lifecycle.Wait()
		if err := lifecycle.Stop(); err != nil {
			t.Fatal(err)
		}
		fmt.Println("lifecycle-stopped")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestLifecycleIgnoresWindowResizeSignal$")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = cmd.Process.Kill()
			<-done
		}
	})

	ready := make(chan error, 1)
	stopped := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(stdout)
		line, err := reader.ReadString('\n')
		if err == nil && strings.TrimSpace(line) != "lifecycle-ready" {
			err = fmt.Errorf("unexpected child output: %q", line)
		}
		ready <- err
		if err != nil {
			return
		}
		_, err = reader.ReadString('\n')
		stopped <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("wait for signal handler: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("child did not install its signal handler")
	}

	if err := cmd.Process.Signal(syscall.SIGWINCH); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		// Observe lifecycle completion directly: the race detector can delay
		// process exit even after the server has already stopped.
		t.Fatalf("SIGWINCH stopped the lifecycle before process exit: %v", err)
	case err := <-done:
		exited = true
		t.Fatalf("SIGWINCH stopped the lifecycle: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		exited = true
		if err != nil {
			t.Fatalf("SIGTERM did not shut down cleanly: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SIGTERM did not stop the lifecycle")
	}
}
