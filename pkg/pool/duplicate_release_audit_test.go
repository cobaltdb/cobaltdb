package pool_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/pool"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func verifyRelease(kind string) error {
	var peers []net.Conn
	p, err := pool.New(&pool.Config{MinConns: 1, MaxConns: 2, HealthCheckInterval: time.Hour}, func() (net.Conn, error) { a, b := net.Pipe(); peers = append(peers, b); return a, nil })
	if err != nil {
		return err
	}
	defer func() {
		p.Close()
		for _, c := range peers {
			c.Close()
		}
	}()
	c, err := p.Acquire(context.Background())
	if err != nil {
		return err
	}
	switch kind {
	case "single":
		c.Release()
	case "ordered duplicate":
		start, done := make(chan struct{}), make(chan struct{})
		go func() { <-start; c.Release(); close(done) }()
		c.Release()
		close(start)
		<-done // force duplicate to finish before reacquisition
	case "concurrent duplicates":
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; c.Release() }()
		}
		close(start)
		wg.Wait() // no connection can be reacquired until all releases finish
	case "closed":
		if err := c.Close(); err != nil {
			return err
		}
		c.Release()
		c.Release()
		stats := p.Stats()
		if stats.TotalConns != 0 || stats.ActiveConns != 0 || stats.IdleConns != 0 {
			return fmt.Errorf("closed stats: %+v", stats)
		}
		fmt.Println("PASS: closed release is a no-op")
		return nil
	}
	stats := p.Stats()
	if stats.TotalConns != 1 || stats.IdleConns != 1 || stats.ActiveConns != 0 || stats.TotalReleases != 1 {
		return fmt.Errorf("%s stats: %+v", kind, stats)
	}
	a, err := p.Acquire(context.Background())
	if err != nil {
		return err
	}
	b, err := p.Acquire(context.Background())
	if err != nil {
		return err
	}
	if a == b {
		return fmt.Errorf("%s gave two callers the same connection", kind)
	}
	fmt.Printf("PASS: %s; active=0 idle=1 releases=1 distinct=true\n", kind)
	return nil
}

func verifyClosedDial() error {
	entered, finish := make(chan struct{}), make(chan struct{})
	client, peer := net.Pipe()
	defer peer.Close()
	p, err := pool.New(&pool.Config{MaxConns: 1, HealthCheckInterval: time.Hour}, func() (net.Conn, error) { close(entered); <-finish; return client, nil })
	if err != nil {
		return err
	}
	defer p.Close()
	result := make(chan error, 1)
	go func() { _, err := p.Acquire(context.Background()); result <- err }()
	<-entered
	if err := p.Close(); err != nil {
		return err
	}
	close(finish)
	if err := <-result; !errors.Is(err, pool.ErrPoolClosed) {
		return fmt.Errorf("stale dial error=%v", err)
	}
	if _, err := client.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		return fmt.Errorf("stale connection not closed: %v", err)
	}
	fmt.Println("PASS: gated dial completed after close is discarded")
	return nil
}
func TestAuditPoolDuplicateRelease(t *testing.T) {
	for _, kind := range []string{"single", "ordered duplicate", "concurrent duplicates", "closed"} {
		t.Run(kind, func(t *testing.T) {
			if err := verifyRelease(kind); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestAuditPoolDialCompletesAfterClose(t *testing.T) {
	if err := verifyClosedDial(); err != nil {
		t.Fatal(err)
	}
}
