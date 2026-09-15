package protocol

// Regression test for the handleStatistics data race:
//
// handleStatistics read len(server.clients) WITHOUT holding server.mu, while
// handleConnection (register/delete) and Close (delete) mutate the same map
// under server.mu, and handleProcessInfo reads it under server.mu. A
// COM_STATISTICS command arriving concurrently with connection churn was a
// genuine data race (Go maps are unsafe for concurrent read-during-write).
//
// Each statistics goroutine owns its own MySQLClient — production gives every
// connection its own client, so c.sequence stays single-goroutine — isolating
// exactly the cross-goroutine s.clients access. The clients use a nil conn:
// handleStatistics builds the stats string (the racy read) before
// writePacket short-circuits on the nil connection.

import (
	"sync"
	"testing"
	"time"
)

func TestHandleStatisticsNoDataRaceWithConnectionChurn(t *testing.T) {
	s := NewMySQLServer(nil, "5.7.0-CobaltDB-race")

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Connection churn mirrors handleConnection's register/delete discipline:
	// both map mutations happen under s.mu.
	wg.Add(1)
	go func() {
		defer wg.Done()
		id := uint32(1000)
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.mu.Lock()
			id++
			s.clients[id] = nil
			s.mu.Unlock()
			s.mu.Lock()
			delete(s.clients, id)
			s.mu.Unlock()
		}
	}()

	// Concurrent COM_STATISTICS handlers reading len(s.clients).
	for i := 0; i < 4; i++ {
		client := &MySQLClient{
			server:      s,
			connID:      uint32(i + 1),
			connectTime: time.Now(),
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = client.handleStatistics()
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}
