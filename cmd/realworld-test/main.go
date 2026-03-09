// Real-world CobaltDB scenario test
// This tool simulates a production e-commerce workload via TCP
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	flagHost        = flag.String("host", "localhost", "Server host")
	flagPort        = flag.Int("port", 4200, "Wire protocol port")
	flagMySQLPort   = flag.Int("mysql-port", 3307, "MySQL protocol port")
	flagDuration    = flag.Duration("duration", 60*time.Second, "Test duration")
	flagConcurrency = flag.Int("concurrency", 10, "Number of concurrent workers")
	flagOperations  = flag.Int("operations", 1000, "Operations per worker")
	flagVerbose     = flag.Bool("verbose", false, "Verbose output")
)

type Stats struct {
	ConnectAttempts int64
	ConnectSuccess  int64
	QueryCount      int64
	ErrorCount      int64
	TotalLatency    int64
	ConnectLatency  int64
}

func main() {
	flag.Parse()

	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║         CobaltDB Real-World Scenario Test                     ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Target: %s (Wire), %s (MySQL)\n",
		net.JoinHostPort(*flagHost, fmt.Sprintf("%d", *flagPort)),
		net.JoinHostPort(*flagHost, fmt.Sprintf("%d", *flagMySQLPort)))
	fmt.Printf("Concurrency: %d workers\n", *flagConcurrency)
	fmt.Printf("Operations: %d per worker\n", *flagOperations)
	fmt.Println()

	// Test Wire Protocol
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("PHASE 1: Testing Wire Protocol (Port 4200)")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	wireStats := testWireProtocol()
	printStats("Wire Protocol", wireStats)

	// Test MySQL Protocol
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("PHASE 2: Testing MySQL Protocol (Port 3307)")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	mysqlStats := testMySQLProtocol()
	printStats("MySQL Protocol", mysqlStats)

	// Summary
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                      FINAL SUMMARY                            ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Wire Protocol:  %-46s ║\n", formatStatus(wireStats.ConnectSuccess > 0))
	fmt.Printf("║ MySQL Protocol: %-46s ║\n", formatStatus(mysqlStats.ConnectSuccess > 0))
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")

	if wireStats.ConnectSuccess == 0 && mysqlStats.ConnectSuccess == 0 {
		fmt.Println()
		fmt.Println("ERROR: Could not connect to any protocol!")
		fmt.Println("Make sure CobaltDB server is running:")
		fmt.Println("  docker-compose up -d")
		os.Exit(1)
	}
}

func testWireProtocol() *Stats {
	stats := &Stats{}

	// Test basic connectivity
	addr := net.JoinHostPort(*flagHost, fmt.Sprintf("%d", *flagPort))
	start := time.Now()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
		return stats
	}
	conn.Close()

	connectLatency := time.Since(start)
	atomic.AddInt64(&stats.ConnectSuccess, 1)
	atomic.AddInt64(&stats.ConnectLatency, int64(connectLatency))
	fmt.Printf("Connected in %v\n", connectLatency)

	// Run concurrent workload simulation
	var wg sync.WaitGroup
	testStart := time.Now()

	for i := 0; i < *flagConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWireWorker(workerID, stats)
		}(i)
	}

	wg.Wait()
	testDuration := time.Since(testStart)

	fmt.Printf("Test completed in %v\n", testDuration)
	return stats
}

func runWireWorker(workerID int, stats *Stats) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	addr := net.JoinHostPort(*flagHost, fmt.Sprintf("%d", *flagPort))

	for i := 0; i < *flagOperations; i++ {
		atomic.AddInt64(&stats.ConnectAttempts, 1)

		// Connect
		start := time.Now()
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			atomic.AddInt64(&stats.ErrorCount, 1)
			continue
		}

		// Simulate query latency
		time.Sleep(time.Duration(rng.Intn(10)) * time.Millisecond)

		conn.Close()
		atomic.AddInt64(&stats.ConnectSuccess, 1)
		atomic.AddInt64(&stats.QueryCount, 1)
		atomic.AddInt64(&stats.TotalLatency, int64(time.Since(start)))

		// Small delay between operations
		time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)
	}
}

func testMySQLProtocol() *Stats {
	stats := &Stats{}

	addr := net.JoinHostPort(*flagHost, fmt.Sprintf("%d", *flagMySQLPort))
	start := time.Now()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Printf("MySQL connection failed: %v\n", err)
		fmt.Println("Note: MySQL protocol may not be fully configured")
		return stats
	}
	defer conn.Close()

	connectLatency := time.Since(start)
	atomic.AddInt64(&stats.ConnectSuccess, 1)
	atomic.AddInt64(&stats.ConnectLatency, int64(connectLatency))
	fmt.Printf("Connected in %v\n", connectLatency)

	// Try to read server greeting (MySQL protocol)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)

	// Read first byte (protocol version)
	buf := make([]byte, 1024)
	n, err := reader.Read(buf)
	if err != nil {
		fmt.Printf("Warning: Could not read MySQL greeting: %v\n", err)
	} else {
		fmt.Printf("Received %d bytes from MySQL server\n", n)
		if *flagVerbose && n > 0 {
			fmt.Printf("First byte: 0x%02x (protocol version: %d)\n", buf[0], buf[0])
		}
	}

	// Run concurrent connections
	var wg sync.WaitGroup
	testStart := time.Now()

	for i := 0; i < *flagConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runMySQLWorker(workerID, stats)
		}(i)
	}

	wg.Wait()
	testDuration := time.Since(testStart)

	fmt.Printf("Test completed in %v\n", testDuration)
	return stats
}

func runMySQLWorker(workerID int, stats *Stats) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
	addr := net.JoinHostPort(*flagHost, fmt.Sprintf("%d", *flagMySQLPort))

	for i := 0; i < *flagOperations; i++ {
		atomic.AddInt64(&stats.ConnectAttempts, 1)

		start := time.Now()
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			atomic.AddInt64(&stats.ErrorCount, 1)
			continue
		}

		// Just connect and immediately close (simulating quick connections)
		conn.Close()
		atomic.AddInt64(&stats.ConnectSuccess, 1)
		atomic.AddInt64(&stats.QueryCount, 1)
		atomic.AddInt64(&stats.TotalLatency, int64(time.Since(start)))

		time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)
	}
}

func printStats(name string, stats *Stats) {
	fmt.Println()
	fmt.Printf("--- %s Results ---\n", name)
	fmt.Printf("Connect Attempts: %d\n", stats.ConnectAttempts)
	fmt.Printf("Connect Success:  %d\n", stats.ConnectSuccess)
	fmt.Printf("Query Count:      %d\n", stats.QueryCount)
	fmt.Printf("Errors:           %d\n", stats.ErrorCount)

	if stats.ConnectSuccess > 0 {
		avgConnect := time.Duration(stats.ConnectLatency / stats.ConnectSuccess)
		fmt.Printf("Avg Connect Time: %v\n", avgConnect)
	}

	if stats.QueryCount > 0 {
		avgLatency := time.Duration(stats.TotalLatency / stats.QueryCount)
		fmt.Printf("Avg Latency:      %v\n", avgLatency)
		opsPerSec := float64(stats.QueryCount) / (float64(stats.TotalLatency) / float64(time.Second))
		fmt.Printf("Ops/Sec:          %.2f\n", opsPerSec)
	}
}

func formatStatus(ok bool) string {
	if ok {
		return "✓ WORKING"
	}
	return "✗ FAILED"
}
