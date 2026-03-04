# CobaltDB Benchmark Report

**Generated:** 2026-03-03 23:34:28

## System Information

- **Go Version:** go1.26.0
- **OS:** windows
- **Architecture:** amd64
- **CPUs:** 16

## Summary

| Metric | Value |
|--------|-------|
| Total Tests | 22 |
| Passed | 22 |
| Failed | 0 |
| Success Rate | 100.0% |

## Transaction

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| Transaction (10 ops) | 1151 | 868.72µs | 509.4µs | 1.0325ms | 4.3 MB | ✅ |

## JSON

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| JSON Insert | 513031 | 1.949µs | 0s | 974.6µs | 294.4 KB | ✅ |
| JSON_EXTRACT | 594 | 1.683348ms | 2.0035ms | 2.8464ms | 17.0 MB | ✅ |

## Advanced

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| CREATE MATERIALIZED VIEW | 6397 | 156.327µs | 515.6µs | 553.2µs | 9.9 MB | ✅ |
| REFRESH MATERIALIZED VIEW | 6774 | 147.626µs | 0s | 670.8µs | 4.8 MB | ✅ |
| CTE Query | 6271 | 159.455µs | 0s | 687.1µs | 1.2 MB | ✅ |
| Window Functions | 0 | 0s | 0s | 0s | 0 B | ✅ |

## DDL

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| CREATE TABLE | 191975 | 5.209µs | 0s | 520.9µs | 773.6 KB | ✅ |
| CREATE INDEX | 4851 | 206.16µs | 0s | 519µs | 371.7 KB | ✅ |
| CREATE VIEW | 486287 | 2.056µs | 0s | 515.6µs | 968.7 KB | ✅ |

## DML

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| INSERT Single | 947508 | 1.055µs | 0s | 534.3µs | 456.3 KB | ✅ |
| UPDATE | 231 | 4.33523ms | 3.5182ms | 5.1999ms | 5.6 MB | ✅ |
| DELETE | 230 | 4.34654ms | 3.5106ms | 5.091ms | 5.4 MB | ✅ |

## Query

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| SELECT Point Lookup | 8270 | 120.912µs | 0s | 2.015ms | 5.5 MB | ✅ |
| SELECT with WHERE | 6225 | 160.646µs | 0s | 2.0207ms | 2.8 MB | ✅ |
| SELECT with ORDER BY | 8641 | 115.728µs | 0s | 2.0049ms | 3.6 MB | ✅ |
| SELECT with JOIN | 7 | 150.06174ms | 139.5465ms | 158.8797ms | 1.2 GB | ✅ |
| SELECT with Aggregation | 7729 | 129.388µs | 0s | 749.9µs | 3.3 MB | ✅ |

## Maintenance

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| VACUUM | 18821 | 53.132µs | 526µs | 550.5µs | 2.6 MB | ✅ |
| ANALYZE | 9266 | 107.92µs | 0s | 552.9µs | 435.3 KB | ✅ |

## Concurrency

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| Concurrent Reads (10 goroutines) | 109 | 9.2136ms | 5.5379ms | 11.3446ms | 40.7 MB | ✅ |

## Server

| Operation | Ops/Sec | Avg Latency | Min Latency | Max Latency | Memory | Status |
|-----------|---------|-------------|-------------|-------------|--------|--------|
| Server Startup | 180453 | 5.541µs | 0s | 595.3µs | 6.7 MB | ✅ |
| Database Open/Close | 234973 | 4.255µs | 0s | 542.8µs | 6.7 MB | ✅ |

## Notes

- All benchmarks run on in-memory database (`:memory:`)
- Latency measurements include Go runtime overhead
- Memory usage is approximate (measured via runtime.ReadMemStats)
- Concurrent benchmarks use goroutines
