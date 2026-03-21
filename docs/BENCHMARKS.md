# Benchmarks

## Running Benchmarks

```bash
# Run full benchmark suite (safe, bounded memory)
go test ./test/ -run=^$ -bench=BenchmarkFullSuite -benchtime=500ms -benchmem

# Run specific benchmark groups
go test ./test/ -run=^$ -bench="BenchmarkFullSuite/Select" -benchtime=500ms -benchmem
go test ./test/ -run=^$ -bench="BenchmarkFullSuite/Join" -benchtime=500ms -benchmem

# Run BTree benchmarks
go test ./pkg/btree/ -bench=. -benchtime=1s -benchmem

# Run storage benchmarks
go test ./pkg/storage/ -bench=. -benchtime=1s -benchmem

# Run parser benchmarks
go test ./pkg/query/ -bench=. -benchtime=1s -benchmem
```

## Results

### B-Tree Core Operations

| Operation | Latency | Throughput | Allocs/op |
|-----------|---------|------------|-----------|
| **PUT** | 641 ns | **1.56M ops/sec** | 4 |
| **PUT (Sequential)** | 694 ns | **1.44M ops/sec** | 5 |
| **GET (Point Lookup)** | 64 ns | **15.7M ops/sec** | 1 |
| **UPDATE** | 153 ns | **6.5M ops/sec** | 4 |
| **DELETE** | 197 ns | **5.1M ops/sec** | 4 |
| **SCAN (1K range)** | 270 µs | 3.7K ops/sec | 2044 |

### SQL Engine Performance (10K rows)

| Operation | Latency | Allocs/op | Detail |
|-----------|---------|-----------|--------|
| **INSERT** | 2.0 µs | 17 | Single row with SQL parsing |
| **INSERT Batch (100)** | 4.1 ms | 1837 | In transaction |
| **Point Lookup** | 2.1 µs | 27 | WHERE id = ? (indexed) |
| **Full Scan (1K)** | 598 µs | 12,808 | Custom fast decoder |
| **Full Scan (10K)** | 8.8 ms | 129,888 | Custom fast decoder |
| **WHERE (10K)** | 10.3 ms | 124,787 | Expression evaluation |
| **ORDER BY (10K)** | 7.6 ms | 129,894 | Sort + scan |
| **LIMIT 100 OFFSET 1K** | 3.7 ms | 31,985 | Early termination |
| **SUM/AVG (10K)** | 3.9–4.4 ms | 20,128 | Byte-level fast path |
| **COUNT(*) (10K)** | 3.4 ms | 20,125 | Skip row decode |
| **GROUP BY (10K)** | 8.9 ms | 130,241 | Full decode + grouping |
| **HAVING (10K)** | 9.4 ms | 150,055 | GROUP BY + filter |
| **Inner JOIN (1K)** | 700 µs | 11,782 | Hash join |
| **Inner JOIN (10K)** | 9.6 ms | 137,102 | Hash join |
| **Left JOIN (10K)** | 8.8 ms | 141,102 | Hash join |
| **3-Way JOIN (1K×3)** | 1.7 ms | 29,580 | Hash join |
| **Window RowNumber (10K)** | 10.0 ms | 139,671 | OVER (ORDER BY) |
| **Simple CTE** | 584 µs | 9,804 | View-based resolution |
| **Recursive CTE** | 3.6 µs | 32 | 1000 nodes |
| **UPDATE (single)** | 2.2 µs | 24 | WHERE id = ? |
| **UPDATE (bulk 10K)** | 9.2 ms | 105,240 | WHERE condition |
| **DELETE (single)** | 4.0 µs | 54 | WHERE id = ? |
| **DELETE (bulk 1K)** | 998 µs | 12,651 | WHERE condition |
| **Transaction** | 347 µs | 49 | Single statement |
| **Rollback** | 167 µs | 1,805 | 100 statements |
| **Concurrent Read (×20)** | 669 ns | 20 | Parallel goroutines |
| **Concurrent Write (×10)** | 2.7 µs | 14 | Parallel goroutines |
| **Index vs No Index** | 458 µs vs 8.7 ms | — | **19× faster** |

### Parser & Storage

| Component | Operation | Latency | Throughput |
|-----------|-----------|---------|------------|
| **SQL Parser** | Parse SELECT | 826 ns | **1.2M ops/sec** |
| **SQL Parser** | Parse INSERT | 1.0 µs | 960K ops/sec |
| **SQL Parser** | Parse Complex | 4.7 µs | 214K ops/sec |
| **Lexer** | Tokenize | 499 ns | **2.0M ops/sec** |
| **Buffer Pool** | Get Page (hit) | 27 ns | **36.5M ops/sec** |
| **Buffer Pool** | New Page | 678 ns | 1.5M ops/sec |
| **Memory Backend** | Write 4KB | 112 ns | 8.9M ops/sec |
| **Memory Backend** | Read 4KB | 34 ns | 29.4M ops/sec |
| **WAL** | Append | 192 µs | 5.2K ops/sec |
| **VersionedRow Decode** | Fast decoder | 204 ns | **4.9M ops/sec** |
| **VersionedRow Decode** | json.Unmarshal | 1,051 ns | 951K ops/sec |

### Key Optimizations

| Optimization | Before | After | Speedup |
|-------------|--------|-------|---------|
| Custom VersionedRow decoder | 1,051 ns | 204 ns | **5.2×** |
| SUM/AVG byte-level fast path | 14 ms | 3.9 ms | **3.6×** |
| LIMIT/OFFSET early termination | 17 ms | 3.7 ms | **4.6×** |
| Hash join key (strconv vs fmt) | 12 ms | 9.6 ms | **1.3×** |
| MemoryBackend capacity reuse | 64 MB/op | 12 B/op | **5,300,000×** |

## Test Environment

- **CPU:** AMD Ryzen 9 9950X3D (16-Core)
- **Go:** 1.26
- **OS:** Windows 11
- **Mode:** In-memory (no disk I/O)

## Test Coverage (v0.3.0)

| Package | Coverage | Package | Coverage |
|---------|----------|---------|----------|
| `pkg/pool` | 97.5% | `pkg/wasm` | 93.4% |
| `pkg/auth` | 96.8% | `pkg/btree` | 92.4% |
| `pkg/cache` | 95.5% | `pkg/backup` | 91.9% |
| `pkg/protocol` | 95.5% | `pkg/security` | 91.9% |
| `pkg/wire` | 94.7% | `pkg/replication` | 91.8% |
| `pkg/metrics` | 94.2% | `pkg/query` | 91.0% |
| `pkg/optimizer` | 93.8% | `pkg/audit` | 90.9% |
| `pkg/logger` | 93.8% | `pkg/server` | 90.2% |
| `pkg/txn` | 93.5% | `pkg/storage` | 90.2% |
| `pkg/engine` | 90.0% | `pkg/catalog` | 85.2% |

> **19/20 packages above 90% coverage.** 10,400+ tests across 22 packages.

## Notes

- In-memory benchmarks show best-case performance
- Disk persistence adds ~20-40% overhead
- Index lookup is used automatically for WHERE clause optimizations
- Hash join is used for equality JOIN conditions
- CacheSize is in **pages** (not bytes): 1024 = 4MB, 2048 = 8MB
