# Benchmarks

## Running Benchmarks

```bash
# Run the bounded regression gate used for releases
./scripts/benchmark-gate.sh

# Compare against a saved baseline if benchstat is installed
BASELINE=artifacts/benchmarks/bench-main.txt ./scripts/benchmark-gate.sh

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

## Regression Gate

`scripts/benchmark-gate.sh` is the canonical bounded benchmark entrypoint for
release qualification. It runs a representative subset across SQL execution,
parser, B-tree, storage, WAL, and concurrent writer paths.

Default settings:

- `BENCHTIME=500ms`
- `COUNT=5`
- output: `artifacts/benchmarks/bench-<timestamp>.txt`

Recommended release flow:

```bash
# 1. Capture or reuse the baseline from the target branch.
git checkout main
./scripts/benchmark-gate.sh artifacts/benchmarks
cp artifacts/benchmarks/bench-*.txt artifacts/benchmarks/bench-main.txt

# 2. Run the candidate and compare.
git checkout -
BASELINE=artifacts/benchmarks/bench-main.txt ./scripts/benchmark-gate.sh artifacts/benchmarks
```

Gate policy:

- Investigate any statistically significant `benchstat` regression above 10%
  in `ns/op` or `B/op`.
- Treat regressions above 20% in core paths as release blockers unless a
  deliberate tradeoff is documented.
- Do not compare runs across different machines, CPU governors, Go versions, or
  `GOMAXPROCS` values.
- Keep `CacheSize` values as page counts. `1024` means 1024 pages, not bytes.

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

## Test Coverage (verified 2026-04-30)

| Package | Coverage | Package | Coverage |
|---------|----------|---------|----------|
| `pkg/pool` | 98.0% | `pkg/auth` | 96.8% |
| `pkg/fdw` | 96.7% | `pkg/metrics` | 96.4% |
| `pkg/advisor` | 95.8% | `pkg/wire` | 94.7% |
| `pkg/protocol` | 94.2% | `pkg/logger` | 94.3% |
| `pkg/optimizer` | 93.8% | `pkg/txn` | 93.4% |
| `pkg/security` | 93.2% | `pkg/btree` | 92.7% |
| `pkg/backup` | 92.0% | `pkg/replication` | 92.2% |
| `pkg/wasm` | 91.5% | `pkg/query` | 91.4% |
| `pkg/server` | 91.4% | `pkg/parallel` | 91.2% |
| `pkg/engine` | 90.9% | `pkg/catalog` | 90.4% |
| `pkg/cache` | 90.9% | `pkg/audit` | 90.3% |
| `pkg/storage` | 90.9% |  |  |

> **24/24 `pkg` packages above 90% coverage.** 7,100+ test functions across 600+ test files.

## Notes

- In-memory benchmarks show best-case performance
- Disk persistence adds ~20-40% overhead
- Index lookup is used automatically for WHERE clause optimizations
- Hash join is used for equality JOIN conditions
- CacheSize is in **pages** (not bytes): 1024 = 4MB, 2048 = 8MB
