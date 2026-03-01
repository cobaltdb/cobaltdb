# Benchmarks

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchtime=2s ./test/...

# Run specific benchmark
go test -bench=BenchmarkInsert -benchtime=2s ./test/...

# Run BTree benchmarks
go test -bench=. -benchtime=2s ./pkg/btree/...
```

## Benchmark CLI

```bash
# Build benchmark tool
go build -o cobaltdb-bench ./cmd/cobaltdb-bench

# Run all benchmarks
./cobaltdb-bench

# Run specific benchmark
./cobaltdb-bench -bench insert
./cobaltdb-bench -bench select
./cobaltdb-bench -bench update
./cobaltdb-bench -bench delete
./cobaltdb-bench -bench transaction

# Custom row count
./cobaltdb-bench -rows 50000
```

## Results

### Engine Benchmarks (10K rows)

| Operation | Time per op | Ops/sec |
|----------|-------------|---------|
| INSERT | ~3,200 ns | ~310,000 |
| INSERT Batch | ~320,000 ns | ~3,100 |
| SELECT (full scan) | ~7,700,000 ns | ~130 |
| SELECT + Scan | ~750,000 ns | ~1,300 |
| SELECT WHERE | ~11,700,000 ns | ~85 |
| UPDATE (single) | ~1,060,000 ns | ~940 |
| DELETE (single) | ~1,600,000 ns | ~620 |
| Transaction | ~3,400 ns | ~290,000 |
| Concurrent Insert | ~2,100 ns | ~470,000 |

### BTree Benchmarks

| Operation | Time per op | Ops/sec |
|----------|-------------|---------|
| Put | ~1,300 ns | ~770,000 |
| Put Sequential | ~1,200 ns | ~830,000 |
| Get | ~300 ns | ~3,300,000 |
| Scan (1K rows) | ~81,000 ns | ~12 |
| Delete | ~1,400 ns | ~710,000 |
| Update | ~230 ns | ~4,300,000 |

## Test Environment

- CPU: AMD Ryzen 7 PRO 6850H
- Go Version: 1.26.0
- OS: Windows

## Notes

- Results may vary based on hardware
- In-memory benchmarks show best-case performance
- Disk persistence adds overhead
- Index creation is supported but not yet used in query execution
