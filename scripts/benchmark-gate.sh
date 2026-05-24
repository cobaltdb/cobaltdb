#!/usr/bin/env bash
# Run the bounded benchmark set used as the performance regression gate.

set -euo pipefail

OUT_DIR="${1:-artifacts/benchmarks}"
BENCHTIME="${BENCHTIME:-500ms}"
COUNT="${COUNT:-5}"
if [[ -z "${GOMAXPROCS:-}" ]]; then
	if command -v nproc >/dev/null 2>&1; then
		GOMAXPROCS="$(nproc)"
	else
		GOMAXPROCS="1"
	fi
fi

mkdir -p "$OUT_DIR"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out_file="$OUT_DIR/bench-$stamp.txt"

echo "CobaltDB benchmark gate"
echo "  output: $out_file"
echo "  benchtime: $BENCHTIME"
echo "  count: $COUNT"
echo "  GOMAXPROCS: $GOMAXPROCS"
echo

run_bench() {
	local package_pattern="$1"
	local bench_pattern="$2"
	echo "==> go test $package_pattern -run=^$ -bench=$bench_pattern"
	GOMAXPROCS="$GOMAXPROCS" go test "$package_pattern" \
		-run=^$ \
		-bench="$bench_pattern" \
		-benchtime="$BENCHTIME" \
		-count="$COUNT" \
		-benchmem | tee -a "$out_file"
	echo >> "$out_file"
}

run_bench ./test/ 'Benchmark(Insert$|Select$|Transaction$|ConcurrentInsert$|FullSuite/(Insert|Select|Join|Aggregation|Concurrent))'
run_bench ./pkg/engine/ 'Benchmark(ExecInsert$|ExecSelect$|ExecSelectIndexed$|TransactionCommit$|PreparedStatement$|ConcurrentWriters$|WriteLatencyUnderReaders)'
run_bench ./pkg/query/ 'Benchmark(ParseSelect$|ParseInsert$|ParseComplexSelect$|LexerTokenize$)'
run_bench ./pkg/btree/ 'Benchmark(BTreePut$|BTreeGet$|BTreeScan$|Put$|Get$|Scan$)'
run_bench ./pkg/storage/ 'Benchmark(BufferPoolGetPage$|BufferPoolNewPage$|WALAppend$|WALAppendBatch$)'

echo
echo "Benchmark output written to $out_file"

if [[ -n "${BASELINE:-}" ]]; then
	if ! command -v benchstat >/dev/null 2>&1; then
		echo "BASELINE was set, but benchstat is not installed." >&2
		echo "Install with: go install golang.org/x/perf/cmd/benchstat@latest" >&2
		exit 2
	fi
	compare_file="$OUT_DIR/benchstat-$stamp.txt"
	benchstat "$BASELINE" "$out_file" | tee "$compare_file"
	echo "Benchstat comparison written to $compare_file"
fi
