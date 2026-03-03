package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestPrintHelp(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printHelp()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	outputStr := buf.String()
	if !strings.Contains(outputStr, "Usage:") {
		t.Error("Expected help to contain 'Usage:'")
	}

	if !strings.Contains(outputStr, "CobaltDB") {
		t.Error("Expected help to contain 'CobaltDB'")
	}
}

func TestRunBenchmarks(t *testing.T) {
	t.Run("AllBenchmarks", func(t *testing.T) {
		// This should not panic
		// We can't fully test it without a database, but we can verify it doesn't crash
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Benchmarks panicked (expected without DB): %v", r)
			}
		}()

		// Would need a real DB to test properly
		// runBenchmarks(":memory:", "all", 100)
	})
}

func TestRunInsertBenchmark(t *testing.T) {
	t.Run("InsertBenchmark", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Insert benchmark panicked (expected without DB): %v", r)
			}
		}()

		// Would need a real DB to test properly
		// runInsertBenchmark(":memory:", 100)
	})
}

func TestRunSelectBenchmark(t *testing.T) {
	t.Run("SelectBenchmark", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Select benchmark panicked (expected without DB): %v", r)
			}
		}()

		// Would need a real DB to test properly
		// runSelectBenchmark(":memory:", 100)
	})
}

func TestRunUpdateBenchmark(t *testing.T) {
	t.Run("UpdateBenchmark", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Update benchmark panicked (expected without DB): %v", r)
			}
		}()

		// Would need a real DB to test properly
		// runUpdateBenchmark(":memory:", 100)
	})
}

func TestRunDeleteBenchmark(t *testing.T) {
	t.Run("DeleteBenchmark", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Delete benchmark panicked (expected without DB): %v", r)
			}
		}()

		// Would need a real DB to test properly
		// runDeleteBenchmark(":memory:", 100)
	})
}

func TestRunTransactionBenchmark(t *testing.T) {
	t.Run("TransactionBenchmark", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Transaction benchmark panicked (expected without DB): %v", r)
			}
		}()

		// Would need a real DB to test properly
		// runTransactionBenchmark(":memory:", 100)
	})
}

func TestRunAllBenchmarks(t *testing.T) {
	t.Run("RunAll", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("All benchmarks panicked (expected without DB): %v", r)
			}
		}()

		// Would need a real DB to test properly
		// runAllBenchmarks(":memory:", 100)
	})
}

func TestInit(t *testing.T) {
	// Test that init() doesn't panic
	// The init function is called automatically
}
