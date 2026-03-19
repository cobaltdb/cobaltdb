package wasm

import (
	"testing"
)

// TestTransactionSupport tests WASM transaction operations
func TestTransactionSupport(t *testing.T) {
	t.Run("begin_transaction", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Call beginTransaction
		params := []uint64{}
		result, err := host.beginTransaction(rt, params)
		if err != nil {
			t.Fatalf("beginTransaction failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		if !host.txActive {
			t.Error("Expected transaction to be active")
		}

		t.Log("Transaction started successfully")
	})

	t.Run("commit_transaction", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Begin transaction first
		host.beginTransaction(rt, []uint64{})

		// Commit transaction
		result, err := host.commitTransaction(rt, []uint64{})
		if err != nil {
			t.Fatalf("commitTransaction failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		if host.txActive {
			t.Error("Expected transaction to be inactive after commit")
		}

		t.Log("Transaction committed successfully")
	})

	t.Run("rollback_transaction", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Begin transaction
		host.beginTransaction(rt, []uint64{})

		// Rollback transaction
		result, err := host.rollbackTransaction(rt, []uint64{})
		if err != nil {
			t.Fatalf("rollbackTransaction failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		if host.txActive {
			t.Error("Expected transaction to be inactive after rollback")
		}

		t.Log("Transaction rolled back successfully")
	})

	t.Run("savepoint", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Begin transaction
		host.beginTransaction(rt, []uint64{})

		// Create savepoint
		params := []uint64{1} // savepointId = 1
		result, err := host.savepoint(rt, params)
		if err != nil {
			t.Fatalf("savepoint failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		if host.txSavepoint != 1 {
			t.Errorf("Expected savepoint 1, got %d", host.txSavepoint)
		}

		t.Log("Savepoint created successfully")
	})

	t.Run("rollback_to_savepoint", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Begin transaction and create savepoint
		host.beginTransaction(rt, []uint64{})
		host.savepoint(rt, []uint64{1})

		// Rollback to savepoint
		params := []uint64{1} // savepointId = 1
		result, err := host.rollbackToSavepoint(rt, params)
		if err != nil {
			t.Fatalf("rollbackToSavepoint failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		t.Log("Rolled back to savepoint successfully")
	})

	t.Run("nested_transaction", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Begin outer transaction
		host.beginTransaction(rt, []uint64{})

		// Try to begin another transaction (should fail)
		result, _ := host.beginTransaction(rt, []uint64{})
		if result[0] != 0 {
			t.Error("Expected nested beginTransaction to fail")
		}

		t.Log("Nested transaction correctly rejected")
	})

	t.Run("transaction_commit_without_begin", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Try to commit without beginning transaction
		result, _ := host.commitTransaction(rt, []uint64{})
		if result[0] != 0 {
			t.Error("Expected commit without begin to fail")
		}

		t.Log("Commit without begin correctly rejected")
	})
}
