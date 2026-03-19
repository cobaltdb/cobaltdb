package wasm

import (
	"encoding/binary"
	"testing"
)

// TestVectorizedExecution tests SIMD-style vectorized operations
func TestVectorizedExecution(t *testing.T) {
	t.Run("vectorized_add", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Setup input arrays
		inPtr1 := int32(1024)
		inPtr2 := int32(2048)
		outPtr := int32(3072)
		count := 4

		// Fill input arrays: [10, 20, 30, 40] + [1, 2, 3, 4] = [11, 22, 33, 44]
		inputs1 := []int64{10, 20, 30, 40}
		inputs2 := []int64{1, 2, 3, 4}
		for i, v := range inputs1 {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(v))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(inputs2[i]))
		}

		// Call vectorizedAdd
		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count)}
		result, err := host.vectorizedAdd(rt, params)
		if err != nil {
			t.Fatalf("vectorizedAdd failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Verify results
		expected := []int64{11, 22, 33, 44}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("Element %d: expected %d, got %d", i, exp, val)
			}
		}

		t.Logf("Vectorized add: %v + %v = %v", inputs1, inputs2, expected)
	})

	t.Run("vectorized_multiply", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		inPtr1 := int32(1024)
		inPtr2 := int32(2048)
		outPtr := int32(3072)
		count := 4

		// [5, 10, 15, 20] * [2, 3, 4, 5] = [10, 30, 60, 100]
		inputs1 := []int64{5, 10, 15, 20}
		inputs2 := []int64{2, 3, 4, 5}
		for i, v := range inputs1 {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(v))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(inputs2[i]))
		}

		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count)}
		result, err := host.vectorizedMultiply(rt, params)
		if err != nil {
			t.Fatalf("vectorizedMultiply failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		expected := []int64{10, 30, 60, 100}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("Element %d: expected %d, got %d", i, exp, val)
			}
		}

		t.Logf("Vectorized multiply result: %v", expected)
	})

	t.Run("vectorized_compare_equal", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		inPtr1 := int32(1024)
		inPtr2 := int32(2048)
		outPtr := int32(3072)
		count := 4

		// [1, 2, 3, 4] == [1, 2, 0, 4] = [1, 1, 0, 1]
		inputs1 := []int64{1, 2, 3, 4}
		inputs2 := []int64{1, 2, 0, 4}
		for i, v := range inputs1 {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(v))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(inputs2[i]))
		}

		// op=0 for equality
		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count), 0}
		result, err := host.vectorizedCompare(rt, params)
		if err != nil {
			t.Fatalf("vectorizedCompare failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		expected := []int64{1, 1, 0, 1}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("Element %d: expected %d, got %d", i, exp, val)
			}
		}

		t.Logf("Vectorized compare (eq) result: %v", expected)
	})

	t.Run("vectorized_compare_less_than", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		inPtr1 := int32(1024)
		inPtr2 := int32(2048)
		outPtr := int32(3072)
		count := 4

		// [1, 5, 10, 3] < [2, 5, 8, 3] = [1, 0, 0, 0]
		inputs1 := []int64{1, 5, 10, 3}
		inputs2 := []int64{2, 5, 8, 3}
		for i, v := range inputs1 {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(v))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(inputs2[i]))
		}

		// op=2 for less than
		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count), 2}
		result, err := host.vectorizedCompare(rt, params)
		if err != nil {
			t.Fatalf("vectorizedCompare failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		expected := []int64{1, 0, 0, 0}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("Element %d: expected %d, got %d", i, exp, val)
			}
		}

		t.Logf("Vectorized compare (lt) result: %v", expected)
	})

	t.Run("vectorized_sum", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		inPtr := int32(1024)
		count := 5

		// Sum of [10, 20, 30, 40, 50] = 150
		inputs := []int64{10, 20, 30, 40, 50}
		for i, v := range inputs {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr+int32(i*8):], uint64(v))
		}

		params := []uint64{uint64(inPtr), uint64(count)}
		result, err := host.vectorizedSum(rt, params)
		if err != nil {
			t.Fatalf("vectorizedSum failed: %v", err)
		}

		expected := int64(150)
		if int64(result[0]) != expected {
			t.Errorf("Expected sum %d, got %d", expected, result[0])
		}

		t.Logf("Vectorized sum: %v = %d", inputs, result[0])
	})

	t.Run("vectorized_minmax", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		inPtr := int32(1024)
		outMinPtr := int32(2048)
		outMaxPtr := int32(2056)
		count := 5

		// Min/Max of [42, 17, 99, 3, 56] = min: 3, max: 99
		inputs := []int64{42, 17, 99, 3, 56}
		for i, v := range inputs {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr+int32(i*8):], uint64(v))
		}

		params := []uint64{uint64(inPtr), uint64(count), uint64(outMinPtr), uint64(outMaxPtr)}
		result, err := host.vectorizedMinMax(rt, params)
		if err != nil {
			t.Fatalf("vectorizedMinMax failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		minVal := int64(binary.LittleEndian.Uint64(rt.Memory[outMinPtr:]))
		maxVal := int64(binary.LittleEndian.Uint64(rt.Memory[outMaxPtr:]))

		if minVal != 3 {
			t.Errorf("Expected min 3, got %d", minVal)
		}
		if maxVal != 99 {
			t.Errorf("Expected max 99, got %d", maxVal)
		}

		t.Logf("Vectorized min/max: min=%d, max=%d", minVal, maxVal)
	})

	t.Run("vectorized_filter", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		inPtr := int32(1024)
		maskPtr := int32(2048)
		outPtr := int32(3072)
		count := 5

		// Input: [10, 20, 30, 40, 50]
		// Mask:  [1,  0,  1,  0,  1] (keep elements where mask != 0)
		// Output: [10, 30, 50] (3 elements)
		inputs := []int64{10, 20, 30, 40, 50}
		masks := []int64{1, 0, 1, 0, 1}
		for i, v := range inputs {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr+int32(i*8):], uint64(v))
			binary.LittleEndian.PutUint64(rt.Memory[maskPtr+int32(i*8):], uint64(masks[i]))
		}

		params := []uint64{uint64(inPtr), uint64(maskPtr), uint64(outPtr), uint64(count)}
		result, err := host.vectorizedFilter(rt, params)
		if err != nil {
			t.Fatalf("vectorizedFilter failed: %v", err)
		}

		if result[0] != 3 {
			t.Errorf("Expected 3 filtered elements, got %d", result[0])
		}

		// Verify filtered output
		expected := []int64{10, 30, 50}
		for i, exp := range expected {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("Element %d: expected %d, got %d", i, exp, val)
			}
		}

		t.Logf("Vectorized filter: kept %d elements - %v", result[0], expected)
	})

	t.Run("vectorized_batch_copy", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		srcPtr := int32(1024)
		dstPtr := int32(2048)
		count := 4

		// Source: [100, 200, 300, 400]
		source := []int64{100, 200, 300, 400}
		for i, v := range source {
			binary.LittleEndian.PutUint64(rt.Memory[srcPtr+int32(i*8):], uint64(v))
		}

		params := []uint64{uint64(srcPtr), uint64(dstPtr), uint64(count)}
		result, err := host.vectorizedBatchCopy(rt, params)
		if err != nil {
			t.Fatalf("vectorizedBatchCopy failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Verify copied data
		for i, exp := range source {
			val := int64(binary.LittleEndian.Uint64(rt.Memory[dstPtr+int32(i*8):]))
			if val != exp {
				t.Errorf("Element %d: expected %d, got %d", i, exp, val)
			}
		}

		t.Logf("Vectorized batch copy: copied %d elements", count)
	})

	t.Run("vectorized_large_array", func(t *testing.T) {
		rt := NewRuntime(50) // Larger memory for big arrays
		host := NewHostFunctions()
		host.RegisterAll(rt)

		inPtr1 := int32(1024)
		inPtr2 := int32(1024 + 8*1000)
		outPtr := int32(1024 + 16*1000)
		count := 1000

		// Initialize arrays
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint64(rt.Memory[inPtr1+int32(i*8):], uint64(i))
			binary.LittleEndian.PutUint64(rt.Memory[inPtr2+int32(i*8):], uint64(i*2))
		}

		// Perform vectorized add
		params := []uint64{uint64(inPtr1), uint64(inPtr2), uint64(outPtr), uint64(count)}
		result, err := host.vectorizedAdd(rt, params)
		if err != nil {
			t.Fatalf("vectorizedAdd failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Verify first and last elements
		first := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr:]))
		last := int64(binary.LittleEndian.Uint64(rt.Memory[outPtr+int32((count-1)*8):]))

		if first != 0 {
			t.Errorf("First element: expected 0, got %d", first)
		}
		expectedLast := int64((count - 1) * 3) // (999) + (999*2) = 999 * 3
		if last != expectedLast {
			t.Errorf("Last element: expected %d, got %d", expectedLast, last)
		}

		t.Logf("Vectorized large array: processed %d elements, first=%d, last=%d", count, first, last)
	})

	t.Run("vectorized_invalid_count", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Test with invalid count (0)
		params := []uint64{1024, 2048, 3072, 0}
		result, _ := host.vectorizedAdd(rt, params)
		if result[0] != 0 {
			t.Errorf("Expected failure (0) for count=0, got %d", result[0])
		}

		// Test with too large count
		params = []uint64{1024, 2048, 3072, 20000}
		result, _ = host.vectorizedAdd(rt, params)
		if result[0] != 0 {
			t.Errorf("Expected failure (0) for large count, got %d", result[0])
		}

		t.Log("Invalid counts correctly rejected")
	})

	t.Run("vectorized_simd_workflow", func(t *testing.T) {
		rt := NewRuntime(20)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Simulate a SIMD workflow:
		// 1. Have two arrays of prices and quantities
		// 2. Multiply them to get values (vectorizedMultiply)
		// 3. Sum all values (vectorizedSum)

		pricePtr := int32(1024)
		qtyPtr := int32(2048)
		valuePtr := int32(3072)
		count := 5

		// Prices: [10, 20, 15, 25, 30]
		// Qty:    [2,  3,  4,  2,  5]
		// Values: [20, 60, 60, 50, 150] -> Sum = 340
		prices := []int64{10, 20, 15, 25, 30}
		quantities := []int64{2, 3, 4, 2, 5}

		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint64(rt.Memory[pricePtr+int32(i*8):], uint64(prices[i]))
			binary.LittleEndian.PutUint64(rt.Memory[qtyPtr+int32(i*8):], uint64(quantities[i]))
		}

		// Step 1: Multiply prices * quantities
		params := []uint64{uint64(pricePtr), uint64(qtyPtr), uint64(valuePtr), uint64(count)}
		result, _ := host.vectorizedMultiply(rt, params)
		if result[0] != 1 {
			t.Fatal("vectorizedMultiply failed")
		}

		// Step 2: Sum all values
		params = []uint64{uint64(valuePtr), uint64(count)}
		sumResult, _ := host.vectorizedSum(rt, params)

		expectedTotal := int64(340)
		if int64(sumResult[0]) != expectedTotal {
			t.Errorf("Expected total %d, got %d", expectedTotal, sumResult[0])
		}

		t.Logf("SIMD workflow: total value = %d", sumResult[0])
	})
}
