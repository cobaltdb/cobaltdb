package main

import (
	"strings"
	"testing"
)

// testCharset mirrors the production charset declared inside
// generateRandomPassword (it is a function-local constant there).
const testCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"

// TestGenerateRandomPasswordUniform pins that generateRandomPassword samples
// the charset uniformly.
//
// Regression: the function mapped raw crypto/rand bytes through
// `charset[int(b[i])%len(charset)]` without rejection sampling. The charset is
// 70 characters and 256 % 70 = 46, so the first 46 charset positions (lowercase
// a-z, uppercase A-T) were drawn with probability 4/256 while the remaining 24
// (U-Z, digits, symbols) were drawn with probability 3/256 — a systematic
// bias (CWE-195) in the function that generates the admin bootstrap
// credential. At 60,000 samples the 4-mapping group's minimum count exceeds
// the 3-mapping group's maximum count by ~9σ, so the bias is deterministically
// observable; uniform (rejection-sampled) output interleaves the buckets.
func TestGenerateRandomPasswordUniform(t *testing.T) {
	const samples = 60000
	counts := make([]int, len(testCharset))
	for i := 0; i < samples; i++ {
		pw, err := generateRandomPassword(20)
		if err != nil {
			t.Fatalf("generateRandomPassword: %v", err)
		}
		if len(pw) != 20 {
			t.Fatalf("password length = %d, want 20", len(pw))
		}
		idx := strings.IndexByte(testCharset, pw[0])
		if idx < 0 {
			t.Fatalf("password starts with non-charset character %q", pw[0])
		}
		counts[idx]++
	}

	// Under the modulo mapping the first (256 % len(charset)) positions are
	// the over-drawn group; under uniform sampling no grouping exists.
	biasGroup := 256 % len(testCharset)

	minBias, maxBias := 1<<30, 0
	for i := 0; i < biasGroup; i++ {
		if counts[i] < minBias {
			minBias = counts[i]
		}
		if counts[i] > maxBias {
			maxBias = counts[i]
		}
	}
	minRest, maxRest := 1<<30, 0
	for i := biasGroup; i < len(testCharset); i++ {
		if counts[i] < minRest {
			minRest = counts[i]
		}
		if counts[i] > maxRest {
			maxRest = counts[i]
		}
	}

	// Uniform sampling: both groups' ranges interleave (the biased mapping
	// makes every over-drawn count exceed every under-drawn count).
	if minBias > maxRest {
		t.Fatalf("non-uniform charset sampling: over-drawn group min %d > under-drawn group max %d (biasGroup=%d chars, samples=%d)", minBias, maxRest, biasGroup, samples)
	}
}
