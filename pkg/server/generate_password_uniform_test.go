package server

import (
	"strings"
	"testing"
)

// serverTestCharset mirrors the production charset declared inside
// generateRandomPassword (pkg/server copy — 62 alphanumeric characters).
const serverTestCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// TestServerGenerateRandomPasswordUniform pins that the pkg/server
// generateRandomPassword — the generator behind DefaultConfig's
// DefaultAdminPass (the default admin credential) — samples its charset
// uniformly.
//
// Regression: the function mapped raw crypto/rand bytes through
// `charset[uint32(b[i])%uint32(len(charset))]` without rejection sampling.
// The charset is 62 characters and 256%62 = 8, so the first 8 positions
// ('a'-'h') were drawn with probability 5/256 while the remaining 54 drew at
// 4/256 — the same CWE-195 modulo bias e0e2deb removed from the
// cmd/cobaltdb-server generator, still live in this copy. At 60,000 samples
// the over-drawn group's minimum count exceeds the under-drawn group's
// maximum by ~9σ, so the bias is deterministically observable; uniform
// (rejection-sampled) output interleaves the buckets.
func TestServerGenerateRandomPasswordUniform(t *testing.T) {
	const samples = 60000
	counts := make([]int, len(serverTestCharset))
	for i := 0; i < samples; i++ {
		pw, err := generateRandomPassword()
		if err != nil {
			t.Fatalf("generateRandomPassword: %v", err)
		}
		if len(pw) != 16 {
			t.Fatalf("password length = %d, want 16", len(pw))
		}
		idx := strings.IndexByte(serverTestCharset, pw[0])
		if idx < 0 {
			t.Fatalf("password starts with non-charset character %q", pw[0])
		}
		counts[idx]++
	}

	// Under the modulo mapping the first (256 % len(charset)) positions are
	// the over-drawn group; under uniform sampling no grouping exists.
	biasGroup := 256 % len(serverTestCharset)

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
	for i := biasGroup; i < len(serverTestCharset); i++ {
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
		t.Fatalf("non-uniform charset sampling: over-drawn group ('a'-'h') min %d > under-drawn group max %d (biasGroup=%d chars, samples=%d) — raw bytes are still mapped through %%62 (CWE-195)", minBias, maxRest, biasGroup, samples)
	}
}
