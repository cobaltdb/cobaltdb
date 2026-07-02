package server

import "crypto/subtle"

// Test-only helpers. These have no production callers and live in a _test.go
// file so the lint gate's unused-code check stays clean.

func adminTokenEqual(provided, expected string) bool {
	providedDigest := adminTokenDigest(provided)
	expectedDigest := adminTokenDigest(expected)
	return subtle.ConstantTimeCompare(providedDigest[:], expectedDigest[:]) == 1
}
