package protocol

// Test-only MySQL protocol helpers. These were moved out of mysql.go (where
// they carried //nolint:unused markers) because they have no production
// callers — they implement the *client* side of the mysql_native_password
// exchange and length-encoded string writing used by protocol tests.

import (
	"crypto/sha1" // #nosec G505 -- MySQL native password protocol requires SHA-1 compatibility.
)

// scramblePassword scrambles a password using MySQL's algorithm (client side).
func scramblePassword(password, scramble []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// SHA1(password)
	// #nosec G401 -- MySQL native password protocol requires SHA-1 compatibility.
	h1 := sha1.New()
	h1.Write(password)
	hash1 := h1.Sum(nil)

	// SHA1(SHA1(password))
	// #nosec G401 -- MySQL native password protocol requires SHA-1 compatibility.
	h2 := sha1.New()
	h2.Write(hash1)
	hash2 := h2.Sum(nil)

	// SHA1(scramble + SHA1(SHA1(password)))
	// #nosec G401 -- MySQL native password protocol requires SHA-1 compatibility.
	h3 := sha1.New()
	h3.Write(scramble)
	h3.Write(hash2)
	hash3 := h3.Sum(nil)

	// XOR
	result := make([]byte, len(hash3))
	for i := range hash3 {
		result[i] = hash1[i] ^ hash3[i]
	}

	return result
}

// writeLenEncString returns a newly allocated length-encoded string.
// Prefer appendLenEncString for zero-allocation appending.
func writeLenEncString(s string) []byte {
	return appendLenEncString(nil, s)
}
