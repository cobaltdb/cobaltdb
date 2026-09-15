package auth

import (
	"crypto/sha1" // #nosec G505 -- mysql_native_password is specified in terms of SHA-1
	"testing"
)

// mysqlNativeArtifacts derives the server-stored hash SHA1(SHA1(password)) and
// the client response SHA1(password) XOR SHA1(scramble + storedHash) for the
// mysql_native_password handshake.
func mysqlNativeArtifacts(password string, scramble []byte) (storedHash, response []byte) {
	// #nosec G401 -- protocol-mandated SHA-1, not a security-sensitive digest
	stage1 := sha1.Sum([]byte(password))
	// #nosec G401 -- protocol-mandated SHA-1
	stage2 := sha1.Sum(stage1[:])
	storedHash = stage2[:]

	// #nosec G401 -- protocol-mandated SHA-1
	h := sha1.New()
	_, _ = h.Write(scramble)
	_, _ = h.Write(storedHash)
	scrambled := h.Sum(nil)

	response = make([]byte, sha1.Size)
	for i := range scrambled {
		response[i] = stage1[i] ^ scrambled[i]
	}
	return storedHash, response
}

// TestVerifyMySQLNativeChallenge exercises the mysql_native_password challenge
// verifier directly, including the malformed-input cases that must be rejected
// before any comparison happens.
func TestVerifyMySQLNativeChallenge(t *testing.T) {
	scramble := []byte("12345678901234567890") // exactly 20 bytes
	storedHash, validResponse := mysqlNativeArtifacts("mysecret", scramble)

	wrongResponse := make([]byte, sha1.Size)
	for i := range wrongResponse {
		wrongResponse[i] = 0xff
	}

	otherHash, _ := mysqlNativeArtifacts("different", scramble)

	tests := []struct {
		name       string
		storedHash []byte
		scramble   []byte
		response   []byte
		want       bool
	}{
		{"valid handshake", storedHash, scramble, validResponse, true},
		{"wrong response", storedHash, scramble, wrongResponse, false},
		{"response for another password", otherHash, scramble, validResponse, false},
		{"nil stored hash", nil, scramble, validResponse, false},
		{"empty stored hash", []byte{}, scramble, validResponse, false},
		{"short stored hash", storedHash[:10], scramble, validResponse, false},
		{"nil response", storedHash, scramble, nil, false},
		{"empty response", storedHash, scramble, []byte{}, false},
		{"short response", storedHash, scramble, validResponse[:10], false},
		{"short scramble", storedHash, scramble[:10], validResponse, false},
		{"nil scramble", storedHash, nil, validResponse, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verifyMySQLNativeChallenge(tt.storedHash, tt.scramble, tt.response); got != tt.want {
				t.Errorf("verifyMySQLNativeChallenge(%s) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

// TestVerifyMySQLNativeChallengeRejectsEmptyCredentials pins the deliberate
// hardening choice that an empty stored hash never authenticates. The MySQL
// protocol allows a passwordless account to send an empty auth response; this
// server rejects that instead of granting access.
func TestVerifyMySQLNativeChallengeRejectsEmptyCredentials(t *testing.T) {
	scramble := make([]byte, sha1.Size)
	if verifyMySQLNativeChallenge(nil, scramble, nil) {
		t.Error("empty stored hash with empty response must not authenticate")
	}
	if verifyMySQLNativeChallenge([]byte{}, scramble, []byte{}) {
		t.Error("empty stored hash with empty response must not authenticate")
	}
}

// TestVerifyMySQLNativeChallengeIsScrambleBound confirms the response is tied
// to the specific scramble, so a captured response cannot be replayed against
// a later connection that issues a different nonce.
func TestVerifyMySQLNativeChallengeIsScrambleBound(t *testing.T) {
	scrambleA := []byte("aaaaaaaaaaaaaaaaaaaa")
	scrambleB := []byte("bbbbbbbbbbbbbbbbbbbb")

	storedHash, responseA := mysqlNativeArtifacts("mysecret", scrambleA)

	if !verifyMySQLNativeChallenge(storedHash, scrambleA, responseA) {
		t.Fatal("response must verify against the scramble it was derived from")
	}
	if verifyMySQLNativeChallenge(storedHash, scrambleB, responseA) {
		t.Error("response replayed against a different scramble must be rejected")
	}
}
