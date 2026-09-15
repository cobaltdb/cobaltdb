package auth

import (
	"crypto/sha1" // #nosec G505 -- test implements mysql_native_password compatibility.
	"errors"
	"strings"
	"testing"
)

func mysqlChallengeResponseForTest(password string, scramble []byte) []byte {
	// #nosec G401 -- MySQL native password protocol compatibility test.
	stage1 := sha1.Sum([]byte(password))
	stored := mysqlNativeHash(password)
	// #nosec G401 -- MySQL native password protocol compatibility test.
	h := sha1.New()
	_, _ = h.Write(scramble)
	_, _ = h.Write(stored)
	mask := h.Sum(nil)
	response := make([]byte, sha1.Size)
	for i := range response {
		response[i] = stage1[i] ^ mask[i]
	}
	return response
}

func TestVerifyMySQLNativeChallengeSharesLockoutPolicy(t *testing.T) {
	a := NewAuthenticator()
	defer a.Stop()
	if err := a.CreateUser("mysql-user", "StrongPass1", false); err != nil {
		t.Fatal(err)
	}
	scramble := []byte("01234567890123456789")
	valid := mysqlChallengeResponseForTest("StrongPass1", scramble)
	invalid := make([]byte, sha1.Size)

	// A success before the threshold clears accumulated failures.
	for i := 0; i < maxLoginAttempts-1; i++ {
		if err := a.VerifyMySQLNativeChallenge("mysql-user", scramble, invalid); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("failure %d error = %v", i+1, err)
		}
	}
	if err := a.VerifyMySQLNativeChallenge("mysql-user", scramble, valid); err != nil {
		t.Fatalf("valid challenge before lockout: %v", err)
	}

	for i := 0; i < maxLoginAttempts; i++ {
		if err := a.VerifyMySQLNativeChallenge("mysql-user", scramble, invalid); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("lockout failure %d error = %v", i+1, err)
		}
	}
	if err := a.VerifyMySQLNativeChallenge("mysql-user", scramble, valid); err == nil || !strings.Contains(err.Error(), "temporarily locked") {
		t.Fatalf("valid challenge bypassed lockout: %v", err)
	}
}

func TestVerifyMySQLNativeChallengeTracksUnknownUsers(t *testing.T) {
	a := NewAuthenticator()
	defer a.Stop()
	scramble := []byte("01234567890123456789")
	invalid := make([]byte, sha1.Size)
	for i := 0; i < maxLoginAttempts; i++ {
		if err := a.VerifyMySQLNativeChallenge("missing", scramble, invalid); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("unknown-user failure %d error = %v", i+1, err)
		}
	}
	if err := a.VerifyMySQLNativeChallenge("missing", scramble, invalid); err == nil || !strings.Contains(err.Error(), "temporarily locked") {
		t.Fatalf("unknown user was not locked out: %v", err)
	}
}
