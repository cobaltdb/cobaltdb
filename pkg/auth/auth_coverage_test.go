package auth

import (
	"strings"
	"testing"
	"time"
)

// TestStopAuthenticator covers the Stop method (0% → 100%).
func TestStopAuthenticator(t *testing.T) {
	a := NewAuthenticator()

	// First stop should succeed
	a.Stop()

	// Second stop should not panic (idempotent)
	a.Stop()

	// After stop, the authenticator should still be usable for non-background ops
	err := a.CreateUser("afterstop", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser after stop failed: %v", err)
	}
}

// TestSetPasswordPolicy covers SetPasswordPolicy (0% → 100%).
func TestSetPasswordPolicy(t *testing.T) {
	a := NewAuthenticator()

	// Enable password policy
	a.SetPasswordPolicy(true)

	// Weak password should be rejected
	err := a.CreateUser("user1", "short", false)
	if err == nil {
		t.Fatal("Expected error for short password with policy enabled")
	}

	// No uppercase
	err = a.CreateUser("user2", "alllowercase1", false)
	if err == nil {
		t.Fatal("Expected error for password without uppercase")
	}

	// No lowercase
	err = a.CreateUser("user3", "ALLUPPERCASE1", false)
	if err == nil {
		t.Fatal("Expected error for password without lowercase")
	}

	// No digit
	err = a.CreateUser("user4", "NoDigitsHere", false)
	if err == nil {
		t.Fatal("Expected error for password without digit")
	}

	// Valid password
	err = a.CreateUser("user5", "ValidPass1", false)
	if err != nil {
		t.Fatalf("Expected success for valid password: %v", err)
	}

	// Disable policy and try weak password
	a.SetPasswordPolicy(false)
	err = a.CreateUser("user6", "weak", false)
	if err != nil {
		t.Fatalf("Expected success with policy disabled: %v", err)
	}
}

// TestValidatePasswordStrength covers validatePasswordStrength (0% → 100%).
func TestValidatePasswordStrength(t *testing.T) {
	tests := []struct {
		name     string
		password string
		wantErr  bool
		errMsg   string
	}{
		{"TooShort", "Ab1", true, "at least 8 characters"},
		{"NoUpper", "lowercase1abc", true, "uppercase"},
		{"NoLower", "UPPERCASE1ABC", true, "lowercase"},
		{"NoDigit", "NoDigitHere", true, "digit"},
		{"Valid", "GoodPass1", false, ""},
		{"MinLength", "Abcdefg1", false, ""},
		{"Empty", "", true, "at least 8 characters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePasswordStrength(tt.password)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// TestValidateCredentials covers ValidateCredentials (0% → 100%).
func TestValidateCredentials(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("creduser", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	t.Run("ValidCredentials", func(t *testing.T) {
		err := a.ValidateCredentials("creduser", "Password1")
		if err != nil {
			t.Fatalf("Expected nil, got %v", err)
		}
	})

	t.Run("WrongPassword", func(t *testing.T) {
		err := a.ValidateCredentials("creduser", "WrongPass1")
		if err != ErrInvalidCredentials {
			t.Fatalf("Expected ErrInvalidCredentials, got %v", err)
		}
	})

	t.Run("NonExistentUser", func(t *testing.T) {
		err := a.ValidateCredentials("noone", "anything")
		if err != ErrInvalidCredentials {
			t.Fatalf("Expected ErrInvalidCredentials, got %v", err)
		}
	})
}

// TestGetMySQLNativeHash covers GetMySQLNativeHash (0% → 100%).
func TestGetMySQLNativeHash(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("hashuser", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	t.Run("ExistingUser", func(t *testing.T) {
		hash, err := a.GetMySQLNativeHash("hashuser")
		if err != nil {
			t.Fatalf("Expected nil error, got %v", err)
		}
		if len(hash) == 0 {
			t.Fatal("Expected non-empty hash")
		}
		// SHA1(SHA1(password)) should be 20 bytes
		if len(hash) != 20 {
			t.Fatalf("Expected 20-byte hash, got %d bytes", len(hash))
		}
	})

	t.Run("NonExistentUser", func(t *testing.T) {
		_, err := a.GetMySQLNativeHash("noone")
		if err != ErrUserNotFound {
			t.Fatalf("Expected ErrUserNotFound, got %v", err)
		}
	})
}

// TestUserExists covers UserExists (0% → 100%).
func TestUserExists(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("existsuser", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	if !a.UserExists("existsuser") {
		t.Fatal("Expected user to exist")
	}

	if a.UserExists("ghost") {
		t.Fatal("Expected user not to exist")
	}
}

// TestStartSessionCleanup covers StartSessionCleanup (0% → 100%).
func TestStartSessionCleanup(t *testing.T) {
	a := NewAuthenticator()
	// Stop the default cleanup goroutine first
	a.Stop()

	err := a.CreateUser("cleanuser", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Manually add an expired session
	a.mu.Lock()
	a.sessions["expired_cleanup"] = &Session{
		Token:     "expired_cleanup",
		Username:  "cleanuser",
		CreatedAt: time.Now().Add(-48 * time.Hour),
		ExpiresAt: time.Now().Add(-24 * time.Hour),
	}
	a.mu.Unlock()

	stopCh := make(chan struct{})
	a.StartSessionCleanup(50*time.Millisecond, stopCh)

	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)

	// Expired session should be cleaned
	a.mu.RLock()
	_, found := a.sessions["expired_cleanup"]
	a.mu.RUnlock()
	if found {
		t.Fatal("Expired session should have been cleaned up")
	}

	// Stop the cleanup goroutine
	close(stopCh)
	time.Sleep(100 * time.Millisecond) // let goroutine exit
}

// TestBruteForceProtection covers lockout path in Authenticate and recordFailedAttempt (88% → 100%).
func TestBruteForceProtection(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("bruteuser", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Fail maxLoginAttempts times to trigger lockout
	for i := 0; i < maxLoginAttempts; i++ {
		_, err := a.Authenticate("bruteuser", "wrong")
		if err != ErrInvalidCredentials {
			t.Fatalf("Attempt %d: expected ErrInvalidCredentials, got %v", i+1, err)
		}
	}

	// Next attempt should be locked out
	_, err = a.Authenticate("bruteuser", "Password1")
	if err == nil {
		t.Fatal("Expected lockout error")
	}
	if !strings.Contains(err.Error(), "temporarily locked") {
		t.Fatalf("Expected lockout message, got: %v", err)
	}

	// Also test lockout for non-existent user (hits recordFailedAttempt for unknown user)
	for i := 0; i < maxLoginAttempts; i++ {
		a.Authenticate("phantom", "whatever")
	}
	_, err = a.Authenticate("phantom", "whatever")
	if err == nil || !strings.Contains(err.Error(), "temporarily locked") {
		t.Fatalf("Expected lockout for phantom user, got: %v", err)
	}
}

// TestBruteForceResetOnSuccess covers clearing failed attempts on successful auth.
func TestBruteForceResetOnSuccess(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("resetuser", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Fail a few times (but less than max)
	for i := 0; i < maxLoginAttempts-1; i++ {
		a.Authenticate("resetuser", "wrong")
	}

	// Succeed - should clear attempts
	_, err = a.Authenticate("resetuser", "Password1")
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	// Fail maxLoginAttempts-1 more times - should NOT lock (counter was reset)
	for i := 0; i < maxLoginAttempts-1; i++ {
		_, err = a.Authenticate("resetuser", "wrong")
		if err != ErrInvalidCredentials {
			t.Fatalf("Expected ErrInvalidCredentials, got %v", err)
		}
	}

	// One more success should still work
	_, err = a.Authenticate("resetuser", "Password1")
	if err != nil {
		t.Fatalf("Expected success after reset, got %v", err)
	}
}

// TestExpiredTokenValidation covers the expired token branch in ValidateToken (87.5% → 100%).
func TestExpiredTokenValidation(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("expuser", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	token, err := a.Authenticate("expuser", "Password1")
	if err != nil {
		t.Fatalf("Authenticate failed: %v", err)
	}

	// Manually expire the session
	a.mu.Lock()
	if s, ok := a.sessions[token]; ok {
		s.ExpiresAt = time.Now().Add(-1 * time.Second)
	}
	a.mu.Unlock()

	// Should return ErrTokenExpired
	_, err = a.ValidateToken(token)
	if err != ErrTokenExpired {
		t.Fatalf("Expected ErrTokenExpired, got %v", err)
	}
}

// TestCleanupStaleFailedAttempts covers the failed-attempt cleanup in CleanupExpiredSessions (81.8% → 100%).
func TestCleanupStaleFailedAttempts(t *testing.T) {
	a := NewAuthenticator()

	// Add a stale failed login attempt record (older than attemptResetAfter)
	a.failedMu.Lock()
	a.failedAttempts["staleuser"] = &loginAttempt{
		count:    3,
		lastFail: time.Now().Add(-attemptResetAfter - time.Minute),
	}
	// Add a fresh failed attempt record (should NOT be cleaned)
	a.failedAttempts["freshuser"] = &loginAttempt{
		count:    2,
		lastFail: time.Now(),
	}
	a.failedMu.Unlock()

	a.CleanupExpiredSessions()

	a.failedMu.RLock()
	_, hasStale := a.failedAttempts["staleuser"]
	_, hasFresh := a.failedAttempts["freshuser"]
	a.failedMu.RUnlock()

	if hasStale {
		t.Fatal("Stale failed attempt should have been cleaned up")
	}
	if !hasFresh {
		t.Fatal("Fresh failed attempt should NOT have been cleaned up")
	}
}

// TestSessionCleanupLoopStop covers the stop branch of sessionCleanupLoop (66.7% → 100%).
func TestSessionCleanupLoopStop(t *testing.T) {
	a := NewAuthenticator()
	// The NewAuthenticator started a goroutine; stop it
	a.Stop()

	// After stop, the goroutine should have exited (no panic, no leak)
	// Give it a moment to finish
	time.Sleep(50 * time.Millisecond)
}

// TestChangePasswordInvalidatesSessions covers session invalidation on password change.
func TestChangePasswordInvalidatesSessions(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("cptest", "OldPass1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Create sessions
	tok1, _ := a.Authenticate("cptest", "OldPass1")
	tok2, _ := a.Authenticate("cptest", "OldPass1")

	// Verify sessions valid
	if _, err := a.ValidateToken(tok1); err != nil {
		t.Fatalf("Token1 should be valid: %v", err)
	}
	if _, err := a.ValidateToken(tok2); err != nil {
		t.Fatalf("Token2 should be valid: %v", err)
	}

	// Change password
	err = a.ChangePassword("cptest", "OldPass1", "NewPass1")
	if err != nil {
		t.Fatalf("ChangePassword failed: %v", err)
	}

	// All old sessions should be invalidated
	if _, err := a.ValidateToken(tok1); err != ErrInvalidToken {
		t.Fatalf("Token1 should be invalid after password change, got %v", err)
	}
	if _, err := a.ValidateToken(tok2); err != ErrInvalidToken {
		t.Fatalf("Token2 should be invalid after password change, got %v", err)
	}

	// New password should work
	_, err = a.Authenticate("cptest", "NewPass1")
	if err != nil {
		t.Fatalf("New password should work: %v", err)
	}
}

// TestRevokeAllActions covers revoking all actions from a permission (removes the entry entirely).
func TestRevokeAllActions(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("revokeall", "Password1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// Grant SELECT and INSERT
	a.GrantPermission("revokeall", "db", "tbl", []string{"SELECT", "INSERT"})

	// Revoke both actions
	err = a.RevokePermission("revokeall", "db", "tbl", []string{"SELECT", "INSERT"})
	if err != nil {
		t.Fatalf("RevokePermission failed: %v", err)
	}

	// Permission entry should be removed entirely
	if a.HasPermission("revokeall", "db", "tbl", "SELECT") {
		t.Fatal("SELECT should be revoked")
	}
	if a.HasPermission("revokeall", "db", "tbl", "INSERT") {
		t.Fatal("INSERT should be revoked")
	}
}

// TestMySQLNativeHashConsistency verifies the MySQL native hash is deterministic.
func TestMySQLNativeHashConsistency(t *testing.T) {
	h1 := mysqlNativeHash("test_password")
	h2 := mysqlNativeHash("test_password")
	if len(h1) != 20 || len(h2) != 20 {
		t.Fatal("Expected 20-byte SHA1 hash")
	}
	for i := range h1 {
		if h1[i] != h2[i] {
			t.Fatal("Same password should produce same hash")
		}
	}

	// Different passwords should produce different hashes
	h3 := mysqlNativeHash("different_password")
	same := true
	for i := range h1 {
		if h1[i] != h3[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("Different passwords should produce different hashes")
	}
}

// TestCreateUserWithPasswordPolicy covers createUserLocked password policy branch.
func TestCreateUserWithPasswordPolicy(t *testing.T) {
	a := NewAuthenticator()
	a.SetPasswordPolicy(true)

	// Duplicate user with policy enabled
	err := a.CreateUser("policyuser", "ValidPass1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	err = a.CreateUser("policyuser", "ValidPass1", false)
	if err != ErrUserExists {
		t.Fatalf("Expected ErrUserExists, got %v", err)
	}

	// Admin user with valid password
	err = a.CreateUser("policyadmin", "AdminPass1", true)
	if err != nil {
		t.Fatalf("CreateUser admin failed: %v", err)
	}

	u, _ := a.GetUser("policyadmin")
	if !u.IsAdmin {
		t.Fatal("Expected admin user")
	}
}

// TestMySQLNativeHashUpdatedOnPasswordChange verifies the MySQL hash updates when password changes.
func TestMySQLNativeHashUpdatedOnPasswordChange(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("hashchange", "OldPass1", false)
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	hashBefore, _ := a.GetMySQLNativeHash("hashchange")

	err = a.ChangePassword("hashchange", "OldPass1", "NewPass1")
	if err != nil {
		t.Fatalf("ChangePassword failed: %v", err)
	}

	hashAfter, _ := a.GetMySQLNativeHash("hashchange")

	if len(hashBefore) != 20 || len(hashAfter) != 20 {
		t.Fatal("Expected 20-byte hashes")
	}

	same := true
	for i := range hashBefore {
		if hashBefore[i] != hashAfter[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("MySQL native hash should change when password changes")
	}
}
