package auth

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCreateUser(t *testing.T) {
	auth := NewAuthenticator()

	// Create a user
	err := auth.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Try to create duplicate user
	err = auth.CreateUser("testuser", "password456", false)
	if err != ErrUserExists {
		t.Errorf("Expected ErrUserExists, got %v", err)
	}
}

func TestAuthenticate(t *testing.T) {
	auth := NewAuthenticator()

	// Create a user
	err := auth.CreateUser("testuser", "password123", false)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Authenticate with correct credentials
	token, err := auth.Authenticate("testuser", "password123")
	if err != nil {
		t.Fatalf("Failed to authenticate: %v", err)
	}
	if token == "" {
		t.Error("Expected non-empty token")
	}

	// Authenticate with wrong password
	_, err = auth.Authenticate("testuser", "wrongpassword")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials, got %v", err)
	}

	// Authenticate with non-existent user
	_, err = auth.Authenticate("nonexistent", "password123")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials, got %v", err)
	}
}

func TestFailedAuthenticateDoesNotHoldAuthLockDuringDelay(t *testing.T) {
	auth := NewAuthenticator()
	defer auth.Stop()

	authDone := make(chan error, 1)
	go func() {
		_, err := auth.Authenticate("missing-user", "password123")
		if err != ErrInvalidCredentials {
			authDone <- fmt.Errorf("expected ErrInvalidCredentials, got %v", err)
			return
		}
		authDone <- nil
	}()

	time.Sleep(25 * time.Millisecond)
	lockCheckDone := make(chan struct{}, 1)
	go func() {
		_ = auth.IsEnabled()
		lockCheckDone <- struct{}{}
	}()

	select {
	case <-lockCheckDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("failed authentication delay should not hold authenticator lock")
	}

	if err := <-authDone; err != nil {
		t.Fatal(err)
	}
}

func TestCreateUserDoesNotHoldAuthLockDuringHash(t *testing.T) {
	auth := NewAuthenticator()
	defer auth.Stop()

	hashStarted, releaseHash := blockPasswordHasher(t)

	createDone := make(chan error, 1)
	go func() {
		createDone <- auth.CreateUser("hashcreate", "password123", false)
	}()

	<-hashStarted
	lockCheckDone := make(chan struct{}, 1)
	go func() {
		_ = auth.IsEnabled()
		lockCheckDone <- struct{}{}
	}()

	select {
	case <-lockCheckDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("user creation hashing should not hold authenticator lock")
	}

	close(releaseHash)
	if err := <-createDone; err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}
}

func TestValidateCredentialsDoesNotHoldAuthLockDuringHash(t *testing.T) {
	auth := NewAuthenticator()
	defer auth.Stop()

	if err := auth.CreateUser("hashvalidate", "password123", false); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	hashStarted, releaseHash := blockPasswordHasher(t)

	validateDone := make(chan error, 1)
	go func() {
		validateDone <- auth.ValidateCredentials("hashvalidate", "password123")
	}()

	<-hashStarted
	lockCheckDone := make(chan struct{}, 1)
	go func() {
		_ = auth.IsEnabled()
		lockCheckDone <- struct{}{}
	}()

	select {
	case <-lockCheckDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("credential validation hashing should not hold authenticator lock")
	}

	close(releaseHash)
	if err := <-validateDone; err != nil {
		t.Fatalf("ValidateCredentials failed: %v", err)
	}
}

func blockPasswordHasher(t *testing.T) (<-chan struct{}, chan<- struct{}) {
	t.Helper()

	originalHasher := passwordHasher
	hashStarted := make(chan struct{})
	releaseHash := make(chan struct{})
	var once sync.Once

	passwordHasher = func(password, salt string) string {
		once.Do(func() {
			close(hashStarted)
		})
		<-releaseHash
		return originalHasher(password, salt)
	}
	t.Cleanup(func() {
		passwordHasher = originalHasher
	})

	return hashStarted, releaseHash
}

func TestValidateToken(t *testing.T) {
	auth := NewAuthenticator()

	// Create a user and authenticate
	auth.CreateUser("testuser", "password123", false)
	token, _ := auth.Authenticate("testuser", "password123")

	// Validate token
	session, err := auth.ValidateToken(token)
	if err != nil {
		t.Fatalf("Failed to validate token: %v", err)
	}
	if session.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %s", session.Username)
	}
	if session.Token != token {
		t.Fatal("ValidateToken should return the client token without storing it internally")
	}

	session.ExpiresAt = time.Now().Add(-time.Hour)
	sessionAgain, err := auth.ValidateToken(token)
	if err != nil {
		t.Fatalf("Token should remain valid after mutating returned session: %v", err)
	}
	if sessionAgain.ExpiresAt.Before(time.Now()) {
		t.Fatal("ValidateToken returned mutable internal session")
	}

	// Validate invalid token
	_, err = auth.ValidateToken("invalidtoken")
	if err != ErrInvalidToken {
		t.Errorf("Expected ErrInvalidToken, got %v", err)
	}
}

func TestAuthenticateStoresOnlySessionTokenDigest(t *testing.T) {
	auth := NewAuthenticator()

	if err := auth.CreateUser("digestuser", "password123", false); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	token, err := auth.Authenticate("digestuser", "password123")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}

	auth.mu.RLock()
	_, rawKeyExists := auth.sessions[token]
	session, digestKeyExists := auth.sessions[sessionTokenKey(token)]
	auth.mu.RUnlock()

	if rawKeyExists {
		t.Fatal("raw session token should not be used as the sessions map key")
	}
	if !digestKeyExists {
		t.Fatal("session should be stored under the session token digest")
	}
	if session.Token != "" {
		t.Fatal("raw session token should not be stored in the internal session")
	}

	validated, err := auth.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if validated.Token != token {
		t.Fatal("validated session should expose the client token for API compatibility")
	}
}

func TestLogout(t *testing.T) {
	auth := NewAuthenticator()

	// Create a user and authenticate
	auth.CreateUser("testuser", "password123", false)
	token, _ := auth.Authenticate("testuser", "password123")

	// Validate token (should work)
	_, err := auth.ValidateToken(token)
	if err != nil {
		t.Fatalf("Token should be valid: %v", err)
	}

	// Logout
	auth.Logout(token)

	// Validate token again (should fail)
	_, err = auth.ValidateToken(token)
	if err != ErrInvalidToken {
		t.Errorf("Expected ErrInvalidToken after logout, got %v", err)
	}
}

func TestChangePassword(t *testing.T) {
	auth := NewAuthenticator()

	// Create a user
	auth.CreateUser("testuser", "oldpassword", false)

	// Change password with wrong old password
	err := auth.ChangePassword("testuser", "wrongpassword", "newpassword")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials, got %v", err)
	}

	// Change password with correct old password
	err = auth.ChangePassword("testuser", "oldpassword", "newpassword")
	if err != nil {
		t.Fatalf("Failed to change password: %v", err)
	}

	// Authenticate with new password
	_, err = auth.Authenticate("testuser", "newpassword")
	if err != nil {
		t.Errorf("Failed to authenticate with new password: %v", err)
	}

	// Authenticate with old password (should fail)
	_, err = auth.Authenticate("testuser", "oldpassword")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials for old password, got %v", err)
	}
}

func TestDeleteUser(t *testing.T) {
	auth := NewAuthenticator()

	// Create a user
	auth.CreateUser("testuser", "password123", false)

	// Delete non-existent user
	err := auth.DeleteUser("nonexistent")
	if err != ErrUserNotFound {
		t.Errorf("Expected ErrUserNotFound, got %v", err)
	}

	// Delete user
	err = auth.DeleteUser("testuser")
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// Try to authenticate as deleted user
	_, err = auth.Authenticate("testuser", "password123")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials for deleted user, got %v", err)
	}
}

func TestAdminPermissions(t *testing.T) {
	auth := NewAuthenticator()

	// Create admin user
	auth.CreateUser("admin", "adminpass", true)

	// Create regular user
	auth.CreateUser("user", "userpass", false)

	// Admin should have all permissions
	if !auth.HasPermission("admin", "db1", "table1", "SELECT") {
		t.Error("Admin should have SELECT permission")
	}
	if !auth.HasPermission("admin", "db1", "table1", "INSERT") {
		t.Error("Admin should have INSERT permission")
	}
	if !auth.HasPermission("admin", "anydb", "anytable", "DELETE") {
		t.Error("Admin should have DELETE permission on any table")
	}

	// Regular user should not have permissions by default
	if auth.HasPermission("user", "db1", "table1", "SELECT") {
		t.Error("Regular user should not have SELECT permission by default")
	}
}

func TestGrantRevokePermissions(t *testing.T) {
	auth := NewAuthenticator()

	// Create user
	auth.CreateUser("testuser", "password123", false)

	// Grant SELECT permission
	err := auth.GrantPermission("testuser", "mydb", "mytable", []string{"SELECT"})
	if err != nil {
		t.Fatalf("Failed to grant permission: %v", err)
	}

	// Check permission
	if !auth.HasPermission("testuser", "mydb", "mytable", "SELECT") {
		t.Error("User should have SELECT permission")
	}
	if auth.HasPermission("testuser", "mydb", "mytable", "INSERT") {
		t.Error("User should not have INSERT permission")
	}

	// Grant INSERT permission
	auth.GrantPermission("testuser", "mydb", "mytable", []string{"INSERT"})

	// Check both permissions
	if !auth.HasPermission("testuser", "mydb", "mytable", "SELECT") {
		t.Error("User should have SELECT permission")
	}
	if !auth.HasPermission("testuser", "mydb", "mytable", "INSERT") {
		t.Error("User should have INSERT permission")
	}

	// Revoke SELECT permission
	err = auth.RevokePermission("testuser", "mydb", "mytable", []string{"SELECT"})
	if err != nil {
		t.Fatalf("Failed to revoke permission: %v", err)
	}

	// Check permission again
	if auth.HasPermission("testuser", "mydb", "mytable", "SELECT") {
		t.Error("User should not have SELECT permission after revoke")
	}
	if !auth.HasPermission("testuser", "mydb", "mytable", "INSERT") {
		t.Error("User should still have INSERT permission")
	}
}

func TestListUsers(t *testing.T) {
	auth := NewAuthenticator()

	// Create users
	auth.CreateUser("user1", "pass1", false)
	auth.CreateUser("user2", "pass2", false)
	auth.CreateUser("user3", "pass3", false)

	// List users
	users := auth.ListUsers()
	if len(users) != 3 {
		t.Errorf("Expected 3 users, got %d", len(users))
	}

	// Check that all users are in the list
	userMap := make(map[string]bool)
	for _, u := range users {
		userMap[u] = true
	}
	if !userMap["user1"] || !userMap["user2"] || !userMap["user3"] {
		t.Error("Not all users found in list")
	}
}

func TestCleanupExpiredSessions(t *testing.T) {
	auth := NewAuthenticator()

	// Create a user and authenticate
	auth.CreateUser("testuser", "password123", false)
	token, _ := auth.Authenticate("testuser", "password123")

	// Manually expire the session
	auth.mu.Lock()
	if session, exists := auth.sessions[sessionTokenKey(token)]; exists {
		session.ExpiresAt = time.Now().Add(-1 * time.Hour) // Expired 1 hour ago
	}
	auth.mu.Unlock()

	// Cleanup expired sessions
	auth.CleanupExpiredSessions()

	// Token should now be invalid
	_, err := auth.ValidateToken(token)
	if err != ErrInvalidToken {
		t.Errorf("Expected ErrInvalidToken after cleanup, got %v", err)
	}
}

func TestEnableDisable(t *testing.T) {
	auth := NewAuthenticator()

	// Authentication should be disabled by default
	if auth.IsEnabled() {
		t.Error("Authentication should be disabled by default")
	}

	// Enable authentication
	auth.Enable()
	if !auth.IsEnabled() {
		t.Error("Authentication should be enabled after Enable()")
	}

	// Disable authentication
	auth.Disable()
	if auth.IsEnabled() {
		t.Error("Authentication should be disabled after Disable()")
	}
}
