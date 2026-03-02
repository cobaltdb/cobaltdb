package auth

import (
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

	// Validate invalid token
	_, err = auth.ValidateToken("invalidtoken")
	if err != ErrInvalidToken {
		t.Errorf("Expected ErrInvalidToken, got %v", err)
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
	if session, exists := auth.sessions[token]; exists {
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
