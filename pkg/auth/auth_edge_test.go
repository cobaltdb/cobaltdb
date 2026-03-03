package auth

import (
	"strings"
	"testing"
	"time"
)

func TestGetUser(t *testing.T) {
	a := NewAuthenticator()

	t.Run("GetExistingUser", func(t *testing.T) {
		err := a.CreateUser("testuser", "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}

		user, err := a.GetUser("testuser")
		if err != nil {
			t.Fatalf("Failed to get user: %v", err)
		}

		if user.Username != "testuser" {
			t.Errorf("Expected username 'testuser', got '%s'", user.Username)
		}

		if user.IsAdmin {
			t.Error("Expected non-admin user")
		}
	})

	t.Run("GetNonExistentUser", func(t *testing.T) {
		_, err := a.GetUser("nonexistent")
		if err != ErrUserNotFound {
			t.Errorf("Expected ErrUserNotFound, got %v", err)
		}
	})

	t.Run("GetUserReturnsCopy", func(t *testing.T) {
		err := a.CreateUser("copytest", "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}

		user1, _ := a.GetUser("copytest")
		user2, _ := a.GetUser("copytest")

		// Modify user1
		user1.IsAdmin = true

		// user2 should not be affected
		if user2.IsAdmin {
			t.Error("GetUser did not return a copy - modification affected other reference")
		}
	})
}

func TestAuthEdgeCases(t *testing.T) {
	a := NewAuthenticator()

	t.Run("EmptyUsername", func(t *testing.T) {
		err := a.CreateUser("", "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user with empty username: %v", err)
		}

		_, err = a.Authenticate("", "password123")
		if err != nil {
			t.Errorf("Failed to authenticate user with empty username: %v", err)
		}
	})

	t.Run("EmptyPassword", func(t *testing.T) {
		err := a.CreateUser("emptypass", "", false)
		if err != nil {
			t.Fatalf("Failed to create user with empty password: %v", err)
		}

		_, err = a.Authenticate("emptypass", "")
		if err != nil {
			t.Errorf("Failed to authenticate with empty password: %v", err)
		}
	})

	t.Run("LongUsername", func(t *testing.T) {
		longUsername := strings.Repeat("a", 1000)
		err := a.CreateUser(longUsername, "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user with long username: %v", err)
		}

		_, err = a.Authenticate(longUsername, "password123")
		if err != nil {
			t.Errorf("Failed to authenticate with long username: %v", err)
		}
	})

	t.Run("LongPassword", func(t *testing.T) {
		longPassword := strings.Repeat("b", 1000)
		err := a.CreateUser("longpass", longPassword, false)
		if err != nil {
			t.Fatalf("Failed to create user with long password: %v", err)
		}

		_, err = a.Authenticate("longpass", longPassword)
		if err != nil {
			t.Errorf("Failed to authenticate with long password: %v", err)
		}
	})

	t.Run("UnicodeUsername", func(t *testing.T) {
		unicodeUsername := "用户_测试_🎉"
		err := a.CreateUser(unicodeUsername, "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user with unicode username: %v", err)
		}

		_, err = a.Authenticate(unicodeUsername, "password123")
		if err != nil {
			t.Errorf("Failed to authenticate with unicode username: %v", err)
		}
	})

	t.Run("SpecialCharactersInPassword", func(t *testing.T) {
		specialPassword := "!@#$%^&*()_+-=[]{}|;':\",./<>?"
		err := a.CreateUser("specialpass", specialPassword, false)
		if err != nil {
			t.Fatalf("Failed to create user with special password: %v", err)
		}

		_, err = a.Authenticate("specialpass", specialPassword)
		if err != nil {
			t.Errorf("Failed to authenticate with special password: %v", err)
		}
	})

	t.Run("CaseSensitiveUsername", func(t *testing.T) {
		err := a.CreateUser("CaseSensitive", "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}

		_, err = a.Authenticate("casesensitive", "password123")
		if err != ErrInvalidCredentials {
			t.Errorf("Expected ErrInvalidCredentials for wrong case, got %v", err)
		}
	})

	t.Run("WhitespaceInUsername", func(t *testing.T) {
		// Username with leading/trailing whitespace
		err := a.CreateUser("  whitespace  ", "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user with whitespace: %v", err)
		}

		// Should not match without whitespace
		_, err = a.Authenticate("whitespace", "password123")
		if err != ErrInvalidCredentials {
			t.Errorf("Expected ErrInvalidCredentials for username without whitespace, got %v", err)
		}

		// Should match with exact whitespace
		_, err = a.Authenticate("  whitespace  ", "password123")
		if err != nil {
			t.Errorf("Failed to authenticate with exact whitespace: %v", err)
		}
	})
}

func TestSessionEdgeCases(t *testing.T) {
	a := NewAuthenticator()
	a.Enable()

	err := a.CreateUser("sessiontest", "password123", false)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	t.Run("EmptyToken", func(t *testing.T) {
		_, err := a.ValidateToken("")
		if err != ErrInvalidToken {
			t.Errorf("Expected ErrInvalidToken for empty token, got %v", err)
		}
	})

	t.Run("InvalidToken", func(t *testing.T) {
		_, err := a.ValidateToken("invalidtoken123")
		if err != ErrInvalidToken {
			t.Errorf("Expected ErrInvalidToken for invalid token, got %v", err)
		}
	})

	t.Run("TokenWithSpecialCharacters", func(t *testing.T) {
		_, err := a.ValidateToken("!@#$%^&*()")
		if err != ErrInvalidToken {
			t.Errorf("Expected ErrInvalidToken for special char token, got %v", err)
		}
	})

	t.Run("LogoutEmptyToken", func(t *testing.T) {
		// Should not panic
		a.Logout("")
	})

	t.Run("LogoutInvalidToken", func(t *testing.T) {
		// Should not panic
		a.Logout("nonexistenttoken")
	})

	t.Run("MultipleLogins", func(t *testing.T) {
		token1, err := a.Authenticate("sessiontest", "password123")
		if err != nil {
			t.Fatalf("First login failed: %v", err)
		}

		// Small delay to ensure different token generation time
		time.Sleep(1 * time.Millisecond)

		token2, err := a.Authenticate("sessiontest", "password123")
		if err != nil {
			t.Fatalf("Second login failed: %v", err)
		}

		// Both tokens should be valid
		_, err = a.ValidateToken(token1)
		if err != nil {
			t.Errorf("First token invalid: %v", err)
		}

		_, err = a.ValidateToken(token2)
		if err != nil {
			t.Errorf("Second token invalid: %v", err)
		}

		// Logout one, other should still work
		a.Logout(token1)

		_, err = a.ValidateToken(token1)
		if err != ErrInvalidToken {
			t.Errorf("Expected first token to be invalid after logout")
		}

		_, err = a.ValidateToken(token2)
		if err != nil {
			t.Errorf("Second token should still be valid: %v", err)
		}
	})
}

func TestPermissionEdgeCases(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("permtest", "password123", false)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	t.Run("EmptyPermissionActions", func(t *testing.T) {
		err := a.GrantPermission("permtest", "db1", "table1", []string{})
		if err != nil {
			t.Fatalf("Failed to grant empty permissions: %v", err)
		}

		hasPerm := a.HasPermission("permtest", "db1", "table1", "SELECT")
		if hasPerm {
			t.Error("Should not have permission with empty actions list")
		}
	})

	t.Run("WildcardPermission", func(t *testing.T) {
		err := a.GrantPermission("permtest", "db2", "table2", []string{"*"})
		if err != nil {
			t.Fatalf("Failed to grant wildcard permission: %v", err)
		}

		// Should have any permission
		if !a.HasPermission("permtest", "db2", "table2", "SELECT") {
			t.Error("Should have SELECT with wildcard")
		}
		if !a.HasPermission("permtest", "db2", "table2", "INSERT") {
			t.Error("Should have INSERT with wildcard")
		}
		if !a.HasPermission("permtest", "db2", "table2", "DELETE") {
			t.Error("Should have DELETE with wildcard")
		}
	})

	t.Run("DatabaseWildcardPermission", func(t *testing.T) {
		err := a.GrantPermission("permtest", "", "table3", []string{"SELECT"})
		if err != nil {
			t.Fatalf("Failed to grant permission: %v", err)
		}

		// Empty database means any database
		if !a.HasPermission("permtest", "anydb", "table3", "SELECT") {
			t.Error("Should have permission with any database when database is empty")
		}
	})

	t.Run("TableWildcardPermission", func(t *testing.T) {
		err := a.GrantPermission("permtest", "db3", "", []string{"SELECT"})
		if err != nil {
			t.Fatalf("Failed to grant permission: %v", err)
		}

		// Empty table means any table
		if !a.HasPermission("permtest", "db3", "anytable", "SELECT") {
			t.Error("Should have permission with any table when table is empty")
		}
	})

	t.Run("NonExistentUserPermission", func(t *testing.T) {
		err := a.GrantPermission("nonexistent", "db", "table", []string{"SELECT"})
		if err != ErrUserNotFound {
			t.Errorf("Expected ErrUserNotFound, got %v", err)
		}
	})

	t.Run("NonExistentUserHasPermission", func(t *testing.T) {
		hasPerm := a.HasPermission("nonexistent", "db", "table", "SELECT")
		if hasPerm {
			t.Error("Non-existent user should not have any permissions")
		}
	})

	t.Run("RevokeNonExistentPermission", func(t *testing.T) {
		// Should not error when revoking from non-existent permission
		err := a.RevokePermission("permtest", "nonexistent", "table", []string{"SELECT"})
		if err != nil {
			t.Errorf("Revoke from non-existent permission should not error: %v", err)
		}
	})

	t.Run("RevokeNonExistentUser", func(t *testing.T) {
		err := a.RevokePermission("nonexistent", "db", "table", []string{"SELECT"})
		if err != ErrUserNotFound {
			t.Errorf("Expected ErrUserNotFound, got %v", err)
		}
	})

	t.Run("DuplicatePermissionGrant", func(t *testing.T) {
		// Grant same permission twice
		err := a.GrantPermission("permtest", "db4", "table4", []string{"SELECT"})
		if err != nil {
			t.Fatalf("First grant failed: %v", err)
		}

		err = a.GrantPermission("permtest", "db4", "table4", []string{"INSERT"})
		if err != nil {
			t.Fatalf("Second grant failed: %v", err)
		}

		// Should have both permissions
		if !a.HasPermission("permtest", "db4", "table4", "SELECT") {
			t.Error("Should have SELECT permission")
		}
		if !a.HasPermission("permtest", "db4", "table4", "INSERT") {
			t.Error("Should have INSERT permission")
		}
	})
}

func TestChangePasswordEdgeCases(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("passtest", "oldpassword", false)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	t.Run("WrongOldPassword", func(t *testing.T) {
		err := a.ChangePassword("passtest", "wrongpassword", "newpassword")
		if err != ErrInvalidCredentials {
			t.Errorf("Expected ErrInvalidCredentials, got %v", err)
		}
	})

	t.Run("NonExistentUserChangePassword", func(t *testing.T) {
		err := a.ChangePassword("nonexistent", "old", "new")
		if err != ErrUserNotFound {
			t.Errorf("Expected ErrUserNotFound, got %v", err)
		}
	})

	t.Run("SamePassword", func(t *testing.T) {
		err := a.ChangePassword("passtest", "oldpassword", "oldpassword")
		if err != nil {
			t.Fatalf("Failed to change to same password: %v", err)
		}

		// Should still be able to authenticate
		_, err = a.Authenticate("passtest", "oldpassword")
		if err != nil {
			t.Errorf("Failed to authenticate after setting same password: %v", err)
		}
	})

	t.Run("ChangePasswordInvalidatesOld", func(t *testing.T) {
		err := a.ChangePassword("passtest", "oldpassword", "newpassword123")
		if err != nil {
			t.Fatalf("Failed to change password: %v", err)
		}

		// Old password should not work
		_, err = a.Authenticate("passtest", "oldpassword")
		if err != ErrInvalidCredentials {
			t.Error("Old password should not work after change")
		}

		// New password should work
		_, err = a.Authenticate("passtest", "newpassword123")
		if err != nil {
			t.Errorf("New password should work: %v", err)
		}
	})
}

func TestDeleteUserEdgeCases(t *testing.T) {
	a := NewAuthenticator()

	t.Run("DeleteNonExistentUser", func(t *testing.T) {
		err := a.DeleteUser("nonexistent")
		if err != ErrUserNotFound {
			t.Errorf("Expected ErrUserNotFound, got %v", err)
		}
	})

	t.Run("DeleteUserInvalidatesSessions", func(t *testing.T) {
		err := a.CreateUser("deletetest", "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}

		token, err := a.Authenticate("deletetest", "password123")
		if err != nil {
			t.Fatalf("Failed to authenticate: %v", err)
		}

		// Verify session is valid
		_, err = a.ValidateToken(token)
		if err != nil {
			t.Fatalf("Session should be valid: %v", err)
		}

		// Delete user
		err = a.DeleteUser("deletetest")
		if err != nil {
			t.Fatalf("Failed to delete user: %v", err)
		}

		// Session should be invalid
		_, err = a.ValidateToken(token)
		if err != ErrInvalidToken {
			t.Error("Session should be invalid after user deletion")
		}
	})

	t.Run("DeleteUserMultipleSessions", func(t *testing.T) {
		err := a.CreateUser("multisession", "password123", false)
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}

		// Create multiple sessions
		token1, _ := a.Authenticate("multisession", "password123")
		token2, _ := a.Authenticate("multisession", "password123")
		token3, _ := a.Authenticate("multisession", "password123")

		// Delete user
		err = a.DeleteUser("multisession")
		if err != nil {
			t.Fatalf("Failed to delete user: %v", err)
		}

		// All sessions should be invalid
		for _, token := range []string{token1, token2, token3} {
			_, err = a.ValidateToken(token)
			if err != ErrInvalidToken {
				t.Errorf("Session %s should be invalid after user deletion", token[:8])
			}
		}
	})
}

func TestCleanupExpiredSessionsEdgeCases(t *testing.T) {
	a := NewAuthenticator()

	err := a.CreateUser("expirytest", "password123", false)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	t.Run("CleanupNoSessions", func(t *testing.T) {
		// Should not panic with no sessions
		a.CleanupExpiredSessions()
	})

	t.Run("CleanupMixedSessions", func(t *testing.T) {
		// Manually create sessions with different expiry times
		a.mu.Lock()
		a.sessions["expired1"] = &Session{
			Token:     "expired1",
			Username:  "expirytest",
			CreatedAt: time.Now().Add(-48 * time.Hour),
			ExpiresAt: time.Now().Add(-24 * time.Hour), // Expired
		}
		a.sessions["expired2"] = &Session{
			Token:     "expired2",
			Username:  "expirytest",
			CreatedAt: time.Now().Add(-48 * time.Hour),
			ExpiresAt: time.Now().Add(-1 * time.Second), // Just expired
		}
		a.sessions["valid"] = &Session{
			Token:     "valid",
			Username:  "expirytest",
			CreatedAt: time.Now(),
			ExpiresAt: time.Now().Add(24 * time.Hour), // Valid
		}
		a.mu.Unlock()

		// Cleanup
		a.CleanupExpiredSessions()

		// Check results
		a.mu.RLock()
		_, hasExpired1 := a.sessions["expired1"]
		_, hasExpired2 := a.sessions["expired2"]
		_, hasValid := a.sessions["valid"]
		a.mu.RUnlock()

		if hasExpired1 {
			t.Error("Expired session 1 should be removed")
		}
		if hasExpired2 {
			t.Error("Expired session 2 should be removed")
		}
		if !hasValid {
			t.Error("Valid session should remain")
		}
	})
}

func TestListUsersEdgeCases(t *testing.T) {
	a := NewAuthenticator()

	t.Run("EmptyUserList", func(t *testing.T) {
		users := a.ListUsers()
		if len(users) != 0 {
			t.Errorf("Expected empty list, got %d users", len(users))
		}
	})

	t.Run("ManyUsers", func(t *testing.T) {
		// Create many users
		for i := 0; i < 100; i++ {
			err := a.CreateUser(string(rune('a'+i%26))+string(rune(i)), "password", false)
			if err != nil {
				t.Fatalf("Failed to create user %d: %v", i, err)
			}
		}

		users := a.ListUsers()
		if len(users) != 100 {
			t.Errorf("Expected 100 users, got %d", len(users))
		}

		// Check all users are unique
		userMap := make(map[string]bool)
		for _, u := range users {
			if userMap[u] {
				t.Errorf("Duplicate user: %s", u)
			}
			userMap[u] = true
		}
	})
}

func TestEnableDisableEdgeCases(t *testing.T) {
	a := NewAuthenticator()

	t.Run("MultipleEnables", func(t *testing.T) {
		a.Enable()
		a.Enable()
		a.Enable()

		if !a.IsEnabled() {
			t.Error("Should be enabled after multiple enables")
		}
	})

	t.Run("MultipleDisables", func(t *testing.T) {
		a.Disable()
		a.Disable()
		a.Disable()

		if a.IsEnabled() {
			t.Error("Should be disabled after multiple disables")
		}
	})

	t.Run("Toggle", func(t *testing.T) {
		a.Enable()
		if !a.IsEnabled() {
			t.Error("Should be enabled")
		}

		a.Disable()
		if a.IsEnabled() {
			t.Error("Should be disabled")
		}

		a.Enable()
		if !a.IsEnabled() {
			t.Error("Should be enabled again")
		}
	})
}
