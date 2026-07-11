package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestGenerateSaltReportsRandFailure(t *testing.T) {
	original := authRandRead
	t.Cleanup(func() { authRandRead = original })

	authRandRead = func(b []byte) (int, error) { return 0, errors.New("mock rand failure") }

	if _, err := generateSalt(); err == nil || !strings.Contains(err.Error(), "mock rand failure") {
		t.Fatalf("generateSalt error = %v, want 'mock rand failure'", err)
	}
}

func TestGenerateTokenReportsRandFailure(t *testing.T) {
	original := authRandRead
	t.Cleanup(func() { authRandRead = original })

	authRandRead = func(b []byte) (int, error) { return 0, errors.New("mock rand failure") }

	if _, err := generateToken("user"); err == nil || !strings.Contains(err.Error(), "mock rand failure") {
		t.Fatalf("generateToken error = %v, want 'mock rand failure'", err)
	}
}

func TestCloneFunctionsHandleNil(t *testing.T) {
	if got := cloneUser(nil); got != nil {
		t.Fatalf("cloneUser(nil) = %#v, want nil", got)
	}
	if got := cloneSession(nil); got != nil {
		t.Fatalf("cloneSession(nil) = %#v, want nil", got)
	}
	if got := clonePermissions(nil); got != nil {
		t.Fatalf("clonePermissions(nil) = %#v, want nil", got)
	}
	if got := cloneStringSlice(nil); got != nil {
		t.Fatalf("cloneStringSlice(nil) = %#v, want nil", got)
	}
	if got := cloneBytes(nil); got != nil {
		t.Fatalf("cloneBytes(nil) = %#v, want nil", got)
	}
}

func TestCreateUserReportsSaltGenerationError(t *testing.T) {
	original := authRandRead
	t.Cleanup(func() { authRandRead = original })
	authRandRead = func(b []byte) (int, error) { return 0, errors.New("salt fail") }

	a := NewAuthenticator()
	if err := a.CreateUser("newuser", "Password1", false); err == nil || !strings.Contains(err.Error(), "salt fail") {
		t.Fatalf("CreateUser salt error = %v", err)
	}
}

func TestCreateUserRaceCheckReportsDuplicate(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("raceuser", "Password1", false); err != nil {
		t.Fatal(err)
	}

	a.mu.Lock()
	a.users["raceuser2"] = a.users["raceuser"]
	a.mu.Unlock()

	if err := a.CreateUser("raceuser2", "Password1", false); err != ErrUserExists {
		t.Fatalf("CreateUser race double-check = %v, want ErrUserExists", err)
	}
}

func TestCreateUserRechecksPolicyUnderWriteLock(t *testing.T) {
	a := NewAuthenticator()
	a.SetPasswordPolicy(true)

	if err := a.CreateUser("policyrecheck", "ValidPass1", false); err != nil {
		t.Fatalf("CreateUser with policy=on failed: %v", err)
	}

	// Create where policy was off at read-lock but on at write-lock
	// (hard to race deterministically, so just verify the write-lock path by
	// first creating then re-creating a user that hits the write-lock policy check).
	a.mu.Lock()
	a.enforcePasswordPolicy = true
	delete(a.users, "policyrecheck")
	a.mu.Unlock()

	a.SetPasswordPolicy(true)
	if err := a.CreateUser("policyrecheck", "short", false); err == nil {
		t.Fatal("CreateUser should reject short password under write-lock policy check")
	}
}

func TestAuthenticateRejectsInvalidCredentialFormat(t *testing.T) {
	a := NewAuthenticator()
	if _, err := a.Authenticate(strings.Repeat("a", maxUsernameBytes+1), "x"); err != ErrInvalidCredentials {
		t.Fatalf("oversized username auth = %v, want ErrInvalidCredentials", err)
	}
}

func TestAuthenticateReportsDecoySaltUsageForUnknownUser(t *testing.T) {
	original := passwordHasher
	t.Cleanup(func() { passwordHasher = original })

	var capturedSalt string
	passwordHasher = func(password, salt string) string {
		capturedSalt = salt
		return ""
	}

	a := NewAuthenticator()
	if _, err := a.Authenticate("ghost", "x"); err != ErrInvalidCredentials {
		t.Fatalf("ghost user auth = %v", err)
	}
	if capturedSalt != authDecoySalt {
		t.Fatalf("decoy salt = %q, want %q", capturedSalt, authDecoySalt)
	}
}

func TestAuthenticateRejectsLockedOutUser(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("lockoutuser", "Password1", false); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxLoginAttempts; i++ {
		a.Authenticate("lockoutuser", "wrong")
	}
	if _, err := a.Authenticate("lockoutuser", "Password1"); err == nil || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("locked user auth = %v, want locked error", err)
	}
}

func TestGrantPermissionValidatesInput(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("grantuser", "Password1", false); err != nil {
		t.Fatal(err)
	}
	tooLong := strings.Repeat("d", maxPermissionTargetBytes+1)
	if err := a.GrantPermission("grantuser", tooLong, "t", []string{"SELECT"}); err != ErrInvalidPermission {
		t.Fatalf("oversized database grant = %v", err)
	}
	if err := a.GrantPermission("grantuser", "d", tooLong, []string{"SELECT"}); err != ErrInvalidPermission {
		t.Fatalf("oversized table grant = %v", err)
	}
}

func TestRevokePermissionValidatesInput(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("revokeuser", "Password1", false); err != nil {
		t.Fatal(err)
	}
	if err := a.GrantPermission("revokeuser", "db", "tbl", []string{"SELECT"}); err != nil {
		t.Fatal(err)
	}
	tooLong := strings.Repeat("d", maxPermissionTargetBytes+1)
	if err := a.RevokePermission("revokeuser", tooLong, "t", []string{"SELECT"}); err != ErrInvalidPermission {
		t.Fatalf("oversized database revoke = %v", err)
	}
	if err := a.RevokePermission("revokeuser", "d", tooLong, []string{"SELECT"}); err != ErrInvalidPermission {
		t.Fatalf("oversized table revoke = %v", err)
	}
}

func TestChangePasswordRaceCheckFailsOnConcurrentChange(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("cpraceuser", "OldPass1", false); err != nil {
		t.Fatal(err)
	}

	// Modify the user under the lock so the hash+salt don't match expectations
	a.mu.Lock()
	a.users["cpraceuser"].PasswordHash = "changed"
	a.mu.Unlock()

	if err := a.ChangePassword("cpraceuser", "OldPass1", "NewPass1"); err != ErrInvalidCredentials {
		t.Fatalf("concurrent change detection = %v, want ErrInvalidCredentials", err)
	}
}

func TestChangePasswordReChecksPolicyUnderWriteLock(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("cppolicy", "ValidPass1", false); err != nil {
		t.Fatal(err)
	}

	a.mu.Lock()
	a.enforcePasswordPolicy = true
	a.mu.Unlock()

	if err := a.ChangePassword("cppolicy", "ValidPass1", "short"); err == nil {
		t.Fatal("ChangePassword should reject short password under write-lock policy check")
	}
}

func TestValidateCredentialsRejectsInvalidInput(t *testing.T) {
	a := NewAuthenticator()
	if err := a.ValidateCredentials("", "x"); err != ErrInvalidCredentials {
		t.Fatalf("empty username = %v", err)
	}
	if err := a.ValidateCredentials("x", ""); err != ErrInvalidCredentials {
		t.Fatalf("empty password = %v", err)
	}
}

func TestGrantPermissionRejectsInvalidUsername(t *testing.T) {
	a := NewAuthenticator()
	if err := a.GrantPermission("", "db", "tbl", []string{"SELECT"}); err == nil {
		t.Fatal("GrantPermission with empty username should fail")
	}
}

func TestRevokePermissionRejectsInvalidUsername(t *testing.T) {
	a := NewAuthenticator()
	if err := a.RevokePermission("", "db", "tbl", []string{"SELECT"}); err == nil {
		t.Fatal("RevokePermission with empty username should fail")
	}
}

func TestChangePasswordReportsInputErrors(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("cpinput", "Password1", false); err != nil {
		t.Fatal(err)
	}
	if err := a.ChangePassword("", "Password1", "NewPass1"); err == nil {
		t.Fatal("empty username should fail")
	}
	if err := a.ChangePassword("cpinput", "", "NewPass1"); err != ErrInvalidCredentials {
		t.Fatalf("empty oldPassword = %v, want ErrInvalidCredentials", err)
	}
	if err := a.ChangePassword("cpinput", "Password1", ""); err == nil {
		t.Fatal("empty newPassword should fail")
	}
}

func TestAuthenticateReportsTokenGenerationFailure(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("tokenfailuser", "Password1", false); err != nil {
		t.Fatal(err)
	}

	original := authRandRead
	t.Cleanup(func() { authRandRead = original })
	authRandRead = func(b []byte) (int, error) { return 0, errors.New("tokenfail") }

	if _, err := a.Authenticate("tokenfailuser", "Password1"); err == nil {
		t.Fatal("Authenticate should fail with rand.Read error")
	}
}

func TestChangePasswordReportsSaltGenerationFailure(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("saltfailuser", "Password1", false); err != nil {
		t.Fatal(err)
	}

	original := authRandRead
	t.Cleanup(func() { authRandRead = original })
	authRandRead = func(b []byte) (int, error) { return 0, errors.New("saltfail") }

	if err := a.ChangePassword("saltfailuser", "Password1", "NewPass1"); err == nil {
		t.Fatal("ChangePassword should fail with salt generation error")
	}
}

func TestCreateUserWriteLockDetectsRace(t *testing.T) {
	a := NewAuthenticator()
	defer a.Stop()

	hashStarted, releaseHash := blockPasswordHasher(t)

	createDone := make(chan error, 1)
	go func() {
		createDone <- a.CreateUser("condup", "Password1", false)
	}()
	<-hashStarted

	// While goroutine is hashing (between read and write locks), create the user directly
	a.mu.Lock()
	a.users["condup"] = &User{Username: "condup", PasswordHash: "h", Salt: "s"}
	a.mu.Unlock()

	close(releaseHash)

	if err := <-createDone; err != ErrUserExists {
		t.Fatalf("concurrent CreateUser = %v, want ErrUserExists", err)
	}
}

func TestCreateUserWriteLockEnforcesPolicy(t *testing.T) {
	a := NewAuthenticator()
	defer a.Stop()

	hashStarted, releaseHash := blockPasswordHasher(t)

	createDone := make(chan error, 1)
	go func() {
		createDone <- a.CreateUser("policyscale", "short", false)
	}()
	<-hashStarted

	// Enable policy while CreateUser is between read and write locks
	a.SetPasswordPolicy(true)

	close(releaseHash)

	if err := <-createDone; err == nil {
		t.Fatal("CreateUser should reject short password when policy was enabled concurrently")
	}
}

func TestChangePasswordWriteLockDetectsDeletion(t *testing.T) {
	a := NewAuthenticator()
	defer a.Stop()

	if err := a.CreateUser("condel", "Password1", false); err != nil {
		t.Fatal(err)
	}

	hashStarted, releaseHash := blockPasswordHasher(t)

	changeDone := make(chan error, 1)
	go func() {
		changeDone <- a.ChangePassword("condel", "Password1", "NewPass1")
	}()
	<-hashStarted

	// Delete user while goroutine is between locks
	a.mu.Lock()
	delete(a.users, "condel")
	a.mu.Unlock()

	close(releaseHash)

	if err := <-changeDone; err != ErrUserNotFound {
		t.Fatalf("ChangePassword after deletion = %v, want ErrUserNotFound", err)
	}
}

func TestChangePasswordWriteLockDetectsSaltChange(t *testing.T) {
	a := NewAuthenticator()
	defer a.Stop()

	if err := a.CreateUser("consalt", "Password1", false); err != nil {
		t.Fatal(err)
	}

	hashStarted, releaseHash := blockPasswordHasher(t)

	changeDone := make(chan error, 1)
	go func() {
		changeDone <- a.ChangePassword("consalt", "Password1", "NewPass1")
	}()
	<-hashStarted

	// Change user's salt+hash while goroutine is between locks
	a.mu.Lock()
	u := a.users["consalt"]
	u.Salt = "newSalt"
	u.PasswordHash = "newHash"
	a.mu.Unlock()

	close(releaseHash)

	if err := <-changeDone; err != ErrInvalidCredentials {
		t.Fatalf("ChangePassword after concurrent change = %v, want ErrInvalidCredentials", err)
	}
}

func TestChangePasswordWriteLockEnforcesEnabledPolicy(t *testing.T) {
	a := NewAuthenticator()
	defer a.Stop()

	if err := a.CreateUser("cppolicyrace", "ValidPass1", false); err != nil {
		t.Fatal(err)
	}

	hashStarted, releaseHash := blockPasswordHasher(t)

	changeDone := make(chan error, 1)
	go func() {
		changeDone <- a.ChangePassword("cppolicyrace", "ValidPass1", "short")
	}()
	<-hashStarted

	// Enable policy while goroutine is between read and write locks
	a.SetPasswordPolicy(true)

	close(releaseHash)

	if err := <-changeDone; err == nil {
		t.Fatal("ChangePassword should reject short password when policy was enabled concurrently")
	}
}

func TestAuthenticateWriteLockDetectsUserChange(t *testing.T) {
	a := NewAuthenticator()
	defer a.Stop()

	if err := a.CreateUser("conauth", "Password1", false); err != nil {
		t.Fatal(err)
	}

	hashStarted, releaseHash := blockPasswordHasher(t)

	authDone := make(chan error, 1)
	go func() {
		_, err := a.Authenticate("conauth", "Password1")
		authDone <- err
	}()
	<-hashStarted

	// Replace user with different password while goroutine is between locks
	a.mu.Lock()
	delete(a.users, "conauth")
	a.users["conauth"] = &User{
		Username:     "conauth",
		PasswordHash: "different_hash",
		Salt:         "salt2",
	}
	a.mu.Unlock()

	close(releaseHash)

	if err := <-authDone; err != ErrInvalidCredentials {
		t.Fatalf("Authenticate after concurrent user change = %v, want ErrInvalidCredentials", err)
	}
}

func TestDefaultSessionCleanupLoopTickerFires(t *testing.T) {
	original := sessionCleanupInterval
	sessionCleanupInterval = 10 * time.Millisecond
	t.Cleanup(func() { sessionCleanupInterval = original })

	a := NewAuthenticator()
	defer a.Stop()

	a.mu.Lock()
	a.sessions["defaultsess"] = &Session{
		Token:     "defaultsess",
		Username:  "anyone",
		CreatedAt: time.Now().Add(-time.Hour),
		ExpiresAt: time.Now().Add(-time.Minute),
	}
	a.mu.Unlock()

	time.Sleep(50 * time.Millisecond)

	a.mu.RLock()
	_, found := a.sessions["defaultsess"]
	a.mu.RUnlock()
	if found {
		t.Fatal("default sessionCleanupLoop should have removed expired session")
	}
}

func TestHasPermissionRejectsInvalidInput(t *testing.T) {
	a := NewAuthenticator()
	if err := a.CreateUser("hasperm", "Password1", false); err != nil {
		t.Fatal(err)
	}
	if err := a.GrantPermission("hasperm", "db", "tbl", []string{"SELECT"}); err != nil {
		t.Fatal(err)
	}
	tooLong := strings.Repeat("d", maxPermissionTargetBytes+1)
	if a.HasPermission("hasperm", tooLong, "t", "SELECT") {
		t.Fatal("oversized database match should be false")
	}
	if a.HasPermission("hasperm", "d", tooLong, "SELECT") {
		t.Fatal("oversized table match should be false")
	}
	if a.HasPermission("hasperm", "d", "t", strings.Repeat("A", maxPermissionActionBytes+1)) {
		t.Fatal("oversized action match should be false")
	}
}
