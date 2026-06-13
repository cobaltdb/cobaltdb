package auth

import (
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- Required for MySQL native_password protocol compatibility.
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrUserExists         = errors.New("user already exists")
	ErrUserNotFound       = errors.New("user not found")
	ErrUnauthorized       = errors.New("unauthorized")
	ErrTokenExpired       = errors.New("token expired")
	ErrInvalidToken       = errors.New("invalid token")
	ErrInvalidUsername    = errors.New("invalid username")
	ErrInvalidPassword    = errors.New("invalid password")
	ErrInvalidPermission  = errors.New("invalid permission")
	ErrTooManySessions    = errors.New("too many active sessions")
)

// User represents a database user
type User struct {
	Username        string
	PasswordHash    string
	Salt            string
	MySQLNativeHash []byte // SHA1(SHA1(password)) for MySQL native_password auth (FIX-004)
	IsAdmin         bool
	CreatedAt       time.Time
	LastLogin       time.Time
	Permissions     []Permission
}

// Permission represents a database permission
type Permission struct {
	Database string
	Table    string
	Actions  []string // SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, etc.
}

// Session represents an authenticated session
type Session struct {
	Token     string
	Username  string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// loginAttempt tracks failed login attempts for brute-force protection
type loginAttempt struct {
	count     int
	lastFail  time.Time
	lockUntil time.Time
}

const (
	maxLoginAttempts  = 5
	lockoutDuration   = 5 * time.Minute
	attemptResetAfter = 15 * time.Minute
	maxUsernameBytes  = 256
	maxPasswordBytes  = 1024
	// Bound failed-attempt tracking so unauthenticated clients cannot grow
	// memory usage indefinitely with unique usernames.
	maxFailedAttemptEntries  = 4096
	maxPermissionTargetBytes = 256
	maxPermissionActionBytes = 64
	maxPermissionActions     = 64
	maxPermissionsPerUser    = 1024
	maxSessionTokenBytes     = 512
	maxActiveSessions        = 4096
)

// Authenticator handles user authentication
type Authenticator struct {
	mu                    sync.RWMutex
	users                 map[string]*User
	sessions              map[string]*Session
	enabled               bool
	stopCh                chan struct{}
	stopped               bool
	wg                    sync.WaitGroup
	failedAttempts        map[string]*loginAttempt
	failedMu              sync.RWMutex
	enforcePasswordPolicy bool
}

// NewAuthenticator creates a new authenticator
func NewAuthenticator() *Authenticator {
	a := &Authenticator{
		users:          make(map[string]*User),
		sessions:       make(map[string]*Session),
		enabled:        false,
		stopCh:         make(chan struct{}),
		failedAttempts: make(map[string]*loginAttempt),
	}
	a.wg.Add(1)
	go a.sessionCleanupLoop()
	return a
}

// Stop stops the authenticator's background goroutine
func (a *Authenticator) Stop() {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return
	}
	a.stopped = true
	close(a.stopCh)
	a.mu.Unlock()
	a.wg.Wait()
}

// sessionCleanupLoop periodically removes expired sessions
func (a *Authenticator) sessionCleanupLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
			a.CleanupExpiredSessions()
		}
	}
}

// Enable enables authentication
func (a *Authenticator) Enable() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.enabled = true
}

// Disable disables authentication
func (a *Authenticator) Disable() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.enabled = false
}

// IsEnabled returns whether authentication is enabled
func (a *Authenticator) IsEnabled() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.enabled
}

// mysqlNativeHash computes SHA1(SHA1(password)) for MySQL native_password auth (FIX-004).
func mysqlNativeHash(password string) []byte {
	// #nosec G401 -- Required for MySQL native_password protocol compatibility.
	h1 := sha1.Sum([]byte(password))
	// #nosec G401 -- Required for MySQL native_password protocol compatibility.
	h2 := sha1.Sum(h1[:])
	return h2[:]
}

// hashPassword hashes a password with salt using Argon2id (memory-hard, GPU-resistant)
func hashPassword(password, salt string) string {
	hash := argon2.IDKey([]byte(password), []byte(salt), 3, 64*1024, 4, 32)
	return hex.EncodeToString(hash)
}

var passwordHasher = hashPassword

// generateSalt generates a cryptographically secure random salt
func generateSalt() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// SetPasswordPolicy enables or disables password complexity enforcement
func (a *Authenticator) SetPasswordPolicy(enforce bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.enforcePasswordPolicy = enforce
}

// validatePasswordStrength checks password complexity requirements
func validatePasswordStrength(password string) error {
	if len(password) < 8 {
		return fmt.Errorf("password must be at least 8 characters")
	}
	var hasUpper, hasLower, hasDigit bool
	for _, ch := range password {
		switch {
		case ch >= 'A' && ch <= 'Z':
			hasUpper = true
		case ch >= 'a' && ch <= 'z':
			hasLower = true
		case ch >= '0' && ch <= '9':
			hasDigit = true
		}
	}
	if !hasUpper || !hasLower || !hasDigit {
		return fmt.Errorf("password must contain at least one uppercase letter, one lowercase letter, and one digit")
	}
	return nil
}

func validateUsername(username string) error {
	if username == "" || len(username) > maxUsernameBytes {
		return ErrInvalidUsername
	}
	return nil
}

func validatePasswordRequired(password string) error {
	if password == "" || len(password) > maxPasswordBytes {
		return ErrInvalidPassword
	}
	return nil
}

func validateCredentialInput(username, password string) error {
	if validateUsername(username) != nil || validatePasswordRequired(password) != nil {
		return ErrInvalidCredentials
	}
	return nil
}

func validatePermissionInput(database, table string, actions []string) error {
	if len(database) > maxPermissionTargetBytes || len(table) > maxPermissionTargetBytes {
		return ErrInvalidPermission
	}
	if len(actions) == 0 || len(actions) > maxPermissionActions {
		return ErrInvalidPermission
	}
	for _, action := range actions {
		if action == "" || len(action) > maxPermissionActionBytes {
			return ErrInvalidPermission
		}
	}
	return nil
}

func validateSessionTokenInput(token string) error {
	if token == "" || len(token) > maxSessionTokenBytes {
		return ErrInvalidToken
	}
	return nil
}

// CreateUser creates a new user
func (a *Authenticator) CreateUser(username, password string, isAdmin bool) error {
	if err := validateUsername(username); err != nil {
		return err
	}
	if err := validatePasswordRequired(password); err != nil {
		return err
	}

	a.mu.RLock()
	if _, exists := a.users[username]; exists {
		a.mu.RUnlock()
		return ErrUserExists
	}
	enforcePasswordPolicy := a.enforcePasswordPolicy
	a.mu.RUnlock()

	if enforcePasswordPolicy {
		if err := validatePasswordStrength(password); err != nil {
			return err
		}
	}

	salt, err := generateSalt()
	if err != nil {
		return err
	}
	passwordHash := passwordHasher(password, salt)
	mysqlHash := mysqlNativeHash(password)

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.users[username]; exists {
		return ErrUserExists
	}
	if a.enforcePasswordPolicy && !enforcePasswordPolicy {
		if err := validatePasswordStrength(password); err != nil {
			return err
		}
	}

	a.users[username] = &User{
		Username:        username,
		PasswordHash:    passwordHash,
		Salt:            salt,
		MySQLNativeHash: mysqlHash,
		IsAdmin:         isAdmin,
		CreatedAt:       time.Now(),
		Permissions:     make([]Permission, 0),
	}

	return nil
}

// ValidateCredentials checks if the username and password are valid without
// creating a session. Returns nil on success or ErrInvalidCredentials.
func (a *Authenticator) ValidateCredentials(username, password string) error {
	if err := validateCredentialInput(username, password); err != nil {
		return err
	}

	a.mu.RLock()
	user, exists := a.users[username]
	if !exists {
		a.mu.RUnlock()
		return ErrInvalidCredentials
	}
	salt := user.Salt
	passwordHashStored := user.PasswordHash
	a.mu.RUnlock()

	passwordHash := passwordHasher(password, salt)
	if subtle.ConstantTimeCompare([]byte(passwordHash), []byte(passwordHashStored)) != 1 {
		return ErrInvalidCredentials
	}

	return nil
}

// GetMySQLNativeHash returns the MySQL native password hash (SHA1(SHA1(password)))
// for the given user. Returns nil,ErrUserNotFound if the user doesn't exist.
func (a *Authenticator) GetMySQLNativeHash(username string) ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	user, exists := a.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}
	return cloneBytes(user.MySQLNativeHash), nil
}

// UserExists returns true if the given username is known to the authenticator.
func (a *Authenticator) UserExists(username string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, exists := a.users[username]
	return exists
}

// Authenticate authenticates a user and returns a session token
func (a *Authenticator) Authenticate(username, password string) (string, error) {
	if err := validateCredentialInput(username, password); err != nil {
		return "", err
	}

	// Check lockout before expensive password work.
	a.failedMu.RLock()
	if attempt, exists := a.failedAttempts[username]; exists && time.Now().Before(attempt.lockUntil) {
		a.failedMu.RUnlock()
		return "", fmt.Errorf("account temporarily locked due to too many failed attempts")
	}
	a.failedMu.RUnlock()

	a.mu.RLock()
	user, exists := a.users[username]
	if !exists {
		a.mu.RUnlock()
		count := a.recordFailedAttempt(username)
		sleepFailedAttempt(count)
		return "", ErrInvalidCredentials
	}
	salt := user.Salt
	passwordHashStored := user.PasswordHash
	a.mu.RUnlock()

	passwordHash := passwordHasher(password, salt)
	if subtle.ConstantTimeCompare([]byte(passwordHash), []byte(passwordHashStored)) != 1 {
		count := a.recordFailedAttempt(username)
		sleepFailedAttempt(count)
		return "", ErrInvalidCredentials
	}

	// Generate session token outside the write lock.
	token, err := generateToken(username)
	if err != nil {
		return "", err
	}

	a.mu.Lock()
	user, exists = a.users[username]
	if !exists || user.Salt != salt || user.PasswordHash != passwordHashStored {
		a.mu.Unlock()
		return "", ErrInvalidCredentials
	}

	now := time.Now()
	if len(a.sessions) >= maxActiveSessions {
		a.cleanupExpiredSessionsLocked(now)
		if len(a.sessions) >= maxActiveSessions {
			a.mu.Unlock()
			return "", ErrTooManySessions
		}
	}

	// Clear failed attempts on success
	a.failedMu.Lock()
	delete(a.failedAttempts, username)
	a.failedMu.Unlock()

	// Update last login
	user.LastLogin = now

	session := &Session{
		Username:  username,
		CreatedAt: now,
		ExpiresAt: now.Add(24 * time.Hour), // 24 hour expiration
	}

	a.sessions[sessionTokenKey(token)] = session
	a.mu.Unlock()
	return token, nil
}

func sleepFailedAttempt(count int) {
	time.Sleep(time.Duration(min(count, 5)) * 200 * time.Millisecond)
}

// recordFailedAttempt records a failed login attempt for brute-force protection.
// Returns the current attempt count for the user.
func (a *Authenticator) recordFailedAttempt(username string) int {
	now := time.Now()

	a.failedMu.Lock()
	if a.failedAttempts[username] == nil {
		if len(a.failedAttempts) >= maxFailedAttemptEntries {
			a.pruneFailedAttemptsLocked(now)
		}
		if len(a.failedAttempts) >= maxFailedAttemptEntries {
			a.failedMu.Unlock()
			return maxLoginAttempts
		}
		a.failedAttempts[username] = &loginAttempt{}
	}
	a.failedAttempts[username].count++
	a.failedAttempts[username].lastFail = now
	if a.failedAttempts[username].count >= maxLoginAttempts {
		a.failedAttempts[username].lockUntil = now.Add(lockoutDuration)
	}
	count := a.failedAttempts[username].count
	a.failedMu.Unlock()
	return count
}

func (a *Authenticator) pruneFailedAttemptsLocked(now time.Time) {
	for username, attempt := range a.failedAttempts {
		if now.After(attempt.lastFail.Add(attemptResetAfter)) {
			delete(a.failedAttempts, username)
		}
	}
}

// generateToken generates a cryptographically secure session token
func generateToken(username string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	// Use only random bytes for token - no predictable input
	return hex.EncodeToString(b), nil
}

func sessionTokenKey(token string) string {
	digest := sessionTokenDigest(token)
	return hex.EncodeToString(digest[:])
}

func sessionTokenDigest(token string) [sha256.Size]byte {
	var lengthPrefix [8]byte
	binary.BigEndian.PutUint64(lengthPrefix[:], uint64(len(token)))

	h := sha256.New()
	_, _ = h.Write(lengthPrefix[:])
	_, _ = h.Write([]byte(token))

	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

// ValidateToken validates a session token
func (a *Authenticator) ValidateToken(token string) (*Session, error) {
	if err := validateSessionTokenInput(token); err != nil {
		return nil, err
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	session, exists := a.sessions[sessionTokenKey(token)]
	if !exists {
		return nil, ErrInvalidToken
	}

	if time.Now().After(session.ExpiresAt) {
		return nil, ErrTokenExpired
	}

	cloned := cloneSession(session)
	cloned.Token = token
	return cloned, nil
}

// Logout invalidates a session token
func (a *Authenticator) Logout(token string) {
	if validateSessionTokenInput(token) != nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sessions, sessionTokenKey(token))
}

// ChangePassword changes a user's password
func (a *Authenticator) ChangePassword(username, oldPassword, newPassword string) error {
	if err := validateUsername(username); err != nil {
		return err
	}
	if err := validatePasswordRequired(newPassword); err != nil {
		return err
	}

	a.mu.RLock()
	user, exists := a.users[username]
	if !exists {
		a.mu.RUnlock()
		return ErrUserNotFound
	}
	oldSalt := user.Salt
	passwordHashStored := user.PasswordHash
	enforcePasswordPolicy := a.enforcePasswordPolicy
	a.mu.RUnlock()

	if err := validateCredentialInput(username, oldPassword); err != nil {
		return ErrInvalidCredentials
	}

	passwordHash := passwordHasher(oldPassword, oldSalt)
	if subtle.ConstantTimeCompare([]byte(passwordHash), []byte(passwordHashStored)) != 1 {
		return ErrInvalidCredentials
	}

	if enforcePasswordPolicy {
		if err := validatePasswordStrength(newPassword); err != nil {
			return err
		}
	}

	// Generate and hash outside the write lock; Argon2 is intentionally expensive.
	newSalt, err := generateSalt()
	if err != nil {
		return err
	}
	newPasswordHash := passwordHasher(newPassword, newSalt)
	newMySQLNativeHash := mysqlNativeHash(newPassword)

	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists = a.users[username]
	if !exists {
		return ErrUserNotFound
	}
	if user.Salt != oldSalt || user.PasswordHash != passwordHashStored {
		return ErrInvalidCredentials
	}
	if a.enforcePasswordPolicy && !enforcePasswordPolicy {
		if err := validatePasswordStrength(newPassword); err != nil {
			return err
		}
	}

	user.Salt = newSalt
	user.PasswordHash = newPasswordHash
	user.MySQLNativeHash = newMySQLNativeHash

	// Invalidate all active sessions for this user
	for token, sess := range a.sessions {
		if sess.Username == username {
			delete(a.sessions, token)
		}
	}

	return nil
}

// DeleteUser deletes a user
func (a *Authenticator) DeleteUser(username string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.users[username]; !exists {
		return ErrUserNotFound
	}

	delete(a.users, username)

	// Invalidate all sessions for this user
	for token, session := range a.sessions {
		if session.Username == username {
			delete(a.sessions, token)
		}
	}

	return nil
}

// GetUser returns a user by username
func (a *Authenticator) GetUser(username string) (*User, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	user, exists := a.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	return cloneUser(user), nil
}

// HasPermission checks if a user has a specific permission
func (a *Authenticator) HasPermission(username, database, table, action string) bool {
	if validateUsername(username) != nil ||
		len(database) > maxPermissionTargetBytes ||
		len(table) > maxPermissionTargetBytes ||
		action == "" ||
		len(action) > maxPermissionActionBytes {
		return false
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	user, exists := a.users[username]
	if !exists {
		return false
	}

	// Admins have all permissions
	if user.IsAdmin {
		return true
	}

	// Check specific permissions
	for _, perm := range user.Permissions {
		if perm.Database != "" && perm.Database != database {
			continue
		}
		if perm.Table != "" && perm.Table != table {
			continue
		}
		for _, permAction := range perm.Actions {
			if permAction == "*" || permAction == action {
				return true
			}
		}
	}

	return false
}

// GrantPermission grants a permission to a user
func (a *Authenticator) GrantPermission(username, database, table string, actions []string) error {
	if err := validateUsername(username); err != nil {
		return err
	}
	if err := validatePermissionInput(database, table, actions); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Check if permission already exists and update it
	for i, perm := range user.Permissions {
		if perm.Database == database && perm.Table == table {
			// Merge actions
			actionMap := make(map[string]bool)
			for _, a := range perm.Actions {
				actionMap[a] = true
			}
			for _, a := range actions {
				actionMap[a] = true
			}
			if len(actionMap) > maxPermissionActions {
				return ErrInvalidPermission
			}
			merged := make([]string, 0, len(actionMap))
			for a := range actionMap {
				merged = append(merged, a)
			}
			sort.Strings(merged)
			user.Permissions[i].Actions = merged
			return nil
		}
	}

	// Add new permission
	if len(user.Permissions) >= maxPermissionsPerUser {
		return ErrInvalidPermission
	}
	user.Permissions = append(user.Permissions, Permission{
		Database: database,
		Table:    table,
		Actions:  cloneStringSlice(actions),
	})

	return nil
}

// RevokePermission revokes a permission from a user
func (a *Authenticator) RevokePermission(username, database, table string, actions []string) error {
	if err := validateUsername(username); err != nil {
		return err
	}
	if err := validatePermissionInput(database, table, actions); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Find and update permission
	for i, perm := range user.Permissions {
		if perm.Database == database && perm.Table == table {
			actionMap := make(map[string]bool)
			for _, a := range perm.Actions {
				actionMap[a] = true
			}
			for _, a := range actions {
				delete(actionMap, a)
			}
			if len(actionMap) == 0 {
				// Remove empty permission
				user.Permissions = append(user.Permissions[:i], user.Permissions[i+1:]...)
			} else {
				merged := make([]string, 0, len(actionMap))
				for a := range actionMap {
					merged = append(merged, a)
				}
				sort.Strings(merged)
				user.Permissions[i].Actions = merged
			}
			return nil
		}
	}

	return nil
}

func cloneUser(user *User) *User {
	if user == nil {
		return nil
	}
	cloned := *user
	cloned.MySQLNativeHash = cloneBytes(user.MySQLNativeHash)
	cloned.Permissions = clonePermissions(user.Permissions)
	return &cloned
}

func cloneSession(session *Session) *Session {
	if session == nil {
		return nil
	}
	cloned := *session
	return &cloned
}

func clonePermissions(permissions []Permission) []Permission {
	if permissions == nil {
		return nil
	}
	cloned := make([]Permission, len(permissions))
	for i, permission := range permissions {
		cloned[i] = permission
		cloned[i].Actions = cloneStringSlice(permission.Actions)
	}
	return cloned
}

func cloneStringSlice(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneBytes(values []byte) []byte {
	if values == nil {
		return nil
	}
	cloned := make([]byte, len(values))
	copy(cloned, values)
	return cloned
}

func (a *Authenticator) cleanupExpiredSessionsLocked(now time.Time) {
	for token, session := range a.sessions {
		if now.After(session.ExpiresAt) {
			delete(a.sessions, token)
		}
	}
}

// ListUsers returns a list of all usernames
func (a *Authenticator) ListUsers() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	usernames := make([]string, 0, len(a.users))
	for username := range a.users {
		usernames = append(usernames, username)
	}
	return usernames
}

// StartSessionCleanup starts a background goroutine that periodically cleans up expired sessions.
// It stops when stopCh is closed.
func (a *Authenticator) StartSessionCleanup(interval time.Duration, stopCh <-chan struct{}) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				a.CleanupExpiredSessions()
			case <-stopCh:
				return
			}
		}
	}()
}

// CleanupExpiredSessions removes expired sessions and stale login attempt records
func (a *Authenticator) CleanupExpiredSessions() {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	a.cleanupExpiredSessionsLocked(now)

	// Clean up stale failed login attempt records (lock ordering: mu first, then failedMu)
	a.failedMu.Lock()
	a.pruneFailedAttemptsLocked(now)
	a.failedMu.Unlock()
}
