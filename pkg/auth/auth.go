package auth

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
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

// Authenticator handles user authentication
type Authenticator struct {
	mu       sync.RWMutex
	users    map[string]*User
	sessions map[string]*Session
	enabled  bool
	stopCh   chan struct{}
	stopped  bool
}

// NewAuthenticator creates a new authenticator
func NewAuthenticator() *Authenticator {
	a := &Authenticator{
		users:    make(map[string]*User),
		sessions: make(map[string]*Session),
		enabled:  false,
		stopCh:   make(chan struct{}),
	}
	go a.sessionCleanupLoop()
	return a
}

// Stop stops the authenticator's background goroutine
func (a *Authenticator) Stop() {
	a.mu.Lock()
	if !a.stopped {
		a.stopped = true
		close(a.stopCh)
	}
	a.mu.Unlock()
}

// sessionCleanupLoop periodically removes expired sessions
func (a *Authenticator) sessionCleanupLoop() {
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

// generateSalt generates a cryptographically secure random salt
func generateSalt() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// CreateUser creates a new user
func (a *Authenticator) CreateUser(username, password string, isAdmin bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.createUserLocked(username, password, isAdmin)
}

// createUserLocked creates a user (must be called with lock held)
func (a *Authenticator) createUserLocked(username, password string, isAdmin bool) error {
	if _, exists := a.users[username]; exists {
		return ErrUserExists
	}

	salt, err := generateSalt()
	if err != nil {
		return err
	}
	passwordHash := hashPassword(password, salt)

	a.users[username] = &User{
		Username:        username,
		PasswordHash:    passwordHash,
		Salt:            salt,
		MySQLNativeHash: mysqlNativeHash(password),
		IsAdmin:         isAdmin,
		CreatedAt:       time.Now(),
		Permissions:     make([]Permission, 0),
	}

	return nil
}

// ValidateCredentials checks if the username and password are valid without
// creating a session. Returns nil on success or ErrInvalidCredentials.
func (a *Authenticator) ValidateCredentials(username, password string) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	user, exists := a.users[username]
	if !exists {
		return ErrInvalidCredentials
	}

	passwordHash := hashPassword(password, user.Salt)
	if subtle.ConstantTimeCompare([]byte(passwordHash), []byte(user.PasswordHash)) != 1 {
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
	return user.MySQLNativeHash, nil
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
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return "", ErrInvalidCredentials
	}

	passwordHash := hashPassword(password, user.Salt)
	if subtle.ConstantTimeCompare([]byte(passwordHash), []byte(user.PasswordHash)) != 1 {
		return "", ErrInvalidCredentials
	}

	// Update last login
	user.LastLogin = time.Now()

	// Generate session token
	token, err := generateToken(username)
	if err != nil {
		return "", err
	}
	session := &Session{
		Token:     token,
		Username:  username,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(24 * time.Hour), // 24 hour expiration
	}

	a.sessions[token] = session
	return token, nil
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

// ValidateToken validates a session token
func (a *Authenticator) ValidateToken(token string) (*Session, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	session, exists := a.sessions[token]
	if !exists {
		return nil, ErrInvalidToken
	}

	if time.Now().After(session.ExpiresAt) {
		return nil, ErrTokenExpired
	}

	return session, nil
}

// Logout invalidates a session token
func (a *Authenticator) Logout(token string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sessions, token)
}

// ChangePassword changes a user's password
func (a *Authenticator) ChangePassword(username, oldPassword, newPassword string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	passwordHash := hashPassword(oldPassword, user.Salt)
	if subtle.ConstantTimeCompare([]byte(passwordHash), []byte(user.PasswordHash)) != 1 {
		return ErrInvalidCredentials
	}

	// Generate new salt and hash
	salt, err := generateSalt()
	if err != nil {
		return err
	}
	user.Salt = salt
	user.PasswordHash = hashPassword(newPassword, salt)
	user.MySQLNativeHash = mysqlNativeHash(newPassword)

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

	// Return a copy
	userCopy := *user
	return &userCopy, nil
}

// HasPermission checks if a user has a specific permission
func (a *Authenticator) HasPermission(username, database, table, action string) bool {
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
			merged := make([]string, 0, len(actionMap))
			for a := range actionMap {
				merged = append(merged, a)
			}
			user.Permissions[i].Actions = merged
			return nil
		}
	}

	// Add new permission
	user.Permissions = append(user.Permissions, Permission{
		Database: database,
		Table:    table,
		Actions:  actions,
	})

	return nil
}

// RevokePermission revokes a permission from a user
func (a *Authenticator) RevokePermission(username, database, table string, actions []string) error {
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
				user.Permissions[i].Actions = merged
			}
			return nil
		}
	}

	return nil
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

// CleanupExpiredSessions removes expired sessions
func (a *Authenticator) CleanupExpiredSessions() {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	for token, session := range a.sessions {
		if now.After(session.ExpiresAt) {
			delete(a.sessions, token)
		}
	}
}
