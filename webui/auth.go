package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Role is a coarse-grained permission level attached to every token.
type Role string

const (
	// RoleAdmin may run any statement (read/write/DDL) and manage tokens + audit.
	RoleAdmin Role = "admin"
	// RoleReadWrite may run reads and DML (INSERT/UPDATE/DELETE/REPLACE).
	RoleReadWrite Role = "readwrite"
	// RoleReadOnly may run reads only (SELECT/SHOW/DESCRIBE/EXPLAIN/...).
	RoleReadOnly Role = "readonly"

	bootstrapTokenID = "bootstrap"
)

// parseRole validates and normalizes a role string.
func parseRole(s string) (Role, error) {
	switch Role(strings.ToLower(strings.TrimSpace(s))) {
	case RoleAdmin:
		return RoleAdmin, nil
	case RoleReadWrite:
		return RoleReadWrite, nil
	case RoleReadOnly:
		return RoleReadOnly, nil
	default:
		return "", fmt.Errorf("invalid role %q (want admin, readwrite, or readonly)", s)
	}
}

// queryClass is the operation category a SQL statement falls into.
type queryClass int

const (
	classRead queryClass = iota
	classWrite
	classDDL
)

func (c queryClass) String() string {
	switch c {
	case classRead:
		return "read"
	case classWrite:
		return "write"
	default:
		return "ddl"
	}
}

// allows reports whether the role is permitted to run a statement of the given class.
func (role Role) allows(c queryClass) bool {
	switch role {
	case RoleAdmin:
		return true
	case RoleReadWrite:
		return c == classRead || c == classWrite
	case RoleReadOnly:
		return c == classRead
	default:
		return false
	}
}

// isAdmin reports whether the role may manage tokens and read the audit log.
func (role Role) isAdmin() bool { return role == RoleAdmin }

// firstSQLKeyword returns the uppercased leading keyword of a statement.
// It skips leading ASCII whitespace; a statement that does not start with a
// letter (e.g. one disguised behind a comment) yields "" and is classified as
// classDDL — fail-safe, since only admins may run unclassifiable statements.
func firstSQLKeyword(sql string) string {
	i, n := 0, len(sql)
	for i < n {
		switch sql[i] {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			i++
			continue
		}
		break
	}
	start := i
	for i < n {
		c := sql[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			i++
			continue
		}
		break
	}
	return toUpperFast(sql[start:i])
}

// classifyQuery maps a SQL statement to its operation class. Unknown leading
// keywords fall through to classDDL so that non-admin roles are denied by
// default (fail-closed).
func classifyQuery(sql string) queryClass {
	switch firstSQLKeyword(sql) {
	case "SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "PRAGMA", "VALUES":
		return classRead
	case "INSERT", "UPDATE", "DELETE", "REPLACE", "MERGE", "UPSERT":
		return classWrite
	default:
		return classDDL
	}
}

// principal identifies an authenticated caller. It carries no secret material.
type principal struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Role      Role      `json:"role"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// expired reports whether the principal's token has passed its expiry.
func (p principal) expired(now time.Time) bool {
	return !p.ExpiresAt.IsZero() && now.After(p.ExpiresAt)
}

// tokenRecord stores a token digest alongside its principal metadata.
type tokenRecord struct {
	hash      [sha256.Size]byte
	principal principal
}

// tokenStore is a concurrency-safe set of API tokens keyed by principal ID.
// Tokens are stored only as SHA-256 digests; raw values are never retained.
type tokenStore struct {
	mu     sync.RWMutex
	tokens map[string]*tokenRecord
	now    func() time.Time
}

func newTokenStore() *tokenStore {
	return &tokenStore{tokens: make(map[string]*tokenRecord), now: time.Now}
}

func (ts *tokenStore) clock() time.Time {
	if ts.now != nil {
		return ts.now()
	}
	return time.Now()
}

// addWithID stores a token digest under an explicit principal ID, replacing any
// existing record with that ID. ttl <= 0 means the token never expires.
func (ts *tokenStore) addWithID(id, value, name string, role Role, ttl time.Duration) (principal, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxWebUITokenBytes {
		return principal{}, fmt.Errorf("token must be non-empty and at most %d bytes", maxWebUITokenBytes)
	}
	if strings.TrimSpace(name) == "" {
		name = id
	}
	if len(name) > maxWebUITokenName {
		return principal{}, fmt.Errorf("token name too large")
	}
	p := principal{ID: id, Name: name, Role: role}
	if ttl > 0 {
		p.ExpiresAt = ts.clock().Add(ttl)
	}
	rec := &tokenRecord{hash: sha256.Sum256([]byte(value)), principal: p}

	ts.mu.Lock()
	if ts.tokens == nil {
		ts.tokens = make(map[string]*tokenRecord)
	}
	ts.tokens[id] = rec
	ts.mu.Unlock()
	return p, nil
}

// mint generates a fresh random token, stores its digest, and returns the raw
// value (which the caller must surface exactly once — it cannot be recovered).
func (ts *tokenStore) mint(name string, role Role, ttl time.Duration) (string, principal, error) {
	id, err := generateToken(8)
	if err != nil {
		return "", principal{}, err
	}
	value, err := generateToken(32)
	if err != nil {
		return "", principal{}, err
	}
	p, err := ts.addWithID(id, value, name, role, ttl)
	if err != nil {
		return "", principal{}, err
	}
	return value, p, nil
}

// setBootstrap installs (or clears) the bootstrap admin token. An empty or
// oversized value fails closed by removing the bootstrap token.
func (ts *tokenStore) setBootstrap(value string) {
	value = strings.TrimSpace(value)
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.tokens == nil {
		ts.tokens = make(map[string]*tokenRecord)
	}
	if value == "" || len(value) > maxWebUITokenBytes {
		delete(ts.tokens, bootstrapTokenID)
		return
	}
	ts.tokens[bootstrapTokenID] = &tokenRecord{
		hash:      sha256.Sum256([]byte(value)),
		principal: principal{ID: bootstrapTokenID, Name: "bootstrap", Role: RoleAdmin},
	}
}

// rotate replaces the token value for an existing principal, returning the new
// raw value. Metadata (role, expiry) is preserved.
func (ts *tokenStore) rotate(id string) (string, principal, bool) {
	value, err := generateToken(32)
	if err != nil {
		return "", principal{}, false
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	rec, ok := ts.tokens[id]
	if !ok {
		return "", principal{}, false
	}
	rec.hash = sha256.Sum256([]byte(value))
	return value, rec.principal, true
}

// revoke removes a token by principal ID.
func (ts *tokenStore) revoke(id string) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, ok := ts.tokens[id]; !ok {
		return false
	}
	delete(ts.tokens, id)
	return true
}

// resolve returns the principal a raw token authenticates as, if any. Expired
// tokens are treated as invalid. The digest comparison is constant-time.
func (ts *tokenStore) resolve(value string) (principal, bool) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxWebUITokenBytes {
		return principal{}, false
	}
	h := sha256.Sum256([]byte(value))
	now := ts.clock()

	ts.mu.RLock()
	defer ts.mu.RUnlock()
	var matched *tokenRecord
	for _, rec := range ts.tokens {
		if subtle.ConstantTimeCompare(h[:], rec.hash[:]) == 1 {
			matched = rec
		}
	}
	if matched == nil || matched.principal.expired(now) {
		return principal{}, false
	}
	return matched.principal, true
}

// list returns principal metadata for all tokens, sorted by name then ID.
func (ts *tokenStore) list() []principal {
	ts.mu.RLock()
	out := make([]principal, 0, len(ts.tokens))
	for _, rec := range ts.tokens {
		out = append(out, rec.principal)
	}
	ts.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].ID < out[j].ID
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// count returns the number of tokens currently stored.
func (ts *tokenStore) count() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return len(ts.tokens)
}

// purgeExpired removes every token whose expiry has passed; returns the count removed.
func (ts *tokenStore) purgeExpired() int {
	now := ts.clock()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	removed := 0
	for id, rec := range ts.tokens {
		if rec.principal.expired(now) {
			delete(ts.tokens, id)
			removed++
		}
	}
	return removed
}

// principalContextKey is the typed key for stashing the resolved principal.
type principalContextKeyType struct{}

var principalContextKey = principalContextKeyType{}

func withPrincipal(r *http.Request, p principal) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), principalContextKey, p))
}

func principalFromRequest(r *http.Request) (principal, bool) {
	p, ok := r.Context().Value(principalContextKey).(principal)
	return p, ok
}
