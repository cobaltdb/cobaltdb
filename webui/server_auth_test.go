package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthMiddlewareRejectsMissingToken(t *testing.T) {
	srv := &Server{
		apiToken:    "test-token",
		authEnabled: true,
	}
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}
}

func TestAuthMiddlewareAcceptsHeaderToken(t *testing.T) {
	srv := &Server{
		apiToken:    "test-token",
		authEnabled: true,
	}
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	req.Header.Set("X-CobaltDB-Token", "test-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}
}

func TestAuthMiddlewareSetsCookieAndRedirectsWhenTokenInQuery(t *testing.T) {
	srv := &Server{
		apiToken:    "test-token",
		authEnabled: true,
	}
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/?token=test-token", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected status %d, got %d", http.StatusFound, rec.Code)
	}
	if rec.Header().Get("Location") != "/" {
		t.Fatalf("expected redirect location '/', got %q", rec.Header().Get("Location"))
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 || cookies[0].Name != authCookieName {
		t.Fatalf("expected auth cookie %q to be set", authCookieName)
	}
	if cookies[0].Secure {
		t.Fatal("expected HTTP auth cookie to be usable without Secure flag")
	}
	if !cookies[0].HttpOnly {
		t.Fatal("expected auth cookie to be HttpOnly")
	}
}

func TestAuthMiddlewareSetsSecureCookieForHTTPS(t *testing.T) {
	srv := &Server{
		apiToken:    "test-token",
		authEnabled: true,
	}
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "https://example.com/?token=test-token", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	cookies := rec.Result().Cookies()
	if len(cookies) == 0 || !cookies[0].Secure {
		t.Fatal("expected HTTPS auth cookie to include Secure flag")
	}
}

func TestAuthMiddlewareSetsSecureCookieBehindHTTPSProxy(t *testing.T) {
	srv := &Server{
		apiToken:    "test-token",
		authEnabled: true,
	}
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/?token=test-token", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	cookies := rec.Result().Cookies()
	if len(cookies) == 0 || !cookies[0].Secure {
		t.Fatal("expected proxied HTTPS auth cookie to include Secure flag")
	}
}

func TestAuthMiddlewareBypassWhenDisabled(t *testing.T) {
	srv := &Server{
		authEnabled: false,
	}
	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, rec.Code)
	}
}

func TestQuoteSQLIdentifier(t *testing.T) {
	got, err := quoteSQLIdentifier(`users"; DROP TABLE users;--`)
	if err != nil {
		t.Fatalf("quoteSQLIdentifier returned error: %v", err)
	}
	want := `"users\"; DROP TABLE users;--"`
	if got != want {
		t.Fatalf("quoteSQLIdentifier = %q, want %q", got, want)
	}
	if _, err := quoteSQLIdentifier(""); err == nil {
		t.Fatal("expected empty identifier to be rejected")
	}
}

func TestHandleUpdateRowRejectsUnsafeIdentifier(t *testing.T) {
	srv := &Server{}
	body, err := json.Marshal(map[string]interface{}{
		"table":  "",
		"column": "name",
		"value":  "alice",
		"where":  map[string]interface{}{"id": 1},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/update-row", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.handleUpdateRow(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}
