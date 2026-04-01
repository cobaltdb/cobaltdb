package main

import (
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
