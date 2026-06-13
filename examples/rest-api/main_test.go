package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newRESTAPITestServer() *Server {
	return &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestCreateUserRejectsOversizedJSONBody(t *testing.T) {
	srv := newRESTAPITestServer()
	body := `{"email":"a@example.com","name":"` + strings.Repeat("x", maxRESTJSONBodyBytes) + `","active":true}`
	req := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.createUser(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "request body too large") {
		t.Fatalf("expected body limit error, got %q", rec.Body.String())
	}
}

func TestUpdateUserRejectsOversizedJSONBody(t *testing.T) {
	srv := newRESTAPITestServer()
	body := `{"name":"` + strings.Repeat("x", maxRESTJSONBodyBytes) + `"}`
	req := httptest.NewRequest(http.MethodPatch, "/users/1", strings.NewReader(body))
	rec := httptest.NewRecorder()

	srv.updateUser(rec, req, 1)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}
}

func TestCreateUserRejectsTrailingJSONDocument(t *testing.T) {
	srv := newRESTAPITestServer()
	req := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(`{"email":"a@example.com","name":"a","active":true} {}`))
	rec := httptest.NewRecorder()

	srv.createUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "single JSON document") {
		t.Fatalf("expected single-document error, got %q", rec.Body.String())
	}
}
