package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Regression tests for URL path segment handling in the saved-query API.
//
// The handlers used to apply url.QueryUnescape to r.URL.Path, which Go's
// net/http has ALREADY percent-decoded. QueryUnescape additionally maps '+'
// to a space, so a saved query stored under the name "c+1" (the POST body is
// JSON, so the map key keeps the literal '+') became unreachable through its
// canonical URL "/api/saved-queries/c%2B1" — exactly what the webui client
// produces via encodeURIComponent (app.js). DELETE was worse: it reported
// {"status":"deleted"} while actually deleting the key "c 1" — a silent
// wrong-entry deletion with false success.

func newSavedQueryServer() *Server {
	return &Server{savedQueries: make(map[string]SavedQuery)}
}

func TestDoubleDecodeSavedQueryPlusNameRoundtrip(t *testing.T) {
	srv := newSavedQueryServer()

	rec := httptest.NewRecorder()
	srv.handleSavedQueries(rec, httptest.NewRequest(http.MethodPost, "/api/saved-queries",
		strings.NewReader(`{"name":"c+1","query":"SELECT 1","description":""}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, body %s", rec.Code, rec.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/api/saved-queries/c%2B1", nil)
	rec = httptest.NewRecorder()
	srv.handleSavedQuery(rec, get)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/saved-queries/c%%2B1 status = %d, body %s; a saved query with '+' in its name must be retrievable through its canonical URL", rec.Code, rec.Body.String())
	}
}

func TestDoubleDecodeDeleteTargetsDecodedName(t *testing.T) {
	srv := newSavedQueryServer()
	for _, name := range []string{"c+1", "c 1"} {
		body := `{"name":"` + name + `","query":"SELECT 1"}`
		rec := httptest.NewRecorder()
		srv.handleSavedQueries(rec, httptest.NewRequest(http.MethodPost, "/api/saved-queries", strings.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("POST %q status = %d, body %s", name, rec.Code, rec.Body.String())
		}
	}

	del := httptest.NewRequest(http.MethodDelete, "/api/saved-queries/c%2B1", nil)
	rec := httptest.NewRecorder()
	srv.handleSavedQuery(rec, del)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE status = %d, body %s", rec.Code, rec.Body.String())
	}

	srv.mu.RLock()
	_, plusStillThere := srv.savedQueries["c+1"]
	_, spaceStillThere := srv.savedQueries["c 1"]
	srv.mu.RUnlock()
	if plusStillThere {
		t.Fatal("DELETE /api/saved-queries/c%2B1 left the 'c+1' entry in place (deleted the wrong key or nothing)")
	}
	if !spaceStillThere {
		t.Fatal("unrelated entry 'c 1' was deleted by DELETE /api/saved-queries/c%2B1")
	}
}

func TestDoubleDecodeSpaceNameRoundtrip(t *testing.T) {
	// Control: a name containing a real space keeps working through its
	// percent-encoded canonical URL.
	srv := newSavedQueryServer()
	rec := httptest.NewRecorder()
	srv.handleSavedQueries(rec, httptest.NewRequest(http.MethodPost, "/api/saved-queries",
		strings.NewReader(`{"name":"c 1","query":"SELECT 1"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, body %s", rec.Code, rec.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/api/saved-queries/c%201", nil)
	rec = httptest.NewRecorder()
	srv.handleSavedQuery(rec, get)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/saved-queries/c%%201 status = %d, body %s", rec.Code, rec.Body.String())
	}
}
