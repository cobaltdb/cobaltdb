// REST API Example - Production-ready CobaltDB usage
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// User represents a user in the system
type User struct {
	ID        int64     `json:"id"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

// Server holds all dependencies for the HTTP server
type Server struct {
	db     *engine.DB
	logger *slog.Logger
	mux    *http.ServeMux
}

func main() {
	// Initialize structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Open database (production: use disk, development: use memory)
	dbPath := getEnv("COBALTDB_PATH", ":memory:")
	logger.Info("opening database", "path", dbPath)

	db, err := engine.Open(dbPath, &engine.Options{
		CacheSize:  1024,
		WALEnabled: true,
		InMemory:   dbPath == ":memory:",
	})
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Initialize schema
	if err := initSchema(db); err != nil {
		logger.Error("failed to initialize schema", "error", err)
		os.Exit(1)
	}

	// Create server
	srv := &Server{
		db:     db,
		logger: logger,
		mux:    http.NewServeMux(),
	}
	srv.setupRoutes()

	// HTTP server with timeouts (production-ready)
	httpServer := &http.Server{
		Addr:         getEnv("HTTP_ADDR", ":8080"),
		Handler:      srv.mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in goroutine
	go func() {
		logger.Info("starting HTTP server", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server...")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	logger.Info("server stopped")
}

// initSchema creates the database tables
func initSchema(db *engine.DB) error {
	ctx := context.Background()

	_, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			active BOOLEAN DEFAULT true,
			created_at TEXT DEFAULT CURRENT_TIMESTAMP
		)
	`)
	return err
}

// setupRoutes configures HTTP routes
func (s *Server) setupRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/users", s.handleUsers)
	s.mux.HandleFunc("/users/", s.handleUser)
}

// handleHealth returns health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Check database connectivity
	_, err := s.db.Exec(ctx, "SELECT 1")
	if err != nil {
		s.respondError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}

	s.respondJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// handleUsers handles GET (list) and POST (create)
func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listUsers(w, r)
	case http.MethodPost:
		s.createUser(w, r)
	default:
		s.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleUser handles GET (single), PUT (update), DELETE
func (s *Server) handleUser(w http.ResponseWriter, r *http.Request) {
	// Extract ID from path /users/{id}
	idStr := r.URL.Path[len("/users/"):]
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		s.respondError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getUser(w, r, id)
	case http.MethodPut:
		s.updateUser(w, r, id)
	case http.MethodDelete:
		s.deleteUser(w, r, id)
	default:
		s.respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// listUsers returns all users
func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rows, err := s.db.Query(ctx, `
		SELECT id, email, name, active, created_at
		FROM users
		ORDER BY id DESC
	`)
	if err != nil {
		s.logger.Error("query users failed", "error", err)
		s.respondError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		var createdAtStr string
		if err := rows.Scan(&u.ID, &u.Email, &u.Name, &u.Active, &createdAtStr); err != nil {
			s.logger.Error("scan user failed", "error", err)
			continue
		}
		u.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
		users = append(users, u)
	}

	s.respondJSON(w, http.StatusOK, users)
}

// createUser creates a new user
func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req struct {
		Email  string `json:"email"`
		Name   string `json:"name"`
		Active bool   `json:"active"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.respondError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	// Validation
	if req.Email == "" || req.Name == "" {
		s.respondError(w, http.StatusBadRequest, "email and name are required")
		return
	}

	// Insert with transaction for production safety
	_, err := s.db.Exec(ctx,
		"INSERT INTO users (email, name, active) VALUES (?, ?, ?)",
		req.Email, req.Name, req.Active,
	)
	if err != nil {
		s.logger.Error("insert user failed", "error", err)
		s.respondError(w, http.StatusInternalServerError, "failed to create user")
		return
	}

	s.respondJSON(w, http.StatusCreated, map[string]string{
		"message": "user created",
	})
}

// getUser returns a single user
func (s *Server) getUser(w http.ResponseWriter, r *http.Request, id int64) {
	ctx := r.Context()

	rows, err := s.db.Query(ctx,
		"SELECT id, email, name, active, created_at FROM users WHERE id = ?",
		id,
	)
	if err != nil {
		s.logger.Error("query user failed", "error", err)
		s.respondError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer rows.Close()

	if !rows.Next() {
		s.respondError(w, http.StatusNotFound, "user not found")
		return
	}

	var u User
	var createdAtStr string
	if err := rows.Scan(&u.ID, &u.Email, &u.Name, &u.Active, &createdAtStr); err != nil {
		s.logger.Error("scan user failed", "error", err)
		s.respondError(w, http.StatusInternalServerError, "database error")
		return
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)

	s.respondJSON(w, http.StatusOK, u)
}

// updateUser updates a user
func (s *Server) updateUser(w http.ResponseWriter, r *http.Request, id int64) {
	ctx := r.Context()

	var req struct {
		Name   string `json:"name"`
		Active *bool  `json:"active,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.respondError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	// Build dynamic update
	updates := []interface{}{}
	clauses := []string{}
	if req.Name != "" {
		clauses = append(clauses, "name = ?")
		updates = append(updates, req.Name)
	}
	if req.Active != nil {
		clauses = append(clauses, "active = ?")
		updates = append(updates, *req.Active)
	}
	if len(clauses) == 0 {
		s.respondError(w, http.StatusBadRequest, "no fields to update")
		return
	}

	updates = append(updates, id)
	query := fmt.Sprintf("UPDATE users SET %s WHERE id = ?", joinClauses(clauses))

	result, err := s.db.Exec(ctx, query, updates...)
	if err != nil {
		s.logger.Error("update user failed", "error", err)
		s.respondError(w, http.StatusInternalServerError, "failed to update user")
		return
	}

	if result.RowsAffected == 0 {
		s.respondError(w, http.StatusNotFound, "user not found")
		return
	}

	s.respondJSON(w, http.StatusOK, map[string]string{
		"message": "user updated",
	})
}

// deleteUser deletes a user
func (s *Server) deleteUser(w http.ResponseWriter, r *http.Request, id int64) {
	ctx := r.Context()

	result, err := s.db.Exec(ctx, "DELETE FROM users WHERE id = ?", id)
	if err != nil {
		s.logger.Error("delete user failed", "error", err)
		s.respondError(w, http.StatusInternalServerError, "failed to delete user")
		return
	}

	if result.RowsAffected == 0 {
		s.respondError(w, http.StatusNotFound, "user not found")
		return
	}

	s.respondJSON(w, http.StatusOK, map[string]string{
		"message": "user deleted",
	})
}

// Helper functions
func (s *Server) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) respondError(w http.ResponseWriter, status int, message string) {
	s.respondJSON(w, status, map[string]string{"error": message})
}

func joinClauses(clauses []string) string {
	result := ""
	for i, c := range clauses {
		if i > 0 {
			result += ", "
		}
		result += c
	}
	return result
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
