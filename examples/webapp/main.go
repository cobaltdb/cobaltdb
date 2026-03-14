// Web App Example - HTML-based application with CobaltDB
package main

import (
	"context"
	"embed"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

//go:embed templates/*
var templatesFS embed.FS

var db *engine.DB
var tmpl *template.Template

type Task struct {
	ID          int64
	Title       string
	Description string
	Status      string
	Priority    int
	CreatedAt   string
}

func main() {
	var err error

	// Open database
	db, err = engine.Open("./tasks.db", &engine.Options{
		CacheSize:  1024,
		WALEnabled: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Init schema
	initSchema()

	// Load templates
	tmpl, err = template.ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		log.Fatal(err)
	}

	// Routes
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/task/new", handleNewTask)
	http.HandleFunc("/task/create", handleCreateTask)
	http.HandleFunc("/task/complete", handleCompleteTask)
	http.HandleFunc("/task/delete", handleDeleteTask)

	// Start server
	go func() {
		log.Println("Server starting on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down...")
}

func initSchema() {
	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE IF NOT EXISTS tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT NOT NULL,
		description TEXT,
		status TEXT DEFAULT 'pending',
		priority INTEGER DEFAULT 1,
		created_at TEXT DEFAULT CURRENT_TIMESTAMP
	)`)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	rows, err := db.Query(ctx, "SELECT id, title, description, status, priority, created_at FROM tasks ORDER BY priority DESC, created_at DESC")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		rows.Scan(&t.ID, &t.Title, &t.Description, &t.Status, &t.Priority, &t.CreatedAt)
		tasks = append(tasks, t)
	}

	tmpl.ExecuteTemplate(w, "index.html", tasks)
}

func handleNewTask(w http.ResponseWriter, r *http.Request) {
	tmpl.ExecuteTemplate(w, "new.html", nil)
}

func handleCreateTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/task/new", 302)
		return
	}

	ctx := context.Background()
	title := r.FormValue("title")
	description := r.FormValue("description")
	priority, _ := strconv.Atoi(r.FormValue("priority"))

	_, err := db.Exec(ctx,
		"INSERT INTO tasks (title, description, priority) VALUES (?, ?, ?)",
		title, description, priority)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	http.Redirect(w, r, "/", 302)
}

func handleCompleteTask(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	ctx := context.Background()

	_, err := db.Exec(ctx,
		"UPDATE tasks SET status = 'completed' WHERE id = ?",
		id)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	http.Redirect(w, r, "/", 302)
}

func handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	ctx := context.Background()

	_, err := db.Exec(ctx, "DELETE FROM tasks WHERE id = ?", id)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	http.Redirect(w, r, "/", 302)
}
