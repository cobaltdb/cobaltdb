// Worker Example - Background job processing with CobaltDB
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// Job represents a background job
type Job struct {
	ID        int64
	Type      string
	Payload   string
	Status    string
	Retries   int
	CreatedAt time.Time
}

// Worker processes jobs from the queue
type Worker struct {
	db       *engine.DB
	wg       sync.WaitGroup
	quit     chan bool
	interval time.Duration
}

func main() {
	fmt.Println("🔷 CobaltDB Worker Example")
	fmt.Println("===========================")

	// Open database
	db, err := engine.Open("./worker.db", &engine.Options{
		CacheSize:  1024,
		WALEnabled: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Initialize schema
	initSchema(db)

	// Create some sample jobs
	createSampleJobs(db)

	// Start worker
	worker := &Worker{
		db:       db,
		quit:     make(chan bool),
		interval: 5 * time.Second,
	}

	log.Println("Starting worker...")
	worker.Start()

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("\nShutting down worker...")
	worker.Stop()

	log.Println("Worker stopped")
}

func initSchema(db *engine.DB) {
	ctx := context.Background()

	// Jobs table
	db.Exec(ctx, `CREATE TABLE IF NOT EXISTS jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		type TEXT NOT NULL,
		payload TEXT,
		status TEXT DEFAULT 'pending',
		retries INTEGER DEFAULT 0,
		result TEXT,
		created_at TEXT DEFAULT CURRENT_TIMESTAMP,
		processed_at TEXT
	)`)

	// Job history table
	db.Exec(ctx, `CREATE TABLE IF NOT EXISTS job_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id INTEGER,
		action TEXT,
		details TEXT,
		created_at TEXT DEFAULT CURRENT_TIMESTAMP
	)`)
}

func createSampleJobs(db *engine.DB) {
	ctx := context.Background()

	// Check if we already have jobs
	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM jobs")
	var count int
	if rows != nil {
		for rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()
	}

	if count > 0 {
		log.Printf("Found %d existing jobs", count)
		return
	}

	// Create sample jobs
	jobs := []struct {
		Type    string
		Payload string
	}{
		{"email", "user1@example.com"},
		{"email", "user2@example.com"},
		{"report", "daily_sales"},
		{"cleanup", "temp_files"},
		{"export", "user_data"},
	}

	for _, job := range jobs {
		db.Exec(ctx,
			"INSERT INTO jobs (type, payload) VALUES (?, ?)",
			job.Type, job.Payload)
	}

	log.Printf("Created %d sample jobs", len(jobs))
}

// Start starts the worker
func (w *Worker) Start() {
	w.wg.Add(1)
	go w.run()
}

// Stop stops the worker gracefully
func (w *Worker) Stop() {
	close(w.quit)
	w.wg.Wait()
}

func (w *Worker) run() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// Process immediately on start
	w.processJobs()

	for {
		select {
		case <-ticker.C:
			w.processJobs()
		case <-w.quit:
			return
		}
	}
}

func (w *Worker) processJobs() {
	ctx := context.Background()

	// Get pending jobs
	rows, err := w.db.Query(ctx, `
		SELECT id, type, payload, status, retries
		FROM jobs
		WHERE status = 'pending' AND retries < 3
		ORDER BY created_at ASC
		LIMIT 5
	`)
	if err != nil {
		log.Printf("Error fetching jobs: %v", err)
		return
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		rows.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Retries)
		jobs = append(jobs, j)
	}

	if len(jobs) == 0 {
		return
	}

	log.Printf("Processing %d jobs...", len(jobs))

	// Process each job
	for _, job := range jobs {
		w.executeJob(ctx, job)
	}
}

func (w *Worker) executeJob(ctx context.Context, job Job) {
	log.Printf("Executing job %d: %s (%s)", job.ID, job.Type, job.Payload)

	// Mark as processing
	w.db.Exec(ctx, "UPDATE jobs SET status = 'processing' WHERE id = ?", job.ID)

	// Execute based on job type
	var result string
	var err error

	switch job.Type {
	case "email":
		result, err = w.processEmail(job.Payload)
	case "report":
		result, err = w.processReport(job.Payload)
	case "cleanup":
		result, err = w.processCleanup(job.Payload)
	case "export":
		result, err = w.processExport(job.Payload)
	default:
		result = "Unknown job type"
		err = fmt.Errorf("unknown type: %s", job.Type)
	}

	now := time.Now().Format(time.RFC3339)

	if err != nil {
		log.Printf("Job %d failed: %v", job.ID, err)
		w.db.Exec(ctx,
			"UPDATE jobs SET status = 'failed', retries = retries + 1, result = ? WHERE id = ?",
			err.Error(), job.ID)
		w.logHistory(ctx, job.ID, "failed", err.Error())
	} else {
		log.Printf("Job %d completed: %s", job.ID, result)
		w.db.Exec(ctx,
			"UPDATE jobs SET status = 'completed', result = ?, processed_at = ? WHERE id = ?",
			result, now, job.ID)
		w.logHistory(ctx, job.ID, "completed", result)
	}
}

func (w *Worker) logHistory(ctx context.Context, jobID int64, action, details string) {
	w.db.Exec(ctx,
		"INSERT INTO job_history (job_id, action, details) VALUES (?, ?, ?)",
		jobID, action, details)
}

// Job processors
func (w *Worker) processEmail(email string) (string, error) {
	// Simulate email processing
	time.Sleep(500 * time.Millisecond)
	return fmt.Sprintf("Email sent to %s", email), nil
}

func (w *Worker) processReport(reportType string) (string, error) {
	// Simulate report generation
	time.Sleep(1 * time.Second)
	return fmt.Sprintf("Generated %s report", reportType), nil
}

func (w *Worker) processCleanup(target string) (string, error) {
	// Simulate cleanup
	time.Sleep(300 * time.Millisecond)
	return fmt.Sprintf("Cleaned up %s", target), nil
}

func (w *Worker) processExport(dataType string) (string, error) {
	// Simulate data export
	time.Sleep(800 * time.Millisecond)
	return fmt.Sprintf("Exported %s", dataType), nil
}
