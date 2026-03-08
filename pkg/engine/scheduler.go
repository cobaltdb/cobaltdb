package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// JobType represents the type of scheduled job
type JobType string

const (
	JobTypeMaintenance JobType = "maintenance"
	JobTypeBackup      JobType = "backup"
	JobTypeAnalyze     JobType = "analyze"
	JobTypeVacuum      JobType = "vacuum"
	JobTypeCustom      JobType = "custom"
	JobTypeOneTime     JobType = "one_time"
)

// JobStatus represents the status of a job
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
)

// JobSchedule defines when a job should run
type JobSchedule struct {
	// Cron expression (e.g., "0 0 * * *" for daily at midnight)
	Cron string

	// One-time execution
	RunAt *time.Time

	// Recurring with interval
	Interval time.Duration

	// Run immediately when scheduled
	RunNow bool
}

// IsRecurring returns true if the schedule is recurring
func (js *JobSchedule) IsRecurring() bool {
	return js.Cron != "" || js.Interval > 0
}

// GetNextRun calculates the next run time
func (js *JobSchedule) GetNextRun(lastRun time.Time) (time.Time, error) {
	if js.RunAt != nil && lastRun.IsZero() {
		return *js.RunAt, nil
	}

	if js.Interval > 0 {
		return lastRun.Add(js.Interval), nil
	}

	if js.Cron != "" {
		// Simple cron parsing for common patterns
		next, err := parseSimpleCron(js.Cron, lastRun)
		if err != nil {
			return time.Time{}, err
		}
		return next, nil
	}

	return time.Time{}, errors.New("no valid schedule")
}

// parseSimpleCron parses simple cron expressions
func parseSimpleCron(cron string, lastRun time.Time) (time.Time, error) {
	// Support simple patterns only
	switch cron {
	case "@every_minute":
		return lastRun.Add(time.Minute), nil
	case "@hourly":
		return lastRun.Add(time.Hour), nil
	case "@daily":
		return lastRun.Add(24 * time.Hour), nil
	case "@weekly":
		return lastRun.Add(7 * 24 * time.Hour), nil
	case "@monthly":
		return lastRun.AddDate(0, 1, 0), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported cron expression: %s", cron)
	}
}

// Job represents a scheduled job
type Job struct {
	ID          string
	Name        string
	Description string
	Type        JobType
	Schedule    JobSchedule
	SQL         string                      // SQL to execute for custom jobs
	Handler     func(context.Context) error // Custom handler
	Enabled     bool
	MaxRetries  int
	Timeout     time.Duration
	CreatedAt   time.Time
	UpdatedAt   time.Time

	// Runtime state (not persisted)
	mu         sync.RWMutex
	Status     JobStatus
	LastRun    *time.Time
	NextRun    *time.Time
	RunCount   uint64
	FailCount  uint64
	LastError  string
	CancelFunc context.CancelFunc
}

// JobHistoryEntry represents a single job execution
type JobHistoryEntry struct {
	JobID       string
	ExecutionID string
	StartedAt   time.Time
	CompletedAt *time.Time
	Status      JobStatus
	Error       string
	Output      string
}

// JobSchedulerConfig configures the job scheduler
type JobSchedulerConfig struct {
	Enabled           bool
	PollInterval      time.Duration
	MaxConcurrentJobs int
	HistoryRetention  time.Duration
	DefaultTimeout    time.Duration
	DefaultMaxRetries int
}

// DefaultJobSchedulerConfig returns default configuration
func DefaultJobSchedulerConfig() *JobSchedulerConfig {
	return &JobSchedulerConfig{
		Enabled:           true,
		PollInterval:      10 * time.Second,
		MaxConcurrentJobs: 5,
		HistoryRetention:  7 * 24 * time.Hour,
		DefaultTimeout:    1 * time.Hour,
		DefaultMaxRetries: 3,
	}
}

// JobScheduler manages scheduled jobs
type JobScheduler struct {
	config    *JobSchedulerConfig
	jobs      map[string]*Job
	history   []JobHistoryEntry
	executing map[string]bool
	mu        sync.RWMutex
	running   atomic.Bool
	stopCh    chan struct{}
	wg        sync.WaitGroup
	db        *DB // Reference to database for SQL execution
}

// NewJobScheduler creates a new job scheduler
func NewJobScheduler(config *JobSchedulerConfig, db *DB) *JobScheduler {
	if config == nil {
		config = DefaultJobSchedulerConfig()
	}

	return &JobScheduler{
		config:    config,
		jobs:      make(map[string]*Job),
		history:   make([]JobHistoryEntry, 0),
		executing: make(map[string]bool),
		stopCh:    make(chan struct{}),
		db:        db,
	}
}

// Start starts the job scheduler
func (js *JobScheduler) Start() {
	if !js.config.Enabled {
		return
	}

	if js.running.CompareAndSwap(false, true) {
		js.wg.Add(1)
		go js.schedulerLoop()
		logger.Default().Info("Job scheduler started")
	}
}

// Stop stops the job scheduler
func (js *JobScheduler) Stop() {
	if js.running.CompareAndSwap(true, false) {
		close(js.stopCh)
		js.wg.Wait()
		logger.Default().Info("Job scheduler stopped")
	}
}

// schedulerLoop runs the main scheduling loop
func (js *JobScheduler) schedulerLoop() {
	defer js.wg.Done()

	ticker := time.NewTicker(js.config.PollInterval)
	defer ticker.Stop()

	// Run immediately on start
	js.checkScheduledJobs()

	for {
		select {
		case <-js.stopCh:
			return
		case <-ticker.C:
			js.checkScheduledJobs()
			js.cleanupOldHistory()
		}
	}
}

// checkScheduledJobs checks for jobs that need to run
func (js *JobScheduler) checkScheduledJobs() {
	js.mu.RLock()
	jobs := make([]*Job, 0, len(js.jobs))
	for _, job := range js.jobs {
		jobs = append(jobs, job)
	}
	js.mu.RUnlock()

	now := time.Now()
	for _, job := range jobs {
		if !job.Enabled {
			continue
		}

		job.mu.RLock()
		status := job.Status
		nextRun := job.NextRun
		job.mu.RUnlock()

		if status == JobStatusRunning {
			continue
		}

		shouldRun := false
		if nextRun != nil && now.After(*nextRun) {
			shouldRun = true
		}

		// Check for first run
		if nextRun == nil && job.Schedule.RunAt != nil && now.After(*job.Schedule.RunAt) {
			shouldRun = true
		}

		if shouldRun {
			js.executeJob(job)
		}
	}
}

// executeJob executes a job
func (js *JobScheduler) executeJob(job *Job) {
	js.mu.Lock()
	if len(js.executing) >= js.config.MaxConcurrentJobs {
		js.mu.Unlock()
		logger.Default().Warnf("Max concurrent jobs reached, skipping %s", job.Name)
		return
	}
	js.executing[job.ID] = true
	js.mu.Unlock()

	js.wg.Add(1)
	go func() {
		defer js.wg.Done()
		defer func() {
			js.mu.Lock()
			delete(js.executing, job.ID)
			js.mu.Unlock()
		}()

		js.runJob(job)
	}()
}

// runJob runs a single job execution
func (js *JobScheduler) runJob(job *Job) {
	executionID := generateExecutionID()

	job.mu.Lock()
	job.Status = JobStatusRunning
	now := time.Now()
	job.LastRun = &now
	ctx, cancel := context.WithTimeout(context.Background(), job.Timeout)
	if job.Timeout == 0 {
		ctx, cancel = context.WithTimeout(ctx, js.config.DefaultTimeout)
	}
	job.CancelFunc = cancel
	job.mu.Unlock()

	historyEntry := JobHistoryEntry{
		JobID:       job.ID,
		ExecutionID: executionID,
		StartedAt:   now,
		Status:      JobStatusRunning,
	}

	js.addHistoryEntry(historyEntry)

	var err error
	var output string

	// Execute the job
	switch job.Type {
	case JobTypeCustom:
		if job.SQL != "" && js.db != nil {
			_, err = js.db.Exec(ctx, job.SQL)
		}
		if job.Handler != nil {
			err = job.Handler(ctx)
		}
	case JobTypeMaintenance, JobTypeBackup, JobTypeAnalyze, JobTypeVacuum:
		if job.Handler != nil {
			err = job.Handler(ctx)
		}
	case JobTypeOneTime:
		if job.Handler != nil {
			err = job.Handler(ctx)
		}
	}

	completedAt := time.Now()

	job.mu.Lock()
	job.CancelFunc = nil
	if err != nil {
		job.Status = JobStatusFailed
		job.FailCount++
		job.LastError = err.Error()
		historyEntry.Status = JobStatusFailed
		historyEntry.Error = err.Error()
		logger.Default().Errorf("Job %s failed: %v", job.Name, err)
	} else {
		job.Status = JobStatusCompleted
		job.RunCount++
		job.LastError = ""
		historyEntry.Status = JobStatusCompleted
		historyEntry.Output = output
	}
	job.mu.Unlock()

	historyEntry.CompletedAt = &completedAt
	js.updateHistoryEntry(historyEntry)

	// Schedule next run for recurring jobs
	if job.Schedule.IsRecurring() && err == nil {
		nextRun, scheduleErr := job.Schedule.GetNextRun(completedAt)
		if scheduleErr == nil {
			job.mu.Lock()
			job.NextRun = &nextRun
			job.mu.Unlock()
		}
	}
}

// ScheduleJob schedules a new job
func (js *JobScheduler) ScheduleJob(job *Job) error {
	if job.ID == "" {
		return errors.New("job ID is required")
	}

	if job.Name == "" {
		return errors.New("job name is required")
	}

	if job.MaxRetries == 0 {
		job.MaxRetries = js.config.DefaultMaxRetries
	}

	if job.Timeout == 0 {
		job.Timeout = js.config.DefaultTimeout
	}

	if job.Schedule.RunNow {
		now := time.Now()
		job.NextRun = &now
	} else if job.Schedule.RunAt != nil {
		job.NextRun = job.Schedule.RunAt
	} else if job.Schedule.Interval > 0 || job.Schedule.Cron != "" {
		nextRun, err := job.Schedule.GetNextRun(time.Now())
		if err != nil {
			return fmt.Errorf("invalid schedule: %w", err)
		}
		job.NextRun = &nextRun
	}

	job.Status = JobStatusPending
	job.CreatedAt = time.Now()
	job.UpdatedAt = time.Now()

	js.mu.Lock()
	js.jobs[job.ID] = job
	js.mu.Unlock()

	logger.Default().Infof("Job scheduled: %s", job.Name)
	return nil
}

// CancelJob cancels a running job
func (js *JobScheduler) CancelJob(jobID string) error {
	js.mu.RLock()
	job, exists := js.jobs[jobID]
	js.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	if job.Status != JobStatusRunning {
		return fmt.Errorf("job is not running: %s", jobID)
	}

	if job.CancelFunc != nil {
		job.CancelFunc()
		job.Status = JobStatusCancelled
		logger.Default().Infof("Job cancelled: %s", job.Name)
	}

	return nil
}

// DeleteJob removes a job
func (js *JobScheduler) DeleteJob(jobID string) error {
	js.mu.Lock()
	defer js.mu.Unlock()

	job, exists := js.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	// Cancel if running
	job.mu.Lock()
	if job.Status == JobStatusRunning && job.CancelFunc != nil {
		job.CancelFunc()
	}
	job.mu.Unlock()

	delete(js.jobs, jobID)
	logger.Default().Infof("Job deleted: %s", job.Name)
	return nil
}

// EnableJob enables a job
func (js *JobScheduler) EnableJob(jobID string) error {
	js.mu.Lock()
	defer js.mu.Unlock()

	job, exists := js.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	job.Enabled = true
	job.UpdatedAt = time.Now()
	return nil
}

// DisableJob disables a job
func (js *JobScheduler) DisableJob(jobID string) error {
	js.mu.Lock()
	defer js.mu.Unlock()

	job, exists := js.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	job.Enabled = false
	job.UpdatedAt = time.Now()
	return nil
}

// GetJob returns a job by ID
func (js *JobScheduler) GetJob(jobID string) (*Job, bool) {
	js.mu.RLock()
	defer js.mu.RUnlock()
	job, exists := js.jobs[jobID]
	return job, exists
}

// ListJobs returns all jobs
func (js *JobScheduler) ListJobs() []*Job {
	js.mu.RLock()
	defer js.mu.RUnlock()

	jobs := make([]*Job, 0, len(js.jobs))
	for _, job := range js.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// ListJobsByType returns jobs filtered by type
func (js *JobScheduler) ListJobsByType(jobType JobType) []*Job {
	js.mu.RLock()
	defer js.mu.RUnlock()

	jobs := make([]*Job, 0)
	for _, job := range js.jobs {
		if job.Type == jobType {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

// GetJobHistory returns execution history for a job
func (js *JobScheduler) GetJobHistory(jobID string) []JobHistoryEntry {
	js.mu.RLock()
	defer js.mu.RUnlock()

	history := make([]JobHistoryEntry, 0)
	for _, entry := range js.history {
		if entry.JobID == jobID {
			history = append(history, entry)
		}
	}
	return history
}

// GetAllHistory returns all job execution history
func (js *JobScheduler) GetAllHistory(limit int) []JobHistoryEntry {
	js.mu.RLock()
	defer js.mu.RUnlock()

	if limit <= 0 || limit > len(js.history) {
		limit = len(js.history)
	}

	// Return most recent first
	result := make([]JobHistoryEntry, limit)
	for i := 0; i < limit; i++ {
		result[i] = js.history[len(js.history)-1-i]
	}
	return result
}

// addHistoryEntry adds a history entry
func (js *JobScheduler) addHistoryEntry(entry JobHistoryEntry) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.history = append(js.history, entry)
}

// updateHistoryEntry updates a history entry
func (js *JobScheduler) updateHistoryEntry(updated JobHistoryEntry) {
	js.mu.Lock()
	defer js.mu.Unlock()

	for i, entry := range js.history {
		if entry.ExecutionID == updated.ExecutionID {
			js.history[i] = updated
			return
		}
	}
}

// cleanupOldHistory removes old history entries
func (js *JobScheduler) cleanupOldHistory() {
	if js.config.HistoryRetention <= 0 {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	cutoff := time.Now().Add(-js.config.HistoryRetention)
	newHistory := make([]JobHistoryEntry, 0, len(js.history))

	for _, entry := range js.history {
		if entry.StartedAt.After(cutoff) {
			newHistory = append(newHistory, entry)
		}
	}

	js.history = newHistory
}

// GetStats returns scheduler statistics
func (js *JobScheduler) GetStats() SchedulerStats {
	js.mu.RLock()
	defer js.mu.RUnlock()

	stats := SchedulerStats{
		TotalJobs:     len(js.jobs),
		HistorySize:   len(js.history),
		ExecutingJobs: len(js.executing),
	}

	for _, job := range js.jobs {
		job.mu.RLock()
		switch job.Status {
		case JobStatusPending:
			stats.PendingJobs++
		case JobStatusRunning:
			stats.RunningJobs++
		case JobStatusFailed:
			stats.FailedJobs++
		}
		if job.Enabled {
			stats.EnabledJobs++
		}
		job.mu.RUnlock()
	}

	return stats
}

// SchedulerStats contains scheduler statistics
type SchedulerStats struct {
	TotalJobs     int
	EnabledJobs   int
	PendingJobs   int
	RunningJobs   int
	FailedJobs    int
	HistorySize   int
	ExecutingJobs int
}

// generateExecutionID generates a unique execution ID
func generateExecutionID() string {
	return fmt.Sprintf("exec_%d", time.Now().UnixNano())
}

// ScheduleMaintenanceJob creates a maintenance job
func (js *JobScheduler) ScheduleMaintenanceJob(name string, schedule JobSchedule, handler func(context.Context) error) (*Job, error) {
	job := &Job{
		ID:          fmt.Sprintf("maint_%s_%d", name, time.Now().Unix()),
		Name:        name,
		Type:        JobTypeMaintenance,
		Schedule:    schedule,
		Handler:     handler,
		Enabled:     true,
		Description: "Maintenance job",
	}

	if err := js.ScheduleJob(job); err != nil {
		return nil, err
	}

	return job, nil
}

// ScheduleBackupJob creates a backup job
func (js *JobScheduler) ScheduleBackupJob(name string, schedule JobSchedule) (*Job, error) {
	job := &Job{
		ID:          fmt.Sprintf("backup_%s_%d", name, time.Now().Unix()),
		Name:        name,
		Type:        JobTypeBackup,
		Schedule:    schedule,
		Enabled:     true,
		Description: "Backup job",
	}

	if err := js.ScheduleJob(job); err != nil {
		return nil, err
	}

	return job, nil
}

// ScheduleSQLJob creates a SQL execution job
func (js *JobScheduler) ScheduleSQLJob(name string, sql string, schedule JobSchedule) (*Job, error) {
	job := &Job{
		ID:          fmt.Sprintf("sql_%s_%d", name, time.Now().Unix()),
		Name:        name,
		Type:        JobTypeCustom,
		Schedule:    schedule,
		SQL:         sql,
		Enabled:     true,
		Description: "SQL execution job",
	}

	if err := js.ScheduleJob(job); err != nil {
		return nil, err
	}

	return job, nil
}

// RunJobNow triggers a job to run immediately
func (js *JobScheduler) RunJobNow(jobID string) error {
	js.mu.RLock()
	job, exists := js.jobs[jobID]
	js.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	// Update next run to now
	now := time.Now()
	job.mu.Lock()
	job.NextRun = &now
	job.mu.Unlock()

	// Trigger immediate check
	js.executeJob(job)

	return nil
}
