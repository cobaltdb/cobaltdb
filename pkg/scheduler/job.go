package scheduler

import (
	"context"
	"fmt"
	"time"
)

// JobType identifies the kind of scheduled work.
type JobType string

const (
	JobTypeVacuum       JobType = "vacuum"
	JobTypeAnalyze      JobType = "analyze"
	JobTypeBackupClean  JobType = "backup_cleanup"
	JobTypeCustom       JobType = "custom"
)

// JobStatus represents the current state of a job.
type JobStatus string

const (
	JobStatusIdle      JobStatus = "idle"
	JobStatusRunning   JobStatus = "running"
	JobStatusFailed    JobStatus = "failed"
	JobStatusDisabled  JobStatus = "disabled"
)

// JobFunc is the function executed when a job fires.
type JobFunc func(ctx context.Context) error

// Job defines a single scheduled unit of work.
type Job struct {
	ID          string
	Name        string
	Type        JobType
	Interval    time.Duration
	Enabled     bool
	Fn          JobFunc
	LastRun     time.Time
	NextRun     time.Time
	LastError   error
	RunCount    int64
	FailCount   int64
	Status      JobStatus
	MaxRetries  int           // 0 = no retry
	RetryDelay  time.Duration // delay between retries
}

// Validate checks that the job is well-formed.
func (j *Job) Validate() error {
	if j.ID == "" {
		return fmt.Errorf("job ID is required")
	}
	if j.Interval <= 0 {
		return fmt.Errorf("job %s: interval must be positive", j.ID)
	}
	if j.Fn == nil {
		return fmt.Errorf("job %s: function is required", j.ID)
	}
	return nil
}

// JobSnapshot is a read-only view of a job for external reporting.
type JobSnapshot struct {
	ID         string        `json:"id"`
	Name       string        `json:"name"`
	Type       string        `json:"type"`
	Interval   time.Duration `json:"interval_ms"`
	Enabled    bool          `json:"enabled"`
	Status     string        `json:"status"`
	LastRun    time.Time     `json:"last_run"`
	NextRun    time.Time     `json:"next_run"`
	RunCount   int64         `json:"run_count"`
	FailCount  int64         `json:"fail_count"`
	LastError  string        `json:"last_error,omitempty"`
}

// Snapshot returns a snapshot of the job.
func (j *Job) Snapshot() JobSnapshot {
	s := JobSnapshot{
		ID:       j.ID,
		Name:     j.Name,
		Type:     string(j.Type),
		Interval: j.Interval,
		Enabled:  j.Enabled,
		Status:   string(j.Status),
		LastRun:  j.LastRun,
		NextRun:  j.NextRun,
		RunCount: j.RunCount,
		FailCount: j.FailCount,
	}
	if j.LastError != nil {
		s.LastError = j.LastError.Error()
	}
	return s
}
