package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultJobSchedulerConfig(t *testing.T) {
	config := DefaultJobSchedulerConfig()

	if !config.Enabled {
		t.Error("Expected scheduler to be enabled by default")
	}

	if config.PollInterval != 10*time.Second {
		t.Errorf("Expected poll interval 10s, got %v", config.PollInterval)
	}

	if config.MaxConcurrentJobs != 5 {
		t.Errorf("Expected max concurrent jobs 5, got %d", config.MaxConcurrentJobs)
	}
}

func TestJobScheduleIsRecurring(t *testing.T) {
	// Non-recurring schedule
	oneTime := JobSchedule{
		RunAt: &[]time.Time{time.Now().Add(time.Hour)}[0],
	}
	if oneTime.IsRecurring() {
		t.Error("One-time schedule should not be recurring")
	}

	// Recurring with interval
	interval := JobSchedule{
		Interval: time.Hour,
	}
	if !interval.IsRecurring() {
		t.Error("Interval schedule should be recurring")
	}

	// Recurring with cron
	cron := JobSchedule{
		Cron: "@daily",
	}
	if !cron.IsRecurring() {
		t.Error("Cron schedule should be recurring")
	}
}

func TestJobScheduleGetNextRun(t *testing.T) {
	now := time.Now()

	// One-time schedule
	oneTime := JobSchedule{
		RunAt: &[]time.Time{now.Add(time.Hour)}[0],
	}
	next, err := oneTime.GetNextRun(time.Time{})
	if err != nil {
		t.Fatalf("Failed to get next run: %v", err)
	}
	if !next.Equal(*oneTime.RunAt) {
		t.Error("Next run should equal RunAt for one-time job")
	}

	// Interval schedule
	interval := JobSchedule{
		Interval: time.Hour,
	}
	next, err = interval.GetNextRun(now)
	if err != nil {
		t.Fatalf("Failed to get next run: %v", err)
	}
	expected := now.Add(time.Hour)
	if next.Sub(expected) > time.Second {
		t.Errorf("Expected next run at %v, got %v", expected, next)
	}

	// Cron schedule
	cron := JobSchedule{
		Cron: "@hourly",
	}
	next, err = cron.GetNextRun(now)
	if err != nil {
		t.Fatalf("Failed to get next run: %v", err)
	}
	if next.Sub(now.Add(time.Hour)) > time.Second {
		t.Errorf("Expected hourly next run, got %v", next)
	}
}

func TestParseSimpleCron(t *testing.T) {
	now := time.Now()

	tests := []struct {
		cron     string
		expected time.Duration
	}{
		{"@every_minute", time.Minute},
		{"@hourly", time.Hour},
		{"@daily", 24 * time.Hour},
		{"@weekly", 7 * 24 * time.Hour},
	}

	for _, tc := range tests {
		next, err := parseSimpleCron(tc.cron, now)
		if err != nil {
			t.Errorf("Failed to parse %s: %v", tc.cron, err)
			continue
		}
		diff := next.Sub(now)
		if diff != tc.expected {
			t.Errorf("%s: expected %v, got %v", tc.cron, tc.expected, diff)
		}
	}

	// Invalid cron
	_, err := parseSimpleCron("invalid", now)
	if err == nil {
		t.Error("Expected error for invalid cron")
	}
}

func TestJobSchedulerScheduleJob(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	// Valid job
	job := &Job{
		ID:       "test_job_1",
		Name:     "Test Job",
		Type:     JobTypeMaintenance,
		Schedule: JobSchedule{Interval: time.Hour},
	}

	err := scheduler.ScheduleJob(job)
	if err != nil {
		t.Fatalf("Failed to schedule job: %v", err)
	}

	// Job without ID
	invalidJob := &Job{
		Name:     "No ID",
		Schedule: JobSchedule{Interval: time.Hour},
	}
	err = scheduler.ScheduleJob(invalidJob)
	if err == nil {
		t.Error("Expected error for job without ID")
	}

	// Job without name
	invalidJob2 := &Job{
		ID:       "test_2",
		Schedule: JobSchedule{Interval: time.Hour},
	}
	err = scheduler.ScheduleJob(invalidJob2)
	if err == nil {
		t.Error("Expected error for job without name")
	}
}

func TestJobSchedulerGetJob(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	job := &Job{
		ID:       "test_job",
		Name:     "Test Job",
		Type:     JobTypeMaintenance,
		Schedule: JobSchedule{Interval: time.Hour},
	}
	scheduler.ScheduleJob(job)

	// Get existing job
	foundJob, exists := scheduler.GetJob("test_job")
	if !exists {
		t.Error("Expected to find job")
	}
	if foundJob.Name != "Test Job" {
		t.Errorf("Expected name 'Test Job', got '%s'", foundJob.Name)
	}

	// Get non-existent job
	_, exists = scheduler.GetJob("non_existent")
	if exists {
		t.Error("Should not find non-existent job")
	}
}

func TestJobSchedulerListJobs(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	// Empty initially
	jobs := scheduler.ListJobs()
	if len(jobs) != 0 {
		t.Errorf("Expected 0 jobs initially, got %d", len(jobs))
	}

	// Add jobs
	scheduler.ScheduleJob(&Job{
		ID:       "job1",
		Name:     "Job 1",
		Type:     JobTypeMaintenance,
		Schedule: JobSchedule{Interval: time.Hour},
	})
	scheduler.ScheduleJob(&Job{
		ID:       "job2",
		Name:     "Job 2",
		Type:     JobTypeBackup,
		Schedule: JobSchedule{Interval: 2 * time.Hour},
	})

	jobs = scheduler.ListJobs()
	if len(jobs) != 2 {
		t.Errorf("Expected 2 jobs, got %d", len(jobs))
	}
}

func TestJobSchedulerListJobsByType(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	scheduler.ScheduleJob(&Job{
		ID:       "maint1",
		Name:     "Maintenance 1",
		Type:     JobTypeMaintenance,
		Schedule: JobSchedule{Interval: time.Hour},
	})
	scheduler.ScheduleJob(&Job{
		ID:       "backup1",
		Name:     "Backup 1",
		Type:     JobTypeBackup,
		Schedule: JobSchedule{Interval: time.Hour},
	})
	scheduler.ScheduleJob(&Job{
		ID:       "maint2",
		Name:     "Maintenance 2",
		Type:     JobTypeMaintenance,
		Schedule: JobSchedule{Interval: time.Hour},
	})

	maintJobs := scheduler.ListJobsByType(JobTypeMaintenance)
	if len(maintJobs) != 2 {
		t.Errorf("Expected 2 maintenance jobs, got %d", len(maintJobs))
	}

	backupJobs := scheduler.ListJobsByType(JobTypeBackup)
	if len(backupJobs) != 1 {
		t.Errorf("Expected 1 backup job, got %d", len(backupJobs))
	}
}

func TestJobSchedulerDeleteJob(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	scheduler.ScheduleJob(&Job{
		ID:       "job1",
		Name:     "Job 1",
		Schedule: JobSchedule{Interval: time.Hour},
	})

	// Delete existing job
	err := scheduler.DeleteJob("job1")
	if err != nil {
		t.Errorf("Failed to delete job: %v", err)
	}

	_, exists := scheduler.GetJob("job1")
	if exists {
		t.Error("Job should be deleted")
	}

	// Delete non-existent job
	err = scheduler.DeleteJob("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent job")
	}
}

func TestJobSchedulerEnableDisableJob(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	job := &Job{
		ID:       "job1",
		Name:     "Job 1",
		Enabled:  true,
		Schedule: JobSchedule{Interval: time.Hour},
	}
	scheduler.ScheduleJob(job)

	// Disable job
	err := scheduler.DisableJob("job1")
	if err != nil {
		t.Fatalf("Failed to disable job: %v", err)
	}

	foundJob, _ := scheduler.GetJob("job1")
	if foundJob.Enabled {
		t.Error("Job should be disabled")
	}

	// Enable job
	err = scheduler.EnableJob("job1")
	if err != nil {
		t.Fatalf("Failed to enable job: %v", err)
	}

	foundJob, _ = scheduler.GetJob("job1")
	if !foundJob.Enabled {
		t.Error("Job should be enabled")
	}
}

func TestJobSchedulerStartStop(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.PollInterval = 100 * time.Millisecond
	scheduler := NewJobScheduler(config, nil)

	// Start
	scheduler.Start()
	if !scheduler.running.Load() {
		t.Error("Expected scheduler to be running")
	}

	// Let it run briefly
	time.Sleep(150 * time.Millisecond)

	// Stop
	scheduler.Stop()
	if scheduler.running.Load() {
		t.Error("Expected scheduler to be stopped")
	}
}

func TestJobSchedulerScheduleMaintenanceJob(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	handler := func(ctx context.Context) error {
		return nil
	}

	job, err := scheduler.ScheduleMaintenanceJob("maint1", JobSchedule{RunNow: true}, handler)
	if err != nil {
		t.Fatalf("Failed to schedule maintenance job: %v", err)
	}

	if job.Type != JobTypeMaintenance {
		t.Errorf("Expected type maintenance, got %s", job.Type)
	}

	if job.Handler == nil {
		t.Error("Expected handler to be set")
	}
}

func TestJobSchedulerScheduleBackupJob(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	schedule := JobSchedule{Interval: 24 * time.Hour}
	job, err := scheduler.ScheduleBackupJob("daily_backup", schedule)
	if err != nil {
		t.Fatalf("Failed to schedule backup job: %v", err)
	}

	if job.Type != JobTypeBackup {
		t.Errorf("Expected type backup, got %s", job.Type)
	}
}

func TestJobSchedulerScheduleSQLJob(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	schedule := JobSchedule{Interval: time.Hour}
	job, err := scheduler.ScheduleSQLJob("cleanup", "DELETE FROM logs WHERE created < NOW() - INTERVAL '7 days'", schedule)
	if err != nil {
		t.Fatalf("Failed to schedule SQL job: %v", err)
	}

	if job.Type != JobTypeCustom {
		t.Errorf("Expected type custom, got %s", job.Type)
	}

	if job.SQL == "" {
		t.Error("Expected SQL to be set")
	}
}

func TestJobSchedulerGetStats(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	// Empty stats
	stats := scheduler.GetStats()
	if stats.TotalJobs != 0 {
		t.Errorf("Expected 0 total jobs, got %d", stats.TotalJobs)
	}

	// Add jobs
	scheduler.ScheduleJob(&Job{
		ID:       "job1",
		Name:     "Job 1",
		Enabled:  true,
		Schedule: JobSchedule{Interval: time.Hour},
	})
	scheduler.ScheduleJob(&Job{
		ID:       "job2",
		Name:     "Job 2",
		Enabled:  false,
		Schedule: JobSchedule{Interval: time.Hour},
	})

	stats = scheduler.GetStats()
	if stats.TotalJobs != 2 {
		t.Errorf("Expected 2 total jobs, got %d", stats.TotalJobs)
	}
	if stats.EnabledJobs != 1 {
		t.Errorf("Expected 1 enabled job, got %d", stats.EnabledJobs)
	}
}

func TestJobSchedulerJobHistory(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	// Add history entries
	entry1 := JobHistoryEntry{
		JobID:     "job1",
		StartedAt: time.Now(),
		Status:    JobStatusCompleted,
	}
	scheduler.addHistoryEntry(entry1)

	entry2 := JobHistoryEntry{
		JobID:     "job1",
		StartedAt: time.Now(),
		Status:    JobStatusFailed,
		Error:     "test error",
	}
	scheduler.addHistoryEntry(entry2)

	// Get job history
	history := scheduler.GetJobHistory("job1")
	if len(history) != 2 {
		t.Errorf("Expected 2 history entries, got %d", len(history))
	}

	// Get all history
	allHistory := scheduler.GetAllHistory(10)
	if len(allHistory) != 2 {
		t.Errorf("Expected 2 history entries, got %d", len(allHistory))
	}

	// Limit history
	limitedHistory := scheduler.GetAllHistory(1)
	if len(limitedHistory) != 1 {
		t.Errorf("Expected 1 history entry, got %d", len(limitedHistory))
	}
}

func TestJobSchedulerCleanupOldHistory(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	config.HistoryRetention = time.Hour
	scheduler := NewJobScheduler(config, nil)

	// Add old entry
	oldEntry := JobHistoryEntry{
		JobID:     "job1",
		StartedAt: time.Now().Add(-2 * time.Hour),
		Status:    JobStatusCompleted,
	}
	scheduler.addHistoryEntry(oldEntry)

	// Add recent entry
	recentEntry := JobHistoryEntry{
		JobID:     "job2",
		StartedAt: time.Now(),
		Status:    JobStatusCompleted,
	}
	scheduler.addHistoryEntry(recentEntry)

	// Cleanup
	scheduler.cleanupOldHistory()

	// Old entry should be removed
	history := scheduler.GetAllHistory(10)
	if len(history) != 1 {
		t.Errorf("Expected 1 history entry after cleanup, got %d", len(history))
	}

	if history[0].JobID != "job2" {
		t.Error("Recent entry should remain")
	}
}

func TestJobSchedulerRunJobNow(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	job := &Job{
		ID:       "job1",
		Name:     "Job 1",
		Type:     JobTypeMaintenance,
		Schedule: JobSchedule{Interval: time.Hour},
	}
	scheduler.ScheduleJob(job)

	// Trigger immediate execution
	err := scheduler.RunJobNow("job1")
	if err != nil {
		t.Fatalf("Failed to run job: %v", err)
	}

	// Check that NextRun was set to now
	foundJob, _ := scheduler.GetJob("job1")
	foundJob.mu.RLock()
	nextRun := foundJob.NextRun
	foundJob.mu.RUnlock()

	if nextRun == nil {
		t.Error("Expected NextRun to be set")
	}
}

func TestJobSchedulerCancelJob(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	// Try to cancel non-existent job
	err := scheduler.CancelJob("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent job")
	}

	// Add job but don't run it
	scheduler.ScheduleJob(&Job{
		ID:       "job1",
		Name:     "Job 1",
		Schedule: JobSchedule{Interval: time.Hour},
	})

	// Try to cancel non-running job
	err = scheduler.CancelJob("job1")
	if err == nil {
		t.Error("Expected error for non-running job")
	}
}

func TestJobExecution(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	config.DefaultTimeout = 5 * time.Second
	scheduler := NewJobScheduler(config, nil)

	executed := false
	job := &Job{
		ID:       "test_job",
		Name:     "Test Job",
		Type:     JobTypeMaintenance,
		Schedule: JobSchedule{RunNow: true},
		Handler: func(ctx context.Context) error {
			executed = true
			return nil
		},
		Enabled: true,
	}

	err := scheduler.ScheduleJob(job)
	if err != nil {
		t.Fatalf("Failed to schedule job: %v", err)
	}

	// Manually trigger execution
	scheduler.runJob(job)

	if !executed {
		t.Error("Job handler should have been executed")
	}

	// Check job status
	foundJob, _ := scheduler.GetJob("test_job")
	foundJob.mu.RLock()
	status := foundJob.Status
	runCount := foundJob.RunCount
	foundJob.mu.RUnlock()

	if status != JobStatusCompleted {
		t.Errorf("Expected status completed, got %s", status)
	}

	if runCount != 1 {
		t.Errorf("Expected run count 1, got %d", runCount)
	}
}

func TestJobExecutionFailure(t *testing.T) {
	config := DefaultJobSchedulerConfig()
	config.Enabled = false
	scheduler := NewJobScheduler(config, nil)

	job := &Job{
		ID:       "failing_job",
		Name:     "Failing Job",
		Type:     JobTypeMaintenance,
		Schedule: JobSchedule{RunNow: true},
		Handler: func(ctx context.Context) error {
			return errors.New("job failed")
		},
		Enabled: true,
	}

	scheduler.ScheduleJob(job)
	scheduler.runJob(job)

	// Check job status
	foundJob, _ := scheduler.GetJob("failing_job")
	foundJob.mu.RLock()
	status := foundJob.Status
	failCount := foundJob.FailCount
	lastError := foundJob.LastError
	foundJob.mu.RUnlock()

	if status != JobStatusFailed {
		t.Errorf("Expected status failed, got %s", status)
	}

	if failCount != 1 {
		t.Errorf("Expected fail count 1, got %d", failCount)
	}

	if lastError != "job failed" {
		t.Errorf("Expected error 'job failed', got '%s'", lastError)
	}
}
