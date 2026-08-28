package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const jobColumns = `
id, watch_name, path, fingerprint, status, attempts, max_retries,
available_at, last_error, created_at, updated_at, started_at, finished_at`

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (Job, error) {
	var (
		job                               Job
		status                            string
		availableAt, createdAt, updatedAt int64
		startedAt, finishedAt             sql.NullInt64
	)
	if err := row.Scan(
		&job.ID,
		&job.WatchName,
		&job.Path,
		&job.Fingerprint,
		&status,
		&job.Attempts,
		&job.MaxRetries,
		&availableAt,
		&job.LastError,
		&createdAt,
		&updatedAt,
		&startedAt,
		&finishedAt,
	); err != nil {
		return Job{}, err
	}
	job.Status = Status(status)
	job.AvailableAt = timeFromUnixNano(availableAt)
	job.CreatedAt = timeFromUnixNano(createdAt)
	job.UpdatedAt = timeFromUnixNano(updatedAt)
	job.StartedAt = nullableTime(startedAt)
	job.FinishedAt = nullableTime(finishedAt)
	return job, nil
}

// Enqueue inserts a discovered file unless the same watch, path, and
// fingerprint was already recorded. The existing Job is returned on a
// duplicate, making discovery safely repeatable across daemon restarts.
func (s *Store) Enqueue(ctx context.Context, p EnqueueParams) (Job, bool, error) {
	if strings.TrimSpace(p.WatchName) == "" {
		return Job{}, false, errors.New("queue: watch name is empty")
	}
	if strings.TrimSpace(p.Path) == "" {
		return Job{}, false, errors.New("queue: file path is empty")
	}
	if p.MaxRetries < 0 {
		return Job{}, false, errors.New("queue: max retries cannot be negative")
	}

	now := s.timestamp()
	availableAt := p.AvailableAt
	if availableAt.IsZero() {
		availableAt = now
	}
	row := s.db.QueryRowContext(ctx, `
INSERT INTO jobs (
    watch_name, path, fingerprint, status, attempts, max_retries,
    available_at, created_at, updated_at
) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)
ON CONFLICT (watch_name, path, fingerprint) DO NOTHING
RETURNING `+jobColumns,
		p.WatchName,
		p.Path,
		p.Fingerprint,
		StatusQueued,
		p.MaxRetries,
		unixNano(availableAt),
		unixNano(now),
		unixNano(now),
	)
	job, err := scanJob(row)
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, fmt.Errorf("queue: enqueue job: %w", err)
	}

	job, err = scanJob(s.db.QueryRowContext(ctx, `
SELECT `+jobColumns+`
FROM jobs
WHERE watch_name = ? AND path = ? AND fingerprint = ?`,
		p.WatchName,
		p.Path,
		p.Fingerprint,
	))
	if err != nil {
		return Job{}, false, fmt.Errorf("queue: find duplicate job: %w", err)
	}
	return job, false, nil
}

// Claim atomically reserves the oldest eligible queued job and creates its Run
// record in the same transaction. ErrNoJob is returned when none is available.
func (s *Store) Claim(ctx context.Context) (*Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("queue: begin claim: %w", err)
	}
	defer rollback(tx)

	now := s.timestamp()
	job, err := scanJob(tx.QueryRowContext(ctx, `
UPDATE jobs
SET status = ?,
    attempts = attempts + 1,
    updated_at = ?,
    started_at = ?,
    finished_at = NULL
WHERE id = (
    SELECT id
    FROM jobs
    WHERE status = ? AND available_at <= ?
    ORDER BY available_at, id
    LIMIT 1
)
RETURNING `+jobColumns,
		StatusRunning,
		unixNano(now),
		unixNano(now),
		StatusQueued,
		unixNano(now),
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoJob
	}
	if err != nil {
		return nil, fmt.Errorf("queue: claim job: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
INSERT INTO runs (job_id, attempt, status, started_at)
VALUES (?, ?, ?, ?)`, job.ID, job.Attempts, StatusRunning, unixNano(now))
	if err != nil {
		return nil, fmt.Errorf("queue: create run: %w", err)
	}
	runID, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("queue: get run id: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("queue: commit claim: %w", err)
	}
	job.RunID = runID
	job.Attempt = job.Attempts
	return &job, nil
}

// Succeed atomically completes a running run and its job.
func (s *Store) Succeed(ctx context.Context, jobID, runID int64) error {
	if jobID <= 0 || runID <= 0 {
		return errors.New("queue: invalid job or run id")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("queue: begin success: %w", err)
	}
	defer rollback(tx)

	now := s.timestamp()
	result, err := tx.ExecContext(ctx, `
UPDATE runs
SET status = ?, error = '', finished_at = ?
WHERE id = ? AND job_id = ? AND status = ?`,
		StatusSucceeded, unixNano(now), runID, jobID, StatusRunning)
	if err != nil {
		return fmt.Errorf("queue: complete run: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return fmt.Errorf("queue: complete run: %w", err)
	}

	result, err = tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, last_error = '', updated_at = ?, finished_at = ?
WHERE id = ? AND status = ?`,
		StatusSucceeded, unixNano(now), unixNano(now), jobID, StatusRunning)
	if err != nil {
		return fmt.Errorf("queue: complete job: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return fmt.Errorf("queue: complete job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("queue: commit success: %w", err)
	}
	return nil
}

// Fail completes a run. If the job still has retries, it becomes QUEUED with
// a persisted AvailableAt; otherwise it becomes terminally FAILED. MaxRetries
// counts retries after the initial attempt.
func (s *Store) Fail(
	ctx context.Context,
	jobID, runID int64,
	reason string,
	retryDelay time.Duration,
) (Status, error) {
	if jobID <= 0 || runID <= 0 {
		return "", errors.New("queue: invalid job or run id")
	}
	if retryDelay < 0 {
		retryDelay = 0
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("queue: begin failure: %w", err)
	}
	defer rollback(tx)

	now := s.timestamp()
	if _, err := tx.ExecContext(ctx, `
UPDATE command_executions
SET status = ?, exit_code = -1, error = ?, finished_at = ?
WHERE run_id = ? AND status = ?`,
		CommandFailed, reason, unixNano(now), runID, CommandRunning); err != nil {
		return "", fmt.Errorf("queue: fail running commands: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE runs
SET status = ?, error = ?, finished_at = ?
WHERE id = ? AND job_id = ? AND status = ?`,
		StatusFailed, reason, unixNano(now), runID, jobID, StatusRunning)
	if err != nil {
		return "", fmt.Errorf("queue: fail run: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return "", fmt.Errorf("queue: fail run: %w", err)
	}

	var attempts, maxRetries int
	if err := tx.QueryRowContext(ctx, `
SELECT attempts, max_retries
FROM jobs
WHERE id = ? AND status = ?`, jobID, StatusRunning).Scan(&attempts, &maxRetries); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("queue: fail job: %w", ErrInvalidTransition)
		}
		return "", fmt.Errorf("queue: read retry state: %w", err)
	}

	resultingStatus := StatusFailed
	availableAt := now
	finishedAt := any(unixNano(now))
	if attempts <= maxRetries {
		resultingStatus = StatusQueued
		availableAt = now.Add(retryDelay)
		finishedAt = nil
	}
	result, err = tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, available_at = ?, last_error = ?, updated_at = ?, finished_at = ?
WHERE id = ? AND status = ?`,
		resultingStatus,
		unixNano(availableAt),
		reason,
		unixNano(now),
		finishedAt,
		jobID,
		StatusRunning,
	)
	if err != nil {
		return "", fmt.Errorf("queue: fail job: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return "", fmt.Errorf("queue: fail job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("queue: commit failure: %w", err)
	}
	return resultingStatus, nil
}

// RecoverRunning marks interrupted history as failed and immediately requeues
// every RUNNING job. Recovery intentionally does not apply max_retries: a
// process crash is ambiguous, so re-execution preserves at-least-once delivery.
func (s *Store) RecoverRunning(ctx context.Context) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("queue: begin recovery: %w", err)
	}
	defer rollback(tx)

	now := s.timestamp()
	const reason = "interrupted by daemon restart"
	if _, err := tx.ExecContext(ctx, `
UPDATE command_executions
SET status = ?, exit_code = -1, error = ?, finished_at = ?
WHERE status = ?`, CommandFailed, reason, unixNano(now), CommandRunning); err != nil {
		return 0, fmt.Errorf("queue: recover commands: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runs
SET status = ?, error = ?, finished_at = ?
WHERE status = ?`, StatusFailed, reason, unixNano(now), StatusRunning); err != nil {
		return 0, fmt.Errorf("queue: recover runs: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, available_at = ?, last_error = ?, updated_at = ?, finished_at = NULL
WHERE status = ?`, StatusQueued, unixNano(now), reason, unixNano(now), StatusRunning)
	if err != nil {
		return 0, fmt.Errorf("queue: recover jobs: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("queue: count recovered jobs: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("queue: commit recovery: %w", err)
	}
	return count, nil
}

func requireOneRow(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrInvalidTransition
	}
	return nil
}
