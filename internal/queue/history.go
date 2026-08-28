package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Counts returns job counts grouped by state.
func (s *Store) Counts(ctx context.Context) (QueueCounts, error) {
	var counts QueueCounts
	err := s.db.QueryRowContext(ctx, `
SELECT
    COALESCE(SUM(CASE WHEN status = 'QUEUED' THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0),
    COUNT(*)
FROM jobs`).Scan(
		&counts.Queued,
		&counts.Running,
		&counts.Succeeded,
		&counts.Failed,
		&counts.Total,
	)
	if err != nil {
		return QueueCounts{}, fmt.Errorf("queue: count jobs: %w", err)
	}
	return counts, nil
}

// Count returns the number of jobs in status. An empty status counts all jobs.
func (s *Store) Count(ctx context.Context, status Status) (int64, error) {
	query := "SELECT COUNT(*) FROM jobs"
	var args []any
	if status != "" {
		if !validStatus(status) {
			return 0, fmt.Errorf("queue: invalid job status %q", status)
		}
		query += " WHERE status = ?"
		args = append(args, status)
	}
	var count int64
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("queue: count jobs: %w", err)
	}
	return count, nil
}

// ListJobs lists newest jobs first. Limit defaults to 100.
func (s *Store) ListJobs(ctx context.Context, filter JobFilter) ([]Job, error) {
	if filter.Status != "" && !validStatus(filter.Status) {
		return nil, fmt.Errorf("queue: invalid job status %q", filter.Status)
	}
	if filter.Offset < 0 {
		return nil, errors.New("queue: job offset cannot be negative")
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	query := "SELECT " + jobColumns + " FROM jobs"
	var (
		where []string
		args  []any
	)
	if filter.Status != "" {
		where = append(where, "status = ?")
		args = append(args, filter.Status)
	}
	if filter.WatchName != "" {
		where = append(where, "watch_name = ?")
		args = append(args, filter.WatchName)
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY id DESC LIMIT ? OFFSET ?"
	args = append(args, limit, filter.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("queue: list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("queue: scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: list jobs: %w", err)
	}
	return jobs, nil
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(ctx context.Context, id int64) (*Job, error) {
	job, err := scanJob(s.db.QueryRowContext(ctx,
		"SELECT "+jobColumns+" FROM jobs WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("queue: get job %d: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("queue: get job %d: %w", id, err)
	}
	return &job, nil
}

const runColumns = `id, job_id, attempt, status, error, started_at, finished_at`

func scanRun(row scanner) (Run, error) {
	var (
		run        Run
		status     string
		startedAt  int64
		finishedAt sql.NullInt64
	)
	if err := row.Scan(
		&run.ID,
		&run.JobID,
		&run.Attempt,
		&status,
		&run.Error,
		&startedAt,
		&finishedAt,
	); err != nil {
		return Run{}, err
	}
	run.Status = Status(status)
	run.StartedAt = timeFromUnixNano(startedAt)
	run.FinishedAt = nullableTime(finishedAt)
	return run, nil
}

// ListRuns returns a job's attempts in execution order.
func (s *Store) ListRuns(ctx context.Context, jobID int64) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT `+runColumns+`
FROM runs
WHERE job_id = ?
ORDER BY attempt, id`, jobID)
	if err != nil {
		return nil, fmt.Errorf("queue: list runs: %w", err)
	}
	defer rows.Close()

	runs := make([]Run, 0)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("queue: scan run: %w", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: list runs: %w", err)
	}
	return runs, nil
}

// GetRun retrieves a run by ID.
func (s *Store) GetRun(ctx context.Context, id int64) (*Run, error) {
	run, err := scanRun(s.db.QueryRowContext(ctx,
		"SELECT "+runColumns+" FROM runs WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("queue: get run %d: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("queue: get run %d: %w", id, err)
	}
	return &run, nil
}

const commandColumns = `
id, run_id, sequence, name, program, args_json, env_json, working_dir,
timeout_ns, status, exit_code, stdout, stderr, error, started_at, finished_at`

func scanCommand(row scanner) (CommandExecution, error) {
	var (
		command              CommandExecution
		argsJSON, envJSON    string
		timeoutNS, startedAt int64
		status               string
		exitCode, finishedAt sql.NullInt64
	)
	if err := row.Scan(
		&command.ID,
		&command.RunID,
		&command.Sequence,
		&command.Name,
		&command.Program,
		&argsJSON,
		&envJSON,
		&command.WorkingDir,
		&timeoutNS,
		&status,
		&exitCode,
		&command.Stdout,
		&command.Stderr,
		&command.Error,
		&startedAt,
		&finishedAt,
	); err != nil {
		return CommandExecution{}, err
	}
	if err := json.Unmarshal([]byte(argsJSON), &command.Args); err != nil {
		return CommandExecution{}, fmt.Errorf("decode args JSON: %w", err)
	}
	if err := json.Unmarshal([]byte(envJSON), &command.Env); err != nil {
		return CommandExecution{}, fmt.Errorf("decode env JSON: %w", err)
	}
	command.Timeout = time.Duration(timeoutNS)
	command.Status = CommandStatus(status)
	if exitCode.Valid {
		value := int(exitCode.Int64)
		command.ExitCode = &value
	}
	command.StartedAt = timeFromUnixNano(startedAt)
	command.FinishedAt = nullableTime(finishedAt)
	return command, nil
}

// ListCommands returns a run's command executions in configured order.
func (s *Store) ListCommands(ctx context.Context, runID int64) ([]CommandExecution, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT `+commandColumns+`
FROM command_executions
WHERE run_id = ?
ORDER BY sequence, id`, runID)
	if err != nil {
		return nil, fmt.Errorf("queue: list commands: %w", err)
	}
	defer rows.Close()

	commands := make([]CommandExecution, 0)
	for rows.Next() {
		command, err := scanCommand(rows)
		if err != nil {
			return nil, fmt.Errorf("queue: scan command: %w", err)
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: list commands: %w", err)
	}
	return commands, nil
}

// GetCommand retrieves a command execution by ID.
func (s *Store) GetCommand(ctx context.Context, id int64) (*CommandExecution, error) {
	command, err := scanCommand(s.db.QueryRowContext(ctx,
		"SELECT "+commandColumns+" FROM command_executions WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("queue: get command %d: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("queue: get command %d: %w", id, err)
	}
	return &command, nil
}

const commandSummaryColumns = `
id, run_id, sequence, name, program, args_json, working_dir, timeout_ns,
status, exit_code, error, started_at, finished_at,
length(CAST(stdout AS BLOB)), length(CAST(stderr AS BLOB))`

func scanCommandSummary(row scanner) (CommandSummary, error) {
	var (
		command              CommandSummary
		argsJSON             string
		timeoutNS, startedAt int64
		status               string
		exitCode, finishedAt sql.NullInt64
	)
	if err := row.Scan(
		&command.ID,
		&command.RunID,
		&command.Sequence,
		&command.Name,
		&command.Program,
		&argsJSON,
		&command.WorkingDir,
		&timeoutNS,
		&status,
		&exitCode,
		&command.Error,
		&startedAt,
		&finishedAt,
		&command.StdoutBytes,
		&command.StderrBytes,
	); err != nil {
		return CommandSummary{}, err
	}
	if err := json.Unmarshal([]byte(argsJSON), &command.Args); err != nil {
		return CommandSummary{}, fmt.Errorf("decode args JSON: %w", err)
	}
	command.Timeout = time.Duration(timeoutNS)
	command.Status = CommandStatus(status)
	if exitCode.Valid {
		value := int(exitCode.Int64)
		command.ExitCode = &value
	}
	command.StartedAt = timeFromUnixNano(startedAt)
	command.FinishedAt = nullableTime(finishedAt)
	return command, nil
}

// ListCommandSummaries returns a run's command metadata without loading stored
// environment values or captured stdout/stderr.
func (s *Store) ListCommandSummaries(ctx context.Context, runID int64) ([]CommandSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT `+commandSummaryColumns+`
FROM command_executions
WHERE run_id = ?
ORDER BY sequence, id`, runID)
	if err != nil {
		return nil, fmt.Errorf("queue: list command summaries: %w", err)
	}
	defer rows.Close()

	commands := make([]CommandSummary, 0)
	for rows.Next() {
		command, err := scanCommandSummary(rows)
		if err != nil {
			return nil, fmt.Errorf("queue: scan command summary: %w", err)
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: list command summaries: %w", err)
	}
	return commands, nil
}

// GetCommandOutput returns captured stdout/stderr without loading command
// arguments or persisted environment values.
func (s *Store) GetCommandOutput(ctx context.Context, id int64) (*CommandOutput, error) {
	var output CommandOutput
	err := s.db.QueryRowContext(ctx, `
SELECT id, run_id, stdout, stderr
FROM command_executions
WHERE id = ?`, id).Scan(&output.ID, &output.RunID, &output.Stdout, &output.Stderr)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("queue: get command output %d: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("queue: get command output %d: %w", id, err)
	}
	return &output, nil
}

func validStatus(status Status) bool {
	switch status {
	case StatusQueued, StatusRunning, StatusSucceeded, StatusFailed:
		return true
	default:
		return false
	}
}
