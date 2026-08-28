// Package queue implements slipway's durable SQLite-backed job queue.
package queue

import (
	"errors"
	"time"
)

// Status is the durable state of a job or run.
type Status string

const (
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
)

// CommandStatus is the durable state of a command execution.
type CommandStatus string

const (
	CommandRunning   CommandStatus = "RUNNING"
	CommandSucceeded CommandStatus = "SUCCEEDED"
	CommandFailed    CommandStatus = "FAILED"
)

var (
	// ErrNoJob means no queued job is currently eligible to run.
	ErrNoJob = errors.New("no job available")
	// ErrNotFound means the requested history record does not exist.
	ErrNotFound = errors.New("queue record not found")
	// ErrInvalidTransition means a stale or already-completed record was updated.
	ErrInvalidTransition = errors.New("invalid queue state transition")
)

// Job is a discovered file and its durable execution state. RunID and Attempt
// are populated by Claim and identify the run reserved for the caller.
type Job struct {
	ID          int64
	WatchName   string
	Path        string
	Fingerprint string
	Status      Status
	Attempts    int
	MaxRetries  int
	AvailableAt time.Time
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time

	RunID   int64
	Attempt int
}

// EnqueueParams describes a file to insert into the queue. AvailableAt defaults
// to the current time. The tuple (WatchName, Path, Fingerprint) is idempotent.
type EnqueueParams struct {
	WatchName   string
	Path        string
	Fingerprint string
	MaxRetries  int
	AvailableAt time.Time
}

// Run records one claimed execution attempt for a job.
type Run struct {
	ID         int64
	JobID      int64
	Attempt    int
	Status     Status
	Error      string
	StartedAt  time.Time
	FinishedAt *time.Time
}

// CommandStart is the immutable command metadata recorded immediately before
// execution. Args and Env are JSON encoded in SQLite without interpretation.
type CommandStart struct {
	RunID      int64
	Sequence   int
	Name       string
	Program    string
	Args       []string
	Env        []string
	WorkingDir string
	Timeout    time.Duration
}

// CommandResult is the output recorded after a command exits.
type CommandResult struct {
	Status   CommandStatus
	ExitCode int
	Stdout   string
	Stderr   string
	Error    string
}

// CommandExecution records a single command within a run.
type CommandExecution struct {
	ID         int64
	RunID      int64
	Sequence   int
	Name       string
	Program    string
	Args       []string
	Env        []string
	WorkingDir string
	Timeout    time.Duration
	Status     CommandStatus
	ExitCode   *int
	Stdout     string
	Stderr     string
	Error      string
	StartedAt  time.Time
	FinishedAt *time.Time
}

// CommandSummary contains command history metadata without persisted
// environment values or captured output. It is suitable for list/detail views
// that load potentially large output only when requested.
type CommandSummary struct {
	ID          int64
	RunID       int64
	Sequence    int
	Name        string
	Program     string
	Args        []string
	WorkingDir  string
	Timeout     time.Duration
	Status      CommandStatus
	ExitCode    *int
	Error       string
	StartedAt   time.Time
	FinishedAt  *time.Time
	StdoutBytes int64
	StderrBytes int64
}

// CommandOutput is the lazily loaded captured output for one command.
type CommandOutput struct {
	ID     int64
	RunID  int64
	Stdout string
	Stderr string
}

// JobFilter controls ListJobs. A zero Limit uses a sensible default.
type JobFilter struct {
	Status    Status
	WatchName string
	Limit     int
	Offset    int
}

// QueueCounts is a point-in-time count of jobs by state.
type QueueCounts struct {
	Queued    int64
	Running   int64
	Succeeded int64
	Failed    int64
	Total     int64
}
