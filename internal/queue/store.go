package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_name    TEXT NOT NULL,
    path          TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    attempts      INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_retries   INTEGER NOT NULL CHECK (max_retries >= 0),
    available_at  INTEGER NOT NULL,
    last_error    TEXT NOT NULL DEFAULT '',
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    started_at    INTEGER,
    finished_at   INTEGER,
    UNIQUE (watch_name, path, fingerprint)
);

CREATE INDEX IF NOT EXISTS jobs_claim_idx
    ON jobs (status, available_at, id);
CREATE INDEX IF NOT EXISTS jobs_watch_idx
    ON jobs (watch_name, id);

CREATE TABLE IF NOT EXISTS runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id        INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt       INTEGER NOT NULL CHECK (attempt > 0),
    status        TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    error         TEXT NOT NULL DEFAULT '',
    started_at    INTEGER NOT NULL,
    finished_at   INTEGER,
    UNIQUE (job_id, attempt)
);

CREATE INDEX IF NOT EXISTS runs_job_idx ON runs (job_id, attempt);

CREATE TABLE IF NOT EXISTS command_executions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    sequence      INTEGER NOT NULL CHECK (sequence >= 0),
    name          TEXT NOT NULL,
    program       TEXT NOT NULL,
    args_json     TEXT NOT NULL,
    env_json      TEXT NOT NULL,
    working_dir   TEXT NOT NULL,
    timeout_ns    INTEGER NOT NULL CHECK (timeout_ns >= 0),
    status        TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    exit_code     INTEGER,
    stdout        TEXT NOT NULL DEFAULT '',
    stderr        TEXT NOT NULL DEFAULT '',
    error         TEXT NOT NULL DEFAULT '',
    started_at    INTEGER NOT NULL,
    finished_at   INTEGER,
    UNIQUE (run_id, sequence)
);

CREATE INDEX IF NOT EXISTS command_executions_run_idx
    ON command_executions (run_id, sequence);
`

// SQLite primary result codes occupy the low byte of extended result codes.
const sqliteCantOpen = 14

// Store owns a SQLite connection pool and the clock used for persisted times.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

// Option customizes a Store.
type Option func(*Store)

// WithClock replaces the wall clock. It is primarily useful for deterministic
// tests and simulations.
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// Open opens a queue database and creates its schema when necessary.
func Open(path string, opts ...Option) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("queue: database path is empty")
	}
	if err := validateDatabaseLocation(path); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, databaseOpenError(path, err)
	}

	// One connection keeps connection-local PRAGMAs deterministic. SQLite still
	// coordinates safely with CLI or daemon processes using WAL and busy_timeout.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	s := &Store{db: db, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, databaseOpenError(path, err)
	}
	for _, pragma := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("queue: configure database %q: %w", path, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("queue: initialize database %q schema: %w", path, err)
	}

	return s, nil
}

// validateDatabaseLocation catches filesystem conditions that SQLite's open
// error cannot describe reliably. In particular, modernc SQLite can append an
// unrelated "out of memory" message when SQLITE_CANTOPEN is caused by a
// missing directory.
func validateDatabaseLocation(path string) error {
	directory := filepath.Dir(path)
	info, err := os.Stat(directory)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("queue: database directory %q does not exist: %w", directory, err)
		}
		return fmt.Errorf("queue: inspect database directory %q: %w", directory, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("queue: database directory %q is not a directory", directory)
	}

	info, err = os.Stat(path)
	if err == nil && info.IsDir() {
		return fmt.Errorf("queue: database path %q is a directory", path)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("queue: inspect database path %q: %w", path, err)
	}
	return nil
}

type sqliteErrorCoder interface {
	Code() int
}

type cantOpenDatabaseError struct {
	path  string
	cause error
}

func (err *cantOpenDatabaseError) Error() string {
	return fmt.Sprintf(
		"queue: cannot open or create database %q (SQLite CANTOPEN); check the database path and permissions",
		err.path,
	)
}

func (err *cantOpenDatabaseError) Unwrap() error {
	return err.cause
}

func databaseOpenError(path string, err error) error {
	var sqliteErr sqliteErrorCoder
	if errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqliteCantOpen {
		return &cantOpenDatabaseError{path: path, cause: err}
	}
	return fmt.Errorf("queue: open database %q: %w", path, err)
}

// OpenReadOnly opens an existing queue for inspection without creating a
// database, initializing its schema, or changing its journal settings.
func OpenReadOnly(path string) (*Store, error) {
	return OpenReadOnlyContext(context.Background(), path)
}

// OpenReadOnlyContext is OpenReadOnly with cancellation for connection setup
// and connection-local configuration. It is useful for request-scoped queue
// inspection such as the web dashboard.
func OpenReadOnlyContext(ctx context.Context, path string) (*Store, error) {
	if ctx == nil {
		return nil, errors.New("queue: read-only context is required")
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("queue: database path is empty")
	}

	dsn, err := readOnlyDSN(path)
	if err != nil {
		return nil, fmt.Errorf("queue: resolve read-only database path: %w", err)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("queue: open read-only database: %w", err)
	}

	// The PRAGMAs below are connection-local. Keeping one connection guarantees
	// that every inspection query uses the configured read-only connection.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("queue: connect read-only database: %w", err)
	}
	for _, pragma := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
		"PRAGMA query_only = ON",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("queue: configure read-only database: %w", err)
		}
	}

	return &Store{db: db, now: time.Now}, nil
}

func readOnlyDSN(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	uriPath := filepath.ToSlash(filepath.Clean(absolute))
	if !strings.HasPrefix(uriPath, "/") {
		// SQLite file URIs use /C:/path on Windows. Unix absolute paths already
		// have the required leading slash.
		uriPath = "/" + uriPath
	}
	uri := url.URL{Scheme: "file", Path: uriPath}
	query := uri.Query()
	query.Set("mode", "ro")
	uri.RawQuery = query.Encode()
	return uri.String(), nil
}

// Close releases the database connection.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) timestamp() time.Time {
	return s.now().UTC()
}

func unixNano(t time.Time) int64 {
	return t.UTC().UnixNano()
}

func timeFromUnixNano(ns int64) time.Time {
	return time.Unix(0, ns).UTC()
}

func nullableTime(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := timeFromUnixNano(v.Int64)
	return &t
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
