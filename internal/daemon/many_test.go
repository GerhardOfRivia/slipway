package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

func TestRunManyRejectsEmptyAndNilConfigs(t *testing.T) {
	t.Parallel()

	if err := runMany(context.Background(), nil, nil, successfulRunner); err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("empty configs error = %v", err)
	}
	configs := []NamedConfig{{Path: "first.yaml", Config: nil}}
	if err := runMany(context.Background(), configs, nil, successfulRunner); err == nil || !strings.Contains(err.Error(), "is nil") {
		t.Fatalf("nil config error = %v", err)
	}
}

func TestRunManyRejectsNormalizedDuplicateDatabasePathsBeforeStarting(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	database := filepath.Join(directory, "slipway.db")
	configs := []NamedConfig{
		{Path: "first.yaml", Config: configWithDatabase(database)},
		{Path: "second.yaml", Config: configWithDatabase(filepath.Join(directory, "unused", "..", "slipway.db"))},
	}
	var calls atomic.Int32
	err := runMany(context.Background(), configs, nil, func(context.Context, *config.Config, *slog.Logger) error {
		calls.Add(1)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "same database") {
		t.Fatalf("duplicate database error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("runner called %d times despite validation failure", calls.Load())
	}
}

func TestRunManyRejectsHardLinkedDatabaseAliasesBeforeStarting(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	database := filepath.Join(directory, "slipway.db")
	alias := filepath.Join(directory, "slipway-alias.db")
	if err := os.WriteFile(database, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(database, alias); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	configs := []NamedConfig{
		{Path: "first.yaml", Config: configWithDatabase(database)},
		{Path: "second.yaml", Config: configWithDatabase(alias)},
	}
	var calls atomic.Int32
	err := runMany(context.Background(), configs, nil, func(context.Context, *config.Config, *slog.Logger) error {
		calls.Add(1)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "same database") {
		t.Fatalf("hard-linked database error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("runner called %d times despite validation failure", calls.Load())
	}
}

func TestRunManyCancelsSiblingsAndWaitsWhenOneFails(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	failingConfig := configWithDatabase(filepath.Join(directory, "one.db"))
	waitingConfig := configWithDatabase(filepath.Join(directory, "two.db"))
	configs := []NamedConfig{
		{Path: "failure.yaml", Config: failingConfig},
		{Path: "waiting.yaml", Config: waitingConfig},
	}
	started := make(chan struct{}, len(configs))
	releaseFailure := make(chan struct{})
	siblingStopped := make(chan struct{})
	wantError := errors.New("watcher failed")
	runner := func(ctx context.Context, cfg *config.Config, _ *slog.Logger) error {
		started <- struct{}{}
		if cfg == failingConfig {
			<-releaseFailure
			return wantError
		}
		<-ctx.Done()
		close(siblingStopped)
		return ctx.Err()
	}

	done := make(chan error, 1)
	go func() { done <- runMany(context.Background(), configs, nil, runner) }()
	for range configs {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("configured daemons were not launched concurrently")
		}
	}
	close(releaseFailure)

	select {
	case err := <-done:
		if !errors.Is(err, wantError) || !strings.Contains(err.Error(), "failure.yaml") {
			t.Fatalf("RunMany() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunMany did not return after a daemon failed")
	}
	select {
	case <-siblingStopped:
	default:
		t.Fatal("RunMany returned before the canceled sibling stopped")
	}
}

func TestRunManyReportsIndependentErrorsRacingSiblingCancellation(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	firstConfig := configWithDatabase(filepath.Join(directory, "one.db"))
	secondConfig := configWithDatabase(filepath.Join(directory, "two.db"))
	configs := []NamedConfig{
		{Path: "first.yaml", Config: firstConfig},
		{Path: "second.yaml", Config: secondConfig},
	}
	started := make(chan struct{}, len(configs))
	release := make(chan struct{})
	firstError := errors.New("first watcher failed")
	secondError := errors.New("second database failed")
	runner := func(_ context.Context, cfg *config.Config, _ *slog.Logger) error {
		started <- struct{}{}
		<-release
		if cfg == firstConfig {
			return firstError
		}
		return secondError
	}

	done := make(chan error, 1)
	go func() { done <- runMany(context.Background(), configs, nil, runner) }()
	for range configs {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("configured daemons were not launched concurrently")
		}
	}
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, firstError) || !errors.Is(err, secondError) {
			t.Fatalf("RunMany() error = %v, want both %v and %v", err, firstError, secondError)
		}
		if !strings.Contains(err.Error(), "first.yaml") || !strings.Contains(err.Error(), "second.yaml") {
			t.Fatalf("RunMany() error = %v, want both config paths", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunMany did not return after runners failed")
	}
}

func TestRunManyTreatsParentCancellationAsNormalShutdown(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	configs := []NamedConfig{
		{Path: "one.yaml", Config: configWithDatabase(filepath.Join(directory, "one.db"))},
		{Path: "two.yaml", Config: configWithDatabase(filepath.Join(directory, "two.db"))},
	}
	started := make(chan struct{}, len(configs))
	runner := func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runMany(ctx, configs, nil, runner) }()
	for range configs {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("configured daemon did not start")
		}
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunMany() error after parent cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunMany did not stop after parent cancellation")
	}
}

func TestRunManyDoesNotHideErrorRacingParentCancellation(t *testing.T) {
	t.Parallel()

	configs := []NamedConfig{{
		Path:   "one.yaml",
		Config: configWithDatabase(filepath.Join(t.TempDir(), "one.db")),
	}}
	started := make(chan struct{})
	wantError := errors.New("database write failed")
	runner := func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
		close(started)
		<-ctx.Done()
		return wantError
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runMany(ctx, configs, nil, runner) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("configured daemon did not start")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, wantError) {
			t.Fatalf("RunMany() error = %v, want %v", err, wantError)
		}
	case <-time.After(time.Second):
		t.Fatal("RunMany did not return after runner failure")
	}
}

func TestRunManyAddsConfigPathToEachLogger(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	configs := []NamedConfig{{Path: "configured/path.yaml", Config: configWithDatabase(filepath.Join(t.TempDir(), "slipway.db"))}}
	err := runMany(context.Background(), configs, logger, func(_ context.Context, _ *config.Config, logger *slog.Logger) error {
		logger.Info("runner started")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode log record %q: %v", output.String(), err)
	}
	if record["config"] != configs[0].Path {
		t.Fatalf("config log attribute = %#v, want %q", record["config"], configs[0].Path)
	}
}

func configWithDatabase(path string) *config.Config {
	return &config.Config{Database: config.DatabaseConfig{Path: path}}
}

func successfulRunner(context.Context, *config.Config, *slog.Logger) error {
	return nil
}
