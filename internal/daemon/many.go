package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

// NamedConfig associates a loaded configuration with the path used to load it.
// Path is used to identify the daemon instance in logs and errors.
type NamedConfig struct {
	Path   string
	Config *config.Config
}

type runnerFunc func(context.Context, *config.Config, *slog.Logger) error

// RunMany runs one daemon per configuration. The configurations must use
// distinct SQLite files because each daemon independently owns and recovers its
// queue. If one daemon fails, all of its siblings are canceled before RunMany
// returns the failure.
func RunMany(ctx context.Context, configs []NamedConfig, logger *slog.Logger) error {
	return runMany(ctx, configs, logger, Run)
}

func runMany(ctx context.Context, configs []NamedConfig, logger *slog.Logger, run runnerFunc) error {
	if ctx == nil {
		return errors.New("daemon: context is required")
	}
	if run == nil {
		return errors.New("daemon: runner is required")
	}
	if err := validateNamedConfigs(configs); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		path string
		err  error
	}
	results := make(chan result, len(configs))
	for _, named := range configs {
		named := named
		go func() {
			results <- result{
				path: named.Path,
				err:  run(runContext, named.Config, logger.With("config", named.Path)),
			}
		}()
	}

	var firstError error
	for range configs {
		outcome := <-results
		if outcome.err == nil {
			continue
		}
		// Cancellation of the parent is an ordinary shutdown, but it must not
		// hide an unrelated daemon error that happens to arrive at the same
		// time. Cancellation of the shared child context after a sibling
		// failure is reported through that sibling's original error instead.
		if isCancellationFrom(outcome.err, ctx.Err()) {
			continue
		}
		if firstError != nil && isCancellationFrom(outcome.err, runContext.Err()) {
			continue
		}
		wrapped := fmt.Errorf("daemon: config %q: %w", outcome.path, outcome.err)
		if firstError == nil {
			firstError = wrapped
			cancel()
			continue
		}
		firstError = errors.Join(firstError, wrapped)
	}
	return firstError
}

// isCancellationFrom reports whether err is made up solely of wrappers around
// the supplied context error. In particular, an errors.Join containing both a
// cancellation and an operational failure is not considered normal shutdown.
func isCancellationFrom(err, contextErr error) bool {
	if err == nil || contextErr == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !isCancellationFrom(cause, contextErr) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isCancellationFrom(wrapped.Unwrap(), contextErr)
	}
	return errors.Is(err, contextErr)
}

func validateNamedConfigs(configs []NamedConfig) error {
	if len(configs) == 0 {
		return errors.New("daemon: at least one config is required")
	}

	type databaseOwner struct {
		configPath   string
		databasePath string
	}
	databaseOwners := make([]databaseOwner, 0, len(configs))
	for index, named := range configs {
		if strings.TrimSpace(named.Path) == "" {
			return fmt.Errorf("daemon: config %d path is required", index+1)
		}
		if named.Config == nil {
			return fmt.Errorf("daemon: config %q is nil", named.Path)
		}
		if strings.TrimSpace(named.Config.Database.Path) == "" {
			return fmt.Errorf("daemon: config %q database path is required", named.Path)
		}

		databasePath, err := config.CanonicalDatabasePath(named.Config.Database.Path)
		if err != nil {
			return fmt.Errorf("daemon: normalize database path for config %q: %w", named.Path, err)
		}
		for _, owner := range databaseOwners {
			equivalent, err := config.PathsEquivalent(owner.databasePath, databasePath)
			if err != nil {
				return fmt.Errorf(
					"daemon: compare database paths for configs %q and %q: %w",
					owner.configPath,
					named.Path,
					err,
				)
			}
			if equivalent {
				return fmt.Errorf(
					"daemon: configs %q and %q use the same database %q",
					owner.configPath,
					named.Path,
					databasePath,
				)
			}
		}
		databaseOwners = append(databaseOwners, databaseOwner{configPath: named.Path, databasePath: databasePath})
	}
	return nil
}
