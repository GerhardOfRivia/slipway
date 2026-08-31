// Package worker claims durable jobs and executes their configured pipelines.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/executor"
	"github.com/GerhardOfRivia/slipway/internal/queue"
)

const (
	defaultPollInterval      = 250 * time.Millisecond
	defaultPersistenceWindow = 5 * time.Second
	maxFailureDetailRunes    = 256
)

// ErrUnknownWatch means a claimed job references no configured pipeline.
var ErrUnknownWatch = errors.New("worker: unknown watch")

// Store is the durable queue surface needed by workers. *queue.Store satisfies
// this interface; keeping it narrow also makes worker behavior easy to test.
type Store interface {
	Claim(context.Context) (*queue.Job, error)
	StartCommand(context.Context, queue.CommandStart) (int64, error)
	CompleteCommand(context.Context, int64, queue.CommandResult) error
	Succeed(context.Context, int64, int64) error
	Fail(context.Context, int64, int64, string, time.Duration) (queue.Status, error)
}

// PipelineResolver resolves the current command pipeline for a watch name.
type PipelineResolver interface {
	Resolve(string) ([]config.CommandConfig, error)
}

// ConfigResolver is an immutable pipeline snapshot built from configuration.
type ConfigResolver struct {
	pipelines map[string][]config.CommandConfig
}

// NewConfigResolver constructs a resolver from configured watches.
func NewConfigResolver(watches []config.WatchConfig) *ConfigResolver {
	pipelines := make(map[string][]config.CommandConfig, len(watches))
	for _, watch := range watches {
		pipelines[watch.Name] = clonePipeline(watch.Pipeline)
	}
	return &ConfigResolver{pipelines: pipelines}
}

// Resolve returns a copy so template expansion for one job cannot mutate the
// configuration used by another worker.
func (resolver *ConfigResolver) Resolve(watchName string) ([]config.CommandConfig, error) {
	if resolver == nil {
		return nil, fmt.Errorf("%w %q", ErrUnknownWatch, watchName)
	}
	pipeline, ok := resolver.pipelines[watchName]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownWatch, watchName)
	}
	return clonePipeline(pipeline), nil
}

func clonePipeline(pipeline []config.CommandConfig) []config.CommandConfig {
	cloned := make([]config.CommandConfig, len(pipeline))
	for i, command := range pipeline {
		cloned[i] = command
		cloned[i].Args = append([]string(nil), command.Args...)
		cloned[i].Mounts = cloneMounts(command.Mounts)
		cloned[i].ContainerArgs = append([]string(nil), command.ContainerArgs...)
		cloned[i].CommandArgs = append([]string(nil), command.CommandArgs...)
		if command.ContainerEnv != nil {
			cloned[i].ContainerEnv = make(map[string]string, len(command.ContainerEnv))
			for key, value := range command.ContainerEnv {
				cloned[i].ContainerEnv[key] = value
			}
		}
		if command.Env != nil {
			cloned[i].Env = make(map[string]string, len(command.Env))
			for key, value := range command.Env {
				cloned[i].Env[key] = value
			}
		}
	}
	return cloned
}

func cloneMounts(mounts []config.MountConfig) []config.MountConfig {
	if mounts == nil {
		return nil
	}
	cloned := make([]config.MountConfig, len(mounts))
	for index, mount := range mounts {
		cloned[index] = mount
		cloned[index].Options = append([]string(nil), mount.Options...)
	}
	return cloned
}

// Options configures a worker pool.
type Options struct {
	Workers            int
	RetryDelay         time.Duration
	PollInterval       time.Duration
	PersistenceTimeout time.Duration
	Logger             *slog.Logger
}

// Pool runs a fixed number of queue consumers.
type Pool struct {
	store              Store
	resolver           PipelineResolver
	executor           executor.Executor
	workers            int
	retryDelay         time.Duration
	pollInterval       time.Duration
	persistenceTimeout time.Duration
	logger             *slog.Logger
}

// New validates and constructs a worker pool.
func New(store Store, resolver PipelineResolver, commandExecutor executor.Executor, options Options) (*Pool, error) {
	if store == nil {
		return nil, errors.New("worker: store is required")
	}
	if resolver == nil {
		return nil, errors.New("worker: pipeline resolver is required")
	}
	if commandExecutor == nil {
		return nil, errors.New("worker: executor is required")
	}
	if options.Workers <= 0 {
		return nil, errors.New("worker: workers must be greater than zero")
	}
	if options.RetryDelay < 0 {
		return nil, errors.New("worker: retry delay must not be negative")
	}
	if options.PollInterval < 0 {
		return nil, errors.New("worker: poll interval must not be negative")
	}
	if options.PersistenceTimeout < 0 {
		return nil, errors.New("worker: persistence timeout must not be negative")
	}
	if options.PollInterval == 0 {
		options.PollInterval = defaultPollInterval
	}
	if options.PersistenceTimeout == 0 {
		options.PersistenceTimeout = defaultPersistenceWindow
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Pool{
		store:              store,
		resolver:           resolver,
		executor:           commandExecutor,
		workers:            options.Workers,
		retryDelay:         options.RetryDelay,
		pollInterval:       options.PollInterval,
		persistenceTimeout: options.PersistenceTimeout,
		logger:             options.Logger,
	}, nil
}

// Run consumes jobs until ctx is canceled, then waits for all workers to stop.
// Running child commands receive the cancellation through their context.
func (pool *Pool) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("worker: context is required")
	}

	var workers sync.WaitGroup
	workers.Add(pool.workers)
	for number := 1; number <= pool.workers; number++ {
		go func(workerNumber int) {
			defer workers.Done()
			pool.consume(ctx, workerNumber)
		}(number)
	}
	workers.Wait()
	return nil
}

func (pool *Pool) consume(ctx context.Context, workerNumber int) {
	logger := pool.logger.With("worker", workerNumber)
	for {
		if ctx.Err() != nil {
			return
		}

		job, err := pool.store.Claim(ctx)
		if err != nil {
			if !errors.Is(err, queue.ErrNoJob) && !errors.Is(err, context.Canceled) {
				logger.Error("claim job", "error", err)
			}
			if !pool.waitForPoll(ctx) {
				return
			}
			continue
		}
		if job == nil {
			logger.Error("claim job", "error", "store returned a nil job without an error")
			if !pool.waitForPoll(ctx) {
				return
			}
			continue
		}

		logger.Info("running job", "job_id", job.ID, "run_id", job.RunID, "attempt", job.Attempt)
		if err := pool.processJob(ctx, job); err != nil {
			logger.Error("job attempt failed", "job_id", job.ID, "run_id", job.RunID, "error", err)
		} else {
			logger.Info("job succeeded", "job_id", job.ID, "run_id", job.RunID)
		}
	}
}

func (pool *Pool) waitForPoll(ctx context.Context) bool {
	timer := time.NewTimer(pool.pollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (pool *Pool) processJob(ctx context.Context, job *queue.Job) error {
	pipeline, err := pool.resolver.Resolve(job.WatchName)
	if err != nil {
		return pool.failJob(job, fmt.Errorf("resolve pipeline: %w", err))
	}

	for index, configured := range pipeline {
		expander := executor.NewExpander(executor.TemplateData{
			File:  job.Path,
			JobID: job.ID,
		})
		expanded := expandConfiguredCommand(configured, expander)
		if err := expanded.ValidateExecution(); err != nil {
			return pool.failJob(job, fmt.Errorf("command %q after template expansion: %w", expanded.Name, err))
		}
		command := commandFromConfig(expanded)
		commandID, err := pool.store.StartCommand(ctx, queue.CommandStart{
			RunID:      job.RunID,
			Sequence:   index + 1,
			Name:       command.Name,
			Program:    command.Program,
			Args:       append([]string(nil), command.Args...),
			Env:        executor.Environment(command.Env),
			WorkingDir: command.WorkingDir,
			Timeout:    command.Timeout,
		})
		if err != nil {
			return pool.failJob(job, fmt.Errorf("record command %q start: %w", command.Name, err))
		}

		pool.logger.Info("running command",
			"job_id", job.ID,
			"run_id", job.RunID,
			"command_id", commandID,
			"sequence", index+1,
			"command", command.Name,
			"program", command.Program,
			"args", command.Args,
		)

		result, executionErr := pool.executor.Execute(ctx, command)
		if executionErr == nil && result.ExitCode != 0 {
			executionErr = fmt.Errorf("command exited with code %d", result.ExitCode)
		}
		executionErr = withFailureOutput(executionErr, result)
		commandStatus := queue.CommandSucceeded
		errorText := ""
		if executionErr != nil {
			commandStatus = queue.CommandFailed
			errorText = executionErr.Error()
		}

		persistContext, cancel := pool.persistenceContext()
		completeErr := pool.store.CompleteCommand(persistContext, commandID, queue.CommandResult{
			Status:   commandStatus,
			ExitCode: result.ExitCode,
			Stdout:   result.Stdout,
			Stderr:   result.Stderr,
			Error:    errorText,
		})
		cancel()
		if completeErr != nil {
			if executionErr != nil {
				executionErr = errors.Join(executionErr, fmt.Errorf("persist command result: %w", completeErr))
			} else {
				executionErr = fmt.Errorf("persist command result: %w", completeErr)
			}
		}
		if executionErr != nil {
			return pool.failJob(job, fmt.Errorf("command %q: %w", command.Name, executionErr))
		}
	}

	persistContext, cancel := pool.persistenceContext()
	err = pool.store.Succeed(persistContext, job.ID, job.RunID)
	cancel()
	if err != nil {
		return fmt.Errorf("mark job succeeded: %w", err)
	}
	return nil
}

// withFailureOutput adds one small diagnostic to an execution error. Complete
// command output remains available in history, so the job error only needs the
// last useful line from stderr, or stdout when stderr is empty.
func withFailureOutput(cause error, result executor.Result) error {
	if cause == nil {
		return nil
	}
	summary := failureOutputSummary(result)
	if summary == "" {
		return cause
	}
	return fmt.Errorf("%w (%s)", cause, summary)
}

func failureOutputSummary(result executor.Result) string {
	if detail := lastFailureOutputLine(result.Stderr); detail != "" {
		return fmt.Sprintf("stderr: %q", detail)
	}
	if detail := lastFailureOutputLine(result.Stdout); detail != "" {
		return fmt.Sprintf("stdout: %q", detail)
	}
	return ""
}

func lastFailureOutputLine(output string) string {
	lines := strings.Split(output, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		detail := strings.TrimSpace(lines[index])
		if detail == "" || isCapturedOutputTruncationMarker(detail) {
			continue
		}
		detail = strings.Join(strings.Fields(detail), " ")
		return truncateFailureDetail(detail)
	}
	return ""
}

func isCapturedOutputTruncationMarker(line string) bool {
	return strings.HasPrefix(line, "[slipway: output truncated after ") && strings.HasSuffix(line, " bytes]")
}

func truncateFailureDetail(detail string) string {
	runes := []rune(detail)
	if len(runes) <= maxFailureDetailRunes {
		return detail
	}
	const suffix = "... [truncated]"
	return string(runes[:maxFailureDetailRunes-len(suffix)]) + suffix
}

func (pool *Pool) failJob(job *queue.Job, cause error) error {
	persistContext, cancel := pool.persistenceContext()
	status, failErr := pool.store.Fail(persistContext, job.ID, job.RunID, cause.Error(), pool.retryDelay)
	cancel()
	if failErr != nil {
		return errors.Join(cause, fmt.Errorf("mark job failed: %w", failErr))
	}
	pool.logger.Info("job attempt persisted",
		"job_id", job.ID,
		"run_id", job.RunID,
		"status", status,
		"retry_delay", pool.retryDelay,
	)
	return cause
}

func (pool *Pool) persistenceContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), pool.persistenceTimeout)
}

func commandFromConfig(command config.CommandConfig) executor.Command {
	return executor.Command{
		Name:       command.Name,
		Program:    command.Program,
		Args:       command.ExecutionArgs(),
		Timeout:    command.Timeout.Duration,
		WorkingDir: command.WorkingDir,
		Output:     command.Output,
		Env:        command.Env,
	}
}

// expandConfiguredCommand expands structured container values before
// ExecutionArgs serializes them. In particular, this lets mount paths be
// escaped after a job path containing CSV delimiters has been substituted.
func expandConfiguredCommand(command config.CommandConfig, expander executor.Expander) config.CommandConfig {
	command.Args = expandConfiguredStrings(command.Args, expander)
	command.Image = expander.String(command.Image)
	command.ContainerArgs = expandConfiguredStrings(command.ContainerArgs, expander)
	command.Command = expander.String(command.Command)
	command.CommandArgs = expandConfiguredStrings(command.CommandArgs, expander)
	command.WorkingDir = expander.String(command.WorkingDir)
	command.Output = expander.String(command.Output)

	command.Mounts = cloneMounts(command.Mounts)
	for index := range command.Mounts {
		command.Mounts[index].Source = expander.String(command.Mounts[index].Source)
		command.Mounts[index].Target = expander.String(command.Mounts[index].Target)
		command.Mounts[index].Options = expandConfiguredStrings(command.Mounts[index].Options, expander)
	}
	command.ContainerEnv = expandConfiguredEnvironment(command.ContainerEnv, expander)
	command.Env = expandConfiguredEnvironment(command.Env, expander)
	return command
}

func expandConfiguredStrings(values []string, expander executor.Expander) []string {
	if values == nil {
		return nil
	}
	expanded := make([]string, len(values))
	for index, value := range values {
		expanded[index] = expander.String(value)
	}
	return expanded
}

func expandConfiguredEnvironment(values map[string]string, expander executor.Expander) map[string]string {
	if values == nil {
		return nil
	}
	expanded := make(map[string]string, len(values))
	for key, value := range values {
		expanded[key] = expander.String(value)
	}
	return expanded
}
