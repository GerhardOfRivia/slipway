// Package executor defines command execution independently from the queue and
// provides the local process implementation.
package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	maxCapturedOutputBytes = 1 << 20
	processWaitDelay       = 2 * time.Second
)

// ErrTimeout identifies a command stopped by its configured timeout.
var ErrTimeout = errors.New("command timed out")

// Command is an executor-neutral description of one pipeline step.
//
// Program and Args are kept separate deliberately: implementations must not
// assemble them into a shell command string.
type Command struct {
	Name       string
	Program    string
	Args       []string
	Timeout    time.Duration
	WorkingDir string
	Output     string
	Env        map[string]string
}

// TemplateData contains the per-job values available to command templates.
type TemplateData struct {
	File  string
	JobID int64
}

// Expander applies one job's template data to configuration values. Keeping a
// reusable expander lets callers expand structured values before serializing
// them into a runtime-specific argument such as a mount specification.
type Expander struct {
	replacer *strings.Replacer
}

// NewExpander constructs a reusable template expander for one job.
func NewExpander(data TemplateData) Expander {
	ext := filepath.Ext(data.File)
	base := filepath.Base(data.File)
	stem := strings.TrimSuffix(base, ext)
	return Expander{replacer: strings.NewReplacer(
		"{{file}}", data.File,
		"{{dir}}", filepath.Dir(data.File),
		"{{basename}}", base,
		"{{stem}}", stem,
		"{{ext}}", ext,
		"{{job_id}}", strconv.FormatInt(data.JobID, 10),
	)}
}

// String expands templates in one scalar value.
func (expander Expander) String(value string) string {
	if expander.replacer == nil {
		return value
	}
	return expander.replacer.Replace(value)
}

// Result describes a completed command, including failures to start or
// commands killed by cancellation.
type Result struct {
	StartedAt  time.Time
	FinishedAt time.Time
	ExitCode   int
	Stdout     string
	Stderr     string
	TimedOut   bool
}

// Executor runs one already-expanded command. Other execution backends can
// implement this interface without changing worker or queue code.
type Executor interface {
	Execute(context.Context, Command) (Result, error)
}

// Expand substitutes the supported file and job templates in arguments, the
// working directory, output path, and environment values. It returns a
// deep-enough copy for callers to safely retain as command history.
func Expand(command Command, data TemplateData) Command {
	return NewExpander(data).Command(command)
}

// Command expands templates throughout one executor command and returns a
// copy that can be retained safely in history.
func (expander Expander) Command(command Command) Command {
	expanded := command
	expanded.Args = make([]string, len(command.Args))
	for i, arg := range command.Args {
		expanded.Args[i] = expander.String(arg)
	}
	expanded.WorkingDir = expander.String(command.WorkingDir)
	expanded.Output = expander.String(command.Output)
	if command.Env != nil {
		expanded.Env = make(map[string]string, len(command.Env))
		for key, value := range command.Env {
			expanded.Env[key] = expander.String(value)
		}
	}
	return expanded
}

// Environment returns configured environment overrides in stable KEY=VALUE
// order. It is useful when persisting the exact command specification.
func Environment(values map[string]string) []string {
	environment := make([]string, 0, len(values))
	for key, value := range values {
		environment = append(environment, key+"="+value)
	}
	sort.Strings(environment)
	return environment
}

// Local executes commands as child processes on the daemon host.
type Local struct {
	logger *slog.Logger
}

// NewLocal constructs a local executor. A nil logger discards log records.
func NewLocal(logger *slog.Logger) *Local {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	}
	return &Local{logger: logger}
}

// Execute runs program with args directly through os/exec. It does not invoke a
// shell, so spaces and shell metacharacters remain literal data. Captured
// stdout and stderr are independently bounded to protect daemon memory and the
// persistent command history. When Output is set, complete stdout is also
// streamed to that file.
func (local *Local) Execute(ctx context.Context, command Command) (Result, error) {
	result := Result{ExitCode: -1}
	if command.Program == "" {
		return result, errors.New("executor: program is required")
	}

	runContext := ctx
	cancel := func() {}
	if command.Timeout > 0 {
		runContext, cancel = context.WithTimeout(ctx, command.Timeout)
	}
	defer cancel()

	process := exec.CommandContext(runContext, command.Program, command.Args...)
	configureProcessCancellation(process)
	process.WaitDelay = processWaitDelay
	process.Dir = command.WorkingDir
	process.Env = mergeEnvironment(os.Environ(), command.Env)
	stdout := newLimitedBuffer(maxCapturedOutputBytes)
	stderr := newLimitedBuffer(maxCapturedOutputBytes)
	outputFile, outputPath, err := openOutput(command)
	if err != nil {
		return result, fmt.Errorf("executor: %s: %w", commandLabel(command), err)
	}
	if outputFile == nil {
		process.Stdout = &stdout
	} else {
		process.Stdout = io.MultiWriter(&stdout, outputFile)
	}
	process.Stderr = &stderr

	result.StartedAt = time.Now()
	local.logger.Debug("starting command",
		"command", command.Name,
		"program", command.Program,
		"job_working_directory", command.WorkingDir,
		"output", outputPath,
	)
	runErr := process.Run()
	cleanupErr := cleanupProcessGroup(process)
	closeErr := closeOutput(outputFile, outputPath)
	result.FinishedAt = time.Now()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	if process.ProcessState != nil {
		result.ExitCode = process.ProcessState.ExitCode()
	}

	if runContext.Err() != nil {
		if errors.Is(runContext.Err(), context.DeadlineExceeded) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.TimedOut = true
			return result, fmt.Errorf("executor: %s: %w", commandLabel(command), errors.Join(ErrTimeout, context.DeadlineExceeded, cleanupErr, closeErr))
		}
		return result, fmt.Errorf("executor: %s: %w", commandLabel(command), errors.Join(runContext.Err(), cleanupErr, closeErr))
	}
	if err := errors.Join(runErr, cleanupErr, closeErr); err != nil {
		return result, fmt.Errorf("executor: %s: %w", commandLabel(command), err)
	}
	return result, nil
}

// openOutput creates the optional stdout destination before the child starts.
// Relative paths use the child's working directory, matching normal process
// redirection semantics. Parent directories are deliberately not created.
func openOutput(command Command) (*os.File, string, error) {
	if command.Output == "" {
		return nil, "", nil
	}
	outputPath := command.Output
	if !filepath.IsAbs(outputPath) && command.WorkingDir != "" {
		outputPath = filepath.Join(command.WorkingDir, outputPath)
	}
	file, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return nil, outputPath, fmt.Errorf("open output %q: %w", outputPath, err)
	}
	return file, outputPath, nil
}

func closeOutput(file *os.File, outputPath string) error {
	if file == nil {
		return nil
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close output %q: %w", outputPath, err)
	}
	return nil
}

type limitedBuffer struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func newLimitedBuffer(limit int) limitedBuffer {
	return limitedBuffer{limit: limit}
}

// Write retains at most limit bytes but always reports the complete write as
// consumed. Returning a short write would make otherwise-successful child
// commands fail merely because their diagnostic output exceeded our history
// budget.
func (buffer *limitedBuffer) Write(data []byte) (int, error) {
	written := len(data)
	remaining := buffer.limit - buffer.buffer.Len()
	if remaining > 0 {
		if remaining > len(data) {
			remaining = len(data)
		}
		_, _ = buffer.buffer.Write(data[:remaining])
	}
	if remaining < len(data) {
		buffer.truncated = true
	}
	return written, nil
}

func (buffer *limitedBuffer) String() string {
	if !buffer.truncated {
		return buffer.buffer.String()
	}
	return buffer.buffer.String() + fmt.Sprintf("\n[slipway: output truncated after %d bytes]\n", buffer.limit)
}

func commandLabel(command Command) string {
	if command.Name != "" {
		return command.Name
	}
	return command.Program
}

func mergeEnvironment(base []string, overrides map[string]string) []string {
	if len(overrides) == 0 {
		return append([]string(nil), base...)
	}

	values := make(map[string]string, len(base)+len(overrides))
	for _, item := range base {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	return Environment(values)
}
