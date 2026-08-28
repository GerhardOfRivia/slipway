package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/control"
	"github.com/GerhardOfRivia/slipway/internal/daemon"
)

const controlTimeout = 30 * time.Second

func startCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("start", stderr, "slipway start [--config path] [--name name] [--socket path]")
	configPath := flags.String("config", configPathDefault(), "YAML configuration file or directory")
	name := flags.String("name", "", "instance name (only with one config)")
	socketPath := flags.String("socket", "", "control socket (defaults to SLIPWAY_SOCKET or a per-user path)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError{message: "start does not accept positional arguments; use --config path"}
	}

	paths, err := config.Discover(*configPath)
	if err != nil {
		return err
	}
	if strings.TrimSpace(*name) != "" && len(paths) != 1 {
		return usageError{message: "start --name requires a single configuration file"}
	}
	client := control.NewClient(*socketPath)
	ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
	defer cancel()
	instances, err := client.Start(ctx, paths, *name)
	if err != nil {
		return err
	}
	return printInstances(stdout, instances, false)
}

func runCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("run", stderr, "slipway run [--config path] [--name name] [--socket path]")
	configPath := flags.String("config", configPathDefault(), "YAML configuration file or directory")
	name := flags.String("name", "", "instance name or daemonless log label (only with one config)")
	socketPath := flags.String("socket", "", "control socket (defaults to SLIPWAY_SOCKET or a per-user path)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError{message: "run does not accept positional arguments; use --config path"}
	}

	runContext, cancelRun := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer func() {
		signal.Stop(signals)
		cancelRun()
	}()
	go func() {
		select {
		case <-signals:
			// Restore the default immediately so a second signal can force an
			// exit while selected runners finish their graceful shutdown.
			signal.Stop(signals)
			cancelRun()
		case <-runContext.Done():
		}
	}()

	client := control.NewClient(*socketPath)
	defer client.CloseIdleConnections()
	return runSelectedConfigsPreferDaemon(
		runContext,
		*configPath,
		*name,
		stdout,
		stderr,
		client,
		daemon.RunMany,
	)
}

type selectedConfigRunner func(context.Context, []daemon.NamedConfig, *slog.Logger) error

type runControlClient interface {
	SocketPath() string
	List(context.Context, bool) ([]control.Instance, error)
	Run(context.Context, string, string, func(control.RunEvent) error) (control.Instance, error)
	Stop(context.Context, string) (control.Instance, error)
}

func runSelectedConfigsPreferDaemon(
	ctx context.Context,
	selection, name string,
	stdout, stderr io.Writer,
	client runControlClient,
	localRunner selectedConfigRunner,
) error {
	paths, name, err := discoverRunConfigs(selection, name)
	if err != nil {
		return err
	}
	if client == nil {
		return errors.New("run: control client is required")
	}

	probeContext, cancelProbe := context.WithTimeout(ctx, controlTimeout)
	_, err = client.List(probeContext, false)
	cancelProbe()
	if err == nil {
		return runDaemonConfigs(ctx, paths, name, stdout, client)
	}
	if ctx.Err() != nil && cancellationOnlyFrom(err, ctx.Err()) {
		return nil
	}
	if !daemonNotRunning(err) {
		return fmt.Errorf("check daemon at %s: %w", client.SocketPath(), err)
	}

	slog.New(slog.NewTextHandler(stderr, nil)).Info(
		"slipway daemon not running; running daemonless",
		"socket", client.SocketPath(),
	)
	return runConfigPaths(ctx, paths, name, stdout, localRunner)
}

func runSelectedConfigs(ctx context.Context, selection, name string, output io.Writer, runner selectedConfigRunner) error {
	paths, name, err := discoverRunConfigs(selection, name)
	if err != nil {
		return err
	}
	return runConfigPaths(ctx, paths, name, output, runner)
}

func discoverRunConfigs(selection, name string) ([]string, string, error) {
	paths, err := config.Discover(selection)
	if err != nil {
		return nil, "", err
	}
	name = strings.TrimSpace(name)
	if name != "" && len(paths) != 1 {
		return nil, "", usageError{message: "run --name requires a single configuration file"}
	}
	return paths, name, nil
}

func runConfigPaths(
	ctx context.Context,
	paths []string,
	name string,
	output io.Writer,
	runner selectedConfigRunner,
) error {
	if runner == nil {
		return errors.New("run: local runner is required")
	}
	loaded, err := loadConfigPaths(paths)
	if err != nil {
		return err
	}
	if name != "" && len(loaded) != 1 {
		return usageError{message: "run --name requires a single configuration file"}
	}
	configs := make([]daemon.NamedConfig, 0, len(loaded))
	for _, item := range loaded {
		configs = append(configs, daemon.NamedConfig{Path: item.path, Config: item.config})
	}
	logger := slog.New(slog.NewTextHandler(output, nil))
	if name != "" {
		logger = logger.With("name", name)
	}
	return runner(ctx, configs, logger)
}

func runDaemonConfigs(
	ctx context.Context,
	paths []string,
	name string,
	output io.Writer,
	client runControlClient,
) error {
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		path string
		err  error
	}
	results := make(chan result, len(paths))
	stream := &lockedWriter{writer: output}
	for _, path := range paths {
		path := path
		go func() {
			results <- result{
				path: path,
				err:  runDaemonConfig(runContext, path, name, stream, client),
			}
		}()
	}

	var firstError error
	for range paths {
		outcome := <-results
		if outcome.err == nil {
			continue
		}
		if cancellationOnlyFrom(outcome.err, ctx.Err()) {
			continue
		}
		if firstError != nil && cancellationOnlyFrom(outcome.err, runContext.Err()) {
			continue
		}
		wrapped := fmt.Errorf("daemon run config %q: %w", outcome.path, outcome.err)
		if firstError == nil {
			firstError = wrapped
			cancel()
			continue
		}
		firstError = errors.Join(firstError, wrapped)
	}
	return firstError
}

func runDaemonConfig(
	ctx context.Context,
	path, name string,
	output io.Writer,
	client runControlClient,
) error {
	var started control.Instance
	finished, err := client.Run(ctx, path, name, func(event control.RunEvent) error {
		switch event.Type {
		case "started":
			started = event.Instance
		case "log":
			return writeRunLog(output, event.Log)
		}
		return nil
	})
	if started.ID == "" && finished.ID != "" {
		started = finished
	}
	if err != nil {
		if started.ID != "" {
			stopContext, cancelStop := context.WithTimeout(context.Background(), controlTimeout)
			_, stopErr := client.Stop(stopContext, started.ID)
			cancelStop()
			if stopErr != nil && !terminalStopRace(stopErr) {
				err = errors.Join(err, fmt.Errorf("stop daemon instance %s: %w", started.ID, stopErr))
			}
		}
		return err
	}

	switch finished.State {
	case control.StateExited:
		return nil
	case control.StateFailed:
		detail := strings.TrimSpace(finished.Error)
		if detail == "" {
			detail = "instance failed without an error message"
		}
		return errors.New(detail)
	default:
		return fmt.Errorf("daemon run ended in unexpected state %q", finished.State)
	}
}

func writeRunLog(output io.Writer, value string) error {
	if _, err := io.WriteString(output, value); err != nil {
		return err
	}
	if !strings.HasSuffix(value, "\n") {
		_, err := fmt.Fprintln(output)
		return err
	}
	return nil
}

type lockedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (writer *lockedWriter) Write(value []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writer.Write(value)
}

func daemonNotRunning(err error) bool {
	return errors.Is(err, control.ErrDaemonUnavailable) &&
		(errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED))
}

func terminalStopRace(err error) bool {
	var apiError *control.APIError
	return errors.As(err, &apiError) && (apiError.Code == "not_active" || apiError.Code == "not_found")
}

func cancellationOnlyFrom(err, contextErr error) bool {
	if err == nil || contextErr == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !cancellationOnlyFrom(cause, contextErr) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return cancellationOnlyFrom(wrapped.Unwrap(), contextErr)
	}
	return errors.Is(err, contextErr)
}

func psCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("ps", stderr, "slipway ps [--all] [--socket path]")
	all := flags.Bool("all", false, "include exited and failed instances")
	flags.BoolVar(all, "a", false, "include exited and failed instances")
	socketPath := flags.String("socket", "", "control socket (defaults to SLIPWAY_SOCKET or a per-user path)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError{message: "ps does not accept positional arguments"}
	}

	client := control.NewClient(*socketPath)
	ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
	defer cancel()
	instances, err := client.List(ctx, *all)
	if err != nil {
		return err
	}
	return printInstances(stdout, instances, *all)
}

func stopCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("stop", stderr, "slipway stop [--socket path] <id-or-name> [id-or-name ...]")
	socketPath := flags.String("socket", "", "control socket (defaults to SLIPWAY_SOCKET or a per-user path)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() == 0 {
		return usageError{message: "stop requires at least one instance ID or name"}
	}

	client := control.NewClient(*socketPath)
	var stopped []control.Instance
	var stopErrors []error
	for _, selector := range flags.Args() {
		ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
		instance, err := client.Stop(ctx, selector)
		cancel()
		if err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("stop %q: %w", selector, err))
			continue
		}
		stopped = append(stopped, instance)
	}
	for _, instance := range stopped {
		fmt.Fprintln(stdout, instance.ID)
	}
	return errors.Join(stopErrors...)
}

func printInstances(output io.Writer, instances []control.Instance, all bool) error {
	w := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if all {
		fmt.Fprintln(w, "ID\tNAME\tSTATUS\tSTARTED\tFINISHED\tCONFIG\tERROR")
	} else {
		fmt.Fprintln(w, "ID\tNAME\tSTATUS\tSTARTED\tCONFIG")
	}
	for _, instance := range instances {
		if all {
			errorText := "-"
			if instance.Error != "" {
				errorText = strconv.Quote(instance.Error)
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				instance.ID, instance.Name, instance.State, formatTime(instance.StartedAt),
				formatOptionalTime(instance.FinishedAt), strconv.Quote(instance.ConfigPath), errorText)
			continue
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			instance.ID, instance.Name, instance.State, formatTime(instance.StartedAt), strconv.Quote(instance.ConfigPath))
	}
	return w.Flush()
}
