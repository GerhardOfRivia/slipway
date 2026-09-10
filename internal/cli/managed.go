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
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/control"
	"github.com/GerhardOfRivia/slipway/internal/daemon"
)

const controlTimeout = 30 * time.Second

func startCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("start", stderr, "slipway start <config-or-instance> [name] [--socket path]")
	socketPath := flags.String("socket", "", "control socket (defaults to SLIPWAY_SOCKET or a per-user path)")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() > 2 {
		return usageError{message: "start expects at most 2 positional arguments (config path, optional instance name)"}
	}

	selection := flags.Arg(0)
	if strings.TrimSpace(selection) == "" {
		return usageError{message: "config path is required"}
	}
	paths, instanceName := []string{selection}, strings.TrimSpace(flags.Arg(1))
	if _, err := os.Stat(selection); err == nil || instanceName != "" {
		var err error
		paths, instanceName, err = discoverStartConfig(selection, instanceName)
		if err != nil {
			return err
		}
	}
	client := control.NewClient(*socketPath)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
	defer cancel()
	instances, err := client.Start(ctx, paths, instanceName)
	if err != nil {
		return err
	}
	return printInstances(stdout, instances, false)
}

func testCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("test", stderr, "slipway test <config>")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if err := requireArguments(flags, "config path"); err != nil {
		return err
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

	return testSelectedConfig(runContext, flags.Arg(0), stdout, daemon.RunMany)
}

func testSelectedConfig(ctx context.Context, selection string, output io.Writer, runner selectedConfigRunner) error {
	paths, _, err := discoverSingleConfig(selection, "")
	if err != nil {
		return err
	}
	if runner == nil {
		return errors.New("test: local runner is required")
	}
	cfg, err := config.Load(paths[0])
	if err != nil {
		return fmt.Errorf("load %s: %w", paths[0], err)
	}
	logger := slog.New(slog.NewTextHandler(output, nil))
	err = runner(ctx, []daemon.NamedConfig{{Path: paths[0], Config: cfg}}, logger)
	if cancellationOnlyFrom(err, ctx.Err()) {
		return nil
	}
	return err
}

type selectedConfigRunner func(context.Context, []daemon.NamedConfig, *slog.Logger) error

func discoverStartConfig(selection, name string) ([]string, string, error) {
	if strings.TrimSpace(selection) == "" {
		return nil, "", usageError{message: "config path is required"}
	}
	return discoverSingleConfig(selection, strings.TrimSpace(name))
}

func discoverSingleConfig(selection, name string) ([]string, string, error) {
	paths, err := config.Discover(selection)
	if err != nil {
		return nil, "", err
	}
	if len(paths) != 1 {
		return nil, "", usageError{message: "config path must select a single configuration file"}
	}
	return paths, name, nil
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
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError{message: "ps does not accept positional arguments"}
	}

	client := control.NewClient(*socketPath)
	defer client.CloseIdleConnections()
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
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() == 0 {
		return usageError{message: "stop requires at least one instance ID or name"}
	}

	client := control.NewClient(*socketPath)
	defer client.CloseIdleConnections()
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
		fmt.Fprintln(w, "ID\tNAME\tSTATUS\tDESIRED\tSTARTED\tFINISHED\tCONFIG\tERROR")
	} else {
		fmt.Fprintln(w, "ID\tNAME\tSTATUS\tSTARTED\tCONFIG")
	}
	for _, instance := range instances {
		if all {
			desired := instance.DesiredState
			if desired == "" {
				desired = "-"
			}
			errorText := "-"
			if instance.Error != "" {
				errorText = strconv.Quote(instance.Error)
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				instance.ID, instance.Name, instance.State, desired, formatTime(instance.StartedAt),
				formatOptionalTime(instance.FinishedAt), strconv.Quote(instance.ConfigPath), errorText)
			continue
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			instance.ID, instance.Name, instance.State, formatTime(instance.StartedAt), strconv.Quote(instance.ConfigPath))
	}
	return w.Flush()
}
