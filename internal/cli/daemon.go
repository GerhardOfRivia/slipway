package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/control"
	"github.com/GerhardOfRivia/slipway/internal/webui"
)

// RunDaemon executes a slipwayd invocation and returns a process exit code.
func RunDaemon(args []string, stdout, stderr io.Writer) int {
	return RunDaemonVersion(args, stdout, stderr, "dev")
}

// RunDaemonVersion executes a slipwayd invocation using version as the displayed
// build version. With no arguments, it starts the daemon.
func RunDaemonVersion(args []string, stdout, stderr io.Writer, version string) int {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if len(args) > 0 {
		switch args[0] {
		case "help", "-h", "--help":
			printDaemonUsage(stdout)
			return 0
		case "version", "-v", "-version", "--version":
			if len(args) != 1 {
				fmt.Fprintln(stderr, "slipwayd: version does not accept arguments")
				return 2
			}
			fmt.Fprintf(stdout, "slipwayd %s\n", version)
			return 0
		}
	}

	err := daemonCommand(args, stderr)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	var usage usageError
	if errors.As(err, &usage) {
		fmt.Fprintln(stderr, "slipwayd:", usage.message)
		return 2
	}
	fmt.Fprintln(stderr, "slipwayd:", err)
	return 1
}

func daemonCommand(args []string, stderr io.Writer) error {
	flags := newFlagSet("slipwayd", stderr, "slipwayd [--config path] [--socket path] [--web-listen address] [--log-level level]")
	configPath := flags.String("config", configPathDefault(), "optional YAML file or directory to start when the daemon starts")
	socketPath := flags.String("socket", "", "control socket (defaults to SLIPWAY_SOCKET or a per-user path)")
	webListen := flags.String("web-listen", webListenDefault(), "optional loopback address for the web dashboard (for example 127.0.0.1:8080)")
	logLevel := flags.String("log-level", "info", "debug, info, warn, or error")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError{message: "does not accept positional arguments"}
	}

	level, err := parseLogLevel(*logLevel)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	ctx, cancelDaemon := context.WithCancel(context.Background())
	daemonSignals := make(chan os.Signal, 1)
	signal.Notify(daemonSignals, os.Interrupt, syscall.SIGTERM)
	defer func() {
		signal.Stop(daemonSignals)
		cancelDaemon()
	}()
	go func() {
		select {
		case <-daemonSignals:
			// Restore the default immediately so a second signal can force an
			// exit if graceful instance shutdown takes too long.
			signal.Stop(daemonSignals)
			cancelDaemon()
		case <-ctx.Done():
		}
	}()
	manager, err := control.NewManager(control.Options{Context: ctx, Logger: logger})
	if err != nil {
		return err
	}
	server, err := control.NewServer(*socketPath, manager, logger)
	if err != nil {
		return err
	}
	var webServer *webui.Server
	if address := strings.TrimSpace(*webListen); address != "" {
		webServer, err = webui.NewServer(address, webTokenPath(server.Path()), manager, logger)
		if err != nil {
			_ = server.Close()
			return err
		}
	}
	serveStarted := false
	defer func() {
		// Serve owns network and socket cleanup once entered. Before that point,
		// bootstrap failures still need an explicit close.
		if !serveStarted {
			_ = webServer.Close()
			_ = server.Close()
		}
	}()

	bootstrapCount := 0
	if selection := strings.TrimSpace(*configPath); selection != "" {
		paths, err := config.Discover(selection)
		if err != nil {
			return err
		}
		instances, err := manager.StartManyContext(ctx, paths, "")
		if err != nil {
			return err
		}
		bootstrapCount = len(instances)
	}

	logger.Info("control daemon listening", "socket", server.Path(), "bootstrapped_instances", bootstrapCount)
	if webServer != nil {
		logger.Info("web dashboard listening", "address", webServer.Address(), "token_file", webServer.TokenPath())
	}
	serveStarted = true
	if webServer == nil {
		if err := server.Serve(ctx); err != nil {
			return err
		}
	} else if err := serveDaemonServers(ctx, cancelDaemon, server, webServer); err != nil {
		return err
	}
	logger.Info("control daemon stopped")
	return nil
}

type daemonServeResult struct {
	name string
	err  error
}

func serveDaemonServers(
	ctx context.Context,
	cancel context.CancelFunc,
	controlServer *control.Server,
	webServer *webui.Server,
) error {
	results := make(chan daemonServeResult, 2)
	go func() {
		results <- daemonServeResult{name: "control", err: controlServer.Serve(ctx)}
	}()
	go func() {
		results <- daemonServeResult{name: "web", err: webServer.Serve(ctx)}
	}()

	first := <-results
	cancel()
	second := <-results
	closeErr := webServer.Close()
	if first.err != nil {
		first.err = fmt.Errorf("%s server: %w", first.name, first.err)
	}
	if second.err != nil {
		second.err = fmt.Errorf("%s server: %w", second.name, second.err)
	}
	return errors.Join(first.err, second.err, closeErr)
}

func printDaemonUsage(output io.Writer) {
	fmt.Fprintln(output, `slipway daemon manages file-watching instances.

Usage:
  slipwayd [--config path] [--socket path] [--web-listen address] [--log-level level]
  slipwayd version

With --config or SLIPWAY_CONFIG, slipwayd bootstraps one YAML file or every YAML
file in the selected directory. Without either, it starts with no instances.`)
}

func webListenDefault() string {
	return strings.TrimSpace(os.Getenv("SLIPWAY_WEB_LISTEN"))
}

func webTokenPath(socketPath string) string {
	return filepath.Clean(socketPath) + ".web-token"
}
