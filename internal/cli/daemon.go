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

	"github.com/GerhardOfRivia/onderzeeer/internal/control"
	"github.com/GerhardOfRivia/onderzeeer/internal/webui"
)

// RunDaemon executes a onderzeeerd invocation and returns a process exit code.
func RunDaemon(args []string, stdout, stderr io.Writer) int {
	return RunDaemonVersion(args, stdout, stderr, "dev")
}

// RunDaemonVersion executes a onderzeeerd invocation using version as the displayed
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
				fmt.Fprintln(stderr, "onderzeeerd: version does not accept arguments")
				return 2
			}
			fmt.Fprintf(stdout, "onderzeeerd %s\n", version)
			return 0
		}
	}

	err := daemonCommand(args, stderr, version)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	var usage usageError
	if errors.As(err, &usage) {
		fmt.Fprintln(stderr, "onderzeeerd:", usage.message)
		return 2
	}
	fmt.Fprintln(stderr, "onderzeeerd:", err)
	return 1
}

func daemonCommand(args []string, stderr io.Writer, version string) error {
	flags := newFlagSet("onderzeeerd", stderr, "onderzeeerd [--socket path] [--web-listen address] [--log-level level] [--state-dir path]")
	socketPath := flags.String("socket", "", "control socket (defaults to ONDERZEEER_SOCKET or a per-user path)")
	webListen := flags.String("web-listen", webListenDefault(), "optional loopback or wildcard address for the web dashboard (for example 127.0.0.1:8080)")
	logLevel := flags.String("log-level", "info", "debug, info, warn, or error")
	stateDirectory := flags.String("state-dir", "", "persistent daemon state (defaults to ONDERZEEER_STATE_DIR or the per-user state directory)")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError{message: "does not accept positional arguments; register instances with onderzeeer start <config> [name]"}
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
	statePath, err := control.ResolveStateDirectory(*stateDirectory)
	if err != nil {
		return err
	}
	manager, err := control.NewManager(control.Options{Context: ctx, Logger: logger, StateDirectory: statePath})
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), controlTimeout)
		defer cancel()
		if err := manager.Shutdown(shutdownContext); err != nil {
			logger.Error("daemon cleanup failed", "error", err)
		}
	}()
	server, err := control.NewServer(*socketPath, manager, logger)
	if err != nil {
		return err
	}
	var webServer *webui.Server
	if address := strings.TrimSpace(*webListen); address != "" {
		webServer, err = webui.NewServer(address, webTokenPath(server.Path()), version, manager, logger)
		if err != nil {
			_ = server.Close()
			return err
		}
	}
	serveStarted := false
	defer func() {
		// Serve owns network and socket cleanup once entered. Before that point,
		// restore failures still need an explicit close.
		if !serveStarted {
			_ = webServer.Close()
			_ = server.Close()
		}
	}()

	restoredCount, err := manager.Restore(ctx)
	if err != nil {
		return err
	}

	logger.Info("control daemon listening", "socket", server.Path(), "state_directory", statePath, "restored_instances", restoredCount)
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
	fmt.Fprintln(output, `onderzeeer daemon manages file-watching instances.

Usage:
  onderzeeerd [--socket path] [--web-listen address] [--log-level level] [--state-dir path]
  onderzeeerd version

Starts the daemon; no existing onderzeeerd is required.
The version command prints the version without starting the daemon.

Register instances separately with: onderzeeer start <config> [name]
Registered instances and their queue databases persist in --state-dir,
ONDERZEEER_STATE_DIR, or $XDG_STATE_HOME/onderzeeer (default ~/.local/state/onderzeeer).
Every start restores saved configurations whose desired state is running.
Explicitly stopped instances stay stopped; a new state directory starts empty.
Config paths, directories, and instance names are not accepted as daemon arguments.
ONDERZEEER_CONFIG is not used.`)
}

func webListenDefault() string {
	return strings.TrimSpace(os.Getenv("ONDERZEEER_WEB_LISTEN"))
}

func webTokenPath(socketPath string) string {
	return filepath.Clean(socketPath) + ".web-token"
}
