//go:build linux || darwin

package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const unixHelperEnvironment = "SLIPWAY_EXECUTOR_UNIX_HELPER"

func TestLocalCancellationKillsProcessGroup(t *testing.T) {
	directory := t.TempDir()
	readyPath := filepath.Join(directory, "ready")
	markerPath := filepath.Join(directory, "descendant-survived")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := NewLocal(nil).Execute(ctx, Command{
			Name:    "process tree",
			Program: os.Args[0],
			Args:    []string{"-test.run=^TestExecutorUnixHelperProcess$"},
			Env: map[string]string{
				unixHelperEnvironment:   "parent",
				"SLIPWAY_HELPER_READY":  readyPath,
				"SLIPWAY_HELPER_MARKER": markerPath,
			},
		})
		done <- err
	}()

	parentPID := waitForHelperPID(t, readyPath)
	defer func() { _ = syscall.Kill(-parentPID, syscall.SIGKILL) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", err)
		}
	case <-time.After(processWaitDelay + 2*time.Second):
		t.Fatal("Execute did not return after cancellation")
	}

	time.Sleep(900 * time.Millisecond)
	if _, err := os.Stat(markerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("descendant survived process-group cancellation; marker stat error = %v", err)
	}
}

func TestLocalWaitDelayBoundsInheritedOutputPipe(t *testing.T) {
	directory := t.TempDir()
	readyPath := filepath.Join(directory, "ready")
	descendantReadyPath := filepath.Join(directory, "descendant-ready")
	triggerPath := filepath.Join(directory, "trigger")
	markerPath := filepath.Join(directory, "background-descendant-survived")
	started := time.Now()
	_, err := NewLocal(nil).Execute(context.Background(), Command{
		Name:    "background descendant",
		Program: os.Args[0],
		Args:    []string{"-test.run=^TestExecutorUnixHelperProcess$"},
		Env: map[string]string{
			unixHelperEnvironment:             "background-parent",
			"SLIPWAY_HELPER_READY":            readyPath,
			"SLIPWAY_HELPER_DESCENDANT_READY": descendantReadyPath,
			"SLIPWAY_HELPER_TRIGGER":          triggerPath,
			"SLIPWAY_HELPER_MARKER":           markerPath,
		},
	})
	elapsed := time.Since(started)
	parentPID := waitForHelperPID(t, readyPath)
	defer func() { _ = syscall.Kill(-parentPID, syscall.SIGKILL) }()

	if !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("Execute() error = %v, want exec.ErrWaitDelay", err)
	}
	if elapsed < processWaitDelay/2 || elapsed > processWaitDelay+2*time.Second {
		t.Fatalf("Execute returned after %s, want approximately %s", elapsed, processWaitDelay)
	}
	if err := os.WriteFile(triggerPath, []byte("continue"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(markerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("background descendant survived command cleanup; marker stat error = %v", err)
	}
}

func TestExecutorUnixHelperProcess(t *testing.T) {
	mode := os.Getenv(unixHelperEnvironment)
	if mode == "" {
		return
	}

	switch mode {
	case "parent":
		child := unixHelperCommand("descendant")
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if err := child.Start(); err != nil {
			panic(err)
		}
		writeHelperPID(os.Getenv("SLIPWAY_HELPER_READY"))
		time.Sleep(10 * time.Second)
	case "descendant":
		time.Sleep(700 * time.Millisecond)
		if err := os.WriteFile(os.Getenv("SLIPWAY_HELPER_MARKER"), []byte("survived"), 0o600); err != nil {
			panic(err)
		}
	case "background-parent":
		child := unixHelperCommand("background-descendant")
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if err := child.Start(); err != nil {
			panic(err)
		}
		waitForHelperFile(os.Getenv("SLIPWAY_HELPER_DESCENDANT_READY"))
		writeHelperPID(os.Getenv("SLIPWAY_HELPER_READY"))
		// Exit immediately instead of letting the race-enabled test harness do
		// its own comparatively slow shutdown; WaitDelay should be measured from
		// the command's exit, not test-runner bookkeeping.
		os.Exit(0)
	case "background-descendant":
		writeHelperPID(os.Getenv("SLIPWAY_HELPER_DESCENDANT_READY"))
		waitForHelperFile(os.Getenv("SLIPWAY_HELPER_TRIGGER"))
		if err := os.WriteFile(os.Getenv("SLIPWAY_HELPER_MARKER"), []byte("survived"), 0o600); err != nil {
			panic(err)
		}
	default:
		panic("unknown unix executor helper mode: " + mode)
	}
}

func unixHelperCommand(mode string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestExecutorUnixHelperProcess$")
	command.Env = mergeEnvironment(os.Environ(), map[string]string{unixHelperEnvironment: mode})
	return command
}

func writeHelperPID(filename string) {
	if err := os.WriteFile(filename, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		panic(err)
	}
}

func waitForHelperFile(filename string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filename); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			panic(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("timed out waiting for helper file: " + filename)
}

func waitForHelperPID(t *testing.T, filename string) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(filename)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr != nil {
				t.Fatalf("parse helper PID %q: %v", data, parseErr)
			}
			return pid
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read helper PID: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("timed out waiting for helper PID at %s", filename))
	return 0
}
