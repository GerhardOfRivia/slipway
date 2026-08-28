//go:build linux || darwin

package executor

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// configureProcessCancellation gives each command its own process group. A
// context cancellation then terminates the whole group instead of leaving
// ordinary child and grandchild processes behind.
func configureProcessCancellation(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
}

// cleanupProcessGroup removes ordinary descendants that outlive the direct
// command. CommandContext stops watching once the direct process exits, so a
// final best-effort group kill is needed even on otherwise-successful runs.
func cleanupProcessGroup(command *exec.Cmd) error {
	if command == nil || command.Process == nil {
		return nil
	}
	err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
