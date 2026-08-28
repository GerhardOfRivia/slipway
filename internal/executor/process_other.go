//go:build !linux && !darwin

package executor

import "os/exec"

// Other platforms retain exec.CommandContext's direct-process cancellation.
// WaitDelay still prevents descendants holding inherited pipes from blocking
// Execute indefinitely.
func configureProcessCancellation(_ *exec.Cmd) {}

func cleanupProcessGroup(_ *exec.Cmd) error { return nil }
