package config

import (
	"path/filepath"
	"strings"
)

// Warnings reports non-fatal issues with a command intended for unattended jobs.
// It does not change the command or reject otherwise valid configuration.
func (command CommandConfig) Warnings() []string {
	if command.Executor != ExecutorDocker &&
		!((command.Executor == ExecutorCommand || command.Executor == "") && filepath.Base(command.Program) == "docker") {
		return nil
	}
	args := command.ExecutionArgs()
	if len(args) > 0 && args[0] == "container" {
		args = args[1:]
	}
	if len(args) == 0 || (args[0] != "run" && args[0] != "exec") {
		return nil
	}
	args = args[1:]
	var interactive, tty, detach bool
	for len(args) > 0 {
		argument := args[0]
		if argument == "--" || argument == "-" || !strings.HasPrefix(argument, "-") {
			break // Everything from the image/container name onward is application data.
		}
		if argument == "--it" || strings.HasPrefix(argument, "--it=") {
			return []string{"Docker --it is not a valid option; -it requests an interactive TTY, which slipway does not provide. Remove --it for unattended jobs."}
		}
		option, reason := ParseContainerRunOption(ExecutorDocker, args)
		if reason != "" {
			break // Do not guess whether an unknown option's next token is a value.
		}
		if option.interactive != nil {
			interactive = *option.interactive
		}
		if option.tty != nil {
			tty = *option.tty
		}
		if option.detach != nil {
			detach = *option.detach
		}
		args = args[option.Consumed:]
	}
	if interactive && tty && !detach {
		return []string{"Docker interactive TTY flags (-it, or --interactive with --tty) can fail because slipway provides no interactive stdin or TTY. Remove -i/--interactive and -t/--tty for unattended jobs."}
	}
	return nil
}
