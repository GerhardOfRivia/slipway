package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestDockerTerminalWarnings(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		args []string
		warn bool
	}{
		{"combined", []string{"run", "-it", "image"}, true},
		{"reversed", []string{"run", "-ti", "image"}, true},
		{"separate", []string{"run", "-i", "-t", "image"}, true},
		{"long", []string{"run", "--interactive", "--tty", "image"}, true},
		{"explicit true", []string{"run", "--interactive=true", "--tty=1", "image"}, true},
		{"exec", []string{"exec", "-it", "container", "sh"}, true},
		{"container alias", []string{"container", "run", "-it", "image"}, true},
		{"exec alias", []string{"container", "exec", "-it", "container", "sh"}, true},
		{"typo", []string{"run", "--it", "image"}, true},
		{"typo with value", []string{"run", "--it=false", "image"}, true},
		{"after valued options", []string{"run", "--gpus", "all", "-p8080:80", "-it", "image"}, true},
		{"cluster before value", []string{"run", "-iteMODE=test", "image"}, true},
		{"interactive only", []string{"run", "-i", "image"}, false},
		{"tty only", []string{"run", "-t", "image"}, false},
		{"disabled tty", []string{"run", "-it=false", "image"}, false},
		{"disabled interactive", []string{"run", "-ti=false", "image"}, false},
		{"later override", []string{"run", "-it", "--tty=false", "image"}, false},
		{"re-enabled", []string{"run", "-it=false", "-t", "image"}, true},
		{"detached", []string{"run", "-dit", "image"}, false},
		{"detach disabled", []string{"run", "-dit", "--detach=false", "image"}, true},
		{"application args", []string{"run", "image", "tool", "-it", "--it"}, false},
		{"default entrypoint args", []string{"run", "image", "--it"}, false},
		{"exec application args", []string{"exec", "container", "tool", "-it"}, false},
		{"option terminator", []string{"run", "--", "image", "--it"}, false},
		{"environment value", []string{"run", "--env", "--it", "image"}, false},
		{"attached environment", []string{"run", "-e-it", "image"}, false},
		{"entrypoint value", []string{"run", "--entrypoint", "-it", "image"}, false},
		{"label value", []string{"run", "--label=--it", "image"}, false},
		{"unknown arity", []string{"run", "--future", "--it", "image"}, false},
		{"other subcommand", []string{"build", "-it", "."}, false},
		{"empty", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := CommandConfig{Executor: ExecutorDocker, Args: test.args}
			before := command.ExecutionArgs()
			warnings := command.Warnings()
			if (len(warnings) > 0) != test.warn {
				t.Fatalf("Warnings(%q) = %q, want warning=%v", test.args, warnings, test.warn)
			}
			if test.warn && (len(warnings) != 1 || !strings.Contains(warnings[0], "TTY") || !strings.Contains(warnings[0], "Remove")) {
				t.Errorf("warning lacks actionable TTY guidance: %q", warnings)
			}
			if after := command.ExecutionArgs(); !reflect.DeepEqual(after, before) {
				t.Errorf("warning changed arguments: before=%q, after=%q", before, after)
			}
		})
	}
}

func TestDockerTerminalWarningsRecognizeCommandForms(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		command CommandConfig
		warn    bool
	}{
		{"structured", CommandConfig{Executor: ExecutorDocker, Image: "image", ContainerArgs: []string{"-it"}}, true},
		{"structured typo", CommandConfig{Executor: ExecutorDocker, Image: "image", ContainerArgs: []string{"--it"}}, true},
		{"direct command", CommandConfig{Executor: ExecutorCommand, Program: "/usr/bin/docker", Args: []string{"run", "-it", "image"}}, true},
		{"implicit command", CommandConfig{Program: "docker", Args: []string{"run", "-it", "image"}}, true},
		{"application flags", CommandConfig{Executor: ExecutorDocker, Image: "image", CommandArgs: []string{"--it"}}, false},
		{"other program", CommandConfig{Executor: ExecutorCommand, Program: "processor", Args: []string{"run", "-it", "image"}}, false},
		{"shell source", CommandConfig{Executor: ExecutorShell, Command: "docker run -it image"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.command.Warnings(); (len(got) > 0) != test.warn {
				t.Errorf("Warnings() = %q, want warning=%v", got, test.warn)
			}
		})
	}
}
