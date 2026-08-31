package cli

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"gopkg.in/yaml.v3"
)

type generatedPipeline struct {
	Pipeline []generatedPipelineStep `yaml:"pipeline"`
}

type generatedPipelineStep struct {
	Name     string              `yaml:"name"`
	Executor config.ExecutorType `yaml:"executor"`
	Program  string              `yaml:"program,omitempty"`
	Args     []string            `yaml:"args,omitempty"`
}

func parseCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("parse", stderr, "slipway parse [--name name] -- <program> [argument ...]")
	name := flags.String("name", "", "pipeline step name (defaults from the program)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	invocation := flags.Args()
	if len(invocation) == 0 {
		return usageError{message: "parse requires a command"}
	}
	program := invocation[0]
	if strings.TrimSpace(program) == "" {
		return usageError{message: "parse command must not be blank"}
	}

	nameWasSet := false
	flags.Visit(func(visited *flag.Flag) {
		if visited.Name == "name" {
			nameWasSet = true
		}
	})
	stepName := strings.TrimSpace(*name)
	if nameWasSet && stepName == "" {
		return usageError{message: "parse --name must not be blank"}
	}
	executor := generatedExecutor(program)
	commandArgs := append([]string(nil), invocation[1:]...)
	if stepName == "" {
		stepName = generatedStepName(program, executor, commandArgs)
	}
	step := generatedPipelineStep{
		Name:     stepName,
		Executor: executor,
		Args:     commandArgs,
	}
	if executor == config.ExecutorCommand || program != string(executor) {
		step.Program = program
	}

	encoder := yaml.NewEncoder(stdout)
	encoder.SetIndent(2)
	if err := encoder.Encode(generatedPipeline{Pipeline: []generatedPipelineStep{step}}); err != nil {
		return fmt.Errorf("write pipeline config: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("write pipeline config: %w", err)
	}
	return nil
}

func generatedExecutor(program string) config.ExecutorType {
	switch filepath.Base(program) {
	case string(config.ExecutorDocker):
		return config.ExecutorDocker
	case string(config.ExecutorPodman):
		return config.ExecutorPodman
	case string(config.ExecutorApptainer):
		return config.ExecutorApptainer
	default:
		return config.ExecutorCommand
	}
}

func generatedStepName(program string, executor config.ExecutorType, args []string) string {
	name := strings.TrimSpace(filepath.Base(program))
	if name == "" {
		name = "command"
	}
	if executor != config.ExecutorCommand && len(args) > 0 && (args[0] == "run" || args[0] == "exec") {
		name += "-" + args[0]
	}
	return name
}
