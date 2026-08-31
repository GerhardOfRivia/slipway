package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"gopkg.in/yaml.v3"
)

func TestParseDockerCommandPreservesArguments(t *testing.T) {
	t.Parallel()
	invocation := []string{
		"parse", "--", "docker", "run", "--gpus", "all", "--label", "note=a b;$(literal)",
		"nvidia/cuda:12.8.1-base-ubuntu24.04", "nvidia-smi", "", "{{file}}",
	}
	code, stdout, stderr := runParseCLI(invocation...)
	if code != 0 || stderr != "" {
		t.Fatalf("Run(%v) = code %d, stderr %q", invocation, code, stderr)
	}
	fragment := decodeGeneratedPipeline(t, stdout)
	if len(fragment.Pipeline) != 1 {
		t.Fatalf("pipeline = %+v", fragment.Pipeline)
	}
	step := fragment.Pipeline[0]
	if step.Name != "docker-run" || step.Executor != config.ExecutorDocker || step.Program != "" {
		t.Fatalf("step = %+v", step)
	}
	if want := invocation[3:]; !reflect.DeepEqual(step.Args, want) {
		t.Errorf("args = %#v, want %#v", step.Args, want)
	}
	if !reflect.DeepEqual(step.ExecutionArgs(), invocation[3:]) {
		t.Errorf("ExecutionArgs() = %#v, want %#v", step.ExecutionArgs(), invocation[3:])
	}
	if strings.Contains(stdout, "program:") || strings.Contains(stdout, "image:") || strings.Contains(stdout, "container_args:") {
		t.Errorf("generated config inferred or redundantly set fields:\n%s", stdout)
	}
	configPath := filepath.Join(t.TempDir(), "slipway.yaml")
	indentedFragment := "    " + strings.ReplaceAll(strings.TrimSuffix(stdout, "\n"), "\n", "\n    ")
	completeConfig := "watches:\n  - name: incoming\n    path: .\n" + indentedFragment + "\n"
	if err := os.WriteFile(configPath, []byte(completeConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config containing generated fragment:\n%s\nerror: %v", completeConfig, err)
	}
	loadedStep := loaded.Watches[0].Pipeline[0]
	if loadedStep.Program != "docker" || !reflect.DeepEqual(loadedStep.ExecutionArgs(), invocation[3:]) {
		t.Errorf("loaded generated step = %+v, execution args %#v", loadedStep, loadedStep.ExecutionArgs())
	}
}

func TestParseSelectsExecutorAndProgramOverride(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		args        []string
		wantName    string
		wantExec    config.ExecutorType
		wantProgram string
		wantArgs    []string
	}{
		{
			name:        "ordinary command",
			args:        []string{"parse", "--name", "gpu-check", "--", "/usr/bin/nvidia-smi", "--query-gpu=name"},
			wantName:    "gpu-check",
			wantExec:    config.ExecutorCommand,
			wantProgram: "/usr/bin/nvidia-smi",
			wantArgs:    []string{"--query-gpu=name"},
		},
		{
			name:        "runtime path",
			args:        []string{"parse", "--", "/opt/bin/podman", "run", "--rm", "example/image"},
			wantName:    "podman-run",
			wantExec:    config.ExecutorPodman,
			wantProgram: "/opt/bin/podman",
			wantArgs:    []string{"run", "--rm", "example/image"},
		},
		{
			name:     "apptainer",
			args:     []string{"parse", "--", "apptainer", "exec", "image.sif", "true"},
			wantName: "apptainer-exec",
			wantExec: config.ExecutorApptainer,
			wantArgs: []string{"exec", "image.sif", "true"},
		},
		{
			name:        "blank basename fallback",
			args:        []string{"parse", "--", "/tmp/ "},
			wantName:    "command",
			wantExec:    config.ExecutorCommand,
			wantProgram: "/tmp/ ",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			code, stdout, stderr := runParseCLI(test.args...)
			if code != 0 || stderr != "" {
				t.Fatalf("Run(%v) = code %d, stderr %q", test.args, code, stderr)
			}
			fragment := decodeGeneratedPipeline(t, stdout)
			if len(fragment.Pipeline) != 1 {
				t.Fatalf("pipeline = %+v", fragment.Pipeline)
			}
			step := fragment.Pipeline[0]
			if step.Name != test.wantName || step.Executor != test.wantExec || step.Program != test.wantProgram || !reflect.DeepEqual(step.Args, test.wantArgs) {
				t.Errorf("step = %+v, want name=%q executor=%q program=%q args=%#v",
					step, test.wantName, test.wantExec, test.wantProgram, test.wantArgs)
			}
		})
	}
}

func TestParseUsageErrorsAndHelp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		args     []string
		wantCode int
		want     string
	}{
		{name: "missing command", args: []string{"parse"}, wantCode: 2, want: "parse requires a command"},
		{name: "blank command", args: []string{"parse", "--", " "}, wantCode: 2, want: "parse command must not be blank"},
		{name: "blank name", args: []string{"parse", "--name", " ", "--", "true"}, wantCode: 2, want: "parse --name must not be blank"},
		{name: "help", args: []string{"parse", "--help"}, wantCode: 0, want: "Usage: slipway parse [--name name] -- <program> [argument ...]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, stdout, stderr := runParseCLI(test.args...)
			if code != test.wantCode || stdout != "" || !strings.Contains(stderr, test.want) {
				t.Fatalf("Run(%v) = code %d, stdout %q, stderr %q; want code %d and stderr containing %q",
					test.args, code, stdout, stderr, test.wantCode, test.want)
			}
		})
	}
}

func TestTopLevelHelpIncludesParse(t *testing.T) {
	t.Parallel()
	code, stdout, stderr := runParseCLI("--help")
	if code != 0 || stderr != "" || !strings.Contains(stdout, "slipway parse [--name name] -- <program> [argument ...]") {
		t.Fatalf("Run(--help) = code %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

func decodeGeneratedPipeline(t *testing.T, contents string) generatedConfigFragment {
	t.Helper()
	var fragment generatedConfigFragment
	decoder := yaml.NewDecoder(strings.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&fragment); err != nil {
		t.Fatalf("decode generated pipeline %q: %v", contents, err)
	}
	return fragment
}

type generatedConfigFragment struct {
	Pipeline []config.CommandConfig `yaml:"pipeline"`
}

func runParseCLI(args ...string) (code int, stdout, stderr string) {
	var stdoutBuffer, stderrBuffer bytes.Buffer
	code = Run(args, &stdoutBuffer, &stderrBuffer)
	return code, stdoutBuffer.String(), stderrBuffer.String()
}
