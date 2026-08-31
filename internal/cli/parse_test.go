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

func TestParseDockerRunUsesStructuredFields(t *testing.T) {
	t.Parallel()
	invocation := []string{
		"parse", "--name", "gpu-check", "--", "docker", "run", "--rm", "--gpus", "all",
		"--mount", "type=bind,source={{dir}}/input files,target=/input,readonly",
		"-v", "/scratch/output files:/output:ro",
		"--env", "INPUT={{basename}}", "-eMODE=batch mode",
		"nvidia/cuda:12.8.1-base-ubuntu24.04", "nvidia-smi", "--query-gpu=name", "",
	}
	code, stdout, stderr := runParseCLI(invocation...)
	if code != 0 || stderr != "" {
		t.Fatalf("Run(%v) = code %d, stderr %q", invocation, code, stderr)
	}
	if strings.Contains(stdout, "read_only:") || !strings.Contains(stdout, "- readonly") {
		t.Fatalf("generated mount access mode was not emitted through options:\n%s", stdout)
	}
	fragment := decodeGeneratedPipeline(t, stdout)
	if len(fragment.Pipeline) != 1 {
		t.Fatalf("pipeline = %+v", fragment.Pipeline)
	}
	step := fragment.Pipeline[0]
	if step.Name != "gpu-check" || step.Executor != config.ExecutorDocker || step.Program != "" {
		t.Fatalf("step = %+v", step)
	}
	if step.Args != nil {
		t.Errorf("args = %#v, want nil", step.Args)
	}
	if step.Image != "nvidia/cuda:12.8.1-base-ubuntu24.04" || step.Command != "nvidia-smi" {
		t.Errorf("image/command = %q/%q", step.Image, step.Command)
	}
	if want := []string{"--rm", "--gpus", "all"}; !reflect.DeepEqual(step.ContainerArgs, want) {
		t.Errorf("container_args = %#v, want %#v", step.ContainerArgs, want)
	}
	wantMounts := []config.MountConfig{
		{Source: "{{dir}}/input files", Target: "/input", Options: []string{"readonly"}},
		{Source: "/scratch/output files", Target: "/output", Options: []string{"ro"}},
	}
	if !reflect.DeepEqual(step.Mounts, wantMounts) {
		t.Errorf("mounts = %#v, want %#v", step.Mounts, wantMounts)
	}
	if want := map[string]string{"INPUT": "{{basename}}", "MODE": "batch mode"}; !reflect.DeepEqual(step.ContainerEnv, want) {
		t.Errorf("container_env = %#v, want %#v", step.ContainerEnv, want)
	}
	if want := []string{"--query-gpu=name", ""}; !reflect.DeepEqual(step.CommandArgs, want) {
		t.Errorf("command_args = %#v, want %#v", step.CommandArgs, want)
	}
	wantExecutionArgs := []string{
		"run", "--rm", "--gpus", "all",
		"--mount", "type=bind,source={{dir}}/input files,target=/input,readonly",
		"--mount", "type=bind,source=/scratch/output files,target=/output,ro",
		"--env", "INPUT={{basename}}", "--env", "MODE=batch mode",
		"nvidia/cuda:12.8.1-base-ubuntu24.04", "nvidia-smi", "--query-gpu=name", "",
	}
	if !reflect.DeepEqual(step.ExecutionArgs(), wantExecutionArgs) {
		t.Errorf("ExecutionArgs() = %#v, want %#v", step.ExecutionArgs(), wantExecutionArgs)
	}
	loadedStep := loadGeneratedPipeline(t, stdout)
	if loadedStep.Program != "docker" || !reflect.DeepEqual(loadedStep.ExecutionArgs(), wantExecutionArgs) {
		t.Errorf("loaded generated step = %+v, execution args %#v", loadedStep, loadedStep.ExecutionArgs())
	}
}

func TestParseDockerRunTreatsLeadingOptionAsDefaultCommandArgs(t *testing.T) {
	t.Parallel()
	invocation := []string{
		"parse", "--", "docker", "run", "--rm", "--gpus", "all",
		"--mount", "type=bind,source={{dir}},target=/data,ro",
		"openalpr/openalpr:latest", "--json", "--country", "eu", "/data/{{basename}}",
	}
	code, stdout, stderr := runParseCLI(invocation...)
	if code != 0 || stderr != "" {
		t.Fatalf("Run(%v) = code %d, stderr %q", invocation, code, stderr)
	}
	step := decodeGeneratedPipeline(t, stdout).Pipeline[0]
	if step.Command != "" {
		t.Errorf("command = %q, want empty to use the image default", step.Command)
	}
	wantCommandArgs := []string{"--json", "--country", "eu", "/data/{{basename}}"}
	if !reflect.DeepEqual(step.CommandArgs, wantCommandArgs) {
		t.Errorf("command_args = %#v, want %#v", step.CommandArgs, wantCommandArgs)
	}
	if want := []config.MountConfig{{Source: "{{dir}}", Target: "/data", Options: []string{"ro"}}}; !reflect.DeepEqual(step.Mounts, want) {
		t.Errorf("mounts = %#v, want %#v", step.Mounts, want)
	}
	loaded := loadGeneratedPipeline(t, stdout)
	if want := invocation[3:]; !reflect.DeepEqual(loaded.ExecutionArgs(), want) {
		t.Errorf("ExecutionArgs() = %#v, want exact %#v", loaded.ExecutionArgs(), want)
	}
}

func TestParsePodmanRunUsesStructuredFieldsAndProgramOverride(t *testing.T) {
	t.Parallel()
	invocation := []string{
		"parse", "--", "/opt/bin/podman", "run", "-it", "--replace", "--userns=keep-id", "--systemd", "always",
		"--mount=type=bind,src={{dir}},dst=/data,ro", "-eMODE=test",
		"example/image", "inspect", "/data", "--format", "{{json .}}",
	}
	code, stdout, stderr := runParseCLI(invocation...)
	if code != 0 || stderr != "" {
		t.Fatalf("Run(%v) = code %d, stderr %q", invocation, code, stderr)
	}
	step := decodeGeneratedPipeline(t, stdout).Pipeline[0]
	if step.Name != "podman-run" || step.Executor != config.ExecutorPodman || step.Program != "/opt/bin/podman" {
		t.Fatalf("step = %+v", step)
	}
	if step.Args != nil || step.Image != "example/image" || step.Command != "inspect" {
		t.Errorf("structured identity fields = %+v", step)
	}
	if want := []string{"-it", "--replace", "--userns=keep-id", "--systemd", "always"}; !reflect.DeepEqual(step.ContainerArgs, want) {
		t.Errorf("container_args = %#v, want %#v", step.ContainerArgs, want)
	}
	if want := []config.MountConfig{{Source: "{{dir}}", Target: "/data", Options: []string{"ro"}}}; !reflect.DeepEqual(step.Mounts, want) {
		t.Errorf("mounts = %#v, want %#v", step.Mounts, want)
	}
	if want := map[string]string{"MODE": "test"}; !reflect.DeepEqual(step.ContainerEnv, want) {
		t.Errorf("container_env = %#v, want %#v", step.ContainerEnv, want)
	}
	if want := []string{"/data", "--format", "{{json .}}"}; !reflect.DeepEqual(step.CommandArgs, want) {
		t.Errorf("command_args = %#v, want %#v", step.CommandArgs, want)
	}
	loadGeneratedPipeline(t, stdout)
}

func TestParseContainerRunRecognizesArgumentBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		program         string
		args            []string
		wantContainer   []string
		wantImage       string
		wantCommand     string
		wantCommandArgs []string
	}{
		{
			name:            "future equals option and default-entrypoint arguments",
			program:         "docker",
			args:            []string{"run", "--future=value", "image", "--command-flag", "tail"},
			wantContainer:   []string{"--future=value"},
			wantImage:       "image",
			wantCommandArgs: []string{"--command-flag", "tail"},
		},
		{
			name:      "option terminator",
			program:   "docker",
			args:      []string{"run", "--", "registry.example/image:latest"},
			wantImage: "registry.example/image:latest",
		},
		{
			name:            "short cluster attached value and negative value",
			program:         "docker",
			args:            []string{"run", "-it", "-p8080:80", "--pids-limit", "-1", "image", "tool", "--"},
			wantContainer:   []string{"-it", "-p8080:80", "--pids-limit", "-1"},
			wantImage:       "image",
			wantCommand:     "tool",
			wantCommandArgs: []string{"--"},
		},
		{
			name:          "short boolean cluster with explicit value",
			program:       "docker",
			args:          []string{"run", "-it=false", "image"},
			wantContainer: []string{"-it=false"},
			wantImage:     "image",
		},
		{
			name:          "podman boolean",
			program:       "podman",
			args:          []string{"run", "--replace", "image"},
			wantContainer: []string{"--replace"},
			wantImage:     "image",
		},
		{
			name:            "container run alias",
			program:         "docker",
			args:            []string{"container", "run", "--rm", "image", "tool", "argument"},
			wantContainer:   []string{"--rm"},
			wantImage:       "image",
			wantCommand:     "tool",
			wantCommandArgs: []string{"argument"},
		},
		{
			name:          "environment file alone remains a container option",
			program:       "docker",
			args:          []string{"run", "--env-file", "vars.env", "image"},
			wantContainer: []string{"--env-file", "vars.env"},
			wantImage:     "image",
		},
		{
			name:            "blank command token is preserved in command args",
			program:         "docker",
			args:            []string{"run", "image", "", "tail"},
			wantImage:       "image",
			wantCommandArgs: []string{"", "tail"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invocation := append([]string{"parse", "--", test.program}, test.args...)
			code, stdout, stderr := runParseCLI(invocation...)
			if code != 0 || stderr != "" {
				t.Fatalf("Run(%v) = code %d, stderr %q", invocation, code, stderr)
			}
			step := decodeGeneratedPipeline(t, stdout).Pipeline[0]
			if step.Name != filepath.Base(test.program)+"-run" || step.Args != nil || !reflect.DeepEqual(step.ContainerArgs, test.wantContainer) ||
				step.Image != test.wantImage || step.Command != test.wantCommand ||
				!reflect.DeepEqual(step.CommandArgs, test.wantCommandArgs) {
				t.Errorf("step = %+v, want container_args=%#v image=%q command=%q command_args=%#v",
					step, test.wantContainer, test.wantImage, test.wantCommand, test.wantCommandArgs)
			}
			loadGeneratedPipeline(t, stdout)
		})
	}
}

func TestParseContainerRunKeepsUnrepresentableOptionGroups(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		program       string
		args          []string
		wantContainer []string
		wantMounts    []config.MountConfig
		wantEnv       map[string]string
	}{
		{
			name:          "inherited environment",
			program:       "podman",
			args:          []string{"run", "-e", "HOME", "--mount", "type=bind,source=/host,target=/data", "image"},
			wantContainer: []string{"-e", "HOME"},
			wantMounts:    []config.MountConfig{{Source: "/host", Target: "/data"}},
		},
		{
			name:          "duplicate and file-sourced environment",
			program:       "docker",
			args:          []string{"run", "-e", "MODE=one", "--env-file", "vars.env", "--env=MODE=two", "image"},
			wantContainer: []string{"-e", "MODE=one", "--env-file", "vars.env", "--env=MODE=two"},
		},
		{
			name:          "named volume",
			program:       "docker",
			args:          []string{"run", "-v", "cache:/cache", "--env", "MODE=batch", "image"},
			wantContainer: []string{"-v", "cache:/cache"},
			wantEnv:       map[string]string{"MODE": "batch"},
		},
		{
			name:    "advanced long bind options are extracted",
			program: "podman",
			args: []string{"run", "--mount", "type=bind,source=/first,target=/first",
				"--mount", "type=bind,source=/second,target=/second,relabel=shared", "image"},
			wantMounts: []config.MountConfig{
				{Source: "/first", Target: "/first"},
				{Source: "/second", Target: "/second", Options: []string{"relabel=shared"}},
			},
		},
		{
			name:          "bind and tmpfs stay ordered",
			program:       "docker",
			args:          []string{"run", "--mount", "type=bind,source=/host,target=/data", "--tmpfs", "/tmp", "image"},
			wantContainer: []string{"--mount", "type=bind,source=/host,target=/data", "--tmpfs", "/tmp"},
		},
		{
			name:    "advanced volume mode keeps all mounts in place",
			program: "docker",
			args: []string{"run", "--mount", "type=bind,source=/first,target=/first",
				"-v", "/second:/second:ro,rshared", "image"},
			wantContainer: []string{"--mount", "type=bind,source=/first,target=/first",
				"-v", "/second:/second:ro,rshared"},
		},
		{
			name:    "duplicate access modes remain ordered mount options",
			program: "podman",
			args: []string{"run", "--mount", "type=bind,source=/first,target=/first",
				"--mount", "type=bind,source=/second,target=/second,ro,rw", "image"},
			wantMounts: []config.MountConfig{
				{Source: "/first", Target: "/first"},
				{Source: "/second", Target: "/second", Options: []string{"ro", "rw"}},
			},
		},
		{
			name:          "unknown inline option disables field extraction",
			program:       "docker",
			args:          []string{"run", "--future=value", "--env", "MODE=batch", "--mount", "type=bind,source=/host,target=/data", "image"},
			wantContainer: []string{"--future=value", "--env", "MODE=batch", "--mount", "type=bind,source=/host,target=/data"},
		},
		{
			name:    "clustered environment and volume stay in place",
			program: "docker",
			args: []string{"run", "-ieMODE=test", "-itv/host:/data", "--env", "EXTRA=yes",
				"--mount", "type=bind,source=/other,target=/other", "image"},
			wantContainer: []string{"-ieMODE=test", "-itv/host:/data", "--env", "EXTRA=yes",
				"--mount", "type=bind,source=/other,target=/other"},
		},
		{
			name:          "mount whitespace is not normalized",
			program:       "docker",
			args:          []string{"run", "--mount", "type=bind, source=/host,target=/data,readonly= true", "image"},
			wantContainer: []string{"--mount", "type=bind, source=/host,target=/data,readonly= true"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invocation := append([]string{"parse", "--", test.program}, test.args...)
			code, stdout, stderr := runParseCLI(invocation...)
			if code != 0 || stderr != "" {
				t.Fatalf("Run(%v) = code %d, stderr %q", invocation, code, stderr)
			}
			step := decodeGeneratedPipeline(t, stdout).Pipeline[0]
			if step.Args != nil || step.Image != "image" || !reflect.DeepEqual(step.ContainerArgs, test.wantContainer) ||
				!reflect.DeepEqual(step.Mounts, test.wantMounts) || !reflect.DeepEqual(step.ContainerEnv, test.wantEnv) {
				t.Errorf("step = %+v, want container_args=%#v mounts=%#v env=%#v",
					step, test.wantContainer, test.wantMounts, test.wantEnv)
			}
			loaded := loadGeneratedPipeline(t, stdout)
			if !reflect.DeepEqual(loaded.ExecutionArgs(), test.args) {
				t.Errorf("ExecutionArgs() = %#v, want exact %#v", loaded.ExecutionArgs(), test.args)
			}
		})
	}
}

func TestParseContainerRunExtractsMountAndEnvironmentForms(t *testing.T) {
	t.Parallel()
	invocation := []string{
		"parse", "--", "podman", "run",
		`--mount=type=bind,"source=/host/a,b ",dest=/data,readwrite=false,bind-propagation=rshared,"custom=a,b"`,
		"--volume={{dir}}:/input:rw", "-e=EMPTY=", "--env=COMPLEX=a=b",
		"example/image",
	}
	code, stdout, stderr := runParseCLI(invocation...)
	if code != 0 || stderr != "" {
		t.Fatalf("Run(%v) = code %d, stderr %q", invocation, code, stderr)
	}
	step := decodeGeneratedPipeline(t, stdout).Pipeline[0]
	wantMounts := []config.MountConfig{
		{Source: "/host/a,b ", Target: "/data", Options: []string{"readwrite=false", "bind-propagation=rshared", "custom=a,b"}},
		{Source: "{{dir}}", Target: "/input"},
	}
	if !reflect.DeepEqual(step.Mounts, wantMounts) {
		t.Errorf("mounts = %#v, want %#v", step.Mounts, wantMounts)
	}
	if want := map[string]string{"EMPTY": "", "COMPLEX": "a=b"}; !reflect.DeepEqual(step.ContainerEnv, want) {
		t.Errorf("container_env = %#v, want %#v", step.ContainerEnv, want)
	}
	if step.Args != nil || step.Image != "example/image" {
		t.Errorf("step = %+v", step)
	}
	loadGeneratedPipeline(t, stdout)
}

func TestParseContainerRunFallsBackToRawArguments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		program string
		args    []string
		warning string
	}{
		{name: "unknown bare option", program: "docker", args: []string{"run", "--future", "value", "image"}, warning: "unknown option arity"},
		{name: "literal option terminator value", program: "docker", args: []string{"run", "--label", "--", "image"}, warning: "cannot be represented in container_args"},
		{name: "missing value", program: "docker", args: []string{"run", "--name"}, warning: "value is missing"},
		{name: "missing image", program: "docker", args: []string{"run", "--rm"}, warning: "no non-blank image"},
		{name: "option-like image after terminator", program: "docker", args: []string{"run", "--", "-image"}, warning: "needs -- to distinguish image"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invocation := append([]string{"parse", "--", test.program}, test.args...)
			code, stdout, stderr := runParseCLI(invocation...)
			if code != 0 || !strings.Contains(stderr, "slipway parse: warning:") ||
				!strings.Contains(stderr, test.warning) || !strings.Contains(stderr, "emitted raw runtime args") {
				t.Fatalf("Run(%v) = code %d, stderr %q", invocation, code, stderr)
			}
			step := decodeGeneratedPipeline(t, stdout).Pipeline[0]
			if !reflect.DeepEqual(step.Args, test.args) {
				t.Errorf("args = %#v, want exact %#v", step.Args, test.args)
			}
			if step.Image != "" || len(step.ContainerArgs) != 0 || len(step.Mounts) != 0 ||
				len(step.ContainerEnv) != 0 || step.Command != "" || len(step.CommandArgs) != 0 {
				t.Errorf("raw fallback mixed in structured fields: %+v", step)
			}
			loadGeneratedPipeline(t, stdout)
		})
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
			name:     "apptainer",
			args:     []string{"parse", "--", "apptainer", "exec", "image.sif", "true"},
			wantName: "apptainer-exec",
			wantExec: config.ExecutorApptainer,
			wantArgs: []string{"exec", "image.sif", "true"},
		},
		{
			name:     "non-run docker action",
			args:     []string{"parse", "--", "docker", "exec", "container", "true"},
			wantName: "docker-exec",
			wantExec: config.ExecutorDocker,
			wantArgs: []string{"exec", "container", "true"},
		},
		{
			name:     "docker runtime-global option",
			args:     []string{"parse", "--", "docker", "--context", "remote", "run", "image"},
			wantName: "docker",
			wantExec: config.ExecutorDocker,
			wantArgs: []string{"--context", "remote", "run", "image"},
		},
		{
			name:     "podman runtime-global option",
			args:     []string{"parse", "--", "podman", "--remote", "run", "image"},
			wantName: "podman",
			wantExec: config.ExecutorPodman,
			wantArgs: []string{"--remote", "run", "image"},
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

func loadGeneratedPipeline(t *testing.T, fragment string) config.CommandConfig {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "slipway.yaml")
	indentedFragment := "    " + strings.ReplaceAll(strings.TrimSuffix(fragment, "\n"), "\n", "\n    ")
	completeConfig := "watches:\n  - name: incoming\n    path: .\n" + indentedFragment + "\n"
	if err := os.WriteFile(configPath, []byte(completeConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config containing generated fragment:\n%s\nerror: %v", completeConfig, err)
	}
	if len(loaded.Watches) != 1 || len(loaded.Watches[0].Pipeline) != 1 {
		t.Fatalf("loaded config has unexpected shape: %+v", loaded)
	}
	return loaded.Watches[0].Pipeline[0]
}

func runParseCLI(args ...string) (code int, stdout, stderr string) {
	var stdoutBuffer, stderrBuffer bytes.Buffer
	code = Run(args, &stdoutBuffer, &stderrBuffer)
	return code, stdoutBuffer.String(), stderrBuffer.String()
}
