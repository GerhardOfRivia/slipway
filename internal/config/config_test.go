package config

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	filename := writeConfig(t, `
queue:
  workers: 4
  max_retries: 3
  retry_delay: 10s
database:
  path: ./queue.db
watches:
  - name: incoming
    path: ./incoming
    recursive: true
    process_existing: true
    reprocess_on_change: true
    include: ["*.csv"]
    exclude: ["*.partial"]
    settle_for: 3s
    pipeline:
      - name: process
        program: /usr/local/bin/process-file
        args: ["--input", "{{file}}"]
        timeout: 15m
        working_directory: /var/tmp
        output: "{{stem}}.json"
        env:
          MODE: batch
`)

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Queue.Workers != 4 || cfg.Queue.MaxRetries != 3 || cfg.Queue.RetryDelay.Duration != 10*time.Second {
		t.Fatalf("unexpected queue config: %+v", cfg.Queue)
	}
	wantDatabase, err := CanonicalDatabasePath(filepath.Join(filepath.Dir(filename), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Path != wantDatabase {
		t.Fatalf("database path = %q, want %q", cfg.Database.Path, wantDatabase)
	}
	if len(cfg.Watches) != 1 {
		t.Fatalf("watch count = %d", len(cfg.Watches))
	}
	watch := cfg.Watches[0]
	if !watch.Recursive || !watch.ProcessExisting || !watch.ReprocessOnChange {
		t.Fatalf("watch booleans not decoded: %+v", watch)
	}
	if watch.SettleFor.Duration != 3*time.Second {
		t.Fatalf("settle_for = %s", watch.SettleFor.Duration)
	}
	if want := filepath.Join(filepath.Dir(filename), "incoming"); watch.Path != want {
		t.Fatalf("watch path = %q, want %q", watch.Path, want)
	}
	command := watch.Pipeline[0]
	if command.Timeout.Duration != 15*time.Minute || command.WorkingDir != "/var/tmp" || command.Output != "{{stem}}.json" || command.Env["MODE"] != "batch" {
		t.Fatalf("unexpected command config: %+v", command)
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	filename := writeConfig(t, `
watches:
  - name: incoming
    path: ./incoming
    pipeline:
      - name: process
        program: process-file
`)

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Queue.Workers != runtime.NumCPU() {
		t.Errorf("workers = %d, want %d", cfg.Queue.Workers, runtime.NumCPU())
	}
	if cfg.Queue.RetryDelay.Duration != 10*time.Second {
		t.Errorf("retry delay = %s", cfg.Queue.RetryDelay.Duration)
	}
	wantDatabase, canonicalErr := CanonicalDatabasePath(filepath.Join(filepath.Dir(filename), "slipway.db"))
	if canonicalErr != nil {
		t.Fatal(canonicalErr)
	}
	if cfg.Database.Path != wantDatabase {
		t.Errorf("database path = %q, want %q", cfg.Database.Path, wantDatabase)
	}
	if want := filepath.Join(filepath.Dir(filename), "incoming"); cfg.Watches[0].Path != want {
		t.Errorf("watch path = %q, want %q", cfg.Watches[0].Path, want)
	}
	if cfg.Watches[0].SettleFor.Duration != time.Second {
		t.Errorf("settle duration = %s", cfg.Watches[0].SettleFor.Duration)
	}
}

func TestLoadPreservesExplicitZeroDurations(t *testing.T) {
	filename := writeConfig(t, `
queue:
  retry_delay: 0s
watches:
  - name: incoming
    path: ./incoming
    settle_for: 0s
    pipeline:
      - name: process
        program: process-file
`)

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Queue.RetryDelay.Duration != 0 {
		t.Errorf("retry delay = %s, want zero", cfg.Queue.RetryDelay.Duration)
	}
	if cfg.Watches[0].SettleFor.Duration != 0 {
		t.Errorf("settle duration = %s, want zero", cfg.Watches[0].SettleFor.Duration)
	}
}

func TestLoadCommandExecutorTypes(t *testing.T) {
	tests := []struct {
		name         string
		commandYAML  string
		wantExecutor string
		wantProgram  string
		wantArgs     []string
	}{
		{
			name:         "omitted executor defaults to command",
			commandYAML:  "        program: process-file\n",
			wantExecutor: "command",
			wantProgram:  "process-file",
		},
		{
			name:         "command",
			commandYAML:  "        executor: command\n        program: process-file\n",
			wantExecutor: "command",
			wantProgram:  "process-file",
		},
		{
			name:         "docker defaults program",
			commandYAML:  "        executor: docker\n        args: [\"run\", \"--rm\", \"{{file}}\"]\n",
			wantExecutor: "docker",
			wantProgram:  "docker",
			wantArgs:     []string{"run", "--rm", "{{file}}"},
		},
		{
			name:         "podman defaults program",
			commandYAML:  "        executor: podman\n",
			wantExecutor: "podman",
			wantProgram:  "podman",
		},
		{
			name:         "apptainer defaults program",
			commandYAML:  "        executor: apptainer\n",
			wantExecutor: "apptainer",
			wantProgram:  "apptainer",
		},
		{
			name:         "runtime program override",
			commandYAML:  "        executor: docker\n        program: docker-wrapper\n",
			wantExecutor: "docker",
			wantProgram:  "docker-wrapper",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filename := writeConfig(t, `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
`+test.commandYAML)

			cfg, err := Load(filename)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			command := cfg.Watches[0].Pipeline[0]
			if got := string(command.Executor); got != test.wantExecutor {
				t.Errorf("executor = %q, want %q", got, test.wantExecutor)
			}
			if command.Program != test.wantProgram {
				t.Errorf("program = %q, want %q", command.Program, test.wantProgram)
			}
			if test.wantArgs != nil && !slices.Equal(command.Args, test.wantArgs) {
				t.Fatalf("args = %#v, want %#v", command.Args, test.wantArgs)
			}
		})
	}
}

func TestLoadStructuredContainerExecutor(t *testing.T) {
	filename := writeConfig(t, `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: process
        executor: docker
        image: example/processor:1.2.3
        container_args: ["--rm", "--network=none"]
        mounts:
          - source: ./inputs/{{stem}}
            target: /data
            options:
              - ro
              - bind-propagation=rslave
              - consistency=cached
          - source: "{{dir}}"
            target: "{{dir}}/source"
        container_env:
          Z_MODE: batch
          INPUT_NAME: "{{basename}}"
        command: /app/process
        command_args: ["--input", "/data/{{basename}}"]
`)

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	command := cfg.Watches[0].Pipeline[0]
	if command.Program != "docker" {
		t.Fatalf("program = %q, want docker", command.Program)
	}
	resolvedMount := filepath.Join(filepath.Dir(filename), "inputs", "{{stem}}")
	wantArgs := []string{
		"run",
		"--rm", "--network=none",
		"--mount", "type=bind,source=" + resolvedMount + ",target=/data,ro,bind-propagation=rslave,consistency=cached",
		"--mount", "type=bind,source={{dir}},target={{dir}}/source",
		"--env", "INPUT_NAME={{basename}}",
		"--env", "Z_MODE=batch",
		"example/processor:1.2.3",
		"/app/process",
		"--input", "/data/{{basename}}",
	}
	if got := command.ExecutionArgs(); !slices.Equal(got, wantArgs) {
		t.Fatalf("ExecutionArgs() = %#v, want %#v", got, wantArgs)
	}
}

func TestContainerExecutionArgs(t *testing.T) {
	tests := []struct {
		name    string
		command CommandConfig
		want    []string
	}{
		{
			name: "podman run",
			command: CommandConfig{
				Executor:      ExecutorPodman,
				Image:         "example/image:latest",
				ContainerArgs: []string{"--rm"},
				Mounts: []MountConfig{{
					Source:  "/host",
					Target:  "/data",
					Options: []string{"relabel=shared", "ro", "bind-propagation=rslave", `custom=a,b"quoted"`},
				}},
				ContainerEnv: map[string]string{"MODE": "batch"},
				CommandArgs:  []string{"--input", "/data/file.csv"},
			},
			want: []string{
				"run", "--rm",
				"--mount", `type=bind,source=/host,target=/data,relabel=shared,ro,bind-propagation=rslave,"custom=a,b""quoted"""`,
				"--env", "MODE=batch",
				"example/image:latest", "--input", "/data/file.csv",
			},
		},
		{
			name: "raw apptainer remains unchanged",
			command: CommandConfig{
				Executor: ExecutorApptainer,
				Args:     []string{"exec", "--containall", "example.sif", "inspect", "a,b"},
			},
			want: []string{"exec", "--containall", "example.sif", "inspect", "a,b"},
		},
		{
			name: "apptainer exec",
			command: CommandConfig{
				Executor:      ExecutorApptainer,
				Image:         "example.sif",
				ContainerArgs: []string{"--containall"},
				Mounts:        []MountConfig{{Source: "/host", Target: "/data", Options: []string{"ro"}}},
				ContainerEnv: map[string]string{
					"LIST": `a=b,c"quoted"`,
					"MODE": "batch",
				},
				Command:     "/app/process",
				CommandArgs: []string{"/data/file.csv"},
			},
			want: []string{
				"exec", "--no-eval", "--containall",
				"--mount", "type=bind,source=/host,target=/data,ro",
				"--env", `"LIST=a=b,c""quoted"""`,
				"--env", "MODE=batch",
				"example.sif", "/app/process", "/data/file.csv",
			},
		},
		{
			name: "apptainer runscript",
			command: CommandConfig{
				Executor:    ExecutorApptainer,
				Image:       "example.sif",
				CommandArgs: []string{"file.csv"},
			},
			want: []string{"run", "--no-eval", "example.sif", "file.csv"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.command.ExecutionArgs(); !slices.Equal(got, test.want) {
				t.Fatalf("ExecutionArgs() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestLoadNormalizesLegacyReadOnlyMountOption(t *testing.T) {
	filename := writeConfig(t, `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: docker
        image: example/image
        mounts:
          - &legacy_read_only
            source: /first
            target: /first
            read_only: true
            options: [relabel=shared]
          - source: /second
            target: /second
            read_only: false
          - <<: *legacy_read_only
            source: /third
            target: /third
`)

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	command := cfg.Watches[0].Pipeline[0]
	wantMounts := []MountConfig{
		{Source: "/first", Target: "/first", Options: []string{"ro", "relabel=shared"}},
		{Source: "/second", Target: "/second"},
		{Source: "/third", Target: "/third", Options: []string{"ro", "relabel=shared"}},
	}
	if !reflect.DeepEqual(command.Mounts, wantMounts) {
		t.Fatalf("mounts = %#v, want %#v", command.Mounts, wantMounts)
	}
	wantArgs := []string{
		"run",
		"--mount", "type=bind,source=/first,target=/first,ro,relabel=shared",
		"--mount", "type=bind,source=/second,target=/second",
		"--mount", "type=bind,source=/third,target=/third,ro,relabel=shared",
		"example/image",
	}
	if got := command.ExecutionArgs(); !reflect.DeepEqual(got, wantArgs) {
		t.Fatalf("ExecutionArgs() = %#v, want %#v", got, wantArgs)
	}
}

func TestApptainerEnvironmentEncodingRoundTripsPflag(t *testing.T) {
	values := []string{
		"plain",
		`a"b`,
		`abc"`,
		"a,b",
		`a=b,c"quoted"`,
	}
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			encoded := formatApptainerEnvironment("VALUE=" + value)
			decoded := parsePflagStringToString(t, encoded)
			if got := decoded["VALUE"]; got != value {
				t.Fatalf("decoded environment = %q, want %q (encoded %q)", got, value, encoded)
			}
		})
	}
}

func parsePflagStringToString(t *testing.T, value string) map[string]string {
	t.Helper()
	var fields []string
	switch strings.Count(value, "=") {
	case 0:
		t.Fatalf("encoded environment has no assignment: %q", value)
	case 1:
		fields = []string{strings.Trim(value, `"`)}
	default:
		var err error
		fields, err = csv.NewReader(strings.NewReader(value)).Read()
		if err != nil {
			t.Fatalf("decode environment CSV %q: %v", value, err)
		}
	}
	decoded := make(map[string]string, len(fields))
	for _, field := range fields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 {
			t.Fatalf("decoded environment field is not an assignment: %q", field)
		}
		decoded[parts[0]] = parts[1]
	}
	return decoded
}

func TestValidatePreservesProgrammaticStructuredMode(t *testing.T) {
	cfg := Config{Watches: []WatchConfig{{
		Name: "incoming",
		Path: ".",
		Pipeline: []CommandConfig{{
			Name:     "container",
			Executor: ExecutorDocker,
			Image:    "{{ext}}",
		}},
	}}}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	command := cfg.Watches[0].Pipeline[0]
	command.Image = ""
	if err := command.ValidateExecution(); err == nil || !strings.Contains(err.Error(), "image is required") {
		t.Fatalf("ValidateExecution() error = %v, want preserved structured-mode error", err)
	}
}

func TestLoadRejectsUnknownCommandExecutor(t *testing.T) {
	_, err := Load(writeConfig(t, `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: kubernetes
`))
	if err == nil || !strings.Contains(err.Error(), ".executor") || !strings.Contains(err.Error(), "kubernetes") {
		t.Fatalf("Load() error = %v, want unknown-executor error", err)
	}
}

func TestLoadRejectsMalformedCommandExecutor(t *testing.T) {
	for _, executorYAML := range []string{
		"        executor: \"\"\n",
		"        executor: \"   \"\n",
		"        executor: []\n",
	} {
		_, err := Load(writeConfig(t, `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        program: run
`+executorYAML))
		if err == nil || !strings.Contains(err.Error(), "executor must be a non-empty scalar string") {
			t.Errorf("Load() error = %v, want malformed-executor error for %q", err, executorYAML)
		}
	}
}

func TestContainerMountOptionsRejectStructuralFields(t *testing.T) {
	reserved := []string{
		"type=bind",
		"source=/other", "src=/other",
		"target=/other", "destination=/other", "dest=/other", "dst=/other",
	}
	for _, option := range reserved {
		t.Run(option, func(t *testing.T) {
			command := CommandConfig{
				Executor: ExecutorDocker,
				Image:    "example/image",
				Mounts: []MountConfig{{
					Source:  "/host",
					Target:  "/data",
					Options: []string{option},
				}},
			}
			err := command.ValidateExecution()
			if err == nil || !strings.Contains(err.Error(), "must not override reserved bind mount field") {
				t.Fatalf("ValidateExecution() error = %v", err)
			}
		})
	}

	command := CommandConfig{
		Executor: ExecutorDocker,
		Image:    "example/image",
		Mounts: []MountConfig{{
			Source:  "/host",
			Target:  "/data",
			Options: []string{"relabel=shared\x00private"},
		}},
	}
	if err := command.ValidateExecution(); err == nil || !strings.Contains(err.Error(), "must not contain a NUL byte") {
		t.Fatalf("ValidateExecution() NUL error = %v", err)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "unknown field",
			yaml: `
queue: {workers: 1, workerz: 2}
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, program: run}]
`,
			wantErr: "workerz",
		},
		{
			name: "negative workers",
			yaml: `
queue: {workers: -1}
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, program: run}]
`,
			wantErr: "queue.workers",
		},
		{
			name: "duplicate watch",
			yaml: `
watches:
  - name: incoming
    path: a
    pipeline: [{name: run, program: run}]
  - name: incoming
    path: b
    pipeline: [{name: run, program: run}]
`,
			wantErr: "duplicated",
		},
		{
			name: "bad duration",
			yaml: `
watches:
  - name: incoming
    path: .
    settle_for: eventually
    pipeline: [{name: run, program: run}]
`,
			wantErr: "invalid duration",
		},
		{
			name: "bad glob",
			yaml: `
watches:
  - name: incoming
    path: .
    include: ["[broken"]
    pipeline: [{name: run, program: run}]
`,
			wantErr: "invalid glob",
		},
		{
			name: "empty pipeline",
			yaml: `
watches:
  - name: incoming
    path: .
`,
			wantErr: "pipeline",
		},
		{
			name: "missing command name",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{program: run}]
`,
			wantErr: "name is required",
		},
		{
			name: "missing command program",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, executor: command}]
`,
			wantErr: "program is required",
		},
		{
			name: "container fields on command executor",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, executor: command, program: run, image: example/image}]
`,
			wantErr: "container fields require",
		},
		{
			name: "explicit empty container fields on command executor",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, executor: command, program: run, mounts: []}]
`,
			wantErr: "container fields require",
		},
		{
			name: "structured and raw args",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, executor: docker, image: example/image, args: [run]}]
`,
			wantErr: ".args cannot be combined",
		},
		{
			name: "explicit empty structured field and raw args",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, executor: apptainer, args: [exec, image.sif, true], mounts: []}]
`,
			wantErr: ".args cannot be combined",
		},
		{
			name: "structured container missing image",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, executor: podman, command_args: [file.csv]}]
`,
			wantErr: ".image is required",
		},
		{
			name: "explicit empty structured image",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, executor: docker, image: ""}]
`,
			wantErr: ".image is required",
		},
		{
			name: "explicit empty container command",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, executor: docker, image: example/image, command: ""}]
`,
			wantErr: ".command must not be blank",
		},
		{
			name: "mount missing source",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: docker
        image: example/image
        mounts: [{target: /data}]
`,
			wantErr: ".mounts[0].source is required",
		},
		{
			name: "mount missing target",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: apptainer
        image: example.sif
        mounts: [{source: /host}]
`,
			wantErr: ".mounts[0].target is required",
		},
		{
			name: "invalid container environment",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: docker
        image: example/image
        container_env: {"BAD=NAME": value}
`,
			wantErr: ".container_env contains invalid variable",
		},
		{
			name: "relative mount target",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: docker
        image: example/image
        mounts: [{source: /host, target: data}]
`,
			wantErr: ".target must be an absolute container path",
		},
		{
			name: "container args option terminator",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: docker
        image: example/image
        container_args: ["--rm", "--"]
`,
			wantErr: ".container_args[1] must not be --",
		},
		{
			name: "blank bind mount option",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: docker
        image: example/image
        mounts: [{source: /host, target: /data, options: [""]}]
`,
			wantErr: ".mounts[0].options[0] must not be blank",
		},
		{
			name: "bind mount option without key",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: docker
        image: example/image
        mounts: [{source: /host, target: /data, options: ["=value"]}]
`,
			wantErr: ".mounts[0].options[0] must have a non-blank key",
		},
		{
			name: "bind mount option overrides structured field",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: podman
        image: example/image
        mounts: [{source: /host, target: /data, options: [source=/other]}]
`,
			wantErr: `.mounts[0].options[0] must not override reserved bind mount field "source"`,
		},
		{
			name: "unknown mount field",
			yaml: `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: run
        executor: docker
        image: example/image
        mounts: [{source: /host, destination: /data}]
`,
			wantErr: "destination",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, test.yaml))
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Load() error = %v, want error containing %q", err, test.wantErr)
			}
		})
	}
}

func TestLoadRejectsMultipleDocuments(t *testing.T) {
	filename := writeConfig(t, `
watches:
  - name: incoming
    path: .
    pipeline: [{name: run, program: run}]
---
queue: {workers: 2}
`)
	_, err := Load(filename)
	if err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("Load() error = %v", err)
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "slipway.yaml")
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return filename
}
