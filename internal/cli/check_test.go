package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckWarnsAboutDockerTerminalFlags(t *testing.T) {
	t.Parallel()
	for _, pipeline := range []string{
		`executor: docker, image: image, container_args: ["{{tty_flags}}"]`,
		`executor: docker, args: [run, --it, image]`,
		`program: /usr/bin/docker, args: [exec, -it, container, sh]`,
	} {
		directory := t.TempDir()
		path := filepath.Join(directory, "pipeline.yaml")
		contents := fmt.Sprintf(`
values: {tty_flags: -it}
database: {path: ./never-created.db}
watches:
  - name: incoming
    path: .
    pipeline: [{name: process, %s}]
`, pipeline)
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, flagArgs := range [][]string{nil, {"--raw"}} {
			var stdout, stderr bytes.Buffer
			if code := Run(append([]string{"check", path}, flagArgs...), &stdout, &stderr); code != 0 {
				t.Fatalf("check code=%d, stderr=%q", code, stderr.String())
			}
			for _, want := range []string{"warning:", path, `watch "incoming"`, `step 1 ("process")`, "TTY", "Remove"} {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("warning %q does not contain %q", stderr.String(), want)
				}
			}
			if strings.Count(stderr.String(), "warning:") != 1 || !strings.Contains(stdout.String(), "process") {
				t.Errorf("expected one warning and the pipeline display: stdout=%q, stderr=%q", stdout.String(), stderr.String())
			}
		}
		if _, err := os.Stat(filepath.Join(directory, "never-created.db")); !os.IsNotExist(err) {
			t.Errorf("check created a database: %v", err)
		}
	}
}

func TestQuoteShellWord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "safe", value: "registry.example/image:tag,mode=fast", want: "registry.example/image:tag,mode=fast"},
		{name: "empty", value: "", want: `''`},
		{name: "spaces", value: "two words", want: `'two words'`},
		{name: "single quote", value: "it's ready", want: `'it'"'"'s ready'`},
		{name: "shell syntax", value: "$HOME; *", want: `'$HOME; *'`},
		{name: "unicode", value: "café/路径", want: `'café/路径'`},
		{name: "newline", value: "line one\nline two", want: "'line one\nline two'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := quoteShellWord(test.value); got != test.want {
				t.Fatalf("quoteShellWord(%q) = %q, want %q", test.value, got, test.want)
			}
		})
	}
}

func TestFormatShellInvocationQuotesCommandGrammar(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		program string
		want    string
	}{
		{program: "convert", want: "convert --input file.csv"},
		{program: "if", want: "'if' --input file.csv"},
		{program: "MODE=convert", want: "'MODE=convert' --input file.csv"},
	} {
		if got := formatShellInvocation(test.program, []string{"--input", "file.csv"}); got != test.want {
			t.Errorf("formatShellInvocation(%q) = %q, want %q", test.program, got, test.want)
		}
	}
}

func TestCheckDisplaysConfiguredPipelines(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	configPath := filepath.Join(directory, "onderzeeer.yaml")
	databasePath := filepath.Join(directory, "never-created.db")
	configuration := `
values:
  shared_dir: /srv/shared
database:
  path: ./never-created.db
watches:
  - name: incoming
    path: ./incoming
    pipeline:
      - name: prepare
        program: convert
        args: ["--input", "{{file}}", "{{shared_dir}}/{{basename}}", "literal; $(no shell)", "", "it's ready"]
      - name: archive
        program: ./bin/archive
        args: ["{{file}}", "done file"]
        output: "{{stem}}.archive.log"
  - name: images
    path: ./images
    pipeline:
      - name: inspect
        program: /bin/true
`
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"check", configPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check) code = %d, stderr = %q", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("Run(check) stderr = %q, want empty", stderr.String())
	}
	want := fmt.Sprintf(`Config: %s
Watch: incoming
  1. prepare [command]: convert --input '{{file}}' '/srv/shared/{{basename}}' 'literal; $(no shell)' '' 'it'"'"'s ready'
  2. archive [command]: %s '{{file}}' 'done file'
     output: "{{stem}}.archive.log"
Watch: images
  1. inspect [command]: /bin/true
`, configPath, filepath.Join(directory, "bin", "archive"))
	if got := stdout.String(); got != want {
		t.Fatalf("Run(check) output = %q, want %q", got, want)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"check", "--raw", configPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check --raw) code = %d, stderr = %q", code, stderr.String())
	}
	rawWant := fmt.Sprintf(`Config: %s
Watch: incoming
  1. prepare [command]: "convert" ["--input","{{file}}","/srv/shared/{{basename}}","literal; $(no shell)","","it's ready"]
  2. archive [command]: %q ["{{file}}","done file"]
     output: "{{stem}}.archive.log"
Watch: images
  1. inspect [command]: "/bin/true" []
`, configPath, filepath.Join(directory, "bin", "archive"))
	if got := stdout.String(); got != rawWant {
		t.Fatalf("Run(check --raw) output = %q, want %q", got, rawWant)
	}
	if _, err := os.Stat(databasePath); !os.IsNotExist(err) {
		t.Fatalf("check created database %s: %v", databasePath, err)
	}
}

func TestCheckDisplaysCanonicalExecutorTypes(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	configPath := filepath.Join(directory, "onderzeeer.yaml")
	configuration := `
watches:
  - name: mixed
    path: .
    pipeline:
      - name: host
        program: inspect
        args: ["{{file}}"]
      - name: dockerized
        executor: docker
        image: example/docker:latest
        container_args: ["--rm"]
        mounts:
          - source: "{{dir}}"
            target: /data
            options: [ro]
        container_env: {MODE: batch}
        command: inspect
        command_args: ["/data/{{basename}}"]
      - name: podmanized
        executor: podman
        args: ["run", "--rm", "example/podman:latest"]
      - name: contained
        executor: apptainer
        image: example.sif
        command: inspect
        command_args: ["{{file}}"]
      - name: wrapped
        executor: docker
        program: ./bin/docker-wrapper
        args: ["version"]
`
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"check", configPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check) code = %d, stderr = %q", code, stderr.String())
	}
	want := fmt.Sprintf(`Config: %s
Watch: mixed
  1. host [command]: inspect '{{file}}'
  2. dockerized [docker]: docker run --rm --mount 'type=bind,source={{dir}},target=/data,ro' --env MODE=batch example/docker:latest inspect '/data/{{basename}}'
  3. podmanized [podman]: podman run --rm example/podman:latest
  4. contained [apptainer]: apptainer exec --no-eval %s inspect '{{file}}'
  5. wrapped [docker]: %s version
`, configPath, filepath.Join(directory, "example.sif"), filepath.Join(directory, "bin", "docker-wrapper"))
	if got := stdout.String(); got != want {
		t.Fatalf("Run(check) output = %q, want %q", got, want)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"check", configPath, "--raw"}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check --raw) code = %d, stderr = %q", code, stderr.String())
	}
	rawWant := fmt.Sprintf(`Config: %s
Watch: mixed
  1. host [command]: "inspect" ["{{file}}"]
  2. dockerized [docker]: "docker" ["run","--rm","--mount","type=bind,source={{dir}},target=/data,ro","--env","MODE=batch","example/docker:latest","inspect","/data/{{basename}}"]
  3. podmanized [podman]: "podman" ["run","--rm","example/podman:latest"]
  4. contained [apptainer]: "apptainer" ["exec","--no-eval",%q,"inspect","{{file}}"]
  5. wrapped [docker]: %q ["version"]
`, configPath, filepath.Join(directory, "example.sif"), filepath.Join(directory, "bin", "docker-wrapper"))
	if got := stdout.String(); got != rawWant {
		t.Fatalf("Run(check --raw) output = %q, want %q", got, rawWant)
	}
}

func TestCheckDisplaysShellExecutorLowering(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	configPath := filepath.Join(directory, "onderzeeer.yaml")
	configuration := `
values:
  tool: process-file
watches:
  - name: incoming
    path: .
    pipeline:
      - name: process-sidecars
        executor: shell
        command: 'for file in "$1"/*.csv; do {{tool}} -- "$file"; done'
        command_args: ["{{dir}}", "literal; $(still data)"]
`
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"check", configPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check) code = %d, stderr = %q", code, stderr.String())
	}
	want := fmt.Sprintf(`Config: %s
Watch: incoming
  1. process-sidecars [shell]: /bin/sh -c 'for file in "$1"/*.csv; do process-file -- "$file"; done' process-sidecars '{{dir}}' 'literal; $(still data)'
`, configPath)
	if got := stdout.String(); got != want {
		t.Fatalf("Run(check) output = %q, want %q", got, want)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"check", "--raw", configPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check --raw) code = %d, stderr = %q", code, stderr.String())
	}
	rawWant := fmt.Sprintf(`Config: %s
Watch: incoming
  1. process-sidecars [shell]: "/bin/sh" ["-c","for file in \"$1\"/*.csv; do process-file -- \"$file\"; done","process-sidecars","{{dir}}","literal; $(still data)"]
`, configPath)
	if got := stdout.String(); got != rawWant {
		t.Fatalf("Run(check --raw) output = %q, want %q", got, rawWant)
	}
}

func TestCheckUsesExplicitConfig(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	t.Setenv("ONDERZEEER_CONFIG", "environment.yaml")
	for _, name := range []string{"onderzeeer", "onderzeeer.yaml", "environment.yaml"} {
		if err := os.WriteFile(name, []byte("invalid: implicit config must not load\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	contents := "watches: [{name: incoming, path: ., pipeline: [{name: inspect, program: inspect}]}]\n"
	if err := os.WriteFile("explicit.yaml", []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"check", "explicit.yaml"}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check) code = %d, stderr = %q", code, stderr.String())
	}
	want := fmt.Sprintf("Config: %s\nWatch: incoming\n  1. inspect [command]: inspect\n", filepath.Join(directory, "explicit.yaml"))
	if got := stdout.String(); got != want {
		t.Fatalf("Run(check) output = %q, want %q", got, want)
	}
}

func TestCheckAcceptsDashPrefixedConfig(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	contents := "watches: [{name: incoming, path: ., pipeline: [{name: inspect, program: inspect}]}]\n"
	if err := os.WriteFile("-pipeline.yaml", []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"check", "--", "-pipeline.yaml"}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check) code = %d, stderr = %q", code, stderr.String())
	}
	if want := "Config: " + filepath.Join(directory, "-pipeline.yaml"); !strings.Contains(stdout.String(), want) {
		t.Fatalf("Run(check) output = %q, want %q", stdout.String(), want)
	}
}

func TestCheckDisplaysConfigDirectoryInDiscoveryOrder(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	configs := filepath.Join(directory, "configs")
	if err := os.Mkdir(configs, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		filename string
		watch    string
		command  string
	}{
		{filename: "b.yml", watch: "beta", command: "second"},
		{filename: "a.yaml", watch: "alpha", command: "first"},
	} {
		contents := fmt.Sprintf(`
watches:
  - name: %s
    path: .
    pipeline: [{name: %s, program: %s}]
`, test.watch, test.command, test.command)
		if err := os.WriteFile(filepath.Join(configs, test.filename), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(configs, "ignored.txt"), []byte("not YAML"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"check", configs}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check directory) code = %d, stderr = %q", code, stderr.String())
	}
	output := stdout.String()
	alpha := "Config: " + filepath.Join(configs, "a.yaml")
	beta := "Config: " + filepath.Join(configs, "b.yml")
	if alphaIndex, betaIndex := strings.Index(output, alpha), strings.Index(output, beta); alphaIndex < 0 || betaIndex < 0 || alphaIndex >= betaIndex {
		t.Fatalf("Run(check directory) output is not in discovery order: %q", output)
	}
	for _, want := range []string{"Watch: alpha", `1. first [command]: first`, "Watch: beta", `1. second [command]: second`} {
		if !strings.Contains(output, want) {
			t.Errorf("Run(check directory) output %q does not contain %q", output, want)
		}
	}
}

func TestCheckRejectsInvalidConfigWithoutOutput(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "invalid.yaml")
	configuration := `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: inspect
        program: inspect
        argz: ["typo"]
`
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"check", configPath}, &stdout, &stderr); code != 1 {
		t.Fatalf("Run(check invalid) code = %d, want 1; stderr = %q", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("Run(check invalid) stdout = %q, want empty", stdout.String())
	}
	for _, want := range []string{"onderzeeer: load " + configPath, "argz"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("Run(check invalid) stderr %q does not contain %q", stderr.String(), want)
		}
	}
}

func TestCheckUsageAndHelp(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"check", "first.yaml", "unexpected"}, &stdout, &stderr); code != 2 {
		t.Fatalf("Run(check positional) code = %d, want 2", code)
	}
	if got, want := stderr.String(), "onderzeeer: check expects 1 positional argument (config path)\n"; got != want {
		t.Fatalf("Run(check positional) stderr = %q, want %q", got, want)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"check", "--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(check --help) code = %d, want 0", code)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "Usage: onderzeeer check [--raw] <config>") {
		t.Fatalf("Run(check --help) stdout/stderr = %q / %q", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("Run(--help) code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "onderzeeer check [--raw] <config>") {
		t.Fatalf("Run(--help) output = %q, want check command", stdout.String())
	}
}
